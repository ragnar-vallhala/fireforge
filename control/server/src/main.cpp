// tdrcp — FireForge control plane (docs/design/phase2.md §8).
//
// Wiring only: config -> Postgres repository (runs migrations) -> ControlApi
// -> mTLS HttpsServer, plus a health-sweep timer. One io_context thread.

#include <charconv>
#include <chrono>
#include <csignal>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <string>

#include <boost/asio/io_context.hpp>
#include <boost/asio/signal_set.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/program_options.hpp>
#include <google/protobuf/stubs/common.h>

#include "tdr/common/common.hpp"
#include "tdr/common/config.hpp"
#include "tdr/cpapi/api.hpp"
#include "tdr/cpdb/db.hpp"
#include "tdr/http/http.hpp"
#include "tdr/tls/tls.hpp"

namespace asio = boost::asio;
namespace fs = std::filesystem;
namespace po = boost::program_options;

namespace {

struct CpConfig {
    std::string listen_address = "0.0.0.0";
    std::uint16_t port = 8080;
    tdr::tls::Credentials tls{"/etc/fireforge/certificates/ca.crt", "/etc/fireforge/certificates/server.crt",
                              "/etc/fireforge/server.key"};
    std::string conninfo = "postgresql:///fireforge";
    tdr::cpapi::ApiOptions api;
    std::string log_level = "info";
};

template <class T>
std::expected<T, std::string> parse_num(const std::string& v) {
    T out{};
    auto [p, ec] = std::from_chars(v.data(), v.data() + v.size(), out);
    if (ec != std::errc{} || p != v.data() + v.size()) return std::unexpected("not a number: '" + v + "'");
    return out;
}

std::expected<CpConfig, std::string> load(const fs::path& path) {
    std::ifstream in(path);
    if (!in) return std::unexpected("cannot read " + path.string());
    std::string text((std::istreambuf_iterator<char>(in)), {});
    auto ini = tdr::parse_ini(text);
    if (!ini) return std::unexpected(ini.error());

    CpConfig c;
    using Setter = std::function<std::expected<void, std::string>(const std::string&)>;
    auto str = [](std::string& d) -> Setter { return [&d](const std::string& v) -> std::expected<void, std::string> { d = v; return {}; }; };
    auto pth = [](fs::path& d) -> Setter { return [&d](const std::string& v) -> std::expected<void, std::string> { d = v; return {}; }; };
    auto num = []<class T>(T& d) -> Setter {
        return [&d](const std::string& v) -> std::expected<void, std::string> {
            auto r = parse_num<T>(v);
            if (!r) return std::unexpected(r.error());
            d = *r;
            return {};
        };
    };
    const std::map<std::string, std::map<std::string, Setter>> schema = {
        {"server", {{"listen_address", str(c.listen_address)}, {"port", num(c.port)}}},
        {"tls", {{"ca_file", pth(c.tls.ca_file)}, {"cert_file", pth(c.tls.cert_file)}, {"key_file", pth(c.tls.key_file)}}},
        {"database", {{"conninfo", str(c.conninfo)}}},
        {"health", {{"heartbeat_interval", num(c.api.heartbeat_interval_seconds)},
                    {"stale_after", num(c.api.stale_after_seconds)},
                    {"offline_after", num(c.api.offline_after_seconds)}}},
        {"log", {{"level", str(c.log_level)}}},
    };
    for (const auto& [sec, keys] : ini->sections) {
        if (sec.empty() && keys.empty()) continue;
        auto s = schema.find(sec);
        if (s == schema.end()) return std::unexpected("unknown section [" + sec + "]");
        for (const auto& [k, v] : keys) {
            auto f = s->second.find(k);
            if (f == s->second.end()) return std::unexpected("unknown key '" + k + "' in [" + sec + "]");
            if (auto r = f->second(v); !r) return std::unexpected("[" + sec + "] " + k + ": " + r.error());
        }
    }
    return c;
}

std::expected<void, std::string> validate(const CpConfig& c) {
    const auto& a = c.api;
    if (a.heartbeat_interval_seconds == 0) return std::unexpected("health.heartbeat_interval must be > 0");
    if (a.stale_after_seconds <= a.heartbeat_interval_seconds)
        return std::unexpected("health.stale_after must exceed heartbeat_interval");
    if (a.offline_after_seconds <= a.stale_after_seconds)
        return std::unexpected("health.offline_after must exceed stale_after");
    return {};
}

int run(const CpConfig& cfg, const std::string& port_file) {
    asio::io_context io;

    auto repo = tdr::cpdb::open_pg_repository(cfg.conninfo);
    tdr::log_info("database ready (schema version {})", tdr::cpdb::schema_version(cfg.conninfo));

    tdr::cpapi::ControlApi api(*repo, cfg.api, [] { return tdr::unix_ms(); });

    auto ctx = tdr::tls::make_server_context(cfg.tls);
    tdr::http::ServerOptions sopts;
    sopts.listen_address = cfg.listen_address;
    sopts.port = cfg.port;
    tdr::http::HttpsServer server(io, ctx, sopts, [&api](tdr::http::Request& r) { return api.handle(r); });
    server.start();
    tdr::log_info("tdrcp {} listening on {}:{}", TDR_VERSION, cfg.listen_address, server.bound_port());
    if (!port_file.empty()) {
        auto tmp = port_file + ".tmp";
        { std::ofstream(tmp) << server.bound_port() << "\n"; }
        fs::rename(tmp, port_file);
    }

    // Health sweep on the heartbeat interval. A database outage only logs;
    // the next tick retries (the repository reconnects).
    asio::steady_timer sweep_timer(io);
    std::function<void()> arm = [&] {
        sweep_timer.expires_after(std::chrono::seconds(cfg.api.heartbeat_interval_seconds));
        sweep_timer.async_wait([&](boost::system::error_code ec) {
            if (ec) return;
            try {
                api.sweep();
            } catch (const std::exception& e) {
                tdr::log_warn("health sweep failed: {}", e.what());
            }
            arm();
        });
    };
    arm();

    asio::signal_set signals(io, SIGINT, SIGTERM);
    signals.async_wait([&](boost::system::error_code ec, int sig) {
        if (ec) return;
        tdr::log_info("signal {}: shutting down", sig);
        sweep_timer.cancel();
        server.stop();
        io.stop();
    });

    io.run();
    tdr::log_info("tdrcp stopped");
    return 0;
}

}  // namespace

int main(int argc, char** argv) {
    std::string config_path, conninfo, listen, log_level, port_file;
    int port = -1;
    po::options_description desc("tdrcp options");
    desc.add_options()
        ("help,h", "show help")
        ("version", "print version")
        ("config,c", po::value(&config_path)->default_value("/etc/fireforge/tdrcp.conf"), "config file")
        ("database", po::value(&conninfo), "override database.conninfo")
        ("listen", po::value(&listen), "override server.listen_address")
        ("port", po::value(&port), "override server.port (0 = ephemeral)")
        ("port-file", po::value(&port_file), "write the bound port to this file once listening")
        ("log-level", po::value(&log_level), "override log level (debug|info|warn|error)");
    po::variables_map vm;
    try {
        po::store(po::parse_command_line(argc, argv, desc), vm);
        po::notify(vm);
    } catch (const std::exception& e) {
        std::cerr << "tdrcp: " << e.what() << "\n" << desc;
        return 2;
    }
    if (vm.count("help")) { std::cout << desc; return 0; }
    if (vm.count("version")) { std::cout << "tdrcp " << TDR_VERSION << "\n"; return 0; }

    std::signal(SIGPIPE, SIG_IGN);
    GOOGLE_PROTOBUF_VERIFY_VERSION;

    CpConfig cfg;
    if (fs::exists(config_path)) {
        auto loaded = load(config_path);
        if (!loaded) { std::cerr << "tdrcp: " << config_path << ": " << loaded.error() << "\n"; return 2; }
        cfg = *loaded;
    } else if (!vm["config"].defaulted()) {
        std::cerr << "tdrcp: config file not found: " << config_path << "\n";
        return 2;
    }
    if (!conninfo.empty()) cfg.conninfo = conninfo;
    if (!listen.empty()) cfg.listen_address = listen;
    if (port >= 0) {
        if (port > 65535) { std::cerr << "tdrcp: bad --port\n"; return 2; }
        cfg.port = static_cast<std::uint16_t>(port);
    }
    if (!log_level.empty()) cfg.log_level = log_level;
    if (auto v = validate(cfg); !v) { std::cerr << "tdrcp: " << v.error() << "\n"; return 2; }
    tdr::set_log_level(tdr::parse_log_level(cfg.log_level));

    try {
        return run(cfg, port_file);
    } catch (const std::exception& e) {
        tdr::log_error("fatal: {}", e.what());
        return 1;
    }
}
