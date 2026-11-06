// tdrcp — FireForge control plane (docs/design/phase2.md §8).
//
// Wiring only: config -> Postgres repository (runs migrations) -> ControlApi
// -> mTLS HttpsServer, plus health/lease, workflow and placement timers. One
// io_context thread. ControlApi applies [workflows] limits and [gangs] options to
// the repository; gangs are placed by the placement timer.

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
    // Phase 6 timers (seconds) and placement settings in config units.
    std::uint32_t workflow_sweep_seconds = 5;
    std::uint32_t placement_interval_seconds = 1;
    std::uint32_t reserve_window_seconds = 5;
    std::uint32_t max_reserve_seconds = 30;
    std::uint64_t strict_input_mb = 64;
    // Phase 7 [gangs] (seconds).
    std::uint32_t drain_timeout_seconds = 600;
    std::uint32_t ready_window_seconds = 5;
};

template <class T>
std::expected<T, std::string> parse_num(const std::string& v) {
    T out{};
    auto [p, ec] = std::from_chars(v.data(), v.data() + v.size(), out);
    if (ec != std::errc{} || p != v.data() + v.size()) return std::unexpected("not a number: '" + v + "'");
    return out;
}

// Config units (seconds, MiB) -> PlacementOptions.
void apply_placement(CpConfig& c) {
    auto& p = c.api.placement;
    p.reserve_window_ms = std::int64_t{c.reserve_window_seconds} * 1000;
    p.max_reserve_ms = std::int64_t{c.max_reserve_seconds} * 1000;
    p.strict_input_bytes = c.strict_input_mb << 20;
    auto& g = c.api.gangs;
    g.drain_timeout_ms = std::int64_t{c.drain_timeout_seconds} * 1000;
    g.ready_window_ms = std::int64_t{c.ready_window_seconds} * 1000;
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
        {"jobs", {{"lease_ttl", num(c.api.lease_ttl_seconds)}, {"max_attempts_default", num(c.api.max_attempts_default)},
                  {"locality_wait", num(c.api.locality_wait_seconds)}}},
        {"workflows", {{"sweep_interval", num(c.workflow_sweep_seconds)},
                       {"max_steps", num(c.api.workflow_limits.max_steps)},
                       {"max_spawn", num(c.api.workflow_limits.max_spawn)},
                       {"max_depth", num(c.api.workflow_limits.max_depth)}}},
        {"placement", {{"interval", num(c.placement_interval_seconds)},
                       {"reserve_window", num(c.reserve_window_seconds)},
                       {"max_reserve", num(c.max_reserve_seconds)},
                       {"strict_input_mb", num(c.strict_input_mb)},
                       {"w_compute", num(c.api.placement.w_compute)},
                       {"w_transfer", num(c.api.placement.w_transfer)},
                       {"w_queue", num(c.api.placement.w_queue)},
                       {"w_memory", num(c.api.placement.w_memory)},
                       {"w_contention", num(c.api.placement.w_contention)}}},
        {"gangs", {{"drain_timeout", num(c.drain_timeout_seconds)},
                   {"ready_window", num(c.ready_window_seconds)},
                   {"max_nodes", num(c.api.gangs.max_nodes)},
                   {"max_ranks", num(c.api.gangs.max_ranks)}}},
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
    apply_placement(c);
    return c;
}

std::expected<void, std::string> validate(const CpConfig& c) {
    const auto& a = c.api;
    if (a.heartbeat_interval_seconds == 0) return std::unexpected("health.heartbeat_interval must be > 0");
    if (a.stale_after_seconds <= a.heartbeat_interval_seconds)
        return std::unexpected("health.stale_after must exceed heartbeat_interval");
    if (a.offline_after_seconds <= a.stale_after_seconds)
        return std::unexpected("health.offline_after must exceed stale_after");
    if (a.lease_ttl_seconds <= a.heartbeat_interval_seconds)
        return std::unexpected("jobs.lease_ttl must exceed health.heartbeat_interval");
    if (a.max_attempts_default == 0) return std::unexpected("jobs.max_attempts_default must be >= 1");
    if (c.workflow_sweep_seconds == 0) return std::unexpected("workflows.sweep_interval must be > 0");
    if (a.workflow_limits.max_steps == 0 || a.workflow_limits.max_spawn == 0)
        return std::unexpected("workflows.max_steps and max_spawn must be >= 1");
    // Strict jobs become leasable only through the placement sweep (phase6.md §7).
    if (c.placement_interval_seconds == 0) return std::unexpected("placement.interval must be > 0");
    if (c.reserve_window_seconds == 0) return std::unexpected("placement.reserve_window must be > 0");
    if (c.strict_input_mb > (std::uint64_t{1} << 40)) return std::unexpected("placement.strict_input_mb is too large");
    const auto& p = a.placement;
    for (double w : {p.w_compute, p.w_transfer, p.w_queue, p.w_memory, p.w_contention})
        if (!(w >= 0)) return std::unexpected("placement weights must be >= 0");
    if (c.drain_timeout_seconds == 0) return std::unexpected("gangs.drain_timeout must be > 0");
    if (c.ready_window_seconds == 0) return std::unexpected("gangs.ready_window must be > 0");
    if (a.gangs.max_nodes == 0 || a.gangs.max_ranks == 0)
        return std::unexpected("gangs.max_nodes and max_ranks must be >= 1");
    return {};
}

int run(const CpConfig& cfg, const std::string& port_file) {
    asio::io_context io;

    auto repo = tdr::cpdb::open_pg_repository(cfg.conninfo);
    tdr::log_info("database ready (schema version {})", tdr::cpdb::schema_version(cfg.conninfo));

    tdr::cpapi::ControlApi api(*repo, cfg.api, [] { return tdr::unix_ms(); });
    if (cfg.api.lease_ttl_seconds < 3 * cfg.api.heartbeat_interval_seconds)
        tdr::log_warn("jobs.lease_ttl {} s is less than 3 heartbeat intervals; leases may expire on a missed heartbeat",
                      cfg.api.lease_ttl_seconds);

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

    // Health and lease-expiry sweeps on the heartbeat interval. A database outage only logs;
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
            try {
                api.sweep_leases();
            } catch (const std::exception& e) {
                tdr::log_warn("lease sweep failed: {}", e.what());
            }
            arm();
        });
    };
    arm();

    // Phase 6: workflow safety-net sweep and ranked placement. Errors only log.
    asio::steady_timer workflow_timer(io), placement_timer(io);
    std::function<void()> arm_workflows = [&] {
        workflow_timer.expires_after(std::chrono::seconds(cfg.workflow_sweep_seconds));
        workflow_timer.async_wait([&](boost::system::error_code ec) {
            if (ec) return;
            try {
                api.sweep_workflows();
            } catch (const std::exception& e) {
                tdr::log_warn("workflow sweep failed: {}", e.what());
            }
            arm_workflows();
        });
    };
    std::function<void()> arm_placement = [&] {
        placement_timer.expires_after(std::chrono::seconds(cfg.placement_interval_seconds));
        placement_timer.async_wait([&](boost::system::error_code ec) {
            if (ec) return;
            try {
                api.place_jobs();
            } catch (const std::exception& e) {
                tdr::log_warn("placement sweep failed: {}", e.what());
            }
            arm_placement();
        });
    };
    arm_workflows();
    arm_placement();

    asio::signal_set signals(io, SIGINT, SIGTERM);
    signals.async_wait([&](boost::system::error_code ec, int sig) {
        if (ec) return;
        tdr::log_info("signal {}: shutting down", sig);
        sweep_timer.cancel();
        workflow_timer.cancel();
        placement_timer.cancel();
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
