// tdr.conf parsing. See config.hpp and deploy/etc/tdr.conf.example.
#include "tdr/common/config.hpp"

#include <charconv>
#include <format>
#include <fstream>
#include <functional>
#include <limits>
#include <sstream>
#include <string_view>

namespace tdr {

namespace {

std::string_view trim(std::string_view s) {
    constexpr std::string_view ws = " \t\r\n\f\v";
    const auto b = s.find_first_not_of(ws);
    if (b == std::string_view::npos) return {};
    const auto e = s.find_last_not_of(ws);
    return s.substr(b, e - b + 1);
}

// Strips a trailing comment: '#' or ';' preceded by whitespace.
std::string_view strip_trailing_comment(std::string_view v) {
    for (std::size_t i = 1; i < v.size(); ++i) {
        if ((v[i] == '#' || v[i] == ';') && (v[i - 1] == ' ' || v[i - 1] == '\t')) {
            return v.substr(0, i);
        }
    }
    return v;
}

template <class T>
std::expected<T, std::string> parse_unsigned(std::string_view text) {
    std::uint64_t v = 0;
    const auto* first = text.data();
    const auto* last = text.data() + text.size();
    auto [ptr, ec] = std::from_chars(first, last, v);
    if (ec != std::errc{} || ptr != last) return std::unexpected(std::string("not a non-negative integer"));
    if (v > std::numeric_limits<T>::max()) return std::unexpected(std::string("value out of range"));
    return static_cast<T>(v);
}

}  // namespace

const std::string* IniFile::find(const std::string& section, const std::string& key) const {
    auto s = sections.find(section);
    if (s == sections.end()) return nullptr;
    auto k = s->second.find(key);
    if (k == s->second.end()) return nullptr;
    return &k->second;
}

std::expected<IniFile, std::string> parse_ini(const std::string& text) {
    IniFile ini;
    std::string section;
    std::istringstream in(text);
    std::string raw;
    int lineno = 0;
    while (std::getline(in, raw)) {
        ++lineno;
        std::string_view line = trim(raw);
        if (line.empty() || line.front() == '#' || line.front() == ';') continue;
        if (line.front() == '[') {
            line = trim(strip_trailing_comment(line));
            if (line.size() < 2 || line.back() != ']') {
                return std::unexpected(std::format("line {}: malformed section header", lineno));
            }
            auto name = trim(line.substr(1, line.size() - 2));
            if (name.empty() || name.find_first_of("[]") != std::string_view::npos) {
                return std::unexpected(std::format("line {}: malformed section header", lineno));
            }
            section = std::string(name);
            ini.sections[section];  // a header alone still creates the section
            continue;
        }
        const auto eq = line.find('=');
        if (eq == std::string_view::npos) {
            return std::unexpected(std::format("line {}: expected 'key = value' or '[section]'", lineno));
        }
        auto key = trim(line.substr(0, eq));
        if (key.empty()) return std::unexpected(std::format("line {}: empty key", lineno));
        auto value = trim(strip_trailing_comment(line.substr(eq + 1)));
        ini.sections[section][std::string(key)] = std::string(value);
    }
    return ini;
}

std::expected<Config, std::string> config_from_ini(const IniFile& ini) {
    Config c;
    using Setter = std::function<std::expected<void, std::string>(const std::string&)>;

    auto str = [](std::string& dst) -> Setter {
        return [&dst](const std::string& v) -> std::expected<void, std::string> { dst = v; return {}; };
    };
    auto path = [](std::filesystem::path& dst) -> Setter {
        return [&dst](const std::string& v) -> std::expected<void, std::string> { dst = v; return {}; };
    };
    auto num = []<class T>(T& dst) -> Setter {
        return [&dst](const std::string& v) -> std::expected<void, std::string> {
            auto r = parse_unsigned<T>(v);
            if (!r) return std::unexpected(r.error());
            dst = *r;
            return {};
        };
    };

    const std::map<std::string, std::map<std::string, Setter>> schema = {
        {"control", {{"address", str(c.control.address)},
                     {"port", num(c.control.port)},
                     {"scheme", str(c.control.scheme)},
                     {"remote_jobs", str(c.control.remote_jobs)},
                     {"lease_poll", num(c.control.lease_poll)},
                     {"lease_poll_idle", num(c.control.lease_poll_idle)}}},
        {"node", {{"name", str(c.node.name)}, {"advertise_address", str(c.node.advertise_address)}}},
        {"network", {{"listen_address", str(c.network.listen_address)},
                     {"listen_port", num(c.network.listen_port)},
                     {"tailscale", str(c.network.tailscale)},
                     {"peer_ping_interval", num(c.network.peer_ping_interval)},
                     {"peer_ping_timeout", num(c.network.peer_ping_timeout)},
                     {"max_transfer_mbps", num(c.network.max_transfer_mbps)}}},
        {"artifacts", {{"gc_high", num(c.artifacts.gc_high)},
                       {"gc_low", num(c.artifacts.gc_low)},
                       {"gc_interval", num(c.artifacts.gc_interval)},
                       {"gc_min_age", num(c.artifacts.gc_min_age)},
                       {"checkpoint_replicas", num(c.artifacts.checkpoint_replicas)}}},
        {"resources", {{"max_cpu_cores", num(c.resources.max_cpu_cores)},
                       {"max_memory_mb", num(c.resources.max_memory_mb)},
                       {"max_jobs", num(c.resources.max_jobs)},
                       {"max_disk_mb", num(c.resources.max_disk_mb)},
                       {"default_job_memory_mb", num(c.resources.default_job_memory_mb)},
                       {"cgroups", str(c.resources.cgroups)},
                       {"job_user", str(c.resources.job_user)}}},
        {"tls", {{"ca_file", path(c.tls.ca_file)},
                 {"cert_file", path(c.tls.cert_file)},
                 {"key_file", path(c.tls.key_file)}}},
        {"paths", {{"state_dir", path(c.paths.state_dir)}, {"log_file", path(c.paths.log_file)}}},
        {"log", {{"level", str(c.log_level)}}},
    };

    for (const auto& [sec, keys] : ini.sections) {
        auto s = schema.find(sec);
        if (s == schema.end()) {
            if (sec.empty() && keys.empty()) continue;
            if (sec.empty()) return std::unexpected(std::format("key '{}' outside any section", keys.begin()->first));
            return std::unexpected(std::format("unknown section [{}]", sec));
        }
        for (const auto& [key, value] : keys) {
            auto k = s->second.find(key);
            if (k == s->second.end()) return std::unexpected(std::format("unknown key '{}' in [{}]", key, sec));
            if (value.empty()) continue;  // empty = keep default
            if (auto r = k->second(value); !r) {
                return std::unexpected(std::format("[{}] {} = '{}': {}", sec, key, value, r.error()));
            }
        }
    }
    return c;
}

std::expected<Config, std::string> load_config(const std::filesystem::path& path) {
    std::ifstream f(path, std::ios::binary);
    if (!f) return std::unexpected(std::format("cannot open config file {}", path.string()));
    std::ostringstream ss;
    ss << f.rdbuf();
    if (f.bad()) return std::unexpected(std::format("error reading config file {}", path.string()));
    auto ini = parse_ini(ss.str());
    if (!ini) return std::unexpected(std::format("{}: {}", path.string(), ini.error()));
    auto cfg = config_from_ini(*ini);
    if (!cfg) return std::unexpected(std::format("{}: {}", path.string(), cfg.error()));
    return cfg;
}

}  // namespace tdr
