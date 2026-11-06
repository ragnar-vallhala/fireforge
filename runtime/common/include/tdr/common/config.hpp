// tdr.conf parsing (INI). See deploy/etc/tdr.conf.example for every key.
#pragma once

#include <cstdint>
#include <expected>
#include <filesystem>
#include <map>
#include <string>

namespace tdr {

// Generic INI: "[section]" headers, "key = value" lines, '#' or ';' comments,
// surrounding whitespace trimmed. Keys outside a section go in section "".
// Duplicate keys: last one wins.
struct IniFile {
    std::map<std::string, std::map<std::string, std::string>> sections;
    const std::string* find(const std::string& section, const std::string& key) const;
};

// Error string includes the line number.
std::expected<IniFile, std::string> parse_ini(const std::string& text);

// Sizes accept plain numbers (MB for *_mb keys) per the example file.
struct Config {
    struct Control {
        std::string address;          // empty = standalone (Phase 1)
        std::uint16_t port = 8080;
        std::string scheme = "https";
        std::string remote_jobs = "on";   // Phase 4: "on" | "off" — lease jobs from the control plane
        std::uint32_t lease_poll = 2;     // seconds between lease requests while leases keep coming
        std::uint32_t lease_poll_idle = 10;  // seconds after an empty lease reply
    } control;

    struct Node {
        std::string name;             // default: hostname
        std::string advertise_address = "auto";
    } node;

    struct Network {
        std::string listen_address = "0.0.0.0";
        std::uint16_t listen_port = 7400;
        std::string tailscale = "auto";
        std::uint32_t peer_ping_interval = 5;   // seconds (phase3.md §4)
        std::uint32_t peer_ping_timeout = 15;   // seconds without any frame -> link closed
    } network;

    struct Resources {
        std::uint32_t max_cpu_cores = 0;     // 0 = all online CPUs
        std::uint64_t max_memory_mb = 0;     // 0 = 80% of physical RAM
        std::uint32_t max_jobs = 0;          // 0 = max_cpu_cores
        std::uint64_t max_disk_mb = 0;       // 0 = unlimited
        std::uint64_t default_job_memory_mb = 1024;  // when a job requests 0
        std::string cgroups = "auto";        // "auto" | "required" | "off"
        std::string job_user;                // run jobs as this user; "" = daemon's user (phase1.md §11)
    } resources;

    // mTLS to the control plane (phase2.md §2, §7). Required when control.address is set.
    struct Tls {
        std::filesystem::path ca_file = "/etc/fireforge/certificates/ca.crt";
        std::filesystem::path cert_file = "/etc/fireforge/certificates/node.crt";
        std::filesystem::path key_file = "/etc/fireforge/node.key";
    } tls;

    struct Paths {
        std::filesystem::path state_dir = "/var/lib/fireforge";  // journal, artifacts, work, run
        std::filesystem::path log_file;      // empty = stderr only
    } paths;

    std::string log_level = "info";

    // Derived locations (all under state_dir).
    std::filesystem::path journal_path() const { return paths.state_dir / "state" / "journal.db"; }
    std::filesystem::path artifacts_dir() const { return paths.state_dir / "artifacts"; }
    std::filesystem::path work_dir() const { return paths.state_dir / "work"; }
    std::filesystem::path run_dir() const { return paths.state_dir / "run"; }
    std::filesystem::path ctl_socket() const { return run_dir() / "ctl.sock"; }
    std::filesystem::path worker_socket() const { return run_dir() / "worker.sock"; }
    std::filesystem::path policy_cache() const { return paths.state_dir / "state" / "policy.json"; }
    std::filesystem::path remote_state() const { return paths.state_dir / "state" / "remote.json"; }
};

// Unknown sections/keys are an error (catches typos). Missing keys keep defaults.
std::expected<Config, std::string> config_from_ini(const IniFile& ini);
std::expected<Config, std::string> load_config(const std::filesystem::path& path);

}  // namespace tdr
