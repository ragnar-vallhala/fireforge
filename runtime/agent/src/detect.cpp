// Resource / address detection and capacity arithmetic (phase2.md §7.1, §4 policy).
#include <arpa/inet.h>
#include <ifaddrs.h>
#include <net/if.h>
#include <netinet/in.h>
#include <sys/statvfs.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cstring>
#include <fstream>
#include <set>
#include <sstream>
#include <tuple>

#include "tdr/agent/agent.hpp"

namespace tdr::agent {

namespace {

bool is_tailscale_v4(std::uint32_t host_order) { return (host_order & 0xFFC00000u) == 0x64400000u; }  // 100.64/10

bool is_tailscale_v6(const std::uint8_t* b) {  // fd7a:115c:a1e0::/48
    static constexpr std::array<std::uint8_t, 6> kPrefix{0xfd, 0x7a, 0x11, 0x5c, 0xa1, 0xe0};
    return std::equal(kPrefix.begin(), kPrefix.end(), b);
}

// Link-local (fe80::/10), multicast, unspecified, loopback and v4-mapped addresses
// are not useful for peers on other hosts.
bool skip_v6(const std::uint8_t* b) {
    if (b[0] == 0xfe && (b[1] & 0xc0) == 0x80) return true;
    if (b[0] == 0xff) return true;
    static constexpr std::array<std::uint8_t, 16> kZero{};
    if (std::equal(kZero.begin(), kZero.begin() + 15, b) && (b[15] == 0 || b[15] == 1)) return true;
    static constexpr std::array<std::uint8_t, 12> kMapped{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff};
    if (std::equal(kMapped.begin(), kMapped.end(), b)) return true;
    return false;
}

}  // namespace

// Address types:
//   - an address in 100.64.0.0/10 or fd7a:115c:a1e0::/48, or any address on an
//     interface whose name starts with "tailscale", is a Tailscale address;
//   - tailscale_mode "auto"/"on": reported with type "tailscale";
//   - tailscale_mode "off": Tailscale addresses are skipped entirely, so peers
//     never try to reach this node over a tailnet that is meant to be unused;
//   - everything else is "lan".
// Order is deterministic: tailscale first, then lan; IPv4 before IPv6; then by address.
std::vector<cv1::NodeAddress> detect_addresses(std::uint16_t port, const std::string& tailscale_mode,
                                               const std::string& advertise) {
    std::vector<cv1::NodeAddress> out;
    if (!advertise.empty() && advertise != "auto") {
        cv1::NodeAddress a;
        a.set_type("lan");
        a.set_address(advertise);
        a.set_port(port);
        out.push_back(std::move(a));
        return out;
    }
    const bool ts_enabled = tailscale_mode != "off";

    // (type rank, family rank, address)
    std::set<std::tuple<int, int, std::string>> found;
    ifaddrs* ifs = nullptr;
    if (getifaddrs(&ifs) != 0) return out;
    for (ifaddrs* i = ifs; i != nullptr; i = i->ifa_next) {
        if (i->ifa_addr == nullptr || (i->ifa_flags & IFF_UP) == 0 || (i->ifa_flags & IFF_LOOPBACK) != 0) continue;
        const bool ts_iface = i->ifa_name != nullptr && std::strncmp(i->ifa_name, "tailscale", 9) == 0;
        char buf[INET6_ADDRSTRLEN] = {};
        bool ts = ts_iface;
        int family_rank = 0;
        if (i->ifa_addr->sa_family == AF_INET) {
            sockaddr_in sin{};
            std::memcpy(&sin, i->ifa_addr, sizeof sin);
            ts = ts || is_tailscale_v4(ntohl(sin.sin_addr.s_addr));
            if (inet_ntop(AF_INET, &sin.sin_addr, buf, sizeof buf) == nullptr) continue;
        } else if (i->ifa_addr->sa_family == AF_INET6) {
            sockaddr_in6 sin6{};
            std::memcpy(&sin6, i->ifa_addr, sizeof sin6);
            const auto* b = sin6.sin6_addr.s6_addr;
            if (skip_v6(b)) continue;
            ts = ts || is_tailscale_v6(b);
            if (inet_ntop(AF_INET6, &sin6.sin6_addr, buf, sizeof buf) == nullptr) continue;
            family_rank = 1;
        } else {
            continue;
        }
        if (ts && !ts_enabled) continue;
        found.emplace(ts ? 0 : 1, family_rank, buf);
    }
    freeifaddrs(ifs);
    for (const auto& [rank, fam, addr] : found) {
        cv1::NodeAddress a;
        a.set_type(rank == 0 ? "tailscale" : "lan");
        a.set_address(addr);
        a.set_port(port);
        out.push_back(std::move(a));
    }
    return out;
}

cv1::NodeResources detect_physical(const fs::path& state_dir) {
    cv1::NodeResources r;
    long cpus = sysconf(_SC_NPROCESSORS_ONLN);
    r.set_cpu_cores(cpus > 0 ? static_cast<std::uint32_t>(cpus) : 0);

    std::ifstream meminfo("/proc/meminfo");
    std::string line;
    while (std::getline(meminfo, line)) {
        if (line.starts_with("MemTotal:")) {
            std::istringstream is(line.substr(9));
            std::uint64_t kb = 0;
            is >> kb;
            r.set_memory_bytes(kb * 1024);
            break;
        }
    }

    struct statvfs vfs{};
    if (statvfs(state_dir.c_str(), &vfs) == 0) {
        r.set_disk_bytes(static_cast<std::uint64_t>(vfs.f_blocks) * vfs.f_frsize);
    }

    std::uint32_t gpus = 0;
    std::error_code ec;
    for (const auto& e : fs::directory_iterator("/dev", ec)) {
        const std::string name = e.path().filename().string();
        if (name.size() > 6 && name.starts_with("nvidia") &&
            std::all_of(name.begin() + 6, name.end(), [](char c) { return c >= '0' && c <= '9'; })) {
            ++gpus;
        }
    }
    r.set_gpu_count(gpus);
    return r;
}

double CpuSampler::sample() {
    std::ifstream stat("/proc/stat");
    std::string label;
    stat >> label;
    if (label != "cpu") return 0.0;
    // user nice system idle iowait irq softirq steal (guest* are already in user/nice)
    std::array<std::uint64_t, 8> v{};
    for (auto& x : v) {
        if (!(stat >> x)) x = 0;
    }
    std::uint64_t total = 0;
    for (auto x : v) total += x;
    const std::uint64_t idle = v[3] + v[4];

    const bool valid = last_total_ != 0 && total > last_total_ && idle >= last_idle_;
    const std::uint64_t dt = total - last_total_;
    const std::uint64_t di = idle - last_idle_;
    last_total_ = total;
    last_idle_ = idle;
    if (!valid || di > dt) return 0.0;
    return std::clamp(1.0 - static_cast<double>(di) / static_cast<double>(dt), 0.0, 1.0);
}

jobs::Capacity effective_capacity(const jobs::Capacity& local, const cv1::ResourcePolicy& policy) {
    auto cap = [](auto l, auto p) { return p == 0 ? l : std::min<decltype(l)>(l, static_cast<decltype(l)>(p)); };
    jobs::Capacity c;
    c.cpu_cores = cap(local.cpu_cores, policy.max_cpu_cores());
    c.memory_bytes = cap(local.memory_bytes, policy.max_memory_bytes());
    c.jobs = cap(local.jobs, policy.max_jobs());
    return c;
}

}  // namespace tdr::agent
