// /proc and cgroup v2 file helpers.
#include "internal.hpp"

#include <cerrno>
#include <charconv>
#include <cstring>
#include <format>
#include <set>
#include <sstream>
#include <vector>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace tdr::supervisor {

namespace detail {

std::optional<std::string> read_file(const fs::path& p) {
    int fd = ::open(p.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd < 0) return std::nullopt;
    std::string out;
    char buf[4096];
    for (;;) {
        ssize_t n = ::read(fd, buf, sizeof buf);
        if (n < 0) {
            if (errno == EINTR) continue;
            ::close(fd);
            return std::nullopt;
        }
        if (n == 0) break;
        out.append(buf, static_cast<std::size_t>(n));
    }
    ::close(fd);
    return out;
}

std::string write_file(const fs::path& p, std::string_view data) {
    int fd = ::open(p.c_str(), O_WRONLY | O_CLOEXEC);
    if (fd < 0) return std::format("{}: {}", p.string(), std::strerror(errno));
    ssize_t n;
    do {
        n = ::write(fd, data.data(), data.size());
    } while (n < 0 && errno == EINTR);
    int err = errno;
    ::close(fd);
    if (n < 0) return std::format("{}: {}", p.string(), std::strerror(err));
    if (static_cast<std::size_t>(n) != data.size()) return std::format("{}: short write", p.string());
    return {};
}

std::optional<std::uint64_t> read_keyed(const fs::path& file, std::string_view key) {
    auto content = read_file(file);
    if (!content) return std::nullopt;
    std::istringstream in(*content);
    std::string k;
    std::uint64_t v = 0;
    while (in >> k >> v) {
        if (k == key) return v;
    }
    return std::nullopt;
}

bool cgroup_populated(const fs::path& cgroup) {
    auto v = read_keyed(cgroup / "cgroup.events", "populated");
    return v && *v != 0;
}

std::optional<std::uint64_t> parse_start_ticks(std::string_view stat) {
    // "pid (comm) state ppid ..." — comm may contain spaces and parens, so
    // split after the LAST ')'. Field 3 (state) is the first token after it.
    auto rp = stat.rfind(')');
    if (rp == std::string_view::npos) return std::nullopt;
    std::string_view rest = stat.substr(rp + 1);
    int field = 2;
    std::size_t i = 0;
    while (i < rest.size()) {
        while (i < rest.size() && rest[i] == ' ') ++i;
        if (i >= rest.size()) break;
        std::size_t j = i;
        while (j < rest.size() && rest[j] != ' ' && rest[j] != '\n') ++j;
        ++field;
        if (field == 22) {
            std::uint64_t v = 0;
            auto [ptr, ec] = std::from_chars(rest.data() + i, rest.data() + j, v);
            if (ec != std::errc{} || ptr != rest.data() + j) return std::nullopt;
            return v;
        }
        i = j;
        if (i < rest.size() && rest[i] == '\n') break;
    }
    return std::nullopt;
}

namespace {

std::set<std::string> words(std::string_view s) {
    std::set<std::string> out;
    std::istringstream in{std::string(s)};
    std::string w;
    while (in >> w) out.insert(w);
    return out;
}

constexpr const char* kNeeded[] = {"memory", "cpu", "pids"};

std::string enable_controllers(const fs::path& dir) {
    auto have = words(read_file(dir / "cgroup.subtree_control").value_or(""));
    std::string req;
    for (const char* c : kNeeded) {
        if (!have.contains(c)) {
            if (!req.empty()) req += ' ';
            req += '+';
            req += c;
        }
    }
    if (req.empty()) return {};
    return write_file(dir / "cgroup.subtree_control", req);
}

std::string make_dir(const fs::path& p) {
    if (::mkdir(p.c_str(), 0755) < 0 && errno != EEXIST)
        return std::format("mkdir {}: {}", p.string(), std::strerror(errno));
    return {};
}

}  // namespace

std::expected<fs::path, std::string> setup_cgroup_root(const fs::path& requested) {
    fs::path root = requested;
    if (root.empty()) {
        auto self = read_file("/proc/self/cgroup");
        if (!self) return std::unexpected("cannot read /proc/self/cgroup");
        std::string rel;
        bool found = false;
        std::istringstream in(*self);
        for (std::string line; std::getline(in, line);) {
            if (line.starts_with("0::")) {
                rel = line.substr(3);
                found = true;
                break;
            }
        }
        if (!found) return std::unexpected("no cgroup v2 entry in /proc/self/cgroup");
        fs::path relp(rel);
        if (relp.filename() == "daemon") relp = relp.parent_path();
        root = fs::path("/sys/fs/cgroup") / relp.relative_path();
    }

    auto controllers = read_file(root / "cgroup.controllers");
    if (!controllers) return std::unexpected(std::format("{} is not a cgroup v2 directory", root.string()));
    auto avail = words(*controllers);
    std::string missing;
    for (const char* c : kNeeded) {
        if (!avail.contains(c)) missing += std::format("{}{}", missing.empty() ? "" : " ", c);
    }
    if (!missing.empty())
    {
        std::string have = *controllers;
        while (!have.empty() && (have.back() == '\n' || have.back() == ' ')) have.pop_back();
        return std::unexpected(
            std::format("{}: controllers not delegated: {} (have: {})", root.string(), missing, have));
    }
    if (::access(root.c_str(), W_OK) != 0 || ::access((root / "cgroup.procs").c_str(), W_OK) != 0 ||
        ::access((root / "cgroup.subtree_control").c_str(), W_OK) != 0)
        return std::unexpected(std::format("{}: not writable (not delegated?)", root.string()));

    // No-internal-processes rule: move whoever lives in <root> (normally just
    // this daemon) into <root>/daemon before enabling controllers.
    auto procs = read_file(root / "cgroup.procs").value_or("");
    if (!words(procs).empty()) {
        if (auto e = make_dir(root / "daemon"); !e.empty()) return std::unexpected(e);
        std::istringstream in(procs);
        for (std::string pid; in >> pid;) {
            auto e = write_file(root / "daemon" / "cgroup.procs", pid);
            if (!e.empty() && !e.ends_with("No such process"))
                return std::unexpected("moving into daemon cgroup: " + e);
        }
    }
    if (auto e = enable_controllers(root); !e.empty()) return std::unexpected(e);
    fs::path jobs = root / "jobs";
    if (auto e = make_dir(jobs); !e.empty()) return std::unexpected(e);
    if (auto e = enable_controllers(jobs); !e.empty()) return std::unexpected(e);
    return jobs;
}

}  // namespace detail

std::optional<std::uint64_t> read_start_ticks(pid_t pid) {
    if (pid <= 0) return std::nullopt;
    auto stat = detail::read_file(std::format("/proc/{}/stat", pid));
    if (!stat) return std::nullopt;
    return detail::parse_start_ticks(*stat);
}

}  // namespace tdr::supervisor
