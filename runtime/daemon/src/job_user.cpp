#include "job_user.hpp"

#include <cerrno>
#include <cstring>
#include <format>
#include <fstream>
#include <string>
#include <vector>

#include <grp.h>
#include <pwd.h>
#include <sys/stat.h>
#include <unistd.h>

namespace tdrd {
namespace {

constexpr int kCapKill = 5;
constexpr int kCapSetgid = 6;
constexpr int kCapSetuid = 7;

std::string errno_msg() { return std::strerror(errno); }

// Effective capability mask from /proc/self/status ("CapEff:\t000001ffffffffff").
std::uint64_t effective_caps() {
    std::ifstream in("/proc/self/status");
    std::string line;
    while (std::getline(in, line)) {
        if (line.rfind("CapEff:", 0) == 0) return std::stoull(line.substr(7), nullptr, 16);
    }
    return 0;
}

}  // namespace

std::expected<JobUser, std::string> resolve_job_user(const std::string& name) {
    errno = 0;
    const passwd* pw = getpwnam(name.c_str());
    if (!pw) {
        return std::unexpected(std::format("job_user '{}' does not exist{} (create it: sudo tools/setup-job-user.sh)", name,
                                           errno ? ": " + errno_msg() : ""));
    }
    JobUser u{name, pw->pw_uid, pw->pw_gid};
    if (u.uid == 0) return std::unexpected("job_user must not be root");
    if (u.uid == geteuid()) return std::unexpected(std::format("job_user '{}' is the daemon's own user; use a separate user", name));
    if (u.gid == 0) return std::unexpected("job_user's primary group must not be root");

    if (geteuid() != 0) {
        auto caps = effective_caps();
        std::string missing;
        for (auto [bit, cap] : {std::pair{kCapSetuid, "CAP_SETUID"}, {kCapSetgid, "CAP_SETGID"}, {kCapKill, "CAP_KILL"}}) {
            if (!(caps & (1ull << bit))) missing += std::string(missing.empty() ? "" : ", ") + cap;
        }
        if (!missing.empty()) {
            return std::unexpected(std::format(
                "job_user is set but tdrd lacks {} (systemd: AmbientCapabilities=CAP_SETUID CAP_SETGID CAP_KILL; "
                "dev: sudo tools/setup-job-user.sh, then run build/caps/tdrd)",
                missing));
        }
    }
    return u;
}

std::expected<void, std::string> join_group(gid_t gid) {
    int n = getgroups(0, nullptr);
    if (n < 0) return std::unexpected("getgroups: " + errno_msg());
    std::vector<gid_t> groups(static_cast<std::size_t>(n));
    if (n > 0 && getgroups(n, groups.data()) < 0) return std::unexpected("getgroups: " + errno_msg());
    for (gid_t g : groups)
        if (g == gid) return {};
    groups.push_back(gid);
    if (setgroups(groups.size(), groups.data()) < 0)
        return std::unexpected(std::format("setgroups (add gid {}): {}", gid, errno_msg()));
    return {};
}

std::expected<void, std::string> check_traversable(const std::filesystem::path& path, const JobUser& user) {
    auto p = std::filesystem::absolute(path);
    for (;;) {
        struct stat st {};
        if (stat(p.c_str(), &st) < 0) return std::unexpected(std::format("stat {}: {}", p.string(), errno_msg()));
        bool ok = (st.st_mode & S_IXOTH) || (st.st_gid == user.gid && (st.st_mode & S_IXGRP)) ||
                  (st.st_uid == user.uid && (st.st_mode & S_IXUSR));
        if (!ok) {
            return std::unexpected(std::format(
                "job_user '{}' cannot traverse {} (mode {:04o}); put state_dir somewhere the job user can reach "
                "(e.g. /var/lib/fireforge) or chmod o+x it",
                user.name, p.string(), st.st_mode & 07777));
        }
        if (p == p.root_path()) return {};
        p = p.parent_path();
    }
}

std::expected<void, std::string> set_mode(const std::filesystem::path& path, mode_t mode) {
    if (chmod(path.c_str(), mode) < 0) return std::unexpected(std::format("chmod {}: {}", path.string(), errno_msg()));
    return {};
}

std::expected<void, std::string> set_group_mode(const std::filesystem::path& path, gid_t gid, mode_t mode) {
    if (chown(path.c_str(), static_cast<uid_t>(-1), gid) < 0)
        return std::unexpected(std::format("chown {} to gid {}: {}", path.string(), gid, errno_msg()));
    return set_mode(path, mode);
}

}  // namespace tdrd
