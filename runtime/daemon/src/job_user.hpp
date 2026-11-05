// Restricted job user setup for tdrd (docs/design/phase1.md §11).
#pragma once

#include <expected>
#include <filesystem>
#include <string>
#include <sys/types.h>

namespace tdrd {

struct JobUser {
    std::string name;
    uid_t uid = 0;
    gid_t gid = 0;  // the user's primary group
};

// Looks up `name` and validates it: must exist, not be root, and differ from
// the daemon's own user. The daemon must hold CAP_SETUID, CAP_SETGID and
// CAP_KILL (or run as euid 0).
std::expected<JobUser, std::string> resolve_job_user(const std::string& name);

// Adds the job group to this process's supplementary groups (needs CAP_SETGID),
// so the daemon can read job outputs and clean up group-writable dirs.
std::expected<void, std::string> join_group(gid_t gid);

// Every ancestor of `path` (and `path` itself) must be traversable by the job
// user: o+x, or g+x with a matching group, or u+x if owned by the job user.
std::expected<void, std::string> check_traversable(const std::filesystem::path& path, const JobUser& user);

// chmod, with an error message naming the path.
std::expected<void, std::string> set_mode(const std::filesystem::path& path, mode_t mode);
// chown to (unchanged owner, gid) and chmod.
std::expected<void, std::string> set_group_mode(const std::filesystem::path& path, gid_t gid, mode_t mode);

}  // namespace tdrd
