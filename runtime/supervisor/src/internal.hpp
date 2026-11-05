// Internal helpers of the Linux supervisor (not part of the public contract).
// Tests include this via "../src/internal.hpp".
#pragma once

#include <cstdint>
#include <expected>
#include <filesystem>
#include <optional>
#include <string>
#include <string_view>
#include <sys/types.h>

namespace tdr::supervisor {

namespace fs = std::filesystem;

// /proc/<pid>/stat field 22 (starttime, clock ticks since boot). nullopt if
// the process does not exist or the file cannot be parsed.
std::optional<std::uint64_t> read_start_ticks(pid_t pid);

namespace detail {

// Whole-file read; nullopt on any error.
std::optional<std::string> read_file(const fs::path& p);

// Writes `data` with a single write(2) (required for cgroup control files).
// Returns "" on success, else "<path>: <strerror>".
std::string write_file(const fs::path& p, std::string_view data);

// Value of "key N" in a flat-keyed file such as memory.events / cgroup.events.
std::optional<std::uint64_t> read_keyed(const fs::path& file, std::string_view key);

// cgroup.events "populated 1". A missing cgroup counts as not populated.
bool cgroup_populated(const fs::path& cgroup);

// Parses the content of /proc/<pid>/stat and returns field 22.
std::optional<std::uint64_t> parse_start_ticks(std::string_view stat);

// Prepares a delegated cgroup v2 subtree for job cgroups:
//   - `requested` empty: detect the own cgroup from /proc/self/cgroup
//     ("0::/path" -> /sys/fs/cgroup/path; a trailing "/daemon" component is
//     stripped because a previous call already moved us there),
//   - checks memory/cpu/pids are available and the directory is writable,
//   - moves every process in <root> into <root>/daemon (no-internal-processes rule),
//   - enables +memory +cpu +pids in <root> and in <root>/jobs.
// Returns <root>/jobs, or why cgroups are unusable.
std::expected<fs::path, std::string> setup_cgroup_root(const fs::path& requested);

}  // namespace detail
}  // namespace tdr::supervisor
