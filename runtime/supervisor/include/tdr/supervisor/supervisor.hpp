// Launches and supervises one isolated child process per job attempt.
//
// Linux mechanisms: posix_spawn (or fork+execve), its own process group,
// pidfd_open for exit notification via Boost.Asio, cgroups v2 for CPU /
// memory / pid limits when a delegated cgroup is available, rlimits otherwise.
// stdout/stderr go straight to files (no pipes), so output survives a daemon crash.
//
// Threading: all methods and callbacks run on the io_context thread.
#pragma once

#include <chrono>
#include <cstdint>
#include <expected>
#include <filesystem>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>
#include <sys/types.h>

#include <boost/asio/io_context.hpp>

namespace tdr::supervisor {

namespace fs = std::filesystem;

struct ResourceLimits {
    std::uint32_t cpu_cores = 1;        // cgroup cpu.max = cores * period
    std::uint64_t memory_bytes = 0;     // cgroup memory.max (+ memory.swap.max=0); 0 = none
    std::uint64_t max_pids = 4096;      // cgroup pids.max
};

struct LaunchSpec {
    std::string attempt_id;                 // unique key for cancel() and the cgroup name
    std::vector<std::string> argv;          // argv[0] looked up in PATH
    std::map<std::string, std::string> env; // full child env = minimal base (PATH, HOME, LANG) + this
    fs::path working_dir;                   // must exist
    fs::path stdout_path;                   // created/truncated, 0640
    fs::path stderr_path;
    ResourceLimits limits;
    std::chrono::seconds timeout{0};        // 0 = none. On expiry: SIGTERM, grace, SIGKILL
};

enum class ExitKind {
    kExited,        // normal exit, see exit_code
    kSignaled,      // killed by a signal not sent by us
    kTimedOut,      // we killed it after LaunchSpec::timeout
    kCancelled,     // we killed it because cancel() was called
    kOomKilled,     // cgroup memory.events oom_kill increased
};

struct ExitInfo {
    ExitKind kind = ExitKind::kExited;
    int exit_code = 0;
    int signal = 0;
    std::chrono::milliseconds wall_time{0};
    std::uint64_t peak_memory_bytes = 0;    // cgroup memory.peak, or ru_maxrss fallback
    std::string message;                    // human-readable summary
};

struct ProcessHandle {
    pid_t pid = 0;
    std::uint64_t start_ticks = 0;          // /proc/<pid>/stat field 22
    std::string cgroup_path;                // "" when cgroups are not in use
};

using ExitCallback = std::function<void(const ExitInfo&)>;

class IProcessSupervisor {
public:
    virtual ~IProcessSupervisor() = default;

    // Spawns the child. On success, `on_exit` is called exactly once, later,
    // on the io thread, after the child (and its whole process group /
    // cgroup) is gone. On failure nothing is called and the error describes
    // why (e.g. "execvp: No such file or directory").
    virtual std::expected<ProcessHandle, std::string> launch(const LaunchSpec& spec, ExitCallback on_exit) = 0;

    // SIGTERM the process group, SIGKILL after `grace`. ExitInfo.kind = kCancelled.
    // No-op if the attempt is unknown or already exited.
    virtual void cancel(const std::string& attempt_id, std::chrono::seconds grace) = 0;

    // Crash recovery: kill a process left over from a previous daemon run.
    // Only acts if /proc/<pid>/stat start time still matches (no PID reuse),
    // then kills the group / cgroup and removes the cgroup. True if it was
    // found alive and killed.
    virtual bool kill_orphan(const ProcessHandle& handle) = 0;

    virtual std::size_t running_count() const = 0;
    virtual bool cgroups_enabled() const = 0;
};

struct SupervisorOptions {
    enum class Cgroups { kAuto, kRequired, kOff };
    Cgroups cgroups = Cgroups::kAuto;
    // Delegated cgroup to create per-attempt children in. Empty = detect the
    // daemon's own cgroup from /proc/self/cgroup. With kAuto, falls back to
    // rlimits (and logs a warning) if it is not writable; kRequired throws.
    fs::path cgroup_root;
    std::chrono::seconds kill_grace{10};     // used for timeouts
};

std::unique_ptr<IProcessSupervisor> make_linux_supervisor(boost::asio::io_context& io,
                                                          const SupervisorOptions& options);

const char* to_string(ExitKind k);

}  // namespace tdr::supervisor
