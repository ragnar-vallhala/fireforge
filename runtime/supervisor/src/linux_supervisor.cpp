// Linux process supervisor: fork+execve, own process group, pidfd exit
// notification through Boost.Asio, cgroups v2 or rlimits for limits.
#include "internal.hpp"

#include <tdr/common/common.hpp>
#include <tdr/supervisor/supervisor.hpp>

#include <cerrno>
#include <csignal>
#include <cstring>
#include <format>
#include <map>
#include <sstream>
#include <stdexcept>
#include <thread>

#include <boost/asio/posix/stream_descriptor.hpp>
#include <boost/asio/steady_timer.hpp>

#include <fcntl.h>
#include <grp.h>
#include <linux/capability.h>
#include <linux/close_range.h>
#include <sys/prctl.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/wait.h>
#include <unistd.h>

namespace tdr::supervisor {

const char* to_string(ExitKind k) {
    switch (k) {
        case ExitKind::kExited: return "exited";
        case ExitKind::kSignaled: return "signaled";
        case ExitKind::kTimedOut: return "timed_out";
        case ExitKind::kCancelled: return "cancelled";
        case ExitKind::kOomKilled: return "oom_killed";
    }
    return "unknown";
}

namespace {

namespace asio = boost::asio;
using Clock = std::chrono::steady_clock;

std::string errno_msg(const char* what, int err) { return std::format("{}: {}", what, std::strerror(err)); }

std::string signal_name(int sig) {
    const char* abbrev = sigabbrev_np(sig);
    return abbrev ? std::format("SIG{}", abbrev) : std::format("signal {}", sig);
}

// Owns a file descriptor.
struct Fd {
    int fd = -1;
    Fd() = default;
    explicit Fd(int f) : fd(f) {}
    Fd(const Fd&) = delete;
    Fd& operator=(const Fd&) = delete;
    Fd(Fd&& o) noexcept : fd(std::exchange(o.fd, -1)) {}
    Fd& operator=(Fd&& o) noexcept {
        if (this != &o) {
            reset();
            fd = std::exchange(o.fd, -1);
        }
        return *this;
    }
    ~Fd() { reset(); }
    void reset() {
        if (fd >= 0) ::close(fd);
        fd = -1;
    }
    int release() { return std::exchange(fd, -1); }
};

// Opens with O_CLOEXEC and makes sure the fd is >= 3, so dup2 onto 0/1/2 in
// the child never clobbers another of our fds.
std::expected<Fd, std::string> open_high(const fs::path& p, int flags, mode_t mode) {
    int fd = ::open(p.c_str(), flags | O_CLOEXEC, mode);
    if (fd < 0) return std::unexpected(std::format("open {}: {}", p.string(), std::strerror(errno)));
    if (fd < 3) {
        int hi = ::fcntl(fd, F_DUPFD_CLOEXEC, 3);
        int err = errno;
        ::close(fd);
        if (hi < 0) return std::unexpected(errno_msg("fcntl", err));
        fd = hi;
    }
    return Fd(fd);
}

// argv[0] lookup in the CHILD's PATH, done in the parent before fork.
std::expected<std::string, std::string> resolve_program(const std::string& name, const std::string& path_var) {
    if (name.empty()) return std::unexpected("execvp: No such file or directory");
    if (name.find('/') != std::string::npos) return name;
    bool eacces = false;
    std::size_t start = 0;
    for (;;) {
        std::size_t end = path_var.find(':', start);
        std::string dir = path_var.substr(start, end == std::string::npos ? std::string::npos : end - start);
        if (dir.empty()) dir = ".";
        std::string cand = dir + "/" + name;
        struct stat st{};
        if (::stat(cand.c_str(), &st) == 0 && S_ISREG(st.st_mode)) {
            if (::access(cand.c_str(), X_OK) == 0) return cand;
            eacces = true;
        }
        if (end == std::string::npos) break;
        start = end + 1;
    }
    return std::unexpected(eacces ? "execvp: Permission denied" : "execvp: No such file or directory");
}

// Stages reported by the child through the CLOEXEC error pipe.
enum ChildStage : int {
    kStageSetpgid = 1,
    kStageCgroup,
    kStageDup,
    kStageChdir,
    kStageRlimit,
    kStageExec,
    kStageSetgroups,
    kStageSetgid,
    kStageSetuid,
    kStageVerifyIds,
    kStageDropCaps,
    kStageNoNewPrivs,
};

const char* stage_name(int stage) {
    switch (stage) {
        case kStageSetpgid: return "setpgid";
        case kStageCgroup: return "cgroup.procs";
        case kStageDup: return "dup2";
        case kStageChdir: return "chdir";
        case kStageRlimit: return "setrlimit";
        case kStageExec: return "execvp";
        case kStageSetgroups: return "setgroups";
        case kStageSetgid: return "setgid";
        case kStageSetuid: return "setuid";
        case kStageVerifyIds: return "verify-ids";
        case kStageDropCaps: return "drop-caps";
        case kStageNoNewPrivs: return "no_new_privs";
    }
    return "child setup";
}

// LaunchSpec::run_as, flattened by the parent before fork.
struct ChildIdentity {
    bool active = false;      // run_as set: harden (drop caps, no_new_privs, umask)
    bool switch_ids = false;  // uid/gid differ from ours: setgroups/setresgid/setresuid
    uid_t uid = 0;
    gid_t gid = 0;
    const gid_t* groups = nullptr;
    std::size_t ngroups = 0;
};

// Switches identity and drops privileges. Raw syscalls on purpose: the glibc
// wrappers of set*id broadcast to all threads (setxid machinery), which we
// neither need (the child has one thread) nor want between fork and exec.
// Returns 0 or the failing stage (errno set).
int child_drop_privileges(const ChildIdentity& id) {
    if (id.switch_ids) {
        if (::syscall(SYS_setgroups, id.ngroups, id.groups) < 0) return kStageSetgroups;
        if (::syscall(SYS_setresgid, id.gid, id.gid, id.gid) < 0) return kStageSetgid;
        if (::syscall(SYS_setresuid, id.uid, id.uid, id.uid) < 0) return kStageSetuid;
    }
    if (::getuid() != id.uid || ::geteuid() != id.uid || ::getgid() != id.gid || ::getegid() != id.gid) {
        errno = EPERM;
        return kStageVerifyIds;
    }
    // Ambient first (systemd AmbientCapabilities= would otherwise survive
    // exec), then permitted/effective/inheritable to empty. A non-root daemon
    // keeps its permitted caps across setresuid, so this is not redundant.
    if (::prctl(PR_CAP_AMBIENT, PR_CAP_AMBIENT_CLEAR_ALL, 0, 0, 0) < 0 && errno != EINVAL) return kStageDropCaps;
    struct __user_cap_header_struct hdr{};
    hdr.version = _LINUX_CAPABILITY_VERSION_3;
    hdr.pid = 0;
    struct __user_cap_data_struct data[2]{};
    if (::syscall(SYS_capset, &hdr, data) < 0) return kStageDropCaps;
    if (::prctl(PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) < 0) return kStageNoNewPrivs;
    ::umask(0007);
    return 0;
}

// Child side of fork(): only async-signal-safe calls, everything prepared by
// the parent. Never returns.
[[noreturn]] void child_exec(int err_fd, int in_fd, int out_fd, int errf_fd, const char* cgroup_procs, const char* cwd,
                             std::uint64_t rlimit_as, const ChildIdentity& ident, const char* prog, char* const* argv,
                             char* const* envp) {
    auto fail = [err_fd](int stage) {
        int msg[2] = {stage, errno};
        ssize_t n = ::write(err_fd, msg, sizeof msg);
        (void)n;
        ::_exit(127);
    };
    if (::setpgid(0, 0) < 0) fail(kStageSetpgid);
    if (cgroup_procs) {
        int fd = ::open(cgroup_procs, O_WRONLY | O_CLOEXEC);
        if (fd < 0) fail(kStageCgroup);
        if (::write(fd, "0", 1) != 1) fail(kStageCgroup);
        ::close(fd);
    }
    sigset_t none;
    sigemptyset(&none);
    ::sigprocmask(SIG_SETMASK, &none, nullptr);
    struct sigaction dfl{};
    dfl.sa_handler = SIG_DFL;
    sigemptyset(&dfl.sa_mask);
    for (int s = 1; s < NSIG; ++s) {
        if (s == SIGKILL || s == SIGSTOP) continue;
        ::sigaction(s, &dfl, nullptr);  // EINVAL for glibc-reserved signals is fine
    }
    if (::dup2(in_fd, 0) < 0 || ::dup2(out_fd, 1) < 0 || ::dup2(errf_fd, 2) < 0) fail(kStageDup);
    // After the cgroup join (the job uid cannot write daemon-owned cgroup files).
    if (ident.active) {
        if (int stage = child_drop_privileges(ident)) fail(stage);
    }
    if (::chdir(cwd) < 0) fail(kStageChdir);
    if (rlimit_as > 0) {
        struct rlimit rl{rlimit_as, rlimit_as};
        if (::setrlimit(RLIMIT_AS, &rl) < 0) fail(kStageRlimit);
    }
    // Everything above fd 2 (including err_fd) closes at exec.
    if (::close_range(3, ~0U, CLOSE_RANGE_CLOEXEC) < 0) {
        struct rlimit nofile{};
        ::getrlimit(RLIMIT_NOFILE, &nofile);
        rlim_t maxfd = nofile.rlim_cur == RLIM_INFINITY || nofile.rlim_cur > 1048576 ? 1048576 : nofile.rlim_cur;
        for (int fd = 3; static_cast<rlim_t>(fd) < maxfd; ++fd) {
            int fl = ::fcntl(fd, F_GETFD);
            if (fl >= 0 && !(fl & FD_CLOEXEC)) ::fcntl(fd, F_SETFD, fl | FD_CLOEXEC);
        }
    }
    ::execve(prog, argv, envp);
    fail(kStageExec);
    ::_exit(127);
}

class LinuxSupervisor final : public IProcessSupervisor {
public:
    LinuxSupervisor(asio::io_context& io, const SupervisorOptions& options) : io_(io), opts_(options) {
        if (opts_.cgroups == SupervisorOptions::Cgroups::kOff) return;
        auto jobs = detail::setup_cgroup_root(opts_.cgroup_root);
        if (jobs) {
            jobs_dir_ = *jobs;
            log_info("supervisor: cgroups v2 enabled, job cgroups under {}", jobs_dir_.string());
            return;
        }
        if (opts_.cgroups == SupervisorOptions::Cgroups::kRequired)
            throw std::runtime_error("cgroups required but unavailable: " + jobs.error());
        log_warn("supervisor: cgroups v2 unavailable ({}); falling back to rlimits and process groups", jobs.error());
    }

    ~LinuxSupervisor() override {
        if (!attempts_.empty())
            log_warn("supervisor: destroyed with {} running attempt(s); leaving them running", attempts_.size());
        for (auto& [id, a] : attempts_) {
            a->abandoned = true;
            a->timeout_timer.cancel();
            a->kill_timer.cancel();
            a->drain_timer.cancel();
            boost::system::error_code ec;
            a->sd.close(ec);
        }
    }

    std::expected<ProcessHandle, std::string> launch(const LaunchSpec& spec, ExitCallback on_exit) override;
    void cancel(const std::string& attempt_id, std::chrono::seconds grace) override;
    bool kill_orphan(const ProcessHandle& handle) override;
    std::size_t running_count() const override { return attempts_.size(); }
    bool cgroups_enabled() const override { return !jobs_dir_.empty(); }

private:
    struct Attempt {
        explicit Attempt(asio::io_context& io) : sd(io), timeout_timer(io), kill_timer(io), drain_timer(io) {}
        std::string id;
        pid_t pid = 0;
        fs::path cgroup;  // empty in fallback mode
        ExitCallback cb;
        asio::posix::stream_descriptor sd;  // pidfd
        asio::steady_timer timeout_timer, kill_timer, drain_timer;
        Clock::time_point start;
        std::chrono::seconds timeout{0};
        std::optional<ExitKind> kill_reason;  // set once we decided to kill it
        bool exited = false;
        bool abandoned = false;  // supervisor destroyed: handlers must not touch it
        int drain_rounds = 0;
        ExitInfo info;
    };
    using AttemptPtr = std::shared_ptr<Attempt>;

    void arm_wait(const AttemptPtr& a);
    void on_pidfd_readable(const AttemptPtr& a);
    void begin_kill(const AttemptPtr& a, ExitKind reason, std::chrono::seconds grace);
    void hard_kill(const AttemptPtr& a);
    void drain(const AttemptPtr& a);
    void finish(const AttemptPtr& a);

    std::expected<fs::path, std::string> create_cgroup(const LaunchSpec& spec);
    static void remove_cgroup(const fs::path& cg);

    // killpg / kill that warn (once per supervisor) on EPERM: a child running
    // as another uid can only be signalled with CAP_KILL. cgroup.kill does not
    // need it. Return false only on EPERM.
    bool signal_group(pid_t pgid, int sig);
    bool signal_pid(pid_t pid, int sig);
    void note_kill_eperm(pid_t pid);

    asio::io_context& io_;
    SupervisorOptions opts_;
    fs::path jobs_dir_;
    std::map<std::string, AttemptPtr> attempts_;
    bool eperm_warned_ = false;
};

void LinuxSupervisor::note_kill_eperm(pid_t pid) {
    if (eperm_warned_) return;
    eperm_warned_ = true;
    log_warn("supervisor: not permitted to signal pid {} (job runs as another user and tdrd lacks CAP_KILL?); {}", pid,
             cgroups_enabled() ? "relying on cgroup.kill" : "without cgroups the job may survive cancel/timeout");
}

bool LinuxSupervisor::signal_group(pid_t pgid, int sig) {
    if (::killpg(pgid, sig) == 0 || errno != EPERM) return true;
    note_kill_eperm(pgid);
    return false;
}

bool LinuxSupervisor::signal_pid(pid_t pid, int sig) {
    if (::kill(pid, sig) == 0 || errno != EPERM) return true;
    note_kill_eperm(pid);
    return false;
}

std::expected<fs::path, std::string> LinuxSupervisor::create_cgroup(const LaunchSpec& spec) {
    fs::path cg = jobs_dir_ / spec.attempt_id;
    if (::mkdir(cg.c_str(), 0755) < 0) {
        if (errno != EEXIST || ::rmdir(cg.c_str()) < 0 || ::mkdir(cg.c_str(), 0755) < 0)
            return std::unexpected(std::format("mkdir {}: {}", cg.string(), std::strerror(errno)));
    }
    const auto& l = spec.limits;
    std::string err;
    auto set = [&](const char* file, const std::string& value, bool optional = false) {
        if (!err.empty()) return;
        if (optional && ::access((cg / file).c_str(), F_OK) != 0) return;
        err = detail::write_file(cg / file, value);
    };
    set("memory.max", l.memory_bytes ? std::to_string(l.memory_bytes) : "max");
    set("memory.swap.max", "0", true);
    set("memory.oom.group", "1", true);
    set("cpu.max", l.cpu_cores ? std::format("{} 100000", std::uint64_t{l.cpu_cores} * 100000) : "max 100000");
    set("pids.max", l.max_pids ? std::to_string(l.max_pids) : "max");
    if (!err.empty()) {
        ::rmdir(cg.c_str());
        return std::unexpected("cgroup setup: " + err);
    }
    return cg;
}

void LinuxSupervisor::remove_cgroup(const fs::path& cg) {
    if (cg.empty()) return;
    for (int i = 0; i < 50; ++i) {
        if (::rmdir(cg.c_str()) == 0 || errno == ENOENT) return;
        if (errno != EBUSY) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    log_warn("supervisor: cannot remove cgroup {}: {}", cg.string(), std::strerror(errno));
}

std::expected<ProcessHandle, std::string> LinuxSupervisor::launch(const LaunchSpec& spec, ExitCallback on_exit) {
    if (spec.argv.empty()) return std::unexpected("empty argv");
    if (spec.attempt_id.empty() || spec.attempt_id.find('/') != std::string::npos || spec.attempt_id == "." ||
        spec.attempt_id == "..")
        return std::unexpected("invalid attempt_id");
    if (attempts_.contains(spec.attempt_id)) return std::unexpected("attempt already running: " + spec.attempt_id);

    // Environment: minimal base, then spec.env (wins).
    std::map<std::string, std::string> env;
    const char* path = std::getenv("PATH");
    env["PATH"] = path && *path ? path : "/usr/local/bin:/usr/bin:/bin";
    const char* home = std::getenv("HOME");
    env["HOME"] = home && *home ? home : "/";
    env["LANG"] = "C.UTF-8";
    for (const auto& [k, v] : spec.env) env[k] = v;

    auto prog = resolve_program(spec.argv[0], env["PATH"]);
    if (!prog) return std::unexpected(prog.error());

    std::vector<std::string> env_strs;
    env_strs.reserve(env.size());
    for (const auto& [k, v] : env) env_strs.push_back(k + "=" + v);
    std::vector<char*> envp;
    for (auto& s : env_strs) envp.push_back(s.data());
    envp.push_back(nullptr);
    std::vector<std::string> argv_strs = spec.argv;
    std::vector<char*> argv;
    for (auto& s : argv_strs) argv.push_back(s.data());
    argv.push_back(nullptr);
    std::string cwd = spec.working_dir.empty() ? std::string(".") : spec.working_dir.string();

    // Output files: opened in the parent so errors are reported precisely.
    auto in = open_high("/dev/null", O_RDONLY, 0);
    if (!in) return std::unexpected(in.error());
    auto out = spec.stdout_path.empty() ? open_high("/dev/null", O_WRONLY, 0)
                                        : open_high(spec.stdout_path, O_WRONLY | O_CREAT | O_TRUNC, 0640);
    if (!out) return std::unexpected(out.error());
    std::expected<Fd, std::string> err_file = std::unexpected(std::string());
    if (!spec.stderr_path.empty() && spec.stderr_path == spec.stdout_path) {
        int d = ::fcntl(out->fd, F_DUPFD_CLOEXEC, 3);
        if (d < 0) return std::unexpected(errno_msg("fcntl", errno));
        err_file = Fd(d);
    } else {
        err_file = spec.stderr_path.empty() ? open_high("/dev/null", O_WRONLY, 0)
                                            : open_high(spec.stderr_path, O_WRONLY | O_CREAT | O_TRUNC, 0640);
    }
    if (!err_file) return std::unexpected(err_file.error());

    fs::path cg;
    std::string cg_procs;
    if (cgroups_enabled()) {
        auto c = create_cgroup(spec);
        if (!c) return std::unexpected(c.error());
        cg = *c;
        cg_procs = (cg / "cgroup.procs").string();
    }
    std::uint64_t rlimit_as = cgroups_enabled() ? 0 : spec.limits.memory_bytes;

    ChildIdentity ident;
    std::vector<gid_t> groups;
    if (spec.run_as) {
        ident.active = true;
        ident.uid = spec.run_as->uid;
        ident.gid = spec.run_as->gid;
        ident.switch_ids = ident.uid != ::getuid() || ident.uid != ::geteuid() || ident.gid != ::getgid() ||
                           ident.gid != ::getegid();
        groups = spec.run_as->groups;
        ident.groups = groups.data();
        ident.ngroups = groups.size();
    }

    int pipefd[2];
    if (::pipe2(pipefd, O_CLOEXEC) < 0) {
        int e = errno;
        remove_cgroup(cg);
        return std::unexpected(errno_msg("pipe2", e));
    }
    Fd rd(pipefd[0]), wr(pipefd[1]);

    pid_t pid = ::fork();
    if (pid < 0) {
        int e = errno;
        remove_cgroup(cg);
        return std::unexpected(errno_msg("fork", e));
    }
    if (pid == 0) {
        child_exec(wr.fd, in->fd, out->fd, err_file->fd, cg_procs.empty() ? nullptr : cg_procs.c_str(), cwd.c_str(),
                   rlimit_as, ident, prog->c_str(), argv.data(), envp.data());
    }
    auto start = Clock::now();
    wr.reset();
    // pidfd right away: the child is unreaped, so the pid cannot be reused.
    int pidfd = static_cast<int>(::syscall(SYS_pidfd_open, pid, 0));
    int pidfd_err = errno;

    int msg[2] = {0, 0};
    ssize_t n;
    do {
        n = ::read(rd.fd, msg, sizeof msg);
    } while (n < 0 && errno == EINTR);
    if (n > 0 || pidfd < 0) {
        // Child setup / exec failed (or we cannot watch it): reap and report.
        bool reaped = true;
        if (n <= 0) {
            // The child already exec'd (possibly as another uid): kill via the
            // cgroup if signalling is not permitted, and don't block forever.
            if (!signal_pid(pid, SIGKILL) && !cg.empty()) detail::write_file(cg / "cgroup.kill", "1");
            reaped = false;
            for (int i = 0; i < 1000 && !reaped; ++i) {  // ~5 s
                int status = 0;
                pid_t w = ::waitpid(pid, &status, WNOHANG);
                if (w == pid || (w < 0 && errno != EINTR)) reaped = true;
                else std::this_thread::sleep_for(std::chrono::milliseconds(5));
            }
            if (!reaped) log_warn("supervisor: unwatchable child pid {} could not be killed; leaving it", pid);
        } else {
            int status = 0;
            while (::waitpid(pid, &status, 0) < 0 && errno == EINTR) {
            }
        }
        if (pidfd >= 0) ::close(pidfd);
        if (reaped) remove_cgroup(cg);
        if (n > 0) return std::unexpected(std::format("{}: {}", stage_name(msg[0]), std::strerror(msg[1])));
        return std::unexpected(errno_msg("pidfd_open", pidfd_err));
    }

    auto a = std::make_shared<Attempt>(io_);
    a->id = spec.attempt_id;
    a->pid = pid;
    a->cgroup = cg;
    a->cb = std::move(on_exit);
    a->sd.assign(pidfd);
    a->start = start;
    a->timeout = spec.timeout;
    attempts_.emplace(a->id, a);

    ProcessHandle h;
    h.pid = pid;
    h.start_ticks = read_start_ticks(pid).value_or(0);  // a zombie still has its stat
    h.cgroup_path = cg.string();

    if (spec.timeout.count() > 0) {
        a->timeout_timer.expires_after(spec.timeout);
        a->timeout_timer.async_wait([this, a](const boost::system::error_code& ec) {
            if (ec || a->abandoned) return;
            begin_kill(a, ExitKind::kTimedOut, opts_.kill_grace);
        });
    }
    arm_wait(a);
    log_debug("supervisor: launched {} pid={} cgroup={}", a->id, pid, h.cgroup_path);
    return h;
}

void LinuxSupervisor::arm_wait(const AttemptPtr& a) {
    a->sd.async_wait(asio::posix::stream_descriptor::wait_read, [this, a](const boost::system::error_code& ec) {
        if (a->abandoned || ec == asio::error::operation_aborted) return;
        if (ec) log_warn("supervisor: pidfd wait for {} failed: {}", a->id, ec.message());
        on_pidfd_readable(a);
    });
}

void LinuxSupervisor::on_pidfd_readable(const AttemptPtr& a) {
    siginfo_t si{};
    struct rusage ru{};
    // Raw waitid: the 5th argument returns this child's rusage.
    long r = ::syscall(SYS_waitid, P_PIDFD, a->sd.native_handle(), &si, WEXITED | WNOHANG, &ru);
    if (r < 0 && errno == EINTR) {
        arm_wait(a);
        return;
    }
    bool lost = false;
    if (r < 0) {
        log_warn("supervisor: waitid for {} (pid {}) failed: {}", a->id, a->pid, std::strerror(errno));
        lost = true;
    } else if (si.si_pid == 0) {
        arm_wait(a);  // spurious wakeup
        return;
    }

    a->exited = true;
    a->timeout_timer.cancel();
    a->kill_timer.cancel();
    boost::system::error_code ignored;
    a->sd.close(ignored);

    ExitInfo& info = a->info;
    info.wall_time = std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - a->start);
    bool signaled = false;
    if (lost) {
        info.exit_code = -1;
    } else if (si.si_code == CLD_EXITED) {
        info.exit_code = si.si_status;
    } else {
        signaled = true;
        info.signal = si.si_status;
    }
    bool oom = false;
    std::uint64_t oom_kills = 0;
    if (!a->cgroup.empty()) {
        oom_kills = detail::read_keyed(a->cgroup / "memory.events", "oom_kill").value_or(0);
        oom = oom_kills > 0;
        if (auto s = detail::read_file(a->cgroup / "memory.peak")) info.peak_memory_bytes = std::strtoull(s->c_str(), nullptr, 10);
    }
    if (info.peak_memory_bytes == 0 && ru.ru_maxrss > 0)
        info.peak_memory_bytes = static_cast<std::uint64_t>(ru.ru_maxrss) * 1024;

    std::string how = lost         ? std::string("exit status lost")
                      : signaled   ? std::format("killed by {}", signal_name(info.signal))
                                   : std::format("exited with code {}", info.exit_code);
    if (a->kill_reason == ExitKind::kTimedOut) {
        info.kind = ExitKind::kTimedOut;
        info.message = std::format("timed out after {}s ({})", a->timeout.count(), how);
    } else if (a->kill_reason == ExitKind::kCancelled) {
        info.kind = ExitKind::kCancelled;
        info.message = std::format("cancelled ({})", how);
    } else if (oom) {
        info.kind = ExitKind::kOomKilled;
        info.message = std::format("out of memory: {} oom kill(s) ({})", oom_kills, how);
    } else if (signaled) {
        info.kind = ExitKind::kSignaled;
        info.message = how;
    } else {
        info.kind = ExitKind::kExited;
        info.message = how;
    }

    // Kill whatever is left of the job (grandchildren), then wait until it is gone.
    if (!a->cgroup.empty()) detail::write_file(a->cgroup / "cgroup.kill", "1");
    signal_group(a->pid, SIGKILL);
    drain(a);
}

void LinuxSupervisor::drain(const AttemptPtr& a) {
    bool empty = a->cgroup.empty() ? (::killpg(a->pid, 0) < 0 && errno == ESRCH) : !detail::cgroup_populated(a->cgroup);
    if (!empty && a->drain_rounds < 1000) {  // ~5 s
        if (a->drain_rounds++ % 20 == 19) {  // re-kill now and then (catches late forks)
            if (!a->cgroup.empty()) detail::write_file(a->cgroup / "cgroup.kill", "1");
            signal_group(a->pid, SIGKILL);
        }
        a->drain_timer.expires_after(std::chrono::milliseconds(5));
        a->drain_timer.async_wait([this, a](const boost::system::error_code& ec) {
            if (ec || a->abandoned) return;
            drain(a);
        });
        return;
    }
    if (!empty) log_warn("supervisor: processes of {} still present after kill", a->id);
    finish(a);
}

void LinuxSupervisor::finish(const AttemptPtr& a) {
    remove_cgroup(a->cgroup);
    attempts_.erase(a->id);
    auto cb = std::move(a->cb);
    log_debug("supervisor: {} finished: {} ({})", a->id, to_string(a->info.kind), a->info.message);
    if (cb) cb(a->info);
}

void LinuxSupervisor::begin_kill(const AttemptPtr& a, ExitKind reason, std::chrono::seconds grace) {
    if (a->exited || a->kill_reason) return;
    a->kill_reason = reason;
    a->timeout_timer.cancel();
    bool delivered = signal_group(a->pid, SIGTERM);
    if (!a->cgroup.empty()) {
        // Also reach processes that left the process group.
        auto procs = detail::read_file(a->cgroup / "cgroup.procs").value_or("");
        std::istringstream in(procs);
        for (long p; in >> p;) delivered = signal_pid(static_cast<pid_t>(p), SIGTERM) && delivered;
    }
    // SIGTERM not permitted: a grace period is pointless, go straight to cgroup.kill.
    if (grace.count() <= 0 || (!delivered && !a->cgroup.empty())) {
        hard_kill(a);
        return;
    }
    a->kill_timer.expires_after(grace);
    a->kill_timer.async_wait([this, a](const boost::system::error_code& ec) {
        if (ec || a->abandoned || a->exited) return;
        hard_kill(a);
    });
}

void LinuxSupervisor::hard_kill(const AttemptPtr& a) {
    if (!a->cgroup.empty()) detail::write_file(a->cgroup / "cgroup.kill", "1");
    signal_group(a->pid, SIGKILL);
}

void LinuxSupervisor::cancel(const std::string& attempt_id, std::chrono::seconds grace) {
    auto it = attempts_.find(attempt_id);
    if (it == attempts_.end()) return;
    begin_kill(it->second, ExitKind::kCancelled, grace);
}

bool LinuxSupervisor::kill_orphan(const ProcessHandle& handle) {
    bool killed = false;
    if (handle.pid > 0) {
        auto ticks = read_start_ticks(handle.pid);
        if (ticks && *ticks == handle.start_ticks) {
            signal_group(handle.pid, SIGKILL);
            signal_pid(handle.pid, SIGKILL);
            killed = true;
            log_info("supervisor: killed orphan pid {}", handle.pid);
        }
    }
    if (!handle.cgroup_path.empty()) {
        fs::path cg(handle.cgroup_path);
        if (::access(cg.c_str(), F_OK) == 0) {
            if (killed) {
                detail::write_file(cg / "cgroup.kill", "1");
                for (int i = 0; i < 1000 && detail::cgroup_populated(cg); ++i)
                    std::this_thread::sleep_for(std::chrono::milliseconds(2));
            }
            if (!detail::cgroup_populated(cg)) {
                if (::rmdir(cg.c_str()) < 0 && errno != ENOENT)
                    log_warn("supervisor: cannot remove cgroup {}: {}", cg.string(), std::strerror(errno));
            } else {
                log_warn("supervisor: leftover cgroup {} still has processes; not removed", cg.string());
            }
        }
    }
    return killed;
}

}  // namespace

std::unique_ptr<IProcessSupervisor> make_linux_supervisor(boost::asio::io_context& io,
                                                          const SupervisorOptions& options) {
    return std::make_unique<LinuxSupervisor>(io, options);
}

}  // namespace tdr::supervisor
