// Tests for the Linux process supervisor. Generic tests use Cgroups::kAuto, so
// they exercise cgroups when run inside a delegated scope and rlimits otherwise:
//   systemd-run --user --scope --quiet --slice=fireforge-build.slice -p Delegate=yes -- <this binary>
#include <tdr/supervisor/supervisor.hpp>

#include "../src/internal.hpp"

#include <boost/asio/io_context.hpp>
#include <boost/test/unit_test.hpp>

#include <chrono>
#include <csignal>
#include <fstream>
#include <optional>
#include <sstream>
#include <thread>

#include <fcntl.h>
#include <sys/wait.h>
#include <unistd.h>

using namespace tdr::supervisor;
using namespace std::chrono_literals;

namespace {

struct TempDir {
    fs::path path;
    TempDir() {
        fs::create_directories("/tmp/claude-1000");
        std::string tmpl = "/tmp/claude-1000/ff-sup-XXXXXX";
        BOOST_REQUIRE(::mkdtemp(tmpl.data()) != nullptr);
        path = tmpl;
    }
    ~TempDir() {
        std::error_code ec;
        fs::remove_all(path, ec);
    }
};

std::string slurp(const fs::path& p) {
    std::ifstream in(p);
    std::stringstream ss;
    ss << in.rdbuf();
    return ss.str();
}

std::string trim(std::string s) {
    while (!s.empty() && (s.back() == '\n' || s.back() == ' ')) s.pop_back();
    return s;
}

struct Fixture {
    boost::asio::io_context io;
    TempDir dir;
    std::unique_ptr<IProcessSupervisor> sup;
    int next = 0;

    explicit Fixture(SupervisorOptions::Cgroups mode = SupervisorOptions::Cgroups::kAuto,
                     std::chrono::seconds grace = 1s) {
        SupervisorOptions o;
        o.cgroups = mode;
        o.kill_grace = grace;
        sup = make_linux_supervisor(io, o);
    }

    LaunchSpec spec(std::vector<std::string> argv) {
        LaunchSpec s;
        s.attempt_id = "att-test-" + std::to_string(::getpid()) + "-" + std::to_string(next++);
        s.argv = std::move(argv);
        s.working_dir = dir.path;
        s.stdout_path = dir.path / "stdout.log";
        s.stderr_path = dir.path / "stderr.log";
        return s;
    }

    // Launches and runs the io_context until on_exit fires.
    struct Run {
        ProcessHandle handle;
        ExitInfo info;
        int calls = 0;
    };
    std::shared_ptr<Run> start(const LaunchSpec& s) {
        auto r = std::make_shared<Run>();
        auto h = sup->launch(s, [r](const ExitInfo& i) {
            r->info = i;
            ++r->calls;
        });
        BOOST_REQUIRE_MESSAGE(h.has_value(), "launch failed: " << (h ? "" : h.error()));
        BOOST_CHECK_EQUAL(r->calls, 0);  // never synchronous
        r->handle = *h;
        return r;
    }
    void wait(const std::shared_ptr<Run>& r, std::chrono::milliseconds limit = 10s) {
        auto deadline = std::chrono::steady_clock::now() + limit;
        while (r->calls == 0 && std::chrono::steady_clock::now() < deadline) {
            io.restart();
            io.run_one_for(50ms);
        }
        BOOST_REQUIRE_MESSAGE(r->calls == 1, "on_exit not called");
        // Drain anything else queued; on_exit must not fire twice.
        io.restart();
        io.poll();
        BOOST_CHECK_EQUAL(r->calls, 1);
    }
    ExitInfo run(const LaunchSpec& s) {
        auto r = start(s);
        wait(r);
        return r->info;
    }
};

bool process_gone(pid_t pid) {
    if (::kill(pid, 0) < 0 && errno == ESRCH) return true;
    // A zombie waiting for its (new) parent to reap it counts as gone.
    auto stat = tdr::supervisor::detail::read_file("/proc/" + std::to_string(pid) + "/stat");
    if (!stat) return true;
    auto rp = stat->rfind(')');
    return rp != std::string::npos && rp + 2 < stat->size() && (*stat)[rp + 2] == 'Z';
}

bool wait_gone(pid_t pid, std::chrono::milliseconds limit = 2s) {
    auto deadline = std::chrono::steady_clock::now() + limit;
    while (!process_gone(pid)) {
        if (std::chrono::steady_clock::now() > deadline) return false;
        std::this_thread::sleep_for(5ms);
    }
    return true;
}

}  // namespace

BOOST_AUTO_TEST_SUITE(supervisor)

BOOST_AUTO_TEST_CASE(to_string_kinds) {
    BOOST_CHECK_EQUAL(std::string(to_string(ExitKind::kExited)), "exited");
    BOOST_CHECK_EQUAL(std::string(to_string(ExitKind::kOomKilled)), "oom_killed");
    BOOST_CHECK_EQUAL(std::string(to_string(ExitKind::kCancelled)), "cancelled");
}

BOOST_AUTO_TEST_CASE(parse_stat_with_weird_comm) {
    std::string stat = "1234 (a b) c)) (d) S 1 1234 1234 0 -1 4194560 100 0 0 0 0 0 0 0 20 0 1 0 987654 1000 10";
    auto t = tdr::supervisor::detail::parse_start_ticks(stat);
    BOOST_REQUIRE(t.has_value());
    BOOST_CHECK_EQUAL(*t, 987654u);
    BOOST_CHECK(!tdr::supervisor::detail::parse_start_ticks("garbage").has_value());
    BOOST_CHECK(read_start_ticks(::getpid()).has_value());
}

BOOST_AUTO_TEST_CASE(exit_codes) {
    Fixture f;
    BOOST_TEST_MESSAGE("cgroups_enabled=" << f.sup->cgroups_enabled());
    auto ok = f.run(f.spec({"true"}));
    BOOST_CHECK(ok.kind == ExitKind::kExited);
    BOOST_CHECK_EQUAL(ok.exit_code, 0);
    BOOST_CHECK(ok.peak_memory_bytes > 0);

    auto three = f.run(f.spec({"sh", "-c", "exit 3"}));
    BOOST_CHECK(three.kind == ExitKind::kExited);
    BOOST_CHECK_EQUAL(three.exit_code, 3);
    BOOST_CHECK_EQUAL(three.message, "exited with code 3");
    BOOST_CHECK_EQUAL(f.sup->running_count(), 0u);
}

BOOST_AUTO_TEST_CASE(output_captured_to_files) {
    Fixture f;
    auto s = f.spec({"sh", "-c", "echo out; echo err >&2"});
    auto info = f.run(s);
    BOOST_CHECK_EQUAL(info.exit_code, 0);
    BOOST_CHECK_EQUAL(slurp(s.stdout_path), "out\n");
    BOOST_CHECK_EQUAL(slurp(s.stderr_path), "err\n");
    auto perms = fs::status(s.stdout_path).permissions();
    BOOST_CHECK((perms & fs::perms::others_all) == fs::perms::none);
}

BOOST_AUTO_TEST_CASE(environment) {
    Fixture f;
    auto s = f.spec({"sh", "-c", "echo \"$FOO|$LANG|$HOME|${PATH:+haspath}|${SECRET_X:-unset}\""});
    s.env["FOO"] = "bar";
    s.env["LANG"] = "en_US.UTF-8";
    ::setenv("SECRET_X", "leak", 1);
    auto info = f.run(s);
    ::unsetenv("SECRET_X");
    BOOST_CHECK_EQUAL(info.exit_code, 0);
    std::string home = std::getenv("HOME") ? std::getenv("HOME") : "/";
    BOOST_CHECK_EQUAL(trim(slurp(s.stdout_path)), "bar|en_US.UTF-8|" + home + "|haspath|unset");

    auto d = f.spec({"sh", "-c", "echo $LANG"});
    f.run(d);
    BOOST_CHECK_EQUAL(trim(slurp(d.stdout_path)), "C.UTF-8");
}

BOOST_AUTO_TEST_CASE(working_dir) {
    Fixture f;
    fs::create_directories(f.dir.path / "sub");
    auto s = f.spec({"pwd"});
    s.working_dir = f.dir.path / "sub";
    f.run(s);
    BOOST_CHECK_EQUAL(trim(slurp(s.stdout_path)), (f.dir.path / "sub").string());

    auto bad = f.spec({"true"});
    bad.working_dir = f.dir.path / "missing";
    auto r = f.sup->launch(bad, [](const ExitInfo&) { BOOST_ERROR("callback for failed launch"); });
    BOOST_REQUIRE(!r.has_value());
    BOOST_CHECK_EQUAL(r.error(), "chdir: No such file or directory");
    BOOST_CHECK_EQUAL(f.sup->running_count(), 0u);
}

BOOST_AUTO_TEST_CASE(nonexistent_binary) {
    Fixture f;
    bool called = false;
    auto r = f.sup->launch(f.spec({"definitely-not-a-binary-xyz"}), [&](const ExitInfo&) { called = true; });
    BOOST_REQUIRE(!r.has_value());
    BOOST_CHECK_EQUAL(r.error(), "execvp: No such file or directory");
    auto r2 = f.sup->launch(f.spec({"/nonexistent/bin"}), [&](const ExitInfo&) { called = true; });
    BOOST_REQUIRE(!r2.has_value());
    BOOST_CHECK_EQUAL(r2.error(), "execvp: No such file or directory");
    f.io.restart();
    f.io.poll();
    BOOST_CHECK(!called);
    BOOST_CHECK_EQUAL(f.sup->running_count(), 0u);
    BOOST_CHECK(!f.sup->launch(f.spec({}), nullptr).has_value());
}

BOOST_AUTO_TEST_CASE(external_signal) {
    Fixture f;
    auto r = f.start(f.spec({"sleep", "30"}));
    BOOST_CHECK(r->handle.start_ticks > 0);
    BOOST_CHECK_EQUAL(f.sup->running_count(), 1u);
    ::kill(r->handle.pid, SIGUSR1);
    f.wait(r);
    BOOST_CHECK(r->info.kind == ExitKind::kSignaled);
    BOOST_CHECK_EQUAL(r->info.signal, SIGUSR1);
    BOOST_CHECK_EQUAL(f.sup->running_count(), 0u);
}

BOOST_AUTO_TEST_CASE(timeout) {
    Fixture f(SupervisorOptions::Cgroups::kAuto, 1s);
    auto s = f.spec({"sleep", "30"});
    s.timeout = 1s;
    auto info = f.run(s);
    BOOST_CHECK(info.kind == ExitKind::kTimedOut);
    BOOST_CHECK_EQUAL(info.signal, SIGTERM);
    BOOST_CHECK(info.wall_time >= 900ms);
    BOOST_CHECK(info.wall_time < 2000ms);
}

BOOST_AUTO_TEST_CASE(term_ignored_then_killed) {
    Fixture f(SupervisorOptions::Cgroups::kAuto, 1s);
    auto s = f.spec({"sh", "-c", "trap '' TERM; sleep 30"});
    s.timeout = 1s;
    auto info = f.run(s);
    BOOST_CHECK(info.kind == ExitKind::kTimedOut);
    BOOST_CHECK_EQUAL(info.signal, SIGKILL);
    BOOST_CHECK(info.wall_time >= 1900ms);
    BOOST_CHECK(info.wall_time < 3500ms);
}

BOOST_AUTO_TEST_CASE(cancel) {
    Fixture f;
    auto s1 = f.spec({"sleep", "30"});
    auto r = f.start(s1);
    f.io.restart();
    f.io.poll();
    auto id = s1.attempt_id;
    f.sup->cancel("unknown-attempt", 0s);  // no-op
    f.sup->cancel(id, 5s);
    f.wait(r, 3s);
    BOOST_CHECK(r->info.kind == ExitKind::kCancelled);
    BOOST_CHECK_EQUAL(r->info.signal, SIGTERM);
    BOOST_CHECK(r->info.wall_time < 3s);
    f.sup->cancel(id, 0s);  // already gone: no-op
    f.io.restart();
    f.io.poll();
    BOOST_CHECK_EQUAL(r->calls, 1);

    // grace 0 = immediate SIGKILL
    auto s2 = f.spec({"sh", "-c", "trap '' TERM; echo ready; sleep 30"});
    auto r2 = f.start(s2);
    for (int i = 0; i < 400 && slurp(s2.stdout_path).empty(); ++i) std::this_thread::sleep_for(5ms);
    f.sup->cancel(s2.attempt_id, 0s);
    f.wait(r2, 3s);
    BOOST_CHECK(r2->info.kind == ExitKind::kCancelled);
    BOOST_CHECK_EQUAL(r2->info.signal, SIGKILL);
}

BOOST_AUTO_TEST_CASE(grandchildren_killed) {
    Fixture f;
    auto s = f.spec({"sh", "-c", "sleep 30 & echo $!; exit 0"});
    auto info = f.run(s);
    BOOST_CHECK_EQUAL(info.exit_code, 0);
    pid_t gc = static_cast<pid_t>(std::stol(trim(slurp(s.stdout_path))));
    BOOST_CHECK_MESSAGE(wait_gone(gc, 2s), "grandchild " << gc << " survived");
}

BOOST_AUTO_TEST_CASE(fds_not_leaked) {
    Fixture f;
    int leak = ::open("/dev/null", O_RDONLY);  // deliberately without O_CLOEXEC
    BOOST_REQUIRE(leak >= 0);
    auto s = f.spec({"ls", "-1", "/proc/self/fd"});
    f.run(s);
    ::close(leak);
    // ls itself has 0,1,2 and the directory fd it is reading.
    std::istringstream in(slurp(s.stdout_path));
    int count = 0;
    for (std::string line; std::getline(in, line);) ++count;
    BOOST_CHECK_EQUAL(count, 4);
}

BOOST_AUTO_TEST_CASE(kill_orphan_checks_start_ticks) {
    Fixture f;
    pid_t pid = ::fork();
    BOOST_REQUIRE(pid >= 0);
    if (pid == 0) {
        ::setpgid(0, 0);
        ::execlp("sleep", "sleep", "30", static_cast<char*>(nullptr));
        ::_exit(127);
    }
    std::optional<std::uint64_t> ticks;
    for (int i = 0; i < 100 && !ticks; ++i) {
        ticks = read_start_ticks(pid);
        std::this_thread::sleep_for(1ms);
    }
    BOOST_REQUIRE(ticks.has_value());
    std::this_thread::sleep_for(20ms);  // let setpgid happen

    ProcessHandle wrong{pid, *ticks + 1, ""};
    BOOST_CHECK(!f.sup->kill_orphan(wrong));
    BOOST_CHECK_EQUAL(::kill(pid, 0), 0);  // still alive

    ProcessHandle right{pid, *ticks, ""};
    BOOST_CHECK(f.sup->kill_orphan(right));
    int status = 0;
    BOOST_REQUIRE_EQUAL(::waitpid(pid, &status, 0), pid);
    BOOST_CHECK(WIFSIGNALED(status) && WTERMSIG(status) == SIGKILL);

    // Missing process now: false.
    BOOST_CHECK(!f.sup->kill_orphan(right));
}

BOOST_AUTO_TEST_CASE(running_count) {
    Fixture f;
    auto a = f.start(f.spec({"sleep", "0.2"}));
    auto b = f.start(f.spec({"sleep", "0.4"}));
    BOOST_CHECK_EQUAL(f.sup->running_count(), 2u);
    f.wait(a);
    BOOST_CHECK_EQUAL(f.sup->running_count(), 1u);
    f.wait(b);
    BOOST_CHECK_EQUAL(f.sup->running_count(), 0u);
    // Duplicate attempt ids are rejected while running.
    auto s = f.spec({"sleep", "0.2"});
    auto c = f.start(s);
    BOOST_CHECK(!f.sup->launch(s, nullptr).has_value());
    f.wait(c);
}

BOOST_AUTO_TEST_CASE(memory_limit_rlimit_fallback) {
    Fixture f(SupervisorOptions::Cgroups::kOff);
    BOOST_CHECK(!f.sup->cgroups_enabled());
    auto s = f.spec({"python3", "-c", "s = 'x' * (256 << 20); print(len(s))"});
    s.limits.memory_bytes = 64ull << 20;
    auto info = f.run(s);
    BOOST_CHECK(info.kind == ExitKind::kExited || info.kind == ExitKind::kSignaled);
    BOOST_CHECK(info.exit_code != 0 || info.signal != 0);
    BOOST_CHECK_MESSAGE(slurp(s.stdout_path).empty(), "allocation unexpectedly succeeded");

    // Control: same program without a limit succeeds (well under the 8 GiB cap).
    auto ok = f.spec({"python3", "-c", "s = 'x' * (16 << 20); print(len(s))"});
    ok.limits.memory_bytes = 0;
    BOOST_CHECK_EQUAL(f.run(ok).exit_code, 0);
}

BOOST_AUTO_TEST_CASE(cgroup_oom_killed) {
    Fixture f;
    if (!f.sup->cgroups_enabled()) {
        BOOST_TEST_MESSAGE("SKIP cgroup_oom_killed: cgroups v2 not delegated here");
        return;
    }
    auto s = f.spec({"python3", "-c", "s = 'x' * (256 << 20); print(len(s))"});
    s.limits.memory_bytes = 64ull << 20;
    auto r = f.start(s);
    BOOST_CHECK(!r->handle.cgroup_path.empty());
    BOOST_CHECK(fs::exists(r->handle.cgroup_path));
    BOOST_CHECK_EQUAL(trim(slurp(fs::path(r->handle.cgroup_path) / "memory.max")), std::to_string(64ull << 20));
    BOOST_CHECK_EQUAL(trim(slurp(fs::path(r->handle.cgroup_path) / "cpu.max")), "100000 100000");
    f.wait(r);
    BOOST_CHECK_MESSAGE(r->info.kind == ExitKind::kOomKilled, "kind=" << to_string(r->info.kind) << " " << r->info.message);
    BOOST_CHECK(r->info.peak_memory_bytes > 0);
    BOOST_CHECK(r->info.peak_memory_bytes <= (64ull << 20));
    BOOST_CHECK(!fs::exists(r->handle.cgroup_path));  // removed before on_exit
}

BOOST_AUTO_TEST_CASE(cgroup_kill_orphan_removes_cgroup) {
    Fixture f;
    if (!f.sup->cgroups_enabled()) {
        BOOST_TEST_MESSAGE("SKIP cgroup_kill_orphan_removes_cgroup: cgroups v2 not delegated here");
        return;
    }
    // A "previous run": launch with one supervisor, then forget it (destroyed,
    // child keeps running), and recover with another one.
    ProcessHandle h;
    {
        boost::asio::io_context io2;
        SupervisorOptions o;
        auto old = make_linux_supervisor(io2, o);
        auto s = f.spec({"sh", "-c", "sleep 30 & sleep 30"});
        auto r = old->launch(s, [](const ExitInfo&) {});
        BOOST_REQUIRE(r.has_value());
        h = *r;
    }
    BOOST_CHECK(fs::exists(h.cgroup_path));
    BOOST_CHECK(f.sup->kill_orphan(h));
    BOOST_CHECK(!fs::exists(h.cgroup_path));
    int status = 0;
    ::waitpid(h.pid, &status, 0);  // it was our child in this test process

    // Leftover empty cgroup with no process: removed, returns false.
    fs::path stale = fs::path(h.cgroup_path).parent_path() / "att-stale-test";
    fs::create_directory(stale);
    BOOST_CHECK(!f.sup->kill_orphan(ProcessHandle{h.pid, h.start_ticks, stale.string()}));
    BOOST_CHECK(!fs::exists(stale));
}

BOOST_AUTO_TEST_CASE(cgroups_required_and_off) {
    boost::asio::io_context io;
    SupervisorOptions req;
    req.cgroups = SupervisorOptions::Cgroups::kRequired;
    req.cgroup_root = "/tmp/claude-1000/not-a-cgroup";
    BOOST_CHECK_THROW(make_linux_supervisor(io, req), std::runtime_error);

    SupervisorOptions autob = req;
    autob.cgroups = SupervisorOptions::Cgroups::kAuto;
    BOOST_CHECK(!make_linux_supervisor(io, autob)->cgroups_enabled());

    SupervisorOptions off;
    off.cgroups = SupervisorOptions::Cgroups::kOff;
    BOOST_CHECK(!make_linux_supervisor(io, off)->cgroups_enabled());
}

// ---- run_as -------------------------------------------------------------

namespace {

RunAs current_identity() {
    RunAs r;
    r.uid = ::getuid();
    r.gid = ::getgid();
    int n = ::getgroups(0, nullptr);
    BOOST_REQUIRE(n >= 0);
    r.groups.resize(static_cast<std::size_t>(n));
    BOOST_REQUIRE(::getgroups(n, r.groups.data()) == n);
    return r;
}

// Value of "Key:\t..." in /proc/self/status-like content.
std::string status_field(const std::string& status, const std::string& key) {
    std::istringstream in(status);
    for (std::string line; std::getline(in, line);) {
        if (line.rfind(key + ":", 0) == 0) {
            auto v = line.substr(key.size() + 1);
            auto b = v.find_first_not_of(" \t");
            return b == std::string::npos ? "" : v.substr(b);
        }
    }
    return "<missing>";
}

bool have_setid_caps() {
    auto st = tdr::supervisor::detail::read_file("/proc/self/status").value_or("");
    std::uint64_t eff = std::strtoull(status_field(st, "CapEff").c_str(), nullptr, 16);
    return (eff >> 6 & 1) && (eff >> 7 & 1);  // CAP_SETGID=6, CAP_SETUID=7
}

}  // namespace

BOOST_AUTO_TEST_CASE(run_as_same_user_hardens) {
    Fixture f;
    auto s = f.spec({"sh", "-c", "cat /proc/self/status; echo umask=$(umask)"});
    s.run_as = current_identity();
    auto info = f.run(s);
    BOOST_CHECK(info.kind == ExitKind::kExited);
    BOOST_CHECK_EQUAL(info.exit_code, 0);
    auto out = slurp(s.stdout_path);
    BOOST_CHECK_EQUAL(status_field(out, "NoNewPrivs"), "1");
    BOOST_CHECK_EQUAL(status_field(out, "CapEff"), "0000000000000000");
    BOOST_CHECK_EQUAL(status_field(out, "CapPrm"), "0000000000000000");
    BOOST_CHECK_EQUAL(status_field(out, "CapInh"), "0000000000000000");
    BOOST_CHECK_EQUAL(status_field(out, "CapAmb"), "0000000000000000");
    BOOST_CHECK(out.find("umask=0007") != std::string::npos);
    // Without run_as nothing changes.
    auto plain = f.spec({"sh", "-c", "cat /proc/self/status"});
    f.run(plain);
    BOOST_CHECK_EQUAL(status_field(slurp(plain.stdout_path), "NoNewPrivs"), "0");
}

BOOST_AUTO_TEST_CASE(run_as_other_user_without_privileges_fails) {
    if (have_setid_caps() || ::geteuid() == 0) {
        BOOST_TEST_MESSAGE("skipped: process holds CAP_SETUID/CAP_SETGID");
        return;
    }
    Fixture f;
    auto s = f.spec({"true"});
    s.run_as = RunAs{65534, 65534, {65534}};
    bool called = false;
    auto r = f.sup->launch(s, [&](const ExitInfo&) { called = true; });
    BOOST_REQUIRE(!r.has_value());
    BOOST_TEST_MESSAGE("error: " << r.error());
    BOOST_CHECK(r.error().find("setgroups") != std::string::npos || r.error().find("setuid") != std::string::npos);
    BOOST_CHECK(r.error().find("Operation not permitted") != std::string::npos);
    f.io.restart();
    f.io.poll();
    BOOST_CHECK(!called);
    BOOST_CHECK_EQUAL(f.sup->running_count(), 0u);
}

// Needs CAP_SETUID + CAP_SETGID (+ CAP_KILL), e.g.
//   sudo setcap cap_setuid,cap_setgid,cap_kill+ep <test binary>
//   tools/capped <test binary> -t 'supervisor/run_as*'
BOOST_AUTO_TEST_CASE(run_as_nobody_with_privileges) {
    if (!have_setid_caps()) {
        BOOST_TEST_MESSAGE("skipped: needs CAP_SETUID + CAP_SETGID");
        return;
    }
    Fixture f;
    auto s = f.spec({"sh", "-c", "id -u; id -g; id -G; cat /proc/self/status"});
    s.working_dir = "/";  // the temp dir's ancestors are 0700
    s.run_as = RunAs{65534, 65534, {65534}};
    auto info = f.run(s);
    BOOST_CHECK_EQUAL(info.exit_code, 0);
    auto out = slurp(s.stdout_path);
    std::istringstream in(out);
    std::string uid, gid, groups;
    std::getline(in, uid);
    std::getline(in, gid);
    std::getline(in, groups);
    BOOST_CHECK_EQUAL(uid, "65534");
    BOOST_CHECK_EQUAL(gid, "65534");
    BOOST_CHECK_EQUAL(groups, "65534");
    BOOST_CHECK_EQUAL(status_field(out, "CapEff"), "0000000000000000");
    BOOST_CHECK_EQUAL(status_field(out, "CapPrm"), "0000000000000000");
    BOOST_CHECK_EQUAL(status_field(out, "CapAmb"), "0000000000000000");
    BOOST_CHECK_EQUAL(status_field(out, "NoNewPrivs"), "1");

    // Cancel and timeout still kill a child owned by another uid.
    auto c = f.spec({"sleep", "30"});
    c.working_dir = "/";
    c.run_as = RunAs{65534, 65534, {65534}};
    auto r = f.start(c);
    f.sup->cancel(c.attempt_id, 1s);
    f.wait(r);
    BOOST_CHECK(r->info.kind == ExitKind::kCancelled);

    auto t = f.spec({"sh", "-c", "trap '' TERM; sleep 30"});
    t.working_dir = "/";
    t.run_as = RunAs{65534, 65534, {65534}};
    t.timeout = 1s;
    auto tr = f.start(t);
    f.wait(tr);
    BOOST_CHECK(tr->info.kind == ExitKind::kTimedOut);
}

BOOST_AUTO_TEST_SUITE_END()
