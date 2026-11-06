#include <boost/test/unit_test.hpp>

#include <fcntl.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <fstream>
#include <functional>
#include <future>
#include <map>
#include <random>
#include <sstream>
#include <thread>

#include "fireforge/tdr/v1/envelope.pb.h"
#include "fireforge/tdr/v1/local.pb.h"
#include "tdr/launch/launch.hpp"
#include "tdr/protocol/envelope.hpp"

namespace fs = std::filesystem;
namespace v1 = ::fireforge::tdr::v1;
namespace proto = tdr::protocol;
using namespace tdr::launch;
using namespace std::chrono_literals;

namespace {

GangFile sample() {
    GangFile g;
    g.gang_id = "att-1";
    g.self_node_id = "node-a";
    g.members = {{"node-a", "alpha", 4}, {"node-b", "beta", 2}};
    g.ranks = 6;
    g.mpirun = "/usr/bin/mpirun";
    g.if_include = "127.0.0.0/8";
    g.mca = "btl_tcp_eager_limit=65536, mpi_yield_when_idle = 1,";
    g.launch_timeout = 30;
    g.mpirun_args = {"--bind-to", "core"};
    return g;
}

std::string slurp(const fs::path& p) {
    std::ifstream in(p, std::ios::binary);
    std::ostringstream ss;
    ss << in.rdbuf();
    return ss.str();
}

struct TempDir {
    fs::path path;
    TempDir() {
        // Short base: Unix socket paths are limited to 108 bytes.
        fs::path base = fs::temp_directory_path();
        if (base.string().size() > 60) base = "/tmp";
        std::random_device rd;
        path = base / ("tdrl-" + std::to_string(rd()) + "-" + std::to_string(::getpid()));
        fs::create_directories(path);
    }
    ~TempDir() {
        std::error_code ec;
        fs::remove_all(path, ec);
    }
};

// A one-connection fake tdrd on a Unix socket. The handler runs on a thread with the
// accepted fd; the fd is closed when the handler returns.
class FakeDaemon {
public:
    using Handler = std::function<void(int fd)>;
    FakeDaemon(const fs::path& sock, Handler h) : path_(sock) {
        lfd_ = ::socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
        BOOST_REQUIRE(lfd_ >= 0);
        sockaddr_un addr{};
        addr.sun_family = AF_UNIX;
        BOOST_REQUIRE(sock.string().size() < sizeof(addr.sun_path));
        std::strcpy(addr.sun_path, sock.c_str());
        BOOST_REQUIRE(::bind(lfd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0);
        BOOST_REQUIRE(::listen(lfd_, 4) == 0);
        thread_ = std::thread([this, h = std::move(h)] {
            int fd = ::accept4(lfd_, nullptr, nullptr, SOCK_CLOEXEC);
            if (fd < 0) return;
            try {
                h(fd);
            } catch (const std::exception& e) {
                error = e.what();
            }
            ::close(fd);
        });
    }
    ~FakeDaemon() {
        ::shutdown(lfd_, SHUT_RDWR);  // unblocks accept if nobody connected
        if (thread_.joinable()) thread_.join();
        ::close(lfd_);
    }
    void join() {
        ::shutdown(lfd_, SHUT_RDWR);
        if (thread_.joinable()) thread_.join();
    }
    std::string error;

private:
    fs::path path_;
    int lfd_ = -1;
    std::thread thread_;
};

template <class Msg>
void reply(int fd, v1::MessageType t, const Msg& m, std::uint64_t corr = 0) {
    proto::write_frame(fd, proto::make_frame(t, m, proto::next_message_id(), corr));
}

void reply_error(int fd, const std::string& msg, std::uint64_t corr = 0) {
    v1::Error e;
    e.set_code(v1::BAD_REQUEST);
    e.set_message(msg);
    reply(fd, v1::ERROR, e, corr);
}

// Runs the built tdr-launch with args and extra env (inherits the rest). Returns pid.
pid_t spawn(const std::vector<std::string>& args, const std::map<std::string, std::string>& env,
            const fs::path& cwd = {}) {
    std::vector<std::string> a{TDR_LAUNCH_BIN};
    a.insert(a.end(), args.begin(), args.end());
    std::vector<std::string> envs;
    for (char** e = environ; *e; ++e) {
        std::string s = *e;
        auto k = s.substr(0, s.find('='));
        if (!env.contains(k)) envs.push_back(s);
    }
    for (auto& [k, v] : env) envs.push_back(k + "=" + v);
    std::vector<char*> cargv, cenv;
    for (auto& s : a) cargv.push_back(s.data());
    cargv.push_back(nullptr);
    for (auto& s : envs) cenv.push_back(s.data());
    cenv.push_back(nullptr);
    pid_t pid = ::fork();
    BOOST_REQUIRE(pid >= 0);
    if (pid == 0) {
        if (!cwd.empty() && ::chdir(cwd.c_str()) != 0) ::_exit(121);
        ::execve(cargv[0], cargv.data(), cenv.data());
        ::_exit(122);
    }
    return pid;
}

// Exit code (128+sig for signals); -1 on timeout (the child is killed).
int wait_exit(pid_t pid, std::chrono::milliseconds timeout = 20s) {
    auto deadline = std::chrono::steady_clock::now() + timeout;
    for (;;) {
        int st = 0;
        pid_t r = ::waitpid(pid, &st, WNOHANG);
        if (r == pid) return WIFEXITED(st) ? WEXITSTATUS(st) : 128 + WTERMSIG(st);
        if (std::chrono::steady_clock::now() > deadline) {
            ::kill(pid, SIGKILL);
            ::waitpid(pid, &st, 0);
            return -1;
        }
        std::this_thread::sleep_for(10ms);
    }
}

int run(const std::vector<std::string>& args, const std::map<std::string, std::string>& env,
        const fs::path& cwd = {}) {
    return wait_exit(spawn(args, env, cwd));
}

// Reads one frame, failing the handler if the connection closes.
proto::Frame must_read(int fd) {
    auto f = proto::read_frame(fd);
    if (!f) throw std::runtime_error("unexpected EOF");
    return *f;
}

// member side of the fake daemon: check hello, reply TASK.
void member_hello(int fd, std::atomic<bool>& hello_ok) {
    auto f = must_read(fd);
    v1::WorkerHello h;
    hello_ok = f.type() == v1::WORKER_HELLO && f.parse(h) && h.attempt_id() == "att-9" && h.token() == "tok-9" &&
               h.pid() > 0;
    v1::WorkerTask t;
    t.set_attempt_id("att-9");
    reply(fd, v1::WORKER_TASK, t, f.header.message_id);
}

std::map<std::string, std::string> worker_env(const TempDir& t) {
    return {{"FIREFORGE_IPC_SOCKET", (t.path / "w.sock").string()},
            {"FIREFORGE_ATTEMPT_ID", "att-9"},
            {"FIREFORGE_ATTEMPT_TOKEN", "tok-9"},
            {"FIREFORGE_WORKDIR", t.path.string()}};
}

}  // namespace

// ---------------------------------------------------------------- builders

BOOST_AUTO_TEST_CASE(gang_file_round_trip) {
    GangFile g = sample();
    auto r = parse_gang_file(to_json(g));
    BOOST_REQUIRE_MESSAGE(r.has_value(), (r.has_value() ? std::string() : r.error()));
    BOOST_TEST(r->gang_id == g.gang_id);
    BOOST_TEST(r->self_node_id == g.self_node_id);
    BOOST_REQUIRE_EQUAL(r->members.size(), 2u);
    BOOST_TEST(r->members[1].node_id == "node-b");
    BOOST_TEST(r->members[1].name == "beta");
    BOOST_TEST(r->members[1].ranks == 2u);
    BOOST_TEST(r->ranks == 6u);
    BOOST_TEST(r->mpirun == g.mpirun);
    BOOST_TEST(r->if_include == g.if_include);
    BOOST_TEST(r->mca == g.mca);
    BOOST_TEST(r->launch_timeout == 30u);
    BOOST_TEST(r->mpirun_args == g.mpirun_args, boost::test_tools::per_element());

    TempDir t;
    std::ofstream(t.path / "g.json") << to_json(g);
    auto f = read_gang_file(t.path / "g.json");
    BOOST_REQUIRE(f.has_value());
    BOOST_TEST(f->ranks == 6u);
}

BOOST_AUTO_TEST_CASE(gang_file_defaults) {
    auto r = parse_gang_file(R"({"members":[{"node_id":"n1","ranks":2}],"ranks":2})");
    BOOST_REQUIRE_MESSAGE(r.has_value(), (r.has_value() ? std::string() : r.error()));
    BOOST_TEST(r->mpirun == "mpirun");
    BOOST_TEST(r->launch_timeout == 120u);
    BOOST_TEST(r->mca.empty());
    BOOST_TEST(r->mpirun_args.empty());
}

BOOST_AUTO_TEST_CASE(gang_file_validation_errors) {
    auto bad = [](const std::string& json, const std::string& needle) {
        auto r = parse_gang_file(json);
        BOOST_TEST_CONTEXT(json) {
            BOOST_REQUIRE(!r.has_value());
            BOOST_TEST(r.error().find(needle) != std::string::npos, r.error() << " lacks " << needle);
        }
    };
    bad("{not json", "bad JSON");
    bad("[1,2]", "not a JSON object");
    bad(R"({"ranks":1})", "members");
    bad(R"({"members":[],"ranks":0})", "no members");
    bad(R"({"members":[{"node_id":"a","ranks":2}],"ranks":3})", "!= sum");
    bad(R"({"members":[{"node_id":"a","ranks":2},{"node_id":"b","ranks":2}],"ranks":2})", "!= sum");
    bad(R"({"members":[{"node_id":"a","ranks":2}]})", "ranks");
    bad(R"({"members":[{"node_id":"a","ranks":2}],"ranks":2,"mca":"btl=tcp,oops"})", "without '='");
    bad(R"({"members":[{"node_id":"a","ranks":2}],"ranks":2,"mca":"=x"})", "empty key");
    bad(R"({"members":[{"ranks":2}],"ranks":2})", "node_id");
    bad(R"({"members":[{"node_id":"a","ranks":0}],"ranks":0})", "0 ranks");
    bad(R"({"members":[{"node_id":"a","ranks":-1}],"ranks":-1})", "non-negative");
    bad(R"({"members":[{"node_id":"a","ranks":2}],"ranks":"2"})", "non-negative");
    bad(R"({"members":[{"node_id":"a","ranks":2}],"ranks":2,"mpirun_args":"x"})", "mpirun_args");
    bad(R"({"members":[{"node_id":"a","ranks":2}],"ranks":2,"mpirun_args":[1]})", "mpirun_args");
    bad(R"({"members":[{"node_id":5,"ranks":2}],"ranks":2})", "string");

    auto missing = read_gang_file("/nonexistent/tdr-launch/g.json");
    BOOST_REQUIRE(!missing.has_value());
    BOOST_TEST(missing.error().find("cannot open") != std::string::npos);
}

BOOST_AUTO_TEST_CASE(hostfile_marks_head_localhost) {
    GangFile g = sample();
    BOOST_TEST(hostfile(g) == "localhost slots=4\nnode-b slots=2\n");
    g.self_node_id = "node-b";
    BOOST_TEST(hostfile(g) == "node-a slots=4\nlocalhost slots=2\n");
}

BOOST_AUTO_TEST_CASE(mpirun_argv_full) {
    GangFile g = sample();
    std::vector<std::string> prog{"./hello", "--n", "3"};
    auto a = mpirun_argv(g, "/w/mpi/hostfile", "/opt/tdr/tdr-launch", prog);
    std::vector<std::string> want{"/usr/bin/mpirun", "--hostfile", "/w/mpi/hostfile", "-np", "6",
            "--set-cwd-to-session-dir", "--mca", "plm_rsh_agent", "/opt/tdr/tdr-launch agent",
            "--mca", "plm_rsh_no_tree_spawn", "1",
            "--mca", "oob_tcp_if_include", "127.0.0.0/8",
            "--mca", "btl_tcp_if_include", "127.0.0.0/8",
            "--mca", "btl_tcp_eager_limit", "65536", "--mca", "mpi_yield_when_idle", "1",
            "--bind-to", "core", "/opt/tdr/tdr-launch", "rank", "--", "./hello", "--n", "3"};
    BOOST_TEST(a == want, boost::test_tools::per_element());
}

BOOST_AUTO_TEST_CASE(mpirun_argv_minimal) {
    GangFile g;
    g.members = {{"n1", "", 2}};
    g.self_node_id = "n1";
    g.ranks = 2;
    std::vector<std::string> prog{"prog"};
    auto a = mpirun_argv(g, "hf", "/x/tdr-launch", prog);
    std::vector<std::string> want{"mpirun", "--hostfile", "hf", "-np", "2", "--set-cwd-to-session-dir",
                                  "--mca", "plm_rsh_agent", "/x/tdr-launch agent",
                                  "--mca", "plm_rsh_no_tree_spawn", "1",
                                  "/x/tdr-launch", "rank", "--", "prog"};
    BOOST_TEST(a == want, boost::test_tools::per_element());
}

BOOST_AUTO_TEST_CASE(agent_command_strips_daemonize) {
    std::vector<std::string> w{"orted", "--daemonize", "-mca", "ess", "env", "-mca", "orte_hnp_uri",
                               "\"123.0;tcp://127.0.0.1:4000\"", "--daemonize"};
    BOOST_TEST(agent_command(w) == "orted -mca ess env -mca orte_hnp_uri \"123.0;tcp://127.0.0.1:4000\"");
    std::vector<std::string> one{"PATH=/x:$PATH ; export PATH ; /usr/bin/orted"};
    BOOST_TEST(agent_command(one) == one[0]);
    BOOST_TEST(agent_command(std::vector<std::string>{}).empty());
    std::vector<std::string> only{"--daemonize"};
    BOOST_TEST(agent_command(only).empty());
}

// ---------------------------------------------------------------- member

BOOST_AUTO_TEST_CASE(member_execs_command) {
    TempDir t;
    std::atomic<bool> hello_ok{false}, closed_before_done{false};
    fs::path done = t.path / "done";
    FakeDaemon d(t.path / "w.sock", [&](int fd) {
        member_hello(fd, hello_ok);
        v1::WorkerExec ex;
        ex.set_command("sleep 1; echo \"$FIREFORGE_ATTEMPT_ID $(pwd)\" > '" + done.string() + "'; exit 7");
        reply(fd, v1::WORKER_EXEC, ex);
        // The socket is close-on-exec: EOF arrives while sh is still sleeping.
        auto f = proto::read_frame(fd);
        closed_before_done = !f && !fs::exists(done);
    });
    int rc = run({"member"}, worker_env(t), t.path);
    d.join();
    BOOST_TEST(d.error.empty(), d.error);
    BOOST_TEST(rc == 7);
    BOOST_TEST(hello_ok.load());
    BOOST_TEST(closed_before_done.load());
    BOOST_TEST(slurp(done) == "att-9 " + t.path.string() + "\n");
}

BOOST_AUTO_TEST_CASE(member_times_out) {
    TempDir t;
    std::atomic<bool> hello_ok{false};
    FakeDaemon d(t.path / "w.sock", [&](int fd) {
        member_hello(fd, hello_ok);
        auto f = proto::read_frame(fd);  // EOF when the member gives up
        (void)f;
    });
    auto env = worker_env(t);
    env["FIREFORGE_LAUNCH_TIMEOUT"] = "30";  // --timeout wins
    auto start = std::chrono::steady_clock::now();
    int rc = run({"member", "--timeout", "1"}, env);
    auto took = std::chrono::steady_clock::now() - start;
    d.join();
    BOOST_TEST(rc == 75);
    BOOST_TEST(hello_ok.load());
    BOOST_TEST(took < 10s);
}

BOOST_AUTO_TEST_CASE(member_hello_error) {
    TempDir t;
    FakeDaemon d(t.path / "w.sock", [&](int fd) {
        auto f = must_read(fd);
        reply_error(fd, "bad token", f.header.message_id);
    });
    BOOST_TEST(run({"member"}, worker_env(t)) == 70);
}

BOOST_AUTO_TEST_CASE(member_cancel_and_close) {
    TempDir t;
    {
        std::atomic<bool> ok{false};
        FakeDaemon d(t.path / "w.sock", [&](int fd) {
            member_hello(fd, ok);
            v1::WorkerCancel c;
            reply(fd, v1::WORKER_CANCEL, c);
        });
        BOOST_TEST(run({"member"}, worker_env(t)) == 143);
    }
    fs::remove(t.path / "w.sock");
    {
        std::atomic<bool> ok{false};
        FakeDaemon d(t.path / "w.sock", [&](int fd) { member_hello(fd, ok); });  // then closes
        BOOST_TEST(run({"member"}, worker_env(t)) == 70);
    }
    fs::remove(t.path / "w.sock");
    BOOST_TEST(run({"member"}, worker_env(t)) == 70);  // nobody listening
}

// ---------------------------------------------------------------- agent

BOOST_AUTO_TEST_CASE(agent_relays_exit_code) {
    TempDir t;
    v1::GangLaunchRequest got;
    std::atomic<bool> got_ok{false};
    FakeDaemon d(t.path / "w.sock", [&](int fd) {
        auto f = must_read(fd);
        got_ok = f.type() == v1::WORKER_GANG_LAUNCH && f.parse(got);
        reply(fd, v1::WORKER_ACK, v1::Ack{}, f.header.message_id);
        std::this_thread::sleep_for(100ms);
        v1::GangExitNotice n;
        n.set_exit_code(3);
        reply(fd, v1::WORKER_GANG_EXIT, n);
    });
    int rc = run({"agent", "-x", "node-b", "orted", "--daemonize", "-mca", "ess", "env"}, worker_env(t));
    d.join();
    BOOST_TEST(rc == 3);
    BOOST_REQUIRE(got_ok.load());
    BOOST_TEST(got.attempt_id() == "att-9");
    BOOST_TEST(got.token() == "tok-9");
    BOOST_TEST(got.host() == "node-b");
    BOOST_TEST(got.command() == "orted -mca ess env");
}

BOOST_AUTO_TEST_CASE(agent_exit_code_clamped) {
    TempDir t;
    FakeDaemon d(t.path / "w.sock", [&](int fd) {
        must_read(fd);
        v1::GangExitNotice n;
        n.set_exit_code(-1);
        n.set_error("member job ended");
        reply(fd, v1::WORKER_GANG_EXIT, n);
    });
    BOOST_TEST(run({"agent", "node-b", "orted"}, worker_env(t)) == 255);
}

BOOST_AUTO_TEST_CASE(agent_error_and_close) {
    TempDir t;
    {
        FakeDaemon d(t.path / "w.sock", [&](int fd) {
            auto f = must_read(fd);
            reply_error(fd, "member not running", f.header.message_id);
        });
        BOOST_TEST(run({"agent", "node-b", "orted"}, worker_env(t)) == 255);
    }
    fs::remove(t.path / "w.sock");
    {
        FakeDaemon d(t.path / "w.sock", [&](int fd) {
            auto f = must_read(fd);
            reply(fd, v1::WORKER_ACK, v1::Ack{}, f.header.message_id);
        });  // then closes without GANG_EXIT
        BOOST_TEST(run({"agent", "node-b", "orted"}, worker_env(t)) == 255);
    }
    BOOST_TEST(run({"agent", "node-b"}, worker_env(t)) == 255);  // no command words
}

BOOST_AUTO_TEST_CASE(agent_sigterm_closes_connection) {
    TempDir t;
    std::promise<void> acked;
    auto acked_f = acked.get_future();
    std::atomic<bool> saw_close{false};
    FakeDaemon d(t.path / "w.sock", [&](int fd) {
        auto f = must_read(fd);
        reply(fd, v1::WORKER_ACK, v1::Ack{}, f.header.message_id);
        acked.set_value();
        saw_close = !proto::read_frame(fd);
    });
    pid_t pid = spawn({"agent", "node-b", "orted"}, worker_env(t));
    BOOST_REQUIRE(acked_f.wait_for(10s) == std::future_status::ready);
    std::this_thread::sleep_for(100ms);  // agent is now blocked waiting for GANG_EXIT
    ::kill(pid, SIGTERM);
    BOOST_TEST(wait_exit(pid) == 143);
    d.join();
    BOOST_TEST(saw_close.load());
}

// ---------------------------------------------------------------- rank / head

BOOST_AUTO_TEST_CASE(rank_chdirs_to_workdir) {
    TempDir t;
    auto env = worker_env(t);
    int rc = run({"rank", "--", "/bin/sh", "-c", "pwd > out.txt; exit 5"}, env, "/");
    BOOST_TEST(rc == 5);
    BOOST_TEST(slurp(t.path / "out.txt") == t.path.string() + "\n");

    env["FIREFORGE_WORKDIR"] = (t.path / "missing").string();
    BOOST_TEST(run({"rank", "--", "/bin/true"}, env) == 70);
    BOOST_TEST(run({"rank", "--", (t.path / "no-such-program").string()}, worker_env(t)) == 70);
}

BOOST_AUTO_TEST_CASE(head_writes_hostfile_and_execs_mpirun) {
    TempDir t;
    fs::path fake = t.path / "fake-mpirun";
    fs::path out = t.path / "argv.txt";
    {
        std::ofstream s(fake);
        s << "#!/bin/sh\nprintf '%s\\n' \"$@\" > '" << out.string() << "'\n"
          << "echo \"DISPLAY=${DISPLAY-unset}\" >> '" << out.string() << "'\nexit 9\n";
    }
    fs::permissions(fake, fs::perms::owner_all);
    GangFile g = sample();
    g.mca = "btl=self,mpi_yield_when_idle=1";
    g.mpirun = fake.string();
    fs::path gang = t.path / "gang.json";
    std::ofstream(gang) << to_json(g);

    auto env = worker_env(t);
    env["DISPLAY"] = ":0";
    int rc = run({"head", "--gang", gang.string(), "--", "./hello", "arg with space"}, env);
    BOOST_TEST(rc == 9);

    fs::path hf = t.path / "mpi" / "hostfile";
    BOOST_TEST(slurp(hf) == "localhost slots=4\nnode-b slots=2\n");
    std::vector<std::string> prog{"./hello", "arg with space"};
    auto want = mpirun_argv(g, hf, fs::canonical(TDR_LAUNCH_BIN), prog);
    std::string expect;
    for (std::size_t i = 1; i < want.size(); ++i) expect += want[i] + "\n";  // $@ excludes argv[0]
    expect += "DISPLAY=unset\n";
    BOOST_TEST(slurp(out) == expect);
}

BOOST_AUTO_TEST_CASE(head_errors_exit_70) {
    TempDir t;
    auto env = worker_env(t);
    BOOST_TEST(run({"head", "--gang", (t.path / "none.json").string(), "--", "x"}, env) == 70);
    std::ofstream(t.path / "bad.json") << R"({"members":[{"node_id":"a","ranks":2}],"ranks":3})";
    BOOST_TEST(run({"head", "--gang", (t.path / "bad.json").string(), "--", "x"}, env) == 70);
    GangFile g = sample();
    g.mca = "";
    g.mpirun = (t.path / "no-mpirun").string();
    std::ofstream(t.path / "g.json") << to_json(g);
    BOOST_TEST(run({"head", "--gang", (t.path / "g.json").string(), "--", "x"}, env) == 70);
    BOOST_TEST(run({"head", "--gang", (t.path / "g.json").string()}, env) == 70);  // no program
}
