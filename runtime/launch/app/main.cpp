// tdr-launch: MPI launch helper (docs/design/phase7.md §5).
//
//   tdr-launch head --gang FILE -- argv...   build the hostfile, exec mpirun
//   tdr-launch agent [-opts] <host> <words...>   mpirun's plm_rsh_agent
//   tdr-launch member [--timeout S]          wait for WORKER_EXEC, exec /bin/sh -c
//   tdr-launch rank -- argv...               chdir $FIREFORGE_WORKDIR, exec argv
//
// Plain blocking POSIX I/O; frames via tdr::protocol.
#include <poll.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <optional>
#include <string>
#include <system_error>
#include <vector>

#include "fireforge/tdr/v1/envelope.pb.h"
#include "fireforge/tdr/v1/local.pb.h"
#include "tdr/launch/launch.hpp"
#include "tdr/protocol/envelope.hpp"

namespace {

namespace fs = std::filesystem;
namespace v1 = ::fireforge::tdr::v1;
namespace proto = tdr::protocol;
using Clock = std::chrono::steady_clock;

constexpr int kExitUsage = 64;
constexpr int kExitError = 70;      // EX_SOFTWARE
constexpr int kExitTimeout = 75;    // EX_TEMPFAIL
constexpr int kExitCancelled = 143; // 128 + SIGTERM
constexpr int kExitAgentFail = 255; // Open MPI: remote launch failed

void err(const std::string& msg) { std::fprintf(stderr, "tdr-launch: %s\n", msg.c_str()); }

std::string env_or(const char* name, const std::string& def = {}) {
    const char* v = std::getenv(name);
    return v ? std::string(v) : def;
}

// Connects to $FIREFORGE_IPC_SOCKET; close-on-exec. -1 + message on failure.
int connect_worker_socket(std::string& error) {
    std::string path = env_or("FIREFORGE_IPC_SOCKET");
    if (path.empty()) {
        error = "FIREFORGE_IPC_SOCKET is not set";
        return -1;
    }
    sockaddr_un addr{};
    if (path.size() >= sizeof(addr.sun_path)) {
        error = "socket path too long: " + path;
        return -1;
    }
    int fd = ::socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (fd < 0) {
        error = std::string("socket: ") + std::strerror(errno);
        return -1;
    }
    addr.sun_family = AF_UNIX;
    std::memcpy(addr.sun_path, path.data(), path.size());
    int rc;
    do {
        rc = ::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    } while (rc < 0 && errno == EINTR);
    if (rc < 0) {
        error = "connect " + path + ": " + std::strerror(errno);
        ::close(fd);
        return -1;
    }
    return fd;
}

template <class Msg>
void send_msg(int fd, v1::MessageType type, const Msg& msg) {
    proto::write_frame(fd, proto::make_frame(type, msg, proto::next_message_id()));
}

// Waits until fd is readable or the deadline passes. False on timeout.
bool wait_readable(int fd, std::optional<Clock::time_point> deadline) {
    for (;;) {
        int timeout_ms = -1;
        if (deadline) {
            auto left = std::chrono::duration_cast<std::chrono::milliseconds>(*deadline - Clock::now()).count();
            if (left <= 0) return false;
            timeout_ms = static_cast<int>(std::min<long long>(left, 60'000));
        }
        pollfd p{fd, POLLIN, 0};
        int rc = ::poll(&p, 1, timeout_ms);
        if (rc < 0) {
            if (errno == EINTR) continue;
            throw std::system_error(errno, std::generic_category(), "poll");
        }
        if (rc > 0) return true;
    }
}

std::string error_text(const proto::Frame& f) {
    v1::Error e;
    if (f.parse(e) && !e.message().empty()) return e.message();
    return "error from tdrd";
}

std::vector<std::string> rest(int argc, char** argv, int from) {
    std::vector<std::string> out;
    for (int i = from; i < argc; ++i) out.emplace_back(argv[i]);
    return out;
}

[[noreturn]] void exec_argv(const std::vector<std::string>& args, int fail_code) {
    std::vector<char*> cargv;
    for (const auto& a : args) cargv.push_back(const_cast<char*>(a.c_str()));
    cargv.push_back(nullptr);
    ::execvp(cargv[0], cargv.data());
    err("exec " + args[0] + ": " + std::strerror(errno));
    std::_Exit(fail_code);
}

// ---------------------------------------------------------------- member

int run_member(const std::vector<std::string>& args) {
    unsigned long timeout = 120;
    if (auto e = env_or("FIREFORGE_LAUNCH_TIMEOUT"); !e.empty()) {
        try {
            timeout = std::stoul(e);
        } catch (...) {
            err("bad FIREFORGE_LAUNCH_TIMEOUT: " + e);
            return kExitUsage;
        }
    }
    for (std::size_t i = 0; i < args.size(); ++i) {
        if (args[i] == "--timeout" && i + 1 < args.size()) {
            try {
                timeout = std::stoul(args[++i]);
            } catch (...) {
                err("bad --timeout: " + args[i]);
                return kExitUsage;
            }
        } else {
            err("member: unexpected argument " + args[i]);
            return kExitUsage;
        }
    }

    std::string error;
    int fd = connect_worker_socket(error);
    if (fd < 0) {
        err(error);
        return kExitError;
    }
    auto deadline = Clock::now() + std::chrono::seconds(timeout);
    try {
        v1::WorkerHello hello;
        hello.set_attempt_id(env_or("FIREFORGE_ATTEMPT_ID"));
        hello.set_token(env_or("FIREFORGE_ATTEMPT_TOKEN"));
        hello.set_pid(static_cast<std::uint32_t>(::getpid()));
        hello.set_sdk_version("tdr-launch");
        send_msg(fd, v1::WORKER_HELLO, hello);

        bool have_task = false;
        for (;;) {
            if (!wait_readable(fd, deadline)) {
                err("no launch within " + std::to_string(timeout) + "s");
                return kExitTimeout;
            }
            auto f = proto::read_frame(fd);
            if (!f) {
                err("connection to tdrd closed");
                return kExitError;
            }
            switch (f->type()) {
                case v1::ERROR:
                    err(error_text(*f));
                    return kExitError;
                case v1::WORKER_TASK:
                    have_task = true;
                    break;
                case v1::WORKER_CANCEL:
                    err("cancelled");
                    return kExitCancelled;
                case v1::WORKER_EXEC: {
                    if (!have_task) {
                        err("WORKER_EXEC before WORKER_TASK");
                        return kExitError;
                    }
                    v1::WorkerExec ex;
                    if (!f->parse(ex)) {
                        err("bad WORKER_EXEC payload");
                        return kExitError;
                    }
                    const char* sh_argv[] = {"sh", "-c", ex.command().c_str(), nullptr};
                    ::execv("/bin/sh", const_cast<char* const*>(sh_argv));
                    err(std::string("exec /bin/sh: ") + std::strerror(errno));
                    return kExitError;
                }
                default:
                    break;  // ignore anything else
            }
        }
    } catch (const std::exception& e) {
        err(e.what());
        return kExitError;
    }
}

// ---------------------------------------------------------------- agent

extern "C" void agent_signal(int) { ::_exit(kExitCancelled); }  // exit closes the socket

int run_agent(const std::vector<std::string>& args) {
    struct sigaction sa{};
    sa.sa_handler = agent_signal;
    sigemptyset(&sa.sa_mask);
    for (int s : {SIGTERM, SIGINT, SIGHUP}) ::sigaction(s, &sa, nullptr);

    std::size_t i = 0;
    while (i < args.size() && !args[i].empty() && args[i][0] == '-') ++i;  // rsh-style options
    if (i + 1 >= args.size()) {
        err("usage: tdr-launch agent <host> <command words...>");
        return kExitAgentFail;
    }
    const std::string host = args[i];
    std::vector<std::string> words(args.begin() + static_cast<std::ptrdiff_t>(i) + 1, args.end());

    std::string error;
    int fd = connect_worker_socket(error);
    if (fd < 0) {
        err(error);
        return kExitAgentFail;
    }
    try {
        v1::GangLaunchRequest req;
        req.set_attempt_id(env_or("FIREFORGE_ATTEMPT_ID"));
        req.set_token(env_or("FIREFORGE_ATTEMPT_TOKEN"));
        req.set_host(host);
        req.set_command(tdr::launch::agent_command(words));
        send_msg(fd, v1::WORKER_GANG_LAUNCH, req);
        for (;;) {
            auto f = proto::read_frame(fd);
            if (!f) {
                err("connection to tdrd closed (launch on " + host + ")");
                return kExitAgentFail;
            }
            switch (f->type()) {
                case v1::ERROR:
                    err("launch on " + host + ": " + error_text(*f));
                    return kExitAgentFail;
                case v1::WORKER_GANG_EXIT: {
                    v1::GangExitNotice n;
                    if (!f->parse(n)) {
                        err("bad WORKER_GANG_EXIT payload");
                        return kExitAgentFail;
                    }
                    if (!n.error().empty()) err(host + ": " + n.error());
                    int code = n.exit_code();
                    if (code < 0 || code > 255) code = 255;
                    return code;
                }
                default:
                    break;  // WORKER_ACK (started) and anything else
            }
        }
    } catch (const std::exception& e) {
        err(e.what());
        return kExitAgentFail;
    }
}

// ---------------------------------------------------------------- head / rank

std::vector<std::string> after_dashes(const std::vector<std::string>& args, std::size_t from) {
    if (from < args.size() && args[from] == "--") ++from;
    return {args.begin() + static_cast<std::ptrdiff_t>(from), args.end()};
}

int run_head(const std::vector<std::string>& args) {
    std::string gang_path;
    std::size_t i = 0;
    for (; i < args.size() && args[i] != "--"; ++i) {
        if (args[i] == "--gang" && i + 1 < args.size()) {
            gang_path = args[++i];
        } else {
            err("head: unexpected argument " + args[i]);
            return kExitError;
        }
    }
    auto program = after_dashes(args, i);
    if (gang_path.empty() || program.empty()) {
        err("usage: tdr-launch head --gang FILE -- argv...");
        return kExitError;
    }
    auto g = tdr::launch::read_gang_file(gang_path);
    if (!g) {
        err(g.error());
        return kExitError;
    }
    std::string workdir = env_or("FIREFORGE_WORKDIR");
    if (workdir.empty()) {
        err("FIREFORGE_WORKDIR is not set");
        return kExitError;
    }
    std::error_code ec;
    fs::path self = fs::canonical("/proc/self/exe", ec);
    if (ec) {
        err("cannot resolve /proc/self/exe: " + ec.message());
        return kExitError;
    }
    fs::path dir = fs::path(workdir) / "mpi";
    fs::create_directories(dir, ec);
    if (ec) {
        err("mkdir " + dir.string() + ": " + ec.message());
        return kExitError;
    }
    fs::path hf = dir / "hostfile";
    {
        std::ofstream out(hf, std::ios::binary | std::ios::trunc);
        out << tdr::launch::hostfile(*g);
        out.close();
        if (!out) {
            err("cannot write " + hf.string());
            return kExitError;
        }
    }
    // hwloc's GL component probes X displays (":0" even without DISPLAY) and prints
    // "Authorization required" noise; keep a user's explicit choice.
    ::unsetenv("DISPLAY");
    ::setenv("HWLOC_COMPONENTS", "-gl", 0);
    exec_argv(tdr::launch::mpirun_argv(*g, hf, self, program), kExitError);
}

int run_rank(const std::vector<std::string>& args) {
    auto program = after_dashes(args, 0);
    if (program.empty()) {
        err("usage: tdr-launch rank -- argv...");
        return kExitError;
    }
    std::string workdir = env_or("FIREFORGE_WORKDIR");
    if (workdir.empty()) {
        err("FIREFORGE_WORKDIR is not set");
        return kExitError;
    }
    if (::chdir(workdir.c_str()) != 0) {
        err("chdir " + workdir + ": " + std::strerror(errno));
        return kExitError;
    }
    exec_argv(program, kExitError);
}

}  // namespace

int main(int argc, char** argv) {
    if (argc < 2) {
        err("usage: tdr-launch head|agent|member|rank ...");
        return kExitUsage;
    }
    std::string cmd = argv[1];
    auto args = rest(argc, argv, 2);
    if (cmd == "member") return run_member(args);
    if (cmd == "agent") return run_agent(args);
    if (cmd == "head") return run_head(args);
    if (cmd == "rank") return run_rank(args);
    err("unknown command " + cmd);
    return kExitUsage;
}
