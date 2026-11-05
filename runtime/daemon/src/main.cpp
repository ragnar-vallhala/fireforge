// tdrd — FireForge TDR daemon (Phase 1: standalone local execution).
//
// Wiring only: config -> supervisor (first, it may move this process into a
// cgroup) -> journal -> artifact store -> JobManager -> two Unix sockets.
// Everything runs on one io_context thread.

#include <csignal>
#include <cstdio>
#include <filesystem>
#include <iostream>
#include <memory>
#include <string>
#include <unordered_map>

#include <unistd.h>
#include <sys/sysinfo.h>

#include <boost/asio/io_context.hpp>
#include <boost/asio/signal_set.hpp>
#include <boost/program_options.hpp>

#include "fireforge/tdr/v1/local.pb.h"
#include "tdr/artifacts/store.hpp"
#include "tdr/common/common.hpp"
#include "tdr/common/config.hpp"
#include "tdr/ipc/ipc.hpp"
#include "tdr/jobs/jobs.hpp"
#include "tdr/journal/journal.hpp"
#include "tdr/protocol/envelope.hpp"
#include "tdr/supervisor/supervisor.hpp"

namespace asio = boost::asio;
namespace fs = std::filesystem;
namespace po = boost::program_options;
namespace v1 = ::fireforge::tdr::v1;

using tdr::ipc::Connection;
using tdr::protocol::Frame;
using tdr::protocol::make_frame;
using tdr::protocol::next_message_id;

namespace {

constexpr std::uint64_t kMiB = 1024ull * 1024ull;
constexpr std::size_t kMaxSocketPath = 107;  // sizeof(sockaddr_un::sun_path) - 1

void reply_error(const std::shared_ptr<Connection>& c, const Frame& req, v1::ErrorCode code,
                 const std::string& message) {
    v1::Error e;
    e.set_code(code);
    e.set_message(message);
    c->send(make_frame(v1::ERROR, e, next_message_id(), req.header.message_id));
}

template <class Msg>
void reply(const std::shared_ptr<Connection>& c, const Frame& req, v1::MessageType type, const Msg& msg) {
    c->send(make_frame(type, msg, next_message_id(), req.header.message_id));
}

std::string hostname() {
    char buf[256] = {};
    if (gethostname(buf, sizeof buf - 1) != 0) return "localhost";
    return buf;
}

struct Resolved {
    tdr::jobs::Capacity capacity;
    std::uint64_t default_job_memory = 0;
    tdr::supervisor::SupervisorOptions::Cgroups cgroups{};
};

std::expected<Resolved, std::string> resolve(const tdr::Config& cfg) {
    Resolved r;
    long cpus = sysconf(_SC_NPROCESSORS_ONLN);
    r.capacity.cpu_cores = cfg.resources.max_cpu_cores ? cfg.resources.max_cpu_cores
                                                       : static_cast<std::uint32_t>(cpus > 0 ? cpus : 1);
    if (cfg.resources.max_memory_mb) {
        r.capacity.memory_bytes = cfg.resources.max_memory_mb * kMiB;
    } else {
        struct sysinfo si {};
        sysinfo(&si);
        r.capacity.memory_bytes = static_cast<std::uint64_t>(si.totalram) * si.mem_unit / 10 * 8;
    }
    r.capacity.jobs = cfg.resources.max_jobs ? cfg.resources.max_jobs : r.capacity.cpu_cores;
    r.default_job_memory = cfg.resources.default_job_memory_mb * kMiB;

    const auto& cg = cfg.resources.cgroups;
    using C = tdr::supervisor::SupervisorOptions::Cgroups;
    if (cg == "auto") r.cgroups = C::kAuto;
    else if (cg == "required") r.cgroups = C::kRequired;
    else if (cg == "off") r.cgroups = C::kOff;
    else return std::unexpected("resources.cgroups must be auto|required|off, got '" + cg + "'");
    return r;
}

class Daemon {
public:
    Daemon(tdr::Config cfg, Resolved res) : cfg_(std::move(cfg)), res_(res) {}

    int run() {
        started_ms_ = tdr::unix_ms();
        for (const auto& p : {cfg_.journal_path().parent_path(), cfg_.artifacts_dir(), cfg_.work_dir(), cfg_.run_dir()})
            fs::create_directories(p);
        for (const auto& s : {cfg_.ctl_socket(), cfg_.worker_socket()}) {
            if (s.native().size() > kMaxSocketPath)
                throw std::runtime_error("socket path too long (max 107 bytes): " + s.native());
        }

        // Supervisor first: with cgroups it moves this process into <root>/daemon.
        tdr::supervisor::SupervisorOptions sopts;
        sopts.cgroups = res_.cgroups;
        supervisor_ = tdr::supervisor::make_linux_supervisor(io_, sopts);
        journal_ = tdr::journal::open_sqlite_journal(cfg_.journal_path());
        store_ = tdr::artifacts::open_local_store(cfg_.artifacts_dir());

        tdr::jobs::ManagerOptions mopts;
        mopts.node_name = node_name();
        mopts.capacity = res_.capacity;
        mopts.default_job_memory_bytes = res_.default_job_memory;
        mopts.work_root = cfg_.work_dir();
        mopts.worker_socket = cfg_.worker_socket();
        manager_ = std::make_unique<tdr::jobs::JobManager>(io_, *journal_, *store_, *supervisor_, mopts);
        manager_->set_cancel_notifier([this](const std::string& attempt, const std::string& reason) {
            notify_cancel(attempt, reason);
        });
        manager_->set_state_listener([](const std::string& job, v1::JobState s) {
            tdr::log_debug("job {} -> {}", job, tdr::jobs::to_string(s));
        });

        worker_server_ = std::make_unique<tdr::ipc::UnixServer>(
            io_, cfg_.worker_socket(),
            tdr::ipc::Handlers{[this](auto& c, Frame f) { on_worker_frame(c, std::move(f)); },
                               [this](auto& c) { on_worker_close(c); }});
        ctl_server_ = std::make_unique<tdr::ipc::UnixServer>(
            io_, cfg_.ctl_socket(),
            tdr::ipc::Handlers{[this](auto& c, Frame f) { on_ctl_frame(c, std::move(f)); }, {}});
        worker_server_->start();
        ctl_server_->start();

        manager_->recover();

        asio::signal_set signals(io_, SIGINT, SIGTERM);
        signals.async_wait([this](const boost::system::error_code& ec, int sig) {
            if (ec) return;
            tdr::log_info("signal {}: shutting down (running jobs are recovered on next start)", sig);
            ctl_server_->stop();
            worker_server_->stop();
            io_.stop();
        });

        tdr::log_info("tdrd {} node={} cpus={} memory={}MiB max_jobs={} cgroups={} state_dir={}", TDR_VERSION,
                      node_name(), res_.capacity.cpu_cores, res_.capacity.memory_bytes / kMiB, res_.capacity.jobs,
                      supervisor_->cgroups_enabled() ? "on" : "off", cfg_.paths.state_dir.string());
        io_.run();

        // Tear down in reverse order of construction; servers first so no
        // callback reaches a destroyed manager.
        ctl_server_.reset();
        worker_server_.reset();
        manager_.reset();
        return 0;
    }

private:
    std::string node_name() const { return cfg_.node.name.empty() ? hostname() : cfg_.node.name; }

    // ---- worker socket ----------------------------------------------------
    void on_worker_frame(const std::shared_ptr<Connection>& c, Frame f) {
        switch (f.type()) {
        case v1::WORKER_HELLO: {
            v1::WorkerHello hello;
            if (!f.parse(hello)) return reply_error(c, f, v1::BAD_REQUEST, "bad WorkerHello");
            auto task = manager_->on_worker_hello(hello);
            if (!task) return reply_error(c, f, v1::UNAUTHORIZED, task.error());
            attempt_by_conn_[c->id()] = hello.attempt_id();
            conn_by_attempt_[hello.attempt_id()] = c;
            return reply(c, f, v1::WORKER_TASK, *task);
        }
        case v1::WORKER_PROGRESS: {
            v1::WorkerProgress p;
            if (!f.parse(p) || !owns(c, p.attempt_id())) return;
            manager_->on_worker_progress(p);
            return;
        }
        case v1::WORKER_RESULT: {
            v1::WorkerResult r;
            if (!f.parse(r)) return reply_error(c, f, v1::BAD_REQUEST, "bad WorkerResult");
            if (!owns(c, r.attempt_id())) return reply_error(c, f, v1::UNAUTHORIZED, "attempt not bound to this connection");
            if (auto ok = manager_->on_worker_result(r); !ok) return reply_error(c, f, v1::CONFLICT, ok.error());
            return reply(c, f, v1::WORKER_ACK, v1::Ack{});
        }
        default:
            return reply_error(c, f, v1::UNSUPPORTED, "unsupported message type on worker socket");
        }
    }

    // Only the connection that completed HELLO for an attempt may speak for it.
    bool owns(const std::shared_ptr<Connection>& c, const std::string& attempt) const {
        auto it = attempt_by_conn_.find(c->id());
        return it != attempt_by_conn_.end() && it->second == attempt;
    }

    void on_worker_close(const std::shared_ptr<Connection>& c) {
        auto it = attempt_by_conn_.find(c->id());
        if (it == attempt_by_conn_.end()) return;
        auto ci = conn_by_attempt_.find(it->second);
        if (ci != conn_by_attempt_.end() && ci->second.lock() == c) conn_by_attempt_.erase(ci);
        attempt_by_conn_.erase(it);
    }

    void notify_cancel(const std::string& attempt, const std::string& reason) {
        auto it = conn_by_attempt_.find(attempt);
        if (it == conn_by_attempt_.end()) return;
        if (auto c = it->second.lock(); c && c->is_open()) {
            v1::WorkerCancel msg;
            msg.set_attempt_id(attempt);
            msg.set_reason(reason);
            c->send(make_frame(v1::WORKER_CANCEL, msg, next_message_id()));
        }
    }

    // ---- control socket ---------------------------------------------------
    void on_ctl_frame(const std::shared_ptr<Connection>& c, Frame f) {
        switch (f.type()) {
        case v1::CTL_SUBMIT: {
            v1::SubmitJobRequest req;
            if (!f.parse(req)) return reply_error(c, f, v1::BAD_REQUEST, "bad SubmitJobRequest");
            auto id = manager_->submit(req.spec());
            if (!id) return reply_error(c, f, v1::BAD_REQUEST, id.error());
            v1::SubmitJobReply rep;
            rep.set_job_id(*id);
            return reply(c, f, v1::CTL_SUBMIT_REPLY, rep);
        }
        case v1::CTL_GET_JOB: {
            v1::GetJobRequest req;
            if (!f.parse(req)) return reply_error(c, f, v1::BAD_REQUEST, "bad GetJobRequest");
            auto st = manager_->status(req.job_id());
            if (!st) return reply_error(c, f, v1::NOT_FOUND, "no such job: " + req.job_id());
            return reply(c, f, v1::CTL_JOB_INFO, *st);
        }
        case v1::CTL_LIST_JOBS: {
            v1::ListJobsRequest req;
            if (!f.parse(req)) return reply_error(c, f, v1::BAD_REQUEST, "bad ListJobsRequest");
            v1::JobList list;
            for (auto& s : manager_->list(req.state(), req.limit() ? req.limit() : 100)) *list.add_jobs() = std::move(s);
            return reply(c, f, v1::CTL_JOB_LIST, list);
        }
        case v1::CTL_CANCEL: {
            v1::CancelJobRequest req;
            if (!f.parse(req)) return reply_error(c, f, v1::BAD_REQUEST, "bad CancelJobRequest");
            if (!manager_->status(req.job_id())) return reply_error(c, f, v1::NOT_FOUND, "no such job: " + req.job_id());
            if (auto ok = manager_->cancel(req.job_id()); !ok) return reply_error(c, f, v1::CONFLICT, ok.error());
            return reply(c, f, v1::CTL_ACK, v1::Ack{});
        }
        case v1::CTL_NODE_STATUS: {
            auto u = manager_->usage();
            v1::NodeStatus st;
            st.set_node_name(node_name());
            st.set_daemon_version(TDR_VERSION);
            auto fill = [](v1::ResourceAmounts* a, const tdr::jobs::Capacity& cap) {
                a->set_cpu_cores(cap.cpu_cores);
                a->set_memory_bytes(cap.memory_bytes);
                a->set_jobs(cap.jobs);
            };
            fill(st.mutable_capacity(), res_.capacity);
            fill(st.mutable_allocated(), u.allocated);
            st.set_running_jobs(u.running);
            st.set_queued_jobs(u.queued);
            st.set_failed_jobs(journal_->failed_job_count());
            st.set_cgroups_enabled(supervisor_->cgroups_enabled());
            st.set_started_unix_ms(started_ms_);
            return reply(c, f, v1::CTL_NODE_STATUS_REPLY, st);
        }
        default:
            return reply_error(c, f, v1::UNSUPPORTED, "unsupported message type on control socket");
        }
    }

    tdr::Config cfg_;
    Resolved res_;
    std::int64_t started_ms_ = 0;
    asio::io_context io_;
    std::unique_ptr<tdr::supervisor::IProcessSupervisor> supervisor_;
    std::unique_ptr<tdr::journal::IJournal> journal_;
    std::unique_ptr<tdr::artifacts::IArtifactStore> store_;
    std::unique_ptr<tdr::jobs::JobManager> manager_;
    std::unique_ptr<tdr::ipc::UnixServer> worker_server_;
    std::unique_ptr<tdr::ipc::UnixServer> ctl_server_;
    std::unordered_map<std::uint64_t, std::string> attempt_by_conn_;
    std::unordered_map<std::string, std::weak_ptr<Connection>> conn_by_attempt_;
};

}  // namespace

int main(int argc, char** argv) {
    std::string config_path, state_dir, log_level;
    po::options_description desc("tdrd options");
    desc.add_options()
        ("help,h", "show help")
        ("version", "print version")
        ("config,c", po::value(&config_path)->default_value("/etc/fireforge/tdr.conf"), "config file")
        ("state-dir", po::value(&state_dir), "override paths.state_dir")
        ("log-level", po::value(&log_level), "override log level (debug|info|warn|error)");
    po::variables_map vm;
    try {
        po::store(po::parse_command_line(argc, argv, desc), vm);
        po::notify(vm);
    } catch (const std::exception& e) {
        std::cerr << "tdrd: " << e.what() << "\n" << desc;
        return 2;
    }
    if (vm.count("help")) { std::cout << desc; return 0; }
    if (vm.count("version")) { std::cout << "tdrd " << TDR_VERSION << "\n"; return 0; }

    std::signal(SIGPIPE, SIG_IGN);
    GOOGLE_PROTOBUF_VERIFY_VERSION;

    try {
        tdr::Config cfg;
        if (fs::exists(config_path)) {
            auto loaded = tdr::load_config(config_path);
            if (!loaded) { std::cerr << "tdrd: " << config_path << ": " << loaded.error() << "\n"; return 2; }
            cfg = *loaded;
        } else if (!vm["config"].defaulted()) {
            std::cerr << "tdrd: config file not found: " << config_path << "\n";
            return 2;
        }
        if (!state_dir.empty()) cfg.paths.state_dir = fs::absolute(state_dir);
        if (!log_level.empty()) cfg.log_level = log_level;
        tdr::set_log_level(tdr::parse_log_level(cfg.log_level));

        auto res = resolve(cfg);
        if (!res) { std::cerr << "tdrd: " << res.error() << "\n"; return 2; }
        return Daemon(std::move(cfg), *res).run();
    } catch (const std::exception& e) {
        tdr::log_error("fatal: {}", e.what());
        return 1;
    }
}
