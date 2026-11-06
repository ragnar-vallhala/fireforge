// tdrd — FireForge TDR daemon (Phase 1 local execution + Phase 2 control-plane agent).
//
// Wiring only: config -> supervisor (first, it may move this process into a
// cgroup) -> journal -> artifact store -> JobManager -> two Unix sockets
// -> NodeAgent (only when [control] address is set; phase2.md §7)
// -> peer listener + PeerManager, started on the first directory (phase3.md)
// -> RemoteExecutor: leases from the control plane run as local jobs (phase4.md).
// Everything runs on one io_context thread.

#include <csignal>
#include <cstdio>
#include <fstream>
#include <filesystem>
#include <iostream>
#include <expected>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>

#include <unistd.h>
#include <sys/statvfs.h>
#include <sys/sysinfo.h>

#include <boost/asio/io_context.hpp>
#include <boost/asio/signal_set.hpp>
#include <boost/program_options.hpp>

#include "fireforge/tdr/v1/local.pb.h"
#include "tdr/agent/agent.hpp"
#include "tdr/peer/manager.hpp"
#include "tdr/peer/transport.hpp"
#include "tdr/artifacts/store.hpp"
#include "tdr/common/common.hpp"
#include "tdr/common/config.hpp"
#include "tdr/ipc/ipc.hpp"
#include "tdr/jobs/jobs.hpp"
#include "tdr/journal/journal.hpp"
#include "tdr/protocol/envelope.hpp"
#include "tdr/remote/executor.hpp"
#include "tdr/supervisor/supervisor.hpp"
#include "tdr/tls/tls.hpp"

#include "job_user.hpp"

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

        std::optional<tdrd::JobUser> job_user;
        if (!cfg_.resources.job_user.empty()) job_user = setup_job_user();

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
        if (job_user) mopts.run_as = tdr::supervisor::RunAs{job_user->uid, job_user->gid, {job_user->gid}};
        manager_ = std::make_unique<tdr::jobs::JobManager>(io_, *journal_, *store_, *supervisor_, mopts);
        manager_->set_cancel_notifier([this](const std::string& attempt, const std::string& reason) {
            notify_cancel(attempt, reason);
        });
        manager_->set_state_listener([this](const std::string& job, v1::JobState s) {
            tdr::log_debug("job {} -> {}", job, tdr::jobs::to_string(s));
            if (remote_) remote_->on_job_state(job, s);
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
        if (job_user) {
            // Jobs may connect to the worker socket, never to the control socket.
            check(tdrd::set_group_mode(cfg_.worker_socket(), job_user->gid, 0660));
            check(tdrd::set_mode(cfg_.ctl_socket(), 0660));
        }

        manager_->recover();
        if (!cfg_.control.address.empty()) start_agent(job_user.has_value());

        asio::signal_set signals(io_, SIGINT, SIGTERM);
        std::function<void(const boost::system::error_code&, int)> on_signal =
            [this, &signals, &on_signal](const boost::system::error_code& ec, int sig) {
            if (ec) return;
            auto finish = [this] {
                ctl_server_->stop();
                worker_server_->stop();
                io_.stop();
            };
            if (stopping_ || !agent_) {
                tdr::log_info("signal {}: shutting down (running jobs are recovered on next start)", sig);
                return finish();
            }
            // Tell the control plane we're going away (bounded), then stop.
            // A second signal stops immediately.
            stopping_ = true;
            tdr::log_info("signal {}: going offline, then shutting down (running jobs are recovered on next start)", sig);
            if (peers_) peers_->stop();
            if (remote_) remote_->stop();
            signals.async_wait(on_signal);
            agent_->stop(finish);
        };
        signals.async_wait(on_signal);

        tdr::log_info("tdrd {} node={} cpus={} memory={}MiB max_jobs={} cgroups={} state_dir={}", TDR_VERSION,
                      node_name(), res_.capacity.cpu_cores, res_.capacity.memory_bytes / kMiB, res_.capacity.jobs,
                      supervisor_->cgroups_enabled() ? "on" : "off", cfg_.paths.state_dir.string());
        if (job_user) tdr::log_info("jobs run as user {} (uid {}, gid {})", job_user->name, job_user->uid, job_user->gid);
        io_.run();

        // Tear down in reverse order of construction; servers first so no
        // callback reaches a destroyed manager.
        ctl_server_.reset();
        worker_server_.reset();
        peers_.reset();
        remote_.reset();
        if (transport_) transport_->stop();
        transport_.reset();
        agent_.reset();
        control_client_.reset();
        manager_.reset();
        return 0;
    }

private:
    static void check(std::expected<void, std::string> r) {
        if (!r) throw std::runtime_error(r.error());
    }

    // phase1.md §11: validate the job user, join its group, and lock down
    // state_dir so the job user can reach only work/ and the worker socket.
    tdrd::JobUser setup_job_user() {
        auto u = tdrd::resolve_job_user(cfg_.resources.job_user);
        if (!u) throw std::runtime_error(u.error());
        check(tdrd::join_group(u->gid));
        check(tdrd::set_mode(cfg_.work_dir(), 0711));
        check(tdrd::set_mode(cfg_.run_dir(), 0711));
        check(tdrd::set_mode(cfg_.artifacts_dir(), 0700));
        check(tdrd::set_mode(cfg_.journal_path().parent_path(), 0700));
        check(tdrd::check_traversable(cfg_.work_dir(), *u));
        check(tdrd::check_traversable(cfg_.run_dir(), *u));
        return *u;
    }

    std::string node_name() const { return cfg_.node.name.empty() ? hostname() : cfg_.node.name; }

    // ---- control plane (phase2.md §7) --------------------------------------
    void start_agent(bool has_job_user) {
        if (cfg_.control.scheme != "https")
            throw std::runtime_error("control.scheme must be https, got '" + cfg_.control.scheme + "'");
        auto ctx = tdr::tls::make_client_context({cfg_.tls.ca_file, cfg_.tls.cert_file, cfg_.tls.key_file});
        auto ident = tdr::tls::read_identity(cfg_.tls.cert_file);
        if (!ident) throw std::runtime_error(cfg_.tls.cert_file.string() + ": " + ident.error());
        if (ident->role != tdr::tls::Role::kNode)
            throw std::runtime_error(cfg_.tls.cert_file.string() + ": not a node certificate");
        if (ident->name != node_name())
            throw std::runtime_error(std::format("node certificate is for '{}' but node.name is '{}'", ident->name,
                                                 node_name()));

        tdr::http::ClientOptions copts;
        copts.host = cfg_.control.address;
        copts.port = cfg_.control.port;
        copts.verify_host = cfg_.control.address;
        control_url_ = std::format("https://{}:{}", cfg_.control.address, cfg_.control.port);
        control_client_ = tdr::agent::make_https_control_client(io_, ctx, copts);

        tdr::agent::AgentOptions aopts;
        auto& reg = aopts.registration;
        reg.set_name(node_name());
        reg.set_daemon_version(TDR_VERSION);
        for (auto& a : advertised_addresses()) *reg.add_addresses() = std::move(a);
        *reg.mutable_physical() = tdr::agent::detect_physical(cfg_.paths.state_dir);
        auto* conf = reg.mutable_configured();
        conf->set_cpu_cores(res_.capacity.cpu_cores);
        conf->set_memory_bytes(res_.capacity.memory_bytes);
        conf->set_disk_bytes(cfg_.resources.max_disk_mb * kMiB);
        conf->set_gpu_count(reg.physical().gpu_count());
        reg.add_capabilities("cpu");
        if (reg.physical().gpu_count() > 0) reg.add_capabilities("gpu:nvidia");
        if (supervisor_->cgroups_enabled()) reg.add_capabilities("cgroups");
        if (has_job_user) reg.add_capabilities("job-user");
        aopts.local_capacity = res_.capacity;
        aopts.policy_cache = cfg_.policy_cache();
        aopts.peer_refresh = std::chrono::seconds(10);
        peer_creds_ = {cfg_.tls.ca_file, cfg_.tls.cert_file, cfg_.tls.key_file};

        tdr::agent::AgentHooks hooks;
        hooks.apply_capacity = [this](const tdr::jobs::Capacity& cap) {
            auto old = manager_->capacity();
            manager_->set_capacity(cap);
            if (old.cpu_cores != cap.cpu_cores || old.memory_bytes != cap.memory_bytes || old.jobs != cap.jobs)
                tdr::log_info("capacity now cpus={} memory={}MiB max_jobs={}", cap.cpu_cores, cap.memory_bytes / kMiB,
                              cap.jobs);
        };
        hooks.fill_stats = [this](tdr::agent::cv1::Heartbeat& hb) { fill_heartbeat(hb); };
        hooks.on_directory = [this](const tdr::agent::cv1::NodeList& nodes) { on_directory(nodes); };
        hooks.on_registered = [this](const std::string& node_id) {
            if (remote_ && !stopping_) remote_->on_registered(node_id);
        };
        hooks.on_revoked = [this](const std::vector<tdr::agent::cv1::LeaseRef>& revoked) {
            if (remote_) remote_->on_revoked(revoked);
        };

        if (cfg_.control.remote_jobs != "on" && cfg_.control.remote_jobs != "off")
            throw std::runtime_error("control.remote_jobs must be on|off, got '" + cfg_.control.remote_jobs + "'");
        tdr::remote::RemoteOptions ropts;
        ropts.state_file = cfg_.remote_state();
        ropts.enabled = cfg_.control.remote_jobs == "on";
        ropts.poll_interval = std::chrono::seconds(std::max(1u, cfg_.control.lease_poll));
        ropts.poll_idle = std::max(ropts.poll_interval, std::chrono::milliseconds(
                                                             std::chrono::seconds(cfg_.control.lease_poll_idle)));
        ropts.default_job_memory_bytes = res_.default_job_memory;
        tdr::remote::RemoteHooks rhooks;
        rhooks.connected = [this] { return agent_ && agent_->state() == tdr::agent::ControlState::kConnected; };
        rhooks.list_local_artifacts = [this] { return local_artifacts(); };
        rhooks.artifact_meta = [this](const std::string& id) { return artifact_ref(id); };
        remote_ = std::make_unique<tdr::remote::RemoteExecutor>(io_, *control_client_, *manager_, std::move(ropts),
                                                                std::move(rhooks));
        remote_->start();  // after JobManager::recover(): reconciles leases with local jobs

        agent_ = std::make_unique<tdr::agent::NodeAgent>(io_, *control_client_, std::move(aopts), std::move(hooks));
        agent_->start();
        tdr::log_info("control plane {} (node cert '{}')", control_url_, ident->name);
    }

    // ---- peers (phase3.md) ---------------------------------------------------
    void on_directory(const tdr::agent::cv1::NodeList& nodes) {
        if (stopping_) return;
        if (!peers_) {
            if (agent_->node_id().empty()) return;
            tdr::peer::TransportOptions topts;
            topts.listen_address = cfg_.network.listen_address;
            topts.listen_port = cfg_.network.listen_port;
            transport_ = std::make_unique<tdr::peer::TlsTransport>(io_, tdr::tls::make_peer_context(peer_creds_), topts);

            tdr::peer::PeerOptions popts;
            popts.self_node_id = agent_->node_id();
            popts.self_name = node_name();
            popts.daemon_version = TDR_VERSION;
            popts.ping_interval = std::chrono::seconds(std::max(1u, cfg_.network.peer_ping_interval));
            popts.ping_timeout = std::chrono::seconds(
                std::max(cfg_.network.peer_ping_interval + 1, cfg_.network.peer_ping_timeout));
            peers_ = std::make_unique<tdr::peer::PeerManager>(io_, *transport_, popts);
            peers_->set_directory_refresh([this] { if (agent_) agent_->refresh_peers(); });
            peers_->set_state_listener([this](const std::string& id, tdr::peer::PeerState st) {
                tdr::log_info("peer {} {}", peer_label(id), tdr::peer::to_string(st));
            });
            peer_self_id_ = agent_->node_id();
            peers_->start();
            transport_->start();  // handlers are installed; now accept
            tdr::log_info("peer listener on {}:{} (node {})", topts.listen_address, transport_->bound_port(),
                          agent_->node_id());
        } else if (agent_->node_id() != peer_self_id_) {
            tdr::log_warn("control plane assigned a new node id {}; peer links keep the old id until restart",
                          agent_->node_id());
        }
        peers_->update_directory(nodes);
    }

    std::string peer_label(const std::string& id) const {
        for (const auto& n : agent_->peers().nodes())
            if (n.node_id() == id) return n.name() + " (" + id + ")";
        return id;
    }

    // node.advertise_address: "auto" (detect), one address, or an explicit
    // list "type=addr[:port], ..." (e.g. "lan=192.168.1.5, tailscale=100.64.0.3:7400").
    std::vector<tdr::agent::cv1::NodeAddress> advertised_addresses() const {
        const auto& adv = cfg_.node.advertise_address;
        if (adv.find('=') == std::string::npos)
            return tdr::agent::detect_addresses(cfg_.network.listen_port, cfg_.network.tailscale, adv);
        std::vector<tdr::agent::cv1::NodeAddress> out;
        std::size_t pos = 0;
        while (pos <= adv.size()) {
            auto end = adv.find(',', pos);
            if (end == std::string::npos) end = adv.size();
            std::string item = adv.substr(pos, end - pos);
            pos = end + 1;
            auto trim = [](std::string v) {
                auto b = v.find_first_not_of(" \t"), e = v.find_last_not_of(" \t");
                return b == std::string::npos ? std::string{} : v.substr(b, e - b + 1);
            };
            item = trim(item);
            if (item.empty()) continue;
            auto eq = item.find('=');
            if (eq == std::string::npos || eq == 0)
                throw std::runtime_error("node.advertise_address: expected type=addr[:port], got '" + item + "'");
            tdr::agent::cv1::NodeAddress a;
            a.set_type(trim(item.substr(0, eq)));
            std::string hp = trim(item.substr(eq + 1));
            std::uint32_t port = cfg_.network.listen_port;
            std::string host = hp;
            if (hp.starts_with('[')) {  // [v6]:port
                auto close = hp.find(']');
                if (close == std::string::npos) throw std::runtime_error("node.advertise_address: bad IPv6 '" + hp + "'");
                host = hp.substr(1, close - 1);
                if (close + 1 < hp.size() && hp[close + 1] == ':') port = static_cast<std::uint32_t>(std::stoul(hp.substr(close + 2)));
            } else if (auto c = hp.rfind(':'); c != std::string::npos && hp.find(':') == c) {
                host = hp.substr(0, c);
                port = static_cast<std::uint32_t>(std::stoul(hp.substr(c + 1)));
            }
            if (host.empty() || port == 0 || port > 65535)
                throw std::runtime_error("node.advertise_address: bad address '" + hp + "'");
            a.set_address(host);
            a.set_port(port);
            out.push_back(std::move(a));
        }
        return out;
    }

    // ---- remote jobs (phase4.md) -------------------------------------------------
    std::optional<tdr::agent::cv1::ArtifactRef> artifact_ref(const std::string& id) const {
        auto row = journal_->get_artifact(id);
        if (!row) {
            auto p = store_->path_of(id);
            if (!p) return std::nullopt;
            tdr::agent::cv1::ArtifactRef r;
            r.set_artifact_id(id);
            std::error_code ec;
            auto size = fs::file_size(*p, ec);
            if (!ec) r.set_size(size);
            return r;
        }
        tdr::agent::cv1::ArtifactRef r;
        r.set_artifact_id(row->id);
        r.set_size(row->size);
        r.set_media_type(row->media_type);
        r.set_producer_job(row->producer_job);
        r.set_created_unix_ms(row->created_unix_ms);
        return r;
    }

    // Everything in the local store: <root>/sha256/<2 hex>/<64 hex>.
    std::vector<tdr::agent::cv1::ArtifactRef> local_artifacts() const {
        std::vector<tdr::agent::cv1::ArtifactRef> out;
        std::error_code ec;
        for (fs::recursive_directory_iterator it(store_->root() / "sha256", ec), end; !ec && it != end; it.increment(ec)) {
            if (!it->is_regular_file(ec)) continue;
            auto id = tdr::artifacts::make_artifact_id(it->path().filename().string());
            if (!tdr::artifacts::parse_artifact_id(id)) continue;
            if (auto r = artifact_ref(id)) out.push_back(std::move(*r));
        }
        return out;
    }

    // Central job id of a local job that ran a lease. The executor knows live
    // leases; finished ones are recognised by the "<central id>.a<N>" local id.
    std::optional<std::string> central_of(const std::string& local_id) const {
        if (remote_)
            if (auto c = remote_->central_job_id(local_id)) return c;
        if (!agent_) return std::nullopt;
        auto dot = local_id.rfind(".a");
        if (dot == std::string::npos || dot == 0 || dot + 2 >= local_id.size()) return std::nullopt;
        for (auto i = dot + 2; i < local_id.size(); ++i)
            if (local_id[i] < '0' || local_id[i] > '9') return std::nullopt;
        return local_id.substr(0, dot);
    }

    // tdrctl --remote: forward to the control plane with this node's identity.
    static v1::ErrorCode error_code_of(const tdr::agent::ClientError& e) {
        switch (e.http_status) {
            case 400: return v1::BAD_REQUEST;
            case 403: return v1::UNAUTHORIZED;
            case 404: return v1::NOT_FOUND;
            case 409: return v1::CONFLICT;
            default: return v1::INTERNAL;
        }
    }
    static std::string error_text(const tdr::agent::ClientError& e) {
        if (e.http_status == 0) return "control plane unreachable: " + e.message;
        return e.message.empty() ? "control plane error " + std::to_string(e.http_status) : e.message;
    }
    static v1::JobStatus to_job_status(const tdr::agent::cv1::Job& j) {
        v1::JobStatus st;
        st.set_job_id(j.job_id());
        st.set_state(j.state());
        *st.mutable_spec() = j.spec();
        st.set_attempts_used(j.attempts_used());
        st.set_current_attempt_id(j.current_attempt_id());
        st.set_last_error(j.last_error());
        for (const auto& o : j.outputs()) st.add_output_artifacts(o.artifact_id());
        st.set_created_unix_ms(j.created_unix_ms());
        st.set_updated_unix_ms(j.updated_unix_ms());
        st.set_progress_fraction(j.progress_fraction());
        st.set_progress_message(j.progress_message());
        st.set_node_id(j.node_id());
        return st;
    }

    // Returns false (after replying with an error) when there is no control plane.
    bool require_control(const std::shared_ptr<Connection>& c, const Frame& f) {
        if (agent_) return true;
        reply_error(c, f, v1::BAD_REQUEST, "no control plane configured ([control] address); remote jobs unavailable");
        return false;
    }

    void on_remote_ctl(const std::shared_ptr<Connection>& c, Frame f) {
        if (!require_control(c, f)) return;
        auto req = std::make_shared<Frame>(std::move(f));
        auto& client = agent_->client();
        auto fail = [c, req](const tdr::agent::ClientError& e) { reply_error(c, *req, error_code_of(e), error_text(e)); };
        switch (req->type()) {
        case v1::CTL_SUBMIT: {
            v1::SubmitJobRequest r;
            if (!req->parse(r)) return reply_error(c, *req, v1::BAD_REQUEST, "bad SubmitJobRequest");
            tdr::agent::cv1::CreateJobRequest cr;
            *cr.mutable_spec() = r.spec();
            client.submit_job(cr, [c, req, fail](tdr::agent::Result<tdr::agent::cv1::Job> res) {
                if (!res) return fail(res.error());
                v1::SubmitJobReply rep;
                rep.set_job_id(res->job_id());
                reply(c, *req, v1::CTL_SUBMIT_REPLY, rep);
            });
            return;
        }
        case v1::CTL_GET_JOB: {
            v1::GetJobRequest r;
            if (!req->parse(r)) return reply_error(c, *req, v1::BAD_REQUEST, "bad GetJobRequest");
            client.get_job(r.job_id(), [c, req, fail](tdr::agent::Result<tdr::agent::cv1::Job> res) {
                if (!res) return fail(res.error());
                reply(c, *req, v1::CTL_JOB_INFO, to_job_status(*res));
            });
            return;
        }
        case v1::CTL_LIST_JOBS: {
            v1::ListJobsRequest r;
            if (!req->parse(r)) return reply_error(c, *req, v1::BAD_REQUEST, "bad ListJobsRequest");
            client.list_jobs(r.state(), "", r.limit(), [c, req, fail](tdr::agent::Result<tdr::agent::cv1::JobList> res) {
                if (!res) return fail(res.error());
                v1::JobList list;
                for (const auto& j : res->jobs()) *list.add_jobs() = to_job_status(j);
                reply(c, *req, v1::CTL_JOB_LIST, list);
            });
            return;
        }
        case v1::CTL_CANCEL: {
            v1::CancelJobRequest r;
            if (!req->parse(r)) return reply_error(c, *req, v1::BAD_REQUEST, "bad CancelJobRequest");
            client.cancel_job(r.job_id(), "cancelled via tdrctl on " + node_name(),
                              [c, req, fail](tdr::agent::Result<tdr::agent::cv1::Job> res) {
                if (!res) return fail(res.error());
                reply(c, *req, v1::CTL_ACK, v1::Ack{});
            });
            return;
        }
        default:
            return reply_error(c, *req, v1::UNSUPPORTED, "not a remote-capable request");
        }
    }

    // Is this tdrctl request flagged `remote`? (Parsed twice; requests are tiny.)
    static bool is_remote(const Frame& f) {
        switch (f.type()) {
            case v1::CTL_SUBMIT: { v1::SubmitJobRequest r; return f.parse(r) && r.remote(); }
            case v1::CTL_GET_JOB: { v1::GetJobRequest r; return f.parse(r) && r.remote(); }
            case v1::CTL_LIST_JOBS: { v1::ListJobsRequest r; return f.parse(r) && r.remote(); }
            case v1::CTL_CANCEL: { v1::CancelJobRequest r; return f.parse(r) && r.remote(); }
            default: return false;
        }
    }

    void fill_heartbeat(tdr::agent::cv1::Heartbeat& hb) {
        if (remote_)
            for (auto& l : remote_->active_leases()) *hb.add_leases() = std::move(l);
        if (peers_)
            for (auto& p : peers_->snapshot()) *hb.add_peers() = std::move(p);
        auto u = manager_->usage();
        auto cap = manager_->capacity();
        hb.mutable_capacity()->set_cpu_cores(cap.cpu_cores);
        hb.mutable_capacity()->set_memory_bytes(cap.memory_bytes);
        hb.mutable_capacity()->set_disk_bytes(cfg_.resources.max_disk_mb * kMiB);
        hb.mutable_allocated()->set_cpu_cores(u.allocated.cpu_cores);
        hb.mutable_allocated()->set_memory_bytes(u.allocated.memory_bytes);
        hb.set_running_jobs(u.running);
        hb.set_queued_jobs(u.queued);
        hb.set_failed_jobs(journal_->failed_job_count());
        hb.set_cpu_utilization(cpu_sampler_.sample());
        hb.set_memory_available_bytes(mem_available());
        struct statvfs vfs {};
        if (statvfs(cfg_.paths.state_dir.c_str(), &vfs) == 0)
            hb.set_disk_available_bytes(static_cast<std::uint64_t>(vfs.f_bavail) * vfs.f_frsize);
    }

    static std::uint64_t mem_available() {
        std::ifstream in("/proc/meminfo");
        std::string key;
        std::uint64_t kb = 0;
        std::string unit;
        while (in >> key >> kb >> unit) {
            if (key == "MemAvailable:") return kb * 1024;
        }
        return 0;
    }

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
        if (is_remote(f)) return on_remote_ctl(c, std::move(f));
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
            if (auto central = central_of(req.job_id())) st->set_remote_job_id(*central);
            return reply(c, f, v1::CTL_JOB_INFO, *st);
        }
        case v1::CTL_LIST_JOBS: {
            v1::ListJobsRequest req;
            if (!f.parse(req)) return reply_error(c, f, v1::BAD_REQUEST, "bad ListJobsRequest");
            v1::JobList list;
            for (auto& s : manager_->list(req.state(), req.limit() ? req.limit() : 100)) {
                if (auto central = central_of(s.job_id())) s.set_remote_job_id(*central);
                *list.add_jobs() = std::move(s);
            }
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
            fill(st.mutable_capacity(), manager_->capacity());
            fill(st.mutable_allocated(), u.allocated);
            st.set_running_jobs(u.running);
            st.set_queued_jobs(u.queued);
            st.set_failed_jobs(journal_->failed_job_count());
            st.set_cgroups_enabled(supervisor_->cgroups_enabled());
            st.set_started_unix_ms(started_ms_);
            if (agent_) {
                st.set_control_state(tdr::agent::to_string(agent_->state()));
                st.set_node_id(agent_->node_id());
                if (auto pol = agent_->policy()) st.set_policy_version(pol->version());
                st.set_last_heartbeat_ok_unix_ms(agent_->last_heartbeat_ok_ms());
                st.set_control_url(control_url_);
                st.set_last_control_error(agent_->last_error());
            } else {
                st.set_control_state("standalone");
            }
            return reply(c, f, v1::CTL_NODE_STATUS_REPLY, st);
        }
        case v1::CTL_PEERS: {
            v1::PeerList list;
            if (agent_) {
                *list.mutable_peers() = agent_->peers().nodes();
                list.set_refreshed_unix_ms(agent_->peers_refreshed_ms());
            }
            if (peers_) {
                for (auto& p : peers_->snapshot()) *list.add_connections() = std::move(p);
                list.set_listen_port(transport_->bound_port());
            }
            return reply(c, f, v1::CTL_PEERS_REPLY, list);
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
    std::unique_ptr<tdr::agent::IControlClient> control_client_;
    std::unique_ptr<tdr::agent::NodeAgent> agent_;
    tdr::agent::CpuSampler cpu_sampler_;
    std::string control_url_;
    tdr::tls::Credentials peer_creds_;
    std::unique_ptr<tdr::peer::TlsTransport> transport_;
    std::unique_ptr<tdr::peer::PeerManager> peers_;
    std::unique_ptr<tdr::remote::RemoteExecutor> remote_;
    std::string peer_self_id_;  // node id the PeerManager was created with
    bool stopping_ = false;
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
