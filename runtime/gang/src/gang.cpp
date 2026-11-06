// GangService (docs/design/phase7.md §5). Everything runs on the io_context
// thread.
//
// Head side: one Launch per accepted WORKER_GANG_LAUNCH, keyed by launch_id
// (a per-process counter). A launch is "pending" until the member answers
// GANG_STARTED (or launch_timeout fires), then "started" until GANG_EXIT, the
// member link is lost, the head job ends or the agent goes away. Records are
// erased before their callbacks run, so callbacks may call back in.
//
// Member side: one MemberLaunch per member local job that accepted a launch,
// until that job ends (GANG_EXIT to the head). A member job accepts at most one
// launch in this process's lifetime (launched_jobs).
//
// Callbacks are invoked synchronously, including `started` with a validation
// error from inside on_agent_launch.
#include "tdr/gang/gang.hpp"

#include <boost/asio/steady_timer.hpp>
#include <format>
#include <map>
#include <set>
#include <vector>

#include "tdr/common/common.hpp"
#include "tdr/jobs/jobs.hpp"

namespace tdr::gang {

namespace asio = boost::asio;
namespace cv1 = ::fireforge::control::v1;

struct GangService::Impl : std::enable_shared_from_this<Impl> {
    struct Launch {
        std::uint64_t launch_id = 0;
        AgentId agent = 0;
        std::string head_job;     // local job id of the HEAD job
        std::string gang_id;
        std::string member_node;  // where GANG_LAUNCH went
        bool started = false;     // GANG_STARTED ok received
        std::function<void(std::expected<void, std::string>)> on_started;
        std::function<void(const v1::GangExitNotice&)> on_exited;
        std::unique_ptr<asio::steady_timer> timer;
    };
    struct MemberLaunch {
        std::string gang_id;
        std::string head_node;
        std::uint64_t launch_id = 0;
    };

    asio::io_context& io;
    GangOptions opts;
    GangHooks hooks;
    bool stopped = false;
    std::uint64_t next_launch_id = 0;
    std::uint64_t next_message_id = 0;

    std::map<std::uint64_t, std::unique_ptr<Launch>> launches;  // head side, by launch id
    std::map<std::string, MemberLaunch> members;               // member side, by local job id
    std::set<std::string> launched_jobs;                       // member jobs that accepted a launch
    // GANG_LAUNCH that arrived before the member job was ready (lease not yet
    // received, inputs still fetching, process not yet connected), retried until
    // a deadline. By gang id; GANG_KILL drops it.
    struct PendingAccept {
        std::string from;
        protocol::Frame frame;
        v1::GangLaunch msg;
        std::chrono::steady_clock::time_point deadline;
        std::unique_ptr<asio::steady_timer> timer;
    };
    std::map<std::string, PendingAccept> pending_accepts;

    Impl(asio::io_context& io_, GangOptions o, GangHooks h) : io(io_), opts(o), hooks(std::move(h)) {}

    template <class Msg>
    bool send(const std::string& node, v1::MessageType type, const Msg& msg, std::uint64_t correlation = 0) {
        if (!hooks.send_peer) return false;
        return hooks.send_peer(node, protocol::make_frame(type, msg, ++next_message_id, correlation));
    }

    void send_kill(const std::string& node, const std::string& gang_id, const std::string& reason) {
        v1::GangKill k;
        k.set_gang_id(gang_id);
        k.set_reason(reason);
        if (!send(node, v1::GANG_KILL, k)) log_debug("gang {}: GANG_KILL to {} not sent (no link)", gang_id, node);
    }

    std::unique_ptr<Launch> take(std::uint64_t launch_id) {
        auto it = launches.find(launch_id);
        if (it == launches.end()) return nullptr;
        auto l = std::move(it->second);
        launches.erase(it);
        if (l->timer) l->timer->cancel();
        return l;
    }

    // Ends a launch with an error: `started` if still pending, else `exited(-1)`.
    static void fail(Launch& l, const std::string& why) {
        if (!l.started) {
            if (l.on_started) l.on_started(std::unexpected(why));
        } else {
            v1::GangExitNotice n;
            n.set_exit_code(-1);
            n.set_error(why);
            if (l.on_exited) l.on_exited(n);
        }
    }

    // ---- head side ----------------------------------------------------------------

    std::expected<std::pair<std::string, remote::GangLocal>, std::string> validate(
        const v1::GangLaunchRequest& req) const {
        if (!hooks.verify_attempt) return std::unexpected("gang launch unavailable");
        auto job = hooks.verify_attempt(req.attempt_id(), req.token());
        if (!job) return std::unexpected(job.error());
        const auto st = hooks.job_state ? hooks.job_state(*job) : std::nullopt;
        if (!st || *st != v1::RUNNING) return std::unexpected(std::format("job {} is not running", *job));
        auto g = hooks.gang_of_local_job ? hooks.gang_of_local_job(*job) : std::nullopt;
        if (!g || g->info.role() != cv1::GANG_HEAD) return std::unexpected(std::format("job {} is not a gang head", *job));
        std::string self;
        for (const auto& m : g->info.members()) {
            if (m.member_index() == g->info.member_index()) self = m.node_id();
        }
        if (req.host().empty()) return std::unexpected("no host");
        bool member = false;
        for (const auto& m : g->info.members()) {
            if (m.node_id() == req.host() && m.member_index() != g->info.member_index()) member = true;
        }
        if (!member || req.host() == self || req.host() == g->info.head_node_id()) {
            return std::unexpected(std::format("host {} is not another member of gang {}", req.host(), g->gang_id));
        }
        return std::pair{*job, std::move(*g)};
    }

    void on_agent_launch(AgentId agent, const v1::GangLaunchRequest& req,
                         std::function<void(std::expected<void, std::string>)> started,
                         std::function<void(const v1::GangExitNotice&)> exited) {
        if (stopped) {
            if (started) started(std::unexpected("shutting down"));
            return;
        }
        auto v = validate(req);
        if (!v) {
            log_warn("gang: launch on {} refused: {}", req.host(), v.error());
            if (started) started(std::unexpected(v.error()));
            return;
        }
        auto& [job, g] = *v;
        auto l = std::make_unique<Launch>();
        l->launch_id = ++next_launch_id;
        l->agent = agent;
        l->head_job = job;
        l->gang_id = g.gang_id;
        l->member_node = req.host();
        l->on_started = std::move(started);
        l->on_exited = std::move(exited);

        v1::GangLaunch msg;
        msg.set_gang_id(g.gang_id);
        msg.set_launch_id(l->launch_id);
        msg.set_command(req.command());
        if (!send(req.host(), v1::GANG_LAUNCH, msg)) {
            const std::string why = std::format("member {} is not connected", req.host());
            log_warn("gang {}: launch refused: {}", g.gang_id, why);
            if (l->on_started) l->on_started(std::unexpected(why));
            return;
        }
        const auto id = l->launch_id;
        l->timer = std::make_unique<asio::steady_timer>(io, opts.launch_timeout);
        l->timer->async_wait([w = weak_from_this(), id](const boost::system::error_code& ec) {
            if (ec) return;
            if (auto self = w.lock(); self && !self->stopped) self->on_timeout(id);
        });
        log_info("gang {}: launch {} sent to {}", g.gang_id, id, req.host());
        launches[id] = std::move(l);
    }

    void on_timeout(std::uint64_t id) {
        auto it = launches.find(id);
        if (it == launches.end() || it->second->started) return;
        auto l = take(id);
        log_warn("gang {}: launch {} on {} timed out", l->gang_id, id, l->member_node);
        // The member may still start it later: make sure it doesn't run alone.
        send_kill(l->member_node, l->gang_id, "launch timed out");
        fail(*l, std::format("member {} did not start the launch within {} ms", l->member_node,
                             opts.launch_timeout.count()));
    }

    void on_agent_closed(AgentId agent) {
        std::vector<std::uint64_t> ids;
        for (const auto& [id, l] : launches) {
            if (l->agent == agent) ids.push_back(id);
        }
        for (auto id : ids) {
            auto l = take(id);
            log_info("gang {}: launch agent closed; killing member {}", l->gang_id, l->member_node);
            send_kill(l->member_node, l->gang_id, "launch agent closed");
        }
    }

    void on_started(const std::string& from, const v1::GangStarted& m) {
        auto it = launches.find(m.launch_id());
        if (it == launches.end() || it->second->member_node != from || it->second->started) return;
        Launch& l = *it->second;
        if (m.ok()) {
            l.started = true;
            if (l.timer) l.timer->cancel();
            log_info("gang {}: launch {} started on {}", l.gang_id, l.launch_id, from);
            if (l.on_started) {
                auto cb = l.on_started;  // the callback may end this launch
                cb({});
            }
            return;
        }
        auto taken = take(m.launch_id());
        const std::string why = std::format("member {} refused: {}", from, m.error());
        log_warn("gang {}: launch {} {}", taken->gang_id, taken->launch_id, why);
        fail(*taken, why);
    }

    void on_exit(const std::string& from, const v1::GangExit& m) {
        auto it = launches.find(m.launch_id());
        if (it == launches.end() || it->second->member_node != from) return;
        auto l = take(m.launch_id());
        if (!l->started) {
            fail(*l, std::format("member {} exited before the launch started", from));
            return;
        }
        log_info("gang {}: launch {} on {} exited {}", l->gang_id, l->launch_id, from, m.exit_code());
        v1::GangExitNotice n;
        n.set_exit_code(m.exit_code());
        n.set_error(m.error());
        if (l->on_exited) l->on_exited(n);
    }

    // ---- member side ----------------------------------------------------------------

    struct Refusal {
        std::string why;
        bool transient = false;  // may succeed once the member job is up
    };
    static std::unexpected<Refusal> refuse(std::string why, bool transient = false) {
        return std::unexpected(Refusal{std::move(why), transient});
    }

    std::expected<std::string, Refusal> accept_launch(const std::string& from, const v1::GangLaunch& m) {
        auto g = hooks.gang_by_id ? hooks.gang_by_id(m.gang_id()) : std::nullopt;
        if (!g) return refuse(std::format("unknown gang {}", m.gang_id()), true);
        if (g->info.role() != cv1::GANG_MEMBER) return refuse("not a gang member on this node");
        if (from != g->info.head_node_id()) return refuse(std::format("{} is not the gang head", from));
        const auto st = hooks.job_state ? hooks.job_state(g->local_job_id) : std::nullopt;
        if (!st || *st != v1::RUNNING) {
            const bool early = !st || !jobs::is_terminal(*st);
            return refuse(std::format("member job {} is not running", g->local_job_id), early);
        }
        if (launched_jobs.contains(g->local_job_id)) {
            return refuse(std::format("member job {} already has a launch", g->local_job_id));
        }
        v1::WorkerExec exec;
        exec.set_command(m.command());
        if (!hooks.exec_member || !hooks.exec_member(g->local_job_id, exec)) {
            return refuse(std::format("member process of job {} is not connected", g->local_job_id), true);
        }
        launched_jobs.insert(g->local_job_id);
        members[g->local_job_id] = MemberLaunch{g->gang_id, from, m.launch_id()};
        return g->local_job_id;
    }

    void on_launch(const std::string& from, const protocol::Frame& f, const v1::GangLaunch& m) {
        // The head may launch before this node has received its member lease or
        // started the member process; wait for it for most of launch_timeout (the
        // head gives up at launch_timeout).
        const auto wait = opts.launch_timeout * 3 / 4;
        try_launch(from, f, m, std::chrono::steady_clock::now() + wait);
    }

    void try_launch(const std::string& from, const protocol::Frame& f, const v1::GangLaunch& m,
                    std::chrono::steady_clock::time_point deadline) {
        auto r = accept_launch(from, m);
        if (!r && r.error().transient && std::chrono::steady_clock::now() < deadline) {
            auto& p = pending_accepts[m.gang_id()];
            if (!p.timer) log_debug("gang {}: launch {} from {} waits: {}", m.gang_id(), m.launch_id(), from, r.error().why);
            p.from = from;
            p.frame = f;
            p.msg = m;
            p.deadline = deadline;
            p.timer = std::make_unique<asio::steady_timer>(io, std::chrono::milliseconds(200));
            p.timer->async_wait([w = weak_from_this(), gang = m.gang_id()](const boost::system::error_code& ec) {
                auto self = w.lock();
                if (ec || !self || self->stopped) return;
                auto it = self->pending_accepts.find(gang);
                if (it == self->pending_accepts.end()) return;
                auto pa = std::move(it->second);
                self->pending_accepts.erase(it);
                self->try_launch(pa.from, pa.frame, pa.msg, pa.deadline);
            });
            return;
        }
        v1::GangStarted reply;
        reply.set_launch_id(m.launch_id());
        reply.set_ok(r.has_value());
        if (r) {
            log_info("gang {}: launch {} from {} started in job {}", m.gang_id(), m.launch_id(), from, *r);
        } else {
            reply.set_error(r.error().why);
            log_warn("gang {}: launch {} from {} refused: {}", m.gang_id(), m.launch_id(), from, r.error().why);
        }
        send(from, v1::GANG_STARTED, reply, f.header.message_id);
    }

    void on_kill(const std::string& from, const v1::GangKill& m) {
        if (auto it = pending_accepts.find(m.gang_id()); it != pending_accepts.end() && it->second.from == from)
            pending_accepts.erase(it);  // the timer's handler finds nothing
        auto g = hooks.gang_by_id ? hooks.gang_by_id(m.gang_id()) : std::nullopt;
        if (!g || g->info.role() != cv1::GANG_MEMBER || from != g->info.head_node_id()) {
            log_debug("gang {}: GANG_KILL from {} ignored", m.gang_id(), from);
            return;
        }
        log_info("gang {}: killed by head ({}); cancelling job {}", m.gang_id(), m.reason(), g->local_job_id);
        if (hooks.cancel_job) hooks.cancel_job(g->local_job_id, "gang killed: " + m.reason());
    }

    // ---- both sides -----------------------------------------------------------------

    void on_peer_frame(const std::string& from, const protocol::Frame& f) {
        if (stopped) return;
        switch (f.type()) {
            case v1::GANG_LAUNCH: {
                v1::GangLaunch m;
                if (f.parse(m)) return on_launch(from, f, m);
                break;
            }
            case v1::GANG_STARTED: {
                v1::GangStarted m;
                if (f.parse(m)) return on_started(from, m);
                break;
            }
            case v1::GANG_EXIT: {
                v1::GangExit m;
                if (f.parse(m)) return on_exit(from, m);
                break;
            }
            case v1::GANG_KILL: {
                v1::GangKill m;
                if (f.parse(m)) return on_kill(from, m);
                break;
            }
            default: return;
        }
        log_warn("gang: unparseable {} from {}", v1::MessageType_Name(f.type()), from);
    }

    void on_peer_lost(const std::string& node) {
        if (stopped) return;
        std::vector<std::uint64_t> ids;
        for (const auto& [id, l] : launches) {
            if (l->member_node == node) ids.push_back(id);
        }
        for (auto id : ids) {
            auto l = take(id);
            if (!l) continue;  // ended by an earlier callback
            log_warn("gang {}: link to member {} lost", l->gang_id, node);
            fail(*l, std::format("link to member {} lost", node));
        }
        std::vector<std::string> jobs;
        for (const auto& [job, m] : members) {
            if (m.head_node == node) jobs.push_back(job);
        }
        for (const auto& job : jobs) {
            auto it = members.find(job);  // a cancel may have ended an earlier one re-entrantly
            if (it == members.end()) continue;
            log_warn("gang {}: link to head {} lost; cancelling job {}", it->second.gang_id, node, job);
            if (hooks.cancel_job) hooks.cancel_job(job, "gang head link lost");
        }
    }

    void on_job_state(const std::string& job, v1::JobState state) {
        if (stopped || !jobs::is_terminal(state)) return;
        // Head job ended: kill every member it launched.
        std::vector<std::uint64_t> ids;
        for (const auto& [id, l] : launches) {
            if (l->head_job == job) ids.push_back(id);
        }
        std::set<std::string> killed;
        for (auto id : ids) {
            auto l = take(id);
            if (!l) continue;
            if (killed.insert(l->member_node).second) send_kill(l->member_node, l->gang_id, "gang head job ended");
            fail(*l, "gang head job ended");
        }
        // Member job ended: report to the head.
        if (auto it = members.find(job); it != members.end()) {
            MemberLaunch m = std::move(it->second);
            members.erase(it);
            v1::GangExit e;
            e.set_launch_id(m.launch_id);
            e.set_exit_code(state == v1::COMPLETED ? 0 : 1);
            if (state != v1::COMPLETED) e.set_error(std::format("member job {} ended {}", job, v1::JobState_Name(state)));
            if (!send(m.head_node, v1::GANG_EXIT, e)) {
                log_debug("gang {}: GANG_EXIT to {} not sent (no link)", m.gang_id, m.head_node);
            }
        }
    }

    void stop() {
        stopped = true;
        for (auto& [_, l] : launches) {
            if (l->timer) l->timer->cancel();
        }
        launches.clear();
        members.clear();
        pending_accepts.clear();
    }
};

GangService::GangService(asio::io_context& io, GangOptions options, GangHooks hooks)
    : impl_(std::make_shared<Impl>(io, options, std::move(hooks))) {}

GangService::~GangService() {
    if (impl_) impl_->stop();
}

void GangService::on_agent_launch(AgentId agent, const v1::GangLaunchRequest& request,
                                  std::function<void(std::expected<void, std::string>)> started,
                                  std::function<void(const v1::GangExitNotice&)> exited) {
    impl_->on_agent_launch(agent, request, std::move(started), std::move(exited));
}
void GangService::on_agent_closed(AgentId agent) { impl_->on_agent_closed(agent); }
void GangService::on_peer_frame(const std::string& from_node_id, protocol::Frame frame) {
    impl_->on_peer_frame(from_node_id, frame);
}
void GangService::on_peer_lost(const std::string& node_id) { impl_->on_peer_lost(node_id); }
void GangService::on_job_state(const std::string& local_job_id, v1::JobState state) {
    impl_->on_job_state(local_job_id, state);
}
void GangService::stop() { impl_->stop(); }

}  // namespace tdr::gang
