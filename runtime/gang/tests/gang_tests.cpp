// GangService: a head node and a member node, each with its own service and
// fake hooks, wired through a fake peer link (frames delivered via asio::post).
#include <boost/asio/post.hpp>
#include <boost/test/unit_test.hpp>
#include <chrono>
#include <map>
#include <memory>
#include <optional>
#include <vector>

#include "tdr/gang/gang.hpp"

namespace asio = boost::asio;
namespace v1 = ::fireforge::tdr::v1;
namespace cv1 = ::fireforge::control::v1;
using namespace std::chrono_literals;
using tdr::gang::GangHooks;
using tdr::gang::GangOptions;
using tdr::gang::GangService;
using tdr::remote::GangLocal;

namespace {

constexpr const char* kHead = "ff-head";
constexpr const char* kMember = "ff-member";
constexpr const char* kGang = "att-gang";
constexpr const char* kHeadJob = "job-g.a1";
constexpr const char* kMemberJob = "job-g.a1";  // same central job/attempt on the member node

cv1::GangInfo gang_info(std::uint32_t self) {
    cv1::GangInfo g;
    g.set_role(self == 0 ? cv1::GANG_HEAD : cv1::GANG_MEMBER);
    g.set_member_index(self);
    g.set_ranks(3);
    g.set_head_node_id(kHead);
    auto* m0 = g.add_members();
    m0->set_node_id(kHead);
    m0->set_member_index(0);
    m0->set_ranks(2);
    m0->set_role(cv1::GANG_HEAD);
    auto* m1 = g.add_members();
    m1->set_node_id(kMember);
    m1->set_member_index(1);
    m1->set_rank_offset(2);
    m1->set_ranks(1);
    m1->set_role(cv1::GANG_MEMBER);
    return g;
}

// One node: its service plus the state behind its hooks.
struct Node {
    explicit Node(std::string node_id) : id(std::move(node_id)) {}
    std::string id;
    std::map<std::string, GangLocal> gangs;          // by local job id
    std::map<std::string, v1::JobState> states;      // local job states
    std::map<std::pair<std::string, std::string>, std::string> attempts;  // (attempt, token) -> job
    bool member_connected = true;                     // exec_member result
    std::vector<std::pair<std::string, std::string>> execs;    // (job, command)
    std::vector<std::pair<std::string, std::string>> cancels;  // (job, reason)
    std::vector<tdr::protocol::Frame> sent;
    std::unique_ptr<GangService> svc;
};

struct Fixture {
    asio::io_context io;
    Node head{kHead}, member{kMember};
    bool link_up = true;
    bool hold_head_frames = false;                  // head -> member frames kept in `held`
    std::vector<tdr::protocol::Frame> held;
    std::uint64_t agent_seq = 0;

    // Results seen by the agent (head side).
    struct AgentResult {
        std::optional<std::expected<void, std::string>> started;
        int started_calls = 0;
        std::optional<v1::GangExitNotice> exited;
        int exited_calls = 0;
    };
    std::map<tdr::gang::AgentId, std::shared_ptr<AgentResult>> agents;

    explicit Fixture(std::chrono::milliseconds timeout = 2000ms) {
        head.gangs[kHeadJob] = GangLocal{kGang, "job-g", kHeadJob, gang_info(0)};
        head.states[kHeadJob] = v1::RUNNING;
        head.attempts[{"att-local", "tok"}] = kHeadJob;
        member.gangs[kMemberJob] = GangLocal{kGang, "job-g", kMemberJob, gang_info(1)};
        member.states[kMemberJob] = v1::RUNNING;
        build(head, member, timeout);
        build(member, head, timeout);
    }

    void build(Node& self, Node& peer, std::chrono::milliseconds timeout) {
        GangHooks h;
        h.send_peer = [this, &self, &peer](const std::string& node, tdr::protocol::Frame f) {
            if (!link_up || node != peer.id) return false;
            self.sent.push_back(f);
            if (hold_head_frames && &self == &head) {
                held.push_back(std::move(f));
                return true;
            }
            asio::post(io, [this, &self, &peer, f = std::move(f)]() mutable {
                if (link_up && peer.svc) peer.svc->on_peer_frame(self.id, std::move(f));
            });
            return true;
        };
        h.gang_by_id = [&self](const std::string& id) -> std::optional<GangLocal> {
            for (const auto& [_, g] : self.gangs) {
                if (g.gang_id == id) return g;
            }
            return std::nullopt;
        };
        h.gang_of_local_job = [&self](const std::string& job) -> std::optional<GangLocal> {
            auto it = self.gangs.find(job);
            if (it == self.gangs.end()) return std::nullopt;
            return it->second;
        };
        h.verify_attempt = [&self](const std::string& a, const std::string& t) -> std::expected<std::string, std::string> {
            auto it = self.attempts.find({a, t});
            if (it == self.attempts.end()) return std::unexpected("unknown attempt or bad token");
            return it->second;
        };
        h.job_state = [&self](const std::string& job) -> std::optional<v1::JobState> {
            auto it = self.states.find(job);
            if (it == self.states.end()) return std::nullopt;
            return it->second;
        };
        h.exec_member = [&self](const std::string& job, const v1::WorkerExec& e) {
            if (!self.member_connected) return false;
            self.execs.emplace_back(job, e.command());
            return true;
        };
        h.cancel_job = [this, &self](const std::string& job, const std::string& reason) {
            self.cancels.emplace_back(job, reason);
            // What JobManager + tdrd do: the job ends and the state listener fires.
            if (self.states.contains(job) && self.states[job] == v1::RUNNING) {
                self.states[job] = v1::CANCELLED;
                asio::post(io, [&self, job] { self.svc->on_job_state(job, v1::CANCELLED); });
            }
        };
        GangOptions o;
        o.launch_timeout = timeout;
        self.svc = std::make_unique<GangService>(io, o, std::move(h));
    }

    // Agent launch on the head; returns its id.
    tdr::gang::AgentId launch(const std::string& host = kMember, const std::string& token = "tok",
                              const std::string& command = "orted -mca x y") {
        const auto id = ++agent_seq;
        auto r = std::make_shared<AgentResult>();
        agents[id] = r;
        v1::GangLaunchRequest req;
        req.set_attempt_id("att-local");
        req.set_token(token);
        req.set_host(host);
        req.set_command(command);
        head.svc->on_agent_launch(
            id, req,
            [r](std::expected<void, std::string> s) {
                r->started = std::move(s);
                ++r->started_calls;
            },
            [r](const v1::GangExitNotice& n) {
                r->exited = n;
                ++r->exited_calls;
            });
        return id;
    }

    void drain() {
        io.restart();
        io.poll();
        io.restart();
    }
    template <class Pred>
    bool run_until(Pred pred, std::chrono::milliseconds timeout = 3000ms) {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (!pred()) {
            if (std::chrono::steady_clock::now() >= deadline) return false;
            if (io.stopped()) io.restart();
            io.run_one_for(10ms);
        }
        return true;
    }

    AgentResult& result(tdr::gang::AgentId id) { return *agents.at(id); }
    bool ok_started(tdr::gang::AgentId id) {
        const auto& r = result(id);
        return r.started && r.started->has_value();
    }
    std::string start_error(tdr::gang::AgentId id) {
        const auto& r = result(id);
        BOOST_REQUIRE(r.started.has_value());
        BOOST_REQUIRE(!r.started->has_value());
        return r.started->error();
    }
    int count(const Node& n, v1::MessageType t) const {
        int c = 0;
        for (const auto& f : n.sent) c += f.type() == t ? 1 : 0;
        return c;
    }
    template <class Msg>
    Msg last(const Node& n, v1::MessageType t) const {
        Msg m;
        for (const auto& f : n.sent) {
            if (f.type() == t) BOOST_REQUIRE(f.parse(m));
        }
        return m;
    }
};

struct ShortTimeout : Fixture {
    ShortTimeout() : Fixture(100ms) {}
};

bool contains(const std::string& s, const std::string& sub) { return s.find(sub) != std::string::npos; }

}  // namespace

BOOST_AUTO_TEST_SUITE(gang_service)

BOOST_FIXTURE_TEST_CASE(launch_starts_member_and_relays_exit, Fixture) {
    auto a = launch();
    BOOST_TEST(result(a).started_calls == 0);  // waits for GANG_STARTED
    drain();
    BOOST_REQUIRE(member.execs.size() == 1u);
    BOOST_TEST(member.execs[0].first == kMemberJob);
    BOOST_TEST(member.execs[0].second == "orted -mca x y");
    auto gl = last<v1::GangLaunch>(head, v1::GANG_LAUNCH);
    BOOST_TEST(gl.gang_id() == kGang);
    BOOST_TEST(gl.launch_id() > 0u);
    // GANG_STARTED replies to the GANG_LAUNCH frame.
    BOOST_REQUIRE(count(member, v1::GANG_STARTED) == 1);
    BOOST_TEST(member.sent.back().header.correlation_id == head.sent.back().header.message_id);
    BOOST_TEST(ok_started(a));
    BOOST_TEST(result(a).started_calls == 1);
    BOOST_TEST(result(a).exited_calls == 0);

    // Member job ends -> GANG_EXIT -> agent's exited.
    member.states[kMemberJob] = v1::COMPLETED;
    member.svc->on_job_state(kMemberJob, v1::COMPLETED);
    drain();
    BOOST_REQUIRE(result(a).exited_calls == 1);
    BOOST_TEST(result(a).exited->exit_code() == 0);
    BOOST_TEST(last<v1::GangExit>(member, v1::GANG_EXIT).launch_id() == gl.launch_id());

    // The head job ending afterwards kills nobody (nothing launched any more).
    head.svc->on_job_state(kHeadJob, v1::COMPLETED);
    drain();
    BOOST_TEST(count(head, v1::GANG_KILL) == 0);
    BOOST_TEST(result(a).exited_calls == 1);
}

BOOST_FIXTURE_TEST_CASE(member_failure_exit_code_is_one, Fixture) {
    auto a = launch();
    drain();
    BOOST_REQUIRE(ok_started(a));
    member.states[kMemberJob] = v1::FAILED;
    member.svc->on_job_state(kMemberJob, v1::FAILED);
    drain();
    BOOST_REQUIRE(result(a).exited_calls == 1);
    BOOST_TEST(result(a).exited->exit_code() == 1);
    BOOST_TEST(contains(result(a).exited->error(), "FAILED"));
}

BOOST_FIXTURE_TEST_CASE(head_side_refusals, Fixture) {
    // Bad token.
    auto a = launch(kMember, "wrong");
    BOOST_TEST(contains(start_error(a), "bad token"));
    // Host is the head itself / not in the gang.
    BOOST_TEST(contains(start_error(launch(kHead)), "not another member"));
    BOOST_TEST(contains(start_error(launch("ff-stranger")), "not another member"));
    // Head job not running.
    head.states[kHeadJob] = v1::COMMITTING;
    BOOST_TEST(contains(start_error(launch()), "not running"));
    head.states[kHeadJob] = v1::RUNNING;
    // Not a gang head.
    head.gangs[kHeadJob].info.set_role(cv1::GANG_MEMBER);
    BOOST_TEST(contains(start_error(launch()), "not a gang head"));
    head.gangs.erase(kHeadJob);
    BOOST_TEST(contains(start_error(launch()), "not a gang head"));
    BOOST_TEST(head.sent.empty());
    for (const auto& [_, r] : agents) BOOST_TEST(r->started_calls == 1);
}

BOOST_FIXTURE_TEST_CASE(member_not_connected_fails_at_once, Fixture) {
    link_up = false;
    auto a = launch();
    BOOST_TEST(contains(start_error(a), "not connected"));
    drain();
    BOOST_TEST(result(a).started_calls == 1);
}

BOOST_FIXTURE_TEST_CASE(member_waits_for_process_to_connect, Fixture) {
    member.member_connected = false;
    auto a = launch();
    drain();
    BOOST_TEST(!result(a).started.has_value());  // waiting, not refused
    BOOST_TEST(count(member, v1::GANG_STARTED) == 0);
    member.member_connected = true;
    BOOST_REQUIRE(run_until([&] { return result(a).started.has_value(); }));
    BOOST_TEST(ok_started(a));
    BOOST_TEST(member.execs.size() == 1u);
}

BOOST_FIXTURE_TEST_CASE(member_gives_up_waiting_before_head_timeout, Fixture) {
    member.member_connected = false;
    auto a = launch();
    // Member waits 3/4 of launch_timeout (1.5 s of 2 s), then refuses.
    BOOST_REQUIRE(run_until([&] { return result(a).started.has_value(); }));
    BOOST_TEST(contains(start_error(a), "not connected"));
    BOOST_TEST(!last<v1::GangStarted>(member, v1::GANG_STARTED).ok());
    // Not counted as the member job's one launch: a later one may succeed.
    member.member_connected = true;
    auto b = launch();
    drain();
    BOOST_TEST(ok_started(b));
}

BOOST_FIXTURE_TEST_CASE(member_waits_for_job_to_run, Fixture) {
    member.states[kMemberJob] = v1::QUEUED;  // e.g. still fetching inputs
    auto a = launch();
    drain();
    BOOST_TEST(!result(a).started.has_value());
    member.states[kMemberJob] = v1::RUNNING;
    BOOST_REQUIRE(run_until([&] { return result(a).started.has_value(); }));
    BOOST_TEST(ok_started(a));
}

BOOST_FIXTURE_TEST_CASE(member_refuses_ended_job_at_once, Fixture) {
    member.states[kMemberJob] = v1::FAILED;
    auto a = launch();
    drain();
    BOOST_TEST(contains(start_error(a), "not running"));
    BOOST_TEST(member.execs.empty());
}

BOOST_FIXTURE_TEST_CASE(kill_drops_a_waiting_launch, Fixture) {
    member.member_connected = false;
    v1::GangLaunch gl;
    gl.set_gang_id(kGang);
    gl.set_launch_id(9);
    gl.set_command("orted");
    member.svc->on_peer_frame(kHead, tdr::protocol::make_frame(v1::GANG_LAUNCH, gl, 1));
    v1::GangKill k;
    k.set_gang_id(kGang);
    member.svc->on_peer_frame(kHead, tdr::protocol::make_frame(v1::GANG_KILL, k, 2));
    member.member_connected = true;
    run_until([] { return false; }, 400ms);
    BOOST_TEST(member.execs.empty());
}

BOOST_FIXTURE_TEST_CASE(member_refuses_second_launch, Fixture) {
    auto a = launch();
    drain();
    BOOST_REQUIRE(ok_started(a));
    auto b = launch();
    drain();
    BOOST_TEST(contains(start_error(b), "already has a launch"));
    BOOST_TEST(member.execs.size() == 1u);
    BOOST_TEST(result(a).exited_calls == 0);  // the first is unaffected
}

BOOST_FIXTURE_TEST_CASE(member_accepts_launch_only_from_head, Fixture) {
    // A GANG_LAUNCH from some other peer is refused (reply goes to that peer).
    v1::GangLaunch gl;
    gl.set_gang_id(kGang);
    gl.set_launch_id(7);
    gl.set_command("evil");
    head.id = "ff-impostor";  // the fake link still routes replies to `head`
    member.svc->on_peer_frame("ff-impostor", tdr::protocol::make_frame(v1::GANG_LAUNCH, gl, 1));
    BOOST_TEST(member.execs.empty());
    // GANG_KILL from a non-head is ignored too.
    v1::GangKill k;
    k.set_gang_id(kGang);
    member.svc->on_peer_frame("ff-impostor", tdr::protocol::make_frame(v1::GANG_KILL, k, 2));
    BOOST_TEST(member.cancels.empty());
    // Unknown gang: may be a lease not received yet, so refused only after waiting.
    head.id = kHead;
    gl.set_gang_id("att-unknown");
    member.svc->on_peer_frame(kHead, tdr::protocol::make_frame(v1::GANG_LAUNCH, gl, 3));
    BOOST_TEST(member.execs.empty());
    BOOST_TEST(count(member, v1::GANG_STARTED) == 1);
    BOOST_REQUIRE(run_until([&] { return count(member, v1::GANG_STARTED) == 2; }));  // both refused
    BOOST_TEST(contains(last<v1::GangStarted>(member, v1::GANG_STARTED).error(), "unknown gang"));
}

BOOST_FIXTURE_TEST_CASE(launch_timeout_fails_and_kills_member, ShortTimeout) {
    link_up = true;
    // The member never answers: drop frames on the floor after sending.
    member.svc.reset();
    auto a = launch();
    BOOST_REQUIRE(run_until([&] { return result(a).started_calls == 1; }));
    BOOST_TEST(contains(start_error(a), "did not start"));
    BOOST_TEST(count(head, v1::GANG_KILL) == 1);
    BOOST_TEST(last<v1::GangKill>(head, v1::GANG_KILL).gang_id() == kGang);
}

BOOST_FIXTURE_TEST_CASE(late_started_after_timeout_is_ignored, ShortTimeout) {
    hold_head_frames = true;
    auto a = launch();
    BOOST_REQUIRE(run_until([&] { return result(a).started_calls == 1; }));
    BOOST_TEST(contains(start_error(a), "did not start"));
    // Now the member sees GANG_LAUNCH then GANG_KILL: it starts, is killed, and
    // its GANG_STARTED / GANG_EXIT reach a head that forgot the launch.
    BOOST_REQUIRE(held.size() == 2u);
    for (auto& f : held) member.svc->on_peer_frame(kHead, f);
    drain();
    BOOST_TEST(member.execs.size() == 1u);
    BOOST_TEST(member.cancels.size() == 1u);
    BOOST_TEST(count(member, v1::GANG_STARTED) == 1);
    BOOST_TEST(count(member, v1::GANG_EXIT) == 1);
    BOOST_TEST(result(a).started_calls == 1);
    BOOST_TEST(result(a).exited_calls == 0);
}

BOOST_FIXTURE_TEST_CASE(peer_lost_fails_pending_and_started_launches, Fixture) {
    auto a = launch();
    drain();
    BOOST_REQUIRE(ok_started(a));
    auto b_pending = launch();  // will be refused by the member, but lose the link first
    head.svc->on_peer_lost(kMember);
    BOOST_TEST(result(a).exited_calls == 1);
    BOOST_TEST(result(a).exited->exit_code() == -1);
    BOOST_TEST(contains(start_error(b_pending), "lost"));
    // Member side: the head is gone -> the launched member job is cancelled.
    member.svc->on_peer_lost(kHead);
    BOOST_REQUIRE(member.cancels.size() == 1u);
    BOOST_TEST(member.cancels[0].first == kMemberJob);
    drain();
    BOOST_TEST(result(a).exited_calls == 1);  // nothing more for the agent
    BOOST_TEST(result(b_pending).started_calls == 1);
}

BOOST_FIXTURE_TEST_CASE(member_peer_lost_without_launch_cancels_nothing, Fixture) {
    member.svc->on_peer_lost(kHead);
    BOOST_TEST(member.cancels.empty());
}

BOOST_FIXTURE_TEST_CASE(agent_closed_kills_member_without_callbacks, Fixture) {
    auto a = launch();
    drain();
    BOOST_REQUIRE(ok_started(a));
    head.svc->on_agent_closed(a);
    drain();
    BOOST_TEST(count(head, v1::GANG_KILL) == 1);
    BOOST_REQUIRE(member.cancels.size() == 1u);
    BOOST_TEST(member.cancels[0].first == kMemberJob);
    BOOST_TEST(contains(member.cancels[0].second, "launch agent closed"));
    // The member job's end is reported, but the agent is gone: no callback.
    BOOST_TEST(count(member, v1::GANG_EXIT) == 1);
    BOOST_TEST(result(a).exited_calls == 0);
}

BOOST_FIXTURE_TEST_CASE(agent_closed_while_pending_kills_member, Fixture) {
    auto a = launch();
    head.svc->on_agent_closed(a);  // before the GANG_LAUNCH is even delivered
    drain();
    BOOST_TEST(result(a).started_calls == 0);
    BOOST_TEST(count(head, v1::GANG_KILL) == 1);
    BOOST_TEST(member.cancels.size() == 1u);
}

BOOST_FIXTURE_TEST_CASE(head_job_end_kills_members_and_fires_exited, Fixture) {
    auto a = launch();
    drain();
    BOOST_REQUIRE(ok_started(a));
    head.states[kHeadJob] = v1::FAILED;
    head.svc->on_job_state(kHeadJob, v1::FAILED);
    BOOST_REQUIRE(result(a).exited_calls == 1);
    BOOST_TEST(result(a).exited->exit_code() == -1);
    drain();
    BOOST_TEST(count(head, v1::GANG_KILL) == 1);
    BOOST_REQUIRE(member.cancels.size() == 1u);
    BOOST_TEST(contains(member.cancels[0].second, "head job ended"));
    BOOST_TEST(result(a).exited_calls == 1);  // the later GANG_EXIT finds nothing
}

BOOST_FIXTURE_TEST_CASE(non_terminal_states_do_nothing, Fixture) {
    auto a = launch();
    drain();
    BOOST_REQUIRE(ok_started(a));
    head.svc->on_job_state(kHeadJob, v1::RUNNING);
    member.svc->on_job_state(kMemberJob, v1::COMMITTING);
    drain();
    BOOST_TEST(count(head, v1::GANG_KILL) == 0);
    BOOST_TEST(count(member, v1::GANG_EXIT) == 0);
    BOOST_TEST(result(a).exited_calls == 0);
}

BOOST_FIXTURE_TEST_CASE(stop_silences_callbacks, Fixture) {
    auto a = launch();
    head.svc->stop();
    drain();
    BOOST_TEST(result(a).started_calls == 0);
    auto b = launch();
    BOOST_TEST(contains(start_error(b), "shutting down"));
}

BOOST_AUTO_TEST_SUITE_END()
