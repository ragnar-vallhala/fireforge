#include <google/protobuf/util/json_util.h>

#include <boost/asio/post.hpp>
#include <boost/test/unit_test.hpp>
#include <chrono>
#include <deque>
#include <fstream>
#include <sstream>

#include "../src/agent_internal.hpp"
#include "tdr/agent/agent.hpp"
#include "tdr/common/common.hpp"

namespace tdr::agent {
inline std::ostream& operator<<(std::ostream& os, ControlState s) { return os << to_string(s); }
}  // namespace tdr::agent

namespace asio = boost::asio;
using namespace std::chrono_literals;
using namespace tdr::agent;
using tdr::jobs::Capacity;

namespace {

constexpr std::uint64_t kGiB = 1ull << 30;

struct TempDir {
    fs::path path;
    TempDir() {
        path = fs::temp_directory_path() / tdr::new_id("agent-test");
        fs::create_directories(path);
    }
    ~TempDir() {
        std::error_code ec;
        fs::remove_all(path, ec);
    }
};

// Queues each call; the test answers it (or an auto-responder does). Replies
// are always delivered via asio::post, like a real client.
class FakeControlClient final : public IControlClient {
public:
    explicit FakeControlClient(asio::io_context& io) : io_(io) {}

    template <class T>
    struct Kind {
        std::deque<std::function<void(Result<T>)>> pending;
        std::function<Result<T>()> auto_reply;  // when set, answers every call
        int calls = 0;
    };
    Kind<cv1::RegisterReply> reg;
    Kind<cv1::HeartbeatReply> hb;
    Kind<cv1::NodeList> nodes;
    Kind<cv1::NodeInfo> off;
    std::vector<cv1::Heartbeat> heartbeats;
    std::vector<std::chrono::steady_clock::time_point> register_times;
    std::vector<std::pair<std::string, std::string>> offline_calls;

    void register_node(const cv1::RegisterRequest&, std::function<void(Result<cv1::RegisterReply>)> cb) override {
        register_times.push_back(std::chrono::steady_clock::now());
        enqueue(reg, std::move(cb));
    }
    void heartbeat(const cv1::Heartbeat& h, std::function<void(Result<cv1::HeartbeatReply>)> cb) override {
        heartbeats.push_back(h);
        enqueue(hb, std::move(cb));
    }
    void list_nodes(std::function<void(Result<cv1::NodeList>)> cb) override { enqueue(nodes, std::move(cb)); }
    void offline(const std::string& id, const std::string& reason,
                 std::function<void(Result<cv1::NodeInfo>)> cb) override {
        offline_calls.emplace_back(id, reason);
        enqueue(off, std::move(cb));
    }

    // Phase 4 calls are not used by NodeAgent itself; answer them with an error.
    void submit_job(const cv1::CreateJobRequest&, std::function<void(Result<cv1::Job>)> cb) override { unused(cb); }
    void get_job(const std::string&, std::function<void(Result<cv1::Job>)> cb) override { unused(cb); }
    void list_jobs(::fireforge::tdr::v1::JobState, const std::string&, std::uint32_t,
                   std::function<void(Result<cv1::JobList>)> cb) override {
        unused(cb);
    }
    void cancel_job(const std::string&, const std::string&, std::function<void(Result<cv1::Job>)> cb) override {
        unused(cb);
    }
    void lease(const cv1::LeaseRequest&, std::function<void(Result<cv1::LeaseReply>)> cb) override { unused(cb); }
    void update_job(const std::string&, const cv1::JobUpdate&, std::function<void(Result<cv1::Job>)> cb) override {
        unused(cb);
    }
    void announce_artifacts(const cv1::ArtifactAnnouncement&, std::function<void(Result<void>)> cb) override {
        unused(cb);
    }
    int unused_calls = 0;

    template <class T>
    void answer(Kind<T>& k, Result<T> r) {
        BOOST_REQUIRE(!k.pending.empty());
        auto cb = std::move(k.pending.front());
        k.pending.pop_front();
        asio::post(io_, [cb = std::move(cb), r = std::move(r)]() mutable { cb(std::move(r)); });
    }

private:
    template <class T>
    void unused(std::function<void(Result<T>)>& cb) {
        ++unused_calls;
        asio::post(io_, [cb = std::move(cb)] { cb(std::unexpected(ClientError{501, "not implemented"})); });
    }
    template <class T>
    void enqueue(Kind<T>& k, std::function<void(Result<T>)> cb) {
        ++k.calls;
        k.pending.push_back(std::move(cb));
        if (k.auto_reply) answer(k, k.auto_reply());
    }
    asio::io_context& io_;
};

cv1::ResourcePolicy policy(std::uint64_t version, std::uint32_t cores, std::uint64_t mem, std::uint32_t jobs) {
    cv1::ResourcePolicy p;
    p.set_version(version);
    p.set_max_cpu_cores(cores);
    p.set_max_memory_bytes(mem);
    p.set_max_jobs(jobs);
    return p;
}

cv1::RegisterReply register_reply(const cv1::ResourcePolicy& p, std::uint32_t interval = 1) {
    cv1::RegisterReply r;
    r.set_node_id("ff-node-0000000000000001");
    *r.mutable_policy() = p;
    r.set_heartbeat_interval_seconds(interval);
    return r;
}

template <class T>
Result<T> fail(int status, std::string msg = "boom") {
    return std::unexpected(ClientError{status, std::move(msg)});
}

struct Fixture {
    TempDir tmp;
    asio::io_context io;
    FakeControlClient client{io};
    std::vector<Capacity> applied;
    std::function<void(cv1::Heartbeat&)> fill;
    std::vector<std::string> registered;
    std::vector<std::vector<cv1::LeaseRef>> revoked;
    bool phase4_hooks = true;  // false = leave on_registered / on_revoked empty
    AgentOptions opts;
    std::unique_ptr<NodeAgent> agent;
    std::chrono::milliseconds saved_unit = detail::g_interval_unit;

    Fixture() {
        detail::g_interval_unit = 50ms;  // heartbeat_interval_seconds=1 -> 50 ms
        opts.registration.set_name("node-a");
        opts.registration.set_daemon_version("1.2.3");
        auto* a = opts.registration.add_addresses();
        a->set_type("lan");
        a->set_address("192.0.2.7");
        a->set_port(7070);
        opts.local_capacity = Capacity{8, 16 * kGiB, 4};
        opts.policy_cache = tmp.path / "state" / "policy.json";
        opts.backoff_min = 20ms;
        opts.backoff_max = 80ms;
        opts.offline_timeout = 100ms;
    }
    ~Fixture() {
        agent.reset();
        detail::g_interval_unit = saved_unit;
    }

    void make() {
        AgentHooks hooks;
        hooks.apply_capacity = [this](const Capacity& c) { applied.push_back(c); };
        hooks.fill_stats = [this](cv1::Heartbeat& h) {
            if (fill) fill(h);
        };
        if (phase4_hooks) {
            hooks.on_registered = [this](const std::string& id) { registered.push_back(id); };
            hooks.on_revoked = [this](const std::vector<cv1::LeaseRef>& r) { revoked.push_back(r); };
        }
        agent = std::make_unique<NodeAgent>(io, client, opts, std::move(hooks));
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
    void run_for(std::chrono::milliseconds d) {
        if (io.stopped()) io.restart();
        io.run_for(d);
    }

    // Start, register with `p`, answer the first peer fetch with an empty list.
    void connect(const cv1::ResourcePolicy& p) {
        make();
        agent->start();
        client.answer(client.reg, Result<cv1::RegisterReply>(register_reply(p)));
        BOOST_REQUIRE(run_until([&] { return agent->state() == ControlState::kConnected; }));
        BOOST_REQUIRE(run_until([&] { return !client.nodes.pending.empty(); }));
        client.answer(client.nodes, Result<cv1::NodeList>(cv1::NodeList{}));
    }

    std::optional<cv1::ResourcePolicy> read_cache() const {
        std::ifstream in(opts.policy_cache);
        if (!in) return std::nullopt;
        std::stringstream ss;
        ss << in.rdbuf();
        cv1::ResourcePolicy p;
        if (!google::protobuf::util::JsonStringToMessage(ss.str(), &p).ok()) return std::nullopt;
        return p;
    }
};

bool same(const Capacity& a, const Capacity& b) {
    return a.cpu_cores == b.cpu_cores && a.memory_bytes == b.memory_bytes && a.jobs == b.jobs;
}

}  // namespace

BOOST_AUTO_TEST_SUITE(agent_helpers)

BOOST_AUTO_TEST_CASE(effective_capacity_min_and_unlimited) {
    const Capacity local{8, 16 * kGiB, 4};
    auto c = effective_capacity(local, policy(1, 4, 0, 10));
    BOOST_TEST(c.cpu_cores == 4u);
    BOOST_TEST(c.memory_bytes == 16 * kGiB);  // 0 = no central limit
    BOOST_TEST(c.jobs == 4u);                 // local cap is lower
    BOOST_TEST(same(effective_capacity(local, policy(1, 0, 0, 0)), local));
    c = effective_capacity(local, policy(2, 0, 2 * kGiB, 1));
    BOOST_TEST(c.cpu_cores == 8u);
    BOOST_TEST(c.memory_bytes == 2 * kGiB);
    BOOST_TEST(c.jobs == 1u);
}

BOOST_AUTO_TEST_CASE(jitter_and_backoff) {
    std::mt19937 rng(42);
    bool below = false, above = false;
    for (int i = 0; i < 2000; ++i) {
        auto d = detail::jitter(1000ms, rng);
        BOOST_TEST(d.count() >= 800);
        BOOST_TEST(d.count() <= 1200);
        below |= d.count() < 950;
        above |= d.count() > 1050;
    }
    BOOST_TEST(below);
    BOOST_TEST(above);

    std::chrono::milliseconds b{0};
    std::vector<std::int64_t> seq;
    for (int i = 0; i < 9; ++i) {
        b = detail::next_backoff_base(b, 1000ms, 60000ms);
        seq.push_back(b.count());
    }
    BOOST_TEST(seq == (std::vector<std::int64_t>{1000, 2000, 4000, 8000, 16000, 32000, 60000, 60000, 60000}),
               boost::test_tools::per_element());
}

BOOST_AUTO_TEST_CASE(cpu_sampler_range) {
    CpuSampler s;
    BOOST_TEST(s.sample() == 0.0);
    volatile std::uint64_t x = 0;
    const auto end = std::chrono::steady_clock::now() + 50ms;
    while (std::chrono::steady_clock::now() < end) x = x + 1;
    const double u = s.sample();
    BOOST_TEST(u >= 0.0);
    BOOST_TEST(u <= 1.0);
}

BOOST_AUTO_TEST_CASE(detect_physical_sanity) {
    TempDir tmp;
    auto r = detect_physical(tmp.path);
    BOOST_TEST(r.cpu_cores() >= 1u);
    BOOST_TEST(r.memory_bytes() > 64ull * 1024 * 1024);
    BOOST_TEST(r.disk_bytes() > 0u);
}

BOOST_AUTO_TEST_CASE(detect_addresses_override_and_auto) {
    auto a = detect_addresses(7070, "auto", "203.0.113.9");
    BOOST_REQUIRE(a.size() == 1u);
    BOOST_TEST(a[0].type() == "lan");
    BOOST_TEST(a[0].address() == "203.0.113.9");
    BOOST_TEST(a[0].port() == 7070u);

    auto all = detect_addresses(7070, "auto", "auto");
    bool seen_lan = false;
    for (const auto& x : all) {
        BOOST_TEST((x.type() == "lan" || x.type() == "tailscale"));
        BOOST_TEST(x.port() == 7070u);
        BOOST_TEST(x.address() != "127.0.0.1");
        BOOST_TEST(!x.address().starts_with("fe80"));
        if (x.type() == "lan") {
            seen_lan = true;
        } else {
            BOOST_TEST(!seen_lan, "tailscale entries come first");
        }
    }
    BOOST_TEST(detect_addresses(7070, "auto", "auto").size() == all.size());  // deterministic
    for (const auto& x : detect_addresses(7070, "off", "auto")) {
        BOOST_TEST(x.type() == "lan");
    }
}

BOOST_AUTO_TEST_CASE(state_names) {
    BOOST_TEST(std::string(to_string(ControlState::kStandalone)) == "standalone");
    BOOST_TEST(std::string(to_string(ControlState::kRegistering)) == "registering");
    BOOST_TEST(std::string(to_string(ControlState::kConnected)) == "connected");
    BOOST_TEST(std::string(to_string(ControlState::kDisconnected)) == "disconnected");
}

BOOST_AUTO_TEST_SUITE_END()

BOOST_AUTO_TEST_SUITE(node_agent)

BOOST_FIXTURE_TEST_CASE(cached_policy_applied_before_network, Fixture) {
    fs::create_directories(opts.policy_cache.parent_path());
    std::ofstream(opts.policy_cache) << R"({"version": "3", "max_jobs": 1, "max_cpu_cores": 2, "future": 1})";
    make();
    agent->start();
    // Synchronously, without running the io_context.
    BOOST_REQUIRE(applied.size() == 1u);
    BOOST_TEST(same(applied[0], Capacity{2, 16 * kGiB, 1}));
    BOOST_REQUIRE(agent->policy().has_value());
    BOOST_TEST(agent->policy()->version() == 3u);
    BOOST_TEST(agent->state() == ControlState::kRegistering);
    BOOST_TEST(client.reg.calls == 1);

    // Same version from the server -> nothing re-applied.
    client.answer(client.reg, Result<cv1::RegisterReply>(register_reply(policy(3, 2, 0, 1))));
    BOOST_REQUIRE(run_until([&] { return agent->state() == ControlState::kConnected; }));
    BOOST_TEST(applied.size() == 1u);
}

BOOST_FIXTURE_TEST_CASE(no_cache_applies_local_capacity, Fixture) {
    std::ofstream(tmp.path / "garbage.json") << "not json";
    opts.policy_cache = tmp.path / "garbage.json";
    make();
    agent->start();
    BOOST_REQUIRE(applied.size() == 1u);
    BOOST_TEST(same(applied[0], opts.local_capacity));
    BOOST_TEST(!agent->policy().has_value());
}

BOOST_FIXTURE_TEST_CASE(register_success_applies_and_caches_policy, Fixture) {
    make();
    agent->start();
    BOOST_TEST(agent->node_id().empty());
    client.answer(client.reg, Result<cv1::RegisterReply>(register_reply(policy(2, 2, 0, 0))));
    BOOST_REQUIRE(run_until([&] { return agent->state() == ControlState::kConnected; }));
    BOOST_TEST(agent->node_id() == "ff-node-0000000000000001");
    BOOST_REQUIRE(applied.size() == 2u);  // local at start, then policy
    BOOST_TEST(same(applied[1], Capacity{2, 16 * kGiB, 4}));
    auto cached = read_cache();
    BOOST_REQUIRE(cached.has_value());
    BOOST_TEST(cached->version() == 2u);
    BOOST_TEST(cached->max_cpu_cores() == 2u);
    BOOST_TEST(!fs::exists(opts.policy_cache.string() + ".tmp"));
    BOOST_TEST(client.nodes.calls == 1);  // peers fetched immediately
}

BOOST_FIXTURE_TEST_CASE(register_failure_backs_off_then_succeeds, Fixture) {
    int failures = 0;
    client.reg.auto_reply = [&]() -> Result<cv1::RegisterReply> {
        if (failures < 3) {
            ++failures;
            return fail<cv1::RegisterReply>(0, "connection refused");
        }
        return register_reply(policy(1, 0, 0, 0));
    };
    make();
    agent->start();
    BOOST_REQUIRE(run_until([&] { return failures == 1 && !agent->last_error().empty(); }));
    BOOST_TEST(agent->last_error() == "connection refused");
    BOOST_TEST(agent->state() == ControlState::kRegistering);
    BOOST_REQUIRE(run_until([&] { return agent->state() == ControlState::kConnected; }));
    BOOST_TEST(agent->last_error().empty());
    BOOST_REQUIRE(client.register_times.size() == 4u);
    auto gap = [&](int i) {
        return std::chrono::duration_cast<std::chrono::milliseconds>(client.register_times[i] -
                                                                     client.register_times[i - 1])
            .count();
    };
    // 20, 40, 80 ms base, ±20%.
    BOOST_TEST(gap(1) >= 16);
    BOOST_TEST(gap(2) >= 32);
    BOOST_TEST(gap(3) >= 64);
    BOOST_TEST(gap(3) < 400);
}

BOOST_FIXTURE_TEST_CASE(heartbeat_carries_stats_and_policy_version, Fixture) {
    fill = [](cv1::Heartbeat& h) {
        h.set_running_jobs(3);
        h.set_cpu_utilization(0.5);
        h.set_node_id("overwritten");
        h.mutable_capacity()->set_cpu_cores(2);
    };
    client.hb.auto_reply = [] { return Result<cv1::HeartbeatReply>(cv1::HeartbeatReply{}); };
    connect(policy(7, 2, 0, 0));
    BOOST_REQUIRE(run_until([&] { return client.heartbeats.size() >= 2 && agent->last_heartbeat_ok_ms() > 0; }));
    const auto& h = client.heartbeats[0];
    BOOST_TEST(h.node_id() == "ff-node-0000000000000001");
    BOOST_TEST(h.policy_version() == 7u);
    BOOST_TEST(h.daemon_version() == "1.2.3");
    BOOST_TEST(h.running_jobs() == 3u);
    BOOST_TEST(h.cpu_utilization() == 0.5);
    BOOST_TEST(h.capacity().cpu_cores() == 2u);
    BOOST_TEST(h.sent_unix_ms() > 0);
    BOOST_REQUIRE(h.addresses_size() == 1);
    BOOST_TEST(h.addresses(0).address() == "192.0.2.7");
}

BOOST_FIXTURE_TEST_CASE(policy_in_heartbeat_reply_applied_live, Fixture) {
    connect(policy(1, 0, 0, 0));
    BOOST_REQUIRE(applied.size() == 2u);
    BOOST_TEST(same(applied[1], opts.local_capacity));  // all-zero policy = unlimited

    BOOST_REQUIRE(run_until([&] { return !client.hb.pending.empty(); }));
    cv1::HeartbeatReply r;
    *r.mutable_policy() = policy(5, 0, 2 * kGiB, 2);
    client.answer(client.hb, Result<cv1::HeartbeatReply>(r));
    BOOST_REQUIRE(run_until([&] { return applied.size() == 3u; }));
    BOOST_TEST(same(applied[2], Capacity{8, 2 * kGiB, 2}));
    BOOST_TEST(agent->policy()->version() == 5u);
    BOOST_TEST(read_cache()->version() == 5u);

    BOOST_REQUIRE(run_until([&] { return client.heartbeats.size() == 2u; }));
    BOOST_TEST(client.heartbeats[1].policy_version() == 5u);
}

BOOST_FIXTURE_TEST_CASE(unknown_node_re_registers, Fixture) {
    connect(policy(4, 2, 0, 0));
    const auto before = applied.size();
    BOOST_REQUIRE(run_until([&] { return !client.hb.pending.empty(); }));
    client.answer(client.hb, fail<cv1::HeartbeatReply>(404, "unknown node"));
    BOOST_REQUIRE(run_until([&] { return client.reg.calls == 2; }));
    BOOST_TEST(agent->state() == ControlState::kRegistering);
    BOOST_TEST(applied.size() == before);  // capacity unchanged

    auto rr = register_reply(policy(4, 2, 0, 0));
    client.answer(client.reg, Result<cv1::RegisterReply>(rr));
    BOOST_REQUIRE(run_until([&] { return agent->state() == ControlState::kConnected; }));
    BOOST_TEST(applied.size() == before);  // same policy version: not re-applied
    BOOST_REQUIRE(run_until([&] { return !client.hb.pending.empty(); }));
    BOOST_TEST(client.heartbeats.back().policy_version() == 4u);
}

BOOST_FIXTURE_TEST_CASE(outage_disconnects_then_recovers, Fixture) {
    connect(policy(1, 0, 0, 0));
    const auto before = applied.size();
    BOOST_REQUIRE(run_until([&] { return !client.hb.pending.empty(); }));
    client.answer(client.hb, Result<cv1::HeartbeatReply>(cv1::HeartbeatReply{}));
    BOOST_REQUIRE(run_until([&] { return !client.hb.pending.empty(); }));
    client.answer(client.hb, fail<cv1::HeartbeatReply>(0, "connection reset"));
    BOOST_REQUIRE(run_until([&] { return agent->state() == ControlState::kDisconnected; }));
    BOOST_TEST(agent->last_error() == "connection reset");
    BOOST_REQUIRE(run_until([&] { return !client.hb.pending.empty(); }));
    client.answer(client.hb, fail<cv1::HeartbeatReply>(503, "unavailable"));
    BOOST_REQUIRE(run_until([&] { return !client.hb.pending.empty(); }));
    BOOST_TEST(agent->state() == ControlState::kDisconnected);
    BOOST_TEST(agent->last_error().find("503") != std::string::npos);
    BOOST_TEST(applied.size() == before);
    BOOST_TEST(client.reg.calls == 1);  // outage does not re-register

    client.answer(client.hb, Result<cv1::HeartbeatReply>(cv1::HeartbeatReply{}));
    BOOST_REQUIRE(run_until([&] { return agent->state() == ControlState::kConnected; }));
    BOOST_TEST(agent->last_error().empty());
    BOOST_TEST(applied.size() == before);
}

BOOST_FIXTURE_TEST_CASE(peers_stored, Fixture) {
    make();
    agent->start();
    client.answer(client.reg, Result<cv1::RegisterReply>(register_reply(policy(1, 0, 0, 0))));
    BOOST_REQUIRE(run_until([&] { return !client.nodes.pending.empty(); }));
    BOOST_TEST(agent->peers_refreshed_ms() == 0);
    cv1::NodeList list;
    list.add_nodes()->set_name("node-a");
    list.add_nodes()->set_name("node-b");
    client.answer(client.nodes, Result<cv1::NodeList>(list));
    BOOST_REQUIRE(run_until([&] { return agent->peers_refreshed_ms() > 0; }));
    BOOST_REQUIRE(agent->peers().nodes_size() == 2);
    BOOST_TEST(agent->peers().nodes(1).name() == "node-b");
}

BOOST_FIXTURE_TEST_CASE(stop_sends_offline_and_calls_done_once, Fixture) {
    connect(policy(1, 0, 0, 0));
    int done = 0;
    agent->stop([&] { ++done; });
    BOOST_REQUIRE(client.offline_calls.size() == 1u);
    BOOST_TEST(client.offline_calls[0].first == "ff-node-0000000000000001");
    BOOST_TEST(client.offline_calls[0].second == "shutdown");
    BOOST_TEST(done == 0);
    client.answer(client.off, Result<cv1::NodeInfo>(cv1::NodeInfo{}));
    BOOST_REQUIRE(run_until([&] { return done == 1; }));
    run_for(200ms);  // past offline_timeout
    BOOST_TEST(done == 1);
    const auto hbs = client.heartbeats.size();
    run_for(150ms);
    BOOST_TEST(client.heartbeats.size() == hbs);  // no more heartbeats after stop
}

BOOST_FIXTURE_TEST_CASE(stop_with_unresponsive_server_times_out, Fixture) {
    connect(policy(1, 0, 0, 0));
    int done = 0;
    const auto t0 = std::chrono::steady_clock::now();
    agent->stop([&] { ++done; });
    BOOST_REQUIRE(run_until([&] { return done == 1; }));
    const auto waited = std::chrono::steady_clock::now() - t0;
    BOOST_TEST(waited >= 90ms);
    BOOST_TEST(waited < 1000ms);
    client.answer(client.off, fail<cv1::NodeInfo>(0, "late"));
    run_for(50ms);
    BOOST_TEST(done == 1);
}

BOOST_FIXTURE_TEST_CASE(stop_before_registration_posts_done, Fixture) {
    make();
    agent->start();
    int done = 0;
    agent->stop([&] { ++done; });
    BOOST_TEST(done == 0);  // never inline
    BOOST_TEST(client.offline_calls.empty());
    BOOST_REQUIRE(run_until([&] { return done == 1; }));
    // Late register reply is ignored.
    client.answer(client.reg, Result<cv1::RegisterReply>(register_reply(policy(1, 0, 0, 0))));
    run_for(50ms);
    BOOST_TEST(agent->state() == ControlState::kRegistering);
}

BOOST_FIXTURE_TEST_CASE(on_registered_fires_on_register_and_re_register, Fixture) {
    connect(policy(1, 0, 0, 0));
    BOOST_REQUIRE(registered.size() == 1u);
    BOOST_TEST(registered[0] == "ff-node-0000000000000001");
    BOOST_TEST(&agent->client() == static_cast<IControlClient*>(&client));

    BOOST_REQUIRE(run_until([&] { return !client.hb.pending.empty(); }));
    client.answer(client.hb, fail<cv1::HeartbeatReply>(404, "unknown node"));
    BOOST_REQUIRE(run_until([&] { return client.reg.calls == 2; }));
    BOOST_TEST(registered.size() == 1u);
    auto rr = register_reply(policy(1, 0, 0, 0));
    rr.set_node_id("ff-node-0000000000000002");
    client.answer(client.reg, Result<cv1::RegisterReply>(rr));
    BOOST_REQUIRE(run_until([&] { return registered.size() == 2u; }));
    BOOST_TEST(registered[1] == "ff-node-0000000000000002");
    BOOST_TEST(agent->node_id() == "ff-node-0000000000000002");
}

BOOST_FIXTURE_TEST_CASE(on_revoked_receives_list, Fixture) {
    connect(policy(1, 0, 0, 0));
    BOOST_REQUIRE(run_until([&] { return !client.hb.pending.empty(); }));
    client.answer(client.hb, Result<cv1::HeartbeatReply>(cv1::HeartbeatReply{}));  // empty: no call
    BOOST_REQUIRE(run_until([&] { return !client.hb.pending.empty(); }));
    BOOST_TEST(revoked.empty());

    cv1::HeartbeatReply r;
    auto* a = r.add_revoked();
    a->set_job_id("job-1");
    a->set_attempt_id("att-1");
    a->set_fencing_token(2);
    r.add_revoked()->set_job_id("job-2");
    client.answer(client.hb, Result<cv1::HeartbeatReply>(r));
    BOOST_REQUIRE(run_until([&] { return revoked.size() == 1u; }));
    BOOST_REQUIRE(revoked[0].size() == 2u);
    BOOST_TEST(revoked[0][0].job_id() == "job-1");
    BOOST_TEST(revoked[0][0].attempt_id() == "att-1");
    BOOST_TEST(revoked[0][0].fencing_token() == 2u);
    BOOST_TEST(revoked[0][1].job_id() == "job-2");
    BOOST_TEST(agent->state() == ControlState::kConnected);
}

BOOST_FIXTURE_TEST_CASE(phase4_hooks_optional, Fixture) {
    phase4_hooks = false;
    connect(policy(1, 0, 0, 0));
    BOOST_REQUIRE(run_until([&] { return !client.hb.pending.empty(); }));
    cv1::HeartbeatReply r;
    r.add_revoked()->set_job_id("job-1");
    client.answer(client.hb, Result<cv1::HeartbeatReply>(r));
    BOOST_REQUIRE(run_until([&] { return client.heartbeats.size() == 2u; }));
    BOOST_TEST(registered.empty());
    BOOST_TEST(revoked.empty());
}

BOOST_FIXTURE_TEST_CASE(destroy_with_pending_callbacks_is_safe, Fixture) {
    connect(policy(1, 0, 0, 0));
    BOOST_REQUIRE(run_until([&] { return !client.hb.pending.empty(); }));
    agent.reset();
    client.answer(client.hb, Result<cv1::HeartbeatReply>(cv1::HeartbeatReply{}));
    run_for(100ms);

    // Destroyed in the middle of stop(): done still runs exactly once.
    connect(policy(1, 0, 0, 0));
    int done = 0;
    agent->stop([&] { ++done; });
    agent.reset();
    client.answer(client.off, Result<cv1::NodeInfo>(cv1::NodeInfo{}));
    run_for(200ms);
    BOOST_TEST(done == 1);
}

BOOST_AUTO_TEST_SUITE_END()
