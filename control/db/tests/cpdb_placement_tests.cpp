// Ranked placement: strict jobs, reservations, cost ranking (docs/design/phase6.md §7, §10).
#include <boost/test/unit_test.hpp>
#include <format>
#include <string>

#include "tdr/cpdb/db.hpp"
#include "tdr/cpdb/testing.hpp"

using namespace tdr::cpdb;

namespace {

constexpr std::int64_t T0 = 1'700'000'000'000;  // ms
constexpr std::int64_t TTL = 60'000;
constexpr std::int64_t WAIT = 15'000;
constexpr std::uint64_t MiB = std::uint64_t{1} << 20;
constexpr std::uint64_t GiB = std::uint64_t{1} << 30;

testing::TempCluster& cluster() {
    static testing::TempCluster c;
    return c;
}

std::string art(int n) { return std::format("artifact://sha256/{:064x}", n); }

cv1::LeaseRequest lreq(const std::string& node, std::uint32_t gpus = 0) {
    cv1::LeaseRequest r;
    r.set_node_id(node);
    r.mutable_free()->set_cpu_cores(64);
    r.mutable_free()->set_memory_bytes(256 * GiB);
    r.mutable_free()->set_gpu_count(gpus);
    r.set_max_jobs(10);
    return r;
}

PlacementOptions popts() {
    PlacementOptions o;
    o.reserve_window_ms = 5000;
    o.max_reserve_ms = 30000;
    o.strict_input_bytes = 64 * MiB;
    return o;
}

struct Db {
    std::string conninfo;
    std::unique_ptr<IRepository> repo;

    Db() {
        static int k = 0;
        conninfo = cluster().create_database("p" + std::to_string(++k));
        repo = open_pg_repository(conninfo);
    }

    std::string node(const std::string& name, std::uint32_t cpus = 8, std::uint32_t gpus = 0, double util = 0.1) {
        cv1::RegisterRequest r;
        r.set_name(name);
        r.add_capabilities("cpu");
        auto id = repo->register_node(r, "", T0).node_id();
        heartbeat(id, cpus, gpus, util);
        return id;
    }

    void heartbeat(const std::string& id, std::uint32_t cpus, std::uint32_t gpus = 0, double util = 0.1,
                   std::uint32_t running = 0) {
        cv1::Heartbeat hb;
        hb.set_node_id(id);
        hb.mutable_capacity()->set_cpu_cores(cpus);
        hb.mutable_capacity()->set_memory_bytes(64 * GiB);
        hb.mutable_capacity()->set_gpu_count(gpus);
        hb.set_cpu_utilization(util);
        hb.set_memory_available_bytes(64 * GiB);
        hb.set_running_jobs(running);
        BOOST_REQUIRE(repo->record_heartbeat(hb, T0));
    }

    void hold(const std::string& node, int artifact, std::uint64_t size) {
        cv1::ArtifactAnnouncement a;
        a.set_node_id(node);
        auto* x = a.add_artifacts();
        x->set_artifact_id(art(artifact));
        x->set_size(size);
        BOOST_REQUIRE(repo->announce_artifacts(a, T0));
    }

    std::string job(std::vector<int> inputs, std::uint32_t gpus = 0, std::int64_t at = T0) {
        tv1::JobSpec s;
        s.mutable_command()->add_argv("/bin/true");
        s.mutable_resources()->set_cpu_cores(1);
        s.mutable_resources()->set_gpu_count(gpus);
        s.mutable_policy()->set_max_attempts(3);
        for (int i : inputs) s.add_input_artifacts(art(i));
        auto j = repo->create_job(s, "admin:a", at);
        BOOST_REQUIRE_MESSAGE(j, j.error());
        return j->job_id();
    }

    std::size_t lease(const std::string& node, std::int64_t now, std::uint32_t gpus = 0) {
        return repo->lease_jobs(lreq(node, gpus), TTL, WAIT, now).size();
    }
};

}  // namespace

BOOST_AUTO_TEST_SUITE(cpdb_placement)

BOOST_AUTO_TEST_CASE(strict_job_waits_for_placement_then_goes_to_holder) {
    Db db;
    auto a = db.node("a");
    auto b = db.node("b");
    db.hold(b, 1, 100 * MiB);
    auto j = db.job({1});
    // A non-holder polling right after submit gets nothing, even after locality_wait:
    // a strict job is only leasable through place_jobs.
    BOOST_TEST(db.lease(a, T0 + 1) == 0u);
    BOOST_TEST(db.lease(a, T0 + WAIT + 1) == 0u);
    BOOST_TEST(db.lease(b, T0 + 1) == 0u);  // not even the holder before placement

    auto rs = db.repo->place_jobs(popts(), T0 + 2);
    BOOST_REQUIRE_EQUAL(rs.size(), 1u);
    BOOST_TEST(rs[0].job_id == j);
    BOOST_TEST(rs[0].node_id == b);
    BOOST_TEST(rs[0].until_ms == T0 + 2 + 5000);
    auto got = db.repo->get_job(j, false);
    BOOST_TEST(got->reserved_node() == b);
    BOOST_TEST(got->reserved_until_unix_ms() == T0 + 5002);
    BOOST_TEST(db.repo->place_jobs(popts(), T0 + 3).empty());  // live reservation: untouched

    BOOST_TEST(db.lease(a, T0 + WAIT + 2) == 0u);  // reserved for b
    auto ls = db.repo->lease_jobs(lreq(b), TTL, WAIT, T0 + 4);
    BOOST_REQUIRE_EQUAL(ls.size(), 1u);
    BOOST_TEST(ls[0].job_id() == j);
    BOOST_TEST(db.repo->get_job(j, false)->reserved_node().empty());  // cleared on lease
}

BOOST_AUTO_TEST_CASE(non_strict_jobs_are_not_held) {
    Db db;
    auto a = db.node("a");
    db.hold(a, 1, 10 * MiB);
    db.job({1});
    db.job({});
    BOOST_TEST(db.repo->place_jobs(popts(), T0).empty());
    BOOST_TEST(db.lease(a, T0 + 1) == 2u);
}

BOOST_AUTO_TEST_CASE(reservation_chain) {
    Db db;
    auto a = db.node("a");
    auto b = db.node("b");
    auto c = db.node("c");
    db.hold(b, 1, 80 * MiB);   // b holds most
    db.hold(c, 2, 20 * MiB);   // c holds the rest
    auto j = db.job({1, 2});
    auto t = T0 + 10;

    auto r1 = db.repo->place_jobs(popts(), t);
    BOOST_REQUIRE_EQUAL(r1.size(), 1u);
    BOOST_TEST(r1[0].node_id == b);  // least to transfer
    BOOST_TEST(db.lease(c, t + 1) == 0u);
    BOOST_TEST(db.lease(a, t + 1) == 0u);

    // b does not take it within the window: next best (c), then a.
    t += 5000;
    auto r2 = db.repo->place_jobs(popts(), t);
    BOOST_REQUIRE_EQUAL(r2.size(), 1u);
    BOOST_TEST(r2[0].node_id == c);
    BOOST_TEST(r2[0].cost > r1[0].cost);
    BOOST_TEST(db.lease(b, t + 1) == 0u);  // b's window is over
    t += 5000;
    auto r3 = db.repo->place_jobs(popts(), t);
    BOOST_REQUIRE_EQUAL(r3.size(), 1u);
    BOOST_TEST(r3[0].node_id == a);
    t += 5000;
    // Every eligible node had its window: released, leasable by anyone (with the
    // locality rule: queued long enough, inputs on ONLINE nodes).
    BOOST_TEST(db.repo->place_jobs(popts(), t).empty());
    auto got = db.repo->get_job(j, false);
    BOOST_TEST(got->reserved_node().empty());
    BOOST_TEST(db.lease(a, t + 1) == 1u);
}

BOOST_AUTO_TEST_CASE(max_reserve_releases) {
    Db db;
    auto a = db.node("a");
    auto b = db.node("b");
    db.hold(b, 1, 100 * MiB);
    auto j = db.job({1});
    BOOST_REQUIRE_EQUAL(db.repo->place_jobs(popts(), T0 + 1).size(), 1u);
    // b never leases; after max_reserve the job is released even though a is untried.
    auto o = popts();
    o.reserve_window_ms = 60'000;
    BOOST_TEST(db.lease(a, T0 + 20'000) == 0u);
    BOOST_TEST(db.repo->place_jobs(o, T0 + 30'000).empty());
    BOOST_TEST(db.repo->get_job(j, false)->reserved_node().empty());
    BOOST_TEST(db.lease(a, T0 + 30'001) == 1u);
}

BOOST_AUTO_TEST_CASE(cost_orders_by_transfer_and_load) {
    Db db;
    auto a = db.node("a");
    auto b = db.node("b");
    auto c = db.node("c");
    db.hold(a, 1, 50 * MiB);
    db.hold(b, 1, 50 * MiB);
    db.hold(b, 2, 50 * MiB);
    db.hold(c, 3, 1 * MiB);
    auto j = db.job({1, 2, 3});  // 101 MiB: b misses 1 MiB, a misses 51, c misses 100
    auto r = db.repo->place_jobs(popts(), T0 + 1);
    BOOST_REQUIRE_EQUAL(r.size(), 1u);
    BOOST_TEST(r[0].job_id == j);
    BOOST_TEST(r[0].node_id == b);

    // Same transfer cost: the less loaded / larger node wins.
    Db d2;
    auto x = d2.node("x", 8, 0, 0.9);
    auto y = d2.node("y", 16, 0, 0.1);
    d2.heartbeat(x, 8, 0, 0.9, 6);
    d2.hold(x, 1, 100 * MiB);
    d2.hold(y, 1, 100 * MiB);
    d2.job({1});
    auto r2 = d2.repo->place_jobs(popts(), T0 + 1);
    BOOST_REQUIRE_EQUAL(r2.size(), 1u);
    BOOST_TEST(r2[0].node_id == y);
}

BOOST_AUTO_TEST_CASE(gpu_jobs_and_eligibility) {
    Db db;
    auto cpu = db.node("cpu");
    auto gpu = db.node("gpu", 8, 2);
    auto j = db.job({}, 1);
    BOOST_TEST(db.lease(gpu, T0 + 1, 2) == 0u);  // strict, not placed yet
    auto r = db.repo->place_jobs(popts(), T0 + 2);
    BOOST_REQUIRE_EQUAL(r.size(), 1u);
    BOOST_TEST(r[0].node_id == gpu);  // the only node with a free GPU
    BOOST_TEST(db.lease(cpu, T0 + 3, 1) == 0u);
    BOOST_TEST(db.lease(gpu, T0 + 3, 2) == 1u);
    (void)j;

    // No eligible node at all: released at once.
    auto j2 = db.job({}, 4);
    BOOST_TEST(db.repo->place_jobs(popts(), T0 + 4).empty());
    BOOST_TEST(db.lease(gpu, T0 + 5, 4) == 1u);
    (void)j2;
}

BOOST_AUTO_TEST_CASE(requeue_resets_reservation) {
    Db db;
    auto a = db.node("a");
    auto b = db.node("b");
    db.hold(b, 1, 100 * MiB);
    auto j = db.job({1});
    BOOST_REQUIRE_EQUAL(db.repo->place_jobs(popts(), T0 + 1).size(), 1u);
    auto ls = db.repo->lease_jobs(lreq(b), TTL, WAIT, T0 + 2);
    BOOST_REQUIRE_EQUAL(ls.size(), 1u);
    cv1::JobUpdate u;
    u.set_attempt_id(ls[0].attempt_id());
    u.set_fencing_token(ls[0].fencing_token());
    u.set_state(tv1::FAILED);
    u.set_retryable(true);
    BOOST_TEST((db.repo->update_job(j, b, u, T0 + 3).result == UpdateResult::kOk));
    // Strict again and not placed: nobody may lease until place_jobs ran, and the
    // chain starts over (b is untried again).
    BOOST_TEST(db.lease(a, T0 + 4 + WAIT) == 0u);
    auto r = db.repo->place_jobs(popts(), T0 + 5);
    BOOST_REQUIRE_EQUAL(r.size(), 1u);
    BOOST_TEST(r[0].node_id == b);
}

BOOST_AUTO_TEST_SUITE_END()
