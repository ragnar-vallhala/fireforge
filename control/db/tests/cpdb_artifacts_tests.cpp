// Phase 5 control-plane pieces on a throwaway PostgreSQL cluster
// (docs/design/phase5.md §4 placement, §6 release/pin, §9).
#include <libpq-fe.h>

#include <boost/test/unit_test.hpp>
#include <atomic>
#include <barrier>
#include <format>
#include <string>
#include <thread>

#include "tdr/cpdb/db.hpp"
#include "tdr/cpdb/testing.hpp"

using namespace tdr::cpdb;

namespace {

constexpr std::int64_t T0 = 1'700'000'000'000;  // ms
constexpr std::int64_t TTL = 60'000;
constexpr std::int64_t WAIT = 15'000;
constexpr std::uint64_t GiB = std::uint64_t{1} << 30;

testing::TempCluster& cluster() {
    static testing::TempCluster c;
    return c;
}

// First column of the first row ("" if none).
std::string psql(const std::string& conninfo, const std::string& sql) {
    PGconn* c = PQconnectdb(conninfo.c_str());
    BOOST_REQUIRE(PQstatus(c) == CONNECTION_OK);
    PGresult* r = PQexec(c, sql.c_str());
    std::string out;
    auto st = PQresultStatus(r);
    BOOST_REQUIRE_MESSAGE(st == PGRES_TUPLES_OK || st == PGRES_COMMAND_OK, PQresultErrorMessage(r));
    if (st == PGRES_TUPLES_OK && PQntuples(r) > 0) out = PQgetvalue(r, 0, 0);
    PQclear(r);
    PQfinish(c);
    return out;
}

std::string art(char c) { return "artifact://sha256/" + std::string(64, c); }

tv1::JobSpec spec(const std::string& id, std::vector<std::string> inputs = {}) {
    tv1::JobSpec s;
    s.set_job_id(id);
    s.mutable_command()->add_argv("/bin/true");
    s.mutable_resources()->set_cpu_cores(1);
    s.mutable_policy()->set_max_attempts(5);
    for (auto& i : inputs) s.add_input_artifacts(i);
    return s;
}

cv1::LeaseRequest lreq(const std::string& node, std::uint32_t max_jobs = 100) {
    cv1::LeaseRequest r;
    r.set_node_id(node);
    r.mutable_free()->set_cpu_cores(64);
    r.mutable_free()->set_memory_bytes(256 * GiB);
    r.set_max_jobs(max_jobs);
    return r;
}

struct Db {
    std::string conninfo;
    std::unique_ptr<IRepository> repo;

    Db() {
        static int n = 0;
        conninfo = cluster().create_database("p5_" + std::to_string(++n));
        repo = open_pg_repository(conninfo);
    }

    std::string node(const std::string& name) {
        cv1::RegisterRequest r;
        r.set_name(name);
        r.add_capabilities("cpu");
        return repo->register_node(r, "", T0).node_id();
    }

    std::string job(const tv1::JobSpec& s, std::int64_t at = T0) {
        auto j = repo->create_job(s, "admin:alice", at);
        BOOST_REQUIRE_MESSAGE(j, j.error());
        return j->job_id();
    }

    void hold(const std::string& node_id, std::vector<std::string> ids, std::int64_t at = T0) {
        cv1::ArtifactAnnouncement a;
        a.set_node_id(node_id);
        for (auto& id : ids) {
            auto* r = a.add_artifacts();
            r->set_artifact_id(id);
            r->set_size(10);
        }
        BOOST_REQUIRE(repo->announce_artifacts(a, at));
    }

    std::size_t lease(const std::string& node_id, std::int64_t now, std::int64_t wait = WAIT) {
        return repo->lease_jobs(lreq(node_id), TTL, wait, now).size();
    }

    // queued_at in unix ms ("" = NULL).
    std::string queued_at(const std::string& job_id) {
        return psql(conninfo, "SELECT (extract(epoch FROM queued_at) * 1000)::bigint FROM jobs WHERE job_id = '" +
                                  job_id + "'");
    }

    std::size_t copies(const std::string& id) {
        return std::stoul(psql(conninfo, "SELECT count(*) FROM artifact_locations WHERE artifact_id = '" + id + "'"));
    }
};

cv1::JobUpdate failed(const cv1::Lease& l) {
    cv1::JobUpdate u;
    u.set_attempt_id(l.attempt_id());
    u.set_fencing_token(l.fencing_token());
    u.set_state(tv1::FAILED);
    u.set_retryable(true);
    u.set_error("boom");
    return u;
}

}  // namespace

BOOST_AUTO_TEST_SUITE(cpdb_artifacts)

// ---- migration -------------------------------------------------------------------

BOOST_AUTO_TEST_CASE(migrates_v2_database_to_v3) {
    Db db;
    auto n = db.node("alpha");
    db.job(spec("q"), T0 - 500);
    db.job(spec("r"), T0 - 400);
    // Roll back to v2 (with a non-QUEUED job) and migrate again.
    psql(db.conninfo,
         "ALTER TABLE jobs DROP COLUMN workflow_id, DROP COLUMN step, DROP COLUMN checkpoint, DROP COLUMN strict,"
         " DROP COLUMN reserved_node, DROP COLUMN reserved_until, DROP COLUMN reserve_tried, DROP COLUMN reserve_done;"
         " DROP TABLE workflow_step_deps, workflow_steps, workflows;"
         " DROP TABLE artifact_pins; ALTER TABLE jobs DROP COLUMN queued_at;"
         " UPDATE jobs SET state = 'CANCELLED' WHERE job_id = 'r';"
         " DELETE FROM schema_migrations WHERE version >= 3");
    BOOST_TEST(schema_version(db.conninfo) == 2);
    auto repo = open_pg_repository(db.conninfo);
    BOOST_TEST(schema_version(db.conninfo) == 4);
    BOOST_TEST(db.queued_at("q") == std::to_string(T0 - 500));  // backfilled from created_at
    BOOST_TEST(db.queued_at("r") == "");                        // not queued
    BOOST_TEST(psql(db.conninfo, "SELECT count(*) FROM artifact_pins") == "0");
    BOOST_TEST(repo->lease_jobs(lreq(n), TTL, WAIT, T0).size() == 1u);
}

// ---- queued_at -------------------------------------------------------------------

BOOST_AUTO_TEST_CASE(queued_at_set_on_create_and_every_requeue) {
    Db db;
    auto n = db.node("alpha");
    auto j = db.job(spec("j"), T0 + 1);
    BOOST_TEST(db.queued_at(j) == std::to_string(T0 + 1));

    // Retryable failure -> QUEUED again.
    auto ls = db.repo->lease_jobs(lreq(n), TTL, WAIT, T0 + 10);
    BOOST_REQUIRE_EQUAL(ls.size(), 1u);
    auto o = db.repo->update_job(j, n, failed(ls[0]), T0 + 20);
    BOOST_REQUIRE(o.result == UpdateResult::kOk);
    BOOST_TEST(o.job->state() == tv1::QUEUED);
    BOOST_TEST(db.queued_at(j) == std::to_string(T0 + 20));

    // Lease expiry -> QUEUED again.
    ls = db.repo->lease_jobs(lreq(n), TTL, WAIT, T0 + 30);
    BOOST_REQUIRE_EQUAL(ls.size(), 1u);
    auto ex = db.repo->expire_leases(T0 + 30 + TTL + 1);
    BOOST_REQUIRE_EQUAL(ex.size(), 1u);
    BOOST_TEST(ex[0].new_state == tv1::QUEUED);
    BOOST_TEST(db.queued_at(j) == std::to_string(T0 + 30 + TTL + 1));

    // A terminal transition leaves queued_at alone.
    ls = db.repo->lease_jobs(lreq(n), TTL, WAIT, T0 + 200'000);
    BOOST_REQUIRE_EQUAL(ls.size(), 1u);
    cv1::JobUpdate done;
    done.set_attempt_id(ls[0].attempt_id());
    done.set_fencing_token(ls[0].fencing_token());
    done.set_state(tv1::COMPLETED);
    BOOST_REQUIRE(db.repo->update_job(j, n, done, T0 + 200'001).result == UpdateResult::kOk);
    BOOST_TEST(db.queued_at(j) == std::to_string(T0 + 30 + TTL + 1));
}

// ---- placement -------------------------------------------------------------------

BOOST_AUTO_TEST_CASE(holder_leases_at_once_others_after_wait) {
    Db db;
    auto holder = db.node("holder");
    auto other = db.node("other");
    db.hold(holder, {art('a'), art('b')});
    db.job(spec("j", {art('a'), art('b')}), T0);

    BOOST_TEST(db.lease(other, T0) == 0u);
    BOOST_TEST(db.lease(other, T0 + WAIT - 1) == 0u);
    BOOST_TEST(db.lease(holder, T0) == 1u);  // holder: no wait

    db.job(spec("k", {art('a'), art('b')}), T0);
    BOOST_TEST(db.lease(other, T0 + WAIT - 1) == 0u);
    BOOST_TEST(db.lease(other, T0 + WAIT) == 1u);  // now - queued_at >= wait
}

BOOST_AUTO_TEST_CASE(inputs_spread_over_holders_lease_after_wait) {
    Db db;
    auto a = db.node("a");
    auto b = db.node("b");
    auto c = db.node("c");
    db.hold(a, {art('1')});
    db.hold(b, {art('2')});
    db.job(spec("j", {art('1'), art('2')}), T0);
    BOOST_TEST(db.lease(a, T0) == 0u);  // a holds only one of them
    BOOST_TEST(db.lease(c, T0 + WAIT) == 1u);
}

BOOST_AUTO_TEST_CASE(input_without_online_holder_stays_queued) {
    Db db;
    auto holder = db.node("holder");
    auto other = db.node("other");
    db.hold(holder, {art('a')});
    db.job(spec("j", {art('a')}), T0);
    db.job(spec("nowhere", {art('z')}), T0);  // held by nobody
    BOOST_REQUIRE(db.repo->set_offline(holder, "test", T0 + 1));
    BOOST_TEST(db.lease(other, T0 + 10 * WAIT) == 0u);
    BOOST_TEST(db.lease(other, T0 + 10 * WAIT, 0) == 0u);
    // STALE is not ONLINE either.
    cv1::Heartbeat hb;
    hb.set_node_id(holder);
    BOOST_REQUIRE(db.repo->record_heartbeat(hb, T0 + 2));
    BOOST_REQUIRE_EQUAL(db.repo->sweep_health(T0 + 40'000, 30'000, 90'000).size(), 2u);  // both go STALE
    BOOST_REQUIRE(db.repo->record_heartbeat([&] {
        cv1::Heartbeat h;
        h.set_node_id(other);
        return h;
    }(), T0 + 40'001));
    BOOST_TEST(db.lease(other, T0 + 10 * WAIT) == 0u);
    // Holder back ONLINE: the waited job goes to the other node.
    BOOST_REQUIRE(db.repo->record_heartbeat(hb, T0 + 40'002));
    auto ls = db.repo->lease_jobs(lreq(other), TTL, WAIT, T0 + 10 * WAIT);
    BOOST_REQUIRE_EQUAL(ls.size(), 1u);
    BOOST_TEST(ls[0].job_id() == "j");
}

BOOST_AUTO_TEST_CASE(wait_zero_is_immediate_and_no_inputs_unaffected) {
    Db db;
    auto holder = db.node("holder");
    auto other = db.node("other");
    db.hold(holder, {art('a')});
    db.job(spec("plain"), T0);
    BOOST_TEST(db.lease(other, T0) == 1u);  // no inputs: never waits
    db.job(spec("j", {art('a')}), T0);
    BOOST_TEST(db.lease(other, T0, 0) == 1u);
}

BOOST_AUTO_TEST_CASE(requeue_restarts_the_wait) {
    Db db;
    auto holder = db.node("holder");
    auto other = db.node("other");
    db.hold(holder, {art('a')});
    auto j = db.job(spec("j", {art('a')}), T0);
    auto ls = db.repo->lease_jobs(lreq(holder), TTL, WAIT, T0 + 100'000);
    BOOST_REQUIRE_EQUAL(ls.size(), 1u);
    BOOST_REQUIRE(db.repo->update_job(j, holder, failed(ls[0]), T0 + 100'001).result == UpdateResult::kOk);
    BOOST_TEST(db.lease(other, T0 + 100'001 + WAIT - 1) == 0u);
    BOOST_TEST(db.lease(other, T0 + 100'001 + WAIT) == 1u);
}

// ---- release ---------------------------------------------------------------------

BOOST_AUTO_TEST_CASE(release_rules) {
    Db db;
    auto a = db.node("a");
    auto b = db.node("b");
    auto c = db.node("c");
    db.hold(a, {art('1'), art('2')});
    db.hold(b, {art('1')});

    auto r = db.repo->release_artifact(art('2'), a, false, T0);
    BOOST_TEST(!r.released());
    BOOST_TEST(r.reason() == "last copy");
    BOOST_TEST(db.copies(art('2')) == 1u);

    r = db.repo->release_artifact(art('1'), a, false, T0);  // b still has it
    BOOST_TEST(r.released());
    BOOST_TEST(r.reason() == "");
    BOOST_TEST(db.copies(art('1')) == 1u);
    BOOST_TEST((db.repo->get_artifact(art('1'))->node_ids(0) == b));

    r = db.repo->release_artifact(art('1'), b, false, T0);  // now the last copy
    BOOST_TEST(!r.released());
    BOOST_TEST(r.reason() == "last copy");

    // A node with no row of its own: released if someone else holds it.
    r = db.repo->release_artifact(art('1'), c, false, T0);
    BOOST_TEST(r.released());
    BOOST_TEST(db.copies(art('1')) == 1u);
    r = db.repo->release_artifact(art('9'), c, false, T0);  // nobody holds it
    BOOST_TEST(!r.released());
    BOOST_TEST(r.reason() == "last copy");

    // force drops even the last copy.
    r = db.repo->release_artifact(art('2'), a, true, T0);
    BOOST_TEST(r.released());
    BOOST_TEST(db.copies(art('2')) == 0u);
    BOOST_TEST(!db.repo->get_artifact(art('2')));
}

BOOST_AUTO_TEST_CASE(pinned_is_never_released_without_force) {
    Db db;
    auto a = db.node("a");
    auto b = db.node("b");
    db.hold(a, {art('p')});
    db.hold(b, {art('p')});
    auto info = db.repo->set_artifact_pinned(art('p'), true, "admin:alice", T0);
    BOOST_TEST(info.pinned());
    BOOST_TEST(info.node_ids_size() == 2);
    BOOST_TEST(psql(db.conninfo, "SELECT pinned_by FROM artifact_pins") == "admin:alice");

    auto r = db.repo->release_artifact(art('p'), a, false, T0);
    BOOST_TEST(!r.released());
    BOOST_TEST(r.reason() == "pinned");
    BOOST_TEST(db.copies(art('p')) == 2u);

    r = db.repo->release_artifact(art('p'), a, true, T0);
    BOOST_TEST(r.released());
    BOOST_TEST(db.copies(art('p')) == 1u);

    info = db.repo->set_artifact_pinned(art('p'), false, "admin:alice", T0);
    BOOST_TEST(!info.pinned());
    db.hold(a, {art('p')});
    BOOST_TEST(db.repo->release_artifact(art('p'), a, false, T0).released());
}

BOOST_AUTO_TEST_CASE(concurrent_release_of_last_two_copies) {
    Db db;
    auto a = db.node("a");
    auto b = db.node("b");
    auto ra = open_pg_repository(db.conninfo);
    auto rb = open_pg_repository(db.conninfo);
    for (int i = 0; i < 50; ++i) {
        const std::string id = "artifact://sha256/" + std::format("{:064x}", i + 1);
        db.hold(a, {id});
        db.hold(b, {id});
        std::barrier sync(2);
        std::atomic<int> released{0};
        auto run = [&](IRepository& repo, const std::string& node) {
            sync.arrive_and_wait();
            if (repo.release_artifact(id, node, false, T0).released()) ++released;
        };
        std::thread ta(run, std::ref(*ra), a);
        std::thread tb(run, std::ref(*rb), b);
        ta.join();
        tb.join();
        BOOST_TEST(released.load() == 1);
        BOOST_TEST(db.copies(id) == 1u);
    }
}

// ---- pins ------------------------------------------------------------------------

BOOST_AUTO_TEST_CASE(pin_without_locations) {
    Db db;
    BOOST_TEST(!db.repo->get_artifact(art('n')));
    auto info = db.repo->set_artifact_pinned(art('n'), true, "admin:bob", T0);
    BOOST_TEST(info.pinned());
    BOOST_TEST(info.artifact().artifact_id() == art('n'));
    BOOST_TEST(info.node_ids_size() == 0);
    // Pinning twice keeps the first pin.
    db.repo->set_artifact_pinned(art('n'), true, "admin:carol", T0 + 1);
    BOOST_TEST(psql(db.conninfo, "SELECT pinned_by FROM artifact_pins") == "admin:bob");

    auto got = db.repo->get_artifact(art('n'));
    BOOST_REQUIRE(got);
    BOOST_TEST(got->pinned());
    BOOST_TEST(got->artifact().artifact_id() == art('n'));
    BOOST_TEST(got->node_ids_size() == 0);

    // A later copy keeps the pin.
    auto a = db.node("a");
    db.hold(a, {art('n')});
    got = db.repo->get_artifact(art('n'));
    BOOST_TEST(got->pinned());
    BOOST_TEST(got->artifact().size() == 10u);

    info = db.repo->set_artifact_pinned(art('n'), false, "admin:bob", T0);
    BOOST_TEST(!info.pinned());
    BOOST_TEST(info.node_ids_size() == 1);
    // Unpinned and held nowhere: info carries only the id; get_artifact -> nullopt.
    BOOST_REQUIRE(db.repo->release_artifact(art('n'), a, true, T0).released());
    info = db.repo->set_artifact_pinned(art('n'), false, "admin:bob", T0);
    BOOST_TEST(!info.pinned());
    BOOST_TEST(info.artifact().artifact_id() == art('n'));
    BOOST_TEST(!db.repo->get_artifact(art('n')));
}

BOOST_AUTO_TEST_SUITE_END()
