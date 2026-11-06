// Job queue, leases and artifact locations on a throwaway PostgreSQL cluster
// (docs/design/phase4.md §2-§4, §9).
#include <libpq-fe.h>

#include <boost/test/unit_test.hpp>
#include <atomic>
#include <set>
#include <string>
#include <thread>

#include "tdr/cpdb/db.hpp"
#include "tdr/cpdb/testing.hpp"

using namespace tdr::cpdb;

namespace {

constexpr std::int64_t T0 = 1'700'000'000'000;  // ms
constexpr std::int64_t TTL = 60'000;
constexpr std::uint64_t GiB = std::uint64_t{1} << 30;

testing::TempCluster& cluster() {
    static testing::TempCluster c;
    return c;
}

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

tv1::JobSpec spec(const std::string& id = {}) {
    tv1::JobSpec s;
    s.set_job_id(id);
    s.mutable_command()->add_argv("/bin/true");
    s.mutable_resources()->set_cpu_cores(1);
    s.mutable_policy()->set_max_attempts(3);
    return s;
}

cv1::LeaseRequest lreq(const std::string& node, std::uint32_t cpus = 64, std::uint64_t mem = 256 * GiB,
                       std::uint32_t gpus = 0, std::uint32_t max_jobs = 100) {
    cv1::LeaseRequest r;
    r.set_node_id(node);
    r.mutable_free()->set_cpu_cores(cpus);
    r.mutable_free()->set_memory_bytes(mem);
    r.mutable_free()->set_gpu_count(gpus);
    r.set_max_jobs(max_jobs);
    return r;
}

std::set<std::string> ids(const std::vector<cv1::Lease>& ls) {
    std::set<std::string> out;
    for (const auto& l : ls) out.insert(l.job_id());
    return out;
}

cv1::JobUpdate upd(const cv1::Lease& l, tv1::JobState s) {
    cv1::JobUpdate u;
    u.set_attempt_id(l.attempt_id());
    u.set_fencing_token(l.fencing_token());
    u.set_state(s);
    return u;
}

cv1::LeaseRef ref(const cv1::Lease& l) {
    cv1::LeaseRef r;
    r.set_job_id(l.job_id());
    r.set_attempt_id(l.attempt_id());
    r.set_fencing_token(l.fencing_token());
    return r;
}

cv1::ArtifactRef aref(const std::string& id, std::uint64_t size = 10) {
    cv1::ArtifactRef a;
    a.set_artifact_id(id);
    a.set_size(size);
    a.set_media_type("application/octet-stream");
    return a;
}

struct Db {
    std::string conninfo;
    std::unique_ptr<IRepository> repo;

    Db() {
        static int n = 0;
        conninfo = cluster().create_database("j" + std::to_string(++n));
        repo = open_pg_repository(conninfo);
    }

    std::string node(const std::string& name, std::vector<std::string> caps = {"cpu"}) {
        cv1::RegisterRequest r;
        r.set_name(name);
        for (auto& c : caps) r.add_capabilities(c);
        return repo->register_node(r, "", T0).node_id();
    }

    std::string job(tv1::JobSpec s, std::int64_t at = T0) {
        auto j = repo->create_job(s, "admin:alice", at);
        BOOST_REQUIRE_MESSAGE(j, j.error());
        return j->job_id();
    }

    std::string campaign(const std::string& name, cv1::CampaignState to = cv1::DRAFT) {
        cv1::CreateCampaignRequest r;
        r.set_name(name);
        auto c = repo->create_campaign(r, "alice", T0);
        BOOST_REQUIRE(c);
        if (to != cv1::DRAFT) set_campaign(*c, to);
        return c->campaign_id();
    }

    void set_campaign(const std::string& id, cv1::CampaignState to) { set_campaign(*repo->get_campaign(id), to); }
    void set_campaign(cv1::Campaign c, cv1::CampaignState to) {
        auto from = c.state();
        c.set_state(to);
        BOOST_REQUIRE(repo->update_campaign(c, from, T0));
    }

    tv1::JobState state(const std::string& id) { return repo->get_job(id, false)->state(); }

    cv1::Lease lease_one(const std::string& node, std::int64_t now = T0) {
        auto ls = repo->lease_jobs(lreq(node, 64, 256 * GiB, 0, 1), TTL, now);
        BOOST_REQUIRE_EQUAL(ls.size(), 1u);
        return ls[0];
    }
};

}  // namespace

BOOST_AUTO_TEST_SUITE(cpdb_jobs)

BOOST_AUTO_TEST_CASE(migrates_v1_database_to_v2) {
    Db db;
    auto n = db.node("alpha");
    // Roll this database back to a v1 schema that holds data.
    psql(db.conninfo,
         "DROP TABLE artifact_syncs, artifact_locations, job_attempts, jobs;"
         " DELETE FROM schema_migrations WHERE version = 2");
    BOOST_TEST(schema_version(db.conninfo) == 1);
    auto repo = open_pg_repository(db.conninfo);
    BOOST_TEST(schema_version(db.conninfo) == 2);
    BOOST_TEST(repo->get_node(n)->name() == "alpha");
    auto j = repo->create_job(spec(), "admin:a", T0);
    BOOST_REQUIRE(j);
    BOOST_TEST(repo->lease_jobs(lreq(n), TTL, T0).size() == 1u);
}

BOOST_AUTO_TEST_CASE(create_get_list) {
    Db db;
    auto s = spec();
    s.set_operation("transport.calculate");
    s.mutable_resources()->set_cpu_cores(0);
    s.mutable_resources()->set_memory_bytes(3 * GiB);
    s.mutable_command()->mutable_env()->insert({"K", "v'\\\""});
    s.add_required_capabilities("cgroups");
    s.add_input_artifacts(art('a'));
    s.set_parameters_json(R"({"x": [1, 2]})");
    auto j = db.repo->create_job(s, "admin:alice", T0);
    BOOST_REQUIRE(j);
    BOOST_TEST(j->job_id().starts_with("job-"));
    BOOST_TEST(j->job_id().size() == 4u + 16u);
    BOOST_TEST(j->spec().job_id() == j->job_id());
    BOOST_TEST(j->state() == tv1::QUEUED);
    BOOST_TEST(j->submitted_by() == "admin:alice");
    BOOST_TEST(j->created_unix_ms() == T0);
    BOOST_TEST(j->updated_unix_ms() == T0);
    BOOST_TEST(j->attempts_used() == 0u);
    BOOST_TEST(!j->cancel_requested());
    BOOST_TEST(j->spec().operation() == "transport.calculate");
    BOOST_TEST(j->spec().command().env().at("K") == "v'\\\"");
    BOOST_TEST(j->spec().parameters_json() == R"({"x": [1, 2]})");
    BOOST_TEST(j->spec().resources().memory_bytes() == 3 * GiB);
    BOOST_TEST(j->spec().input_artifacts(0) == art('a'));

    auto fixed = spec("my.job-1");
    auto j2 = db.repo->create_job(fixed, "node:n1", T0 + 10);
    BOOST_REQUIRE(j2);
    BOOST_TEST(j2->job_id() == "my.job-1");
    auto dup = db.repo->create_job(fixed, "node:n1", T0 + 11);
    BOOST_REQUIRE(!dup);
    BOOST_TEST(dup.error() == "job id already exists");
    auto bad = spec();
    bad.set_campaign_id("camp-nope");
    auto bc = db.repo->create_job(bad, "x", T0);
    BOOST_REQUIRE(!bc);
    BOOST_TEST(bc.error() == "unknown campaign");

    auto camp = db.campaign("c1");
    auto cs = spec("in-camp");
    cs.set_campaign_id(camp);
    db.job(cs, T0 + 20);

    auto all = db.repo->list_jobs(tv1::JOB_STATE_UNSPECIFIED, "", 100);
    BOOST_REQUIRE_EQUAL(all.size(), 3u);
    BOOST_TEST(all[0].job_id() == "in-camp");  // newest first
    BOOST_TEST(all[2].job_id() == j->job_id());
    BOOST_TEST(db.repo->list_jobs(tv1::JOB_STATE_UNSPECIFIED, "", 1).size() == 1u);
    auto byc = db.repo->list_jobs(tv1::JOB_STATE_UNSPECIFIED, camp, 100);
    BOOST_REQUIRE_EQUAL(byc.size(), 1u);
    BOOST_TEST(byc[0].spec().campaign_id() == camp);
    BOOST_TEST(db.repo->list_jobs(tv1::RUNNING, "", 100).empty());
    BOOST_TEST(db.repo->list_jobs(tv1::QUEUED, "", 100).size() == 3u);
    BOOST_TEST(!db.repo->get_job("job-missing", true));
}

BOOST_AUTO_TEST_CASE(cancel_paths) {
    Db db;
    auto n = db.node("n1");
    auto q = db.job(spec("q"), T0);
    auto r = db.job(spec("r"), T0 + 1);

    BOOST_TEST((db.repo->cancel_job("missing", "", T0).result == CancelResult::kNotFound));

    // Lease r only (oldest first would pick q; cancel q first).
    auto c1 = db.repo->cancel_job(q, "no longer needed", T0 + 5);
    BOOST_TEST((c1.result == CancelResult::kCancelled));
    BOOST_REQUIRE(c1.job);
    BOOST_TEST(c1.job->state() == tv1::CANCELLED);
    BOOST_TEST(c1.job->last_error() == "no longer needed");
    BOOST_TEST((db.repo->cancel_job(q, "", T0 + 6).result == CancelResult::kTerminal));

    auto l = db.lease_one(n);
    BOOST_TEST(l.job_id() == r);
    auto c2 = db.repo->cancel_job(r, "", T0 + 7);
    BOOST_TEST((c2.result == CancelResult::kRequested));
    BOOST_TEST(c2.job->state() == tv1::LEASED);
    BOOST_TEST(c2.job->cancel_requested());
    // The holder's heartbeat now gets it revoked.
    auto revoked = db.repo->renew_leases(n, {ref(l)}, TTL, T0 + 8);
    BOOST_REQUIRE_EQUAL(revoked.size(), 1u);
    BOOST_TEST(revoked[0].attempt_id() == l.attempt_id());
    // Its CANCELLED report finishes the job.
    auto u = db.repo->update_job(r, n, upd(l, tv1::CANCELLED), T0 + 9);
    BOOST_TEST((u.result == UpdateResult::kOk));
    BOOST_TEST(u.job->state() == tv1::CANCELLED);
    auto full = db.repo->get_job(r, true);
    BOOST_REQUIRE_EQUAL(full->attempts_size(), 1);
    BOOST_TEST(full->attempts(0).state() == "CANCELLED");
    BOOST_TEST(full->attempts(0).ended_unix_ms() == T0 + 9);
}

BOOST_AUTO_TEST_CASE(cancel_requested_then_lease_expires_cancels) {
    Db db;
    auto n = db.node("n1");
    auto j = db.job(spec());
    db.lease_one(n);
    BOOST_TEST((db.repo->cancel_job(j, "", T0 + 1).result == CancelResult::kRequested));
    auto ex = db.repo->expire_leases(T0 + TTL + 1);
    BOOST_REQUIRE_EQUAL(ex.size(), 1u);
    BOOST_TEST(ex[0].new_state == tv1::CANCELLED);
    BOOST_TEST(db.state(j) == tv1::CANCELLED);
}

BOOST_AUTO_TEST_CASE(lease_grant_fields) {
    Db db;
    auto n = db.node("n1");
    auto j = db.job(spec());
    auto l = db.lease_one(n, T0 + 100);
    BOOST_TEST(l.job_id() == j);
    BOOST_TEST(l.attempt_id().starts_with("att-"));
    BOOST_TEST(l.attempt_id().size() == 4u + 16u);
    BOOST_TEST(l.attempt_number() == 1u);
    BOOST_TEST(l.fencing_token() == 1u);
    BOOST_TEST(l.expires_unix_ms() == T0 + 100 + TTL);
    BOOST_TEST(l.spec().job_id() == j);
    BOOST_TEST(l.spec().command().argv(0) == "/bin/true");
    auto got = db.repo->get_job(j, true);
    BOOST_TEST(got->state() == tv1::LEASED);
    BOOST_TEST(got->attempts_used() == 1u);
    BOOST_TEST(got->current_attempt_id() == l.attempt_id());
    BOOST_TEST(got->node_id() == n);
    BOOST_REQUIRE_EQUAL(got->attempts_size(), 1);
    BOOST_TEST(got->attempts(0).state() == "LEASED");
    BOOST_TEST(got->attempts(0).node_id() == n);
    BOOST_TEST(got->attempts(0).leased_unix_ms() == T0 + 100);
    BOOST_TEST(got->attempts(0).expires_unix_ms() == T0 + 100 + TTL);
    BOOST_TEST(got->attempts(0).ended_unix_ms() == 0);
    // Nothing left.
    BOOST_TEST(db.repo->lease_jobs(lreq(n), TTL, T0).empty());
}

BOOST_AUTO_TEST_CASE(lease_campaign_state_gates) {
    Db db;
    auto n = db.node("n1");
    auto camp = db.campaign("c1");  // DRAFT
    auto s = spec("cj");
    s.set_campaign_id(camp);
    db.job(s);
    BOOST_TEST(db.repo->lease_jobs(lreq(n), TTL, T0).empty());  // DRAFT holds
    db.set_campaign(camp, cv1::ACTIVE);
    db.set_campaign(camp, cv1::PAUSED);
    BOOST_TEST(db.repo->lease_jobs(lreq(n), TTL, T0).empty());  // PAUSED holds
    db.set_campaign(camp, cv1::ACTIVE);
    auto ls = db.repo->lease_jobs(lreq(n), TTL, T0);
    BOOST_REQUIRE_EQUAL(ls.size(), 1u);
    BOOST_TEST(ls[0].job_id() == "cj");
}

BOOST_AUTO_TEST_CASE(lease_capabilities_subset) {
    Db db;
    auto plain = db.node("plain", {"cpu"});
    auto gpu = db.node("gpu", {"cpu", "gpu:cuda", "cgroups"});
    auto s = spec("needs-cuda");
    s.add_required_capabilities("gpu:cuda");
    s.add_required_capabilities("cgroups");
    db.job(s);
    BOOST_TEST(db.repo->lease_jobs(lreq(plain), TTL, T0).empty());
    BOOST_TEST(db.repo->lease_jobs(lreq(gpu), TTL, T0).size() == 1u);
}

BOOST_AUTO_TEST_CASE(lease_locality) {
    Db db;
    auto a = db.node("a");
    auto b = db.node("b");
    auto s = spec("needs-inputs");
    s.add_input_artifacts(art('1'));
    s.add_input_artifacts(art('2'));
    db.job(s);
    cv1::ArtifactAnnouncement an;
    an.set_node_id(b);
    *an.add_artifacts() = aref(art('1'));
    BOOST_REQUIRE(db.repo->announce_artifacts(an, T0));
    // b has only one of two inputs.
    BOOST_TEST(db.repo->lease_jobs(lreq(b), TTL, T0).empty());
    an.clear_artifacts();
    *an.add_artifacts() = aref(art('2'));
    BOOST_REQUIRE(db.repo->announce_artifacts(an, T0));
    BOOST_TEST(db.repo->lease_jobs(lreq(a), TTL, T0).empty());  // a has none
    BOOST_TEST(db.repo->lease_jobs(lreq(b), TTL, T0).size() == 1u);
}

BOOST_AUTO_TEST_CASE(lease_requires_online_node) {
    Db db;
    auto n = db.node("n1");
    db.job(spec());
    BOOST_TEST(db.repo->lease_jobs(lreq("ff-node-unknown"), TTL, T0).empty());
    BOOST_REQUIRE(db.repo->set_offline(n, "maintenance", T0));
    BOOST_TEST(db.repo->lease_jobs(lreq(n), TTL, T0).empty());
    cv1::Heartbeat hb;
    hb.set_node_id(n);
    BOOST_REQUIRE(db.repo->record_heartbeat(hb, T0 + 1));
    // STALE also gets nothing.
    BOOST_TEST(db.repo->sweep_health(T0 + 40'000, 30'000, 90'000).size() == 1u);
    BOOST_TEST(db.repo->get_node(n)->health() == cv1::STALE);
    BOOST_TEST(db.repo->lease_jobs(lreq(n), TTL, T0).empty());
    BOOST_REQUIRE(db.repo->record_heartbeat(hb, T0 + 40'001));
    BOOST_TEST(db.repo->lease_jobs(lreq(n), TTL, T0).size() == 1u);
}

BOOST_AUTO_TEST_CASE(lease_fit_and_greedy_skip) {
    Db db;
    auto n = db.node("n1");
    auto mk = [&](const std::string& id, std::uint32_t cpu, std::uint64_t mem, std::uint32_t gpu, std::int64_t at) {
        auto s = spec(id);
        s.mutable_resources()->set_cpu_cores(cpu);
        s.mutable_resources()->set_memory_bytes(mem);
        s.mutable_resources()->set_gpu_count(gpu);
        db.job(s, at);
    };
    mk("big", 16, 1 * GiB, 0, T0 + 1);      // too many cpus: skipped, not blocking
    mk("zero-cpu", 0, 0, 0, T0 + 2);        // counts as 1 cpu; memory 0 always fits
    mk("mem-hog", 1, 64 * GiB, 0, T0 + 3);  // too much memory
    mk("gpu", 1, 1 * GiB, 1, T0 + 4);       // no gpus free
    mk("fits-a", 2, 2 * GiB, 0, T0 + 5);
    mk("fits-b", 1, 2 * GiB, 0, T0 + 6);    // takes the last cpu
    mk("fits-c", 1, 1 * GiB, 0, T0 + 7);    // no cpu left

    auto ls = db.repo->lease_jobs(lreq(n, 4, 8 * GiB, 0, 10), TTL, T0 + 10);
    BOOST_TEST((ids(ls) == std::set<std::string>{"zero-cpu", "fits-a", "fits-b"}));
    BOOST_REQUIRE_EQUAL(ls.size(), 3u);
    BOOST_TEST(ls[0].job_id() == "zero-cpu");  // oldest first
    BOOST_TEST(ls[1].job_id() == "fits-a");

    // max_jobs caps grants; gpu fits once gpus are free.
    auto l2 = db.repo->lease_jobs(lreq(n, 64, 256 * GiB, 1, 2), TTL, T0 + 11);
    BOOST_TEST((ids(l2) == std::set<std::string>{"big", "mem-hog"}));
    auto l3 = db.repo->lease_jobs(lreq(n, 64, 256 * GiB, 1, 10), TTL, T0 + 12);
    BOOST_TEST((ids(l3) == std::set<std::string>{"gpu", "fits-c"}));

    // memory fit is cumulative.
    mk("m1", 1, 3 * GiB, 0, T0 + 20);
    mk("m2", 1, 3 * GiB, 0, T0 + 21);
    mk("m3", 1, 1 * GiB, 0, T0 + 22);
    auto l4 = db.repo->lease_jobs(lreq(n, 64, 4 * GiB, 0, 10), TTL, T0 + 23);
    BOOST_TEST((ids(l4) == std::set<std::string>{"m1", "m3"}));
    // zero capacity / zero slots -> nothing.
    BOOST_TEST(db.repo->lease_jobs(lreq(n, 0, 256 * GiB, 0, 10), TTL, T0).empty());
    BOOST_TEST(db.repo->lease_jobs(lreq(n, 64, 256 * GiB, 0, 0), TTL, T0).empty());
    BOOST_TEST(db.repo->lease_jobs(lreq(n), TTL, T0).size() == 1u);  // m2
}

BOOST_AUTO_TEST_CASE(two_connections_never_double_grant) {
    Db db;
    auto a = db.node("a");
    auto b = db.node("b");
    for (int i = 0; i < 10; ++i) db.job(spec("s" + std::to_string(i)), T0 + i);
    auto other = open_pg_repository(db.conninfo);
    // Sequential, separate connections.
    auto la = db.repo->lease_jobs(lreq(a, 64, 256 * GiB, 0, 4), TTL, T0);
    auto lb = other->lease_jobs(lreq(b, 64, 256 * GiB, 0, 100), TTL, T0);
    BOOST_TEST(la.size() == 4u);
    BOOST_TEST(lb.size() == 6u);
    for (const auto& id : ids(la)) BOOST_TEST(!ids(lb).contains(id));

    // A lease transaction holding row locks makes the other connection skip them.
    for (int i = 0; i < 3; ++i) db.job(spec("x" + std::to_string(i)), T0 + 100 + i);
    PGconn* hold = PQconnectdb(db.conninfo.c_str());
    BOOST_REQUIRE(PQstatus(hold) == CONNECTION_OK);
    PQclear(PQexec(hold, "BEGIN"));
    PGresult* locked = PQexec(hold, "SELECT job_id FROM jobs WHERE job_id = 'x0' FOR UPDATE");
    BOOST_REQUIRE(PQresultStatus(locked) == PGRES_TUPLES_OK);
    PQclear(locked);
    auto skip = other->lease_jobs(lreq(b), TTL, T0);  // must not block on x0
    BOOST_TEST((ids(skip) == std::set<std::string>{"x1", "x2"}));
    PQclear(PQexec(hold, "ROLLBACK"));
    PQfinish(hold);
    BOOST_TEST((ids(db.repo->lease_jobs(lreq(a), TTL, T0)) == std::set<std::string>{"x0"}));
}

BOOST_AUTO_TEST_CASE(concurrent_lease_threads_disjoint) {
    Db db;
    constexpr int kJobs = 300;
    const std::string a = db.node("a"), b = db.node("b");
    for (int i = 0; i < kJobs; ++i) db.job(spec("c" + std::to_string(i)), T0 + i);
    std::vector<std::string> got[2];
    std::atomic<bool> failed{false};
    auto worker = [&](int k, const std::string& node) {
        try {
            auto repo = open_pg_repository(db.conninfo);
            for (;;) {
                auto ls = repo->lease_jobs(lreq(node, 64, 256 * GiB, 0, 3), TTL, T0 + 1000);
                if (ls.empty()) break;
                for (auto& l : ls) got[k].push_back(l.job_id());
            }
        } catch (const std::exception& e) {
            failed = true;
            BOOST_TEST_MESSAGE("worker failed: " << e.what());
        }
    };
    std::thread t1(worker, 0, a), t2(worker, 1, b);
    t1.join();
    t2.join();
    BOOST_REQUIRE(!failed);
    std::set<std::string> all(got[0].begin(), got[0].end());
    const std::size_t n0 = all.size();
    BOOST_TEST(n0 == got[0].size());
    all.insert(got[1].begin(), got[1].end());
    BOOST_TEST(all.size() == got[0].size() + got[1].size());  // disjoint
    BOOST_TEST(all.size() == static_cast<std::size_t>(kJobs));
    BOOST_TEST(psql(db.conninfo, "SELECT count(*) FROM job_attempts") == std::to_string(kJobs));
    BOOST_TEST_MESSAGE("split " << got[0].size() << " / " << got[1].size());
}

BOOST_AUTO_TEST_CASE(fencing) {
    Db db;
    auto n = db.node("n1");
    auto other = db.node("n2");
    auto j = db.job(spec());
    auto l = db.lease_one(n);

    auto bad_token = upd(l, tv1::RUNNING);
    bad_token.set_fencing_token(2);
    BOOST_TEST((db.repo->update_job(j, n, bad_token, T0).result == UpdateResult::kStale));
    auto bad_attempt = upd(l, tv1::RUNNING);
    bad_attempt.set_attempt_id("att-0000000000000000");
    BOOST_TEST((db.repo->update_job(j, n, bad_attempt, T0).result == UpdateResult::kStale));
    auto wrong_node = db.repo->update_job(j, other, upd(l, tv1::RUNNING), T0);
    BOOST_TEST((wrong_node.result == UpdateResult::kStale));
    BOOST_TEST(!wrong_node.message.empty());
    BOOST_TEST((db.repo->update_job("nope", n, upd(l, tv1::RUNNING), T0).result == UpdateResult::kNotFound));
    BOOST_TEST((db.repo->update_job(j, n, upd(l, tv1::QUEUED), T0).result == UpdateResult::kInvalid));
    BOOST_TEST((db.repo->update_job(j, n, upd(l, tv1::LEASED), T0).result == UpdateResult::kInvalid));
    BOOST_TEST(db.state(j) == tv1::LEASED);

    auto run = upd(l, tv1::RUNNING);
    run.set_progress_fraction(0.25);
    run.set_progress_message("step 1");
    auto ok = db.repo->update_job(j, n, run, T0 + 1);
    BOOST_REQUIRE((ok.result == UpdateResult::kOk));
    BOOST_TEST(ok.job->state() == tv1::RUNNING);
    BOOST_TEST(ok.job->progress_fraction() == 0.25);
    BOOST_TEST(ok.job->progress_message() == "step 1");
    BOOST_TEST(db.repo->get_job(j, true)->attempts(0).state() == "RUNNING");
    // Progress-only update while RUNNING; empty progress keeps the old one.
    BOOST_TEST(db.repo->update_job(j, n, upd(l, tv1::RUNNING), T0 + 2).job->progress_message() == "step 1");

    // Lease expires -> requeued; the old holder is fenced from then on.
    auto ex = db.repo->expire_leases(T0 + TTL + 1);
    BOOST_REQUIRE_EQUAL(ex.size(), 1u);
    BOOST_TEST(ex[0].job_id == j);
    BOOST_TEST(ex[0].attempt_id == l.attempt_id());
    BOOST_TEST(ex[0].node_id == n);
    BOOST_TEST(ex[0].new_state == tv1::QUEUED);
    BOOST_TEST((db.repo->update_job(j, n, upd(l, tv1::COMPLETED), T0 + TTL + 2).result == UpdateResult::kStale));
    auto l2 = db.lease_one(other, T0 + TTL + 3);
    BOOST_TEST(l2.fencing_token() == 2u);
    BOOST_TEST(l2.attempt_number() == 2u);
    BOOST_TEST((db.repo->update_job(j, n, upd(l, tv1::COMPLETED), T0 + TTL + 4).result == UpdateResult::kStale));
    BOOST_TEST((db.repo->update_job(j, other, upd(l2, tv1::COMPLETED), T0 + TTL + 5).result == UpdateResult::kOk));
    BOOST_TEST((db.repo->update_job(j, other, upd(l2, tv1::COMPLETED), T0 + TTL + 6).result == UpdateResult::kStale));
    auto full = db.repo->get_job(j, true);
    BOOST_REQUIRE_EQUAL(full->attempts_size(), 2);
    BOOST_TEST(full->attempts(0).state() == "ABANDONED");
    BOOST_TEST(full->attempts(0).error() == "lease expired");
    BOOST_TEST(full->attempts(1).state() == "SUCCEEDED");
    BOOST_TEST(full->node_id() == other);
}

BOOST_AUTO_TEST_CASE(retry_rules) {
    Db db;
    auto n = db.node("n1");
    auto s = spec("r");
    s.mutable_policy()->set_max_attempts(2);
    db.job(s);
    auto fail = [&](const cv1::Lease& l, bool retryable) {
        auto u = upd(l, tv1::FAILED);
        u.set_error("boom");
        u.set_retryable(retryable);
        auto o = db.repo->update_job("r", n, u, T0 + 5);
        BOOST_REQUIRE((o.result == UpdateResult::kOk));
        return *o.job;
    };
    auto j1 = fail(db.lease_one(n), true);
    BOOST_TEST(j1.state() == tv1::QUEUED);
    BOOST_TEST(j1.last_error() == "boom");
    auto j2 = fail(db.lease_one(n), true);  // attempt 2 of 2
    BOOST_TEST(j2.state() == tv1::FAILED);
    BOOST_TEST(j2.attempts_used() == 2u);
    BOOST_TEST(db.repo->lease_jobs(lreq(n), TTL, T0).empty());

    db.job(spec("nr"));
    auto l = db.lease_one(n);
    auto u = upd(l, tv1::FAILED);
    u.set_retryable(false);
    auto o = db.repo->update_job("nr", n, u, T0);
    BOOST_TEST(o.job->state() == tv1::FAILED);
    BOOST_TEST(o.job->last_error() == "failed");

    for (auto st : {tv1::EXPIRED, tv1::VALIDATION_FAILED}) {
        auto id = db.job(spec());
        auto lx = db.lease_one(n);
        auto ux = upd(lx, st);
        ux.set_error("why");
        ux.set_retryable(true);  // ignored: terminal
        auto ox = db.repo->update_job(id, n, ux, T0);
        BOOST_TEST(ox.job->state() == st);
        BOOST_TEST(ox.job->last_error() == "why");
        BOOST_TEST(db.repo->get_job(id, true)->attempts(0).state() == "FAILED");
    }

    // cancel_requested + retryable failure is not retried (db.hpp: FAILED).
    auto c = db.job(spec());
    auto lc = db.lease_one(n);
    db.repo->cancel_job(c, "", T0);
    auto uc = upd(lc, tv1::FAILED);
    uc.set_retryable(true);
    BOOST_TEST(db.repo->update_job(c, n, uc, T0).job->state() == tv1::FAILED);

    // Expiry with no attempts left -> FAILED.
    auto e = spec("e");
    e.mutable_policy()->set_max_attempts(1);
    db.job(e);
    db.lease_one(n, T0);
    auto ex = db.repo->expire_leases(T0 + TTL + 1);
    BOOST_REQUIRE_EQUAL(ex.size(), 1u);
    BOOST_TEST(ex[0].new_state == tv1::FAILED);
    auto ej = db.repo->get_job("e", false);
    BOOST_TEST(ej->state() == tv1::FAILED);
    BOOST_TEST(ej->last_error() == "lease expired");
}

BOOST_AUTO_TEST_CASE(renew_extends_and_revokes) {
    Db db;
    auto n = db.node("n1");
    auto m = db.node("n2");
    db.job(spec("a"), T0);
    db.job(spec("b"), T0 + 1);
    db.job(spec("c"), T0 + 2);
    auto ls = db.repo->lease_jobs(lreq(n, 64, 256 * GiB, 0, 3), TTL, T0);
    BOOST_REQUIRE_EQUAL(ls.size(), 3u);
    BOOST_REQUIRE(db.repo->update_job("c", n, upd(ls[2], tv1::COMPLETED), T0).result == UpdateResult::kOk);

    auto forged = ref(ls[0]);
    forged.set_fencing_token(9);
    cv1::LeaseRef unknown;
    unknown.set_job_id("zzz");
    unknown.set_attempt_id("att-x");
    auto revoked = db.repo->renew_leases(n, {ref(ls[0]), forged, ref(ls[1]), unknown, ref(ls[2])}, TTL, T0 + 50'000);
    BOOST_REQUIRE_EQUAL(revoked.size(), 3u);  // forged, unknown, completed; input order
    BOOST_TEST(revoked[0].fencing_token() == 9u);
    BOOST_TEST(revoked[1].job_id() == "zzz");
    BOOST_TEST(revoked[2].job_id() == "c");
    // Another node cannot renew n's lease.
    BOOST_TEST(db.repo->renew_leases(m, {ref(ls[1])}, TTL, T0 + 50'000).size() == 1u);
    BOOST_TEST(db.repo->get_job("a", true)->attempts(0).expires_unix_ms() == T0 + 50'000 + TTL);
    BOOST_TEST(db.repo->renew_leases(n, {}, TTL, T0).empty());

    // Renewed leases survive the original expiry.
    BOOST_TEST(db.repo->expire_leases(T0 + TTL + 1).empty());
    auto ex = db.repo->expire_leases(T0 + 50'000 + TTL + 1);
    BOOST_TEST(ex.size() == 2u);
}

BOOST_AUTO_TEST_CASE(expire_skips_live_and_terminal) {
    Db db;
    auto n = db.node("n1");
    db.job(spec("a"), T0);
    db.job(spec("b"), T0 + 1);
    auto la = db.lease_one(n, T0);
    auto lb = db.lease_one(n, T0 + 30'000);
    BOOST_REQUIRE(db.repo->update_job("a", n, upd(la, tv1::RUNNING), T0 + 1).result == UpdateResult::kOk);
    BOOST_TEST(db.repo->expire_leases(T0 + TTL).empty());  // not strictly past
    auto ex = db.repo->expire_leases(T0 + TTL + 1);
    BOOST_REQUIRE_EQUAL(ex.size(), 1u);
    BOOST_TEST(ex[0].job_id == "a");  // RUNNING expires too
    BOOST_REQUIRE(db.repo->update_job("b", n, upd(lb, tv1::COMPLETED), T0 + 40'000).result == UpdateResult::kOk);
    BOOST_TEST(db.repo->expire_leases(T0 + 10 * TTL).empty());  // a requeued, b completed
}

BOOST_AUTO_TEST_CASE(completed_records_outputs_and_locations) {
    Db db;
    auto n = db.node("n1");
    auto j = db.job(spec());
    auto l = db.lease_one(n);
    auto u = upd(l, tv1::COMPLETED);
    *u.add_outputs() = aref(art('f'), 1234);
    auto o2 = aref(art('e'), 5);
    o2.set_producer_job("explicit");
    o2.set_created_unix_ms(T0 - 5);
    *u.add_outputs() = o2;
    auto o = db.repo->update_job(j, n, u, T0 + 77);
    BOOST_REQUIRE((o.result == UpdateResult::kOk));
    BOOST_TEST(o.job->state() == tv1::COMPLETED);
    BOOST_REQUIRE_EQUAL(o.job->outputs_size(), 2);
    BOOST_TEST(o.job->outputs(0).artifact_id() == art('f'));
    BOOST_TEST(o.job->outputs(0).size() == 1234u);
    BOOST_TEST(o.job->outputs(0).producer_job() == j);
    BOOST_TEST(o.job->outputs(0).created_unix_ms() == T0 + 77);
    BOOST_TEST(o.job->progress_fraction() == 1.0);
    BOOST_TEST(db.repo->get_job(j, true)->attempts(0).state() == "SUCCEEDED");

    auto info = db.repo->get_artifact(art('f'));
    BOOST_REQUIRE(info);
    BOOST_TEST(info->artifact().size() == 1234u);
    BOOST_TEST(info->artifact().producer_job() == j);
    BOOST_TEST(info->artifact().media_type() == "application/octet-stream");
    BOOST_REQUIRE_EQUAL(info->node_ids_size(), 1);
    BOOST_TEST(info->node_ids(0) == n);
    auto e = db.repo->get_artifact(art('e'));
    BOOST_TEST(e->artifact().producer_job() == "explicit");
    BOOST_TEST(e->artifact().created_unix_ms() == T0 - 5);
    BOOST_TEST(!db.repo->get_artifact(art('0')));
}

BOOST_AUTO_TEST_CASE(announcements_incremental_and_full_sync) {
    Db db;
    auto zed = db.node("zed");
    auto amy = db.node("amy");
    auto announce = [&](const std::string& node, std::vector<char> arts, std::string sync, bool full,
                        std::int64_t at) {
        cv1::ArtifactAnnouncement a;
        a.set_node_id(node);
        for (char c : arts) *a.add_artifacts() = aref(art(c));
        a.set_sync_id(sync);
        a.set_full(full);
        return db.repo->announce_artifacts(a, at);
    };
    auto holders = [&](char c) {
        auto i = db.repo->get_artifact(art(c));
        std::vector<std::string> out;
        if (i)
            for (auto& id : i->node_ids()) out.push_back(id);
        return out;
    };
    BOOST_TEST(!announce("ff-node-none", {'1'}, "", false, T0));
    BOOST_TEST(announce(zed, {'1', '2', '3', '1'}, "", false, T0));  // duplicate in one page is fine
    BOOST_TEST(announce(amy, {'1'}, "", false, T0));
    BOOST_TEST((holders('1') == std::vector<std::string>{amy, zed}));  // ordered by node name

    // zed full sync in two pages: {2} then {4}; meanwhile an incremental {5}.
    BOOST_TEST(announce(zed, {'2'}, "s1", false, T0 + 100));
    BOOST_TEST(announce(zed, {'5'}, "", false, T0 + 150));
    BOOST_TEST(holders('3').size() == 1u);  // not dropped mid-sync
    BOOST_TEST(announce(zed, {'4'}, "s1", true, T0 + 200));
    BOOST_TEST((holders('1') == std::vector<std::string>{amy}));
    BOOST_TEST(holders('2').size() == 1u);
    BOOST_TEST(holders('3').empty());
    BOOST_TEST(holders('4').size() == 1u);
    BOOST_TEST(holders('5').size() == 1u);  // announced during the sync: kept
    BOOST_TEST(psql(db.conninfo, "SELECT count(*) FROM artifact_syncs") == "0");

    // A one-page full sync without sync_id.
    BOOST_TEST(announce(amy, {'9'}, "", true, T0 + 300));
    BOOST_TEST(holders('1').empty());
    BOOST_TEST((holders('9') == std::vector<std::string>{amy}));
    // Empty full sync clears the node.
    BOOST_TEST(announce(zed, {}, "s2", true, T0 + 400));
    BOOST_TEST(psql(db.conninfo, "SELECT count(*) FROM artifact_locations WHERE node_id = '" + zed + "'") == "0");
}

BOOST_AUTO_TEST_SUITE_END()
