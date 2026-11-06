// Gangs: multi-node MPI placement, holds, readiness, gang attempts
// (docs/design/phase7.md §3, §4, §10).
#include <libpq-fe.h>

#include <boost/test/unit_test.hpp>
#include <format>
#include <map>
#include <set>
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

std::string art(int n) { return std::format("artifact://sha256/{:064x}", n); }

GangOptions gopts() {
    GangOptions g;
    g.drain_timeout_ms = 600'000;
    g.ready_window_ms = 5'000;
    g.max_nodes = 16;
    g.max_ranks = 4096;
    g.poll_recent_ms = 30'000;
    return g;
}

struct NodeShape {
    std::uint32_t cpus = 8;
    std::uint32_t gpus = 0;
};

struct Db {
    std::string conninfo;
    std::unique_ptr<IRepository> repo;
    std::map<std::string, std::string> ids;  // name -> node_id
    std::map<std::string, NodeShape> shape;  // name -> capacity
    std::map<std::string, std::set<std::string>> links;  // name -> names it reports CONNECTED

    explicit Db(GangOptions g = gopts()) {
        static int k = 0;
        conninfo = cluster().create_database("g" + std::to_string(++k));
        repo = open_pg_repository(conninfo);
        repo->set_gang_options(g);
    }

    // Registers a node, polls once at `polled` (gang eligibility). Peer links: see mesh().
    std::string node(const std::string& name, std::uint32_t cpus, std::uint32_t gpus = 0, std::int64_t polled = T0) {
        cv1::RegisterRequest r;
        r.set_name(name);
        r.add_capabilities("cpu");
        auto id = repo->register_node(r, "", T0).node_id();
        ids[name] = id;
        shape[name] = {cpus, gpus};
        heartbeat(name);
        if (polled != 0) poll(name, polled, 0, 0);
        return id;
    }

    void heartbeat(const std::string& name, std::int64_t at = T0) {
        cv1::Heartbeat hb;
        hb.set_node_id(ids.at(name));
        hb.mutable_capacity()->set_cpu_cores(shape[name].cpus);
        hb.mutable_capacity()->set_memory_bytes(64 * GiB);
        hb.mutable_capacity()->set_gpu_count(shape[name].gpus);
        hb.set_cpu_utilization(0.1);
        hb.set_memory_available_bytes(64 * GiB);
        for (const auto& peer : links[name]) {
            auto* p = hb.add_peers();
            p->set_node_id(ids.at(peer));
            p->set_name(peer);
            p->set_state("CONNECTED");
        }
        BOOST_REQUIRE(repo->record_heartbeat(hb, at));
    }

    // Every node reports every other one CONNECTED.
    void mesh() {
        for (const auto& [a, _] : ids)
            for (const auto& [b, __] : ids)
                if (a != b) links[a].insert(b);
        for (const auto& [a, _] : ids) heartbeat(a);
    }

    std::vector<cv1::Lease> poll(const std::string& name, std::int64_t now, std::uint32_t cpus,
                                 std::uint32_t max_jobs = 10, std::uint32_t gpus = 0) {
        cv1::LeaseRequest r;
        r.set_node_id(ids.at(name));
        r.mutable_free()->set_cpu_cores(cpus);
        r.mutable_free()->set_memory_bytes(64 * GiB);
        r.mutable_free()->set_gpu_count(gpus);
        r.set_max_jobs(max_jobs);
        return repo->lease_jobs(r, TTL, WAIT, now);
    }
    // Polls with the node's whole capacity free.
    std::vector<cv1::Lease> poll_free(const std::string& name, std::int64_t now) {
        return poll(name, now, shape[name].cpus, 10, shape[name].gpus);
    }

    static tv1::JobSpec gang_spec(std::uint32_t ranks, std::uint32_t cpu = 1, std::uint32_t per_node = 0,
                                  std::uint32_t gpu = 0) {
        tv1::JobSpec s;
        s.mutable_command()->add_argv("./a.out");
        s.mutable_resources()->set_cpu_cores(cpu);
        s.mutable_resources()->set_gpu_count(gpu);
        s.mutable_policy()->set_max_attempts(3);
        s.mutable_mpi()->set_ranks(ranks);
        s.mutable_mpi()->set_max_ranks_per_node(per_node);
        return s;
    }

    std::string gang(std::uint32_t ranks, std::uint32_t cpu = 1, std::uint32_t per_node = 0, std::uint32_t gpu = 0) {
        auto j = repo->create_job(gang_spec(ranks, cpu, per_node, gpu), "admin:a", T0);
        BOOST_REQUIRE_MESSAGE(j, j.error());
        return j->job_id();
    }

    std::string plain(std::uint32_t cpu = 1) {
        tv1::JobSpec s;
        s.mutable_command()->add_argv("/bin/true");
        s.mutable_resources()->set_cpu_cores(cpu);
        s.mutable_policy()->set_max_attempts(3);
        auto j = repo->create_job(s, "admin:a", T0);
        BOOST_REQUIRE_MESSAGE(j, j.error());
        return j->job_id();
    }

    void place(std::int64_t now) { (void)repo->place_jobs(PlacementOptions{}, now); }

    cv1::Job job(const std::string& id) {
        auto j = repo->get_job(id, true);
        BOOST_REQUIRE(j);
        return *j;
    }

    // "name:ranks:state" per member.
    std::vector<std::string> gang_of(const std::string& id) {
        std::vector<std::string> out;
        const auto j = job(id);
        for (const auto& m : j.gang()) {
            std::string name;
            for (const auto& [n, nid] : ids)
                if (nid == m.node_id()) name = n;
            out.push_back(std::format("{}:{}:{}", name, m.ranks(), m.state()));
        }
        return out;
    }

    UpdateOutcome update(const cv1::Lease& l, const std::string& name, tv1::JobState st, std::int64_t now,
                         const std::string& error = "", bool retryable = false) {
        cv1::JobUpdate u;
        u.set_attempt_id(l.attempt_id());
        u.set_fencing_token(l.fencing_token());
        u.set_state(st);
        u.set_error(error);
        u.set_retryable(retryable);
        return repo->update_job(l.job_id(), ids.at(name), u, now);
    }

    // Revoked refs from a heartbeat of `name` holding `l`.
    std::size_t renew(const cv1::Lease& l, const std::string& name, std::int64_t now) {
        cv1::LeaseRef r;
        r.set_job_id(l.job_id());
        r.set_attempt_id(l.attempt_id());
        r.set_fencing_token(l.fencing_token());
        return repo->renew_leases(ids.at(name), {r}, TTL, now).size();
    }

    // Places a gang on a + b (all free) and hands out both member leases.
    // Returns {head lease (a), member lease (b)}.
    std::pair<cv1::Lease, cv1::Lease> run_gang(const std::string& job_id, std::int64_t now) {
        place(now);
        auto la = poll_free("a", now);
        BOOST_REQUIRE_EQUAL(la.size(), 0u);  // b not ready yet
        auto lb = poll_free("b", now + 1);
        BOOST_REQUIRE_EQUAL(lb.size(), 1u);  // b completes the gang
        la = poll_free("a", now + 2);
        BOOST_REQUIRE_EQUAL(la.size(), 1u);  // a's lease on its next poll
        BOOST_REQUIRE(la[0].job_id() == job_id);
        return {la[0], lb[0]};
    }
};

}  // namespace

BOOST_AUTO_TEST_SUITE(cpdb_gangs)

BOOST_AUTO_TEST_CASE(migrates_v4_database_to_v5) {
    Db db;
    db.node("a", 8);
    auto p = db.plain();
    psql(db.conninfo,
         "DROP TABLE gang_holds, job_attempt_members; ALTER TABLE nodes DROP COLUMN last_lease_poll;"
         " ALTER TABLE jobs DROP COLUMN is_gang, DROP COLUMN pending_reason, DROP COLUMN gang_tried;"
         " DELETE FROM schema_migrations WHERE version >= 5");
    BOOST_TEST(schema_version(db.conninfo) == 4);
    auto repo = open_pg_repository(db.conninfo);
    BOOST_TEST(schema_version(db.conninfo) == 5);
    BOOST_TEST(psql(db.conninfo, "SELECT is_gang::text || pending_reason FROM jobs") == "false");
    db.repo = std::move(repo);
    auto l = db.poll_free("a", T0 + 1);
    BOOST_REQUIRE_EQUAL(l.size(), 1u);
    BOOST_TEST(l[0].job_id() == p);
    BOOST_TEST(!l[0].has_gang());
}

BOOST_AUTO_TEST_CASE(packs_ranks_onto_unequal_nodes_and_leases_each_member_once) {
    Db db;
    db.node("a", 16);
    db.node("b", 8);
    db.mesh();
    auto j = db.gang(20);
    db.place(T0 + 1);
    BOOST_TEST(db.gang_of(j) == (std::vector<std::string>{"a:16:HELD", "b:4:HELD"}), boost::test_tools::per_element());
    auto job = db.job(j);
    BOOST_TEST(job.state() == tv1::QUEUED);
    BOOST_TEST(job.pending_reason() == "draining a, b (0 s of 600 s)");
    BOOST_TEST(job.gang(0).role() == cv1::GANG_HEAD);
    BOOST_TEST(job.gang(1).rank_offset() == 16u);

    // a is free: ready, but b is not yet.
    BOOST_TEST(db.poll_free("a", T0 + 1000).empty());
    BOOST_TEST(db.gang_of(j) == (std::vector<std::string>{"a:16:READY", "b:4:HELD"}), boost::test_tools::per_element());
    db.place(T0 + 3000);
    BOOST_TEST(db.job(j).pending_reason() == "draining b (2 s of 600 s)");

    // b's poll completes the gang: it gets its MEMBER lease in this reply.
    auto lb = db.poll_free("b", T0 + 2000);
    BOOST_REQUIRE_EQUAL(lb.size(), 1u);
    const auto& g = lb[0].gang();
    BOOST_TEST(g.role() == cv1::GANG_MEMBER);
    BOOST_TEST(g.member_index() == 1u);
    BOOST_TEST(g.ranks() == 20u);
    BOOST_TEST(g.head_node_id() == db.ids["a"]);
    BOOST_REQUIRE_EQUAL(g.members_size(), 2);
    BOOST_TEST(g.members(0).name() == "a");
    BOOST_TEST(g.members(0).ranks() == 16u);
    BOOST_TEST(g.members(1).rank_offset() == 16u);
    BOOST_TEST(lb[0].spec().mpi().ranks() == 20u);  // full spec
    BOOST_TEST(lb[0].expires_unix_ms() == T0 + 2000 + TTL);

    job = db.job(j);
    BOOST_TEST(job.state() == tv1::LEASED);
    BOOST_TEST(job.node_id() == db.ids["a"]);  // the head
    BOOST_TEST(job.pending_reason().empty());
    BOOST_REQUIRE_EQUAL(job.attempts_size(), 1);
    BOOST_TEST(job.attempts(0).members_size() == 2);
    BOOST_TEST(db.gang_of(j) == (std::vector<std::string>{"a:16:LEASED", "b:4:LEASED"}),
               boost::test_tools::per_element());

    // a's HEAD lease on its next poll, once; same attempt and token.
    auto la = db.poll("a", T0 + 3000, 0, 0);  // delivered even with no free capacity
    BOOST_REQUIRE_EQUAL(la.size(), 1u);
    BOOST_TEST(la[0].gang().role() == cv1::GANG_HEAD);
    BOOST_TEST(la[0].attempt_id() == lb[0].attempt_id());
    BOOST_TEST(la[0].fencing_token() == lb[0].fencing_token());
    BOOST_TEST(la[0].expires_unix_ms() == T0 + 3000 + TTL);
    BOOST_TEST(db.poll_free("a", T0 + 4000).empty());
    BOOST_TEST(db.poll_free("b", T0 + 4000).empty());
}

BOOST_AUTO_TEST_CASE(max_ranks_per_node_spreads_ranks) {
    Db db;
    db.node("a", 16);
    db.node("b", 16);
    db.node("c", 16);
    db.mesh();
    auto j = db.gang(12, 1, 4);
    db.place(T0 + 1);
    BOOST_TEST(db.gang_of(j) == (std::vector<std::string>{"a:4:HELD", "b:4:HELD", "c:4:HELD"}),
               boost::test_tools::per_element());
}

BOOST_AUTO_TEST_CASE(gpu_slots_and_gangs_are_not_strict) {
    Db db;
    db.node("a", 32, 2);
    db.node("b", 32, 4);
    db.mesh();
    auto j = db.gang(6, 2, 0, 1);  // 2 cores + 1 GPU per rank: a has 2 slots, b 4
    BOOST_TEST(db.repo->place_jobs(PlacementOptions{}, T0 + 1).empty());  // no strict reservation
    BOOST_TEST(db.gang_of(j) == (std::vector<std::string>{"b:4:HELD", "a:2:HELD"}), boost::test_tools::per_element());
    // Hold resources are ranks x per rank: b needs 8 cores + 4 GPUs.
    BOOST_TEST(db.poll("b", T0 + 10, 8, 10, 3).empty());  // 3 GPUs free: not ready
    BOOST_TEST(db.gang_of(j)[0] == "b:4:HELD");
    BOOST_TEST(db.poll("b", T0 + 20, 8, 10, 4).empty());
    BOOST_TEST(db.gang_of(j)[0] == "b:4:READY");
}

BOOST_AUTO_TEST_CASE(node_not_connected_to_the_head_is_skipped) {
    Db db;
    db.node("a", 16);
    db.node("b", 8);
    db.node("c", 8);
    db.links["a"] = {"c"};
    db.links["b"] = {"a", "c"};  // b says it reaches a, but a doesn't report b
    db.links["c"] = {"a", "b"};
    for (auto n : {"a", "b", "c"}) db.heartbeat(n);
    auto j = db.gang(20);
    db.place(T0 + 1);
    BOOST_TEST(db.gang_of(j) == (std::vector<std::string>{"a:16:HELD", "c:4:HELD"}), boost::test_tools::per_element());

    // Nobody linked: a gang that needs two nodes can't be placed.
    Db d2;
    d2.node("a", 16);
    d2.node("b", 8);
    auto k = d2.gang(20);
    d2.place(T0 + 1);
    BOOST_TEST(d2.job(k).pending_reason() == "not enough capacity: 16 of 20 ranks on 2 eligible nodes");
    BOOST_TEST(d2.job(k).gang_size() == 0);
}

BOOST_AUTO_TEST_CASE(stale_poller_is_not_eligible) {
    Db db;
    db.node("a", 16);
    db.node("b", 8, 0, T0 - 60'000);  // last polled a minute before
    db.node("c", 8, 0, 0);            // never polled
    db.mesh();
    auto j = db.gang(20);
    db.place(T0);
    BOOST_TEST(db.job(j).pending_reason() == "not enough capacity: 16 of 20 ranks on 1 eligible nodes");
    BOOST_TEST(db.job(j).gang_size() == 0);
    (void)db.poll("b", T0 + 1000, 0, 0);  // a poll with nothing free still counts
    db.place(T0 + 2000);
    BOOST_TEST(db.gang_of(j) == (std::vector<std::string>{"a:16:HELD", "b:4:HELD"}), boost::test_tools::per_element());
}

BOOST_AUTO_TEST_CASE(holds_drain_capacity_from_ordinary_jobs) {
    Db db;
    db.node("a", 8);
    db.node("b", 8);
    db.mesh();
    auto g = db.gang(8);  // fits on a alone (a sorts first)
    db.place(T0 + 1);
    BOOST_TEST(db.gang_of(g) == (std::vector<std::string>{"a:8:HELD"}), boost::test_tools::per_element());
    auto p1 = db.plain(1);
    // a is busy (4 free): its hold (8) takes everything; no ordinary job.
    BOOST_TEST(db.poll("a", T0 + 100, 4).empty());
    BOOST_TEST(db.job(p1).state() == tv1::QUEUED);
    // b has no hold: it gets the ordinary job.
    auto lb = db.poll_free("b", T0 + 200);
    BOOST_REQUIRE_EQUAL(lb.size(), 1u);
    BOOST_TEST(lb[0].job_id() == p1);
    // Another gang can't hold b's capacity twice over: holds are subtracted in placement too.
    auto g2 = db.gang(16);
    db.place(T0 + 300);
    BOOST_TEST(db.job(g2).pending_reason() == "not enough capacity: 8 of 16 ranks on 1 eligible nodes");
    BOOST_TEST(static_cast<int>(db.repo->cancel_job(g2, "", T0 + 301).result) ==
               static_cast<int>(CancelResult::kCancelled));

    // a frees up: a gang of one is leased at once; the ordinary job behind it waits.
    auto p2 = db.plain(1);
    auto la = db.poll("a", T0 + 400, 9);
    BOOST_REQUIRE_EQUAL(la.size(), 2u);  // the gang (8 cores) plus p2 (1 core)
    BOOST_TEST(la[0].job_id() == g);
    BOOST_TEST(la[0].gang().role() == cv1::GANG_HEAD);
    BOOST_TEST(la[0].gang().members_size() == 1);
    BOOST_TEST(la[1].job_id() == p2);

    // A gang lease uses one job slot.
    auto g3 = db.gang(4);
    db.place(T0 + 500);
    BOOST_TEST(db.gang_of(g3).size() == 1u);
    auto p3 = db.plain(1);
    auto lx = db.poll(db.gang_of(g3)[0].substr(0, 1), T0 + 600, 8, 1);
    BOOST_REQUIRE_EQUAL(lx.size(), 1u);
    BOOST_TEST(lx[0].job_id() == g3);
    BOOST_TEST(db.job(p3).state() == tv1::QUEUED);
}

BOOST_AUTO_TEST_CASE(readiness_window) {
    Db db;
    db.node("a", 8);
    db.node("b", 8);
    db.mesh();
    auto j = db.gang(16);
    db.place(T0 + 1);
    BOOST_TEST(db.poll_free("a", T0 + 1000).empty());
    // b becomes ready 6 s later: a's readiness is older than the 5 s window.
    BOOST_TEST(db.poll_free("b", T0 + 7000).empty());
    BOOST_TEST(db.job(j).state() == tv1::QUEUED);
    // A busy poll clears readiness.
    BOOST_TEST(db.poll("a", T0 + 8000, 2).empty());
    BOOST_TEST(db.gang_of(j) == (std::vector<std::string>{"a:8:HELD", "b:8:READY"}), boost::test_tools::per_element());
    // a ready again within 5 s of b: the gang is leased.
    auto la = db.poll_free("a", T0 + 9000);
    BOOST_REQUIRE_EQUAL(la.size(), 1u);
    BOOST_TEST(la[0].gang().role() == cv1::GANG_HEAD);
    BOOST_TEST(db.poll_free("b", T0 + 9500).size() == 1u);
}

BOOST_AUTO_TEST_CASE(drain_timeout_repicks_preferring_untried_nodes) {
    auto g = gopts();
    g.drain_timeout_ms = 20'000;
    Db db(g);
    for (auto n : {"a", "b", "c", "d"}) db.node(n, 8);
    db.mesh();
    auto j = db.gang(16);
    db.place(T0);
    BOOST_TEST(db.gang_of(j) == (std::vector<std::string>{"a:8:HELD", "b:8:HELD"}), boost::test_tools::per_element());
    db.place(T0 + 10'000);
    BOOST_TEST(db.job(j).pending_reason() == "draining a, b (10 s of 20 s)");
    db.place(T0 + 20'000);  // timed out: a, b tried; c, d picked
    BOOST_TEST(db.gang_of(j) == (std::vector<std::string>{"c:8:HELD", "d:8:HELD"}), boost::test_tools::per_element());
    BOOST_TEST(db.job(j).pending_reason() == "draining c, d (0 s of 20 s)");
    // Everyone polls again; c, d time out too. No untried set left: any set may be used.
    for (auto n : {"a", "b", "c", "d"}) (void)db.poll(n, T0 + 30'000, 0, 0);
    db.place(T0 + 40'000);
    BOOST_TEST(db.gang_of(j).size() == 2u);
    // A held member going offline drops the hold at once.
    const auto head = db.job(j).gang(0).node_id();
    BOOST_REQUIRE(db.repo->set_offline(head, "test", T0 + 41'000));
    db.place(T0 + 42'000);
    const auto after = db.job(j);
    BOOST_TEST(after.gang_size() == 2);
    for (const auto& m : after.gang()) BOOST_TEST(m.node_id() != head);
}

BOOST_AUTO_TEST_CASE(concurrent_member_polls_create_one_attempt) {
    for (int round = 0; round < 5; ++round) {
        Db db;
        db.node("a", 8);
        db.node("b", 8);
        db.mesh();
        auto j = db.gang(16);
        db.place(T0 + 1);
        auto other = open_pg_repository(db.conninfo);
        other->set_gang_options(gopts());
        auto req = [&](const std::string& n) {
            cv1::LeaseRequest r;
            r.set_node_id(db.ids[n]);
            r.mutable_free()->set_cpu_cores(8);
            r.mutable_free()->set_memory_bytes(64 * GiB);
            r.set_max_jobs(4);
            return r;
        };
        const auto qa = req("a"), qb = req("b");
        std::vector<cv1::Lease> ra, rb;
        std::thread ta([&] { ra = db.repo->lease_jobs(qa, TTL, WAIT, T0 + 100); });
        std::thread tb([&] { rb = other->lease_jobs(qb, TTL, WAIT, T0 + 100); });
        ta.join();
        tb.join();
        // Whoever came second created the attempt; the other gets its lease now or next poll.
        if (ra.empty()) ra = db.poll("a", T0 + 200, 8);
        if (rb.empty()) rb = db.poll("b", T0 + 200, 8);
        BOOST_REQUIRE_EQUAL(ra.size(), 1u);
        BOOST_REQUIRE_EQUAL(rb.size(), 1u);
        BOOST_TEST(ra[0].attempt_id() == rb[0].attempt_id());
        BOOST_TEST(ra[0].gang().role() == cv1::GANG_HEAD);
        BOOST_TEST(rb[0].gang().role() == cv1::GANG_MEMBER);
        BOOST_TEST(db.job(j).attempts_size() == 1);
        BOOST_TEST(db.poll("a", T0 + 300, 8).empty());
        BOOST_TEST(db.poll("b", T0 + 300, 8).empty());
    }
}

BOOST_AUTO_TEST_CASE(member_updates_are_fenced_by_membership) {
    Db db;
    db.node("a", 8);
    db.node("b", 8);
    db.node("c", 8);
    db.links["a"] = {"b"};
    db.links["b"] = {"a"};
    db.heartbeat("a");
    db.heartbeat("b");
    auto j = db.gang(16);
    auto [la, lb] = db.run_gang(j, T0);
    // c is not a member.
    auto o = db.update(la, "c", tv1::RUNNING, T0 + 10);
    BOOST_TEST(static_cast<int>(o.result) == static_cast<int>(UpdateResult::kStale));
    BOOST_TEST(db.renew(la, "c", T0 + 10) == 1u);
    // Wrong token.
    auto bad = lb;
    bad.set_fencing_token(7);
    BOOST_TEST(static_cast<int>(db.update(bad, "b", tv1::RUNNING, T0 + 10).result) ==
               static_cast<int>(UpdateResult::kStale));
    // Members renew their own leases.
    BOOST_TEST(db.renew(la, "a", T0 + 20) == 0u);
    BOOST_TEST(db.renew(lb, "b", T0 + 20) == 0u);
}

BOOST_AUTO_TEST_CASE(head_terminal_decides_and_revokes_members) {
    Db db;
    db.node("a", 8);
    db.node("b", 8);
    db.mesh();
    auto j = db.gang(16);
    auto [la, lb] = db.run_gang(j, T0);
    BOOST_TEST(static_cast<int>(db.update(lb, "b", tv1::RUNNING, T0 + 10).result) == static_cast<int>(UpdateResult::kOk));
    BOOST_TEST(db.job(j).state() == tv1::LEASED);  // a member doesn't move the job
    BOOST_TEST(db.gang_of(j) == (std::vector<std::string>{"a:8:LEASED", "b:8:RUNNING"}), boost::test_tools::per_element());
    BOOST_TEST(static_cast<int>(db.update(la, "a", tv1::RUNNING, T0 + 20).result) == static_cast<int>(UpdateResult::kOk));
    BOOST_TEST(db.job(j).state() == tv1::RUNNING);

    // A non-head COMPLETED only marks that member.
    auto c = db.update(lb, "b", tv1::COMPLETED, T0 + 30);
    BOOST_TEST(static_cast<int>(c.result) == static_cast<int>(UpdateResult::kOk));
    BOOST_TEST(db.job(j).state() == tv1::RUNNING);
    BOOST_TEST(db.gang_of(j)[1] == "b:8:SUCCEEDED");
    BOOST_TEST(db.renew(lb, "b", T0 + 31) == 1u);  // done: revoked

    cv1::JobUpdate u;
    u.set_attempt_id(la.attempt_id());
    u.set_fencing_token(la.fencing_token());
    u.set_state(tv1::COMPLETED);
    auto* out = u.add_outputs();
    out->set_artifact_id(art(1));
    out->set_size(5);
    out->set_name("result");
    auto r = db.repo->update_job(j, db.ids["a"], u, T0 + 40);
    BOOST_REQUIRE(static_cast<int>(r.result) == static_cast<int>(UpdateResult::kOk));
    BOOST_TEST(r.job->state() == tv1::COMPLETED);
    BOOST_TEST(r.job->outputs_size() == 1);
    BOOST_TEST(db.gang_of(j) == (std::vector<std::string>{"a:8:SUCCEEDED", "b:8:SUCCEEDED"}),
               boost::test_tools::per_element());
    auto info = db.repo->get_artifact(art(1));
    BOOST_REQUIRE(info);
    BOOST_TEST(info->node_ids(0) == db.ids["a"]);

    // Head finishes while a member still runs: that member is revoked.
    auto k = db.gang(16);
    auto [ha, hb] = db.run_gang(k, T0 + 1000);
    BOOST_TEST(static_cast<int>(db.update(ha, "a", tv1::FAILED, T0 + 1100, "rank 0 died", false).result) ==
               static_cast<int>(UpdateResult::kOk));
    auto jk = db.job(k);
    BOOST_TEST(jk.state() == tv1::FAILED);
    BOOST_TEST(jk.gang(0).state() == "FAILED");
    BOOST_TEST(jk.gang(1).state() == "CANCELLED");
    BOOST_TEST(db.renew(hb, "b", T0 + 1200) == 1u);
    BOOST_TEST(static_cast<int>(db.update(hb, "b", tv1::FAILED, T0 + 1300, "orted killed").result) ==
               static_cast<int>(UpdateResult::kStale));
}

BOOST_AUTO_TEST_CASE(member_failure_fails_the_attempt_and_requeues) {
    Db db;
    db.node("a", 8);
    db.node("b", 8);
    db.mesh();
    auto j = db.gang(16);
    auto [la, lb] = db.run_gang(j, T0);
    auto o = db.update(lb, "b", tv1::FAILED, T0 + 10, "orted exited 1", true);
    BOOST_REQUIRE(static_cast<int>(o.result) == static_cast<int>(UpdateResult::kOk));
    auto job = db.job(j);
    BOOST_TEST(job.state() == tv1::QUEUED);
    BOOST_TEST(job.last_error() == "member b failed: orted exited 1");
    BOOST_TEST(job.attempts(0).state() == "FAILED");
    BOOST_TEST(job.attempts(0).members(0).state() == "CANCELLED");
    BOOST_TEST(job.attempts(0).members(1).state() == "FAILED");
    BOOST_TEST(job.gang_size() == 0);  // QUEUED again, not yet placed
    BOOST_TEST(db.renew(la, "a", T0 + 20) == 1u);
    BOOST_TEST(static_cast<int>(db.update(la, "a", tv1::RUNNING, T0 + 30).result) ==
               static_cast<int>(UpdateResult::kStale));

    // Retried as a unit: placed and leased again (attempt 2).
    auto [la2, lb2] = db.run_gang(j, T0 + 100);
    BOOST_TEST(la2.attempt_number() == 2u);
    BOOST_TEST(la2.attempt_id() == lb2.attempt_id());
    // A non-retryable member failure fails the job.
    BOOST_TEST(static_cast<int>(db.update(lb2, "b", tv1::FAILED, T0 + 200, "bad binary", false).result) ==
               static_cast<int>(UpdateResult::kOk));
    BOOST_TEST(db.job(j).state() == tv1::FAILED);
    BOOST_TEST(db.renew(la2, "a", T0 + 300) == 1u);
}

BOOST_AUTO_TEST_CASE(member_lease_expiry_fails_the_attempt) {
    Db db;
    db.node("a", 8);
    db.node("b", 8);
    db.mesh();
    auto j = db.gang(16);
    auto [la, lb] = db.run_gang(j, T0);  // b's lease expires at T0+1+TTL, a's at T0+2+TTL
    BOOST_TEST(db.renew(la, "a", T0 + 50'000) == 0u);
    BOOST_TEST(db.repo->expire_leases(T0 + TTL).empty());
    auto e = db.repo->expire_leases(T0 + TTL + 10);
    BOOST_REQUIRE_EQUAL(e.size(), 1u);
    BOOST_TEST(e[0].job_id == j);
    BOOST_TEST(e[0].node_id == db.ids["b"]);
    BOOST_TEST(e[0].new_state == tv1::QUEUED);
    auto job = db.job(j);
    BOOST_TEST(job.state() == tv1::QUEUED);
    BOOST_TEST(job.last_error() == "member b failed: lease expired");
    BOOST_TEST(job.attempts(0).state() == "ABANDONED");
    BOOST_TEST(job.attempts(0).members(1).state() == "ABANDONED");
    BOOST_TEST(db.renew(la, "a", T0 + TTL + 20) == 1u);
    BOOST_TEST(db.repo->expire_leases(T0 + 10 * TTL).empty());
}

BOOST_AUTO_TEST_CASE(cancel_revokes_every_member) {
    Db db;
    db.node("a", 8);
    db.node("b", 8);
    db.mesh();
    // QUEUED with holds: cancelled at once, holds gone.
    auto q = db.gang(16);
    db.place(T0);
    BOOST_TEST(db.gang_of(q).size() == 2u);
    BOOST_TEST(static_cast<int>(db.repo->cancel_job(q, "no", T0 + 1).result) ==
               static_cast<int>(CancelResult::kCancelled));
    BOOST_TEST(db.job(q).gang_size() == 0);
    BOOST_TEST(db.job(q).pending_reason().empty());

    auto j = db.gang(16);
    auto [la, lb] = db.run_gang(j, T0 + 10);
    BOOST_TEST(static_cast<int>(db.repo->cancel_job(j, "stop", T0 + 100).result) ==
               static_cast<int>(CancelResult::kRequested));
    BOOST_TEST(db.renew(la, "a", T0 + 110) == 1u);
    BOOST_TEST(db.renew(lb, "b", T0 + 110) == 1u);
    // The first member to report CANCELLED ends the job CANCELLED.
    BOOST_TEST(static_cast<int>(db.update(lb, "b", tv1::CANCELLED, T0 + 120).result) ==
               static_cast<int>(UpdateResult::kOk));
    BOOST_TEST(db.job(j).state() == tv1::CANCELLED);
    BOOST_TEST(db.job(j).last_error() == "stop");
    BOOST_TEST(static_cast<int>(db.update(la, "a", tv1::CANCELLED, T0 + 130).result) ==
               static_cast<int>(UpdateResult::kStale));
}

BOOST_AUTO_TEST_CASE(gang_step_in_a_workflow) {
    Db db;
    db.node("a", 8);
    db.node("b", 8);
    db.mesh();
    auto step = [](const std::string& name, std::vector<std::string> deps = {}) {
        tv1::StepSpec s;
        s.set_name(name);
        *s.mutable_job() = Db::gang_spec(16);
        s.mutable_job()->mutable_policy()->set_max_attempts(1);
        for (auto& d : deps) s.add_depends_on(d);
        return s;
    };
    cv1::CreateWorkflowRequest req;
    req.set_name("mpi");
    *req.add_steps() = step("sim");
    *req.add_steps() = step("post", {"sim"});
    auto w = db.repo->create_workflow(req, "admin:a", T0);
    BOOST_REQUIRE_MESSAGE(w, w.error());
    auto jobs = db.repo->list_jobs(tv1::JOB_STATE_UNSPECIFIED, "", 10);
    BOOST_REQUIRE_EQUAL(jobs.size(), 1u);
    const auto sim = jobs[0].job_id();
    auto [la, lb] = db.run_gang(sim, T0 + 10);
    // A member completing does not finish the step; the head does.
    BOOST_TEST(static_cast<int>(db.update(lb, "b", tv1::COMPLETED, T0 + 20).result) ==
               static_cast<int>(UpdateResult::kOk));
    BOOST_TEST(db.repo->list_jobs(tv1::JOB_STATE_UNSPECIFIED, "", 10).size() == 1u);
    BOOST_TEST(static_cast<int>(db.update(la, "a", tv1::COMPLETED, T0 + 30).result) ==
               static_cast<int>(UpdateResult::kOk));
    jobs = db.repo->list_jobs(tv1::JOB_STATE_UNSPECIFIED, "", 10);
    BOOST_REQUIRE_EQUAL(jobs.size(), 2u);  // post created once
    std::string post;
    for (const auto& x : jobs)
        if (x.job_id() != sim) post = x.job_id();
    BOOST_TEST(db.job(post).state() == tv1::QUEUED);

    // post: a member failure (not retryable, max_attempts 1) fails step and workflow once.
    auto [pa, pb] = db.run_gang(post, T0 + 100);
    BOOST_TEST(static_cast<int>(db.update(pb, "b", tv1::FAILED, T0 + 110, "segfault").result) ==
               static_cast<int>(UpdateResult::kOk));
    auto wf = db.repo->get_workflow(w->workflow_id(), true);
    BOOST_REQUIRE(wf);
    BOOST_TEST(wf->state() == "FAILED");
    BOOST_TEST(wf->step_counts().at("COMPLETED") == 1u);
    BOOST_TEST(wf->step_counts().at("FAILED") == 1u);
    BOOST_TEST(static_cast<int>(db.update(pa, "a", tv1::FAILED, T0 + 120, "mpirun exit 1").result) ==
               static_cast<int>(UpdateResult::kStale));
    BOOST_TEST(db.repo->get_workflow(w->workflow_id(), true)->step_counts().at("FAILED") == 1u);

    // MPI validation applies to step templates.
    cv1::CreateWorkflowRequest bad;
    bad.set_name("bad");
    auto s = step("x");
    s.mutable_job()->mutable_command()->set_argv(0, "/usr/bin/mpirun");
    *bad.add_steps() = s;
    auto e = db.repo->create_workflow(bad, "admin:a", T0);
    BOOST_REQUIRE(!e);
    BOOST_TEST(e.error().find("must not start with mpirun") != std::string::npos);
    s = step("x");
    s.mutable_job()->mutable_mpi()->set_ranks(5000);
    *bad.mutable_steps(0) = s;
    BOOST_TEST(!db.repo->create_workflow(bad, "admin:a", T0));
}

BOOST_AUTO_TEST_SUITE_END()
