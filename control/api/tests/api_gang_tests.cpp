// Gang (MPI) jobs through the API: validation, [gangs] options, JSON shape
// (docs/design/phase7.md §2, §3, §6, §7).
#include <boost/json.hpp>
#include <boost/test/unit_test.hpp>

#include "fake_repo.hpp"
#include "tdr/cpapi/api.hpp"
#include "tdr/cpdb/testing.hpp"

using namespace tdr;
using namespace tdr::cpapi;
using tdr::cpapi::testing::FakeRepository;
namespace tv1 = ::fireforge::tdr::v1;

namespace {

tls::Identity ident(tls::Role role, const std::string& name) {
    tls::Identity id;
    id.role = role;
    id.name = name;
    id.fingerprint_sha256 = "fp-" + name;
    return id;
}
tls::Identity node(const std::string& n) { return ident(tls::Role::kNode, n); }
tls::Identity admin() { return ident(tls::Role::kAdmin, "alice"); }

std::string err_message(const http::Response& r) {
    return std::string(boost::json::parse(r.body).as_object().at("message").as_string());
}

template <class M>
M parse(const http::Response& r) {
    M m;
    std::string err;
    BOOST_REQUIRE_MESSAGE(from_json(r.body, m, &err), "bad reply JSON: " << err << " body=" << r.body);
    return m;
}

tv1::JobSpec gang_spec(std::uint32_t ranks, const std::string& argv0 = "./a.out") {
    tv1::JobSpec s;
    s.mutable_command()->add_argv(argv0);
    s.mutable_resources()->set_cpu_cores(1);
    s.mutable_mpi()->set_ranks(ranks);
    return s;
}

struct Fixture {
    FakeRepository repo;
    std::int64_t now = 10'000'000;
    ApiOptions opts = [] {
        ApiOptions o;
        o.gangs.max_ranks = 64;
        o.gangs.max_nodes = 4;
        o.gangs.drain_timeout_ms = 120'000;
        return o;
    }();
    ControlApi api{repo, opts, [this] { return now; }};

    http::Response call(const std::string& method, const std::string& path, tls::Identity peer, std::string body) {
        http::Request r;
        r.method = method;
        r.path = path;
        r.body = std::move(body);
        r.peer = std::move(peer);
        return api.handle(r);
    }
    http::Response submit(const tv1::JobSpec& s) {
        cv1::CreateJobRequest r;
        *r.mutable_spec() = s;
        return call("POST", "/api/v1/jobs", admin(), to_json(r));
    }
};

}  // namespace

BOOST_AUTO_TEST_SUITE(cpapi_gangs)

BOOST_FIXTURE_TEST_CASE(gang_options_reach_the_repository, Fixture) {
    BOOST_TEST(repo.gang_options_set == 1);
    BOOST_TEST(repo.gang_options.max_ranks == 64u);
    BOOST_TEST(repo.gang_options.max_nodes == 4u);
    BOOST_TEST(repo.gang_options.drain_timeout_ms == 120'000);
}

BOOST_FIXTURE_TEST_CASE(create_job_validates_mpi, Fixture) {
    BOOST_TEST(submit(gang_spec(8)).status == 201);

    auto r = submit(gang_spec(65));
    BOOST_TEST(r.status == 400);
    BOOST_TEST(err_message(r) == "spec.mpi.ranks 65 exceeds the maximum of 64");

    for (const char* launcher : {"mpirun", "/usr/bin/mpiexec", "orterun", "/opt/prterun"}) {
        r = submit(gang_spec(4, launcher));
        BOOST_TEST(r.status == 400);
        BOOST_TEST(err_message(r).find("must not start with") != std::string::npos);
    }
    // Without ranks, mpirun is just a program (a job running its own single-node MPI).
    auto plain = gang_spec(0, "mpirun");
    BOOST_TEST(submit(plain).status == 201);
    plain.mutable_mpi()->add_mpirun_args("--bind-to");
    BOOST_TEST(submit(plain).status == 400);

    auto spread = gang_spec(20);
    spread.mutable_mpi()->set_max_ranks_per_node(4);  // 5 nodes > max_nodes 4
    r = submit(spread);
    BOOST_TEST(r.status == 400);
    BOOST_TEST(err_message(r) == "spec.mpi.ranks 20 at 4 per node needs 5 nodes (at most 4)");
    spread.mutable_mpi()->set_max_ranks_per_node(5);
    BOOST_TEST(submit(spread).status == 201);
}

BOOST_FIXTURE_TEST_CASE(workflow_steps_are_validated, Fixture) {
    cv1::CreateWorkflowRequest w;
    w.set_name("w");
    auto* s = w.add_steps();
    s->set_name("sim");
    *s->mutable_job() = gang_spec(4, "mpiexec");
    auto r = call("POST", "/api/v1/workflows", admin(), to_json(w));
    BOOST_TEST(r.status == 400);
    BOOST_TEST(err_message(r).starts_with("step sim: job.command.argv"));
    *s->mutable_job() = gang_spec(4);
    BOOST_TEST(call("POST", "/api/v1/workflows", admin(), to_json(w)).status == 201);
}

BOOST_AUTO_TEST_CASE(pg_gang_flow) {
    std::unique_ptr<cpdb::testing::TempCluster> cluster;
    try {
        cluster = std::make_unique<cpdb::testing::TempCluster>();
    } catch (const std::exception& e) {
        BOOST_TEST_MESSAGE("skipping: no PostgreSQL cluster: " << e.what());
        return;
    }
    auto repo = cpdb::open_pg_repository(cluster->conninfo());
    std::int64_t now = 1'700'000'000'000;
    ControlApi api(*repo, ApiOptions{}, [&] { return now; });
    auto call = [&](const std::string& method, const std::string& path, tls::Identity peer, std::string body = {}) {
        http::Request r;
        r.method = method;
        r.path = path;
        r.body = std::move(body);
        r.peer = std::move(peer);
        return api.handle(r);
    };
    std::map<std::string, std::string> ids;
    for (std::string n : {"n1", "n2"})
        ids[n] = parse<cv1::RegisterReply>(call("POST", "/api/v1/nodes/register", node(n), R"({"name":")" + n + R"("})"))
                     .node_id();
    auto heartbeat = [&](const std::string& n, const std::string& peer) {
        return call("POST", "/api/v1/nodes/heartbeat", node(n),
                    R"({"node_id":")" + ids[n] + R"(","capacity":{"cpu_cores":4,"memory_bytes":"8589934592"},)" +
                        R"("peers":[{"node_id":")" + ids[peer] + R"(","state":"CONNECTED"}]})");
    };
    auto lease = [&](const std::string& n, int cpus) {
        auto r = call("POST", "/api/v1/leases", node(n),
                      R"({"node_id":")" + ids[n] + R"(","free":{"cpu_cores":)" + std::to_string(cpus) +
                          R"(,"memory_bytes":"8589934592"},"max_jobs":4})");
        BOOST_REQUIRE_EQUAL(r.status, 200);
        return parse<cv1::LeaseReply>(r);
    };
    BOOST_REQUIRE_EQUAL(heartbeat("n1", "n2").status, 200);
    BOOST_REQUIRE_EQUAL(heartbeat("n2", "n1").status, 200);
    BOOST_TEST(lease("n1", 0).leases_size() == 0);
    BOOST_TEST(lease("n2", 0).leases_size() == 0);

    auto s = call("POST", "/api/v1/jobs", admin(),
                  R"({"spec":{"command":{"argv":["./ring"]},"resources":{"cpu_cores":1},"mpi":{"ranks":6}}})");
    BOOST_REQUIRE_MESSAGE(s.status == 201, s.body);
    const auto job_id = parse<cv1::Job>(s).job_id();
    BOOST_TEST(call("POST", "/api/v1/jobs", admin(),
                    R"({"spec":{"command":{"argv":["mpirun","./ring"]},"mpi":{"ranks":2}}})")
                   .status == 400);

    BOOST_TEST(api.place_jobs().empty());  // gang holds are not reservations
    auto j = parse<cv1::Job>(call("GET", "/api/v1/jobs/" + job_id, admin()));
    BOOST_TEST(j.pending_reason() == "draining n1, n2 (0 s of 600 s)");
    BOOST_REQUIRE_EQUAL(j.gang_size(), 2);
    BOOST_TEST(j.gang(0).ranks() == 4u);
    BOOST_TEST(j.gang(0).state() == "HELD");
    BOOST_TEST(j.gang(1).rank_offset() == 4u);

    now += 1000;
    BOOST_TEST(lease("n1", 4).leases_size() == 0);
    auto r2 = lease("n2", 4);
    BOOST_REQUIRE_EQUAL(r2.leases_size(), 1);
    BOOST_TEST(r2.leases(0).gang().role() == cv1::GANG_MEMBER);
    auto r1 = lease("n1", 4);
    BOOST_REQUIRE_EQUAL(r1.leases_size(), 1);
    const auto& head = r1.leases(0);
    BOOST_TEST(head.gang().role() == cv1::GANG_HEAD);
    BOOST_TEST(head.gang().head_node_id() == ids["n1"]);

    const std::string upd =
        R"({"attempt_id":")" + head.attempt_id() + R"(","fencing_token":")" + std::to_string(head.fencing_token()) +
        R"(","state":)";
    BOOST_TEST(call("POST", "/api/v1/jobs/" + job_id + "/status", node("n2"), upd + R"("FAILED","error":"x","retryable":true})").status ==
               200);
    // The member's failure requeued the job; the head's report is now stale.
    BOOST_TEST(call("POST", "/api/v1/jobs/" + job_id + "/status", node("n1"), upd + R"("RUNNING"})").status == 409);
    j = parse<cv1::Job>(call("GET", "/api/v1/jobs/" + job_id, admin()));
    BOOST_TEST(j.state() == tv1::QUEUED);
    BOOST_TEST(j.last_error() == "member n2 failed: x");
    BOOST_REQUIRE_EQUAL(j.attempts_size(), 1);
    BOOST_REQUIRE_EQUAL(j.attempts(0).members_size(), 2);
    BOOST_TEST(j.attempts(0).members(1).state() == "FAILED");
}

BOOST_AUTO_TEST_SUITE_END()
