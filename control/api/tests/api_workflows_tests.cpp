// Workflow endpoints, spawn/checkpoint plumbing, sweeps (docs/design/phase6.md §8, §10).
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
tls::Identity admin(const std::string& n = "alice") { return ident(tls::Role::kAdmin, n); }

std::string err_code(const http::Response& r) {
    return std::string(boost::json::parse(r.body).as_object().at("code").as_string());
}

template <class M>
M parse(const http::Response& r) {
    M m;
    std::string err;
    BOOST_REQUIRE_MESSAGE(from_json(r.body, m, &err), "bad reply JSON: " << err << " body=" << r.body);
    return m;
}

std::string art(int n) { return std::format("artifact://sha256/{:064x}", n); }

tv1::StepSpec step(const std::string& name, std::vector<std::string> deps = {}) {
    tv1::StepSpec s;
    s.set_name(name);
    s.mutable_job()->mutable_command()->add_argv("/bin/true");
    for (auto& d : deps) s.add_depends_on(d);
    return s;
}

std::string wf_body(std::vector<tv1::StepSpec> steps, const std::string& name = "wf") {
    cv1::CreateWorkflowRequest r;
    r.set_name(name);
    for (auto& s : steps) *r.add_steps() = s;
    return to_json(r);
}

struct Base {
    std::int64_t now = 10'000'000;
    ApiOptions opts = [] {
        ApiOptions o;
        o.max_attempts_default = 4;
        o.workflow_limits.max_steps = 77;
        o.workflow_limits.max_spawn = 7;
        o.placement.reserve_window_ms = 1234;
        o.placement.w_transfer = 0.9;
        return o;
    }();
};

struct Fixture : Base {
    FakeRepository repo;
    ControlApi api{repo, opts, [this] { return now; }};

    http::Response call(const std::string& method, const std::string& path, std::optional<tls::Identity> peer,
                        std::string body = {}, std::map<std::string, std::string> query = {}) {
        http::Request r;
        r.method = method;
        r.path = path;
        r.query = std::move(query);
        r.body = std::move(body);
        if (!r.body.empty()) r.content_type = "application/json";  // like tdrctl and tdrd
        r.peer = std::move(peer);
        r.remote = "127.0.0.1:5555";
        return api.handle(r);
    }
};

}  // namespace

BOOST_AUTO_TEST_SUITE(cpapi_workflows)

BOOST_FIXTURE_TEST_CASE(limits_applied_at_construction, Fixture) {
    BOOST_TEST(repo.limits_set == 1);
    BOOST_TEST(repo.workflow_limits.max_steps == 77u);
    BOOST_TEST(repo.workflow_limits.max_spawn == 7u);
}

BOOST_FIXTURE_TEST_CASE(create_get_list_and_audit, Fixture) {
    auto a = step("a");
    a.mutable_job()->mutable_policy()->set_max_attempts(2);
    auto r = call("POST", "/api/v1/workflows", admin(), wf_body({a, step("b", {"a"})}, "diamond"));
    BOOST_REQUIRE_MESSAGE(r.status == 201, r.status << " " << r.body);
    auto w = parse<cv1::Workflow>(r);
    BOOST_TEST(w.workflow_id().starts_with("wf-"));
    BOOST_TEST(w.steps_size() == 2);
    BOOST_TEST(w.submitted_by() == "admin:alice");
    // max_attempts default applied to templates that leave it 0, kept otherwise.
    BOOST_TEST(repo.last_workflow_request.steps(0).job().policy().max_attempts() == 2u);
    BOOST_TEST(repo.last_workflow_request.steps(1).job().policy().max_attempts() == 4u);
    BOOST_REQUIRE(!repo.audit_rows.empty());
    BOOST_TEST(repo.audit_rows.back().action == "workflow.create");
    BOOST_TEST(repo.audit_rows.back().target == w.workflow_id());
    auto d = boost::json::parse(repo.audit_rows.back().details_json).as_object();
    BOOST_TEST(d.at("steps").as_int64() == 2);
    BOOST_TEST(d.at("name").as_string() == "diamond");

    // node certs may submit too
    auto r2 = call("POST", "/api/v1/workflows", node("n1"), wf_body({step("x")}));
    BOOST_TEST(r2.status == 201);
    BOOST_TEST(parse<cv1::Workflow>(r2).submitted_by() == "node:n1");

    auto g = call("GET", "/api/v1/workflows/" + w.workflow_id(), node("n1"));
    BOOST_REQUIRE_EQUAL(g.status, 200);
    BOOST_TEST(parse<cv1::Workflow>(g).steps_size() == 2);
    BOOST_TEST(call("GET", "/api/v1/workflows/wf-nope", admin()).status == 404);

    auto l = call("GET", "/api/v1/workflows", admin());
    BOOST_REQUIRE_EQUAL(l.status, 200);
    auto list = parse<cv1::WorkflowList>(l);
    BOOST_REQUIRE_EQUAL(list.workflows_size(), 2);
    BOOST_TEST(list.workflows(0).steps_size() == 0);
    BOOST_TEST(parse<cv1::WorkflowList>(call("GET", "/api/v1/workflows", admin(), "", {{"state", "running"}}))
                   .workflows_size() == 2);
    BOOST_TEST(parse<cv1::WorkflowList>(call("GET", "/api/v1/workflows", admin(), "", {{"limit", "1"}}))
                   .workflows_size() == 1);
    BOOST_TEST(parse<cv1::WorkflowList>(call("GET", "/api/v1/workflows", admin(), "", {{"campaign", "camp-x"}}))
                   .workflows_size() == 0);
    auto bad = call("GET", "/api/v1/workflows", admin(), "", {{"state", "DONE"}});
    BOOST_TEST(bad.status == 400);
    BOOST_TEST(call("GET", "/api/v1/workflows", admin(), "", {{"limit", "0"}}).status == 400);
}

BOOST_FIXTURE_TEST_CASE(create_errors, Fixture) {
    auto r = call("POST", "/api/v1/workflows", admin(), wf_body({}));
    BOOST_TEST(r.status == 400);
    BOOST_TEST(err_code(r) == "bad_request");
    BOOST_TEST(r.body.find("no steps") != std::string::npos);
    BOOST_TEST(call("POST", "/api/v1/workflows", admin(), "{not json").status == 400);
    BOOST_TEST(call("POST", "/api/v1/workflows", admin(), wf_body({step("a")}, std::string(300, 'n'))).status == 400);
    cv1::CreateWorkflowRequest req;
    req.set_campaign_id("camp-nope");
    *req.add_steps() = step("a");
    BOOST_TEST(call("POST", "/api/v1/workflows", admin(), to_json(req)).status == 400);
    BOOST_TEST(call("POST", "/api/v1/workflows", std::nullopt, wf_body({step("a")})).status == 401);
    BOOST_TEST(repo.audit_rows.empty());
}

BOOST_FIXTURE_TEST_CASE(cancel_admin_only, Fixture) {
    auto w = parse<cv1::Workflow>(call("POST", "/api/v1/workflows", admin(), wf_body({step("a")})));
    const auto path = "/api/v1/workflows/" + w.workflow_id() + "/cancel";
    auto f = call("POST", path, node("n1"));
    BOOST_TEST(f.status == 403);
    auto c = call("POST", path, admin(), R"({"reason":"enough"})");
    BOOST_REQUIRE_EQUAL(c.status, 200);
    auto cw = parse<cv1::Workflow>(c);
    BOOST_TEST(cw.state() == "CANCELLED");
    BOOST_TEST(cw.error() == "enough");
    BOOST_TEST(repo.audit_rows.back().action == "workflow.cancel");
    BOOST_TEST(repo.audit_rows.back().target == w.workflow_id());
    auto c2 = call("POST", "/api/v1/workflows/wf-x/cancel", admin());
    BOOST_TEST(c2.status == 404);
    // default reason names the actor
    auto w2 = parse<cv1::Workflow>(call("POST", "/api/v1/workflows", admin(), wf_body({step("a")})));
    auto c3 = parse<cv1::Workflow>(call("POST", "/api/v1/workflows/" + w2.workflow_id() + "/cancel", admin("bob")));
    BOOST_TEST(c3.error() == "cancelled by admin:bob");
}

BOOST_FIXTURE_TEST_CASE(spawn_and_checkpoint_plumbing, Fixture) {
    cv1::RegisterRequest rr;
    rr.set_name("n1");
    auto nid = parse<cv1::RegisterReply>(call("POST", "/api/v1/nodes/register", node("n1"), to_json(rr))).node_id();
    cv1::CreateJobRequest jr;
    jr.mutable_spec()->mutable_command()->add_argv("/bin/true");
    auto job = parse<cv1::Job>(call("POST", "/api/v1/jobs", admin(), to_json(jr)));
    repo.jobs[job.job_id()].set_workflow_id("wf-1");  // pretend it is a step job
    cv1::LeaseRequest lr;
    lr.set_node_id(nid);
    lr.mutable_free()->set_cpu_cores(4);
    lr.mutable_free()->set_memory_bytes(1 << 30);
    lr.set_max_jobs(4);
    auto lease = parse<cv1::LeaseReply>(call("POST", "/api/v1/leases", node("n1"), to_json(lr))).leases(0);

    cv1::JobUpdate u;
    u.set_attempt_id(lease.attempt_id());
    u.set_fencing_token(lease.fencing_token());
    u.set_state(tv1::RUNNING);
    u.mutable_checkpoint()->set_artifact_id("bogus");
    const auto path = "/api/v1/jobs/" + job.job_id() + "/status";
    BOOST_TEST(call("POST", path, node("n1"), to_json(u)).status == 400);
    u.mutable_checkpoint()->set_artifact_id(art(7));
    u.mutable_checkpoint()->set_name("ck");
    auto r = call("POST", path, node("n1"), to_json(u));
    BOOST_REQUIRE_MESSAGE(r.status == 200, r.body);
    BOOST_TEST(repo.last_update.checkpoint().artifact_id() == art(7));
    BOOST_TEST(parse<cv1::Job>(r).checkpoint().name() == "ck");

    u.clear_checkpoint();
    u.set_state(tv1::COMPLETED);
    auto sp = step("child");
    *u.add_spawn() = sp;
    auto r2 = call("POST", path, node("n1"), to_json(u));
    BOOST_REQUIRE_MESSAGE(r2.status == 200, r2.body);
    BOOST_REQUIRE_EQUAL(repo.last_update.spawn_size(), 1);
    BOOST_TEST(repo.last_update.spawn(0).name() == "child");
    BOOST_TEST(repo.last_update.spawn(0).job().policy().max_attempts() == 4u);  // default applied

    // A standalone job that spawns: repository says kInvalid -> 400.
    auto job2 = parse<cv1::Job>(call("POST", "/api/v1/jobs", admin(), to_json(jr)));
    auto l2 = parse<cv1::LeaseReply>(call("POST", "/api/v1/leases", node("n1"), to_json(lr))).leases(0);
    cv1::JobUpdate u2;
    u2.set_attempt_id(l2.attempt_id());
    u2.set_fencing_token(l2.fencing_token());
    u2.set_state(tv1::COMPLETED);
    *u2.add_spawn() = sp;
    auto r3 = call("POST", "/api/v1/jobs/" + job2.job_id() + "/status", node("n1"), to_json(u2));
    BOOST_TEST(r3.status == 400);
    BOOST_TEST(r3.body.find("invalid spawn") != std::string::npos);
}

BOOST_FIXTURE_TEST_CASE(sweeps_forward_options, Fixture) {
    repo.sweep_result = 3;
    BOOST_TEST(api.sweep_workflows() == 3u);
    BOOST_TEST(repo.workflow_sweeps == 1);
    repo.reservations.push_back(cpdb::Reservation{"job-1", "ff-node-1", 0.25, now + 1234});
    auto rs = api.place_jobs();
    BOOST_REQUIRE_EQUAL(rs.size(), 1u);
    BOOST_TEST(repo.place_calls == 1);
    BOOST_TEST(repo.last_placement.reserve_window_ms == 1234);
    BOOST_TEST(repo.last_placement.w_transfer == 0.9);
    repo.fail_all = true;
    BOOST_CHECK_THROW(api.sweep_workflows(), cpdb::DbError);
}

// Real Postgres: workflow submitted over the API, driven by lease + status updates.
BOOST_AUTO_TEST_CASE(pg_workflow_end_to_end) {
    cpdb::testing::TempCluster cluster;
    auto repo = cpdb::open_pg_repository(cluster.conninfo());
    std::int64_t now = 20'000'000;
    ControlApi api(*repo, ApiOptions{}, [&] { return now; });
    auto call = [&](const std::string& method, const std::string& path, tls::Identity peer, std::string body = {}) {
        http::Request r;
        r.method = method;
        r.path = path;
        r.body = std::move(body);
        if (!r.body.empty()) r.content_type = "application/json";  // like tdrctl and tdrd
        r.peer = std::move(peer);
        return api.handle(r);
    };
    cv1::RegisterRequest rr;
    rr.set_name("n1");
    auto nid = parse<cv1::RegisterReply>(call("POST", "/api/v1/nodes/register", node("n1"), to_json(rr))).node_id();
    auto join = step("join", {"fan"});
    auto* in = join.add_inputs();
    in->set_step("fan");
    in->set_output("r");
    in->set_spawned(true);
    auto created = call("POST", "/api/v1/workflows", admin(), wf_body({step("fan"), join}));
    BOOST_REQUIRE_MESSAGE(created.status == 201, created.body);
    const auto wid = parse<cv1::Workflow>(created).workflow_id();

    cv1::LeaseRequest lr;
    lr.set_node_id(nid);
    lr.mutable_free()->set_cpu_cores(16);
    lr.mutable_free()->set_memory_bytes(std::uint64_t{16} << 30);
    lr.set_max_jobs(16);
    auto lease_all = [&] {
        auto rep = parse<cv1::LeaseReply>(call("POST", "/api/v1/leases", node("n1"), to_json(lr)));
        return std::vector<cv1::Lease>(rep.leases().begin(), rep.leases().end());
    };
    auto done = [&](const cv1::Lease& l, std::vector<cv1::ArtifactRef> outs, std::vector<tv1::StepSpec> spawn) {
        cv1::JobUpdate u;
        u.set_attempt_id(l.attempt_id());
        u.set_fencing_token(l.fencing_token());
        u.set_state(tv1::COMPLETED);
        for (auto& o : outs) *u.add_outputs() = o;
        for (auto& s : spawn) *u.add_spawn() = s;
        ++now;
        return call("POST", "/api/v1/jobs/" + l.job_id() + "/status", node("n1"), to_json(u));
    };
    auto ls = lease_all();
    BOOST_REQUIRE_EQUAL(ls.size(), 1u);
    BOOST_TEST(ls[0].spec().step() == "fan");
    BOOST_TEST(ls[0].spec().policy().max_attempts() == 3u);  // ApiOptions default reached the step job
    BOOST_TEST(done(ls[0], {}, {step("a"), step("b")}).status == 200);
    auto kids = lease_all();
    BOOST_REQUIRE_EQUAL(kids.size(), 2u);
    int n = 0;
    for (const auto& k : kids) {
        cv1::ArtifactRef o;
        o.set_artifact_id(art(++n));
        o.set_name("r");
        o.set_size(1);
        BOOST_TEST(done(k, {o}, {}).status == 200);
    }
    auto j = lease_all();
    BOOST_REQUIRE_EQUAL(j.size(), 1u);
    BOOST_TEST(j[0].spec().step() == "join");
    BOOST_TEST(j[0].spec().input_artifacts_size() == 2);
    BOOST_TEST(done(j[0], {}, {}).status == 200);
    auto w = parse<cv1::Workflow>(call("GET", "/api/v1/workflows/" + wid, admin()));
    BOOST_TEST(w.state() == "SUCCEEDED");
    BOOST_TEST(w.steps_size() == 4);
    BOOST_TEST(api.sweep_workflows() == 0u);
    BOOST_TEST(api.place_jobs().empty());
    auto job = parse<cv1::Job>(call("GET", "/api/v1/jobs/" + j[0].job_id(), admin()));
    BOOST_TEST(job.workflow_id() == wid);
    BOOST_TEST(job.step() == "join");
}

BOOST_AUTO_TEST_SUITE_END()
