// Job, lease, status and artifact endpoints (docs/design/phase4.md §6).
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

std::string hex(char c) { return std::string(64, c); }
std::string art(char c) { return "artifact://sha256/" + hex(c); }

tv1::JobSpec spec(const std::string& id = {}) {
    tv1::JobSpec s;
    s.set_job_id(id);
    s.mutable_command()->add_argv("/bin/echo");
    s.mutable_command()->add_argv("hi");
    s.mutable_resources()->set_cpu_cores(1);
    return s;
}

std::string submit_body(const tv1::JobSpec& s) {
    cv1::CreateJobRequest r;
    *r.mutable_spec() = s;
    return to_json(r);
}

struct Fixture {
    FakeRepository repo;
    std::int64_t now = 10'000'000;
    ApiOptions opts = [] {
        ApiOptions o;
        o.lease_ttl_seconds = 30;
        o.max_attempts_default = 4;
        return o;
    }();
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

    std::string register_node(const std::string& name) {
        cv1::RegisterRequest req;
        req.set_name(name);
        auto r = call("POST", "/api/v1/nodes/register", node(name), to_json(req));
        BOOST_REQUIRE_EQUAL(r.status, 200);
        return parse<cv1::RegisterReply>(r).node_id();
    }

    cv1::Job submit(const tv1::JobSpec& s, tls::Identity who = admin()) {
        auto r = call("POST", "/api/v1/jobs", who, submit_body(s));
        BOOST_REQUIRE_MESSAGE(r.status == 201, r.status << " " << r.body);
        return parse<cv1::Job>(r);
    }

    http::Response lease_call(const std::string& caller, const std::string& node_id, std::uint32_t max_jobs = 10) {
        cv1::LeaseRequest req;
        req.set_node_id(node_id);
        req.mutable_free()->set_cpu_cores(8);
        req.mutable_free()->set_memory_bytes(std::uint64_t{16} << 30);
        req.set_max_jobs(max_jobs);
        return call("POST", "/api/v1/leases", node(caller), to_json(req));
    }

    std::vector<cv1::Lease> lease(const std::string& caller, const std::string& node_id, std::uint32_t max_jobs = 10) {
        auto r = lease_call(caller, node_id, max_jobs);
        BOOST_REQUIRE_MESSAGE(r.status == 200, r.status << " " << r.body);
        auto rep = parse<cv1::LeaseReply>(r);
        return {rep.leases().begin(), rep.leases().end()};
    }

    http::Response status(const std::string& caller, const cv1::Lease& l, tv1::JobState st,
                          std::optional<tls::Identity> who = std::nullopt) {
        cv1::JobUpdate u;
        u.set_attempt_id(l.attempt_id());
        u.set_fencing_token(l.fencing_token());
        u.set_state(st);
        return call("POST", "/api/v1/jobs/" + l.job_id() + "/status", who ? *who : node(caller), to_json(u));
    }

    cv1::HeartbeatReply heartbeat(const std::string& name, const std::string& id, const std::vector<cv1::Lease>& ls) {
        cv1::Heartbeat hb;
        hb.set_node_id(id);
        hb.set_policy_version(1);
        for (const auto& l : ls) {
            auto* r = hb.add_leases();
            r->set_job_id(l.job_id());
            r->set_attempt_id(l.attempt_id());
            r->set_fencing_token(l.fencing_token());
        }
        auto r = call("POST", "/api/v1/nodes/heartbeat", node(name), to_json(hb));
        BOOST_REQUIRE_EQUAL(r.status, 200);
        return parse<cv1::HeartbeatReply>(r);
    }
};

}  // namespace

BOOST_AUTO_TEST_SUITE(cpapi_jobs)

BOOST_FIXTURE_TEST_CASE(submit_happy_path_and_audit, Fixture) {
    auto j = submit(spec());
    BOOST_TEST(j.job_id().starts_with("job-"));
    BOOST_TEST(j.state() == tv1::QUEUED);
    BOOST_TEST(j.submitted_by() == "admin:alice");
    BOOST_TEST(j.spec().policy().max_attempts() == 4u);  // max_attempts_default
    BOOST_REQUIRE_EQUAL(repo.audit_rows.size(), 1u);
    BOOST_TEST(repo.audit_rows[0].action == "job.submit");
    BOOST_TEST(repo.audit_rows[0].target == j.job_id());
    BOOST_TEST(repo.audit_rows[0].actor == "admin:alice");

    auto s = spec("sweep.step_1-a");
    s.mutable_policy()->set_max_attempts(1);
    s.add_input_artifacts(art('a'));
    auto j2 = submit(s, node("n1"));
    BOOST_TEST(j2.job_id() == "sweep.step_1-a");
    BOOST_TEST(j2.submitted_by() == "node:n1");
    BOOST_TEST(j2.spec().policy().max_attempts() == 1u);

    auto camp = repo.create_campaign([] {
        cv1::CreateCampaignRequest c;
        c.set_name("c");
        return c;
    }(), "alice", now);
    auto cs = spec();
    cs.set_campaign_id(camp->campaign_id());
    BOOST_TEST(submit(cs).spec().campaign_id() == camp->campaign_id());

    auto dup = call("POST", "/api/v1/jobs", admin(), submit_body(s));
    BOOST_TEST(dup.status == 409);
    BOOST_TEST(err_code(dup) == "conflict");
}

BOOST_FIXTURE_TEST_CASE(submit_validation_and_auth, Fixture) {
    auto bad = [&](tv1::JobSpec s) {
        auto r = call("POST", "/api/v1/jobs", admin(), submit_body(s));
        BOOST_TEST(r.status == 400);
        BOOST_TEST(err_code(r) == "bad_request");
    };
    auto noargv = spec();
    noargv.mutable_command()->clear_argv();
    bad(noargv);
    auto emptyarg0 = spec();
    emptyarg0.mutable_command()->set_argv(0, "");
    bad(emptyarg0);
    bad(spec("has space"));
    bad(spec("slash/no"));
    bad(spec(std::string(101, 'x')));
    auto upper = spec();
    upper.add_input_artifacts("artifact://sha256/" + std::string(64, 'A'));
    bad(upper);
    auto shorth = spec();
    shorth.add_input_artifacts("artifact://sha256/abc");
    bad(shorth);
    auto other = spec();
    other.add_input_artifacts("file:///etc/passwd");
    bad(other);
    auto camp = spec();
    camp.set_campaign_id("camp-missing");
    bad(camp);
    BOOST_TEST(call("POST", "/api/v1/jobs", admin(), "{not json").status == 400);
    BOOST_TEST(submit(spec(std::string(100, 'x'))).job_id().size() == 100u);

    BOOST_TEST(call("POST", "/api/v1/jobs", ident(tls::Role::kServer, "cp"), submit_body(spec())).status == 403);
    BOOST_TEST(call("POST", "/api/v1/jobs", std::nullopt, submit_body(spec())).status == 401);
    BOOST_TEST(repo.jobs.size() == 1u);
}

BOOST_FIXTURE_TEST_CASE(list_and_get, Fixture) {
    auto a = submit(spec("a"));
    auto b = submit(spec("b"));
    const auto n1 = register_node("n1");
    auto ls = lease("n1", n1, 1);
    BOOST_REQUIRE_EQUAL(ls.size(), 1u);

    auto all = parse<cv1::JobList>(call("GET", "/api/v1/jobs", admin()));
    BOOST_REQUIRE_EQUAL(all.jobs_size(), 2);
    BOOST_TEST(all.jobs(0).job_id() == "b");
    auto leased = parse<cv1::JobList>(call("GET", "/api/v1/jobs", node("n1"), "", {{"state", "leased"}}));
    BOOST_REQUIRE_EQUAL(leased.jobs_size(), 1);
    BOOST_TEST(leased.jobs(0).job_id() == "a");
    BOOST_TEST(parse<cv1::JobList>(call("GET", "/api/v1/jobs", admin(), "", {{"limit", "1"}})).jobs_size() == 1);
    BOOST_TEST(parse<cv1::JobList>(call("GET", "/api/v1/jobs", admin(), "", {{"campaign", "camp-x"}})).jobs_size() == 0);
    BOOST_TEST(call("GET", "/api/v1/jobs", admin(), "", {{"state", "BOGUS"}}).status == 400);
    BOOST_TEST(call("GET", "/api/v1/jobs", admin(), "", {{"state", "JOB_STATE_UNSPECIFIED"}}).status == 400);
    BOOST_TEST(call("GET", "/api/v1/jobs", admin(), "", {{"limit", "0"}}).status == 400);

    auto got = call("GET", "/api/v1/jobs/a", admin());
    BOOST_REQUIRE_EQUAL(got.status, 200);
    auto ja = parse<cv1::Job>(got);
    BOOST_TEST(ja.state() == tv1::LEASED);
    BOOST_REQUIRE_EQUAL(ja.attempts_size(), 1);
    BOOST_TEST(ja.attempts(0).node_id() == n1);
    auto nf = call("GET", "/api/v1/jobs/zzz", admin());
    BOOST_TEST(nf.status == 404);
    BOOST_TEST(err_code(nf) == "not_found");
    BOOST_TEST(call("GET", "/api/v1/jobs", ident(tls::Role::kServer, "cp")).status == 403);
    (void)a;
    (void)b;
}

BOOST_FIXTURE_TEST_CASE(cancel_and_heartbeat_revokes, Fixture) {
    const auto n1 = register_node("n1");
    submit(spec("run"));
    submit(spec("wait"));
    auto ls = lease("n1", n1, 1);
    BOOST_REQUIRE_EQUAL(ls.size(), 1u);
    BOOST_TEST(ls[0].job_id() == "run");

    auto c = call("POST", "/api/v1/jobs/wait/cancel", admin(), R"({"reason":"changed my mind"})");
    BOOST_REQUIRE_EQUAL(c.status, 200);
    BOOST_TEST(parse<cv1::Job>(c).state() == tv1::CANCELLED);
    BOOST_TEST(parse<cv1::Job>(c).last_error() == "changed my mind");
    auto again = call("POST", "/api/v1/jobs/wait/cancel", admin());
    BOOST_TEST(again.status == 409);
    BOOST_TEST(err_code(again) == "conflict");
    BOOST_TEST(call("POST", "/api/v1/jobs/nope/cancel", admin()).status == 404);

    // Heartbeat before the cancel renews; after it, revokes.
    BOOST_TEST(heartbeat("n1", n1, ls).revoked_size() == 0);
    auto cr = call("POST", "/api/v1/jobs/run/cancel", node("n2"));
    BOOST_REQUIRE_EQUAL(cr.status, 200);
    auto running = parse<cv1::Job>(cr);
    BOOST_TEST(running.state() == tv1::LEASED);
    BOOST_TEST(running.cancel_requested());
    auto hb = heartbeat("n1", n1, ls);
    BOOST_REQUIRE_EQUAL(hb.revoked_size(), 1);
    BOOST_TEST(hb.revoked(0).job_id() == "run");
    BOOST_TEST(hb.revoked(0).attempt_id() == ls[0].attempt_id());
    auto fin = status("n1", ls[0], tv1::CANCELLED);
    BOOST_REQUIRE_EQUAL(fin.status, 200);
    BOOST_TEST(parse<cv1::Job>(fin).state() == tv1::CANCELLED);

    int cancels = 0;
    for (const auto& a : repo.audit_rows)
        if (a.action == "job.cancel") ++cancels;
    BOOST_TEST(cancels == 2);  // the 409 is not audited
    BOOST_TEST(repo.audit_rows.back().actor == "node:n2");
    BOOST_TEST(repo.audit_rows.back().target == "run");
}

BOOST_FIXTURE_TEST_CASE(lease_auth_and_reply, Fixture) {
    const auto a = register_node("a");
    const auto b = register_node("b");
    submit(spec("j1"));
    auto forged = lease_call("a", b);
    BOOST_TEST(forged.status == 403);
    BOOST_TEST(err_code(forged) == "forbidden");
    BOOST_TEST(lease_call("a", "ff-node-ffffffffffffffff").status == 404);
    BOOST_TEST(lease_call("a", "").status == 400);
    cv1::LeaseRequest req;
    req.set_node_id(a);
    BOOST_TEST(call("POST", "/api/v1/leases", admin(), to_json(req)).status == 403);
    BOOST_TEST(repo.jobs.at("j1").state() == tv1::QUEUED);

    auto r = lease_call("a", a);
    BOOST_REQUIRE_EQUAL(r.status, 200);
    auto rep = parse<cv1::LeaseReply>(r);
    BOOST_TEST(rep.lease_ttl_seconds() == 30u);
    BOOST_REQUIRE_EQUAL(rep.leases_size(), 1);
    BOOST_TEST(rep.leases(0).job_id() == "j1");
    BOOST_TEST(rep.leases(0).expires_unix_ms() == now + 30'000);
    BOOST_TEST(rep.leases(0).spec().command().argv(1) == "hi");
    BOOST_TEST(repo.last_lease_request.free().cpu_cores() == 8u);
    // Empty reply is normal.
    auto empty = parse<cv1::LeaseReply>(lease_call("a", a));
    BOOST_TEST(empty.leases_size() == 0);
    BOOST_TEST(empty.lease_ttl_seconds() == 30u);
    // Leases are not audited.
    BOOST_TEST(repo.audit_rows.size() == 1u);
}

BOOST_FIXTURE_TEST_CASE(status_fencing_and_auth, Fixture) {
    const auto a = register_node("a");
    register_node("b");
    submit(spec("j"));
    auto ls = lease("a", a);
    BOOST_REQUIRE_EQUAL(ls.size(), 1u);
    const auto& l = ls[0];

    BOOST_TEST(status("", l, tv1::RUNNING, admin()).status == 403);
    auto notholder = status("b", l, tv1::RUNNING);
    BOOST_TEST(notholder.status == 409);
    BOOST_TEST(err_code(notholder) == "stale");
    auto unreg = status("ghost", l, tv1::RUNNING);
    BOOST_TEST(unreg.status == 403);
    auto wrong_token = l;
    wrong_token.set_fencing_token(7);
    BOOST_TEST(status("a", wrong_token, tv1::RUNNING).status == 409);
    auto missing = l;
    missing.set_job_id("nope");
    BOOST_TEST(status("a", missing, tv1::RUNNING).status == 404);
    auto invalid = status("a", l, tv1::QUEUED);
    BOOST_TEST(invalid.status == 400);
    BOOST_TEST(err_code(invalid) == "bad_request");
    cv1::JobUpdate noatt;
    noatt.set_state(tv1::RUNNING);
    BOOST_TEST(call("POST", "/api/v1/jobs/j/status", node("a"), to_json(noatt)).status == 400);

    auto run = status("a", l, tv1::RUNNING);
    BOOST_REQUIRE_EQUAL(run.status, 200);
    BOOST_TEST(parse<cv1::Job>(run).state() == tv1::RUNNING);

    cv1::JobUpdate done;
    done.set_attempt_id(l.attempt_id());
    done.set_fencing_token(l.fencing_token());
    done.set_state(tv1::COMPLETED);
    done.add_outputs()->set_artifact_id("artifact://sha256/nothex");
    BOOST_TEST(call("POST", "/api/v1/jobs/j/status", node("a"), to_json(done)).status == 400);
    done.mutable_outputs(0)->set_artifact_id(art('c'));
    auto ok = call("POST", "/api/v1/jobs/j/status", node("a"), to_json(done));
    BOOST_REQUIRE_EQUAL(ok.status, 200);
    auto job = parse<cv1::Job>(ok);
    BOOST_TEST(job.state() == tv1::COMPLETED);
    BOOST_REQUIRE_EQUAL(job.outputs_size(), 1);
    BOOST_TEST(job.outputs(0).artifact_id() == art('c'));
    // Replays after the job finished are stale.
    BOOST_TEST(call("POST", "/api/v1/jobs/j/status", node("a"), to_json(done)).status == 409);
    BOOST_TEST(repo.audit_rows.size() == 1u);  // status updates are not audited
}

BOOST_FIXTURE_TEST_CASE(artifacts_announce_and_lookup, Fixture) {
    const auto a = register_node("a");
    const auto b = register_node("b");
    cv1::ArtifactAnnouncement an;
    an.set_node_id(a);
    auto* x = an.add_artifacts();
    x->set_artifact_id(art('d'));
    x->set_size(42);
    auto r = call("POST", "/api/v1/artifacts/announce", node("a"), to_json(an));
    BOOST_REQUIRE_EQUAL(r.status, 200);
    BOOST_TEST(r.body == "{}");
    BOOST_TEST(call("POST", "/api/v1/artifacts/announce", node("b"), to_json(an)).status == 403);
    BOOST_TEST(call("POST", "/api/v1/artifacts/announce", admin(), to_json(an)).status == 403);
    an.set_node_id(b);
    x->set_artifact_id("artifact://sha256/" + hex('D'));
    BOOST_TEST(call("POST", "/api/v1/artifacts/announce", node("b"), to_json(an)).status == 400);
    BOOST_TEST(repo.announcements.size() == 1u);

    auto got = call("GET", "/api/v1/artifacts/sha256/" + hex('d'), admin());
    BOOST_REQUIRE_EQUAL(got.status, 200);
    auto info = parse<cv1::ArtifactInfo>(got);
    BOOST_TEST(info.artifact().size() == 42u);
    BOOST_REQUIRE_EQUAL(info.node_ids_size(), 1);
    BOOST_TEST(info.node_ids(0) == a);
    BOOST_TEST(call("GET", "/api/v1/artifacts/sha256/" + hex('d'), node("b")).status == 200);
    BOOST_TEST(call("GET", "/api/v1/artifacts/sha256/" + hex('e'), admin()).status == 404);
    BOOST_TEST(call("GET", "/api/v1/artifacts/sha256/" + hex('D'), admin()).status == 400);
    BOOST_TEST(call("GET", "/api/v1/artifacts/sha256/abc", admin()).status == 400);
}

BOOST_FIXTURE_TEST_CASE(sweep_leases_startup_grace, Fixture) {
    const auto a = register_node("a");
    submit(spec("j"));
    auto ls = lease("a", a);
    BOOST_REQUIRE_EQUAL(ls.size(), 1u);
    // Lease expires at construction + 30 s; grace also lasts 30 s.
    now += 29'999;
    BOOST_TEST(api.sweep_leases().empty());
    BOOST_TEST(repo.expire_calls == 0);
    now += 1;  // grace over, lease not yet strictly expired
    BOOST_TEST(api.sweep_leases().empty());
    BOOST_TEST(repo.expire_calls == 1);
    now += 1;
    auto ex = api.sweep_leases();
    BOOST_REQUIRE_EQUAL(ex.size(), 1u);
    BOOST_TEST(ex[0].job_id == "j");
    BOOST_TEST(ex[0].new_state == tv1::QUEUED);
    // The old holder is fenced now.
    BOOST_TEST(status("a", ls[0], tv1::COMPLETED).status == 409);
}

BOOST_AUTO_TEST_CASE(sweep_leases_grace_counts_from_construction) {
    FakeRepository repo;
    std::int64_t now = 5'000'000;
    ApiOptions o;  // lease_ttl 60 s
    BOOST_TEST(o.lease_ttl_seconds == 60u);
    BOOST_TEST(o.max_attempts_default == 3u);
    ControlApi api(repo, o, [&] { return now; });
    now += 59'000;
    api.sweep_leases();
    BOOST_TEST(repo.expire_calls == 0);
    now += 1'000;
    api.sweep_leases();
    BOOST_TEST(repo.expire_calls == 1);
}

// End to end against the real repository: JSON wire forms of the new messages.
BOOST_AUTO_TEST_CASE(pg_jobs_flow) {
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
        if (!r.body.empty()) r.content_type = "application/json";  // like tdrctl and tdrd
        r.peer = std::move(peer);
        return api.handle(r);
    };
    auto reg = parse<cv1::RegisterReply>(call("POST", "/api/v1/nodes/register", node("n1"), R"({"name":"n1"})"));
    const auto id = reg.node_id();

    auto s = call("POST", "/api/v1/jobs", admin(),
                  R"({"spec":{"command":{"argv":["/bin/true"]},"resources":{"cpu_cores":2,"memory_bytes":"1073741824"}}})");
    BOOST_REQUIRE_MESSAGE(s.status == 201, s.body);
    auto job = parse<cv1::Job>(s);
    BOOST_TEST(job.spec().policy().max_attempts() == 3u);

    auto lr = call("POST", "/api/v1/leases", node("n1"),
                   R"({"node_id":")" + id + R"(","free":{"cpu_cores":4,"memory_bytes":"8589934592"},"max_jobs":2})");
    BOOST_REQUIRE_EQUAL(lr.status, 200);
    auto rep = parse<cv1::LeaseReply>(lr);
    BOOST_REQUIRE_EQUAL(rep.leases_size(), 1);
    const auto& l = rep.leases(0);
    BOOST_TEST(rep.lease_ttl_seconds() == 60u);

    const std::string upd = R"({"attempt_id":")" + l.attempt_id() + R"(","fencing_token":"1","state":)";
    BOOST_TEST(call("POST", "/api/v1/jobs/" + job.job_id() + "/status", node("n1"), upd + R"("RUNNING"})").status == 200);
    BOOST_TEST(call("POST", "/api/v1/jobs/" + job.job_id() + "/cancel", admin()).status == 200);
    now += 1000;
    auto hb = parse<cv1::HeartbeatReply>(call(
        "POST", "/api/v1/nodes/heartbeat", node("n1"),
        R"({"node_id":")" + id + R"(","leases":[{"job_id":")" + job.job_id() + R"(","attempt_id":")" + l.attempt_id() +
            R"(","fencing_token":"1"}]})"));
    BOOST_REQUIRE_EQUAL(hb.revoked_size(), 1);
    auto fin = call("POST", "/api/v1/jobs/" + job.job_id() + "/status", node("n1"), upd + R"("CANCELLED"})");
    BOOST_REQUIRE_EQUAL(fin.status, 200);
    BOOST_TEST(parse<cv1::Job>(fin).state() == tv1::CANCELLED);
    BOOST_TEST(call("POST", "/api/v1/jobs/" + job.job_id() + "/status", node("n1"), upd + R"("CANCELLED"})").status ==
               409);

    auto full = parse<cv1::Job>(call("GET", "/api/v1/jobs/" + job.job_id(), admin()));
    BOOST_REQUIRE_EQUAL(full.attempts_size(), 1);
    BOOST_TEST(full.attempts(0).state() == "CANCELLED");

    auto an = call("POST", "/api/v1/artifacts/announce", node("n1"),
                   R"({"node_id":")" + id + R"(","artifacts":[{"artifact_id":")" + art('7') +
                       R"(","size":"99"}],"sync_id":"s","full":true})");
    BOOST_REQUIRE_EQUAL(an.status, 200);
    auto info = parse<cv1::ArtifactInfo>(call("GET", "/api/v1/artifacts/sha256/" + hex('7'), admin()));
    BOOST_TEST(info.artifact().size() == 99u);
    BOOST_TEST(info.node_ids(0) == id);

    auto audit = parse<cv1::AuditList>(call("GET", "/api/v1/audit", admin()));
    BOOST_REQUIRE_EQUAL(audit.entries_size(), 2);
    BOOST_TEST(audit.entries(0).action() == "job.cancel");
    BOOST_TEST(audit.entries(1).action() == "job.submit");
}

BOOST_AUTO_TEST_SUITE_END()
