// Workflow engine on a throwaway PostgreSQL cluster (docs/design/phase6.md §3-§6, §10).
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

cv1::ArtifactRef out(int n, const std::string& name) {
    cv1::ArtifactRef a;
    a.set_artifact_id(art(n));
    a.set_size(10);
    a.set_name(name);
    return a;
}

tv1::StepSpec step(const std::string& name, std::vector<std::string> deps = {}) {
    tv1::StepSpec s;
    s.set_name(name);
    s.mutable_job()->mutable_command()->add_argv("/bin/true");
    s.mutable_job()->set_operation("op." + name);
    s.mutable_job()->mutable_policy()->set_max_attempts(1);
    for (auto& d : deps) s.add_depends_on(d);
    return s;
}

tv1::StepInput input(const std::string& from, const std::string& output, bool spawned = false,
                     const std::string& as = "") {
    tv1::StepInput i;
    i.set_step(from);
    i.set_output(output);
    i.set_spawned(spawned);
    i.set_input_name(as);
    return i;
}

cv1::CreateWorkflowRequest wfreq(std::vector<tv1::StepSpec> steps, const std::string& name = "wf") {
    cv1::CreateWorkflowRequest r;
    r.set_name(name);
    for (auto& s : steps) *r.add_steps() = s;
    return r;
}

cv1::LeaseRequest lreq(const std::string& node) {
    cv1::LeaseRequest r;
    r.set_node_id(node);
    r.mutable_free()->set_cpu_cores(1000);
    r.mutable_free()->set_memory_bytes(1024 * GiB);
    r.set_max_jobs(1000);
    return r;
}

struct Db {
    std::string conninfo;
    std::unique_ptr<IRepository> repo;
    std::string n;                           // a node
    std::map<std::string, cv1::Lease> held;  // job_id -> lease
    std::int64_t now = T0;

    Db() {
        static int k = 0;
        conninfo = cluster().create_database("w" + std::to_string(++k));
        repo = open_pg_repository(conninfo);
        n = node("n1");
    }

    std::string node(const std::string& name) {
        cv1::RegisterRequest r;
        r.set_name(name);
        r.add_capabilities("cpu");
        return repo->register_node(r, "", T0).node_id();
    }

    cv1::Workflow create(std::vector<tv1::StepSpec> steps) {
        auto w = repo->create_workflow(wfreq(std::move(steps)), "admin:alice", now);
        BOOST_REQUIRE_MESSAGE(w, w.error());
        return *w;
    }

    cv1::Workflow get(const std::string& id) { return *repo->get_workflow(id, true); }

    static const cv1::WorkflowStep* find(const cv1::Workflow& w, const std::string& name) {
        for (const auto& s : w.steps())
            if (s.name() == name) return &s;
        return nullptr;
    }
    std::string st(const std::string& wf, const std::string& name) {
        auto w = get(wf);
        auto* s = find(w, name);
        return s ? s->state() : std::string("<none>");
    }
    std::string job_of(const std::string& wf, const std::string& name) {
        auto w = get(wf);
        auto* s = find(w, name);
        BOOST_REQUIRE_MESSAGE(s, "no step " + name);
        return s->job_id();
    }
    cv1::Job job(const std::string& id) { return *repo->get_job(id, false); }

    // Leases everything leasable on node n.
    std::size_t lease_all() {
        auto ls = repo->lease_jobs(lreq(n), TTL, WAIT, ++now);
        for (auto& l : ls) held[l.job_id()] = l;
        return ls.size();
    }

    UpdateOutcome report(const std::string& wf, const std::string& name, tv1::JobState state,
                         std::vector<cv1::ArtifactRef> outputs = {}, std::vector<tv1::StepSpec> spawn = {},
                         bool retryable = false) {
        lease_all();
        const auto id = job_of(wf, name);
        BOOST_REQUIRE_MESSAGE(held.contains(id), "step " + name + " job not leased");
        const auto& l = held.at(id);
        cv1::JobUpdate u;
        u.set_attempt_id(l.attempt_id());
        u.set_fencing_token(l.fencing_token());
        u.set_state(state);
        u.set_retryable(retryable);
        if (state != tv1::COMPLETED) u.set_error("boom");
        for (auto& o : outputs) *u.add_outputs() = o;
        for (auto& s : spawn) *u.add_spawn() = s;
        auto r = repo->update_job(id, n, u, ++now);
        held.erase(id);
        return r;
    }
    void complete(const std::string& wf, const std::string& name, std::vector<cv1::ArtifactRef> outputs = {},
                  std::vector<tv1::StepSpec> spawn = {}) {
        auto r = report(wf, name, tv1::COMPLETED, std::move(outputs), std::move(spawn));
        BOOST_REQUIRE_MESSAGE(r.result == UpdateResult::kOk, r.message);
    }
    void fail(const std::string& wf, const std::string& name) {
        auto r = report(wf, name, tv1::FAILED);
        BOOST_REQUIRE(r.result == UpdateResult::kOk);
    }
    int job_count(const std::string& wf) {
        return std::stoi(psql(conninfo, "SELECT count(*) FROM jobs WHERE workflow_id = '" + wf + "'"));
    }
};

std::vector<std::pair<std::string, std::string>> inputs_of(const tv1::JobSpec& s) {
    std::vector<std::pair<std::string, std::string>> v;
    for (int i = 0; i < s.input_artifacts_size(); ++i)
        v.emplace_back(s.input_artifacts(i), i < s.input_names_size() ? s.input_names(i) : "");
    return v;
}

}  // namespace

BOOST_AUTO_TEST_SUITE(cpdb_workflows)

BOOST_AUTO_TEST_CASE(create_validation) {
    Db db;
    auto bad = [&](std::vector<tv1::StepSpec> steps, const std::string& want, const std::string& camp = "") {
        auto r = wfreq(std::move(steps));
        r.set_campaign_id(camp);
        auto w = db.repo->create_workflow(r, "a", T0);
        BOOST_REQUIRE(!w);
        BOOST_TEST_MESSAGE(w.error());
        BOOST_TEST(w.error().find(want) != std::string::npos, w.error() << " lacks " << want);
    };
    bad({}, "no steps");
    bad({step("a b")}, "must be 1-64");
    bad({step("a/b")}, "must be 1-64");
    bad({step(std::string(65, 'x'))}, "must be 1-64");
    bad({step("a"), step("a")}, "duplicate step name a");
    bad({step("a", {"zz"})}, "unknown dependency zz");
    bad({step("a", {"a"})}, "depends on itself");
    auto in = step("b");
    *in.add_inputs() = input("nope", "*");
    bad({step("a"), in}, "unknown step 'nope'");
    bad({step("a", {"c"}), step("b", {"a"}), step("c", {"b"})}, "cycle");
    auto t = step("a");
    t.mutable_job()->set_job_id("fixed");
    bad({t}, "must not set job_id");
    auto t2 = step("a");
    t2.mutable_job()->set_campaign_id("camp-x");
    bad({t2}, "must not set");
    auto t3 = step("a");
    t3.mutable_job()->clear_command();
    bad({t3}, "argv");
    auto t4 = step("a");
    t4.mutable_job()->add_input_artifacts("not-an-artifact");
    bad({t4}, "is not artifact://");
    bad({step("a")}, "unknown campaign", "camp-nope");
    db.repo->set_workflow_limits(WorkflowLimits{3, 1000, 8});
    bad({step("a"), step("b"), step("c"), step("d")}, "too many steps");
    BOOST_TEST(db.repo->list_workflows("", "", 100).empty());
    BOOST_TEST(std::stoi(psql(db.conninfo, "SELECT count(*) FROM jobs")) == 0);
}

BOOST_AUTO_TEST_CASE(diamond_with_wiring) {
    Db db;
    cv1::CreateCampaignRequest cr;
    cr.set_name("c");
    auto camp = db.repo->create_campaign(cr, "a", T0)->campaign_id();
    auto c = *db.repo->get_campaign(camp);
    c.set_state(cv1::ACTIVE);
    BOOST_REQUIRE(db.repo->update_campaign(c, cv1::DRAFT, T0));

    auto relax = step("relax");
    relax.mutable_job()->add_input_artifacts(art(900));
    relax.mutable_job()->add_input_names("seed");
    auto bands = step("bands");
    *bands.add_inputs() = input("relax", "structure");
    auto phon = step("phonons");
    *phon.add_inputs() = input("relax", "*");
    auto eval = step("evaluate", {"phonons"});
    *eval.add_inputs() = input("bands", "bands", false, "x");
    *eval.add_inputs() = input("phonons", "", false, "");
    eval.mutable_job()->add_input_artifacts(art(901));  // static input kept, unnamed

    auto req = wfreq({relax, bands, phon, eval}, "diamond");
    req.set_campaign_id(camp);
    // The static inputs must be located somewhere for relax to be leasable.
    cv1::ArtifactAnnouncement an;
    an.set_node_id(db.n);
    *an.add_artifacts() = out(900, "");
    *an.add_artifacts() = out(901, "");
    BOOST_REQUIRE(db.repo->announce_artifacts(an, T0));
    auto w = db.repo->create_workflow(req, "admin:alice", T0);
    BOOST_REQUIRE_MESSAGE(w, w.error());
    BOOST_TEST(w->workflow_id().starts_with("wf-"));
    BOOST_TEST(w->state() == "RUNNING");
    BOOST_TEST(w->name() == "diamond");
    BOOST_TEST(w->campaign_id() == camp);
    BOOST_REQUIRE_EQUAL(w->steps_size(), 4);
    BOOST_TEST(w->steps(0).name() == "relax");
    BOOST_TEST(w->steps(0).state() == "QUEUED");
    BOOST_TEST(w->steps(1).state() == "WAITING");
    BOOST_TEST(w->steps(1).job_id().empty());
    BOOST_TEST(w->step_counts().at("WAITING") == 3u);
    BOOST_TEST(w->step_counts().at("QUEUED") == 1u);
    const auto id = w->workflow_id();

    auto rj = db.job(db.job_of(id, "relax"));
    BOOST_TEST(rj.workflow_id() == id);
    BOOST_TEST(rj.step() == "relax");
    BOOST_TEST(rj.spec().workflow_id() == id);
    BOOST_TEST(rj.spec().campaign_id() == camp);
    BOOST_TEST(rj.submitted_by() == "admin:alice");
    BOOST_TEST(rj.spec().policy().max_attempts() == 1u);

    db.complete(id, "relax", {out(1, "structure"), out(2, "log")});
    BOOST_TEST(db.st(id, "bands") == "QUEUED");
    BOOST_TEST(db.st(id, "phonons") == "QUEUED");
    BOOST_TEST(db.st(id, "evaluate") == "WAITING");
    auto bj = db.job(db.job_of(id, "bands"));
    BOOST_TEST((inputs_of(bj.spec()) == std::vector<std::pair<std::string, std::string>>{{art(1), "structure"}}));
    auto pj = db.job(db.job_of(id, "phonons"));
    BOOST_TEST((inputs_of(pj.spec()) ==
                std::vector<std::pair<std::string, std::string>>{{art(1), "structure"}, {art(2), "log"}}));

    db.complete(id, "bands", {out(3, "bands")});
    BOOST_TEST(db.st(id, "evaluate") == "WAITING");
    db.complete(id, "phonons", {out(4, "phonons"), out(5, "bands")});
    auto ej = db.job(db.job_of(id, "evaluate"));
    // static (unnamed) first, then wiring; "bands" from phonons collides with nothing
    // ("x" was used for bands.bands).
    BOOST_TEST((inputs_of(ej.spec()) == std::vector<std::pair<std::string, std::string>>{
                                            {art(901), ""}, {art(3), "x"}, {art(4), "phonons"}, {art(5), "bands"}}));
    BOOST_TEST(db.get(id).state() == "RUNNING");
    db.complete(id, "evaluate", {out(6, "score")});
    auto done = db.get(id);
    BOOST_TEST(done.state() == "SUCCEEDED");
    BOOST_TEST(done.finished_unix_ms() == db.now);
    BOOST_TEST(done.error().empty());
    BOOST_TEST(done.step_counts().at("COMPLETED") == 4u);
    BOOST_TEST(db.job_count(id) == 4);

    // list / filter
    BOOST_TEST(db.repo->list_workflows("", "", 10).size() == 1u);
    BOOST_TEST(db.repo->list_workflows("SUCCEEDED", camp, 10).size() == 1u);
    BOOST_TEST(db.repo->list_workflows("RUNNING", "", 10).empty());
    BOOST_TEST(db.repo->list_workflows("", "camp-other", 10).empty());
    BOOST_TEST(db.repo->list_workflows("", "", 10)[0].steps_size() == 0);
    BOOST_TEST(!db.repo->get_workflow("wf-nope", true));
}

BOOST_AUTO_TEST_CASE(name_collisions_get_suffixes) {
    Db db;
    auto j = step("join", {"a", "b"});
    *j.add_inputs() = input("a", "r");
    *j.add_inputs() = input("b", "r");
    *j.add_inputs() = input("b", "*", false, "r");
    auto w = db.create({step("a"), step("b"), j});
    db.complete(w.workflow_id(), "a", {out(1, "r")});
    db.complete(w.workflow_id(), "b", {out(2, "r"), out(3, "s")});
    auto jj = db.job(db.job_of(w.workflow_id(), "join"));
    BOOST_TEST((inputs_of(jj.spec()) == std::vector<std::pair<std::string, std::string>>{
                                            {art(1), "r"}, {art(2), "r#2"}, {art(2), "r#3"}, {art(3), "r#4"}}));
}

BOOST_AUTO_TEST_CASE(fanout_and_join_with_spawned_inputs) {
    Db db;
    auto join = step("join", {"fan"});
    *join.add_inputs() = input("fan", "result", true);
    auto w = db.create({step("prep"), step("fan", {"prep"}), join});
    const auto id = w.workflow_id();
    db.complete(id, "prep", {out(1, "cfg")});

    auto child = [](const std::string& n) { return step(n); };
    auto c3 = step("c3", {"c1"});
    *c3.add_inputs() = input(".", "log");        // spawning step's output, not a dependency
    *c3.add_inputs() = input("prep", "cfg");     // existing step by full name
    db.complete(id, "fan", {out(10, "log")}, {child("c1"), child("c2"), c3});
    auto g = db.get(id);
    BOOST_REQUIRE_EQUAL(g.steps_size(), 6);
    BOOST_TEST(g.steps(3).name() == "fan/c1");
    BOOST_TEST(g.steps(3).parent_step() == "fan");
    BOOST_TEST(g.steps(5).spec().depends_on(0) == "fan/c1");
    BOOST_TEST(g.steps(5).spec().inputs(0).step() == "fan");
    BOOST_TEST(db.st(id, "fan") == "COMPLETED");
    BOOST_TEST(db.st(id, "fan/c1") == "QUEUED");
    BOOST_TEST(db.st(id, "fan/c2") == "QUEUED");
    BOOST_TEST(db.st(id, "fan/c3") == "WAITING");
    BOOST_TEST(db.st(id, "join") == "WAITING");  // fan is terminal but not settled

    auto c1j = db.job(db.job_of(id, "fan/c1"));
    BOOST_TEST(c1j.step() == "fan/c1");
    db.complete(id, "fan/c1", {out(11, "result")});
    BOOST_TEST(db.st(id, "fan/c3") == "QUEUED");
    auto c3j = db.job(db.job_of(id, "fan/c3"));
    BOOST_TEST((inputs_of(c3j.spec()) ==
                std::vector<std::pair<std::string, std::string>>{{art(10), "log"}, {art(1), "cfg"}}));
    db.complete(id, "fan/c2", {out(12, "result")});
    BOOST_TEST(db.st(id, "join") == "WAITING");
    // A grandchild keeps the subtree open.
    db.complete(id, "fan/c3", {out(13, "result")}, {step("deep")});
    BOOST_TEST(db.st(id, "fan/c3/deep") == "QUEUED");
    BOOST_TEST(db.st(id, "join") == "WAITING");
    db.complete(id, "fan/c3/deep", {out(14, "result"), out(15, "other")});
    BOOST_TEST(db.st(id, "join") == "QUEUED");
    auto jj = db.job(db.job_of(id, "join"));
    BOOST_TEST((inputs_of(jj.spec()) == std::vector<std::pair<std::string, std::string>>{
                                            {art(11), "result"},
                                            {art(12), "result#2"},
                                            {art(13), "result#3"},
                                            {art(14), "result#4"}}));
    db.complete(id, "join");
    BOOST_TEST(db.get(id).state() == "SUCCEEDED");
    BOOST_TEST(db.get(id).step_counts().at("COMPLETED") == 7u);
}

BOOST_AUTO_TEST_CASE(spawned_failure_fails_join_rule) {
    Db db;
    auto w = db.create({step("fan"), step("join", {"fan"}), [] {
                            auto h = step("handler", {"fan"});
                            h.set_run_if(tv1::ANY_FAILED);
                            return h;
                        }()});
    const auto id = w.workflow_id();
    db.complete(id, "fan", {}, {step("a"), step("b")});
    db.complete(id, "fan/a");
    db.fail(id, "fan/b");
    BOOST_TEST(db.st(id, "join") == "SKIPPED");
    BOOST_TEST(db.st(id, "handler") == "QUEUED");
    db.complete(id, "handler");
    auto g = db.get(id);
    BOOST_TEST(g.state() == "FAILED");
    BOOST_TEST(g.error().find("fan/b") != std::string::npos, g.error());
}

BOOST_AUTO_TEST_CASE(skipped_chain_handlers_and_final_state) {
    Db db;
    auto h = step("on_fail", {"a"});
    h.set_run_if(tv1::ANY_FAILED);
    auto h2 = step("on_fail_b", {"b"});  // b is SKIPPED: ANY_FAILED ignores it
    h2.set_run_if(tv1::ANY_FAILED);
    auto cleanup = step("cleanup", {"c"});
    cleanup.set_run_if(tv1::ALWAYS);
    *cleanup.add_inputs() = input("c", "out");  // c never completes: contributes nothing
    auto w = db.create({step("a"), step("b", {"a"}), step("c", {"b"}), h, h2, cleanup});
    const auto id = w.workflow_id();
    db.fail(id, "a");
    BOOST_TEST(db.st(id, "a") == "FAILED");
    BOOST_TEST(db.st(id, "b") == "SKIPPED");
    BOOST_TEST(db.st(id, "c") == "SKIPPED");
    BOOST_TEST(db.st(id, "on_fail") == "QUEUED");
    BOOST_TEST(db.st(id, "on_fail_b") == "SKIPPED");
    BOOST_TEST(db.st(id, "cleanup") == "QUEUED");
    BOOST_TEST(db.job(db.job_of(id, "cleanup")).spec().input_artifacts_size() == 0);
    auto g = db.get(id);
    BOOST_TEST(Db::find(g, "b")->error().find("run_if") != std::string::npos);
    BOOST_TEST(g.state() == "RUNNING");
    db.complete(id, "on_fail");
    db.complete(id, "cleanup");
    g = db.get(id);
    BOOST_TEST(g.state() == "FAILED");
    BOOST_TEST(g.error().find("step a FAILED") != std::string::npos, g.error());
    BOOST_TEST(g.step_counts().at("SKIPPED") == 3u);

    // Success path: the handler is SKIPPED and the workflow SUCCEEDS.
    auto w2 = db.create({step("a"), h});
    db.complete(w2.workflow_id(), "a");
    BOOST_TEST(db.st(w2.workflow_id(), "on_fail") == "SKIPPED");
    BOOST_TEST(db.get(w2.workflow_id()).state() == "SUCCEEDED");

    // A lone ANY_FAILED step is skipped at once: the workflow finishes on creation.
    auto lone = step("x");
    lone.set_run_if(tv1::ANY_FAILED);
    auto w3 = db.create({lone});
    BOOST_TEST(w3.state() == "SUCCEEDED");
    BOOST_TEST(w3.steps(0).state() == "SKIPPED");
}

BOOST_AUTO_TEST_CASE(allow_failure) {
    Db db;
    auto a = step("a");
    a.set_allow_failure(true);
    auto b = step("b", {"a"});
    b.set_run_if(tv1::ALWAYS);
    auto w = db.create({a, b, step("c", {"a"})});
    const auto id = w.workflow_id();
    db.fail(id, "a");
    BOOST_TEST(db.st(id, "c") == "SKIPPED");
    db.complete(id, "b");
    auto g = db.get(id);
    BOOST_TEST(g.state() == "SUCCEEDED");
    BOOST_TEST(g.error().empty());
}

BOOST_AUTO_TEST_CASE(retryable_failure_is_not_terminal) {
    Db db;
    auto a = step("a");
    a.mutable_job()->mutable_policy()->set_max_attempts(2);
    auto w = db.create({a, step("b", {"a"})});
    const auto id = w.workflow_id();
    auto r = db.report(id, "a", tv1::FAILED, {}, {}, true);
    BOOST_REQUIRE(r.result == UpdateResult::kOk);
    BOOST_TEST(r.job->state() == tv1::QUEUED);
    BOOST_TEST(db.st(id, "a") == "QUEUED");
    BOOST_TEST(db.st(id, "b") == "WAITING");
    db.complete(id, "a");
    BOOST_TEST(db.st(id, "b") == "QUEUED");
}

BOOST_AUTO_TEST_CASE(missing_named_output_fails_step) {
    Db db;
    auto b = step("b");
    *b.add_inputs() = input("a", "out");
    auto w = db.create({step("a"), b, step("c", {"b"})});
    const auto id = w.workflow_id();
    db.complete(id, "a", {out(1, "other")});
    auto g = db.get(id);
    auto* sb = Db::find(g, "b");
    BOOST_TEST(sb->state() == "FAILED");
    BOOST_TEST(sb->job_id().empty());
    BOOST_TEST(sb->error() == "input a.out missing");
    BOOST_TEST(db.st(id, "c") == "SKIPPED");
    BOOST_TEST(g.state() == "FAILED");
    BOOST_TEST(g.error().find("input a.out missing") != std::string::npos);
}

BOOST_AUTO_TEST_CASE(invalid_spawns) {
    Db db;
    db.repo->set_workflow_limits(WorkflowLimits{30, 2, 1});

    // A standalone job may not spawn.
    tv1::JobSpec js;
    js.mutable_command()->add_argv("/bin/true");
    auto solo = db.repo->create_job(js, "a", T0);
    BOOST_REQUIRE(solo);
    db.lease_all();
    {
        const auto& l = db.held.at(solo->job_id());
        cv1::JobUpdate u;
        u.set_attempt_id(l.attempt_id());
        u.set_fencing_token(l.fencing_token());
        u.set_state(tv1::COMPLETED);
        *u.add_outputs() = out(1, "o");
        *u.add_spawn() = step("x");
        auto r = db.repo->update_job(solo->job_id(), db.n, u, ++db.now);
        BOOST_TEST((r.result == UpdateResult::kInvalid));
        BOOST_TEST(r.message == "invalid spawn: only workflow steps may spawn");
        auto j = db.job(solo->job_id());
        BOOST_TEST(j.state() == tv1::FAILED);
        BOOST_TEST(j.outputs_size() == 1);
        BOOST_TEST(db.repo->get_artifact(art(1)).has_value());
    }

    auto join = step("join", {"p_deadlock"});
    std::vector<tv1::StepSpec> steps = {join};
    const std::vector<std::string> cases = {"p_name",  "p_dup",   "p_unknown", "p_cycle",    "p_parent",
                                            "p_dot",   "p_many",  "p_depth",   "p_deadlock", "p_self",
                                            "p_spawn_dot", "p_template"};
    for (const auto& c : cases) steps.push_back(step(c));
    auto w = db.create(steps);
    const auto id = w.workflow_id();
    auto check = [&](const std::string& s, std::vector<tv1::StepSpec> spawn, const std::string& want) {
        auto r = db.report(id, s, tv1::COMPLETED, {out(100 + static_cast<int>(s.size()), "o")}, std::move(spawn));
        BOOST_TEST((r.result == UpdateResult::kInvalid), s);
        BOOST_TEST(r.message.starts_with("invalid spawn: "), r.message);
        BOOST_TEST(r.message.find(want) != std::string::npos, s << ": " << r.message << " lacks " << want);
        BOOST_REQUIRE(r.job);
        BOOST_TEST(r.job->state() == tv1::FAILED);
        BOOST_TEST(r.job->outputs_size() == 1);
        BOOST_TEST(db.st(id, s) == "FAILED");
    };
    check("p_name", {step("bad/name")}, "must be 1-64");
    check("p_dup", {step("x"), step("x")}, "duplicate step name x");
    check("p_unknown", {step("x", {"ghost"})}, "unknown dependency ghost");
    check("p_cycle", {step("x", {"y"}), step("y", {"x"})}, "cycle");
    check("p_parent", {step("x", {"p_parent"})}, "spawning step");
    check("p_dot", {step("x", {"."})}, "spawning step");
    check("p_many", {step("x"), step("y"), step("z")}, "too many steps in one expansion");
    // join depends on p_deadlock; a child depending on join would never run.
    check("p_deadlock", {step("x", {"join"})}, "would wait for spawning step");
    check("p_self", {step("x", {"x"})}, "depends on itself");
    auto sd = step("x");
    *sd.add_inputs() = input(".", "o", true);
    check("p_spawn_dot", {sd}, "cannot be spawned");
    auto tj = step("x");
    tj.mutable_job()->set_job_id("j1");
    check("p_template", {tj}, "must not set");
    // Depth: p_depth spawns (depth 1 = max) fine; its child spawning again is invalid.
    db.complete(id, "p_depth", {}, {step("k")});
    check("p_depth/k", {step("deeper")}, "depth 2 exceeds 1");
    BOOST_TEST(db.st(id, "join") == "SKIPPED");  // p_deadlock failed
    auto g = db.get(id);
    BOOST_TEST(g.state() == "FAILED");
    // Nothing from the invalid expansions was added.
    BOOST_TEST(g.steps_size() == static_cast<int>(steps.size()) + 1);
}

BOOST_AUTO_TEST_CASE(spawn_limit_per_workflow) {
    Db db;
    db.repo->set_workflow_limits(WorkflowLimits{4, 1000, 8});
    auto w = db.create({step("a"), step("b")});
    auto r = db.report(w.workflow_id(), "a", tv1::COMPLETED, {}, {step("x"), step("y"), step("z")});
    BOOST_TEST((r.result == UpdateResult::kInvalid));
    BOOST_TEST(r.message.find("exceed 4 steps") != std::string::npos, r.message);
    db.complete(w.workflow_id(), "b", {}, {step("x"), step("y")});
    BOOST_TEST(db.get(w.workflow_id()).steps_size() == 4);
}

BOOST_AUTO_TEST_CASE(cancel_workflow) {
    Db db;
    auto w = db.create({step("a"), step("q"), step("b", {"a"}), step("c", {"b"})});
    const auto id = w.workflow_id();
    db.lease_all();  // a and q LEASED
    const auto qa = db.job_of(id, "q");
    auto cw = db.repo->cancel_workflow(id, "stop", ++db.now);
    BOOST_REQUIRE(cw);
    BOOST_TEST(cw->state() == "CANCELLED");
    BOOST_TEST(cw->finished_unix_ms() == db.now);
    BOOST_TEST(Db::find(*cw, "b")->state() == "CANCELLED");
    BOOST_TEST(Db::find(*cw, "c")->state() == "CANCELLED");
    BOOST_TEST(db.job(db.job_of(id, "a")).cancel_requested());
    BOOST_TEST(db.job(qa).cancel_requested());
    // a completes anyway with a spawn: recorded, spawn dropped, nothing new starts.
    db.complete(id, "a", {out(1, "o")}, {step("x")});
    BOOST_TEST(db.st(id, "a") == "COMPLETED");
    BOOST_TEST(db.get(id).steps_size() == 4);
    auto r = db.report(id, "q", tv1::CANCELLED);
    BOOST_TEST((r.result == UpdateResult::kOk));
    auto g = db.get(id);
    BOOST_TEST(g.state() == "CANCELLED");
    BOOST_TEST(Db::find(g, "q")->state() == "CANCELLED");
    BOOST_TEST(db.job_count(id) == 2);
    // Cancel again: unchanged.
    BOOST_TEST(db.repo->cancel_workflow(id, "again", ++db.now)->state() == "CANCELLED");
    BOOST_TEST(!db.repo->cancel_workflow("wf-none", "", db.now));

    // QUEUED jobs are cancelled at once.
    auto w2 = db.create({step("a"), step("b", {"a"})});
    auto c2 = db.repo->cancel_workflow(w2.workflow_id(), "", ++db.now);
    BOOST_TEST(Db::find(*c2, "a")->state() == "CANCELLED");
    BOOST_TEST(db.job(db.job_of(w2.workflow_id(), "a")).state() == tv1::CANCELLED);
    BOOST_TEST(db.repo->list_workflows("CANCELLED", "", 10).size() == 2u);
}

BOOST_AUTO_TEST_CASE(cancel_job_and_expiry_drive_engine) {
    Db db;
    auto h = step("h", {"a"});
    h.set_run_if(tv1::ANY_FAILED);
    auto w = db.create({step("a"), step("b", {"a"}), h});
    const auto id = w.workflow_id();
    auto c = db.repo->cancel_job(db.job_of(id, "a"), "nope", ++db.now);
    BOOST_TEST((c.result == CancelResult::kCancelled));
    BOOST_TEST(db.st(id, "a") == "CANCELLED");
    BOOST_TEST(db.st(id, "b") == "SKIPPED");
    BOOST_TEST(db.st(id, "h") == "QUEUED");

    // Lease expiry with no attempts left -> FAILED -> engine.
    db.lease_all();
    auto ex = db.repo->expire_leases(db.now + TTL + 1);
    BOOST_REQUIRE_EQUAL(ex.size(), 1u);
    BOOST_TEST(ex[0].new_state == tv1::FAILED);
    auto g = db.get(id);
    BOOST_TEST(Db::find(g, "h")->state() == "FAILED");
    BOOST_TEST(Db::find(g, "h")->error() == "lease expired");
    BOOST_TEST(g.state() == "FAILED");
}

BOOST_AUTO_TEST_CASE(checkpoint_stored_and_resumed) {
    Db db;
    auto a = step("a");
    a.mutable_job()->mutable_policy()->set_max_attempts(3);
    a.mutable_job()->mutable_policy()->set_checkpoint(true);
    auto w = db.create({a});
    const auto id = w.workflow_id();
    db.lease_all();
    const auto jid = db.job_of(id, "a");
    auto l1 = db.held.at(jid);
    BOOST_TEST(l1.spec().resume_checkpoint().empty());
    cv1::JobUpdate u;
    u.set_attempt_id(l1.attempt_id());
    u.set_fencing_token(l1.fencing_token());
    u.set_state(tv1::RUNNING);
    *u.mutable_checkpoint() = out(50, "");
    BOOST_TEST((db.repo->update_job(jid, db.n, u, ++db.now).result == UpdateResult::kOk));
    auto j = db.job(jid);
    BOOST_TEST(j.checkpoint().artifact_id() == art(50));
    BOOST_TEST(j.checkpoint().name() == "checkpoint");
    BOOST_TEST(j.checkpoint().producer_job() == jid);
    BOOST_TEST(db.repo->get_artifact(art(50))->node_ids(0) == db.n);
    *u.mutable_checkpoint() = out(51, "ck");  // latest wins
    BOOST_TEST((db.repo->update_job(jid, db.n, u, ++db.now).result == UpdateResult::kOk));
    BOOST_TEST(db.job(jid).checkpoint().artifact_id() == art(51));

    // Retryable failure: the next lease resumes from the checkpoint.
    u.clear_checkpoint();
    u.set_state(tv1::FAILED);
    u.set_retryable(true);
    BOOST_TEST((db.repo->update_job(jid, db.n, u, ++db.now).result == UpdateResult::kOk));
    db.held.clear();
    db.lease_all();
    auto l2 = db.held.at(jid);
    BOOST_TEST(l2.spec().resume_checkpoint() == art(51));
    BOOST_TEST(db.job(jid).spec().resume_checkpoint().empty());  // stored spec untouched

    // A stale attempt cannot move the checkpoint (fenced).
    cv1::JobUpdate stale;
    stale.set_attempt_id(l1.attempt_id());
    stale.set_fencing_token(l1.fencing_token());
    stale.set_state(tv1::RUNNING);
    *stale.mutable_checkpoint() = out(52, "ck");
    BOOST_TEST((db.repo->update_job(jid, db.n, stale, ++db.now).result == UpdateResult::kStale));
    BOOST_TEST(db.job(jid).checkpoint().artifact_id() == art(51));

    // The only holder goes OFFLINE: the next attempt starts from scratch.
    cv1::JobUpdate f2;
    f2.set_attempt_id(l2.attempt_id());
    f2.set_fencing_token(l2.fencing_token());
    f2.set_state(tv1::FAILED);
    f2.set_retryable(true);
    BOOST_TEST((db.repo->update_job(jid, db.n, f2, ++db.now).result == UpdateResult::kOk));
    BOOST_REQUIRE(db.repo->set_offline(db.n, "gone", db.now));
    auto other = db.node("n2");
    auto ls = db.repo->lease_jobs(lreq(other), TTL, WAIT, ++db.now);
    BOOST_REQUIRE_EQUAL(ls.size(), 1u);
    BOOST_TEST(ls[0].job_id() == jid);
    BOOST_TEST(ls[0].spec().resume_checkpoint().empty());
}

BOOST_AUTO_TEST_CASE(sweep_is_a_safety_net) {
    Db db;
    auto w = db.create({step("a"), step("b", {"a"})});
    const auto id = w.workflow_id();
    BOOST_TEST(db.repo->sweep_workflows(db.now) == 0u);
    // Finish a's job behind the engine's back.
    psql(db.conninfo, "UPDATE jobs SET state = 'COMPLETED' WHERE workflow_id = '" + id + "'");
    BOOST_TEST(db.st(id, "b") == "WAITING");
    BOOST_TEST(db.repo->sweep_workflows(++db.now) == 2u);  // a recorded, b's job created
    BOOST_TEST(db.st(id, "b") == "QUEUED");
    BOOST_TEST(db.repo->sweep_workflows(++db.now) == 0u);  // idempotent
    db.complete(id, "b");
    BOOST_TEST(db.get(id).state() == "SUCCEEDED");
    BOOST_TEST(db.repo->sweep_workflows(++db.now) == 0u);
}

BOOST_AUTO_TEST_CASE(concurrent_sibling_completions) {
    Db db;
    constexpr int N = 12;
    for (int round = 0; round < 4; ++round) {
        std::vector<tv1::StepSpec> steps;
        std::vector<std::string> all;
        for (int i = 0; i < N; ++i) {
            steps.push_back(step(std::format("s{}", i)));
            steps.push_back(step(std::format("t{}", i), {std::format("s{}", i)}));
            all.push_back(std::format("s{}", i));
        }
        steps.push_back(step("join", all));
        auto w = db.create(steps);
        const auto id = w.workflow_id();
        db.lease_all();
        std::vector<cv1::Lease> ls;
        for (int i = 0; i < N; ++i) ls.push_back(db.held.at(db.job_of(id, std::format("s{}", i))));
        std::string errors[2];
        auto worker = [&](int parity) {
            auto repo = open_pg_repository(db.conninfo);
            for (int i = parity; i < N; i += 2) {
                cv1::JobUpdate u;
                u.set_attempt_id(ls[static_cast<std::size_t>(i)].attempt_id());
                u.set_fencing_token(ls[static_cast<std::size_t>(i)].fencing_token());
                u.set_state(tv1::COMPLETED);
                *u.add_outputs() = out(1000 + i, "o");
                auto r = repo->update_job(ls[static_cast<std::size_t>(i)].job_id(), db.n, u, T0 + 100);
                if (r.result != UpdateResult::kOk) errors[parity] += r.message + "; ";
            }
        };
        std::thread a(worker, 0), b(worker, 1);
        a.join();
        b.join();
        BOOST_TEST(errors[0].empty(), errors[0]);
        BOOST_TEST(errors[1].empty(), errors[1]);
        auto g = db.get(id);
        for (int i = 0; i < N; ++i) BOOST_TEST(Db::find(g, std::format("t{}", i))->state() == "QUEUED");
        BOOST_TEST(Db::find(g, "join")->state() == "QUEUED");
        BOOST_TEST(db.job_count(id) == 2 * N + 1);
        BOOST_TEST(psql(db.conninfo, "SELECT count(DISTINCT step) FROM jobs WHERE workflow_id = '" + id + "'") ==
                   std::to_string(2 * N + 1));
        for (const auto& l : ls) db.held.erase(l.job_id());
    }
}

BOOST_AUTO_TEST_CASE(concurrent_fanout_children_one_join) {
    Db db;
    constexpr int N = 10;
    auto join = step("join", {"fan"});
    *join.add_inputs() = input("fan", "o", true);
    auto w = db.create({step("fan"), join});
    const auto id = w.workflow_id();
    std::vector<tv1::StepSpec> kids;
    for (int i = 0; i < N; ++i) kids.push_back(step(std::format("k{}", i)));
    db.complete(id, "fan", {}, kids);
    db.lease_all();
    std::vector<cv1::Lease> ls;
    for (int i = 0; i < N; ++i) ls.push_back(db.held.at(db.job_of(id, std::format("fan/k{}", i))));
    auto worker = [&](int parity) {
        auto repo = open_pg_repository(db.conninfo);
        for (int i = parity; i < N; i += 2) {
            cv1::JobUpdate u;
            u.set_attempt_id(ls[static_cast<std::size_t>(i)].attempt_id());
            u.set_fencing_token(ls[static_cast<std::size_t>(i)].fencing_token());
            u.set_state(tv1::COMPLETED);
            *u.add_outputs() = out(2000 + i, "o");
            repo->update_job(ls[static_cast<std::size_t>(i)].job_id(), db.n, u, T0 + 100);
        }
    };
    std::thread a(worker, 0), b(worker, 1);
    a.join();
    b.join();
    BOOST_TEST(db.st(id, "join") == "QUEUED");
    BOOST_TEST(db.job(db.job_of(id, "join")).spec().input_artifacts_size() == N);
    BOOST_TEST(db.job_count(id) == N + 2);
}

BOOST_AUTO_TEST_SUITE_END()
