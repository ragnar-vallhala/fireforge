// Materials database on a throwaway PostgreSQL cluster (docs/design/phase8.md §2-§3).
#include <libpq-fe.h>

#include <boost/test/unit_test.hpp>
#include <atomic>
#include <format>
#include <functional>
#include <set>
#include <string>
#include <thread>

#include "tdr/cpdb/db.hpp"
#include "tdr/cpdb/testing.hpp"

using namespace tdr::cpdb;

BOOST_TEST_DONT_PRINT_LOG_VALUE(tdr::cpdb::UpdateResult)
BOOST_TEST_DONT_PRINT_LOG_VALUE(::fireforge::tdr::v1::JobState)
BOOST_TEST_DONT_PRINT_LOG_VALUE(::fireforge::tdr::v1::Evidence)

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

cv1::ArtifactRef output(int n, const std::string& name) {
    cv1::ArtifactRef a;
    a.set_artifact_id(art(n));
    a.set_size(10);
    a.set_name(name);
    return a;
}

// Half-Heusler-like structure: three sites.
tv1::Structure structure(const std::string& fp, std::vector<std::string> species = {"Zr", "Ni", "Sn"},
                         const std::string& reduced = "ZrNiSn") {
    tv1::Structure s;
    for (double x : {0.0, 3.0, 3.0, 3.0, 0.0, 3.0, 3.0, 3.0, 0.0}) s.add_lattice(x);
    double f = 0;
    for (const auto& sp : species) {
        auto* site = s.add_sites();
        site->set_species(sp);
        for (int i = 0; i < 3; ++i) site->add_frac(f);
        f += 0.25;
    }
    s.set_reduced_formula(reduced);
    s.set_spacegroup_number(216);
    s.set_prototype("half-heusler");
    s.set_fingerprint(fp);
    return s;
}

tv1::CalcRecord record(const std::string& kind, const std::string& fp, const std::string& props = "{\"e_ev\": -1.5}") {
    tv1::CalcRecord r;
    r.set_kind(kind);
    *r.mutable_structure() = structure(fp);
    r.set_properties_json(props);
    r.mutable_method()->set_solver("toy");
    r.mutable_method()->set_settings_json("{\"ecut\": 40}");
    return r;
}

cv1::LeaseRequest lreq(const std::string& node, std::uint32_t max_jobs = 1) {
    cv1::LeaseRequest r;
    r.set_node_id(node);
    r.mutable_free()->set_cpu_cores(64);
    r.mutable_free()->set_memory_bytes(256 * GiB);
    r.set_max_jobs(max_jobs);
    return r;
}

cv1::JobUpdate upd(const cv1::Lease& l, tv1::JobState s) {
    cv1::JobUpdate u;
    u.set_attempt_id(l.attempt_id());
    u.set_fencing_token(l.fencing_token());
    u.set_state(s);
    return u;
}

struct Db {
    std::string conninfo;
    std::unique_ptr<IRepository> repo;
    std::string n;

    Db() {
        static int k = 0;
        conninfo = cluster().create_database("m" + std::to_string(++k));
        repo = open_pg_repository(conninfo);
        n = node("n1");
    }

    std::string node(const std::string& name) {
        cv1::RegisterRequest r;
        r.set_name(name);
        r.add_capabilities("cpu");
        return repo->register_node(r, "", T0).node_id();
    }

    std::string campaign(const std::string& name) {
        cv1::CreateCampaignRequest r;
        r.set_name(name);
        auto c = repo->create_campaign(r, "alice", T0);
        BOOST_REQUIRE(c);
        auto cc = *c;
        cc.set_state(cv1::ACTIVE);
        BOOST_REQUIRE(repo->update_campaign(cc, cv1::DRAFT, T0));
        return c->campaign_id();
    }

    std::string job(const std::string& campaign = {}, std::uint32_t attempts = 3) {
        tv1::JobSpec s;
        s.mutable_command()->add_argv("/bin/true");
        s.mutable_policy()->set_max_attempts(attempts);
        s.set_campaign_id(campaign);
        auto j = repo->create_job(s, "admin:alice", T0);
        BOOST_REQUIRE_MESSAGE(j, j.error());
        return j->job_id();
    }

    cv1::Lease lease(std::int64_t now = T0) {
        auto ls = repo->lease_jobs(lreq(n), TTL, WAIT, now);
        BOOST_REQUIRE_EQUAL(ls.size(), 1u);
        return ls[0];
    }

    // New job, leased and completed with these records; returns the outcome.
    UpdateOutcome complete(std::vector<tv1::CalcRecord> records, const std::string& campaign = {},
                           std::vector<cv1::ArtifactRef> outputs = {}) {
        job(campaign);
        auto l = lease();
        auto u = upd(l, tv1::COMPLETED);
        for (auto& o : outputs) *u.add_outputs() = o;
        for (auto& r : records) *u.add_records() = r;
        return repo->update_job(l.job_id(), n, u, T0 + 1);
    }

    std::string count(const std::string& table) { return psql(conninfo, "SELECT count(*) FROM " + table); }
};

}  // namespace

BOOST_AUTO_TEST_SUITE(cpdb_materials)

BOOST_AUTO_TEST_CASE(records_stored_with_completed_job) {
    Db db;
    auto camp = db.campaign("hh");
    auto r = record("qe.relax", "fp1:aaa");
    r.add_output_names("relax.out");
    r.add_input_artifacts(art(9));
    r.set_conditions_json("{\"temperature_k\": 300}");
    r.set_note("first");
    auto out = db.complete({r}, camp, {output(1, "relax.out"), output(2, "other")});
    BOOST_REQUIRE_MESSAGE(out.result == UpdateResult::kOk, out.message);
    BOOST_TEST(out.job->state() == tv1::COMPLETED);

    auto list = db.repo->list_calculations({});
    BOOST_REQUIRE_EQUAL(list.calculations_size(), 1);
    const auto& c = list.calculations(0);
    BOOST_TEST(c.calc_id().starts_with("calc-"));
    BOOST_TEST(c.structure_id().starts_with("st-"));
    BOOST_TEST(c.material_id().starts_with("mat-"));
    BOOST_TEST(c.reduced_formula() == "ZrNiSn");
    BOOST_TEST(c.kind() == "qe.relax");
    BOOST_TEST(c.properties_json() == "{\"e_ev\": -1.5}");
    BOOST_TEST(c.conditions_json() == "{\"temperature_k\": 300}");
    BOOST_TEST(c.convergence_json() == "{}");
    BOOST_TEST(c.method().solver() == "toy");
    BOOST_TEST(c.method().settings_json() == "{\"ecut\": 40}");
    BOOST_TEST(c.evidence() == tv1::PREDICTED);
    BOOST_REQUIRE_EQUAL(c.outputs_size(), 1);
    BOOST_TEST(c.outputs(0).name() == "relax.out");
    BOOST_TEST(c.outputs(0).artifact_id() == art(1));
    BOOST_REQUIRE_EQUAL(c.input_artifacts_size(), 1);
    BOOST_TEST(c.input_artifacts(0) == art(9));
    BOOST_TEST(c.job_id() == out.job->job_id());
    BOOST_TEST(c.attempt_id() == out.job->current_attempt_id());
    BOOST_TEST(c.campaign_id() == camp);
    BOOST_TEST(c.node_id() == db.n);
    BOOST_TEST(c.workflow_id() == "");
    BOOST_TEST(c.note() == "first");
    BOOST_TEST(c.created_unix_ms() == T0 + 1);
    BOOST_TEST(list.next() == "");

    auto got = db.repo->get_calculation(c.calc_id());
    BOOST_REQUIRE(got);
    BOOST_TEST(got->SerializeAsString() == c.SerializeAsString());
    BOOST_TEST(!db.repo->get_calculation("calc-nope"));

    auto st = db.repo->get_structure(c.structure_id());
    BOOST_REQUIRE(st);
    BOOST_TEST(st->material_id() == c.material_id());
    BOOST_TEST(st->source_job_id() == out.job->job_id());
    BOOST_TEST(st->structure().fingerprint() == "fp1:aaa");
    BOOST_TEST(st->structure().sites_size() == 3);
    BOOST_TEST(st->structure().spacegroup_number() == 216);
    BOOST_TEST(!db.repo->get_structure("st-nope"));

    auto m = db.repo->get_material(c.material_id());
    BOOST_REQUIRE(m);
    BOOST_TEST(m->reduced_formula() == "ZrNiSn");
    BOOST_TEST((std::vector<std::string>(m->elements().begin(), m->elements().end()) ==
                std::vector<std::string>{"Ni", "Sn", "Zr"}));
    BOOST_TEST(m->structure_count() == 1u);
    BOOST_TEST(m->calculation_count() == 1u);
    BOOST_TEST(m->structures_size() == 1);
    BOOST_TEST(m->latest_size() == 1);
    BOOST_TEST(psql(db.conninfo, "SELECT nsites_min FROM materials") == "3");
    BOOST_TEST(!db.repo->get_material("mat-nope"));
}

BOOST_AUTO_TEST_CASE(workflow_context_and_reduced_formula_fallback) {
    Db db;
    cv1::CreateWorkflowRequest req;
    req.set_name("wf");
    auto* s = req.add_steps();
    s->set_name("relax");
    s->mutable_job()->mutable_command()->add_argv("/bin/true");
    s->mutable_job()->mutable_policy()->set_max_attempts(1);
    auto w = db.repo->create_workflow(req, "admin:alice", T0);
    BOOST_REQUIRE_MESSAGE(w, w.error());
    auto l = db.lease();
    auto u = upd(l, tv1::COMPLETED);
    auto r = record("qe.scf", "fp1:nacl");
    *r.mutable_structure() = structure("fp1:nacl", {"Na", "Cl", "Na", "Cl"}, "");
    *u.add_records() = r;
    auto out = db.repo->update_job(l.job_id(), db.n, u, T0 + 1);
    BOOST_REQUIRE_MESSAGE(out.result == UpdateResult::kOk, out.message);
    auto list = db.repo->list_calculations({});
    BOOST_REQUIRE_EQUAL(list.calculations_size(), 1);
    BOOST_TEST(list.calculations(0).workflow_id() == w->workflow_id());
    BOOST_TEST(list.calculations(0).step() == "relax");
    BOOST_TEST(list.calculations(0).reduced_formula() == "NaCl");  // derived: first appearance, gcd
    BOOST_TEST(psql(db.conninfo, "SELECT nsites_min FROM materials") == "4");
    IRepository::CalcQuery q;
    q.workflow_id = w->workflow_id();
    BOOST_TEST(db.repo->list_calculations(q).calculations_size() == 1);
    q.workflow_id = "wf-other";
    BOOST_TEST(db.repo->list_calculations(q).calculations_size() == 0);
}

BOOST_AUTO_TEST_CASE(structures_and_materials_deduplicated) {
    Db db;
    BOOST_REQUIRE(db.complete({record("qe.relax", "fp1:a")}).result == UpdateResult::kOk);
    // Same fingerprint again (twice in one update) + a new structure of the same material.
    BOOST_REQUIRE(db.complete({record("qe.scf", "fp1:a"), record("phonon", "fp1:a"), record("qe.relax", "fp1:b")})
                      .result == UpdateResult::kOk);
    BOOST_TEST(db.count("materials") == "1");
    BOOST_TEST(db.count("structures") == "2");
    BOOST_TEST(db.count("calculations") == "4");
    // structure_id instead of an inline structure.
    const std::string sid = psql(db.conninfo, "SELECT structure_id FROM structures WHERE fingerprint = 'fp1:b'");
    tv1::CalcRecord byid;
    byid.set_kind("te.evaluate");
    byid.set_structure_id(sid);
    BOOST_REQUIRE(db.complete({byid}).result == UpdateResult::kOk);
    // A different material.
    auto other = record("qe.relax", "fp1:c");
    *other.mutable_structure() = structure("fp1:c", {"Ti", "Co", "Sb"}, "TiCoSb");
    BOOST_REQUIRE(db.complete({other}).result == UpdateResult::kOk);
    BOOST_TEST(db.count("materials") == "2");
    BOOST_TEST(db.count("structures") == "3");
    IRepository::CalcQuery q;
    q.structure_id = sid;
    auto l = db.repo->list_calculations(q);
    BOOST_REQUIRE_EQUAL(l.calculations_size(), 2);
    BOOST_TEST(l.calculations(0).kind() == "te.evaluate");
    BOOST_TEST(l.calculations(1).kind() == "qe.relax");
}

BOOST_AUTO_TEST_CASE(invalid_records_store_nothing_and_fail_job) {
    struct Case {
        std::string expect;
        std::function<void(std::vector<tv1::CalcRecord>&)> mutate;
    };
    std::string big(300 * 1024, 'x');
    std::vector<Case> cases = {
        {"kind is required", [](auto& rs) { rs[0].clear_kind(); }},
        {"kind is longer", [](auto& rs) { rs[0].set_kind(std::string(65, 'k')); }},
        {"structure_id or structure is required", [](auto& rs) { rs[0].clear_structure(); }},
        {"fingerprint", [](auto& rs) { rs[0].mutable_structure()->set_fingerprint("sha:abc"); }},
        {"fingerprint", [](auto& rs) { rs[0].mutable_structure()->clear_fingerprint(); }},
        {"lattice", [](auto& rs) { rs[0].mutable_structure()->clear_lattice(); }},
        {"sites must not be empty", [](auto& rs) { rs[0].mutable_structure()->clear_sites(); }},
        {"not an element symbol", [](auto& rs) { rs[0].mutable_structure()->mutable_sites(1)->set_species("ni"); }},
        {"unknown structure", [](auto& rs) {
             rs[0].clear_structure();
             rs[0].set_structure_id("st-missing");
         }},
        {"properties_json is not valid JSON", [](auto& rs) { rs[0].set_properties_json("{nope"); }},
        {"properties_json must be a JSON object", [](auto& rs) { rs[0].set_properties_json("[1, 2]"); }},
        {"conditions_json must be a JSON object", [](auto& rs) { rs[0].set_conditions_json("3"); }},
        {"convergence_json", [](auto& rs) { rs[0].set_convergence_json("\"x\""); }},
        {"uncertainty_json", [](auto& rs) { rs[0].set_uncertainty_json("null"); }},
        {"method.settings_json", [](auto& rs) { rs[0].mutable_method()->set_settings_json("[]"); }},
        {"'missing.out' is not an output", [](auto& rs) { rs[0].add_output_names("missing.out"); }},
        {"evidence PREDICTED", [](auto& rs) { rs[0].set_evidence(tv1::LITERATURE); }},
        {"evidence PREDICTED", [](auto& rs) { rs[0].set_evidence(tv1::MEASURED); }},
        {"maximum of 64", [](auto& rs) { rs.resize(65, rs[0]); }},
        {"bytes", [&big](auto& rs) {
             rs[0].set_note(big);
         }},
        // A valid first record followed by an invalid one: nothing at all is stored.
        {"record 1", [](auto& rs) {
             rs.push_back(rs[0]);
             rs[1].clear_kind();
         }},
    };
    Db db;
    for (const auto& c : cases) {
        BOOST_TEST_CONTEXT("case " << c.expect) {
            std::vector<tv1::CalcRecord> rs = {record("qe.relax", "fp1:v")};
            c.mutate(rs);
            auto out = db.complete(rs, {}, {output(1, "relax.out")});
            BOOST_TEST(out.result == UpdateResult::kInvalid);
            BOOST_TEST(out.message.find(c.expect) != std::string::npos, out.message);
            BOOST_TEST(out.message.starts_with("invalid records: "), out.message);
            BOOST_REQUIRE(out.job);
            BOOST_TEST(out.job->state() == tv1::FAILED);  // not retried, not COMPLETED
            BOOST_TEST(out.job->last_error() == out.message);
            BOOST_TEST(out.job->outputs_size() == 1);     // outputs are kept (like an invalid spawn)
            BOOST_TEST(db.count("calculations") == "0");
            BOOST_TEST(db.count("structures") == "0");
            BOOST_TEST(db.count("materials") == "0");
        }
    }
}

BOOST_AUTO_TEST_CASE(records_only_with_completed) {
    Db db;
    auto id = db.job();
    auto l = db.lease();
    for (auto s : {tv1::RUNNING, tv1::FAILED, tv1::CANCELLED}) {
        auto u = upd(l, s);
        *u.add_records() = record("qe.relax", "fp1:a");
        auto out = db.repo->update_job(id, db.n, u, T0 + 1);
        BOOST_TEST(out.result == UpdateResult::kInvalid);
        BOOST_TEST(out.message.find("COMPLETED") != std::string::npos);
        BOOST_TEST(db.repo->get_job(id, false)->state() == tv1::LEASED);  // unchanged
    }
    BOOST_TEST(db.count("calculations") == "0");
}

BOOST_AUTO_TEST_CASE(stale_attempt_stores_nothing) {
    Db db;
    auto id = db.job();
    auto first = db.lease();
    BOOST_REQUIRE_EQUAL(db.repo->expire_leases(T0 + TTL + 1).size(), 1u);
    auto second = db.lease(T0 + TTL + 2);
    BOOST_TEST(second.attempt_id() != first.attempt_id());

    auto u = upd(first, tv1::COMPLETED);
    *u.add_records() = record("qe.relax", "fp1:a");
    auto out = db.repo->update_job(id, db.n, u, T0 + TTL + 3);
    BOOST_TEST(out.result == UpdateResult::kStale);
    BOOST_TEST(db.count("calculations") == "0");
    BOOST_TEST(db.count("structures") == "0");

    // An invalid record from a stale attempt is still just stale.
    u.mutable_records(0)->clear_kind();
    BOOST_TEST(db.repo->update_job(id, db.n, u, T0 + TTL + 3).result == UpdateResult::kStale);

    auto ok = upd(second, tv1::COMPLETED);
    *ok.add_records() = record("qe.relax", "fp1:a");
    BOOST_REQUIRE(db.repo->update_job(id, db.n, ok, T0 + TTL + 4).result == UpdateResult::kOk);
    // Replaying the completed update is stale and adds nothing.
    BOOST_TEST(db.repo->update_job(id, db.n, ok, T0 + TTL + 5).result == UpdateResult::kStale);
    BOOST_TEST(db.count("calculations") == "1");
    BOOST_TEST(db.repo->list_calculations({}).calculations(0).attempt_id() == second.attempt_id());
}

BOOST_AUTO_TEST_CASE(retried_attempt_stores_once) {
    Db db;
    auto id = db.job();
    auto first = db.lease();
    auto f = upd(first, tv1::FAILED);
    f.set_retryable(true);
    f.set_error("boom");
    BOOST_REQUIRE(db.repo->update_job(id, db.n, f, T0 + 1).result == UpdateResult::kOk);
    auto second = db.lease(T0 + 2);
    auto ok = upd(second, tv1::COMPLETED);
    *ok.add_records() = record("qe.relax", "fp1:a");
    BOOST_REQUIRE(db.repo->update_job(id, db.n, ok, T0 + 3).result == UpdateResult::kOk);
    auto l = db.repo->list_calculations({});
    BOOST_REQUIRE_EQUAL(l.calculations_size(), 1);
    BOOST_TEST(l.calculations(0).attempt_id() == second.attempt_id());
}

BOOST_AUTO_TEST_CASE(limits_at_the_edge_are_accepted) {
    Db db;
    std::vector<tv1::CalcRecord> rs(64, record("qe.relax", "fp1:a", "{}"));
    auto out = db.complete(rs);
    BOOST_REQUIRE_MESSAGE(out.result == UpdateResult::kOk, out.message);
    BOOST_TEST(db.count("calculations") == "64");
    // Just under 256 KiB in total.
    auto r = record("qe.relax", "fp1:a", "{}");
    r.set_note(std::string(250 * 1024, 'n'));
    out = db.complete({r});
    BOOST_REQUIRE_MESSAGE(out.result == UpdateResult::kOk, out.message);
}

BOOST_AUTO_TEST_CASE(list_filters_and_paging) {
    Db db;
    auto c1 = db.campaign("c1");
    auto c2 = db.campaign("c2");
    auto ti = record("qe.relax", "fp1:ti");
    *ti.mutable_structure() = structure("fp1:ti", {"Ti", "Co", "Sb"}, "TiCoSb");
    BOOST_REQUIRE(db.complete({record("qe.relax", "fp1:a"), record("qe.scf", "fp1:a")}, c1).result == UpdateResult::kOk);
    BOOST_REQUIRE(db.complete({record("qe.scf", "fp1:a"), ti}, c2).result == UpdateResult::kOk);
    BOOST_REQUIRE(db.complete({record("phonon", "fp1:a")}).result == UpdateResult::kOk);

    // Newest first.
    auto all = db.repo->list_calculations({});
    BOOST_REQUIRE_EQUAL(all.calculations_size(), 5);
    BOOST_TEST(all.calculations(0).kind() == "phonon");
    BOOST_TEST(all.calculations(4).kind() == "qe.relax");
    const std::string zr_mat = all.calculations(0).material_id();

    IRepository::CalcQuery q;
    q.kind = "qe.scf";
    BOOST_TEST(db.repo->list_calculations(q).calculations_size() == 2);
    q.campaign_id = c2;
    BOOST_TEST(db.repo->list_calculations(q).calculations_size() == 1);
    q = {};
    q.material_id = zr_mat;
    BOOST_TEST(db.repo->list_calculations(q).calculations_size() == 4);
    q.evidence = tv1::PREDICTED;
    BOOST_TEST(db.repo->list_calculations(q).calculations_size() == 4);
    q.evidence = tv1::LITERATURE;
    BOOST_TEST(db.repo->list_calculations(q).calculations_size() == 0);

    // Paging: 2 + 2 + 1.
    q = {};
    q.limit = 2;
    std::vector<std::string> seen;
    for (int page = 0; page < 3; ++page) {
        auto l = db.repo->list_calculations(q);
        BOOST_REQUIRE_EQUAL(l.calculations_size(), page < 2 ? 2 : 1);
        for (const auto& c : l.calculations()) seen.push_back(c.calc_id());
        BOOST_TEST((l.next().empty() == (page == 2)));
        q.after = l.next();
    }
    std::vector<std::string> expect;
    for (const auto& c : all.calculations()) expect.push_back(c.calc_id());
    BOOST_TEST(seen == expect);
    // Exactly one full page: no next.
    q = {};
    q.limit = 5;
    BOOST_TEST(db.repo->list_calculations(q).next() == "");
    q.limit = 0;  // clamped to 1
    BOOST_TEST(db.repo->list_calculations(q).calculations_size() == 1);
    q.limit = 100000;  // capped at 1000
    BOOST_TEST(db.repo->list_calculations(q).calculations_size() == 5);
    q = {};
    q.after = "calc-unknown";
    BOOST_TEST(db.repo->list_calculations(q).calculations_size() == 0);

    // Materials.
    auto mats = db.repo->list_materials("", "", "", 100);
    BOOST_REQUIRE_EQUAL(mats.size(), 2u);
    BOOST_TEST(mats[0].reduced_formula() == "TiCoSb");  // ordered by formula
    BOOST_TEST(mats[1].reduced_formula() == "ZrNiSn");
    BOOST_TEST(mats[1].calculation_count() == 4u);
    BOOST_TEST(mats[1].structures_size() == 0);  // list has no details
    BOOST_TEST(db.repo->list_materials("ZrNiSn", "", "", 100).size() == 1u);
    BOOST_TEST(db.repo->list_materials("Zr", "", "", 100).size() == 0u);
    BOOST_TEST(db.repo->list_materials("", "Ni", "", 100).size() == 1u);
    BOOST_TEST(db.repo->list_materials("", "Sb", "", 100)[0].reduced_formula() == "TiCoSb");
    BOOST_TEST(db.repo->list_materials("", "Fe", "", 100).size() == 0u);
    BOOST_TEST(db.repo->list_materials("", "", c1, 100).size() == 1u);
    BOOST_TEST(db.repo->list_materials("", "", c2, 100).size() == 2u);
    BOOST_TEST(db.repo->list_materials("", "", "", 1).size() == 1u);
}

BOOST_AUTO_TEST_CASE(get_material_latest_per_kind) {
    Db db;
    BOOST_REQUIRE(db.complete({record("qe.relax", "fp1:a", "{\"v\": 1}"), record("qe.scf", "fp1:a", "{\"v\": 2}")})
                      .result == UpdateResult::kOk);
    BOOST_REQUIRE(db.complete({record("qe.relax", "fp1:b", "{\"v\": 3}")}).result == UpdateResult::kOk);
    auto mats = db.repo->list_materials("", "", "", 10);
    BOOST_REQUIRE_EQUAL(mats.size(), 1u);
    auto m = db.repo->get_material(mats[0].material_id());
    BOOST_REQUIRE(m);
    BOOST_TEST(m->structures_size() == 2);
    BOOST_TEST(m->structures(0).structure().fingerprint() == "fp1:a");
    BOOST_TEST(m->calculation_count() == 3u);
    BOOST_REQUIRE_EQUAL(m->latest_size(), 2);
    BOOST_TEST(m->latest(0).kind() == "qe.relax");
    BOOST_TEST(m->latest(0).properties_json() == "{\"v\": 3}");
    BOOST_TEST(m->latest(1).kind() == "qe.scf");
    BOOST_TEST(m->latest(1).properties_json() == "{\"v\": 2}");
}

BOOST_AUTO_TEST_CASE(import_rules) {
    Db db;
    auto camp = db.campaign("lit");
    cv1::ImportCalculationsRequest req;
    auto r = record("literature.kappa", "fp1:a", "{\"kappa_l_w_mk_300k\": 7.2}");
    r.set_evidence(tv1::LITERATURE);
    *req.add_records() = r;
    r.set_evidence(tv1::MEASURED);
    r.set_kind("measured.zt");
    *req.add_records() = r;
    req.set_campaign_id(camp);

    // Evidence must be LITERATURE/MEASURED; output_names are not allowed; all or nothing.
    auto bad = req;
    bad.mutable_records(1)->set_evidence(tv1::PREDICTED);
    auto e = db.repo->import_calculations(bad, "admin:alice", T0);
    BOOST_REQUIRE(!e);
    BOOST_TEST(e.error().find("LITERATURE or MEASURED") != std::string::npos, e.error());
    bad = req;
    bad.mutable_records(1)->add_output_names("x");
    e = db.repo->import_calculations(bad, "admin:alice", T0);
    BOOST_REQUIRE(!e);
    BOOST_TEST(e.error().find("output_names") != std::string::npos, e.error());
    bad = req;
    bad.mutable_records(1)->mutable_structure()->set_fingerprint("nofp");
    BOOST_TEST(!db.repo->import_calculations(bad, "admin:alice", T0));
    bad = req;
    bad.set_campaign_id("camp-missing");
    e = db.repo->import_calculations(bad, "admin:alice", T0);
    BOOST_REQUIRE(!e);
    BOOST_TEST(e.error().find("unknown campaign") != std::string::npos);
    BOOST_TEST(!db.repo->import_calculations(cv1::ImportCalculationsRequest{}, "admin:alice", T0));
    BOOST_TEST(db.count("calculations") == "0");
    BOOST_TEST(db.count("structures") == "0");
    BOOST_TEST(db.count("materials") == "0");

    auto ok = db.repo->import_calculations(req, "admin:alice", T0 + 5);
    BOOST_REQUIRE_MESSAGE(ok, ok.error());
    BOOST_REQUIRE_EQUAL(ok->calc_ids_size(), 2);
    auto c = db.repo->get_calculation(ok->calc_ids(0));
    BOOST_REQUIRE(c);
    BOOST_TEST(c->evidence() == tv1::LITERATURE);
    BOOST_TEST(c->campaign_id() == camp);
    BOOST_TEST(c->job_id() == "");
    BOOST_TEST(c->node_id() == "");
    BOOST_TEST(db.repo->get_structure(c->structure_id())->source_job_id() == "");
    IRepository::CalcQuery q;
    q.evidence = tv1::MEASURED;
    auto l = db.repo->list_calculations(q);
    BOOST_REQUIRE_EQUAL(l.calculations_size(), 1);
    BOOST_TEST(l.calculations(0).kind() == "measured.zt");
    auto audit = db.repo->list_audit(10);
    BOOST_REQUIRE(!audit.empty());
    BOOST_TEST(audit[0].action() == "calculations.import");
    BOOST_TEST(audit[0].actor() == "admin:alice");
    BOOST_TEST(audit[0].target() == camp);

    // A worker record referencing the imported structure by id shares its material.
    tv1::CalcRecord byid;
    byid.set_kind("qe.relax");
    byid.set_structure_id(c->structure_id());
    BOOST_REQUIRE(db.complete({byid}).result == UpdateResult::kOk);
    BOOST_TEST(db.count("materials") == "1");
}

BOOST_AUTO_TEST_CASE(migrates_v5_database_to_v6) {
    Db db;
    auto id = db.job();
    auto l = db.lease();
    psql(db.conninfo,
         "DROP TABLE calculations, structures, materials; DELETE FROM schema_migrations WHERE version >= 6");
    BOOST_TEST(schema_version(db.conninfo) == 5);
    db.repo = open_pg_repository(db.conninfo);
    BOOST_TEST(schema_version(db.conninfo) == 6);
    BOOST_TEST(db.repo->get_job(id, false)->state() == tv1::LEASED);
    auto u = upd(l, tv1::COMPLETED);
    *u.add_records() = record("qe.relax", "fp1:a");
    BOOST_REQUIRE(db.repo->update_job(id, db.n, u, T0 + 1).result == UpdateResult::kOk);
    BOOST_TEST(db.count("calculations") == "1");
}

BOOST_AUTO_TEST_CASE(concurrent_same_fingerprint_inserts) {
    Db db;
    auto second = open_pg_repository(db.conninfo);
    constexpr int kRounds = 15;
    // Two jobs per round, each leased on its own node / connection.
    auto n2 = db.node("n2");
    std::vector<std::pair<cv1::Lease, cv1::Lease>> leases;
    for (int i = 0; i < kRounds; ++i) {
        db.job();
        auto a = db.repo->lease_jobs(lreq(db.n), TTL, WAIT, T0);
        db.job();
        auto b = db.repo->lease_jobs(lreq(n2), TTL, WAIT, T0);
        BOOST_REQUIRE(a.size() == 1u && b.size() == 1u);
        leases.emplace_back(a[0], b[0]);
    }
    auto make = [](const cv1::Lease& l, int i) {
        auto u = upd(l, tv1::COMPLETED);
        // Overlapping fingerprints in different orders, plus a fresh formula per round.
        for (const auto& fp : {std::format("fp1:r{}", i), std::format("fp1:s{}", i)}) {
            auto r = record("qe.relax", fp);
            *r.mutable_structure() = structure(fp, {"Ti", "Co", "Sb"}, std::format("Ti{}CoSb", i + 2));
            *u.add_records() = r;
        }
        return u;
    };
    std::atomic<int> ok{0};
    std::string err;
    for (int i = 0; i < kRounds; ++i) {
        auto ua = make(leases[i].first, i);
        auto ub = make(leases[i].second, i);
        std::swap(*ub.mutable_records(0), *ub.mutable_records(1));
        std::thread t([&] {
            try {
                if (second->update_job(leases[i].second.job_id(), n2, ub, T0 + 1).result == UpdateResult::kOk) ++ok;
            } catch (const std::exception& e) {
                err = e.what();
            }
        });
        if (db.repo->update_job(leases[i].first.job_id(), db.n, ua, T0 + 1).result == UpdateResult::kOk) ++ok;
        t.join();
    }
    BOOST_TEST(err == "");
    BOOST_TEST(ok.load() == 2 * kRounds);
    BOOST_TEST(db.count("structures") == std::to_string(2 * kRounds));
    BOOST_TEST(db.count("materials") == std::to_string(kRounds));
    BOOST_TEST(db.count("calculations") == std::to_string(4 * kRounds));
}

BOOST_AUTO_TEST_SUITE_END()
