// Phase 8 endpoints: materials, structures, calculations, import (docs/design/phase8.md §2).
#include <format>
#include <map>
#include <memory>

#include <boost/test/unit_test.hpp>

#include "fake_repo.hpp"
#include "tdr/cpapi/api.hpp"
#include "tdr/cpdb/testing.hpp"

using namespace tdr;
using namespace tdr::cpapi;
using tdr::cpapi::testing::FakeRepository;

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

template <class M>
M parse(const http::Response& r) {
    M m;
    std::string err;
    BOOST_REQUIRE_MESSAGE(from_json(r.body, m, &err), "bad reply JSON: " << err << " body=" << r.body);
    return m;
}

http::Response call(ControlApi& api, const std::string& method, const std::string& path, std::optional<tls::Identity> peer,
                    std::string body = {}, std::map<std::string, std::string> query = {}) {
    http::Request r;
    r.method = method;
    r.path = path;
    r.query = std::move(query);
    r.body = std::move(body);
    r.peer = std::move(peer);
    r.remote = "127.0.0.1:5555";
    return api.handle(r);
}

struct Fixture {
    FakeRepository repo;
    std::int64_t now = 10'000'000;
    ControlApi api{repo, ApiOptions{}, [this] { return now; }};
};

constexpr const char* kStructure =
    R"({"lattice":[0,3,3,3,0,3,3,3,0],"sites":[{"species":"Zr","frac":[0,0,0]},{"species":"Ni","frac":[0.25,0.25,0.25]},)"
    R"({"species":"Sn","frac":[0.5,0.5,0.5]}],"reduced_formula":"ZrNiSn","fingerprint":"fp1:abc"})";

}  // namespace

BOOST_AUTO_TEST_SUITE(cpapi_materials)

BOOST_FIXTURE_TEST_CASE(routes_roles_and_params, Fixture) {
    cv1::Material m;
    m.set_material_id("mat-1");
    m.set_reduced_formula("ZrNiSn");
    m.add_elements("Ni");
    repo.materials["mat-1"] = m;
    cv1::StructureInfo s;
    s.set_structure_id("st-1");
    repo.structures["st-1"] = s;
    cv1::Calculation c;
    c.set_calc_id("calc-1");
    c.set_kind("qe.relax");
    repo.calculations.push_back(c);

    // Reads: node and admin, like jobs.
    for (const auto& who : {node("n1"), admin()}) {
        auto r = call(api, "GET", "/api/v1/materials", who, {}, {{"formula", "ZrNiSn"}, {"element", "Ni"},
                                                                 {"campaign_id", "camp-1"}, {"limit", "5000"}});
        BOOST_REQUIRE_EQUAL(r.status, 200);
        BOOST_TEST(parse<cv1::MaterialList>(r).materials_size() == 1);
        BOOST_TEST(repo.last_formula == "ZrNiSn");
        BOOST_TEST(repo.last_element == "Ni");
        BOOST_TEST(repo.last_material_campaign == "camp-1");
        BOOST_TEST(repo.last_material_limit == 1000u);
        BOOST_TEST(call(api, "GET", "/api/v1/materials/mat-1", who).status == 200);
        BOOST_TEST(call(api, "GET", "/api/v1/structures/st-1", who).status == 200);
        BOOST_TEST(call(api, "GET", "/api/v1/calculations/calc-1", who).status == 200);
    }
    BOOST_TEST(call(api, "GET", "/api/v1/materials/mat-x", admin()).status == 404);
    BOOST_TEST(call(api, "GET", "/api/v1/structures/st-x", admin()).status == 404);
    BOOST_TEST(call(api, "GET", "/api/v1/calculations/calc-x", admin()).status == 404);
    BOOST_TEST(call(api, "GET", "/api/v1/materials", std::nullopt).status == 401);
    BOOST_TEST(call(api, "GET", "/api/v1/materials", admin(), {}, {{"limit", "0"}}).status == 400);

    auto r = call(api, "GET", "/api/v1/calculations", admin(), {},
                  {{"material_id", "mat-1"}, {"structure_id", "st-1"}, {"kind", "qe.relax"}, {"campaign_id", "c"},
                   {"workflow_id", "w"}, {"evidence", "literature"}, {"after", "calc-9"}, {"limit", "7"}});
    BOOST_REQUIRE_EQUAL(r.status, 200);
    BOOST_REQUIRE(repo.last_calc_query);
    const auto& q = *repo.last_calc_query;
    BOOST_TEST(q.material_id == "mat-1");
    BOOST_TEST(q.structure_id == "st-1");
    BOOST_TEST(q.kind == "qe.relax");
    BOOST_TEST(q.campaign_id == "c");
    BOOST_TEST(q.workflow_id == "w");
    BOOST_TEST((q.evidence && *q.evidence == ::fireforge::tdr::v1::LITERATURE));
    BOOST_TEST(q.after == "calc-9");
    BOOST_TEST(q.limit == 7u);
    r = call(api, "GET", "/api/v1/calculations", node("n1"));
    BOOST_REQUIRE_EQUAL(r.status, 200);
    BOOST_TEST(!repo.last_calc_query->evidence);
    BOOST_TEST(repo.last_calc_query->limit == 100u);
    BOOST_TEST(parse<cv1::CalculationList>(r).calculations_size() == 1);
    BOOST_TEST(call(api, "GET", "/api/v1/calculations", admin(), {}, {{"evidence", "guessed"}}).status == 400);
    BOOST_TEST(call(api, "POST", "/api/v1/calculations", admin(), "{}").status == 405);

    // Import: admin only.
    const std::string body =
        R"({"campaign_id":"camp-1","records":[{"kind":"literature.kappa","evidence":"LITERATURE","structure":)" +
        std::string(kStructure) + "}]}";
    BOOST_TEST(call(api, "POST", "/api/v1/calculations:import", node("n1"), body).status == 403);
    BOOST_TEST(call(api, "POST", "/api/v1/calculations:import", admin(), "{bad").status == 400);
    BOOST_TEST(call(api, "POST", "/api/v1/calculations:import", admin(), R"({"records":[{"kind":"x"}]})").status == 400);
    r = call(api, "POST", "/api/v1/calculations:import", admin(), body);
    BOOST_REQUIRE_EQUAL(r.status, 200);
    BOOST_TEST(parse<cv1::ImportCalculationsReply>(r).calc_ids_size() == 1);
    BOOST_REQUIRE_EQUAL(repo.imports.size(), 1u);
    BOOST_TEST(repo.imports[0].records(0).structure().fingerprint() == "fp1:abc");
    BOOST_TEST(repo.audit_rows.back().actor == "admin:alice");
}

BOOST_AUTO_TEST_CASE(pg_records_through_job_status) {
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
    const auto n1 = node("n1");
    const auto adm = admin();

    auto r = call(api, "POST", "/api/v1/nodes/register", n1, R"({"name":"n1"})");
    BOOST_REQUIRE_EQUAL(r.status, 200);
    const auto node_id = parse<cv1::RegisterReply>(r).node_id();
    auto lease = [&] {
        BOOST_REQUIRE_EQUAL(call(api, "POST", "/api/v1/jobs", adm, R"({"spec":{"command":{"argv":["/bin/true"]}}})").status,
                            201);
        auto lr = call(api, "POST", "/api/v1/leases", n1,
                       R"({"node_id":")" + node_id + R"(","free":{"cpu_cores":4,"memory_bytes":1000000000},"max_jobs":1})");
        BOOST_REQUIRE_EQUAL(lr.status, 200);
        auto l = parse<cv1::LeaseReply>(lr);
        BOOST_REQUIRE_EQUAL(l.leases_size(), 1);
        return l.leases(0);
    };
    auto status = [&](const cv1::Lease& l, const std::string& records) {
        const std::string body = std::format(
            R"({{"attempt_id":"{}","fencing_token":{},"state":"COMPLETED","outputs":[{{"artifact_id":"artifact://sha256/{}","name":"scf.out","size":3}}],"records":{}}})",
            l.attempt_id(), l.fencing_token(), std::string(64, 'a'), records);
        return call(api, "POST", "/api/v1/jobs/" + l.job_id() + "/status", n1, body);
    };

    auto l1 = lease();
    r = status(l1, R"([{"kind":"qe.scf","structure":)" + std::string(kStructure) +
                       R"(,"properties_json":"{\"band_gap_ev\": 0.5}","output_names":["scf.out"]}])");
    BOOST_REQUIRE_MESSAGE(r.status == 200, r.body);
    BOOST_TEST(parse<cv1::Job>(r).state() == ::fireforge::tdr::v1::COMPLETED);

    // Invalid records -> 400, job FAILED, nothing stored.
    auto l2 = lease();
    r = status(l2, R"([{"kind":"qe.scf","structure":)" + std::string(kStructure) + R"(,"output_names":["nope"]}])");
    BOOST_TEST(r.status == 400);
    BOOST_TEST(r.body.find("invalid records") != std::string::npos, r.body);
    BOOST_TEST(parse<cv1::Job>(call(api, "GET", "/api/v1/jobs/" + l2.job_id(), adm)).state() ==
               ::fireforge::tdr::v1::FAILED);

    auto calcs = parse<cv1::CalculationList>(call(api, "GET", "/api/v1/calculations", adm, {}, {{"kind", "qe.scf"}}));
    BOOST_REQUIRE_EQUAL(calcs.calculations_size(), 1);
    const auto& c = calcs.calculations(0);
    BOOST_TEST(c.job_id() == l1.job_id());
    BOOST_TEST(c.node_id() == node_id);
    BOOST_REQUIRE_EQUAL(c.outputs_size(), 1);
    BOOST_TEST(c.outputs(0).artifact_id() == "artifact://sha256/" + std::string(64, 'a'));

    auto mats = parse<cv1::MaterialList>(call(api, "GET", "/api/v1/materials", n1, {}, {{"element", "Sn"}}));
    BOOST_REQUIRE_EQUAL(mats.materials_size(), 1);
    auto m = parse<cv1::Material>(call(api, "GET", "/api/v1/materials/" + mats.materials(0).material_id(), n1));
    BOOST_TEST(m.latest_size() == 1);
    BOOST_TEST(m.structures_size() == 1);
    BOOST_TEST(call(api, "GET", "/api/v1/structures/" + c.structure_id(), n1).status == 200);
    BOOST_TEST(call(api, "GET", "/api/v1/calculations/" + c.calc_id(), n1).status == 200);

    // Import: PREDICTED is refused, LITERATURE accepted and audited.
    const std::string imp = R"({"records":[{"kind":"lit.kappa","evidence":"%s","structure":)" + std::string(kStructure) + "}]}";
    auto with = [&](const std::string& ev) {
        std::string b = imp;
        b.replace(b.find("%s"), 2, ev);
        return b;
    };
    BOOST_TEST(call(api, "POST", "/api/v1/calculations:import", adm, with("PREDICTED")).status == 400);
    r = call(api, "POST", "/api/v1/calculations:import", adm, with("LITERATURE"));
    BOOST_REQUIRE_MESSAGE(r.status == 200, r.body);
    auto lit = parse<cv1::CalculationList>(
        call(api, "GET", "/api/v1/calculations", adm, {}, {{"evidence", "LITERATURE"}}));
    BOOST_TEST(lit.calculations_size() == 1);
    BOOST_TEST(parse<cv1::AuditList>(call(api, "GET", "/api/v1/audit", adm)).entries(0).action() ==
               "calculations.import");
}

BOOST_AUTO_TEST_SUITE_END()
