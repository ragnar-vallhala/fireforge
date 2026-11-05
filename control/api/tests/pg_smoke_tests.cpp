// A few handlers against the real PostgreSQL repository (throwaway cluster).
#include <memory>

#include <boost/test/unit_test.hpp>

#include "tdr/cpapi/api.hpp"
#include "tdr/cpdb/testing.hpp"

using namespace tdr;
using namespace tdr::cpapi;

namespace {

tls::Identity ident(tls::Role role, const std::string& name) {
    tls::Identity id;
    id.role = role;
    id.name = name;
    id.fingerprint_sha256 = "ab12";
    return id;
}

http::Response call(ControlApi& api, const std::string& method, const std::string& path, tls::Identity peer,
                    std::string body = {}) {
    http::Request r;
    r.method = method;
    r.path = path;
    r.body = std::move(body);
    r.peer = std::move(peer);
    return api.handle(r);
}

template <class M>
M parse(const http::Response& r) {
    M m;
    BOOST_REQUIRE_MESSAGE(from_json(r.body, m), r.body);
    return m;
}

}  // namespace

BOOST_AUTO_TEST_CASE(pg_smoke_handlers) {
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
    const auto n1 = ident(tls::Role::kNode, "n1");
    const auto adm = ident(tls::Role::kAdmin, "alice");

    auto r = call(api, "POST", "/api/v1/nodes/register", n1, R"({"name":"n1","daemon_version":"0.1"})");
    BOOST_REQUIRE_EQUAL(r.status, 200);
    auto reg = parse<cv1::RegisterReply>(r);
    BOOST_TEST(reg.policy().version() == 1u);

    r = call(api, "PATCH", "/api/v1/nodes/" + reg.node_id() + "/limits", adm, R"({"profile":"small","max_jobs":1})");
    BOOST_REQUIRE_EQUAL(r.status, 200);
    auto pol = parse<cv1::ResourcePolicy>(r);
    BOOST_TEST(pol.version() == 2u);
    BOOST_TEST(pol.max_cpu_cores() == 4u);
    BOOST_TEST(pol.max_jobs() == 1u);

    now += 1000;
    r = call(api, "POST", "/api/v1/nodes/heartbeat", n1, R"({"node_id":")" + reg.node_id() + R"(","policy_version":1})");
    BOOST_REQUIRE_EQUAL(r.status, 200);
    BOOST_TEST(parse<cv1::HeartbeatReply>(r).policy().version() == 2u);
    r = call(api, "POST", "/api/v1/nodes/heartbeat", n1, R"({"node_id":")" + reg.node_id() + R"(","policy_version":2})");
    BOOST_TEST(!parse<cv1::HeartbeatReply>(r).has_policy());
    BOOST_TEST(call(api, "POST", "/api/v1/nodes/heartbeat", n1, R"({"node_id":"ff-node-0000000000000000"})").status == 404);

    r = call(api, "POST", "/api/v1/campaigns", adm, R"({"name":"c1","parameters_json":"{\"a\":1}"})");
    BOOST_REQUIRE_EQUAL(r.status, 201);
    auto camp = parse<cv1::Campaign>(r);
    BOOST_TEST(call(api, "POST", "/api/v1/campaigns", adm, R"({"name":"c1"})").status == 409);
    r = call(api, "PATCH", "/api/v1/campaigns/" + camp.campaign_id(), adm, R"({"state":"ACTIVE"})");
    BOOST_REQUIRE_EQUAL(r.status, 200);
    BOOST_TEST(parse<cv1::Campaign>(r).state() == cv1::ACTIVE);
    BOOST_TEST(call(api, "PATCH", "/api/v1/campaigns/" + camp.campaign_id(), adm, R"({"state":"DRAFT"})").status == 409);

    r = call(api, "POST", "/api/v1/nodes/" + reg.node_id() + "/offline", n1);
    BOOST_REQUIRE_EQUAL(r.status, 200);
    BOOST_TEST(parse<cv1::NodeInfo>(r).health() == cv1::OFFLINE);

    auto audit = parse<cv1::AuditList>(call(api, "GET", "/api/v1/audit", adm));
    BOOST_REQUIRE_EQUAL(audit.entries_size(), 3);  // limits, campaign.create, campaign.update
    BOOST_TEST(audit.entries(0).action() == "campaign.update");
    BOOST_TEST(audit.entries(0).actor() == "admin:alice");

    now += 200'000;
    BOOST_TEST(api.sweep().empty());  // already offline
}
