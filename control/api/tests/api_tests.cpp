#include <set>
#include <tuple>

#include <boost/json.hpp>
#include <boost/test/unit_test.hpp>

#include "fake_repo.hpp"
#include "tdr/cpapi/api.hpp"

using namespace tdr;
using namespace tdr::cpapi;
using tdr::cpapi::testing::FakeRepository;

namespace {

constexpr std::uint64_t kGiB = std::uint64_t{1} << 30;

tls::Identity ident(tls::Role role, const std::string& name) {
    tls::Identity id;
    id.role = role;
    id.name = name;
    id.fingerprint_sha256 = "fp-" + name;
    return id;
}
tls::Identity node(const std::string& n) { return ident(tls::Role::kNode, n); }
tls::Identity admin(const std::string& n = "alice") { return ident(tls::Role::kAdmin, n); }
tls::Identity server(const std::string& n = "cp") { return ident(tls::Role::kServer, n); }

std::string err_code(const http::Response& r) {
    return std::string(boost::json::parse(r.body).as_object().at("code").as_string());
}
std::string err_msg(const http::Response& r) {
    return std::string(boost::json::parse(r.body).as_object().at("message").as_string());
}

template <class M>
M parse(const http::Response& r) {
    M m;
    std::string err;
    BOOST_REQUIRE_MESSAGE(from_json(r.body, m, &err), "bad reply JSON: " << err << " body=" << r.body);
    return m;
}

struct Fixture {
    FakeRepository repo;
    std::int64_t now = 1'000'000;
    ControlApi api{repo,
                   [] {
                       ApiOptions o;
                       o.heartbeat_interval_seconds = 10;
                       o.stale_after_seconds = 30;
                       o.offline_after_seconds = 90;
                       return o;
                   }(),
                   [this] { return now; }};

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
        req.set_daemon_version("0.1.0");
        auto* a = req.add_addresses();
        a->set_type("lan");
        a->set_address("10.0.0.1");
        a->set_port(7000);
        auto r = call("POST", "/api/v1/nodes/register", node(name), to_json(req));
        BOOST_REQUIRE_EQUAL(r.status, 200);
        return parse<cv1::RegisterReply>(r).node_id();
    }

    std::string create_campaign(const std::string& name) {
        cv1::CreateCampaignRequest req;
        req.set_name(name);
        auto r = call("POST", "/api/v1/campaigns", admin(), to_json(req));
        BOOST_REQUIRE_EQUAL(r.status, 201);
        return parse<cv1::Campaign>(r).campaign_id();
    }

    http::Response heartbeat(const std::string& name, const std::string& id, std::uint64_t policy_version) {
        cv1::Heartbeat hb;
        hb.set_node_id(id);
        hb.set_policy_version(policy_version);
        return call("POST", "/api/v1/nodes/heartbeat", node(name), to_json(hb));
    }
};

}  // namespace

// ---- helpers ------------------------------------------------------------------

BOOST_AUTO_TEST_CASE(profile_presets) {
    auto s = profile_preset("small");
    BOOST_REQUIRE(s);
    BOOST_TEST(s->profile() == "small");
    BOOST_TEST(s->max_cpu_cores() == 4u);
    BOOST_TEST(s->max_memory_bytes() == 8 * kGiB);
    BOOST_TEST(s->max_jobs() == 2u);
    BOOST_TEST(s->max_gpus() == 0u);
    auto m = profile_preset("medium");
    BOOST_TEST(m->max_cpu_cores() == 12u);
    BOOST_TEST(m->max_memory_bytes() == 24 * kGiB);
    BOOST_TEST(m->max_jobs() == 4u);
    auto c = profile_preset("compute");
    BOOST_TEST(c->max_cpu_cores() == 32u);
    BOOST_TEST(c->max_memory_bytes() == 96 * kGiB);
    BOOST_TEST(c->max_jobs() == 8u);
    auto g = profile_preset("gpu");
    BOOST_TEST(g->max_cpu_cores() == 16u);
    BOOST_TEST(g->max_memory_bytes() == 48 * kGiB);
    BOOST_TEST(g->max_jobs() == 4u);
    BOOST_TEST(g->max_gpus() == 1u);
    BOOST_TEST(!profile_preset("huge"));
    BOOST_TEST(!profile_preset(""));
    BOOST_TEST(!profile_preset("Small"));
}

BOOST_AUTO_TEST_CASE(json_roundtrip_and_errors) {
    cv1::RegisterRequest req;
    req.set_name("n1");
    req.add_capabilities("cpu");
    auto j = to_json(req);
    BOOST_TEST(j.find("\"name\":\"n1\"") != std::string::npos);
    cv1::RegisterRequest back;
    BOOST_REQUIRE(from_json(j, back));
    BOOST_TEST(back.name() == "n1");
    // Unknown fields ignored.
    BOOST_REQUIRE(from_json(R"({"name":"x","future_field":{"a":1}})", back));
    BOOST_TEST(back.name() == "x");
    std::string err;
    BOOST_TEST(!from_json("{not json", back, &err));
    BOOST_TEST(!err.empty());
    // Original field names in output.
    cv1::Heartbeat hb;
    hb.set_node_id("a");
    hb.set_policy_version(3);
    BOOST_TEST(to_json(hb).find("\"policy_version\"") != std::string::npos);
}

// ---- auth / routing -------------------------------------------------------------

BOOST_FIXTURE_TEST_CASE(no_peer_is_401_everywhere, Fixture) {
    for (auto [m, p] : std::vector<std::pair<std::string, std::string>>{{"GET", "/healthz"},
                                                                          {"POST", "/api/v1/nodes/register"},
                                                                          {"GET", "/api/v1/nodes"},
                                                                          {"GET", "/api/v1/audit"},
                                                                          {"GET", "/nope"}}) {
        auto r = call(m, p, std::nullopt);
        BOOST_TEST(r.status == 401);
        BOOST_TEST(err_code(r) == "unauthorized");
    }
}

BOOST_FIXTURE_TEST_CASE(healthz_any_authenticated, Fixture) {
    for (auto id : {node("n1"), admin(), server()}) {
        auto r = call("GET", "/healthz", id);
        BOOST_TEST(r.status == 200);
        BOOST_TEST(r.body == R"({"status":"ok"})");
    }
}

BOOST_FIXTURE_TEST_CASE(unknown_route_404_wrong_method_405, Fixture) {
    auto r = call("GET", "/api/v1/whatever", admin());
    BOOST_TEST(r.status == 404);
    r = call("DELETE", "/api/v1/nodes", admin());
    BOOST_TEST(r.status == 405);
    r = call("GET", "/api/v1/nodes/register", admin());  // matches /nodes/{id} -> 404 unknown node
    BOOST_TEST(r.status == 404);
    r = call("PUT", "/api/v1/campaigns/x", admin());
    BOOST_TEST(r.status == 405);
}

BOOST_FIXTURE_TEST_CASE(server_role_is_not_a_client, Fixture) {
    auto id = register_node("n1");
    auto cid = create_campaign("c1");
    const std::vector<std::pair<std::string, std::string>> routes = {
        {"POST", "/api/v1/nodes/register"},     {"POST", "/api/v1/nodes/heartbeat"},
        {"GET", "/api/v1/nodes"},               {"GET", "/api/v1/nodes/" + id},
        {"PATCH", "/api/v1/nodes/" + id + "/limits"}, {"POST", "/api/v1/nodes/" + id + "/offline"},
        {"GET", "/api/v1/nodes/" + id + "/events"},   {"POST", "/api/v1/campaigns"},
        {"GET", "/api/v1/campaigns"},           {"GET", "/api/v1/campaigns/" + cid},
        {"PATCH", "/api/v1/campaigns/" + cid},  {"GET", "/api/v1/audit"}};
    for (auto& [m, p] : routes) {
        auto r = call(m, p, server("n1"), "{}");
        BOOST_TEST_CONTEXT(m << " " << p) {
            BOOST_TEST(r.status == 403);
            BOOST_TEST(err_code(r) == "forbidden");
        }
    }
}

BOOST_FIXTURE_TEST_CASE(node_cannot_call_admin_endpoints, Fixture) {
    auto id = register_node("n1");
    auto cid = create_campaign("c1");
    const std::vector<std::pair<std::string, std::string>> routes = {
        {"PATCH", "/api/v1/nodes/" + id + "/limits"}, {"GET", "/api/v1/nodes/" + id + "/events"},
        {"POST", "/api/v1/campaigns"}, {"PATCH", "/api/v1/campaigns/" + cid}, {"GET", "/api/v1/audit"}};
    for (auto& [m, p] : routes) {
        auto r = call(m, p, node("n1"), R"({"name":"zz","profile":"small"})");
        BOOST_TEST_CONTEXT(m << " " << p) { BOOST_TEST(r.status == 403); }
    }
    // admin can't register / heartbeat
    BOOST_TEST(call("POST", "/api/v1/nodes/register", admin("n1"), R"({"name":"n1"})").status == 403);
    BOOST_TEST(call("POST", "/api/v1/nodes/heartbeat", admin("n1"), R"({"node_id":")" + id + "\"}").status == 403);
    // nothing changed
    BOOST_TEST(repo.nodes.at(id).policy().version() == 1u);
    BOOST_TEST(repo.campaigns.size() == 1u);
}

// ---- register ---------------------------------------------------------------

BOOST_FIXTURE_TEST_CASE(register_new_and_reconnect, Fixture) {
    cv1::RegisterRequest req;
    req.set_name("n1");
    req.set_daemon_version("0.1.0");
    req.add_capabilities("cpu");
    auto r = call("POST", "/api/v1/nodes/register", node("n1"), to_json(req));
    BOOST_REQUIRE_EQUAL(r.status, 200);
    auto rep = parse<cv1::RegisterReply>(r);
    BOOST_TEST(rep.node_id().starts_with("ff-node-"));
    BOOST_TEST(rep.heartbeat_interval_seconds() == 10u);
    BOOST_TEST(rep.server_time_unix_ms() == now);
    BOOST_TEST(rep.policy().version() == 1u);
    const auto& stored = repo.nodes.at(rep.node_id());
    BOOST_TEST(stored.cert_fingerprint() == "fp-n1");
    BOOST_TEST(stored.health() == cv1::ONLINE);

    // Reconnect keeps id.
    repo.set_offline(rep.node_id(), "x", now);
    now += 5000;
    r = call("POST", "/api/v1/nodes/register", node("n1"), to_json(req));
    BOOST_REQUIRE_EQUAL(r.status, 200);
    BOOST_TEST(parse<cv1::RegisterReply>(r).node_id() == rep.node_id());
    BOOST_TEST(repo.nodes.size() == 1u);
    BOOST_TEST(repo.nodes.at(rep.node_id()).health() == cv1::ONLINE);
}

BOOST_FIXTURE_TEST_CASE(register_rules, Fixture) {
    auto r = call("POST", "/api/v1/nodes/register", node("n1"), R"({"name":"n2"})");
    BOOST_TEST(r.status == 403);
    BOOST_TEST(err_code(r) == "forbidden");
    r = call("POST", "/api/v1/nodes/register", node("n1"), "");
    BOOST_TEST(r.status == 400);
    BOOST_TEST(err_code(r) == "bad_request");
    r = call("POST", "/api/v1/nodes/register", node("n1"), R"({"daemon_version":"1"})");
    BOOST_TEST(r.status == 400);
    r = call("POST", "/api/v1/nodes/register", node("n1"), R"({"name": )");
    BOOST_TEST(r.status == 400);
    BOOST_TEST(err_msg(r).starts_with("invalid JSON body: "));
    BOOST_TEST(err_msg(r).size() > std::string("invalid JSON body: ").size());
    r = call("POST", "/api/v1/nodes/register", node("n1"), R"({"name":5})");
    BOOST_TEST(r.status == 400);
    r = call("POST", "/api/v1/nodes/register", node("n1"), R"({"name":"n1","unknown":[1,2]})");
    BOOST_TEST(r.status == 200);
    BOOST_TEST(repo.nodes.size() == 1u);
}

// ---- heartbeat ----------------------------------------------------------------

BOOST_FIXTURE_TEST_CASE(heartbeat_rules, Fixture) {
    auto id1 = register_node("n1");
    auto id2 = register_node("n2");
    auto r = heartbeat("n1", id2, 1);
    BOOST_TEST(r.status == 403);
    r = heartbeat("n1", "ff-node-doesnotexist", 1);
    BOOST_TEST(r.status == 404);
    BOOST_TEST(err_code(r) == "not_found");
    r = call("POST", "/api/v1/nodes/heartbeat", node("n1"), "{}");
    BOOST_TEST(r.status == 400);
    r = call("POST", "/api/v1/nodes/heartbeat", node("n1"), "[");
    BOOST_TEST(r.status == 400);
    now += 1234;
    r = heartbeat("n1", id1, 1);
    BOOST_TEST(r.status == 200);
    BOOST_TEST(repo.nodes.at(id1).last_seen_unix_ms() == now);
    BOOST_TEST(repo.nodes.at(id2).last_seen_unix_ms() == now - 1234);
}

BOOST_FIXTURE_TEST_CASE(heartbeat_policy_only_on_version_mismatch, Fixture) {
    auto id = register_node("n1");
    auto r = heartbeat("n1", id, 1);
    BOOST_REQUIRE_EQUAL(r.status, 200);
    auto rep = parse<cv1::HeartbeatReply>(r);
    BOOST_TEST(!rep.has_policy());
    BOOST_TEST(rep.heartbeat_interval_seconds() == 10u);
    BOOST_TEST(rep.server_time_unix_ms() == now);
    BOOST_TEST(r.body.find("policy") == std::string::npos);

    BOOST_REQUIRE_EQUAL(call("PATCH", "/api/v1/nodes/" + id + "/limits", admin(), R"({"profile":"small"})").status, 200);
    rep = parse<cv1::HeartbeatReply>(heartbeat("n1", id, 1));
    BOOST_REQUIRE(rep.has_policy());
    BOOST_TEST(rep.policy().version() == 2u);
    BOOST_TEST(rep.policy().profile() == "small");
    rep = parse<cv1::HeartbeatReply>(heartbeat("n1", id, 2));
    BOOST_TEST(!rep.has_policy());
    // Node ahead/behind in any direction still gets it.
    rep = parse<cv1::HeartbeatReply>(heartbeat("n1", id, 7));
    BOOST_TEST(rep.has_policy());
}

BOOST_FIXTURE_TEST_CASE(heartbeat_brings_stale_node_online, Fixture) {
    auto id = register_node("n1");
    now += 40'000;
    api.sweep();
    BOOST_TEST(repo.nodes.at(id).health() == cv1::STALE);
    BOOST_TEST(heartbeat("n1", id, 1).status == 200);
    BOOST_TEST(repo.nodes.at(id).health() == cv1::ONLINE);
}

// ---- node queries ---------------------------------------------------------------

BOOST_FIXTURE_TEST_CASE(list_and_get_nodes, Fixture) {
    auto id2 = register_node("n2");
    auto id1 = register_node("n1");
    repo.set_offline(id2, "bye", now);
    auto r = call("GET", "/api/v1/nodes", node("n1"));
    BOOST_REQUIRE_EQUAL(r.status, 200);
    auto list = parse<cv1::NodeList>(r);
    BOOST_REQUIRE_EQUAL(list.nodes_size(), 2);
    BOOST_TEST(list.nodes(0).name() == "n1");
    BOOST_TEST(r.body.find("\"health\":\"ONLINE\"") != std::string::npos);

    for (auto h : {"online", "ONLINE", "Online"}) {
        list = parse<cv1::NodeList>(call("GET", "/api/v1/nodes", admin(), "", {{"health", h}}));
        BOOST_REQUIRE_EQUAL(list.nodes_size(), 1);
        BOOST_TEST(list.nodes(0).node_id() == id1);
    }
    list = parse<cv1::NodeList>(call("GET", "/api/v1/nodes", admin(), "", {{"health", "offline"}}));
    BOOST_REQUIRE_EQUAL(list.nodes_size(), 1);
    BOOST_TEST(list.nodes(0).offline_reason() == "bye");
    list = parse<cv1::NodeList>(call("GET", "/api/v1/nodes", admin(), "", {{"health", "stale"}}));
    BOOST_TEST(list.nodes_size() == 0);
    BOOST_TEST(call("GET", "/api/v1/nodes", admin(), "", {{"health", "sleepy"}}).status == 400);
    BOOST_TEST(call("GET", "/api/v1/nodes", admin(), "", {{"health", "NODE_HEALTH_UNSPECIFIED"}}).status == 400);

    r = call("GET", "/api/v1/nodes/" + id2, node("n1"));
    BOOST_REQUIRE_EQUAL(r.status, 200);
    BOOST_TEST(parse<cv1::NodeInfo>(r).name() == "n2");
    r = call("GET", "/api/v1/nodes/ff-node-x", admin());
    BOOST_TEST(r.status == 404);
    BOOST_TEST(err_code(r) == "not_found");
}

BOOST_FIXTURE_TEST_CASE(node_events, Fixture) {
    auto id = register_node("n1");
    for (int i = 0; i < 3; ++i) {
        repo.set_offline(id, "off", now);
        register_node("n1");
    }
    // 1 (register) + 3*2 events
    auto r = call("GET", "/api/v1/nodes/" + id + "/events", admin());
    BOOST_REQUIRE_EQUAL(r.status, 200);
    auto evs = parse<cv1::NodeEventList>(r);
    BOOST_TEST(evs.events_size() == 7);
    BOOST_TEST(evs.events(0).to() == cv1::ONLINE);  // newest first
    BOOST_TEST(evs.events(1).to() == cv1::OFFLINE);
    evs = parse<cv1::NodeEventList>(call("GET", "/api/v1/nodes/" + id + "/events", admin(), "", {{"limit", "2"}}));
    BOOST_TEST(evs.events_size() == 2);
    evs = parse<cv1::NodeEventList>(call("GET", "/api/v1/nodes/" + id + "/events", admin(), "", {{"limit", "99999"}}));
    BOOST_TEST(evs.events_size() == 7);
    for (auto bad : {"0", "-1", "abc", "5x"})
        BOOST_TEST(call("GET", "/api/v1/nodes/" + id + "/events", admin(), "", {{"limit", bad}}).status == 400);
    BOOST_TEST(call("GET", "/api/v1/nodes/nope/events", admin()).status == 404);
}

// ---- limits -------------------------------------------------------------------

BOOST_FIXTURE_TEST_CASE(limits_profiles_overrides_and_clear, Fixture) {
    auto id = register_node("n1");
    const auto path = "/api/v1/nodes/" + id + "/limits";

    auto r = call("PATCH", path, admin(), R"({"profile":"compute"})");
    BOOST_REQUIRE_EQUAL(r.status, 200);
    auto p = parse<cv1::ResourcePolicy>(r);
    BOOST_TEST(p.version() == 2u);
    BOOST_TEST(p.profile() == "compute");
    BOOST_TEST(p.max_cpu_cores() == 32u);
    BOOST_TEST(p.max_memory_bytes() == 96 * kGiB);
    BOOST_TEST(p.max_jobs() == 8u);
    BOOST_TEST(p.updated_by() == "admin:alice");
    BOOST_TEST(p.updated_unix_ms() == now);

    // Explicit field on top of current policy keeps the rest.
    p = parse<cv1::ResourcePolicy>(call("PATCH", path, admin(), R"({"max_jobs":3})"));
    BOOST_TEST(p.version() == 3u);
    BOOST_TEST(p.profile() == "compute");
    BOOST_TEST(p.max_cpu_cores() == 32u);
    BOOST_TEST(p.max_jobs() == 3u);

    // Profile first, then explicit fields (including explicit 0).
    p = parse<cv1::ResourcePolicy>(
        call("PATCH", path, admin(), R"({"profile":"gpu","max_cpu_cores":20,"max_gpus":0,"max_disk_bytes":"1000"})"));
    BOOST_TEST(p.profile() == "gpu");
    BOOST_TEST(p.max_cpu_cores() == 20u);
    BOOST_TEST(p.max_memory_bytes() == 48 * kGiB);
    BOOST_TEST(p.max_jobs() == 4u);
    BOOST_TEST(p.max_gpus() == 0u);
    BOOST_TEST(p.max_disk_bytes() == 1000u);

    // Clearing.
    for (auto clear : {R"({"profile":""})", R"({"profile":"none"})"}) {
        call("PATCH", path, admin(), R"({"profile":"small"})");
        p = parse<cv1::ResourcePolicy>(call("PATCH", path, admin(), clear));
        BOOST_TEST(p.profile() == "");
        BOOST_TEST(p.max_cpu_cores() == 0u);
        BOOST_TEST(p.max_memory_bytes() == 0u);
        BOOST_TEST(p.max_jobs() == 0u);
        BOOST_TEST(p.max_disk_bytes() == 0u);
        BOOST_TEST(p.max_gpus() == 0u);
    }
    p = parse<cv1::ResourcePolicy>(call("PATCH", path, admin(), R"({"profile":"none","max_jobs":1})"));
    BOOST_TEST(p.max_jobs() == 1u);
    BOOST_TEST(p.max_cpu_cores() == 0u);

    // Empty body = no-op change but still a new version.
    const auto v = repo.nodes.at(id).policy().version();
    p = parse<cv1::ResourcePolicy>(call("PATCH", path, admin(), ""));
    BOOST_TEST(p.version() == v + 1);
    BOOST_TEST(p.max_jobs() == 1u);
}

BOOST_FIXTURE_TEST_CASE(limits_errors_and_audit, Fixture) {
    auto id = register_node("n1");
    const auto path = "/api/v1/nodes/" + id + "/limits";
    auto r = call("PATCH", path, admin(), R"({"profile":"huge"})");
    BOOST_TEST(r.status == 400);
    BOOST_TEST(err_msg(r).find("huge") != std::string::npos);
    BOOST_TEST(call("PATCH", path, admin(), R"({"max_jobs":-1})").status == 400);
    BOOST_TEST(call("PATCH", path, admin(), "{{").status == 400);
    BOOST_TEST(call("PATCH", "/api/v1/nodes/ff-node-nope/limits", admin(), R"({"profile":"small"})").status == 404);
    BOOST_TEST(repo.nodes.at(id).policy().version() == 1u);
    BOOST_TEST(repo.audit_rows.empty());

    BOOST_REQUIRE_EQUAL(call("PATCH", path, admin("bob"), R"({"profile":"medium","max_jobs":2})").status, 200);
    BOOST_REQUIRE_EQUAL(repo.audit_rows.size(), 1u);
    const auto& a = repo.audit_rows[0];
    BOOST_TEST(a.actor == "admin:bob");
    BOOST_TEST(a.action == "node.limits");
    BOOST_TEST(a.target == id);
    BOOST_TEST(a.at == now);
    auto details = boost::json::parse(a.details_json).as_object();
    BOOST_TEST(details.at("profile").as_string() == "medium");
    BOOST_TEST(details.at("max_jobs").as_int64() == 2);
}

// ---- offline ------------------------------------------------------------------

BOOST_FIXTURE_TEST_CASE(offline_rules, Fixture) {
    auto id1 = register_node("n1");
    auto id2 = register_node("n2");

    // node: other -> 403
    auto r = call("POST", "/api/v1/nodes/" + id2 + "/offline", node("n1"), "{}");
    BOOST_TEST(r.status == 403);
    BOOST_TEST(repo.nodes.at(id2).health() == cv1::ONLINE);

    // node: self, empty body, default reason, no audit
    r = call("POST", "/api/v1/nodes/" + id1 + "/offline", node("n1"), "");
    BOOST_REQUIRE_EQUAL(r.status, 200);
    auto info = parse<cv1::NodeInfo>(r);
    BOOST_TEST(info.health() == cv1::OFFLINE);
    BOOST_TEST(info.offline_reason() == "requested by node:n1");
    BOOST_TEST(repo.audit_rows.empty());

    // admin: any, custom reason, audited
    r = call("POST", "/api/v1/nodes/" + id2 + "/offline", admin(), R"({"reason":"maintenance"})");
    BOOST_REQUIRE_EQUAL(r.status, 200);
    info = parse<cv1::NodeInfo>(r);
    BOOST_TEST(info.offline_reason() == "maintenance");
    BOOST_REQUIRE_EQUAL(repo.audit_rows.size(), 1u);
    BOOST_TEST(repo.audit_rows[0].actor == "admin:alice");
    BOOST_TEST(repo.audit_rows[0].action == "node.offline");
    BOOST_TEST(repo.audit_rows[0].target == id2);

    r = call("POST", "/api/v1/nodes/" + id1 + "/offline", admin(), "");
    BOOST_TEST(parse<cv1::NodeInfo>(r).offline_reason() == "requested by admin:alice");

    BOOST_TEST(call("POST", "/api/v1/nodes/nope/offline", admin(), "").status == 404);
    BOOST_TEST(call("POST", "/api/v1/nodes/nope/offline", node("n1"), "").status == 404);
    BOOST_TEST(call("POST", "/api/v1/nodes/" + id1 + "/offline", node("n1"), "[").status == 400);
}

// ---- campaigns ----------------------------------------------------------------

BOOST_FIXTURE_TEST_CASE(campaign_create_and_validation, Fixture) {
    cv1::CreateCampaignRequest req;
    req.set_name("Thermo run_1.v2 - a");
    req.set_domain("thermoelectric");
    req.set_description("d");
    req.set_parameters_json(R"({"t":300})");
    auto r = call("POST", "/api/v1/campaigns", admin(), to_json(req));
    BOOST_REQUIRE_EQUAL(r.status, 201);
    auto c = parse<cv1::Campaign>(r);
    BOOST_TEST(c.campaign_id().starts_with("camp-"));
    BOOST_TEST(c.state() == cv1::DRAFT);
    BOOST_TEST(c.created_by() == "alice");
    BOOST_TEST(c.domain() == "thermoelectric");
    BOOST_REQUIRE_EQUAL(repo.audit_rows.size(), 1u);
    BOOST_TEST(repo.audit_rows[0].actor == "admin:alice");
    BOOST_TEST(repo.audit_rows[0].action == "campaign.create");
    BOOST_TEST(repo.audit_rows[0].target == c.campaign_id());

    // duplicate
    r = call("POST", "/api/v1/campaigns", admin(), to_json(req));
    BOOST_TEST(r.status == 409);
    BOOST_TEST(err_code(r) == "conflict");
    BOOST_TEST(repo.audit_rows.size() == 1u);

    auto bad = [&](const std::string& body) {
        auto resp = call("POST", "/api/v1/campaigns", admin(), body);
        BOOST_TEST_CONTEXT(body) {
            BOOST_TEST(resp.status == 400);
            BOOST_TEST(err_code(resp) == "bad_request");
        }
    };
    bad("");
    bad(R"({"name":""})");
    bad(R"({"name":"a/b"})");
    bad(R"({"name":"semi;colon"})");
    bad(R"({"name":")" + std::string(129, 'a') + "\"}");
    bad(R"({"name":"ok","parameters_json":"{bad"})");
    bad(R"({"name":"ok","parameters_json":"[1,2]"})");
    bad(R"({"name":"ok","parameters_json":"42"})");
    bad("not json");
    BOOST_TEST(call("POST", "/api/v1/campaigns", admin(), R"({"name":")" + std::string(128, 'a') + "\"}").status == 201);
    BOOST_TEST(call("POST", "/api/v1/campaigns", admin(), R"({"name":"noparams","parameters_json":""})").status == 201);
}

BOOST_FIXTURE_TEST_CASE(campaign_list_and_get, Fixture) {
    auto a = create_campaign("a");
    now += 1;
    auto b = create_campaign("b");
    repo.campaigns.at(a).set_state(cv1::ACTIVE);
    auto list = parse<cv1::CampaignList>(call("GET", "/api/v1/campaigns", node("n1")));
    BOOST_REQUIRE_EQUAL(list.campaigns_size(), 2);
    BOOST_TEST(list.campaigns(0).campaign_id() == b);
    list = parse<cv1::CampaignList>(call("GET", "/api/v1/campaigns", admin(), "", {{"state", "active"}}));
    BOOST_REQUIRE_EQUAL(list.campaigns_size(), 1);
    BOOST_TEST(list.campaigns(0).campaign_id() == a);
    BOOST_TEST(call("GET", "/api/v1/campaigns", admin(), "", {{"state", "bogus"}}).status == 400);
    auto r = call("GET", "/api/v1/campaigns/" + a, node("n1"));
    BOOST_REQUIRE_EQUAL(r.status, 200);
    BOOST_TEST(parse<cv1::Campaign>(r).name() == "a");
    BOOST_TEST(call("GET", "/api/v1/campaigns/camp-nope", admin()).status == 404);
}

BOOST_AUTO_TEST_CASE(campaign_transition_table) {
    using enum cv1::CampaignState;
    const std::set<std::pair<int, int>> allowed = {{DRAFT, ACTIVE},     {DRAFT, ARCHIVED},  {ACTIVE, PAUSED},
                                                    {PAUSED, ACTIVE},    {ACTIVE, COMPLETED}, {PAUSED, COMPLETED},
                                                    {COMPLETED, ARCHIVED}};
    for (int f = 0; f <= 5; ++f)
        for (int t = 0; t <= 5; ++t) {
            auto from = static_cast<cv1::CampaignState>(f), to = static_cast<cv1::CampaignState>(t);
            BOOST_TEST_CONTEXT(f << "->" << t) {
                BOOST_TEST(is_valid_campaign_transition(from, to) == allowed.contains({f, t}));
            }
        }
}

BOOST_FIXTURE_TEST_CASE(campaign_patch_transitions_all_pairs, Fixture) {
    using enum cv1::CampaignState;
    auto id = create_campaign("c");
    const auto path = "/api/v1/campaigns/" + id;
    for (int f = DRAFT; f <= ARCHIVED; ++f)
        for (int t = DRAFT; t <= ARCHIVED; ++t) {
            auto from = static_cast<cv1::CampaignState>(f), to = static_cast<cv1::CampaignState>(t);
            repo.campaigns.at(id).set_state(from);
            cv1::UpdateCampaignRequest req;
            req.set_state(to);
            auto r = call("PATCH", path, admin(), to_json(req));
            BOOST_TEST_CONTEXT(cv1::CampaignState_Name(from) << "->" << cv1::CampaignState_Name(to)) {
                if (is_valid_campaign_transition(from, to)) {
                    BOOST_TEST(r.status == 200);
                    BOOST_TEST(parse<cv1::Campaign>(r).state() == to);
                    BOOST_TEST(repo.campaigns.at(id).state() == to);
                } else {
                    BOOST_TEST(r.status == 409);
                    BOOST_TEST(err_code(r) == "conflict");
                    BOOST_TEST(err_msg(r).find(cv1::CampaignState_Name(from)) != std::string::npos);
                    BOOST_TEST(err_msg(r).find(cv1::CampaignState_Name(to)) != std::string::npos);
                    BOOST_TEST(repo.campaigns.at(id).state() == from);
                }
            }
        }
    // State names in JSON also accepted.
    repo.campaigns.at(id).set_state(DRAFT);
    BOOST_TEST(call("PATCH", path, admin(), R"({"state":"ACTIVE"})").status == 200);
}

BOOST_FIXTURE_TEST_CASE(campaign_patch_fields_and_audit, Fixture) {
    auto id = create_campaign("c");
    const auto path = "/api/v1/campaigns/" + id;
    repo.audit_rows.clear();
    now += 100;
    auto r = call("PATCH", path, admin("bob"), R"({"description":"new","parameters_json":"{\"k\":1}"})");
    BOOST_REQUIRE_EQUAL(r.status, 200);
    auto c = parse<cv1::Campaign>(r);
    BOOST_TEST(c.description() == "new");
    BOOST_TEST(c.parameters_json() == R"({"k":1})");
    BOOST_TEST(c.state() == cv1::DRAFT);
    BOOST_TEST(c.updated_unix_ms() == now);
    BOOST_REQUIRE_EQUAL(repo.audit_rows.size(), 1u);
    BOOST_TEST(repo.audit_rows[0].actor == "admin:bob");
    BOOST_TEST(repo.audit_rows[0].action == "campaign.update");
    BOOST_TEST(repo.audit_rows[0].target == id);

    // description unset keeps value
    c = parse<cv1::Campaign>(call("PATCH", path, admin(), R"({"state":"ACTIVE"})"));
    BOOST_TEST(c.description() == "new");
    // explicit empty description clears
    c = parse<cv1::Campaign>(call("PATCH", path, admin(), R"({"description":""})"));
    BOOST_TEST(c.description() == "");

    BOOST_TEST(call("PATCH", path, admin(), R"({"parameters_json":"nope"})").status == 400);
    BOOST_TEST(call("PATCH", path, admin(), "[").status == 400);
    BOOST_TEST(call("PATCH", "/api/v1/campaigns/camp-x", admin(), "{}").status == 404);
    BOOST_TEST(repo.audit_rows.size() == 3u);
}

BOOST_FIXTURE_TEST_CASE(campaign_patch_cas_conflict, Fixture) {
    auto id = create_campaign("c");
    repo.audit_rows.clear();
    // Another admin moves it to ARCHIVED between our read and our write.
    repo.before_update_campaign = [&] { repo.campaigns.at(id).set_state(cv1::ARCHIVED); };
    auto r = call("PATCH", "/api/v1/campaigns/" + id, admin(), R"({"state":"ACTIVE"})");
    BOOST_TEST(r.status == 409);
    BOOST_TEST(err_code(r) == "conflict");
    BOOST_TEST(err_msg(r).find("concurrent") != std::string::npos);
    BOOST_TEST(repo.campaigns.at(id).state() == cv1::ARCHIVED);
    BOOST_TEST(repo.audit_rows.empty());
    // Deleted underneath -> 404.
    repo.before_update_campaign = [&] { repo.campaigns.erase(id); };
    BOOST_TEST(call("PATCH", "/api/v1/campaigns/" + id, admin(), R"({"description":"x"})").status == 404);
}

// ---- audit --------------------------------------------------------------------

BOOST_FIXTURE_TEST_CASE(audit_list, Fixture) {
    for (int i = 0; i < 5; ++i) {
        now += 1;
        create_campaign("c" + std::to_string(i));
    }
    auto r = call("GET", "/api/v1/audit", admin());
    BOOST_REQUIRE_EQUAL(r.status, 200);
    auto list = parse<cv1::AuditList>(r);
    BOOST_REQUIRE_EQUAL(list.entries_size(), 5);
    BOOST_TEST(list.entries(0).at_unix_ms() == now);  // newest first
    BOOST_TEST(list.entries(0).actor() == "admin:alice");
    list = parse<cv1::AuditList>(call("GET", "/api/v1/audit", admin(), "", {{"limit", "2"}}));
    BOOST_TEST(list.entries_size() == 2);
    BOOST_TEST(call("GET", "/api/v1/audit", admin(), "", {{"limit", "x"}}).status == 400);
    BOOST_TEST(call("GET", "/api/v1/audit", admin(), "", {{"limit", "5000"}}).status == 200);
}

// ---- failures / sweep -----------------------------------------------------------

BOOST_FIXTURE_TEST_CASE(db_error_is_500_without_details, Fixture) {
    auto id = register_node("n1");
    repo.fail_all = true;
    for (auto [m, p, peer, body] : std::vector<std::tuple<std::string, std::string, tls::Identity, std::string>>{
             {"POST", "/api/v1/nodes/register", node("n1"), R"({"name":"n1"})"},
             {"POST", "/api/v1/nodes/heartbeat", node("n1"), R"({"node_id":")" + id + "\"}"},
             {"GET", "/api/v1/nodes", admin(), ""},
             {"GET", "/api/v1/campaigns", admin(), ""},
             {"GET", "/api/v1/audit", admin(), ""}}) {
        auto r = call(m, p, peer, body);
        BOOST_TEST_CONTEXT(m << " " << p) {
            BOOST_TEST(r.status == 500);
            BOOST_TEST(err_code(r) == "internal");
            BOOST_TEST(err_msg(r) == "database error");
            BOOST_TEST(r.body.find("secret") == std::string::npos);
            BOOST_TEST(r.body.find("42P01") == std::string::npos);
        }
    }
    // Auth checks run before the DB is touched.
    BOOST_TEST(call("GET", "/api/v1/audit", node("n1")).status == 403);
    BOOST_TEST(call("GET", "/healthz", node("n1")).status == 200);
}

BOOST_FIXTURE_TEST_CASE(sweep_uses_clock_and_options, Fixture) {
    BOOST_TEST(api.options().stale_after_seconds == 30u);
    auto a = register_node("a");
    now += 20'000;
    auto b = register_node("b");
    BOOST_TEST(api.sweep().empty());

    now += 15'000;  // a: 35 s, b: 15 s
    auto ch = api.sweep();
    BOOST_REQUIRE_EQUAL(ch.size(), 1u);
    BOOST_TEST(ch[0].node_id == a);
    BOOST_TEST(ch[0].from == cv1::ONLINE);
    BOOST_TEST(ch[0].to == cv1::STALE);
    BOOST_TEST(api.sweep().empty());

    now += 60'000;  // a: 95 s, b: 75 s
    ch = api.sweep();
    BOOST_REQUIRE_EQUAL(ch.size(), 2u);
    std::map<std::string, cpdb::HealthChange> by;
    for (auto& c : ch) by[c.node_id] = c;
    BOOST_TEST(by.at(a).from == cv1::STALE);
    BOOST_TEST(by.at(a).to == cv1::OFFLINE);
    BOOST_TEST(by.at(b).from == cv1::ONLINE);
    BOOST_TEST(by.at(b).to == cv1::STALE);
    BOOST_TEST(repo.nodes.at(a).offline_reason() == "heartbeat timeout");

    now += 30'000;
    ch = api.sweep();
    BOOST_REQUIRE_EQUAL(ch.size(), 1u);
    BOOST_TEST(ch[0].node_id == b);
    BOOST_TEST(ch[0].to == cv1::OFFLINE);
    BOOST_TEST(api.sweep().empty());
}
