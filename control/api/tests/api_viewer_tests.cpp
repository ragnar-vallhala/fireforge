// Phase 9 (phase9.md §3): viewer role on every route, GET /api/v1/whoami, browser CSRF guard, 415.
#include <map>
#include <set>
#include <string>

#include <boost/json.hpp>
#include <boost/test/unit_test.hpp>

#include "fake_repo.hpp"
#include "tdr/cpapi/api.hpp"

using namespace tdr;
using namespace tdr::cpapi;
using tdr::cpapi::testing::FakeRepository;

namespace {

tls::Identity ident(tls::Role role, const std::string& name) {
    tls::Identity id;
    id.role = role;
    id.name = name;
    id.fingerprint_sha256 = "ab12" + name;
    return id;
}
tls::Identity viewer() { return ident(tls::Role::kViewer, "vera"); }
tls::Identity admin() { return ident(tls::Role::kAdmin, "alice"); }

std::string err_code(const http::Response& r) {
    auto v = boost::json::parse(r.body);
    if (!v.is_object() || !v.as_object().contains("code")) return {};
    return std::string(v.as_object().at("code").as_string());
}

// Viewer access per route. Every registered route must be listed here, so a new route
// forces a decision about the viewer role.
const std::map<std::string, bool>& viewer_table() {
    static const std::map<std::string, bool> t = {
        {"GET /healthz", true},
        {"GET /api/v1/whoami", true},
        {"POST /api/v1/nodes/register", false},
        {"POST /api/v1/nodes/heartbeat", false},
        {"GET /api/v1/nodes", true},
        {"GET /api/v1/nodes/{id}", true},
        {"PATCH /api/v1/nodes/{id}/limits", false},
        {"POST /api/v1/nodes/{id}/offline", false},
        {"GET /api/v1/nodes/{id}/events", true},
        {"POST /api/v1/campaigns", false},
        {"GET /api/v1/campaigns", true},
        {"GET /api/v1/campaigns/{id}", true},
        {"PATCH /api/v1/campaigns/{id}", false},
        {"POST /api/v1/jobs", false},
        {"GET /api/v1/jobs", true},
        {"GET /api/v1/jobs/{id}", true},
        {"POST /api/v1/jobs/{id}/cancel", false},
        {"POST /api/v1/jobs/{id}/status", false},
        {"POST /api/v1/leases", false},
        {"POST /api/v1/artifacts/announce", false},
        {"GET /api/v1/artifacts/sha256/{hex}", false},
        {"POST /api/v1/artifacts/sha256/{hex}/release", false},
        {"POST /api/v1/artifacts/sha256/{hex}/pin", false},
        {"POST /api/v1/artifacts/sha256/{hex}/unpin", false},
        {"POST /api/v1/workflows", false},
        {"GET /api/v1/workflows", true},
        {"GET /api/v1/workflows/{id}", true},
        {"POST /api/v1/workflows/{id}/cancel", false},
        {"GET /api/v1/materials", true},
        {"GET /api/v1/materials/{id}", true},
        {"GET /api/v1/structures/{id}", true},
        {"GET /api/v1/calculations", true},
        {"GET /api/v1/calculations/{id}", true},
        {"POST /api/v1/calculations:import", false},
        {"GET /api/v1/audit", true},
    };
    return t;
}

std::string concrete(std::string pattern) {
    auto sub = [&](const std::string& from, const std::string& to) {
        for (auto p = pattern.find(from); p != std::string::npos; p = pattern.find(from)) pattern.replace(p, from.size(), to);
    };
    sub("{id}", "x1");
    sub("{hex}", std::string(64, 'a'));
    return pattern;
}

struct Fixture {
    FakeRepository repo;
    ControlApi api{repo, ApiOptions{}, [] { return std::int64_t{1'000'000}; }};

    http::Response call(const std::string& method, const std::string& path, tls::Identity peer, std::string body = {},
                        std::map<std::string, std::string> headers = {}, std::string content_type = "") {
        http::Request r;
        r.method = method;
        r.path = path;
        r.body = std::move(body);
        if (content_type.empty() && !r.body.empty()) content_type = "application/json";
        r.content_type = content_type;
        r.headers = std::move(headers);
        r.peer = std::move(peer);
        r.remote = "127.0.0.1:5555";
        return api.handle(r);
    }

    http::Response create_campaign(std::map<std::string, std::string> headers, const std::string& name,
                                   std::string content_type = "") {
        return call("POST", "/api/v1/campaigns", admin(), R"({"name":")" + name + R"("})", std::move(headers),
                    std::move(content_type));
    }
};

}  // namespace

BOOST_FIXTURE_TEST_CASE(route_table_covers_every_route, Fixture) {
    std::set<std::string> seen;
    for (const auto& [m, p] : api.routes()) {
        const auto key = m + " " + p;
        BOOST_TEST_INFO(key);
        BOOST_TEST(viewer_table().contains(key));
        seen.insert(key);
    }
    BOOST_TEST(seen.size() == viewer_table().size());
}

BOOST_FIXTURE_TEST_CASE(viewer_reads_allowed_everything_else_forbidden, Fixture) {
    for (const auto& [m, p] : api.routes()) {
        const auto key = m + " " + p;
        auto it = viewer_table().find(key);
        if (it == viewer_table().end()) continue;  // reported by route_table_covers_every_route
        auto r = call(m, concrete(p), viewer());
        BOOST_TEST_INFO(key << " -> " << r.status << " " << r.body);
        if (it->second) {
            BOOST_TEST(r.status != 403);
            BOOST_TEST(r.status != 401);
            BOOST_TEST(r.status < 500);
        } else {
            BOOST_TEST(r.status == 403);
            BOOST_TEST(err_code(r) == "forbidden");
        }
    }
}

BOOST_FIXTURE_TEST_CASE(admin_may_read_what_viewer_reads, Fixture) {
    for (const auto& [key, allowed] : viewer_table()) {
        if (!allowed) continue;
        const auto sp = key.find(' ');
        auto r = call(key.substr(0, sp), concrete(key.substr(sp + 1)), admin());
        BOOST_TEST_INFO(key << " -> " << r.status);
        BOOST_TEST(r.status != 403);
    }
}

BOOST_FIXTURE_TEST_CASE(server_role_still_refused_on_reads, Fixture) {
    for (const char* p : {"/api/v1/nodes", "/api/v1/jobs", "/api/v1/audit", "/api/v1/materials"})
        BOOST_TEST(call("GET", p, ident(tls::Role::kServer, "cp")).status == 403);
}

BOOST_FIXTURE_TEST_CASE(viewer_lists_real_data, Fixture) {
    BOOST_REQUIRE_EQUAL(create_campaign({}, "c1").status, 201);
    auto r = call("GET", "/api/v1/campaigns", viewer());
    BOOST_TEST(r.status == 200);
    BOOST_TEST(r.body.find("c1") != std::string::npos);
    auto a = call("GET", "/api/v1/audit", viewer());
    BOOST_TEST(a.status == 200);
    BOOST_TEST(a.body.find("campaign.create") != std::string::npos);
}

BOOST_FIXTURE_TEST_CASE(whoami_every_role, Fixture) {
    for (auto [role, name] : {std::pair{tls::Role::kViewer, "viewer"}, std::pair{tls::Role::kAdmin, "admin"},
                              std::pair{tls::Role::kNode, "node"}, std::pair{tls::Role::kServer, "server"}}) {
        auto r = call("GET", "/api/v1/whoami", ident(role, "who"));
        BOOST_TEST_INFO(name);
        BOOST_REQUIRE_EQUAL(r.status, 200);
        BOOST_TEST(r.content_type == "application/json");
        auto o = boost::json::parse(r.body).as_object();
        BOOST_TEST(o.at("role").as_string() == name);
        BOOST_TEST(o.at("name").as_string() == "who");
        BOOST_TEST(o.at("fingerprint").as_string() == "ab12who");
    }
    http::Request r;
    r.method = "GET";
    r.path = "/api/v1/whoami";
    BOOST_TEST(api.handle(r).status == 401);
}

BOOST_FIXTURE_TEST_CASE(csrf_rejects_cross_site_fetch_metadata, Fixture) {
    for (const char* site : {"cross-site", "same-site", "none"}) {
        auto r = create_campaign({{"sec-fetch-site", site}}, std::string("c-") + site);
        BOOST_TEST_INFO(site);
        BOOST_TEST(r.status == 403);
        BOOST_TEST(err_code(r) == "csrf");
    }
    BOOST_TEST(repo.list_campaigns(cv1::CAMPAIGN_STATE_UNSPECIFIED).empty());
    // Also before the role check: a viewer's cross-site POST is a csrf refusal.
    auto v = call("POST", "/api/v1/jobs/j1/cancel", viewer(), {}, {{"sec-fetch-site", "cross-site"}});
    BOOST_TEST(err_code(v) == "csrf");
}

BOOST_FIXTURE_TEST_CASE(csrf_rejects_origin_mismatch, Fixture) {
    const std::map<std::string, std::string> bad[] = {
        {{"origin", "https://evil.example"}, {"host", "cp.example:8443"}},
        {{"origin", "https://cp.example:9999"}, {"host", "cp.example:8443"}},
        {{"origin", "https://cp.example"}, {"host", "cp.example:8443"}},
        {{"origin", "null"}, {"host", "cp.example:8443"}},
        {{"origin", "https://cp.example:8443"}},  // no Host
    };
    int i = 0;
    for (const auto& h : bad) {
        auto r = create_campaign(h, "bad" + std::to_string(i++));
        BOOST_TEST_INFO(r.body);
        BOOST_TEST(r.status == 403);
        BOOST_TEST(err_code(r) == "csrf");
    }
    BOOST_TEST(repo.list_campaigns(cv1::CAMPAIGN_STATE_UNSPECIFIED).empty());
}

BOOST_FIXTURE_TEST_CASE(csrf_allows_same_origin_and_headerless, Fixture) {
    BOOST_TEST(create_campaign({}, "cli").status == 201);  // tdrctl / tdrd: neither header
    BOOST_TEST(create_campaign({{"origin", "https://cp.example:8443"},
                                {"host", "cp.example:8443"},
                                {"sec-fetch-site", "same-origin"}},
                               "ui")
                   .status == 201);
    BOOST_TEST(create_campaign({{"origin", "https://CP.example:8443"}, {"host", "cp.example:8443"}}, "ui2").status ==
               201);
    BOOST_TEST(create_campaign({{"sec-fetch-site", "same-origin"}}, "ui3").status == 201);
    // GET is never subject to the guard.
    auto g = call("GET", "/api/v1/campaigns", viewer(), {}, {{"sec-fetch-site", "cross-site"}, {"origin", "https://evil"}});
    BOOST_TEST(g.status == 200);
}

BOOST_FIXTURE_TEST_CASE(non_json_body_is_415, Fixture) {
    for (const char* ct : {"text/plain", "application/x-www-form-urlencoded", "multipart/form-data; boundary=x"}) {
        auto r = create_campaign({}, "f", ct);
        BOOST_TEST_INFO(ct);
        BOOST_TEST(r.status == 415);
    }
    // Body without any Content-Type.
    http::Request r;
    r.method = "POST";
    r.path = "/api/v1/campaigns";
    r.body = R"({"name":"x"})";
    r.peer = admin();
    BOOST_TEST(api.handle(r).status == 415);

    BOOST_TEST(create_campaign({}, "j1", "application/json; charset=utf-8").status == 201);
    BOOST_TEST(create_campaign({}, "j2", "Application/JSON").status == 201);
    // Empty-body POSTs (e.g. cancel) need no Content-Type.
    auto c = call("POST", "/api/v1/workflows/wf-none/cancel", admin());
    BOOST_TEST(c.status == 404);
}
