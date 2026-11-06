// Phase 5 endpoints: release, pin/unpin, locality_wait pass-through
// (docs/design/phase5.md §4, §6).
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

std::string hex(char c) { return std::string(64, c); }
std::string art(char c) { return "artifact://sha256/" + hex(c); }

struct Fixture {
    FakeRepository repo;
    std::int64_t now = 10'000'000;
    ApiOptions opts = [] {
        ApiOptions o;
        o.locality_wait_seconds = 7;
        return o;
    }();
    ControlApi api{repo, opts, [this] { return now; }};

    http::Response call(const std::string& method, const std::string& path, std::optional<tls::Identity> peer,
                        std::string body = {}) {
        http::Request r;
        r.method = method;
        r.path = path;
        r.body = std::move(body);
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

    void hold(const std::string& node_id, char c) { repo.locations[{art(c), node_id}].set_artifact_id(art(c)); }

    http::Response release(tls::Identity who, char c, const std::string& node_id, bool force = false) {
        cv1::ReleaseRequest req;
        req.set_node_id(node_id);
        req.set_force(force);
        return call("POST", "/api/v1/artifacts/sha256/" + hex(c) + "/release", std::move(who), to_json(req));
    }
};

}  // namespace

BOOST_AUTO_TEST_SUITE(cpapi_artifacts)

BOOST_FIXTURE_TEST_CASE(lease_passes_locality_wait, Fixture) {
    const auto a = register_node("a");
    cv1::LeaseRequest req;
    req.set_node_id(a);
    req.mutable_free()->set_cpu_cores(1);
    req.set_max_jobs(1);
    BOOST_REQUIRE_EQUAL(call("POST", "/api/v1/leases", node("a"), to_json(req)).status, 200);
    BOOST_TEST(repo.last_locality_wait_ms == 7'000);
    BOOST_TEST(ApiOptions{}.locality_wait_seconds == 15u);
}

BOOST_FIXTURE_TEST_CASE(release_endpoint, Fixture) {
    const auto a = register_node("a");
    const auto b = register_node("b");
    hold(a, '1');
    hold(b, '1');
    hold(a, '2');

    auto r = release(node("a"), '1', a);
    BOOST_REQUIRE_EQUAL(r.status, 200);
    auto rep = parse<cv1::ReleaseReply>(r);
    BOOST_TEST(rep.released());
    BOOST_TEST(!repo.locations.contains({art('1'), a}));

    rep = parse<cv1::ReleaseReply>(release(node("b"), '1', b));
    BOOST_TEST(!rep.released());
    BOOST_TEST(rep.reason() == "last copy");

    rep = parse<cv1::ReleaseReply>(release(node("a"), '2', a, true));  // node may force
    BOOST_TEST(rep.released());
    BOOST_TEST(!repo.locations.contains({art('2'), a}));

    // Auth and validation.
    BOOST_TEST(release(admin(), '1', b).status == 403);                     // admin can't release
    BOOST_TEST(release(node("a"), '1', b).status == 403);                   // not the caller's node
    BOOST_TEST(release(node("a"), '1', "").status == 400);                  // node_id required
    BOOST_TEST(release(node("a"), '1', "ff-node-ffffffffffffffff").status == 404);
    BOOST_TEST(release(node("a"), 'D', a).status == 400);                   // uppercase hex
    BOOST_TEST(call("POST", "/api/v1/artifacts/sha256/abc/release", node("a"), "{}").status == 400);
    BOOST_TEST(call("POST", "/api/v1/artifacts/sha256/" + hex('1') + "/release", node("a"), "{bad").status == 400);
    BOOST_TEST(call("POST", "/api/v1/artifacts/sha256/" + hex('1') + "/release", std::nullopt, "{}").status == 401);
    BOOST_TEST(repo.locations.contains({art('1'), b}));
    BOOST_TEST(repo.audit_rows.empty());  // releases are not audited
}

BOOST_FIXTURE_TEST_CASE(pin_and_unpin, Fixture) {
    const auto a = register_node("a");
    const auto b = register_node("b");
    hold(a, 'c');
    hold(b, 'c');

    auto r = call("POST", "/api/v1/artifacts/sha256/" + hex('c') + "/pin", admin());
    BOOST_REQUIRE_EQUAL(r.status, 200);
    auto info = parse<cv1::ArtifactInfo>(r);
    BOOST_TEST(info.pinned());
    BOOST_TEST(info.node_ids_size() == 2);
    BOOST_TEST(repo.pins.at(art('c')) == "admin:alice");
    BOOST_REQUIRE_EQUAL(repo.audit_rows.size(), 1u);
    BOOST_TEST(repo.audit_rows[0].action == "artifact.pin");
    BOOST_TEST(repo.audit_rows[0].actor == "admin:alice");
    BOOST_TEST(repo.audit_rows[0].target == art('c'));
    BOOST_TEST(repo.audit_rows[0].at == now);

    auto rep = parse<cv1::ReleaseReply>(release(node("a"), 'c', a));
    BOOST_TEST(!rep.released());
    BOOST_TEST(rep.reason() == "pinned");
    BOOST_TEST(parse<cv1::ArtifactInfo>(call("GET", "/api/v1/artifacts/sha256/" + hex('c'), node("b"))).pinned());

    r = call("POST", "/api/v1/artifacts/sha256/" + hex('c') + "/unpin", admin("bob"));
    BOOST_REQUIRE_EQUAL(r.status, 200);
    BOOST_TEST(!parse<cv1::ArtifactInfo>(r).pinned());
    BOOST_REQUIRE_EQUAL(repo.audit_rows.size(), 2u);
    BOOST_TEST(repo.audit_rows[1].action == "artifact.unpin");
    BOOST_TEST(repo.audit_rows[1].actor == "admin:bob");
    BOOST_TEST(parse<cv1::ReleaseReply>(release(node("a"), 'c', a)).released());

    // Pinning an artifact nobody holds is allowed; GET then finds it.
    r = call("POST", "/api/v1/artifacts/sha256/" + hex('e') + "/pin", admin());
    BOOST_REQUIRE_EQUAL(r.status, 200);
    info = parse<cv1::ArtifactInfo>(r);
    BOOST_TEST(info.pinned());
    BOOST_TEST(info.artifact().artifact_id() == art('e'));
    BOOST_TEST(info.node_ids_size() == 0);
    BOOST_TEST(call("GET", "/api/v1/artifacts/sha256/" + hex('e'), admin()).status == 200);

    // Auth and validation (nothing audited).
    const auto audited = repo.audit_rows.size();
    BOOST_TEST(call("POST", "/api/v1/artifacts/sha256/" + hex('c') + "/pin", node("a")).status == 403);
    BOOST_TEST(call("POST", "/api/v1/artifacts/sha256/" + hex('c') + "/unpin", node("a")).status == 403);
    BOOST_TEST(call("POST", "/api/v1/artifacts/sha256/" + hex('P') + "/pin", admin()).status == 400);
    BOOST_TEST(call("POST", "/api/v1/artifacts/sha256/xyz/unpin", admin()).status == 400);
    BOOST_TEST(call("GET", "/api/v1/artifacts/sha256/" + hex('c') + "/pin", admin()).status == 405);
    BOOST_TEST(repo.audit_rows.size() == audited);
}

BOOST_FIXTURE_TEST_CASE(db_error_is_500, Fixture) {
    const auto a = register_node("a");
    repo.fail_all = true;
    auto r = release(node("a"), '1', a);
    BOOST_TEST(r.status == 500);
    BOOST_TEST(r.body.find("secret-sql-detail") == std::string::npos);
    BOOST_TEST(call("POST", "/api/v1/artifacts/sha256/" + hex('1') + "/pin", admin()).status == 500);
}

BOOST_AUTO_TEST_SUITE_END()
