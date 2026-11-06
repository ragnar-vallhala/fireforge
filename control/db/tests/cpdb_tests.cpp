#include <libpq-fe.h>

#include <boost/test/unit_test.hpp>
#include <string>

#include "tdr/cpdb/db.hpp"
#include "tdr/cpdb/testing.hpp"

using namespace tdr::cpdb;

namespace {

constexpr std::int64_t T0 = 1'700'000'000'000;  // ms

testing::TempCluster& cluster() {
    static testing::TempCluster c;
    return c;
}

// A fresh database (and migrated repository) per test case.
struct Db {
    std::string conninfo;
    std::unique_ptr<IRepository> repo;

    Db() {
        static int n = 0;
        conninfo = cluster().create_database("t" + std::to_string(++n));
        repo = open_pg_repository(conninfo);
    }
};

cv1::RegisterRequest reg(const std::string& name) {
    cv1::RegisterRequest r;
    r.set_name(name);
    r.set_daemon_version("0.2.0");
    auto* a = r.add_addresses();
    a->set_type("lan");
    a->set_address("192.168.1.10");
    a->set_port(7443);
    r.mutable_physical()->set_cpu_cores(16);
    r.mutable_physical()->set_memory_bytes(64ULL << 30);
    r.mutable_configured()->set_cpu_cores(8);
    r.add_capabilities("cpu");
    r.add_capabilities("cgroups");
    return r;
}

// Runs one SQL statement on a separate connection; returns the first value.
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

}  // namespace

BOOST_AUTO_TEST_SUITE(cpdb)

BOOST_AUTO_TEST_CASE(migrations_idempotent) {
    Db db;
    BOOST_TEST(schema_version(db.conninfo) == 2);
    auto again = open_pg_repository(db.conninfo);
    BOOST_TEST(schema_version(db.conninfo) == 2);
    BOOST_TEST(psql(db.conninfo, "SELECT count(*) FROM schema_migrations") == "2");
    auto empty = cluster().create_database("unmigrated");
    BOOST_TEST(schema_version(empty) == 0);
}

BOOST_AUTO_TEST_CASE(unreachable_throws) {
    BOOST_CHECK_THROW(open_pg_repository("host=/nonexistent-dir port=5432 dbname=x connect_timeout=2"), DbError);
}

BOOST_AUTO_TEST_CASE(register_new_and_reregister) {
    Db db;
    auto n = db.repo->register_node(reg("alpha"), "aa11", T0);
    BOOST_TEST(n.node_id().starts_with("ff-node-"));
    BOOST_TEST(n.node_id().size() == 8 + 16);
    BOOST_TEST(n.name() == "alpha");
    BOOST_TEST(n.health() == cv1::ONLINE);
    BOOST_TEST(n.cert_fingerprint() == "aa11");
    BOOST_TEST(n.registered_unix_ms() == T0);
    BOOST_TEST(n.last_seen_unix_ms() == T0);
    BOOST_TEST(n.policy().version() == 1u);
    BOOST_TEST(n.policy().max_cpu_cores() == 0u);
    BOOST_REQUIRE(n.addresses_size() == 1);
    BOOST_TEST(n.addresses(0).address() == "192.168.1.10");
    BOOST_TEST(n.addresses(0).port() == 7443u);
    BOOST_TEST(n.physical().memory_bytes() == 64ULL << 30);
    BOOST_TEST(n.configured().cpu_cores() == 8u);
    BOOST_REQUIRE(n.capabilities_size() == 2);
    BOOST_TEST(n.capabilities(1) == "cgroups");
    BOOST_TEST(!n.has_last_heartbeat());

    auto ev = db.repo->node_events(n.node_id(), 10);
    BOOST_REQUIRE(ev.size() == 1u);
    BOOST_TEST(ev[0].from() == cv1::NODE_HEALTH_UNSPECIFIED);
    BOOST_TEST(ev[0].to() == cv1::ONLINE);
    BOOST_TEST(ev[0].reason() == "registered");

    cv1::ResourcePolicy lim;
    lim.set_max_jobs(3);
    BOOST_REQUIRE(db.repo->set_policy(n.node_id(), lim, "admin:root", T0 + 1));

    auto r2 = reg("alpha");
    r2.set_daemon_version("0.3.0");
    r2.clear_addresses();
    auto* a = r2.add_addresses();
    a->set_type("tailscale");
    a->set_address("100.64.0.1");
    r2.clear_capabilities();
    r2.add_capabilities("gpu:cuda");
    auto m = db.repo->register_node(r2, "bb22", T0 + 5000);
    BOOST_TEST(m.node_id() == n.node_id());
    BOOST_TEST(m.daemon_version() == "0.3.0");
    BOOST_TEST(m.cert_fingerprint() == "bb22");
    BOOST_TEST(m.registered_unix_ms() == T0);
    BOOST_TEST(m.last_seen_unix_ms() == T0 + 5000);
    BOOST_TEST(m.policy().version() == 2u);
    BOOST_TEST(m.policy().max_jobs() == 3u);
    BOOST_REQUIRE(m.addresses_size() == 1);
    BOOST_TEST(m.addresses(0).type() == "tailscale");
    BOOST_REQUIRE(m.capabilities_size() == 1);
    BOOST_TEST(m.capabilities(0) == "gpu:cuda");
    // ONLINE -> ONLINE: no new event.
    BOOST_TEST(db.repo->node_events(n.node_id(), 10).size() == 1u);

    BOOST_TEST(db.repo->get_node(n.node_id())->name() == "alpha");
    BOOST_TEST(db.repo->find_node_by_name("alpha")->node_id() == n.node_id());
    BOOST_TEST(!db.repo->get_node("ff-node-0000000000000000"));
    BOOST_TEST(!db.repo->find_node_by_name("nope"));
}

BOOST_AUTO_TEST_CASE(list_and_filter) {
    Db db;
    auto c = db.repo->register_node(reg("charlie"), "", T0);
    auto a = db.repo->register_node(reg("alpha"), "", T0);
    auto b = db.repo->register_node(reg("bravo"), "", T0);
    BOOST_REQUIRE(db.repo->set_offline(b.node_id(), "maintenance", T0 + 1));
    auto all = db.repo->list_nodes(cv1::NODE_HEALTH_UNSPECIFIED);
    BOOST_REQUIRE(all.size() == 3u);
    BOOST_TEST(all[0].name() == "alpha");
    BOOST_TEST(all[1].name() == "bravo");
    BOOST_TEST(all[2].name() == "charlie");
    auto online = db.repo->list_nodes(cv1::ONLINE);
    BOOST_REQUIRE(online.size() == 2u);
    BOOST_TEST(online[0].node_id() == a.node_id());
    BOOST_TEST(online[1].node_id() == c.node_id());
    auto off = db.repo->list_nodes(cv1::OFFLINE);
    BOOST_REQUIRE(off.size() == 1u);
    BOOST_TEST(off[0].offline_reason() == "maintenance");
    BOOST_TEST(db.repo->list_nodes(cv1::STALE).empty());
}

BOOST_AUTO_TEST_CASE(heartbeat) {
    Db db;
    cv1::Heartbeat hb;
    hb.set_node_id("ff-node-unknown");
    BOOST_TEST(!db.repo->record_heartbeat(hb, T0));

    auto n = db.repo->register_node(reg("alpha"), "", T0);
    hb.set_node_id(n.node_id());
    hb.set_sent_unix_ms(T0 + 9);
    hb.set_running_jobs(2);
    hb.set_cpu_utilization(0.5);
    hb.set_memory_available_bytes(1ULL << 40);
    hb.mutable_allocated()->set_cpu_cores(4);
    hb.set_policy_version(1);
    BOOST_TEST(db.repo->record_heartbeat(hb, T0 + 10));
    auto got = db.repo->get_node(n.node_id());
    BOOST_REQUIRE(got->has_last_heartbeat());
    BOOST_TEST(got->last_heartbeat().running_jobs() == 2u);
    BOOST_TEST(got->last_heartbeat().cpu_utilization() == 0.5);
    BOOST_TEST(got->last_heartbeat().memory_available_bytes() == 1ULL << 40);
    BOOST_TEST(got->last_heartbeat().allocated().cpu_cores() == 4u);
    BOOST_TEST(got->last_seen_unix_ms() == T0 + 10);
    BOOST_REQUIRE(got->addresses_size() == 1);  // empty heartbeat addresses keep the old ones
    BOOST_TEST(got->addresses(0).address() == "192.168.1.10");

    auto* a = hb.add_addresses();
    a->set_type("public");
    a->set_address("2001:db8::1");
    a->set_port(443);
    auto* b = hb.add_addresses();
    b->set_type("lan");
    b->set_address("10.0.0.2");
    BOOST_TEST(db.repo->record_heartbeat(hb, T0 + 20));
    got = db.repo->get_node(n.node_id());
    BOOST_REQUIRE(got->addresses_size() == 2);
    BOOST_TEST(got->addresses(0).address() == "2001:db8::1");
    BOOST_TEST(got->addresses(1).address() == "10.0.0.2");
    BOOST_TEST(db.repo->node_events(n.node_id(), 10).size() == 1u);

    // Revives STALE and OFFLINE with events.
    auto ch = db.repo->sweep_health(T0 + 20 + 31'000, 30'000, 90'000);
    BOOST_REQUIRE(ch.size() == 1u);
    BOOST_TEST(db.repo->get_node(n.node_id())->health() == cv1::STALE);
    BOOST_TEST(db.repo->record_heartbeat(hb, T0 + 60'000));
    BOOST_TEST(db.repo->get_node(n.node_id())->health() == cv1::ONLINE);
    BOOST_REQUIRE(db.repo->set_offline(n.node_id(), "drain", T0 + 61'000));
    BOOST_TEST(db.repo->record_heartbeat(hb, T0 + 62'000));
    got = db.repo->get_node(n.node_id());
    BOOST_TEST(got->health() == cv1::ONLINE);
    BOOST_TEST(got->offline_reason() == "");
    auto ev = db.repo->node_events(n.node_id(), 10);
    BOOST_REQUIRE(ev.size() == 5u);
    BOOST_TEST(ev[0].from() == cv1::OFFLINE);
    BOOST_TEST(ev[0].to() == cv1::ONLINE);
    BOOST_TEST(ev[0].at_unix_ms() == T0 + 62'000);
    BOOST_TEST(ev[1].to() == cv1::OFFLINE);
    BOOST_TEST(ev[1].reason() == "drain");
    BOOST_TEST(ev[2].from() == cv1::STALE);
    BOOST_TEST(ev[2].to() == cv1::ONLINE);
    BOOST_TEST(ev[3].to() == cv1::STALE);
}

BOOST_AUTO_TEST_CASE(policy_versions) {
    Db db;
    auto n = db.repo->register_node(reg("alpha"), "", T0);
    cv1::ResourcePolicy p;
    p.set_profile("compute");
    p.set_max_cpu_cores(32);
    p.set_max_memory_bytes(96ULL << 30);
    p.set_max_jobs(8);
    p.set_max_disk_bytes(18'000'000'000'000'000'000ULL);  // > INT64_MAX
    p.set_max_gpus(1);
    p.set_version(999);  // ignored
    auto v2 = db.repo->set_policy(n.node_id(), p, "admin:alice", T0 + 100);
    BOOST_REQUIRE(v2);
    BOOST_TEST(v2->version() == 2u);
    BOOST_TEST(v2->profile() == "compute");
    BOOST_TEST(v2->max_cpu_cores() == 32u);
    BOOST_TEST(v2->max_memory_bytes() == 96ULL << 30);
    BOOST_TEST(v2->max_jobs() == 8u);
    BOOST_TEST(v2->max_disk_bytes() == 18'000'000'000'000'000'000ULL);
    BOOST_TEST(v2->max_gpus() == 1u);
    BOOST_TEST(v2->updated_by() == "admin:alice");
    BOOST_TEST(v2->updated_unix_ms() == T0 + 100);
    auto v3 = db.repo->set_policy(n.node_id(), cv1::ResourcePolicy{}, "admin:bob", T0 + 200);
    BOOST_TEST(v3->version() == 3u);
    BOOST_TEST(v3->max_cpu_cores() == 0u);
    auto stored = db.repo->get_node(n.node_id())->policy();
    BOOST_TEST(stored.version() == 3u);
    BOOST_TEST(stored.updated_by() == "admin:bob");
    BOOST_TEST(!db.repo->set_policy("ff-node-missing", p, "x", T0));
}

BOOST_AUTO_TEST_CASE(offline_then_reregister) {
    Db db;
    auto n = db.repo->register_node(reg("alpha"), "", T0);
    BOOST_TEST(!db.repo->set_offline("ff-node-missing", "x", T0));
    BOOST_REQUIRE(db.repo->set_offline(n.node_id(), "decommission", T0 + 1));
    auto got = db.repo->get_node(n.node_id());
    BOOST_TEST(got->health() == cv1::OFFLINE);
    BOOST_TEST(got->offline_reason() == "decommission");
    auto m = db.repo->register_node(reg("alpha"), "", T0 + 2);
    BOOST_TEST(m.health() == cv1::ONLINE);
    BOOST_TEST(m.offline_reason() == "");
    auto ev = db.repo->node_events(n.node_id(), 10);
    BOOST_REQUIRE(ev.size() == 3u);
    BOOST_TEST(ev[0].from() == cv1::OFFLINE);
    BOOST_TEST(ev[0].to() == cv1::ONLINE);
    BOOST_TEST(ev[0].reason() == "registered");
    BOOST_TEST(ev[1].from() == cv1::ONLINE);
    BOOST_TEST(ev[1].to() == cv1::OFFLINE);
}

BOOST_AUTO_TEST_CASE(sweep_thresholds) {
    Db db;
    auto fresh = db.repo->register_node(reg("fresh"), "", T0 + 80'000);
    auto stale = db.repo->register_node(reg("stale"), "", T0 + 50'000);
    auto gone = db.repo->register_node(reg("gone"), "", T0);
    const std::int64_t now = T0 + 100'000;

    // Fresh node, nothing passed: no change.
    BOOST_TEST(db.repo->sweep_health(T0 + 20'000, 30'000, 90'000).empty());

    auto ch = db.repo->sweep_health(now, 30'000, 90'000);
    BOOST_REQUIRE(ch.size() == 2u);
    BOOST_TEST(ch[0].node_id == gone.node_id());  // ONLINE straight to OFFLINE
    BOOST_TEST(ch[0].from == cv1::ONLINE);
    BOOST_TEST(ch[0].to == cv1::OFFLINE);
    BOOST_TEST(ch[0].reason == "heartbeat timeout");
    BOOST_TEST(ch[1].node_id == stale.node_id());
    BOOST_TEST(ch[1].from == cv1::ONLINE);
    BOOST_TEST(ch[1].to == cv1::STALE);
    BOOST_TEST(db.repo->get_node(fresh.node_id())->health() == cv1::ONLINE);
    BOOST_TEST(db.repo->get_node(gone.node_id())->offline_reason() == "heartbeat timeout");
    BOOST_TEST(db.repo->get_node(stale.node_id())->offline_reason() == "");

    // Idempotent at the same time.
    BOOST_TEST(db.repo->sweep_health(now, 30'000, 90'000).empty());

    // STALE -> OFFLINE later.
    ch = db.repo->sweep_health(T0 + 50'000 + 90'001, 30'000, 90'000);
    BOOST_REQUIRE(ch.size() == 2u);  // stale -> OFFLINE, fresh -> STALE
    for (const auto& c : ch) {
        if (c.node_id == stale.node_id()) {
            BOOST_TEST(c.from == cv1::STALE);
            BOOST_TEST(c.to == cv1::OFFLINE);
        } else {
            BOOST_TEST(c.node_id == fresh.node_id());
            BOOST_TEST(c.to == cv1::STALE);
        }
    }
    auto ev = db.repo->node_events(gone.node_id(), 10);
    BOOST_REQUIRE(ev.size() == 2u);
    BOOST_TEST(ev[0].to() == cv1::OFFLINE);
    BOOST_TEST(ev[0].at_unix_ms() == now);
    BOOST_TEST(ev[0].reason() == "heartbeat timeout");
}

BOOST_AUTO_TEST_CASE(events_newest_first_with_limit) {
    Db db;
    auto n = db.repo->register_node(reg("alpha"), "", T0);
    for (int i = 1; i <= 4; ++i) {
        db.repo->set_offline(n.node_id(), "r" + std::to_string(i), T0 + i * 10);
        db.repo->register_node(reg("alpha"), "", T0 + i * 10 + 5);
    }
    auto all = db.repo->node_events(n.node_id(), 100);
    BOOST_REQUIRE(all.size() == 9u);
    for (std::size_t i = 1; i < all.size(); ++i) BOOST_TEST(all[i - 1].at_unix_ms() >= all[i].at_unix_ms());
    auto two = db.repo->node_events(n.node_id(), 2);
    BOOST_REQUIRE(two.size() == 2u);
    BOOST_TEST(two[0].at_unix_ms() == T0 + 45);
    BOOST_TEST(two[1].reason() == "r4");
    BOOST_TEST(db.repo->node_events("ff-node-missing", 10).empty());
}

BOOST_AUTO_TEST_CASE(campaigns) {
    Db db;
    cv1::CreateCampaignRequest req;
    req.set_name("thermo-1");
    req.set_description("first");
    req.set_domain("thermoelectric");
    auto c = db.repo->create_campaign(req, "admin:alice", T0);
    BOOST_REQUIRE(c);
    BOOST_TEST(c->campaign_id().starts_with("camp-"));
    BOOST_TEST(c->campaign_id().size() == 5 + 16);
    BOOST_TEST(c->state() == cv1::DRAFT);
    BOOST_TEST(c->parameters_json() == "{}");
    BOOST_TEST(c->created_by() == "admin:alice");
    BOOST_TEST(c->created_unix_ms() == T0);
    BOOST_TEST(c->updated_unix_ms() == T0);

    auto dup = db.repo->create_campaign(req, "admin:bob", T0 + 1);
    BOOST_REQUIRE(!dup);
    BOOST_TEST(dup.error() == "campaign name already exists");

    req.set_name("thermo-2");
    req.set_parameters_json(R"({"target": {"zT": 1.5}, "elements": ["Bi", "Te"]})");
    auto c2 = db.repo->create_campaign(req, "admin:bob", T0 + 10);
    BOOST_REQUIRE(c2);
    BOOST_TEST(c2->parameters_json().find("\"zT\": 1.5") != std::string::npos);

    auto got = db.repo->get_campaign(c->campaign_id());
    BOOST_REQUIRE(got);
    BOOST_TEST(got->name() == "thermo-1");
    BOOST_TEST(got->domain() == "thermoelectric");
    BOOST_TEST(!db.repo->get_campaign("camp-missing"));

    auto all = db.repo->list_campaigns(cv1::CAMPAIGN_STATE_UNSPECIFIED);
    BOOST_REQUIRE(all.size() == 2u);
    BOOST_TEST(all[0].name() == "thermo-2");  // newest first
    BOOST_TEST(all[1].name() == "thermo-1");

    auto upd = *got;
    upd.set_state(cv1::ACTIVE);
    upd.set_description("running");
    upd.set_parameters_json(R"({"x": 1})");
    upd.set_name("ignored");
    BOOST_TEST(!db.repo->update_campaign(upd, cv1::PAUSED, T0 + 20));  // wrong expected state
    BOOST_TEST(db.repo->get_campaign(c->campaign_id())->state() == cv1::DRAFT);
    BOOST_TEST(db.repo->update_campaign(upd, cv1::DRAFT, T0 + 20));
    BOOST_TEST(!db.repo->update_campaign(upd, cv1::DRAFT, T0 + 21));  // CAS lost
    got = db.repo->get_campaign(c->campaign_id());
    BOOST_TEST(got->state() == cv1::ACTIVE);
    BOOST_TEST(got->description() == "running");
    BOOST_TEST(got->parameters_json() == R"({"x": 1})");
    BOOST_TEST(got->name() == "thermo-1");
    BOOST_TEST(got->updated_unix_ms() == T0 + 20);
    BOOST_TEST(got->created_unix_ms() == T0);

    upd.set_campaign_id("camp-missing");
    BOOST_TEST(!db.repo->update_campaign(upd, cv1::ACTIVE, T0 + 30));

    auto active = db.repo->list_campaigns(cv1::ACTIVE);
    BOOST_REQUIRE(active.size() == 1u);
    BOOST_TEST(active[0].campaign_id() == c->campaign_id());
    auto drafts = db.repo->list_campaigns(cv1::DRAFT);
    BOOST_REQUIRE(drafts.size() == 1u);
    BOOST_TEST(drafts[0].name() == "thermo-2");
    BOOST_TEST(db.repo->list_campaigns(cv1::ARCHIVED).empty());

    // Invalid JSON is rejected by Postgres (the API validates first).
    req.set_name("bad");
    req.set_parameters_json("{not json");
    BOOST_CHECK_THROW((void)db.repo->create_campaign(req, "x", T0), DbError);
    // The repository is still usable afterwards.
    BOOST_TEST(db.repo->list_campaigns(cv1::CAMPAIGN_STATE_UNSPECIFIED).size() == 2u);
}

BOOST_AUTO_TEST_CASE(audit_entries) {
    Db db;
    db.repo->audit("admin:alice", "node.limits", "ff-node-1", R"({"max_jobs": 3})", T0);
    db.repo->audit("admin:bob", "campaign.create", "camp-1", "", T0 + 1);
    db.repo->audit("admin:bob", "node.offline", "ff-node-1", R"({"reason": "x"})", T0 + 2);
    auto all = db.repo->list_audit(10);
    BOOST_REQUIRE(all.size() == 3u);
    BOOST_TEST(all[0].action() == "node.offline");
    BOOST_TEST(all[0].at_unix_ms() == T0 + 2);
    BOOST_TEST(all[1].details_json() == "{}");
    BOOST_TEST(all[2].actor() == "admin:alice");
    BOOST_TEST(all[2].target() == "ff-node-1");
    BOOST_TEST(all[2].details_json() == R"({"max_jobs": 3})");
    auto one = db.repo->list_audit(1);
    BOOST_REQUIRE(one.size() == 1u);
    BOOST_TEST(one[0].action() == "node.offline");
}

BOOST_AUTO_TEST_CASE(hostile_strings_round_trip) {
    Db db;
    const std::string evil = "x'); DROP TABLE nodes; -- \\ \" $1 %s \xc3\xa9\xe2\x9c\x93 \xf0\x9f\x94\xa5";
    auto r = reg(evil);
    r.set_daemon_version(evil);
    r.mutable_addresses(0)->set_address(evil);
    r.add_capabilities(evil);
    auto n = db.repo->register_node(r, evil, T0);
    BOOST_TEST(n.name() == evil);
    BOOST_TEST(n.daemon_version() == evil);
    BOOST_TEST(n.cert_fingerprint() == evil);
    BOOST_TEST(n.addresses(0).address() == evil);
    BOOST_TEST(n.capabilities(2) == evil);
    BOOST_TEST(db.repo->find_node_by_name(evil)->node_id() == n.node_id());
    BOOST_REQUIRE(db.repo->set_offline(n.node_id(), evil, T0 + 1));
    BOOST_TEST(db.repo->get_node(n.node_id())->offline_reason() == evil);
    BOOST_TEST(db.repo->node_events(n.node_id(), 1)[0].reason() == evil);
    cv1::ResourcePolicy p;
    p.set_profile(evil);
    BOOST_TEST(db.repo->set_policy(n.node_id(), p, evil, T0)->updated_by() == evil);

    cv1::CreateCampaignRequest c;
    c.set_name(evil);
    c.set_description(evil);
    c.set_domain(evil);
    auto camp = db.repo->create_campaign(c, evil, T0);
    BOOST_REQUIRE(camp);
    BOOST_TEST(camp->name() == evil);
    BOOST_TEST(camp->description() == evil);
    BOOST_TEST(camp->created_by() == evil);

    db.repo->audit(evil, evil, evil, R"({"k": "it's \\ \"q\" é"})", T0);
    auto a = db.repo->list_audit(1);
    BOOST_TEST(a[0].actor() == evil);
    BOOST_TEST(a[0].details_json() == R"({"k": "it's \\ \"q\" é"})");
    BOOST_TEST(db.repo->list_nodes(cv1::NODE_HEALTH_UNSPECIFIED).size() == 1u);
}

BOOST_AUTO_TEST_CASE(reconnects_after_backend_terminated) {
    Db db;
    auto n = db.repo->register_node(reg("alpha"), "", T0);
    const std::string kill =
        "SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity"
        " WHERE datname = current_database() AND pid <> pg_backend_pid()";
    BOOST_TEST(psql(db.conninfo, kill) == "1");
    // Read path.
    BOOST_TEST(db.repo->get_node(n.node_id())->name() == "alpha");
    BOOST_TEST(psql(db.conninfo, kill) == "1");
    // Transactional write path.
    BOOST_TEST(db.repo->set_offline(n.node_id(), "after reconnect", T0 + 1));
    BOOST_TEST(db.repo->get_node(n.node_id())->offline_reason() == "after reconnect");
}

BOOST_AUTO_TEST_SUITE_END()
