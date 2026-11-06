// make_https_control_client against a real HttpsServer with mTLS (same io_context).
#include <google/protobuf/util/json_util.h>

#include <boost/test/unit_test.hpp>
#include <chrono>

#include "tdr/agent/agent.hpp"
#include "tdr/common/common.hpp"
#include "tdr/tls/tls.hpp"

namespace asio = boost::asio;
namespace tls = tdr::tls;
namespace http = tdr::http;
using namespace std::chrono_literals;
using namespace tdr::agent;

namespace {

struct Pki {
    fs::path dir = fs::temp_directory_path() / tdr::new_id("agent-pki");
    Pki() {
        fs::create_directories(dir);
        must(tls::create_ca(dir, "ca"));
        tls::IssueOptions s;
        s.role = tls::Role::kServer;
        s.name = "cp";
        s.ip_sans = {"127.0.0.1"};
        must(tls::issue(dir, s, dir / "cp.key", dir / "cp.crt"));
        tls::IssueOptions n;
        n.name = "node-a";
        must(tls::issue(dir, n, dir / "node.key", dir / "node.crt"));
    }
    ~Pki() {
        std::error_code ec;
        fs::remove_all(dir, ec);
    }
    static void must(const std::expected<void, std::string>& r) {
        if (!r) throw std::runtime_error(r.error());
    }
};

std::string json_of(const google::protobuf::Message& m) {
    std::string s;
    google::protobuf::util::JsonPrintOptions o;
    o.preserve_proto_field_names = true;
    (void)google::protobuf::util::MessageToJsonString(m, &s, o);
    return s;
}

struct Rig {
    Pki pki;
    asio::io_context io;
    http::Router router;
    std::unique_ptr<http::HttpsServer> server;
    std::unique_ptr<IControlClient> client;
    std::vector<std::string> seen;  // "METHOD path body"
    cv1::RegisterRequest last_register;
    bool fail_list = false;

    Rig() {
        router.add("POST", "/api/v1/nodes/register", [this](http::Request& r) {
            seen.push_back("register " + r.peer.value_or(tls::Identity{}).name);
            google::protobuf::util::JsonParseOptions po;
            po.ignore_unknown_fields = true;
            if (!google::protobuf::util::JsonStringToMessage(r.body, &last_register, po).ok()) {
                return http::error_response(400, "bad_request", "bad json");
            }
            cv1::RegisterReply rep;
            rep.set_node_id("ff-node-00000000000000aa");
            rep.mutable_policy()->set_version(3);
            rep.mutable_policy()->set_max_jobs(1);
            rep.set_heartbeat_interval_seconds(10);
            return http::Response{200, json_of(rep) + R"(                                     )"};
        });
        router.add("POST", "/api/v1/nodes/heartbeat", [this](http::Request& r) {
            seen.push_back("heartbeat");
            if (r.body.find("ff-node-00000000000000aa") == std::string::npos) {
                return http::error_response(404, "not_found", "unknown node");
            }
            return http::Response{200, R"({"heartbeat_interval_seconds": 10, "extra": true})"};
        });
        router.add("GET", "/api/v1/nodes", [this](http::Request&) {
            seen.push_back("list");
            if (fail_list) return http::Response{502, "gateway down", "text/plain"};
            return http::Response{200, R"({"nodes": [{"node_id": "x", "name": "node-a"}]})"};
        });
        router.add("POST", "/api/v1/nodes/{id}/offline", [this](http::Request& r) {
            seen.push_back("offline " + r.params.at("id") + " " + r.body);
            return http::Response{200, "not json"};
        });

        // ---- Phase 4 routes ----
        router.add("POST", "/api/v1/jobs", [this](http::Request& r) {
            seen.push_back("submit " + r.body);
            return http::Response{201, R"({"job_id": "job-1", "state": "QUEUED"})"};
        });
        router.add("GET", "/api/v1/jobs", [this](http::Request& r) {
            std::string q;
            for (const auto& [k, v] : r.query) q += k + "=" + v + ";";
            seen.push_back("list_jobs " + q);
            return http::Response{200, R"({"jobs": [{"job_id": "job-1"}, {"job_id": "job-2"}]})"};
        });
        router.add("GET", "/api/v1/jobs/{id}", [this](http::Request& r) {
            seen.push_back("get " + r.params.at("id"));
            if (r.params.at("id") != "job-1") return http::error_response(404, "not_found", "no such job");
            return http::Response{200, R"({"job_id": "job-1", "state": "RUNNING"})"};
        });
        router.add("POST", "/api/v1/jobs/{id}/cancel", [this](http::Request& r) {
            seen.push_back("cancel " + r.params.at("id") + " " + r.body);
            return http::Response{200, R"({"job_id": "job-1", "cancel_requested": true})"};
        });
        router.add("POST", "/api/v1/jobs/{id}/status", [this](http::Request& r) {
            seen.push_back("status " + r.params.at("id"));
            if (r.body.find("att-old") != std::string::npos) return http::error_response(409, "stale", "stale attempt");
            return http::Response{200, R"({"job_id": "job-1", "state": "COMPLETED"})"};
        });
        router.add("POST", "/api/v1/leases", [this](http::Request& r) {
            seen.push_back("lease " + r.body);
            return http::Response{200, R"({"leases": [{"job_id": "job-1", "attempt_id": "att-1",
                "fencing_token": "1", "attempt_number": 1, "spec": {"command": {"argv": ["true"]}}}],
                "lease_ttl_seconds": 60})"};
        });
        router.add("POST", "/api/v1/artifacts/announce", [this](http::Request& r) {
            seen.push_back("announce " + r.body);
            if (r.body.find("bad") != std::string::npos) return http::error_response(400, "bad_request", "bad id");
            return http::Response{200, "{}"};
        });
        // ---- Phase 5 routes ----
        router.add("GET", "/api/v1/artifacts/sha256/{hex}", [this](http::Request& r) {
            seen.push_back("get_artifact " + r.params.at("hex"));
            if (r.params.at("hex") != "ab12") return http::error_response(404, "not_found", "no holder");
            return http::Response{200, R"({"artifact": {"artifact_id": "artifact://sha256/ab12", "size": "5"},
                "node_ids": ["n1", "n2"], "pinned": true})"};
        });
        router.add("POST", "/api/v1/artifacts/sha256/{hex}/release", [this](http::Request& r) {
            seen.push_back("release " + r.params.at("hex") + " " + r.body);
            if (r.body.find("\"force\":true") != std::string::npos) return http::Response{200, R"({"released": true})"};
            return http::Response{200, R"({"released": false, "reason": "last copy"})"};
        });

        http::ServerOptions so;
        so.listen_address = "127.0.0.1";
        so.port = 0;
        tls::Credentials sc{pki.dir / "ca.crt", pki.dir / "cp.crt", pki.dir / "cp.key"};
        server = std::make_unique<http::HttpsServer>(io, tls::make_server_context(sc), so,
                                                     [this](http::Request& r) { return router.dispatch(r); });
        server->start();
        client = make_https_control_client(io, client_ctx(), options(server->bound_port()));
    }
    ~Rig() {
        client.reset();
        server->stop();
        io.restart();
        io.run_for(100ms);
    }
    std::shared_ptr<asio::ssl::context> client_ctx() const {
        return tls::make_client_context({pki.dir / "ca.crt", pki.dir / "node.crt", pki.dir / "node.key"});
    }
    static http::ClientOptions options(std::uint16_t port) {
        http::ClientOptions o;
        o.host = "127.0.0.1";
        o.port = port;
        o.verify_host = "127.0.0.1";
        o.timeout = 3000ms;
        return o;
    }
    template <class Pred>
    bool run_until(Pred pred, std::chrono::milliseconds timeout = 5000ms) {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (!pred()) {
            if (std::chrono::steady_clock::now() >= deadline) return false;
            if (io.stopped()) io.restart();
            io.run_one_for(10ms);
        }
        return true;
    }
};

}  // namespace

BOOST_AUTO_TEST_SUITE(https_control_client)

BOOST_AUTO_TEST_CASE(round_trips_and_errors) {
    Rig rig;
    cv1::RegisterRequest req;
    req.set_name("node-a");
    req.set_daemon_version("0.1.0");
    req.add_capabilities("cpu");

    std::optional<Result<cv1::RegisterReply>> reg;
    rig.client->register_node(req, [&](Result<cv1::RegisterReply> r) { reg = std::move(r); });
    BOOST_REQUIRE(rig.run_until([&] { return reg.has_value(); }));
    BOOST_REQUIRE_MESSAGE(reg->has_value(), (reg->has_value() ? "" : reg->error().message));
    BOOST_TEST((*reg)->node_id() == "ff-node-00000000000000aa");
    BOOST_TEST((*reg)->policy().version() == 3u);
    BOOST_TEST(rig.last_register.name() == "node-a");
    BOOST_TEST(rig.last_register.capabilities_size() == 1);
    BOOST_TEST(rig.seen.at(0) == "register node-a");  // mTLS identity reached the server

    std::optional<Result<cv1::HeartbeatReply>> hb;
    cv1::Heartbeat h;
    h.set_node_id("ff-node-unknown");
    rig.client->heartbeat(h, [&](Result<cv1::HeartbeatReply> r) { hb = std::move(r); });
    BOOST_REQUIRE(rig.run_until([&] { return hb.has_value(); }));
    BOOST_REQUIRE(!hb->has_value());
    BOOST_TEST(hb->error().http_status == 404);
    BOOST_TEST(hb->error().message == "unknown node");

    hb.reset();
    h.set_node_id("ff-node-00000000000000aa");
    rig.client->heartbeat(h, [&](Result<cv1::HeartbeatReply> r) { hb = std::move(r); });
    BOOST_REQUIRE(rig.run_until([&] { return hb.has_value(); }));
    BOOST_REQUIRE(hb->has_value());  // unknown field ignored
    BOOST_TEST((*hb)->heartbeat_interval_seconds() == 10u);

    std::optional<Result<cv1::NodeList>> list;
    rig.client->list_nodes([&](Result<cv1::NodeList> r) { list = std::move(r); });
    BOOST_REQUIRE(rig.run_until([&] { return list.has_value(); }));
    BOOST_REQUIRE(list->has_value());
    BOOST_TEST((*list)->nodes_size() == 1);

    list.reset();
    rig.fail_list = true;
    rig.client->list_nodes([&](Result<cv1::NodeList> r) { list = std::move(r); });
    BOOST_REQUIRE(rig.run_until([&] { return list.has_value(); }));
    BOOST_REQUIRE(!list->has_value());
    BOOST_TEST(list->error().http_status == 502);
    BOOST_TEST(list->error().message == "gateway down");  // non-ApiError body passed through

    std::optional<Result<cv1::NodeInfo>> off;
    rig.client->offline("ff-node-00000000000000aa", "shutdown", [&](Result<cv1::NodeInfo> r) { off = std::move(r); });
    BOOST_REQUIRE(rig.run_until([&] { return off.has_value(); }));
    BOOST_REQUIRE(!off->has_value());  // 2xx with a non-JSON body
    BOOST_TEST(off->error().http_status == 200);
    BOOST_TEST(off->error().message.starts_with("bad response"));
    BOOST_TEST(rig.seen.back() == R"(offline ff-node-00000000000000aa {"reason":"shutdown"})");
}

BOOST_AUTO_TEST_CASE(phase4_calls) {
    Rig rig;
    namespace tv1 = ::fireforge::tdr::v1;

    std::optional<Result<cv1::Job>> job;
    cv1::CreateJobRequest cr;
    cr.mutable_spec()->mutable_command()->add_argv("true");
    rig.client->submit_job(cr, [&](Result<cv1::Job> r) { job = std::move(r); });
    BOOST_REQUIRE(rig.run_until([&] { return job.has_value(); }));
    BOOST_REQUIRE(job->has_value());
    BOOST_TEST((*job)->job_id() == "job-1");
    BOOST_TEST((*job)->state() == tv1::QUEUED);
    BOOST_TEST(rig.seen.back() == R"(submit {"spec":{"command":{"argv":["true"]}}})");

    job.reset();
    rig.client->get_job("job-1", [&](Result<cv1::Job> r) { job = std::move(r); });
    BOOST_REQUIRE(rig.run_until([&] { return job.has_value(); }));
    BOOST_REQUIRE(job->has_value());
    BOOST_TEST((*job)->state() == tv1::RUNNING);

    job.reset();
    rig.client->get_job("nope", [&](Result<cv1::Job> r) { job = std::move(r); });
    BOOST_REQUIRE(rig.run_until([&] { return job.has_value(); }));
    BOOST_REQUIRE(!job->has_value());
    BOOST_TEST(job->error().http_status == 404);
    BOOST_TEST(job->error().message == "no such job");

    std::optional<Result<cv1::JobList>> list;
    rig.client->list_jobs(tv1::RUNNING, "camp 1&x", 5, [&](Result<cv1::JobList> r) { list = std::move(r); });
    BOOST_REQUIRE(rig.run_until([&] { return list.has_value(); }));
    BOOST_REQUIRE(list->has_value());
    BOOST_TEST((*list)->jobs_size() == 2);
    BOOST_TEST(rig.seen.back() == "list_jobs campaign=camp 1&x;limit=5;state=RUNNING;");  // encoded + decoded
    list.reset();
    rig.client->list_jobs(tv1::JOB_STATE_UNSPECIFIED, "", 0, [&](Result<cv1::JobList> r) { list = std::move(r); });
    BOOST_REQUIRE(rig.run_until([&] { return list.has_value(); }));
    BOOST_TEST(rig.seen.back() == "list_jobs ");

    job.reset();
    rig.client->cancel_job("job-1", "user", [&](Result<cv1::Job> r) { job = std::move(r); });
    BOOST_REQUIRE(rig.run_until([&] { return job.has_value(); }));
    BOOST_REQUIRE(job->has_value());
    BOOST_TEST((*job)->cancel_requested());
    BOOST_TEST(rig.seen.back() == R"(cancel job-1 {"reason":"user"})");

    std::optional<Result<cv1::LeaseReply>> lease;
    cv1::LeaseRequest lr;
    lr.set_node_id("ff-node-00000000000000aa");
    lr.set_max_jobs(2);
    rig.client->lease(lr, [&](Result<cv1::LeaseReply> r) { lease = std::move(r); });
    BOOST_REQUIRE(rig.run_until([&] { return lease.has_value(); }));
    BOOST_REQUIRE(lease->has_value());
    BOOST_REQUIRE((*lease)->leases_size() == 1);
    BOOST_TEST((*lease)->leases(0).fencing_token() == 1u);
    BOOST_TEST((*lease)->leases(0).spec().command().argv(0) == "true");
    BOOST_TEST((*lease)->lease_ttl_seconds() == 60u);

    job.reset();
    cv1::JobUpdate up;
    up.set_attempt_id("att-old");
    up.set_state(tv1::COMPLETED);
    rig.client->update_job("job-1", up, [&](Result<cv1::Job> r) { job = std::move(r); });
    BOOST_REQUIRE(rig.run_until([&] { return job.has_value(); }));
    BOOST_REQUIRE(!job->has_value());
    BOOST_TEST(job->error().http_status == 409);
    job.reset();
    up.set_attempt_id("att-1");
    rig.client->update_job("job-1", up, [&](Result<cv1::Job> r) { job = std::move(r); });
    BOOST_REQUIRE(rig.run_until([&] { return job.has_value(); }));
    BOOST_REQUIRE(job->has_value());
    BOOST_TEST(rig.seen.back() == "status job-1");

    std::optional<Result<void>> ann;
    cv1::ArtifactAnnouncement a;
    a.set_node_id("n");
    a.add_artifacts()->set_artifact_id("artifact://sha256/00");
    a.set_full(true);
    rig.client->announce_artifacts(a, [&](Result<void> r) { ann = std::move(r); });
    BOOST_REQUIRE(rig.run_until([&] { return ann.has_value(); }));
    BOOST_TEST(ann->has_value());
    ann.reset();
    a.mutable_artifacts(0)->set_artifact_id("bad");
    rig.client->announce_artifacts(a, [&](Result<void> r) { ann = std::move(r); });
    BOOST_REQUIRE(rig.run_until([&] { return ann.has_value(); }));
    BOOST_REQUIRE(!ann->has_value());
    BOOST_TEST(ann->error().http_status == 400);
    BOOST_TEST(ann->error().message == "bad id");

    std::optional<Result<cv1::ArtifactInfo>> info;
    rig.client->get_artifact("artifact://sha256/ab12", [&](Result<cv1::ArtifactInfo> r) { info = std::move(r); });
    BOOST_REQUIRE(rig.run_until([&] { return info.has_value(); }));
    BOOST_REQUIRE(info->has_value());
    BOOST_TEST((*info)->artifact().size() == 5u);
    BOOST_TEST((*info)->node_ids_size() == 2);
    BOOST_TEST((*info)->pinned());
    BOOST_TEST(rig.seen.back() == "get_artifact ab12");
    info.reset();
    rig.client->get_artifact("artifact://sha256/cd34", [&](Result<cv1::ArtifactInfo> r) { info = std::move(r); });
    BOOST_REQUIRE(rig.run_until([&] { return info.has_value(); }));
    BOOST_REQUIRE(!info->has_value());
    BOOST_TEST(info->error().http_status == 404);
    BOOST_TEST(info->error().message == "no holder");

    std::optional<Result<cv1::ReleaseReply>> rel;
    cv1::ReleaseRequest rr;
    rr.set_node_id("n1");
    rig.client->release_artifact("artifact://sha256/ab12", rr, [&](Result<cv1::ReleaseReply> r) { rel = std::move(r); });
    BOOST_REQUIRE(rig.run_until([&] { return rel.has_value(); }));
    BOOST_REQUIRE(rel->has_value());
    BOOST_TEST(!(*rel)->released());
    BOOST_TEST((*rel)->reason() == "last copy");
    BOOST_TEST(rig.seen.back() == R"(release ab12 {"node_id":"n1"})");
    rel.reset();
    rr.set_force(true);
    rig.client->release_artifact("artifact://sha256/ab12", rr, [&](Result<cv1::ReleaseReply> r) { rel = std::move(r); });
    BOOST_REQUIRE(rig.run_until([&] { return rel.has_value(); }));
    BOOST_REQUIRE(rel->has_value());
    BOOST_TEST((*rel)->released());
}

BOOST_AUTO_TEST_CASE(transport_error_has_status_zero) {
    Rig rig;
    const auto port = rig.server->bound_port();
    rig.server->stop();
    auto dead = make_https_control_client(rig.io, rig.client_ctx(), Rig::options(port));
    std::optional<Result<cv1::NodeList>> list;
    dead->list_nodes([&](Result<cv1::NodeList> r) { list = std::move(r); });
    BOOST_REQUIRE(rig.run_until([&] { return list.has_value(); }));
    BOOST_REQUIRE(!list->has_value());
    BOOST_TEST(list->error().http_status == 0);
    BOOST_TEST(!list->error().message.empty());
}

BOOST_AUTO_TEST_CASE(node_agent_end_to_end) {
    Rig rig;
    AgentOptions o;
    o.registration.set_name("node-a");
    o.registration.set_daemon_version("0.1.0");
    o.local_capacity = {4, 8ull << 30, 4};
    o.policy_cache = rig.pki.dir / "policy.json";
    std::vector<tdr::jobs::Capacity> applied;
    AgentHooks hooks;
    hooks.apply_capacity = [&](const tdr::jobs::Capacity& c) { applied.push_back(c); };
    NodeAgent agent(rig.io, *rig.client, o, hooks);
    agent.start();
    BOOST_REQUIRE(rig.run_until([&] { return agent.state() == ControlState::kConnected && agent.peers_refreshed_ms() > 0; }));
    BOOST_TEST(agent.node_id() == "ff-node-00000000000000aa");
    BOOST_REQUIRE(applied.size() == 2u);
    BOOST_TEST(applied[1].jobs == 1u);
    BOOST_TEST(fs::exists(o.policy_cache));
    BOOST_TEST(agent.peers().nodes_size() == 1);

    bool done = false;
    agent.stop([&] { done = true; });
    BOOST_REQUIRE(rig.run_until([&] { return done; }));
    BOOST_TEST(rig.seen.back().starts_with("offline ff-node-00000000000000aa"));
}

BOOST_AUTO_TEST_SUITE_END()
