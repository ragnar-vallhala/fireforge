// ControlApi: routes, auth rules and handlers (docs/design/phase2.md §3-§5).
#include <algorithm>
#include <cctype>
#include <charconv>
#include <exception>
#include <expected>
#include <format>

#include <boost/json.hpp>

#include "tdr/common/common.hpp"
#include "tdr/cpapi/api.hpp"

namespace tdr::cpapi {

namespace {

using http::error_response;
using http::Request;
using http::Response;

// Local role names (keeps this module independent of tls::to_string).
const char* role_name(tls::Role r) {
    switch (r) {
        case tls::Role::kNode: return "node";
        case tls::Role::kAdmin: return "admin";
        case tls::Role::kServer: return "server";
    }
    return "unknown";
}

std::string actor_of(const tls::Identity& id) { return std::format("{}:{}", role_name(id.role), id.name); }

enum Allow : unsigned { kNode = 1, kAdmin = 2 };

std::optional<Response> check_role(const Request& r, unsigned allow) {
    const auto role = r.peer->role;
    const bool ok = (role == tls::Role::kNode && (allow & kNode)) || (role == tls::Role::kAdmin && (allow & kAdmin));
    if (ok) return std::nullopt;
    return error_response(403, "forbidden", std::format("role {} may not call {} {}", role_name(role), r.method, r.path));
}

Response reply(const google::protobuf::Message& m, int status = 200) { return Response{status, to_json(m), "application/json"}; }

// Empty body parses as {}.
std::optional<Response> parse_body(const Request& r, google::protobuf::Message& m) {
    std::string err;
    const bool empty = std::all_of(r.body.begin(), r.body.end(), [](unsigned char c) { return std::isspace(c); });
    if (!from_json(empty ? std::string("{}") : r.body, m, &err))
        return error_response(400, "bad_request", "invalid JSON body: " + err);
    return std::nullopt;
}

std::string upper(std::string s) {
    for (auto& c : s) c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
    return s;
}

std::expected<std::size_t, Response> parse_limit(const Request& r, std::size_t def, std::size_t max) {
    auto it = r.query.find("limit");
    if (it == r.query.end() || it->second.empty()) return def;
    const auto& s = it->second;
    std::size_t v = 0;
    auto [p, ec] = std::from_chars(s.data(), s.data() + s.size(), v);
    if (ec != std::errc{} || p != s.data() + s.size() || v == 0)
        return std::unexpected(error_response(400, "bad_request", "limit must be a positive integer"));
    return std::min(v, max);
}

bool valid_campaign_name(const std::string& n) {
    if (n.empty() || n.size() > 128) return false;
    return std::all_of(n.begin(), n.end(), [](unsigned char c) {
        return std::isalnum(c) || c == '.' || c == '_' || c == ' ' || c == '-';
    });
}

// "" is allowed (= {}); otherwise must parse as a JSON object.
std::optional<std::string> check_parameters_json(const std::string& s) {
    if (s.empty()) return std::nullopt;
    boost::system::error_code ec;
    auto v = boost::json::parse(s, ec);
    if (ec) return "parameters_json is not valid JSON: " + ec.message();
    if (!v.is_object()) return std::string("parameters_json must be a JSON object");
    return std::nullopt;
}


namespace tv1 = ::fireforge::tdr::v1;

// [A-Za-z0-9._-]{1,100}
bool valid_job_id(const std::string& id) {
    if (id.empty() || id.size() > 100) return false;
    return std::all_of(id.begin(), id.end(),
                       [](unsigned char c) { return std::isalnum(c) || c == '.' || c == '_' || c == '-'; });
}

bool valid_sha256_hex(std::string_view h) {
    return h.size() == 64 && std::all_of(h.begin(), h.end(), [](char c) {
               return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
           });
}

constexpr std::string_view kArtifactPrefix = "artifact://sha256/";

// "artifact://sha256/<64 lowercase hex>"
bool valid_artifact_id(const std::string& id) {
    return id.starts_with(kArtifactPrefix) && valid_sha256_hex(std::string_view(id).substr(kArtifactPrefix.size()));
}

}  // namespace

struct ControlApi::Impl {
    cpdb::IRepository& repo;
    ApiOptions options;
    std::function<std::int64_t()> clock;
    http::Router router;
    std::int64_t started_ms = 0;  // lease-expiry grace starts here (phase4.md §3)

    Impl(cpdb::IRepository& r, ApiOptions o, std::function<std::int64_t()> c)
        : repo(r), options(o), clock(std::move(c)) {
        if (!clock) clock = [] { return unix_ms(); };
        started_ms = clock();
        auto add = [this](const char* method, const char* pattern, Response (Impl::*fn)(Request&)) {
            router.add(method, pattern, [this, fn](Request& req) { return (this->*fn)(req); });
        };
        add("GET", "/healthz", &Impl::healthz);
        add("POST", "/api/v1/nodes/register", &Impl::register_node);
        add("POST", "/api/v1/nodes/heartbeat", &Impl::heartbeat);
        add("GET", "/api/v1/nodes", &Impl::list_nodes);
        add("GET", "/api/v1/nodes/{id}", &Impl::get_node);
        add("PATCH", "/api/v1/nodes/{id}/limits", &Impl::patch_limits);
        add("POST", "/api/v1/nodes/{id}/offline", &Impl::offline);
        add("GET", "/api/v1/nodes/{id}/events", &Impl::events);
        add("POST", "/api/v1/campaigns", &Impl::create_campaign);
        add("GET", "/api/v1/campaigns", &Impl::list_campaigns);
        add("GET", "/api/v1/campaigns/{id}", &Impl::get_campaign);
        add("PATCH", "/api/v1/campaigns/{id}", &Impl::patch_campaign);
        add("POST", "/api/v1/jobs", &Impl::create_job);
        add("GET", "/api/v1/jobs", &Impl::list_jobs);
        add("GET", "/api/v1/jobs/{id}", &Impl::get_job);
        add("POST", "/api/v1/jobs/{id}/cancel", &Impl::cancel_job);
        add("POST", "/api/v1/jobs/{id}/status", &Impl::job_status);
        add("POST", "/api/v1/leases", &Impl::lease);
        add("POST", "/api/v1/artifacts/announce", &Impl::announce);
        add("GET", "/api/v1/artifacts/sha256/{hex}", &Impl::get_artifact);
        add("POST", "/api/v1/artifacts/sha256/{hex}/release", &Impl::release_artifact);
        add("POST", "/api/v1/artifacts/sha256/{hex}/pin", &Impl::pin_artifact);
        add("POST", "/api/v1/artifacts/sha256/{hex}/unpin", &Impl::unpin_artifact);
        add("POST", "/api/v1/workflows", &Impl::create_workflow);
        add("GET", "/api/v1/workflows", &Impl::list_workflows);
        add("GET", "/api/v1/workflows/{id}", &Impl::get_workflow);
        add("POST", "/api/v1/workflows/{id}/cancel", &Impl::cancel_workflow);
        add("GET", "/api/v1/audit", &Impl::audit);
        repo.set_workflow_limits(options.workflow_limits);
    }

    std::int64_t ttl_ms() const { return std::int64_t{options.lease_ttl_seconds} * 1000; }
    std::int64_t locality_wait_ms() const { return std::int64_t{options.locality_wait_seconds} * 1000; }

    static Response node_not_found(const std::string& id) { return error_response(404, "not_found", "unknown node " + id); }
    static Response campaign_not_found(const std::string& id) {
        return error_response(404, "not_found", "unknown campaign " + id);
    }
    static Response job_not_found(const std::string& id) { return error_response(404, "not_found", "unknown job " + id); }
    static Response workflow_not_found(const std::string& id) {
        return error_response(404, "not_found", "unknown workflow " + id);
    }

    // Step templates get the same max_attempts default as submitted jobs.
    void apply_defaults(google::protobuf::RepeatedPtrField<tv1::StepSpec>& steps) const {
        for (auto& s : steps)
            if (s.job().policy().max_attempts() == 0)
                s.mutable_job()->mutable_policy()->set_max_attempts(options.max_attempts_default);
    }

    // The caller's node when node_id names it (mirrors the heartbeat check).
    std::expected<cv1::NodeInfo, Response> own_node(const Request& r, const std::string& node_id) {
        if (node_id.empty()) return std::unexpected(error_response(400, "bad_request", "node_id is required"));
        auto node = repo.get_node(node_id);
        if (!node) return std::unexpected(node_not_found(node_id));
        if (node->name() != r.peer->name)
            return std::unexpected(
                error_response(403, "forbidden", std::format("node {} does not belong to {}", node_id, r.peer->name)));
        return *node;
    }

    Response healthz(Request&) {
        boost::json::object o;
        o["status"] = "ok";
        return Response{200, boost::json::serialize(o), "application/json"};
    }

    // ---- nodes ----------------------------------------------------------------

    Response register_node(Request& r) {
        if (auto e = check_role(r, kNode)) return *e;
        cv1::RegisterRequest req;
        if (auto e = parse_body(r, req)) return *e;
        if (req.name().empty()) return error_response(400, "bad_request", "name is required");
        if (req.name() != r.peer->name)
            return error_response(403, "forbidden",
                                  std::format("name '{}' does not match certificate CN '{}'", req.name(), r.peer->name));
        const auto now = clock();
        const bool known = repo.find_node_by_name(req.name()).has_value();
        auto node = repo.register_node(req, r.peer->fingerprint_sha256, now);
        if (known) log_info("node {} ({}) re-registered from {}", node.name(), node.node_id(), r.remote);
        else log_info("node {} registered as {} from {}", node.name(), node.node_id(), r.remote);
        cv1::RegisterReply out;
        out.set_node_id(node.node_id());
        *out.mutable_policy() = node.policy();
        out.set_heartbeat_interval_seconds(options.heartbeat_interval_seconds);
        out.set_server_time_unix_ms(now);
        return reply(out);
    }

    Response heartbeat(Request& r) {
        if (auto e = check_role(r, kNode)) return *e;
        cv1::Heartbeat hb;
        if (auto e = parse_body(r, hb)) return *e;
        if (hb.node_id().empty()) return error_response(400, "bad_request", "node_id is required");
        auto node = repo.get_node(hb.node_id());
        if (!node) return node_not_found(hb.node_id());
        if (node->name() != r.peer->name)
            return error_response(403, "forbidden", std::format("node {} does not belong to {}", hb.node_id(), r.peer->name));
        const auto now = clock();
        if (!repo.record_heartbeat(hb, now)) return node_not_found(hb.node_id());
        if (node->health() != cv1::ONLINE)
            log_info("node {} ({}) back online via heartbeat", node->name(), node->node_id());
        cv1::HeartbeatReply out;
        if (node->policy().version() != hb.policy_version()) *out.mutable_policy() = node->policy();
        out.set_heartbeat_interval_seconds(options.heartbeat_interval_seconds);
        out.set_server_time_unix_ms(now);
        if (!hb.leases().empty()) {
            const std::vector<cv1::LeaseRef> held(hb.leases().begin(), hb.leases().end());
            for (auto& rv : repo.renew_leases(hb.node_id(), held, ttl_ms(), now)) {
                log_info("node {} lease of job {} attempt {} revoked", node->name(), rv.job_id(), rv.attempt_id());
                *out.add_revoked() = std::move(rv);
            }
        }
        return reply(out);
    }

    Response list_nodes(Request& r) {
        if (auto e = check_role(r, kNode | kAdmin)) return *e;
        cv1::NodeHealth h = cv1::NODE_HEALTH_UNSPECIFIED;
        if (auto it = r.query.find("health"); it != r.query.end() && !it->second.empty()) {
            if (!cv1::NodeHealth_Parse(upper(it->second), &h) || h == cv1::NODE_HEALTH_UNSPECIFIED)
                return error_response(400, "bad_request", "health must be ONLINE, STALE or OFFLINE");
        }
        cv1::NodeList out;
        for (auto& n : repo.list_nodes(h)) *out.add_nodes() = std::move(n);
        return reply(out);
    }

    Response get_node(Request& r) {
        if (auto e = check_role(r, kNode | kAdmin)) return *e;
        auto node = repo.get_node(r.params["id"]);
        if (!node) return node_not_found(r.params["id"]);
        return reply(*node);
    }

    Response patch_limits(Request& r) {
        if (auto e = check_role(r, kAdmin)) return *e;
        const auto& id = r.params["id"];
        cv1::UpdateLimitsRequest req;
        if (auto e = parse_body(r, req)) return *e;
        auto node = repo.get_node(id);
        if (!node) return node_not_found(id);

        cv1::ResourcePolicy p = node->policy();
        if (req.has_profile()) {
            if (req.profile().empty() || req.profile() == "none") {
                p.clear_profile();
                p.set_max_cpu_cores(0);
                p.set_max_memory_bytes(0);
                p.set_max_jobs(0);
                p.set_max_disk_bytes(0);
                p.set_max_gpus(0);
            } else {
                auto pre = profile_preset(req.profile());
                if (!pre)
                    return error_response(400, "bad_request",
                                          std::format("unknown profile '{}' (small, medium, compute, gpu, none)",
                                                      req.profile()));
                p.set_profile(pre->profile());
                p.set_max_cpu_cores(pre->max_cpu_cores());
                p.set_max_memory_bytes(pre->max_memory_bytes());
                p.set_max_jobs(pre->max_jobs());
                p.set_max_disk_bytes(pre->max_disk_bytes());
                p.set_max_gpus(pre->max_gpus());
            }
        }
        if (req.has_max_cpu_cores()) p.set_max_cpu_cores(req.max_cpu_cores());
        if (req.has_max_memory_bytes()) p.set_max_memory_bytes(req.max_memory_bytes());
        if (req.has_max_jobs()) p.set_max_jobs(req.max_jobs());
        if (req.has_max_disk_bytes()) p.set_max_disk_bytes(req.max_disk_bytes());
        if (req.has_max_gpus()) p.set_max_gpus(req.max_gpus());

        const auto actor = actor_of(*r.peer);
        const auto now = clock();
        auto stored = repo.set_policy(id, p, actor, now);
        if (!stored) return node_not_found(id);
        repo.audit(actor, "node.limits", id, to_json(req), now);
        log_info("{} set limits of node {} ({}): policy v{} profile='{}' cpus={} mem={} jobs={} disk={} gpus={}", actor,
                 node->name(), id, stored->version(), stored->profile(), stored->max_cpu_cores(),
                 stored->max_memory_bytes(), stored->max_jobs(), stored->max_disk_bytes(), stored->max_gpus());
        return reply(*stored);
    }

    Response offline(Request& r) {
        if (auto e = check_role(r, kNode | kAdmin)) return *e;
        const auto& id = r.params["id"];
        cv1::OfflineRequest req;
        if (auto e = parse_body(r, req)) return *e;
        auto node = repo.get_node(id);
        if (!node) return node_not_found(id);
        const bool admin = r.peer->role == tls::Role::kAdmin;
        if (!admin && node->name() != r.peer->name)
            return error_response(403, "forbidden", std::format("node {} does not belong to {}", id, r.peer->name));
        const auto actor = actor_of(*r.peer);
        const std::string reason = req.reason().empty() ? "requested by " + actor : req.reason();
        const auto now = clock();
        if (!repo.set_offline(id, reason, now)) return node_not_found(id);
        if (admin) repo.audit(actor, "node.offline", id, to_json(req), now);
        log_info("node {} ({}) offline: {}", node->name(), id, reason);
        auto updated = repo.get_node(id);
        if (!updated) return node_not_found(id);
        return reply(*updated);
    }

    Response events(Request& r) {
        if (auto e = check_role(r, kAdmin)) return *e;
        const auto& id = r.params["id"];
        auto limit = parse_limit(r, 50, 1000);
        if (!limit) return limit.error();
        if (!repo.get_node(id)) return node_not_found(id);
        cv1::NodeEventList out;
        for (auto& ev : repo.node_events(id, *limit)) *out.add_events() = std::move(ev);
        return reply(out);
    }

    // ---- campaigns ------------------------------------------------------------

    Response create_campaign(Request& r) {
        if (auto e = check_role(r, kAdmin)) return *e;
        cv1::CreateCampaignRequest req;
        if (auto e = parse_body(r, req)) return *e;
        if (!valid_campaign_name(req.name()))
            return error_response(400, "bad_request", "name must be 1-128 characters of [A-Za-z0-9._ -]");
        if (auto err = check_parameters_json(req.parameters_json())) return error_response(400, "bad_request", *err);
        const auto actor = actor_of(*r.peer);
        const auto now = clock();
        auto c = repo.create_campaign(req, r.peer->name, now);
        if (!c) return error_response(409, "conflict", c.error());
        repo.audit(actor, "campaign.create", c->campaign_id(), to_json(req), now);
        log_info("{} created campaign {} '{}'", actor, c->campaign_id(), c->name());
        return reply(*c, 201);
    }

    Response list_campaigns(Request& r) {
        if (auto e = check_role(r, kNode | kAdmin)) return *e;
        cv1::CampaignState s = cv1::CAMPAIGN_STATE_UNSPECIFIED;
        if (auto it = r.query.find("state"); it != r.query.end() && !it->second.empty()) {
            if (!cv1::CampaignState_Parse(upper(it->second), &s) || s == cv1::CAMPAIGN_STATE_UNSPECIFIED)
                return error_response(400, "bad_request", "state must be DRAFT, ACTIVE, PAUSED, COMPLETED or ARCHIVED");
        }
        cv1::CampaignList out;
        for (auto& c : repo.list_campaigns(s)) *out.add_campaigns() = std::move(c);
        return reply(out);
    }

    Response get_campaign(Request& r) {
        if (auto e = check_role(r, kNode | kAdmin)) return *e;
        auto c = repo.get_campaign(r.params["id"]);
        if (!c) return campaign_not_found(r.params["id"]);
        return reply(*c);
    }

    Response patch_campaign(Request& r) {
        if (auto e = check_role(r, kAdmin)) return *e;
        const auto& id = r.params["id"];
        cv1::UpdateCampaignRequest req;
        if (auto e = parse_body(r, req)) return *e;
        if (req.has_parameters_json())
            if (auto err = check_parameters_json(req.parameters_json())) return error_response(400, "bad_request", *err);
        auto cur = repo.get_campaign(id);
        if (!cur) return campaign_not_found(id);

        cv1::Campaign updated = *cur;
        if (req.has_description()) updated.set_description(req.description());
        if (req.has_parameters_json()) updated.set_parameters_json(req.parameters_json());
        if (req.state() != cv1::CAMPAIGN_STATE_UNSPECIFIED) {
            if (!is_valid_campaign_transition(cur->state(), req.state()))
                return error_response(409, "conflict",
                                      std::format("invalid campaign transition {} -> {}",
                                                  cv1::CampaignState_Name(cur->state()),
                                                  cv1::CampaignState_Name(req.state())));
            updated.set_state(req.state());
        }
        const auto actor = actor_of(*r.peer);
        const auto now = clock();
        if (!repo.update_campaign(updated, cur->state(), now)) {
            if (!repo.get_campaign(id)) return campaign_not_found(id);
            return error_response(409, "conflict", "campaign was modified concurrently; reload and retry");
        }
        repo.audit(actor, "campaign.update", id, to_json(req), now);
        if (updated.state() != cur->state())
            log_info("{} moved campaign {} {} -> {}", actor, id, cv1::CampaignState_Name(cur->state()),
                     cv1::CampaignState_Name(updated.state()));
        else log_info("{} updated campaign {}", actor, id);
        auto fresh = repo.get_campaign(id);
        if (!fresh) {
            updated.set_updated_unix_ms(now);
            return reply(updated);
        }
        return reply(*fresh);
    }

    // ---- jobs (phase4.md §2, §6) ------------------------------------------------

    Response create_job(Request& r) {
        if (auto e = check_role(r, kNode | kAdmin)) return *e;
        cv1::CreateJobRequest req;
        if (auto e = parse_body(r, req)) return *e;
        tv1::JobSpec spec = req.spec();
        if (spec.command().argv().empty() || spec.command().argv(0).empty())
            return error_response(400, "bad_request", "spec.command.argv must not be empty");
        if (!spec.job_id().empty() && !valid_job_id(spec.job_id()))
            return error_response(400, "bad_request", "job_id must be 1-100 characters of [A-Za-z0-9._-]");
        for (const auto& a : spec.input_artifacts())
            if (!valid_artifact_id(a))
                return error_response(400, "bad_request",
                                      std::format("input artifact '{}' is not artifact://sha256/<64 hex>", a));
        if (!spec.campaign_id().empty() && !repo.get_campaign(spec.campaign_id()))
            return error_response(400, "bad_request", "unknown campaign " + spec.campaign_id());
        if (spec.policy().max_attempts() == 0) spec.mutable_policy()->set_max_attempts(options.max_attempts_default);

        const auto actor = actor_of(*r.peer);
        const auto now = clock();
        auto job = repo.create_job(spec, actor, now);
        if (!job) {
            if (job.error() == "unknown campaign")
                return error_response(400, "bad_request", "unknown campaign " + spec.campaign_id());
            return error_response(409, "conflict", job.error());
        }
        boost::json::object details;
        details["operation"] = spec.operation();
        details["campaign_id"] = spec.campaign_id();
        details["max_attempts"] = spec.policy().max_attempts();
        repo.audit(actor, "job.submit", job->job_id(), boost::json::serialize(details), now);
        log_info("{} submitted job {}{}{}", actor, job->job_id(), spec.campaign_id().empty() ? "" : " in campaign ",
                 spec.campaign_id());
        return reply(*job, 201);
    }

    Response list_jobs(Request& r) {
        if (auto e = check_role(r, kNode | kAdmin)) return *e;
        tv1::JobState s = tv1::JOB_STATE_UNSPECIFIED;
        if (auto it = r.query.find("state"); it != r.query.end() && !it->second.empty()) {
            if (!tv1::JobState_Parse(upper(it->second), &s) || s == tv1::JOB_STATE_UNSPECIFIED)
                return error_response(400, "bad_request", "unknown job state '" + it->second + "'");
        }
        std::string campaign;
        if (auto it = r.query.find("campaign"); it != r.query.end()) campaign = it->second;
        auto limit = parse_limit(r, 100, 1000);
        if (!limit) return limit.error();
        cv1::JobList out;
        for (auto& j : repo.list_jobs(s, campaign, *limit)) *out.add_jobs() = std::move(j);
        return reply(out);
    }

    Response get_job(Request& r) {
        if (auto e = check_role(r, kNode | kAdmin)) return *e;
        auto job = repo.get_job(r.params["id"], true);
        if (!job) return job_not_found(r.params["id"]);
        return reply(*job);
    }

    Response cancel_job(Request& r) {
        if (auto e = check_role(r, kNode | kAdmin)) return *e;
        const auto& id = r.params["id"];
        cv1::CancelRequest req;
        if (auto e = parse_body(r, req)) return *e;
        const auto actor = actor_of(*r.peer);
        const auto now = clock();
        const std::string reason = req.reason().empty() ? "cancelled by " + actor : req.reason();
        auto out = repo.cancel_job(id, reason, now);
        switch (out.result) {
            case cpdb::CancelResult::kNotFound: return job_not_found(id);
            case cpdb::CancelResult::kTerminal:
                return error_response(409, "conflict",
                                      std::format("job {} is already {}", id,
                                                  out.job ? tv1::JobState_Name(out.job->state()) : "finished"));
            case cpdb::CancelResult::kCancelled: log_info("{} cancelled job {}", actor, id); break;
            case cpdb::CancelResult::kRequested:
                log_info("{} requested cancel of job {} (running on {})", actor, id, out.job ? out.job->node_id() : "");
                break;
        }
        repo.audit(actor, "job.cancel", id, to_json(req), now);
        if (!out.job) return job_not_found(id);
        return reply(*out.job);
    }

    Response job_status(Request& r) {
        if (auto e = check_role(r, kNode)) return *e;
        const auto& id = r.params["id"];
        cv1::JobUpdate u;
        if (auto e = parse_body(r, u)) return *e;
        auto node = repo.find_node_by_name(r.peer->name);
        if (!node) return error_response(403, "forbidden", "node " + r.peer->name + " is not registered");
        if (u.attempt_id().empty()) return error_response(400, "bad_request", "attempt_id is required");
        for (const auto& o : u.outputs())
            if (!valid_artifact_id(o.artifact_id()))
                return error_response(400, "bad_request",
                                      std::format("output '{}' is not artifact://sha256/<64 hex>", o.artifact_id()));
        if (u.has_checkpoint() && !u.checkpoint().artifact_id().empty() && !valid_artifact_id(u.checkpoint().artifact_id()))
            return error_response(400, "bad_request",
                                  std::format("checkpoint '{}' is not artifact://sha256/<64 hex>",
                                              u.checkpoint().artifact_id()));
        apply_defaults(*u.mutable_spawn());  // spawn validity is the repository's (kInvalid -> 400)
        auto out = repo.update_job(id, node->node_id(), u, clock());
        switch (out.result) {
            case cpdb::UpdateResult::kNotFound: return job_not_found(id);
            case cpdb::UpdateResult::kInvalid: return error_response(400, "bad_request", out.message);
            case cpdb::UpdateResult::kStale:
                log_info("stale update for job {} from {} ({}): {}", id, node->name(), tv1::JobState_Name(u.state()),
                         out.message);
                return error_response(409, "stale", out.message);
            case cpdb::UpdateResult::kOk: break;
        }
        if (!out.job) return job_not_found(id);
        if (u.state() != tv1::RUNNING || out.job->state() != tv1::RUNNING)
            log_info("job {} attempt {} on {}: {} -> job {}{}{}", id, u.attempt_id(), node->name(),
                     tv1::JobState_Name(u.state()), tv1::JobState_Name(out.job->state()), u.error().empty() ? "" : ": ",
                     u.error());
        return reply(*out.job);
    }

    Response lease(Request& r) {
        if (auto e = check_role(r, kNode)) return *e;
        cv1::LeaseRequest req;
        if (auto e = parse_body(r, req)) return *e;
        auto node = own_node(r, req.node_id());
        if (!node) return node.error();
        cv1::LeaseReply out;
        out.set_lease_ttl_seconds(options.lease_ttl_seconds);
        for (auto& l : repo.lease_jobs(req, ttl_ms(), locality_wait_ms(), clock())) {
            log_info("job {} attempt {} (#{}) leased to {}", l.job_id(), l.attempt_id(), l.attempt_number(),
                     node->name());
            *out.add_leases() = std::move(l);
        }
        return reply(out);
    }

    // ---- artifact locations (phase4.md §4) ------------------------------------------

    Response announce(Request& r) {
        if (auto e = check_role(r, kNode)) return *e;
        cv1::ArtifactAnnouncement a;
        if (auto e = parse_body(r, a)) return *e;
        auto node = own_node(r, a.node_id());
        if (!node) return node.error();
        for (const auto& x : a.artifacts())
            if (!valid_artifact_id(x.artifact_id()))
                return error_response(400, "bad_request",
                                      std::format("artifact '{}' is not artifact://sha256/<64 hex>", x.artifact_id()));
        if (!repo.announce_artifacts(a, clock())) return node_not_found(a.node_id());
        if (a.full())
            log_info("node {} finished artifact sync {} ({} in last page)", node->name(), a.sync_id(),
                     a.artifacts_size());
        return Response{200, "{}", "application/json"};
    }

    // The artifact id named by {hex}, or a 400.
    static std::expected<std::string, Response> artifact_param(Request& r) {
        const auto& hex = r.params["hex"];
        if (!valid_sha256_hex(hex))
            return std::unexpected(error_response(400, "bad_request", "expected 64 lowercase hex characters"));
        return std::string(kArtifactPrefix) + hex;
    }

    Response get_artifact(Request& r) {
        if (auto e = check_role(r, kNode | kAdmin)) return *e;
        auto id = artifact_param(r);
        if (!id) return id.error();
        auto info = repo.get_artifact(*id);
        if (!info) return error_response(404, "not_found", "no node holds " + *id);
        return reply(*info);
    }

    // ---- retention (phase5.md §6) ------------------------------------------------

    Response release_artifact(Request& r) {
        if (auto e = check_role(r, kNode)) return *e;
        auto id = artifact_param(r);
        if (!id) return id.error();
        cv1::ReleaseRequest req;
        if (auto e = parse_body(r, req)) return *e;
        auto node = own_node(r, req.node_id());
        if (!node) return node.error();
        auto out = repo.release_artifact(*id, req.node_id(), req.force(), clock());
        if (out.released()) log_info("node {} released {}{}", node->name(), *id, req.force() ? " (force)" : "");
        return reply(out);
    }

    Response set_pinned(Request& r, bool pinned) {
        if (auto e = check_role(r, kAdmin)) return *e;
        auto id = artifact_param(r);
        if (!id) return id.error();
        const auto now = clock();
        const auto actor = actor_of(*r.peer);
        auto info = repo.set_artifact_pinned(*id, pinned, actor, now);
        repo.audit(actor, pinned ? "artifact.pin" : "artifact.unpin", *id, "{}", now);
        return reply(info);
    }
    Response pin_artifact(Request& r) { return set_pinned(r, true); }
    Response unpin_artifact(Request& r) { return set_pinned(r, false); }

    // ---- workflows (phase6.md §8) ---------------------------------------------------

    Response create_workflow(Request& r) {
        if (auto e = check_role(r, kNode | kAdmin)) return *e;
        cv1::CreateWorkflowRequest req;
        if (auto e = parse_body(r, req)) return *e;
        if (req.name().size() > 256) return error_response(400, "bad_request", "name must be at most 256 characters");
        apply_defaults(*req.mutable_steps());
        const auto actor = actor_of(*r.peer);
        const auto now = clock();
        auto w = repo.create_workflow(req, actor, now);
        if (!w) return error_response(400, "bad_request", w.error());
        boost::json::object details;
        details["name"] = req.name();
        details["campaign_id"] = req.campaign_id();
        details["steps"] = req.steps_size();
        repo.audit(actor, "workflow.create", w->workflow_id(), boost::json::serialize(details), now);
        log_info("{} submitted workflow {} '{}' ({} steps){}{}", actor, w->workflow_id(), w->name(), req.steps_size(),
                 req.campaign_id().empty() ? "" : " in campaign ", req.campaign_id());
        return reply(*w, 201);
    }

    Response list_workflows(Request& r) {
        if (auto e = check_role(r, kNode | kAdmin)) return *e;
        std::string state;
        if (auto it = r.query.find("state"); it != r.query.end() && !it->second.empty()) {
            state = upper(it->second);
            if (state != "RUNNING" && state != "SUCCEEDED" && state != "FAILED" && state != "CANCELLED")
                return error_response(400, "bad_request", "state must be RUNNING, SUCCEEDED, FAILED or CANCELLED");
        }
        std::string campaign;
        if (auto it = r.query.find("campaign"); it != r.query.end()) campaign = it->second;
        auto limit = parse_limit(r, 100, 1000);
        if (!limit) return limit.error();
        cv1::WorkflowList out;
        for (auto& w : repo.list_workflows(state, campaign, *limit)) *out.add_workflows() = std::move(w);
        return reply(out);
    }

    Response get_workflow(Request& r) {
        if (auto e = check_role(r, kNode | kAdmin)) return *e;
        auto w = repo.get_workflow(r.params["id"], true);
        if (!w) return workflow_not_found(r.params["id"]);
        return reply(*w);
    }

    Response cancel_workflow(Request& r) {
        if (auto e = check_role(r, kAdmin)) return *e;
        const auto& id = r.params["id"];
        cv1::CancelRequest req;
        if (auto e = parse_body(r, req)) return *e;
        const auto actor = actor_of(*r.peer);
        const auto now = clock();
        const std::string reason = req.reason().empty() ? "cancelled by " + actor : req.reason();
        auto w = repo.cancel_workflow(id, reason, now);
        if (!w) return workflow_not_found(id);
        repo.audit(actor, "workflow.cancel", id, to_json(req), now);
        log_info("{} cancelled workflow {} (now {})", actor, id, w->state());
        return reply(*w);
    }

    // ---- audit ----------------------------------------------------------------

    Response audit(Request& r) {
        if (auto e = check_role(r, kAdmin)) return *e;
        auto limit = parse_limit(r, 100, 1000);
        if (!limit) return limit.error();
        cv1::AuditList out;
        for (auto& a : repo.list_audit(*limit)) *out.add_entries() = std::move(a);
        return reply(out);
    }
};

ControlApi::ControlApi(cpdb::IRepository& repo, ApiOptions options, std::function<std::int64_t()> clock)
    : impl_(std::make_unique<Impl>(repo, options, std::move(clock))) {}

ControlApi::~ControlApi() = default;

const ApiOptions& ControlApi::options() const { return impl_->options; }

http::Response ControlApi::handle(http::Request& req) {
    if (!req.peer) return error_response(401, "unauthorized", "client certificate required");
    try {
        return impl_->router.dispatch(req);
    } catch (const cpdb::DbError& e) {
        log_error("database error in {} {}: {} (sqlstate {})", req.method, req.path, e.what(), e.sqlstate);
        return error_response(500, "internal", "database error");
    } catch (const std::exception& e) {
        log_error("error in {} {}: {}", req.method, req.path, e.what());
        return error_response(500, "internal", "internal error");
    }
}

std::vector<cpdb::HealthChange> ControlApi::sweep() {
    const auto now = impl_->clock();
    const auto& o = impl_->options;
    auto changes = impl_->repo.sweep_health(now, std::int64_t{o.stale_after_seconds} * 1000,
                                            std::int64_t{o.offline_after_seconds} * 1000);
    for (const auto& c : changes)
        log_info("node {} health {} -> {}{}{}", c.node_id, cv1::NodeHealth_Name(c.from), cv1::NodeHealth_Name(c.to),
                 c.reason.empty() ? "" : ": ", c.reason);
    return changes;
}

std::vector<cpdb::ExpiredLease> ControlApi::sweep_leases() {
    const auto now = impl_->clock();
    // Startup grace: give nodes a full lease_ttl to renew after a control-plane outage.
    if (now - impl_->started_ms < impl_->ttl_ms()) return {};
    auto expired = impl_->repo.expire_leases(now);
    for (const auto& e : expired)
        log_info("lease expired: job {} attempt {} on node {} -> {}", e.job_id, e.attempt_id, e.node_id,
                 tv1::JobState_Name(e.new_state));
    return expired;
}

std::size_t ControlApi::sweep_workflows() {
    const auto changed = impl_->repo.sweep_workflows(impl_->clock());
    if (changed > 0) log_info("workflow sweep: {} step(s) changed", changed);
    return changed;
}

std::vector<cpdb::Reservation> ControlApi::place_jobs() {
    auto rs = impl_->repo.place_jobs(impl_->options.placement, impl_->clock());
    for (const auto& r : rs)
        log_info("job {} reserved for node {} (cost {:.3f}) until {}", r.job_id, r.node_id, r.cost, r.until_ms);
    return rs;
}

}  // namespace tdr::cpapi
