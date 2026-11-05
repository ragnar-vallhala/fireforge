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

}  // namespace

struct ControlApi::Impl {
    cpdb::IRepository& repo;
    ApiOptions options;
    std::function<std::int64_t()> clock;
    http::Router router;

    Impl(cpdb::IRepository& r, ApiOptions o, std::function<std::int64_t()> c)
        : repo(r), options(o), clock(std::move(c)) {
        if (!clock) clock = [] { return unix_ms(); };
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
        add("GET", "/api/v1/audit", &Impl::audit);
    }

    static Response node_not_found(const std::string& id) { return error_response(404, "not_found", "unknown node " + id); }
    static Response campaign_not_found(const std::string& id) {
        return error_response(404, "not_found", "unknown campaign " + id);
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

}  // namespace tdr::cpapi
