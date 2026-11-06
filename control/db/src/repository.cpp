// PgRepository: IRepository on PostgreSQL (docs/design/phase2.md §4-§6).
#include <limits>
#include <string>

#include "jobs.hpp"
#include "workflows.hpp"
#include "pg.hpp"
#include "util.hpp"
#include "tdr/common/common.hpp"
#include "tdr/cpdb/db.hpp"

namespace tdr::cpdb {

namespace {

using pg::Conn;
using pg::Result;
using pg::transact;

// ---- conversions ------------------------------------------------------------

using detail::clamp_limit;
using detail::from_json;
using detail::short_id;
using detail::to_json;

// Repeated fields are stored as plain JSON arrays: serialize them through a
// carrier message ({"addresses": [...]}) and keep only the array.
std::string addresses_json(const google::protobuf::RepeatedPtrField<cv1::NodeAddress>& a) {
    std::string out = "[";
    for (int i = 0; i < a.size(); ++i) {
        if (i) out += ',';
        out += to_json(a[i]);
    }
    return out + "]";
}

std::string capabilities_json(const google::protobuf::RepeatedPtrField<std::string>& caps) {
    return detail::string_array_json(caps);
}

std::string health_name(cv1::NodeHealth h) { return cv1::NodeHealth_Name(h); }

cv1::NodeHealth parse_health(const Result& r, int row, int col) {
    if (r.null(row, col)) return cv1::NODE_HEALTH_UNSPECIFIED;
    cv1::NodeHealth h = cv1::NODE_HEALTH_UNSPECIFIED;
    if (!cv1::NodeHealth_Parse(r.str(row, col), &h)) throw DbError("unknown health '" + r.str(row, col) + "'");
    return h;
}

cv1::CampaignState parse_state(const Result& r, int row, int col) {
    cv1::CampaignState s = cv1::CAMPAIGN_STATE_UNSPECIFIED;
    if (!cv1::CampaignState_Parse(r.str(row, col), &s)) throw DbError("unknown campaign state '" + r.str(row, col) + "'");
    return s;
}

std::string json_or_empty_object(const std::string& s) { return s.empty() ? "{}" : s; }

// SQL fragments. $n is a unix-ms bigint parameter.
#define TS(n) "to_timestamp($" #n "::bigint / 1000.0)"
#define MS(col) "(extract(epoch FROM " col ") * 1000)::bigint"

constexpr const char* kNodeSelect =
    "SELECT n.node_id, n.name, n.health, n.addresses::text, n.capabilities::text, n.daemon_version,"
    " n.physical::text, n.configured::text, n.cert_fingerprint, " MS("n.registered_at") ", " MS("n.last_seen") ","
    " n.offline_reason, n.last_heartbeat::text,"
    " p.version, p.profile, p.max_cpu_cores, p.max_memory_bytes::text, p.max_jobs, p.max_disk_bytes::text,"
    " p.max_gpus, " MS("p.updated_at") ", p.updated_by"
    " FROM nodes n JOIN node_policies p ON p.node_id = n.node_id ";

cv1::ResourcePolicy policy_from(const Result& r, int row, int c) {
    cv1::ResourcePolicy p;
    p.set_version(r.u64(row, c + 0));
    p.set_profile(r.str(row, c + 1));
    p.set_max_cpu_cores(static_cast<std::uint32_t>(r.u64(row, c + 2)));
    p.set_max_memory_bytes(r.u64(row, c + 3));
    p.set_max_jobs(static_cast<std::uint32_t>(r.u64(row, c + 4)));
    p.set_max_disk_bytes(r.u64(row, c + 5));
    p.set_max_gpus(static_cast<std::uint32_t>(r.u64(row, c + 6)));
    p.set_updated_unix_ms(r.i64(row, c + 7));
    p.set_updated_by(r.str(row, c + 8));
    return p;
}

constexpr const char* kPolicyCols =
    "version, profile, max_cpu_cores, max_memory_bytes::text, max_jobs, max_disk_bytes::text, max_gpus, " MS(
        "updated_at") ", updated_by";

cv1::NodeInfo node_from(const Result& r, int row) {
    cv1::NodeInfo n;
    n.set_node_id(r.str(row, 0));
    n.set_name(r.str(row, 1));
    n.set_health(parse_health(r, row, 2));
    cv1::RegisterRequest carrier;
    from_json("{\"addresses\":" + r.str(row, 3) + ",\"capabilities\":" + r.str(row, 4) + "}", &carrier);
    *n.mutable_addresses() = std::move(*carrier.mutable_addresses());
    *n.mutable_capabilities() = std::move(*carrier.mutable_capabilities());
    n.set_daemon_version(r.str(row, 5));
    from_json(r.str(row, 6), n.mutable_physical());
    from_json(r.str(row, 7), n.mutable_configured());
    n.set_cert_fingerprint(r.str(row, 8));
    n.set_registered_unix_ms(r.i64(row, 9));
    n.set_last_seen_unix_ms(r.i64(row, 10));
    n.set_offline_reason(r.str(row, 11));
    if (!r.null(row, 12)) from_json(r.str(row, 12), n.mutable_last_heartbeat());
    *n.mutable_policy() = policy_from(r, row, 13);
    return n;
}

constexpr const char* kCampaignCols =
    "campaign_id, name, description, domain, state, parameters::text, created_by, " MS("created_at") ", " MS(
        "updated_at");

cv1::Campaign campaign_from(const Result& r, int row) {
    cv1::Campaign c;
    c.set_campaign_id(r.str(row, 0));
    c.set_name(r.str(row, 1));
    c.set_description(r.str(row, 2));
    c.set_domain(r.str(row, 3));
    c.set_state(parse_state(r, row, 4));
    c.set_parameters_json(r.str(row, 5));
    c.set_created_by(r.str(row, 6));
    c.set_created_unix_ms(r.i64(row, 7));
    c.set_updated_unix_ms(r.i64(row, 8));
    return c;
}

// ---- repository ---------------------------------------------------------------

class PgRepository final : public IRepository {
public:
    explicit PgRepository(const std::string& conninfo) : c_(conninfo), jobs_(c_), wf_(c_, jobs_) {
        jobs_.set_workflows(&wf_);
        pg::migrate(c_);
    }

    cv1::NodeInfo register_node(const cv1::RegisterRequest& req, const std::string& cert_fingerprint,
                                std::int64_t now_ms) override {
        const std::string addrs = addresses_json(req.addresses());
        const std::string caps = capabilities_json(req.capabilities());
        const std::string phys = to_json(req.physical());
        const std::string conf = to_json(req.configured());
        transact(c_, [&] {
            // New name: insert the node + default policy. A concurrent insert of
            // the same name waits on the unique index and then sees the conflict.
            auto ins = c_.exec(
                "INSERT INTO nodes (node_id, name, health, addresses, capabilities, daemon_version, physical,"
                " configured, cert_fingerprint, registered_at, last_seen, offline_reason)"
                " VALUES ($1, $2, 'ONLINE', $3::jsonb, $4::jsonb, $5, $6::jsonb, $7::jsonb, $8, " TS(9) ", " TS(
                    9) ", '')"
                " ON CONFLICT (name) DO NOTHING RETURNING node_id",
                short_id("ff-node-"), req.name(), addrs, caps, req.daemon_version(), phys, conf, cert_fingerprint,
                now_ms);
            if (ins.rows() == 1) {
                const std::string id = ins.str(0, 0);
                c_.exec("INSERT INTO node_policies (node_id, updated_at, updated_by) VALUES ($1, " TS(2) ", '')", id,
                        now_ms);
                add_event(id, std::nullopt, "ONLINE", "registered", now_ms);
                return;
            }
            auto old = c_.exec("SELECT node_id, health FROM nodes WHERE name = $1 FOR UPDATE", req.name());
            if (old.rows() != 1) throw DbError("register: node vanished during upsert");
            const std::string id = old.str(0, 0);
            c_.exec(
                "UPDATE nodes SET health = 'ONLINE', addresses = $2::jsonb, capabilities = $3::jsonb,"
                " daemon_version = $4, physical = $5::jsonb, configured = $6::jsonb, cert_fingerprint = $7,"
                " last_seen = " TS(8) ", offline_reason = '' WHERE node_id = $1",
                id, addrs, caps, req.daemon_version(), phys, conf, cert_fingerprint, now_ms);
            if (old.str(0, 1) != "ONLINE") add_event(id, old.str(0, 1), "ONLINE", "registered", now_ms);
        });
        auto n = find_node_by_name(req.name());
        if (!n) throw DbError("register: node missing after upsert");
        return *n;
    }

    std::optional<cv1::NodeInfo> get_node(const std::string& node_id) override {
        return one_node(std::string(kNodeSelect) + "WHERE n.node_id = $1", node_id);
    }

    std::optional<cv1::NodeInfo> find_node_by_name(const std::string& name) override {
        return one_node(std::string(kNodeSelect) + "WHERE n.name = $1", name);
    }

    std::vector<cv1::NodeInfo> list_nodes(cv1::NodeHealth health) override {
        return c_.retry([&] {
            const std::optional<std::string> h =
                health == cv1::NODE_HEALTH_UNSPECIFIED ? std::nullopt : std::optional(health_name(health));
            auto r = c_.exec(std::string(kNodeSelect) + "WHERE ($1::text IS NULL OR n.health = $1) ORDER BY n.name",
                             h);
            std::vector<cv1::NodeInfo> out;
            out.reserve(static_cast<std::size_t>(r.rows()));
            for (int i = 0; i < r.rows(); ++i) out.push_back(node_from(r, i));
            return out;
        });
    }

    bool record_heartbeat(const cv1::Heartbeat& hb, std::int64_t now_ms) override {
        const std::string json = to_json(hb);
        const std::optional<std::string> addrs =
            hb.addresses().empty() ? std::nullopt : std::optional(addresses_json(hb.addresses()));
        return transact(c_, [&] {
            auto r = c_.exec(
                "UPDATE nodes n SET last_heartbeat = $2::jsonb, addresses = coalesce($3::jsonb, n.addresses),"
                " daemon_version = CASE WHEN $4 = '' THEN n.daemon_version ELSE $4 END,"
                " last_seen = " TS(5) ", health = 'ONLINE', offline_reason = ''"
                " FROM (SELECT node_id, health FROM nodes WHERE node_id = $1 FOR UPDATE) o"
                " WHERE n.node_id = o.node_id RETURNING o.health",
                hb.node_id(), json, addrs, hb.daemon_version(), now_ms);
            if (r.rows() != 1) return false;
            if (r.str(0, 0) != "ONLINE") add_event(hb.node_id(), r.str(0, 0), "ONLINE", "heartbeat", now_ms);
            return true;
        });
    }

    std::optional<cv1::ResourcePolicy> set_policy(const std::string& node_id, const cv1::ResourcePolicy& p,
                                                  const std::string& actor, std::int64_t now_ms) override {
        return transact(c_, [&]() -> std::optional<cv1::ResourcePolicy> {
            auto r = c_.exec(std::string("UPDATE node_policies SET version = version + 1, profile = $2,"
                                         " max_cpu_cores = $3::bigint, max_memory_bytes = $4::numeric,"
                                         " max_jobs = $5::bigint, max_disk_bytes = $6::numeric, max_gpus = $7::bigint,"
                                         " updated_at = " TS(8) ", updated_by = $9 WHERE node_id = $1 RETURNING ") +
                                 kPolicyCols,
                             node_id, p.profile(), p.max_cpu_cores(), p.max_memory_bytes(), p.max_jobs(),
                             p.max_disk_bytes(), p.max_gpus(), now_ms, actor);
            if (r.rows() != 1) return std::nullopt;
            return policy_from(r, 0, 0);
        });
    }

    bool set_offline(const std::string& node_id, const std::string& reason, std::int64_t now_ms) override {
        return transact(c_, [&] {
            auto r = c_.exec(
                "UPDATE nodes n SET health = 'OFFLINE', offline_reason = $2"
                " FROM (SELECT node_id, health FROM nodes WHERE node_id = $1 FOR UPDATE) o"
                " WHERE n.node_id = o.node_id RETURNING o.health",
                node_id, reason);
            if (r.rows() != 1) return false;
            if (r.str(0, 0) != "OFFLINE") add_event(node_id, r.str(0, 0), "OFFLINE", reason, now_ms);
            return true;
        });
    }

    std::vector<HealthChange> sweep_health(std::int64_t now_ms, std::int64_t stale_after_ms,
                                           std::int64_t offline_after_ms) override {
        // OFFLINE first so a node past both thresholds goes straight there.
        // One statement per transition: lock candidates, update, log events.
        static constexpr const char* kSweep =
            "WITH o AS (SELECT node_id, health FROM nodes WHERE health = ANY ($2::text[])"
            "   AND last_seen < to_timestamp(($3::bigint - $4::bigint) / 1000.0) FOR UPDATE),"
            " u AS (UPDATE nodes n SET health = $1,"
            "   offline_reason = CASE WHEN $1 = 'OFFLINE' THEN $5 ELSE n.offline_reason END"
            "   FROM o WHERE n.node_id = o.node_id RETURNING n.node_id, o.health AS from_health),"
            " e AS (INSERT INTO node_events (node_id, at, from_health, to_health, reason)"
            "   SELECT node_id, " TS(3) ", from_health, $1, $5 FROM u)"
            " SELECT node_id, from_health FROM u ORDER BY node_id";
        return transact(c_, [&] {
            std::vector<HealthChange> out;
            auto run = [&](cv1::NodeHealth to, const char* from_set, std::int64_t after, const char* reason) {
                auto r = c_.exec(kSweep, health_name(to), from_set, now_ms, after, reason);
                for (int i = 0; i < r.rows(); ++i)
                    out.push_back(HealthChange{r.str(i, 0), parse_health(r, i, 1), to, reason});
            };
            run(cv1::OFFLINE, "{ONLINE,STALE}", offline_after_ms, "heartbeat timeout");
            run(cv1::STALE, "{ONLINE}", stale_after_ms, "heartbeat stale");
            return out;
        });
    }

    std::vector<cv1::NodeEvent> node_events(const std::string& node_id, std::size_t limit) override {
        return c_.retry([&] {
            auto r = c_.exec("SELECT " MS("at") ", from_health, to_health, reason FROM node_events"
                             " WHERE node_id = $1 ORDER BY at DESC, id DESC LIMIT $2::bigint",
                             node_id, clamp_limit(limit));
            std::vector<cv1::NodeEvent> out;
            for (int i = 0; i < r.rows(); ++i) {
                cv1::NodeEvent e;
                e.set_at_unix_ms(r.i64(i, 0));
                e.set_from(parse_health(r, i, 1));
                e.set_to(parse_health(r, i, 2));
                e.set_reason(r.str(i, 3));
                out.push_back(std::move(e));
            }
            return out;
        });
    }

    std::expected<cv1::Campaign, std::string> create_campaign(const cv1::CreateCampaignRequest& req,
                                                              const std::string& actor, std::int64_t now_ms) override {
        try {
            return transact(c_, [&] {
                auto r = c_.exec(std::string("INSERT INTO campaigns (campaign_id, name, description, domain, state,"
                                             " parameters, created_by, created_at, updated_at)"
                                             " VALUES ($1, $2, $3, $4, 'DRAFT', $5::jsonb, $6, " TS(7) ", " TS(
                                                 7) ") RETURNING ") +
                                     kCampaignCols,
                                 short_id("camp-"), req.name(), req.description(), req.domain(),
                                 json_or_empty_object(req.parameters_json()), actor, now_ms);
                return campaign_from(r, 0);
            });
        } catch (const DbError& e) {
            if (e.sqlstate == "23505") return std::unexpected("campaign name already exists");
            throw;
        }
    }

    std::optional<cv1::Campaign> get_campaign(const std::string& campaign_id) override {
        return c_.retry([&]() -> std::optional<cv1::Campaign> {
            auto r = c_.exec(std::string("SELECT ") + kCampaignCols + " FROM campaigns WHERE campaign_id = $1",
                             campaign_id);
            if (r.rows() != 1) return std::nullopt;
            return campaign_from(r, 0);
        });
    }

    std::vector<cv1::Campaign> list_campaigns(cv1::CampaignState state) override {
        return c_.retry([&] {
            const std::optional<std::string> s = state == cv1::CAMPAIGN_STATE_UNSPECIFIED
                                                     ? std::nullopt
                                                     : std::optional(cv1::CampaignState_Name(state));
            auto r = c_.exec(std::string("SELECT ") + kCampaignCols +
                                 " FROM campaigns WHERE ($1::text IS NULL OR state = $1)"
                                 " ORDER BY created_at DESC, campaign_id DESC",
                             s);
            std::vector<cv1::Campaign> out;
            for (int i = 0; i < r.rows(); ++i) out.push_back(campaign_from(r, i));
            return out;
        });
    }

    bool update_campaign(const cv1::Campaign& u, cv1::CampaignState expected_state, std::int64_t now_ms) override {
        if (u.state() == cv1::CAMPAIGN_STATE_UNSPECIFIED || expected_state == cv1::CAMPAIGN_STATE_UNSPECIFIED)
            return false;
        return transact(c_, [&] {
            auto r = c_.exec(
                "UPDATE campaigns SET description = $2, state = $3, parameters = $4::jsonb, updated_at = " TS(5)
                " WHERE campaign_id = $1 AND state = $6",
                u.campaign_id(), u.description(), cv1::CampaignState_Name(u.state()),
                json_or_empty_object(u.parameters_json()), now_ms, cv1::CampaignState_Name(expected_state));
            return r.affected() == 1;
        });
    }

    // ---- jobs (jobs.cpp) ------------------------------------------------------

    std::expected<cv1::Job, std::string> create_job(const tv1::JobSpec& spec, const std::string& submitted_by,
                                                    std::int64_t now_ms) override {
        return jobs_.create_job(spec, submitted_by, now_ms);
    }
    std::optional<cv1::Job> get_job(const std::string& job_id, bool with_attempts) override {
        return jobs_.get_job(job_id, with_attempts);
    }
    std::vector<cv1::Job> list_jobs(tv1::JobState state, const std::string& campaign_id, std::size_t limit) override {
        return jobs_.list_jobs(state, campaign_id, limit);
    }
    CancelOutcome cancel_job(const std::string& job_id, const std::string& reason, std::int64_t now_ms) override {
        return jobs_.cancel_job(job_id, reason, now_ms);
    }
    std::vector<cv1::Lease> lease_jobs(const cv1::LeaseRequest& req, std::int64_t ttl_ms, std::int64_t locality_wait_ms,
                                       std::int64_t now_ms) override {
        return jobs_.lease_jobs(req, ttl_ms, locality_wait_ms, now_ms);
    }
    UpdateOutcome update_job(const std::string& job_id, const std::string& node_id, const cv1::JobUpdate& update,
                             std::int64_t now_ms) override {
        return jobs_.update_job(job_id, node_id, update, now_ms);
    }
    std::vector<cv1::LeaseRef> renew_leases(const std::string& node_id, const std::vector<cv1::LeaseRef>& leases,
                                            std::int64_t ttl_ms, std::int64_t now_ms) override {
        return jobs_.renew_leases(node_id, leases, ttl_ms, now_ms);
    }
    std::vector<ExpiredLease> expire_leases(std::int64_t now_ms) override { return jobs_.expire_leases(now_ms); }
    bool announce_artifacts(const cv1::ArtifactAnnouncement& a, std::int64_t now_ms) override {
        return jobs_.announce_artifacts(a, now_ms);
    }
    std::optional<cv1::ArtifactInfo> get_artifact(const std::string& artifact_id) override {
        return jobs_.get_artifact(artifact_id);
    }
    cv1::ReleaseReply release_artifact(const std::string& artifact_id, const std::string& node_id, bool force,
                                       std::int64_t now_ms) override {
        return jobs_.release_artifact(artifact_id, node_id, force, now_ms);
    }
    cv1::ArtifactInfo set_artifact_pinned(const std::string& artifact_id, bool pinned, const std::string& actor,
                                          std::int64_t now_ms) override {
        return jobs_.set_artifact_pinned(artifact_id, pinned, actor, now_ms);
    }

    // ---- workflows (workflows.cpp) ----------------------------------------------

    std::expected<cv1::Workflow, std::string> create_workflow(const cv1::CreateWorkflowRequest& req,
                                                              const std::string& submitted_by,
                                                              std::int64_t now_ms) override {
        return wf_.create_workflow(req, submitted_by, now_ms);
    }
    std::optional<cv1::Workflow> get_workflow(const std::string& id, bool with_steps) override {
        return wf_.get_workflow(id, with_steps);
    }
    std::vector<cv1::Workflow> list_workflows(const std::string& state, const std::string& campaign_id,
                                              std::size_t limit) override {
        return wf_.list_workflows(state, campaign_id, limit);
    }
    std::optional<cv1::Workflow> cancel_workflow(const std::string& id, const std::string& reason,
                                                 std::int64_t now_ms) override {
        return wf_.cancel_workflow(id, reason, now_ms);
    }
    std::size_t sweep_workflows(std::int64_t now_ms) override { return wf_.sweep_workflows(now_ms); }
    void set_workflow_limits(const WorkflowLimits& limits) override { wf_.set_limits(limits); }
    void set_gang_options(const GangOptions& options) override {
        jobs_.set_gang_options(options);
        wf_.set_gang_options(options);
    }
    std::vector<Reservation> place_jobs(const PlacementOptions& options, std::int64_t now_ms) override {
        return jobs_.place_jobs(options, now_ms);
    }

    void audit(const std::string& actor, const std::string& action, const std::string& target,
               const std::string& details_json, std::int64_t now_ms) override {
        transact(c_, [&] {
            c_.exec("INSERT INTO audit_log (at, actor, action, target, details) VALUES (" TS(1) ", $2, $3, $4, $5::jsonb)",
                    now_ms, actor, action, target, json_or_empty_object(details_json));
        });
    }

    std::vector<cv1::AuditEntry> list_audit(std::size_t limit) override {
        return c_.retry([&] {
            auto r = c_.exec("SELECT " MS("at") ", actor, action, target, details::text FROM audit_log"
                             " ORDER BY at DESC, id DESC LIMIT $1::bigint",
                             clamp_limit(limit));
            std::vector<cv1::AuditEntry> out;
            for (int i = 0; i < r.rows(); ++i) {
                cv1::AuditEntry a;
                a.set_at_unix_ms(r.i64(i, 0));
                a.set_actor(r.str(i, 1));
                a.set_action(r.str(i, 2));
                a.set_target(r.str(i, 3));
                a.set_details_json(r.str(i, 4));
                out.push_back(std::move(a));
            }
            return out;
        });
    }

private:
    void add_event(const std::string& node_id, std::optional<std::string> from, const std::string& to,
                   const std::string& reason, std::int64_t now_ms) {
        c_.exec("INSERT INTO node_events (node_id, at, from_health, to_health, reason) VALUES ($1, " TS(2) ", $3, $4, $5)",
                node_id, now_ms, std::move(from), to, reason);
    }

    std::optional<cv1::NodeInfo> one_node(const std::string& sql, const std::string& key) {
        return c_.retry([&]() -> std::optional<cv1::NodeInfo> {
            auto r = c_.exec(sql, key);
            if (r.rows() != 1) return std::nullopt;
            return node_from(r, 0);
        });
    }

    Conn c_;
    JobStore jobs_;
    WorkflowStore wf_;
};

#undef TS
#undef MS

}  // namespace

std::unique_ptr<IRepository> open_pg_repository(const std::string& conninfo) {
    return std::make_unique<PgRepository>(conninfo);
}

int schema_version(const std::string& conninfo) {
    pg::Conn c(conninfo);
    return pg::current_version(c);
}

}  // namespace tdr::cpdb
