// Control-plane persistence on PostgreSQL via libpq (docs/design/phase2.md §6).
//
// IRepository is the only thing the API layer sees. All values are bound with
// PQexecParams; SQL text never contains data. Every mutating method is one
// transaction. Errors throw DbError (message includes the SQLSTATE).
#pragma once

#include <cstdint>
#include <expected>
#include <filesystem>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

#include "fireforge/control/v1/control.pb.h"

namespace tdr::cpdb {

namespace cv1 = ::fireforge::control::v1;

struct DbError : std::runtime_error {
    DbError(const std::string& msg, std::string state = {}) : std::runtime_error(msg), sqlstate(std::move(state)) {}
    std::string sqlstate;  // e.g. "23505" unique_violation
};

struct HealthChange {
    std::string node_id;
    cv1::NodeHealth from = cv1::NODE_HEALTH_UNSPECIFIED;
    cv1::NodeHealth to = cv1::NODE_HEALTH_UNSPECIFIED;
    std::string reason;
};

class IRepository {
public:
    virtual ~IRepository() = default;

    // ---- nodes ---------------------------------------------------------------
    // Insert (new node_id "ff-node-<16 hex>", policy version 1, all limits 0)
    // or update the existing node with this name (keeps node_id and policy).
    // Sets health ONLINE, last_seen = now, clears offline_reason, records a
    // node_event if health changed. Returns the stored node (with policy).
    virtual cv1::NodeInfo register_node(const cv1::RegisterRequest& req, const std::string& cert_fingerprint,
                                        std::int64_t now_ms) = 0;
    virtual std::optional<cv1::NodeInfo> get_node(const std::string& node_id) = 0;
    virtual std::optional<cv1::NodeInfo> find_node_by_name(const std::string& name) = 0;
    // Ordered by name. health UNSPECIFIED = all.
    virtual std::vector<cv1::NodeInfo> list_nodes(cv1::NodeHealth health) = 0;

    // Stores the heartbeat (last_heartbeat, addresses if non-empty), last_seen = now,
    // health ONLINE (+ event if changed). False if the node does not exist.
    virtual bool record_heartbeat(const cv1::Heartbeat& hb, std::int64_t now_ms) = 0;

    // Replaces the node's policy limits/profile, version = old + 1, updated_by/at set.
    // Returns the new policy, or nullopt if the node does not exist.
    virtual std::optional<cv1::ResourcePolicy> set_policy(const std::string& node_id, const cv1::ResourcePolicy& limits,
                                                          const std::string& actor, std::int64_t now_ms) = 0;

    // health OFFLINE, offline_reason = reason (+ event). False if no such node.
    virtual bool set_offline(const std::string& node_id, const std::string& reason, std::int64_t now_ms) = 0;

    // ONLINE nodes with last_seen < now - stale_after -> STALE; ONLINE/STALE with
    // last_seen < now - offline_after -> OFFLINE (reason "heartbeat timeout").
    // Records events; returns the changes made.
    virtual std::vector<HealthChange> sweep_health(std::int64_t now_ms, std::int64_t stale_after_ms,
                                                   std::int64_t offline_after_ms) = 0;
    // Newest first.
    virtual std::vector<cv1::NodeEvent> node_events(const std::string& node_id, std::size_t limit) = 0;

    // ---- campaigns -----------------------------------------------------------
    // New campaign_id "camp-<16 hex>", state DRAFT. Unique name violation ->
    // std::unexpected("campaign name already exists").
    virtual std::expected<cv1::Campaign, std::string> create_campaign(const cv1::CreateCampaignRequest& req,
                                                                      const std::string& actor, std::int64_t now_ms) = 0;
    virtual std::optional<cv1::Campaign> get_campaign(const std::string& campaign_id) = 0;
    // Newest first. state UNSPECIFIED = all.
    virtual std::vector<cv1::Campaign> list_campaigns(cv1::CampaignState state) = 0;
    // Compare-and-set: writes description/state/parameters_json/updated_at from
    // `updated` only if the stored state == expected_state. False otherwise
    // (or if missing). Validation of the transition is the caller's job.
    virtual bool update_campaign(const cv1::Campaign& updated, cv1::CampaignState expected_state, std::int64_t now_ms) = 0;

    // ---- audit ---------------------------------------------------------------
    virtual void audit(const std::string& actor, const std::string& action, const std::string& target,
                       const std::string& details_json, std::int64_t now_ms) = 0;
    virtual std::vector<cv1::AuditEntry> list_audit(std::size_t limit) = 0;  // newest first
};

// Connects (libpq conninfo / URI) and applies pending migrations.
// Throws DbError if the database is unreachable.
std::unique_ptr<IRepository> open_pg_repository(const std::string& conninfo);

// Current schema version after migrations (for tests / logging).
int schema_version(const std::string& conninfo);

}  // namespace tdr::cpdb
