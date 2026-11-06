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
#include "fireforge/tdr/v1/job.pb.h"

namespace tdr::cpdb {

namespace cv1 = ::fireforge::control::v1;
namespace tv1 = ::fireforge::tdr::v1;

struct DbError : std::runtime_error {
    DbError(const std::string& msg, std::string state = {}) : std::runtime_error(msg), sqlstate(std::move(state)) {}
    std::string sqlstate;  // e.g. "23505" unique_violation
};

// ---- jobs (docs/design/phase4.md §2-§4) ---------------------------------------

enum class CancelResult { kCancelled, kRequested, kTerminal, kNotFound };
struct CancelOutcome {
    CancelResult result = CancelResult::kNotFound;
    std::optional<cv1::Job> job;   // set unless kNotFound
};

enum class UpdateResult { kOk, kStale, kNotFound, kInvalid };
struct UpdateOutcome {
    UpdateResult result = UpdateResult::kNotFound;
    std::optional<cv1::Job> job;   // set for kOk (job after the update)
    std::string message;           // why, for kStale / kInvalid
};

struct ExpiredLease {
    std::string job_id;
    std::string attempt_id;
    std::string node_id;
    tv1::JobState new_state = tv1::JOB_STATE_UNSPECIFIED;  // QUEUED (retry) | FAILED | CANCELLED
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

    // ---- jobs ----------------------------------------------------------------
    // Inserts a QUEUED job. spec.job_id empty -> "job-<16 hex>"; set -> must be
    // unique ("job id already exists"). Caller has validated the spec and filled
    // policy.max_attempts. The stored spec has job_id set.
    virtual std::expected<cv1::Job, std::string> create_job(const tv1::JobSpec& spec, const std::string& submitted_by,
                                                            std::int64_t now_ms) = 0;
    // with_attempts fills Job.attempts (oldest first).
    virtual std::optional<cv1::Job> get_job(const std::string& job_id, bool with_attempts) = 0;
    // Newest first. state UNSPECIFIED = all; campaign_id "" = all.
    virtual std::vector<cv1::Job> list_jobs(tv1::JobState state, const std::string& campaign_id, std::size_t limit) = 0;
    // QUEUED/RETRY_PENDING -> CANCELLED (kCancelled). LEASED/RUNNING -> set
    // cancel_requested (kRequested; the holder is told via heartbeat revoked).
    // Terminal -> kTerminal (no change).
    virtual CancelOutcome cancel_job(const std::string& job_id, const std::string& reason, std::int64_t now_ms) = 0;

    // One transaction (SELECT ... FOR UPDATE SKIP LOCKED, oldest QUEUED first,
    // scanning at most 200). Grants greedily while the job fits the remaining
    // req.free (cpu_cores: 0 counts as 1; memory_bytes; gpu_count) and
    // req.max_jobs. Eligibility (§3): campaign absent or ACTIVE, capabilities
    // subset of the node's, every input artifact located on the node, node
    // ONLINE (OFFLINE/STALE/unknown -> no leases). Each grant: new attempt
    // (number = attempts_used + 1, fencing_token = number), job LEASED,
    // current_attempt_id/node_id set, expires = now + ttl.
    virtual std::vector<cv1::Lease> lease_jobs(const cv1::LeaseRequest& req, std::int64_t ttl_ms, std::int64_t now_ms) = 0;

    // Fenced status update from node_id (§2). kStale if the job's current attempt
    // is not (attempt_id, fencing_token), the attempt belongs to another node, or
    // the job is not LEASED/RUNNING. Transitions:
    //   RUNNING            LEASED -> RUNNING (or progress update while RUNNING)
    //   COMPLETED          -> COMPLETED; attempt SUCCEEDED; outputs stored and
    //                         artifact_locations recorded for node_id
    //   FAILED             attempt FAILED; retryable && attempts_used < max_attempts
    //                         && !cancel_requested -> QUEUED (via RETRY_PENDING), else FAILED
    //   CANCELLED          attempt CANCELLED; job CANCELLED
    //   EXPIRED / VALIDATION_FAILED   attempt FAILED; job takes that state (terminal)
    // Any other state -> kInvalid.
    virtual UpdateOutcome update_job(const std::string& job_id, const std::string& node_id, const cv1::JobUpdate& update,
                                     std::int64_t now_ms) = 0;

    // Extends expires = now + ttl for each ref that is still the current attempt
    // of a LEASED/RUNNING job held by node_id and not cancel_requested. Returns
    // the rest (revoked), in input order.
    virtual std::vector<cv1::LeaseRef> renew_leases(const std::string& node_id, const std::vector<cv1::LeaseRef>& leases,
                                                    std::int64_t ttl_ms, std::int64_t now_ms) = 0;

    // Attempts LEASED/RUNNING with expires < now: attempt ABANDONED ("lease
    // expired"); job -> CANCELLED if cancel_requested, else QUEUED if
    // attempts_used < max_attempts, else FAILED ("lease expired").
    virtual std::vector<ExpiredLease> expire_leases(std::int64_t now_ms) = 0;

    // ---- artifact locations ---------------------------------------------------
    // Upserts (artifact_id, node_id) rows. sync_id/full as in ArtifactAnnouncement.
    // False if the node does not exist.
    virtual bool announce_artifacts(const cv1::ArtifactAnnouncement& a, std::int64_t now_ms) = 0;
    // nullopt if no node holds it.
    virtual std::optional<cv1::ArtifactInfo> get_artifact(const std::string& artifact_id) = 0;

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
