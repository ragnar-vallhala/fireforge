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
#include "fireforge/control/v1/materials.pb.h"
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

// Phase 6 limits (tdrcp [workflows]).
struct WorkflowLimits {
    std::size_t max_steps = 10000;   // per workflow, static + spawned
    std::size_t max_spawn = 1000;    // per expansion
    std::size_t max_depth = 8;       // spawn nesting
};

// Phase 6 ranked placement (tdrcp [placement], phase6.md §7).
struct PlacementOptions {
    std::int64_t reserve_window_ms = 5000;
    std::int64_t max_reserve_ms = 30000;        // since queued_at; then anyone may lease
    std::uint64_t strict_input_bytes = 64ull << 20;
    double w_compute = 0.1, w_transfer = 0.5, w_queue = 0.2, w_memory = 0.1, w_contention = 0.1;
};

// Phase 7 gangs (docs/design/phase7.md §3, §7: tdrcp [gangs]).
struct GangOptions {
    std::int64_t drain_timeout_ms = 600000;  // a hold older than this is dropped and re-picked
    std::int64_t ready_window_ms = 5000;     // every member ready within this -> gang lease
    std::uint32_t max_nodes = 16;
    std::uint32_t max_ranks = 4096;
    std::int64_t poll_recent_ms = 30000;     // eligible only if it polled lease_jobs this recently
};

struct Reservation {
    std::string job_id;
    std::string node_id;
    double cost = 0;
    std::int64_t until_ms = 0;
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
    // subset of the node's, node ONLINE (OFFLINE/STALE/unknown -> no leases), and
    // locality (phase5.md §4): every input located on the node, OR
    // now - queued_at >= locality_wait_ms and every input located on at least
    // one ONLINE node. Holders are not otherwise prioritised. Each grant: new attempt
    // (number = attempts_used + 1, fencing_token = number), job LEASED,
    // current_attempt_id/node_id set, expires = now + ttl.
    virtual std::vector<cv1::Lease> lease_jobs(const cv1::LeaseRequest& req, std::int64_t ttl_ms,
                                               std::int64_t locality_wait_ms, std::int64_t now_ms) = 0;

    // Fenced status update from node_id (§2). kStale if the job's current attempt
    // is not (attempt_id, fencing_token), the attempt belongs to another node, or
    // the job is not LEASED/RUNNING. Transitions:
    //   RUNNING            LEASED -> RUNNING (or progress update while RUNNING)
    //   COMPLETED          -> COMPLETED; attempt SUCCEEDED; outputs stored and
    //                         artifact_locations recorded for node_id; Phase 8:
    //                         update.records stored as calculations in the same
    //                         transaction (see store rules below set_gang_options)
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
    // nullopt if no node holds it and it is not pinned. Fills pinned.
    virtual std::optional<cv1::ArtifactInfo> get_artifact(const std::string& artifact_id) = 0;

    // ---- Phase 5 (docs/design/phase5.md §4, §6) ----------------------------------
    // One transaction, locking the artifact's location rows (FOR UPDATE). force:
    // delete node_id's row, released. Else: pinned -> {false, "pinned"}; no row for
    // another node that isn't OFFLINE -> {false, "last copy"}; otherwise delete node_id's row (if any)
    // -> released. Two nodes releasing the last two copies concurrently: exactly
    // one succeeds.
    virtual cv1::ReleaseReply release_artifact(const std::string& artifact_id, const std::string& node_id, bool force,
                                               std::int64_t now_ms) = 0;
    // Pinned artifacts are never released (without force). Pinning an artifact
    // with no locations is allowed (it pins future copies). Returns the info after.
    virtual cv1::ArtifactInfo set_artifact_pinned(const std::string& artifact_id, bool pinned, const std::string& actor,
                                                  std::int64_t now_ms) = 0;

    // ---- audit ---------------------------------------------------------------
    // ---- workflows (docs/design/phase6.md §3-§5) ---------------------------------
    // Validates (names, unique, deps exist, acyclic, step limit, campaign exists if
    // set) and stores the workflow; creates jobs for steps with no dependencies
    // (others WAITING). Error string = 400 message.
    virtual std::expected<cv1::Workflow, std::string> create_workflow(const cv1::CreateWorkflowRequest& req,
                                                                      const std::string& submitted_by,
                                                                      std::int64_t now_ms) = 0;
    // with_steps: fill Workflow.steps (creation order). Always fills step_counts.
    virtual std::optional<cv1::Workflow> get_workflow(const std::string& workflow_id, bool with_steps) = 0;
    // Newest first; state "" = all.
    virtual std::vector<cv1::Workflow> list_workflows(const std::string& state, const std::string& campaign_id,
                                                      std::size_t limit) = 0;
    // RUNNING -> CANCELLED: WAITING steps CANCELLED, their jobs cancelled as
    // cancel_job does. nullopt = not found; a finished workflow is returned unchanged.
    virtual std::optional<cv1::Workflow> cancel_workflow(const std::string& workflow_id, const std::string& reason,
                                                         std::int64_t now_ms) = 0;
    // Safety net (§4): re-evaluate RUNNING workflows; returns how many steps changed.
    virtual std::size_t sweep_workflows(std::int64_t now_ms) = 0;
    // The engine also runs inside update_job / expire_leases / cancel_job whenever a
    // workflow step's job becomes terminal (same transaction). update_job applies
    // JobUpdate.spawn (validated against the limits from set_workflow_limits;
    // invalid -> kInvalid, job FAILED non-retryable "invalid spawn: ...", outputs
    // kept) and JobUpdate.checkpoint (RUNNING updates; fenced; stored as
    // Job.checkpoint). lease_jobs sets Lease.spec.resume_checkpoint from it, skips
    // jobs reserved for another node until reserved_until, and orders jobs
    // reserved for the caller first.
    virtual void set_workflow_limits(const WorkflowLimits& limits) = 0;

    // ---- ranked placement (phase6.md §7) ----------------------------------------
    // Reserve QUEUED strict jobs for their best-ranked untried eligible node;
    // release reservations past max_reserve. Returns the reservations made.
    virtual std::vector<Reservation> place_jobs(const PlacementOptions& options, std::int64_t now_ms) = 0;

    // ---- gangs (docs/design/phase7.md §3, §4) -----------------------------------
    // A job is a gang job iff spec.mpi.ranks > 0 (resources are per rank). Gangs are
    // placed by place_jobs (same pass, before strict jobs): slots per node from its
    // heartbeat capacity, members chosen by slots desc then Phase 6 cost, pairwise
    // CONNECTED peers, head = first; holds inserted (gang_holds) and Job.gang /
    // pending_reason updated; holds older than drain_timeout dropped (members ->
    // gang_tried) and re-picked; not coverable -> pending_reason only. Gang
    // reservations are not returned in the Reservation list.
    //
    // lease_jobs (gang parts): records the caller's poll time (eligibility); subtracts
    // other jobs' holds on the caller from req.free before granting ordinary jobs;
    // marks the caller's member ready when req.free covers its hold; when every member
    // is ready within ready_window creates the gang attempt (one attempt id + token,
    // job_attempt_members rows, job LEASED, node_id = head, holds deleted) and returns
    // the caller's Lease with `gang`; undelivered member leases are returned on that
    // member's next call (once). A gang lease uses one job slot of req.max_jobs.
    //
    // update_job from a gang member (fenced by attempt id + token + membership):
    // RUNNING marks the member; a terminal report from the head decides the job as for
    // a normal job and revokes the other members; FAILED / CANCELLED from a non-head
    // member (or any member's lease expiring, in expire_leases) fails the attempt
    // ("member <name> failed: ...", retryable unless the member said otherwise) and
    // revokes the rest; COMPLETED from a non-head member only marks it SUCCEEDED.
    // renew_leases renews per member; a member of an attempt that is no longer
    // current, or of a cancel_requested job, is revoked.
    // Job / JobAttempt protos fill gang / members / pending_reason.
    virtual void set_gang_options(const GangOptions& options) = 0;

    // ---- materials database (docs/design/phase8.md §2-§3) ---------------------------
    //
    // Records from update_job(COMPLETED) and import_calculations are stored as
    // calculations. Per record: the structure is structure_id (must exist) or the
    // inline Structure, deduplicated by fingerprint (required, "fp1:" prefix; a new
    // fingerprint inserts a structure, creating the material for its
    // reduced_formula if needed; elements derived from the sites). kind non-empty
    // (<= 64 chars); properties/conditions/convergence/uncertainty/settings JSON must
    // parse as objects (empty = {}). output_names must name outputs of this update
    // (resolved to artifact ids); job/attempt/workflow/step/campaign/node come from
    // the job. Worker records must be PREDICTED. Limits: <= 64 records and <= 256 KiB
    // of record JSON per update. Any violation makes update_job return kInvalid with
    // the reason and store nothing (the API answers 400; the node fails the job, not
    // retryable). A fenced-out (stale) update stores nothing.
    virtual std::vector<cv1::Material> list_materials(const std::string& formula, const std::string& element,
                                                      const std::string& campaign_id, std::size_t limit) = 0;
    // With structures and the newest calculation per kind.
    virtual std::optional<cv1::Material> get_material(const std::string& material_id) = 0;
    virtual std::optional<cv1::StructureInfo> get_structure(const std::string& structure_id) = 0;
    struct CalcQuery {
        std::string material_id, structure_id, kind, campaign_id, workflow_id;
        std::optional<tv1::Evidence> evidence;
        std::size_t limit = 100;   // capped at 1000
        std::string after;         // calc_id cursor from CalculationList.next; newest first
    };
    virtual cv1::CalculationList list_calculations(const CalcQuery& q) = 0;
    virtual std::optional<cv1::Calculation> get_calculation(const std::string& calc_id) = 0;
    // Admin import: evidence must be LITERATURE or MEASURED; output_names not allowed;
    // same validation and limits as worker records. All or nothing.
    virtual std::expected<cv1::ImportCalculationsReply, std::string> import_calculations(
        const cv1::ImportCalculationsRequest& req, const std::string& actor, std::int64_t now_ms) = 0;

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
