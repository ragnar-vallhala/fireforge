// Job queue, leases and artifact locations on PostgreSQL (docs/design/phase4.md §2-§4).
// Internal: PgRepository forwards the IRepository job methods here.
#pragma once

#include "pg.hpp"
#include "tdr/cpdb/db.hpp"

namespace tdr::cpdb {

class WorkflowStore;

class JobStore {
public:
    explicit JobStore(pg::Conn& c) : c_(c) {}
    // The workflow engine runs inside job transitions (phase6.md §4).
    void set_workflows(WorkflowStore* wf) { wf_ = wf; }

    std::expected<cv1::Job, std::string> create_job(const tv1::JobSpec& spec, const std::string& submitted_by,
                                                    std::int64_t now_ms);
    std::optional<cv1::Job> get_job(const std::string& job_id, bool with_attempts);
    std::vector<cv1::Job> list_jobs(tv1::JobState state, const std::string& campaign_id, std::size_t limit);
    CancelOutcome cancel_job(const std::string& job_id, const std::string& reason, std::int64_t now_ms);
    std::vector<cv1::Lease> lease_jobs(const cv1::LeaseRequest& req, std::int64_t ttl_ms, std::int64_t locality_wait_ms,
                                       std::int64_t now_ms);
    UpdateOutcome update_job(const std::string& job_id, const std::string& node_id, const cv1::JobUpdate& update,
                             std::int64_t now_ms);
    std::vector<cv1::LeaseRef> renew_leases(const std::string& node_id, const std::vector<cv1::LeaseRef>& leases,
                                            std::int64_t ttl_ms, std::int64_t now_ms);
    std::vector<ExpiredLease> expire_leases(std::int64_t now_ms);
    bool announce_artifacts(const cv1::ArtifactAnnouncement& a, std::int64_t now_ms);
    std::optional<cv1::ArtifactInfo> get_artifact(const std::string& artifact_id);
    cv1::ReleaseReply release_artifact(const std::string& artifact_id, const std::string& node_id, bool force,
                                       std::int64_t now_ms);
    cv1::ArtifactInfo set_artifact_pinned(const std::string& artifact_id, bool pinned, const std::string& actor,
                                          std::int64_t now_ms);

    std::vector<Reservation> place_jobs(const PlacementOptions& options, std::int64_t now_ms);

    // ---- inside the caller's transaction ----------------------------------------
    // Inserts a QUEUED job (spec.job_id set if empty; workflow_id/step columns from
    // the spec) and computes jobs.strict. Throws DbError (23505 = duplicate id).
    std::string insert_job(tv1::JobSpec& spec, const std::string& submitted_by, std::int64_t now_ms);
    std::optional<cv1::Job> load_job(const std::string& job_id, bool with_attempts);

private:
    std::optional<cv1::ArtifactInfo> load_artifact(const std::string& artifact_id);
    void lock_artifact(const std::string& artifact_id);
    void upsert_locations(const std::string& node_id, const std::string& json, const char* key,
                          const std::string& sync_id, std::int64_t now_ms);

    // jobs.strict from gpu_count and input + checkpoint bytes (phase6.md §7).
    void refresh_strict(const std::string& job_id);

    pg::Conn& c_;
    WorkflowStore* wf_ = nullptr;
    // Threshold for jobs.strict; updated by every place_jobs call.
    std::uint64_t strict_bytes_ = PlacementOptions{}.strict_input_bytes;
};

}  // namespace tdr::cpdb
