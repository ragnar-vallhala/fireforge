// Job queue, leases and artifact locations on PostgreSQL (docs/design/phase4.md §2-§4).
// Internal: PgRepository forwards the IRepository job methods here.
#pragma once

#include "pg.hpp"
#include "tdr/cpdb/db.hpp"

namespace tdr::cpdb {

class JobStore {
public:
    explicit JobStore(pg::Conn& c) : c_(c) {}

    std::expected<cv1::Job, std::string> create_job(const tv1::JobSpec& spec, const std::string& submitted_by,
                                                    std::int64_t now_ms);
    std::optional<cv1::Job> get_job(const std::string& job_id, bool with_attempts);
    std::vector<cv1::Job> list_jobs(tv1::JobState state, const std::string& campaign_id, std::size_t limit);
    CancelOutcome cancel_job(const std::string& job_id, const std::string& reason, std::int64_t now_ms);
    std::vector<cv1::Lease> lease_jobs(const cv1::LeaseRequest& req, std::int64_t ttl_ms, std::int64_t now_ms);
    UpdateOutcome update_job(const std::string& job_id, const std::string& node_id, const cv1::JobUpdate& update,
                             std::int64_t now_ms);
    std::vector<cv1::LeaseRef> renew_leases(const std::string& node_id, const std::vector<cv1::LeaseRef>& leases,
                                            std::int64_t ttl_ms, std::int64_t now_ms);
    std::vector<ExpiredLease> expire_leases(std::int64_t now_ms);
    bool announce_artifacts(const cv1::ArtifactAnnouncement& a, std::int64_t now_ms);
    std::optional<cv1::ArtifactInfo> get_artifact(const std::string& artifact_id);

private:
    std::optional<cv1::Job> load_job(const std::string& job_id, bool with_attempts);
    void upsert_locations(const std::string& node_id, const std::string& json, const char* key,
                          const std::string& sync_id, std::int64_t now_ms);

    pg::Conn& c_;
};

}  // namespace tdr::cpdb
