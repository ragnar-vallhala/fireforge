// Job state machine, capacity defaults and JobRecord -> JobStatus conversion.
#include "tdr/jobs/jobs.hpp"

namespace tdr::jobs {

bool is_terminal(v1::JobState s) {
    switch (s) {
        case v1::COMPLETED:
        case v1::FAILED:
        case v1::CANCELLED:
        case v1::EXPIRED:
        case v1::VALIDATION_FAILED:
            return true;
        default:
            return false;
    }
}

// Allowed transitions (docs/design/phase1.md section 3, plus LEASED for later
// phases). Beyond the diagram:
//   CREATED -> CANCELLED          (crash between insert and queueing, then cancel)
//   ASSIGNED -> FAILED            (launch failed, no attempts left)
//   COMMITTING -> RETRY_PENDING / FAILED   (store ingest error, or a crash during
//                                           commit found by recovery)
//   RETRY_PENDING -> FAILED       (defensive; recovery of an exhausted job)
bool is_valid_transition(v1::JobState from, v1::JobState to) {
    if (from == to) return false;
    switch (from) {
        case v1::CREATED:
            return to == v1::QUEUED || to == v1::CANCELLED;
        case v1::QUEUED:
            return to == v1::LEASED || to == v1::ASSIGNED || to == v1::CANCELLED;
        case v1::LEASED:
            return to == v1::ASSIGNED || to == v1::QUEUED || to == v1::EXPIRED || to == v1::CANCELLED;
        case v1::ASSIGNED:
            return to == v1::RUNNING || to == v1::RETRY_PENDING || to == v1::FAILED || to == v1::CANCELLED;
        case v1::RUNNING:
            return to == v1::COMMITTING || to == v1::RETRY_PENDING || to == v1::FAILED ||
                   to == v1::EXPIRED || to == v1::CANCELLED;
        case v1::COMMITTING:
            return to == v1::COMPLETED || to == v1::VALIDATION_FAILED || to == v1::RETRY_PENDING ||
                   to == v1::FAILED;
        case v1::RETRY_PENDING:
            return to == v1::QUEUED || to == v1::FAILED || to == v1::CANCELLED;
        default:
            return false;  // terminal states and UNSPECIFIED have no exits
    }
}

const char* to_string(v1::JobState s) {
    switch (s) {
        case v1::JOB_STATE_UNSPECIFIED: return "UNSPECIFIED";
        case v1::CREATED: return "CREATED";
        case v1::QUEUED: return "QUEUED";
        case v1::LEASED: return "LEASED";
        case v1::ASSIGNED: return "ASSIGNED";
        case v1::RUNNING: return "RUNNING";
        case v1::COMMITTING: return "COMMITTING";
        case v1::COMPLETED: return "COMPLETED";
        case v1::FAILED: return "FAILED";
        case v1::RETRY_PENDING: return "RETRY_PENDING";
        case v1::CANCELLED: return "CANCELLED";
        case v1::EXPIRED: return "EXPIRED";
        case v1::VALIDATION_FAILED: return "VALIDATION_FAILED";
        default: return "UNKNOWN";
    }
}

Capacity effective_request(const v1::JobSpec& spec, std::uint64_t default_job_memory_bytes) {
    Capacity c;
    c.cpu_cores = spec.resources().cpu_cores() == 0 ? 1 : spec.resources().cpu_cores();
    c.memory_bytes = spec.resources().memory_bytes() == 0 ? default_job_memory_bytes
                                                          : spec.resources().memory_bytes();
    c.jobs = 1;
    return c;
}

v1::JobStatus to_status(const journal::JobRecord& rec) {
    v1::JobStatus st;
    st.set_job_id(rec.job_id);
    st.set_state(rec.state);
    *st.mutable_spec() = rec.spec;
    st.set_attempts_used(rec.attempts_used);
    st.set_current_attempt_id(rec.current_attempt_id);
    st.set_last_error(rec.last_error);
    for (const auto& a : rec.output_artifacts) st.add_output_artifacts(a);
    st.set_created_unix_ms(rec.created_unix_ms);
    st.set_updated_unix_ms(rec.updated_unix_ms);
    st.set_progress_fraction(rec.progress_fraction);
    st.set_progress_message(rec.progress_message);
    for (const auto& o : rec.outputs) *st.add_outputs() = o;
    if (!rec.checkpoint.artifact_id().empty()) *st.mutable_checkpoint() = rec.checkpoint;
    for (const auto& s : rec.spawn) *st.add_spawn() = s;
    for (const auto& r : rec.records) *st.add_records() = r;
    return st;
}

}  // namespace tdr::jobs
