// Durable local journal (SQLite, WAL mode). Source of truth for job state on
// this node; the daemon rebuilds everything from it after a crash.
//
// All methods are synchronous and called from the daemon's single io thread.
// Every mutating call is its own transaction (committed before return).
// Errors from SQLite throw JournalError.
#pragma once

#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

#include "fireforge/tdr/v1/job.pb.h"

namespace tdr::journal {

namespace v1 = ::fireforge::tdr::v1;

struct JournalError : std::runtime_error {
    using std::runtime_error::runtime_error;
};

struct JobRecord {
    std::string job_id;
    v1::JobSpec spec;                 // stored as serialized protobuf
    v1::JobState state = v1::CREATED;
    std::uint32_t attempts_used = 0;
    std::string current_attempt_id;   // "" when no attempt yet
    std::string last_error;
    // Distinct output artifact ids, in output order (JobStatus.output_artifacts).
    std::vector<std::string> output_artifacts;
    // Phase 6: every named output in order (JobStatus.outputs). Two names may
    // share an artifact id (identical content), so this can be longer than
    // output_artifacts. Rows journaled before Phase 6 have empty names. On
    // insert_job, an empty `outputs` means "output_artifacts, unnamed".
    std::vector<v1::NamedArtifact> outputs;
    std::vector<v1::StepSpec> spawn;  // Phase 6: WorkerResult.spawn of the committed attempt
    v1::NamedArtifact checkpoint;     // Phase 6: latest checkpoint (empty artifact_id = none)
    std::int64_t created_unix_ms = 0;
    std::int64_t updated_unix_ms = 0;
    double progress_fraction = 0;
    std::string progress_message;
};

enum class AttemptState { kStarting, kRunning, kSucceeded, kFailed, kCancelled, kAbandoned };

struct AttemptRecord {
    std::string attempt_id;
    std::string job_id;
    std::uint32_t number = 0;         // 1-based
    std::string token;                // worker auth secret
    AttemptState state = AttemptState::kStarting;
    std::int32_t pid = 0;
    std::uint64_t pid_start_ticks = 0;  // /proc/<pid>/stat field 22, guards PID reuse
    std::string cgroup_path;
    std::string work_dir;
    std::int32_t exit_code = 0;
    std::int32_t exit_signal = 0;
    std::string error;
    std::int64_t started_unix_ms = 0;
    std::int64_t ended_unix_ms = 0;
};

struct ArtifactRow {
    std::string id;
    std::string sha256_hex;
    std::uint64_t size = 0;
    std::string media_type;
    std::string producer_job;
    std::string local_path;
    std::int64_t created_unix_ms = 0;
};

class IJournal {
public:
    virtual ~IJournal() = default;

    // ---- jobs --------------------------------------------------------------
    // Throws JournalError if job_id already exists.
    virtual void insert_job(const JobRecord& job) = 0;
    virtual std::optional<JobRecord> get_job(const std::string& job_id) const = 0;
    // Newest first. state == JOB_STATE_UNSPECIFIED means all states.
    virtual std::vector<JobRecord> list_jobs(v1::JobState state, std::size_t limit) const = 0;
    // Oldest first (FIFO order for the local queue).
    virtual std::vector<JobRecord> jobs_in_states(const std::vector<v1::JobState>& states) const = 0;
    virtual std::size_t count_in_state(v1::JobState state) const = 0;

    // Compare-and-set. Returns false (and changes nothing) if the job's current
    // state != from. Sets last_error when `error` is non-empty; updates updated_unix_ms.
    virtual bool transition(const std::string& job_id, v1::JobState from, v1::JobState to,
                            const std::string& error = {}) = 0;

    virtual void set_progress(const std::string& job_id, double fraction, const std::string& message) = 0;

    // ---- attempts ----------------------------------------------------------
    // Inserts the attempt, sets job.current_attempt_id and increments
    // job.attempts_used, atomically.
    virtual void begin_attempt(const AttemptRecord& attempt) = 0;
    virtual std::optional<AttemptRecord> get_attempt(const std::string& attempt_id) const = 0;
    virtual void update_attempt_process(const std::string& attempt_id, std::int32_t pid,
                                        std::uint64_t pid_start_ticks, const std::string& cgroup_path) = 0;
    // Sets state, exit fields, error, ended_unix_ms.
    virtual void finish_attempt(const std::string& attempt_id, AttemptState state,
                                std::int32_t exit_code, std::int32_t exit_signal,
                                const std::string& error) = 0;
    // Attempts in kStarting or kRunning (used for crash recovery).
    virtual std::vector<AttemptRecord> open_attempts() const = 0;

    // ---- commit (fencing) --------------------------------------------------
    // Atomically: if job.state == COMMITTING and job.current_attempt_id ==
    // attempt_id, store the named outputs (in order) and the spawn list, set
    // state COMPLETED, mark the attempt kSucceeded and return true. Otherwise
    // change nothing, return false. This is the local fencing check that stops
    // a stale attempt from publishing.
    virtual bool commit_outputs(const std::string& job_id, const std::string& attempt_id,
                                const std::vector<v1::NamedArtifact>& outputs,
                                const std::vector<v1::StepSpec>& spawn) = 0;

    // ---- checkpoints (Phase 6) ---------------------------------------------
    // Replaces the job's latest checkpoint; unknown job = no-op.
    virtual void set_checkpoint(const std::string& job_id, const v1::NamedArtifact& checkpoint) = 0;

    // ---- artifacts ---------------------------------------------------------
    virtual void record_artifact(const ArtifactRow& row) = 0;  // upsert by id
    virtual std::optional<ArtifactRow> get_artifact(const std::string& id) const = 0;

    // ---- node counters -----------------------------------------------------
    virtual std::uint64_t failed_job_count() const = 0;
};

// Creates parent dirs and schema if missing. Sets journal_mode=WAL,
// synchronous=FULL, foreign_keys=ON. Schema has a user_version for migrations.
std::unique_ptr<IJournal> open_sqlite_journal(const std::filesystem::path& path);

const char* to_string(AttemptState s);

}  // namespace tdr::journal
