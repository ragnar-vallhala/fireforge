// Local job manager: queue, admission against node capacity, attempt
// lifecycle, retries, cancellation, output commit, crash recovery.
//
// Depends only on the IJournal / IArtifactStore / IProcessSupervisor
// interfaces, so it is unit-tested with fakes. Single-threaded: every method
// must be called on the io_context thread.
#pragma once

#include <chrono>
#include <cstdint>
#include <expected>
#include <filesystem>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <boost/asio/io_context.hpp>

#include "fireforge/tdr/v1/job.pb.h"
#include "fireforge/tdr/v1/local.pb.h"
#include "tdr/artifacts/store.hpp"
#include "tdr/journal/journal.hpp"
#include "tdr/supervisor/supervisor.hpp"

namespace tdr::jobs {

namespace v1 = ::fireforge::tdr::v1;
namespace fs = std::filesystem;

// ---- state machine ----------------------------------------------------------

bool is_terminal(v1::JobState s);  // COMPLETED, FAILED, CANCELLED, EXPIRED, VALIDATION_FAILED
// Encodes the allowed transitions of Description.md 10.1 (including LEASED
// for later phases). Self-transitions are invalid.
bool is_valid_transition(v1::JobState from, v1::JobState to);
const char* to_string(v1::JobState s);

// ---- capacity accounting -----------------------------------------------------

struct Capacity {
    std::uint32_t cpu_cores = 0;
    std::uint64_t memory_bytes = 0;
    std::uint32_t jobs = 0;
};

// What a job will reserve, after applying defaults (0 cores -> 1, 0 memory ->
// default_job_memory_bytes).
Capacity effective_request(const v1::JobSpec& spec, std::uint64_t default_job_memory_bytes);

// ---- manager -----------------------------------------------------------------

struct ManagerOptions {
    std::string node_name;
    Capacity capacity;                         // node caps (jobs = max concurrent)
    std::uint64_t default_job_memory_bytes = 1ull << 30;
    fs::path work_root;                        // <state>/work
    fs::path worker_socket;                    // passed to children as FIREFORGE_IPC_SOCKET
    std::chrono::seconds cancel_grace{10};
    bool keep_work_dirs = false;               // keep attempt dirs after success (debugging)

    // Restricted job user (docs/design/phase1.md §11). When set:
    //  - LaunchSpec.run_as = this, and HOME = the attempt dir
    //  - <work>/<job_id>          group run_as->gid, mode 02750
    //    attempt dir, outputs/, checkpoints/   group gid, mode 02770
    //    inputs/                  group gid, mode 02750 (job can read, not add/remove)
    //  - outputs are ingested with move_source=false (copied: the file is
    //    owned by the job user, so the store copy must be a new daemon-owned file)
    //  - cleanup failures (job made a dir unwritable) are logged, not fatal
    std::optional<supervisor::RunAs> run_as;
};

struct Usage {
    Capacity allocated;
    std::uint32_t running = 0;
    std::uint32_t queued = 0;
};

class JobManager {
public:
    JobManager(boost::asio::io_context& io, journal::IJournal& journal,
               artifacts::IArtifactStore& store, supervisor::IProcessSupervisor& supervisor,
               ManagerOptions options);
    ~JobManager();

    JobManager(const JobManager&) = delete;
    JobManager& operator=(const JobManager&) = delete;

    // Call once at startup, before submit(). For every open attempt left by a
    // previous run: kill_orphan(), mark the attempt kAbandoned, and move the
    // job to RETRY_PENDING -> QUEUED if attempts remain, else FAILED.
    // Jobs left in QUEUED/RETRY_PENDING/ASSIGNED are re-queued. Then schedules.
    void recover();

    // Validates (non-empty argv; request fits node capacity at all; input
    // artifacts and spec.resume_checkpoint exist locally; input_names no longer
    // than input_artifacts), assigns job_id if empty, persists
    // CREATED -> QUEUED, then tries to start work. Returns the job id.
    std::expected<std::string, std::string> submit(v1::JobSpec spec);

    // QUEUED/RETRY_PENDING -> CANCELLED immediately. ASSIGNED/RUNNING: notify
    // the worker (cancel notifier), supervisor->cancel(grace); job becomes
    // CANCELLED when the process exits. Terminal jobs -> error.
    std::expected<void, std::string> cancel(const std::string& job_id);

    std::optional<v1::JobStatus> status(const std::string& job_id) const;
    std::vector<v1::JobStatus> list(v1::JobState state, std::size_t limit) const;
    Usage usage() const;

    // Live node capacity (Phase 2: control-plane policy, phase2.md §4). Raising
    // it starts queued work; lowering it only affects admission of new attempts
    // (running jobs are never killed). New submits are validated against it;
    // already-queued jobs that no longer fit stay queued until capacity grows.
    void set_capacity(const Capacity& capacity);
    Capacity capacity() const;

    // ---- worker IPC hooks (called by the daemon's worker-socket handler) ----
    // Validates attempt id + token and that the attempt is RUNNING. Returns the
    // task to send back, or an error string (daemon replies ERROR UNAUTHORIZED).
    std::expected<v1::WorkerTask, std::string> on_worker_hello(const v1::WorkerHello& hello);
    void on_worker_progress(const v1::WorkerProgress& progress);
    // Stores the result; the outcome is applied when the process exits.
    // Phase 6: result.spawn is kept with the job (JobStatus.spawn) when it
    // completes; output names (JobStatus.outputs) come from OutputFile.name, else
    // the relative path; a duplicate name fails the job (non-retryable).
    std::expected<void, std::string> on_worker_result(const v1::WorkerResult& result);

    // Phase 6 (phase6.md §6): the attempt must be RUNNING and its job have
    // policy.checkpoint; path must be a regular file inside the attempt's
    // checkpoint dir. Ingests it (producer_job = job id), journals it as the
    // job's latest checkpoint (JobStatus.checkpoint) and fires the checkpoint
    // listener. A later attempt of the same job (local retry) resumes from it:
    // it is materialized into the new attempt's checkpoint dir as
    // resume/<name> and passed as WorkerTask.resume_checkpoint; so is
    // spec.resume_checkpoint for the first attempt.
    std::expected<v1::NamedArtifact, std::string> on_worker_checkpoint(const v1::WorkerCheckpoint& checkpoint);
    void set_checkpoint_listener(std::function<void(const std::string& job_id, const v1::NamedArtifact&)> fn);

    // The daemon registers this to push WORKER_CANCEL to a connected worker.
    void set_cancel_notifier(std::function<void(const std::string& attempt_id, const std::string& reason)> fn);

    // Fired after every job state change (for logging / later control-plane sync).
    void set_state_listener(std::function<void(const std::string& job_id, v1::JobState state)> fn);

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

v1::JobStatus to_status(const journal::JobRecord& rec);

}  // namespace tdr::jobs
