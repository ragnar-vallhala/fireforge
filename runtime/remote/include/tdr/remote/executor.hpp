// RemoteExecutor (docs/design/phase4.md §5): runs control-plane leases as
// local jobs. Pulls leases when the node has spare capacity, maps each lease
// to a local JobManager job, reports state changes to the control plane
// through a persisted outbox, cancels revoked leases, and announces local
// artifacts. Never blocks local job execution.
//
// Single-threaded: everything runs on the daemon's io_context thread.
#pragma once

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <boost/asio/io_context.hpp>

#include "fireforge/control/v1/control.pb.h"
#include "tdr/agent/agent.hpp"
#include "tdr/jobs/jobs.hpp"
#include "tdr/transfer/fetcher.hpp"

namespace tdr::remote {

namespace cv1 = ::fireforge::control::v1;
namespace tv1 = ::fireforge::tdr::v1;
namespace fs = std::filesystem;

// Local job id for a lease: "<central job id>.a<attempt_number>" (a node can
// get a later attempt of the same job, so the central id alone may collide).
std::string local_job_id(const std::string& central_job_id, std::uint32_t attempt_number);

struct RemoteOptions {
    fs::path state_file;                            // <state>/state/remote.json (written atomically)
    bool enabled = true;                            // [control] remote_jobs; false = never lease
    std::chrono::milliseconds poll_interval{2000};  // while leases keep coming
    std::chrono::milliseconds poll_idle{10000};     // after an empty reply / error
    std::chrono::milliseconds retry_min{1000};      // outbox retry backoff
    std::chrono::milliseconds retry_max{30000};
    std::uint64_t default_job_memory_bytes = 1ull << 30;  // same as ManagerOptions
    std::size_t announce_page = 1000;
};

struct RemoteHooks {
    // True while the NodeAgent is connected (no leases / updates otherwise; the
    // outbox waits).
    std::function<bool()> connected;
    // Everything in the local artifact store (full sync after registration).
    std::function<std::vector<cv1::ArtifactRef>()> list_local_artifacts;
    // Metadata of one local artifact (outputs of COMPLETED jobs); nullopt if unknown.
    std::function<std::optional<cv1::ArtifactRef>(const std::string& artifact_id)> artifact_meta;
};

struct RemoteStats {
    std::uint32_t active_leases = 0;
    std::uint32_t pending_updates = 0;
    std::uint64_t leases_received = 0;
    std::uint64_t stale_updates = 0;   // 409s
    std::string last_error;
};

class RemoteExecutor {
public:
    RemoteExecutor(boost::asio::io_context& io, agent::IControlClient& client, jobs::JobManager& manager,
                   RemoteOptions options, RemoteHooks hooks);
    ~RemoteExecutor();
    RemoteExecutor(const RemoteExecutor&) = delete;
    RemoteExecutor& operator=(const RemoteExecutor&) = delete;

    // Loads the state file and reconciles it with the JobManager (call after
    // JobManager::recover()): leases whose local job is terminal get their
    // outcome queued; leases with no local job are reported FAILED (retryable).
    // Nothing is sent before on_registered().
    void start();
    // Cancels timers; in-flight callbacks become no-ops. Does not cancel jobs.
    void stop();

    // NodeAgent hook: (re)registered as node_id. Starts/continues pulling,
    // flushes the outbox, and runs a full artifact sync.
    void on_registered(const std::string& node_id);
    // JobManager state listener (tdrd forwards every change, local jobs too).
    // Leased jobs: RUNNING / terminal -> outbox. Any job reaching COMPLETED:
    // its outputs are announced (incremental).
    void on_job_state(const std::string& local_job_id, tv1::JobState state);
    // NodeAgent hook: cancel the local jobs of these leases. Their final state
    // is still reported (a stale report just gets 409 and is dropped).
    void on_revoked(const std::vector<cv1::LeaseRef>& revoked);

    // Active leases (for Heartbeat.leases): leased jobs not yet terminal locally.
    std::vector<cv1::LeaseRef> active_leases() const;
    // Phase 5 (phase5.md §5): fetcher for inputs a lease needs but the store
    // lacks. nullptr (the default, and until the peer layer is up) -> such
    // leases fail retryable "artifact transfer unavailable". Not owned.
    void set_fetcher(transfer::IArtifactFetcher* fetcher);
    // Queue an incremental announcement (e.g. an artifact fetched or put by tdrctl).
    void announce(const std::vector<cv1::ArtifactRef>& refs);
    // Inputs of leases still fetching or not yet submitted locally (GC must keep them).
    std::vector<std::string> protected_inputs() const;

    // Phase 6 (phase6.md §5, §6): a local leased job checkpointed -> queue a
    // fenced RUNNING JobUpdate with `checkpoint` (name set) and announce it.
    // Leases with spec.resume_checkpoint fetch it like an input before submit.
    // COMPLETED updates carry named outputs (JobStatus.outputs) and JobStatus.spawn.
    void on_checkpoint(const std::string& local_job_id, const tv1::NamedArtifact& checkpoint);

    // Central job id of a local leased job, if it is one.
    std::optional<std::string> central_job_id(const std::string& local_job_id) const;
    RemoteStats stats() const;

private:
    struct Impl;
    std::shared_ptr<Impl> impl_;
};

}  // namespace tdr::remote
