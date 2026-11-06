// GangService (docs/design/phase7.md §5): the tdrd side of multi-node MPI launch.
//
// Head side: `tdr-launch agent` (run by mpirun inside the HEAD job) sends
// WORKER_GANG_LAUNCH on the worker socket; the service forwards GANG_LAUNCH on
// the peer link to the member node, reports GANG_STARTED back to the agent and
// later relays GANG_EXIT. Member side: GANG_LAUNCH from the gang's head starts
// the command in the waiting `tdr-launch member` process (WORKER_EXEC), and the
// member job's end is reported as GANG_EXIT. GANG_KILL cancels a member job.
//
// Single-threaded: runs on the daemon's io_context thread. No knowledge of
// sockets: the daemon passes agent requests in and frames out via hooks.
#pragma once
#include <chrono>
#include <cstdint>
#include <expected>
#include <functional>
#include <memory>
#include <optional>
#include <string>

#include <boost/asio/io_context.hpp>

#include "fireforge/tdr/v1/gang.pb.h"
#include "fireforge/tdr/v1/job.pb.h"
#include "fireforge/tdr/v1/local.pb.h"
#include "tdr/protocol/envelope.hpp"
#include "tdr/remote/executor.hpp"

namespace tdr::gang {

namespace v1 = ::fireforge::tdr::v1;

struct GangHooks {
    // Send on the live peer link to node_id (PeerManager::send). False if not connected.
    std::function<bool(const std::string& node_id, protocol::Frame frame)> send_peer;
    // RemoteExecutor lookups.
    std::function<std::optional<remote::GangLocal>(const std::string& gang_id)> gang_by_id;
    std::function<std::optional<remote::GangLocal>(const std::string& local_job_id)> gang_of_local_job;
    // JobManager::verify_attempt: attempt id + token -> local job id.
    std::function<std::expected<std::string, std::string>(const std::string& attempt_id, const std::string& token)>
        verify_attempt;
    // Local job state (JobManager::status()->state()); nullopt = unknown job.
    std::function<std::optional<v1::JobState>(const std::string& local_job_id)> job_state;
    // Send WORKER_EXEC to the member job's connected `tdr-launch member` process.
    // False if that process is not connected (not started yet / gone).
    std::function<bool(const std::string& local_job_id, const v1::WorkerExec& exec)> exec_member;
    // JobManager::cancel.
    std::function<void(const std::string& local_job_id, const std::string& reason)> cancel_job;
};

struct GangOptions {
    std::chrono::milliseconds launch_timeout{120000};  // GANG_STARTED wait on the head side
};

// Identifies one agent connection on the worker socket (the daemon's connection id).
using AgentId = std::uint64_t;

class GangService {
public:
    GangService(boost::asio::io_context& io, GangOptions options, GangHooks hooks);
    ~GangService();
    GangService(const GangService&) = delete;
    GangService& operator=(const GangService&) = delete;

    // ---- head side ----------------------------------------------------------
    // WORKER_GANG_LAUNCH from an agent connection. Validates (attempt/token of a
    // RUNNING local HEAD job; host is a member node id of that gang other than
    // this node) and sends GANG_LAUNCH. `started` is called exactly once: ok when
    // the member replied GANG_STARTED ok, else an error (validation, peer not
    // connected, member refused, launch_timeout). After a successful start,
    // `exited` is called at most once with the member's GANG_EXIT (or exit -1
    // when the member link is lost / the head job ends first).
    void on_agent_launch(AgentId agent, const v1::GangLaunchRequest& request,
                         std::function<void(std::expected<void, std::string>)> started,
                         std::function<void(const v1::GangExitNotice&)> exited);
    // The agent connection closed: GANG_KILL to that member if still running.
    // No callbacks fire afterwards for this agent.
    void on_agent_closed(AgentId agent);

    // ---- both sides ---------------------------------------------------------
    // GANG_LAUNCH / GANG_STARTED / GANG_EXIT / GANG_KILL from a peer (sender
    // authenticated by the peer layer). Other frame types are ignored.
    void on_peer_frame(const std::string& from_node_id, protocol::Frame frame);
    // A peer link went away: pending launches to/from it fail; started member
    // launches from that head are cancelled (the head is gone).
    void on_peer_lost(const std::string& node_id);
    // JobManager state listener. A HEAD job ending -> GANG_KILL to every member
    // with a running launch (and their `exited` fire with -1). A MEMBER job ending
    // -> GANG_EXIT{exit 0 if COMPLETED else 1} to its head.
    void on_job_state(const std::string& local_job_id, v1::JobState state);

    void stop();

private:
    struct Impl;
    std::shared_ptr<Impl> impl_;
};

}  // namespace tdr::gang
