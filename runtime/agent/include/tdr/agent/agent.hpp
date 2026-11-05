// tdrd's control-plane agent (docs/design/phase2.md §7): registration,
// heartbeats with backoff, live policy application, peer directory cache,
// graceful offline. Never blocks local job execution.
//
// Single-threaded: everything runs on the daemon's io_context thread.
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

#include "fireforge/control/v1/control.pb.h"
#include "tdr/http/http.hpp"
#include "tdr/jobs/jobs.hpp"

namespace tdr::agent {

namespace cv1 = ::fireforge::control::v1;
namespace fs = std::filesystem;

// ---- transport ---------------------------------------------------------------

struct ClientError {
    int http_status = 0;      // 0 = transport/TLS error
    std::string message;
};

template <class T>
using Result = std::expected<T, ClientError>;

// Typed control-plane client. Callbacks run later on the io thread, exactly once.
class IControlClient {
public:
    virtual ~IControlClient() = default;
    virtual void register_node(const cv1::RegisterRequest& req, std::function<void(Result<cv1::RegisterReply>)> cb) = 0;
    virtual void heartbeat(const cv1::Heartbeat& hb, std::function<void(Result<cv1::HeartbeatReply>)> cb) = 0;
    virtual void list_nodes(std::function<void(Result<cv1::NodeList>)> cb) = 0;
    virtual void offline(const std::string& node_id, const std::string& reason,
                         std::function<void(Result<cv1::NodeInfo>)> cb) = 0;
};

// Over http::AsyncClient, proto3 JSON bodies; non-2xx -> ClientError{status, ApiError.message}.
std::unique_ptr<IControlClient> make_https_control_client(boost::asio::io_context& io,
                                                          std::shared_ptr<boost::asio::ssl::context> ctx,
                                                          http::ClientOptions options);

// ---- detection helpers -----------------------------------------------------------

// tailscale_mode: "auto" | "on" | "off". advertise: "auto" or an explicit address
// (then a single "lan" entry with that address).
std::vector<cv1::NodeAddress> detect_addresses(std::uint16_t port, const std::string& tailscale_mode,
                                               const std::string& advertise);
cv1::NodeResources detect_physical(const fs::path& state_dir);

// Whole-machine CPU utilization from /proc/stat deltas between calls (first call -> 0).
class CpuSampler {
public:
    double sample();
private:
    std::uint64_t last_total_ = 0, last_idle_ = 0;
};

// min(local, policy) per field, where a policy value of 0 means no limit.
jobs::Capacity effective_capacity(const jobs::Capacity& local, const cv1::ResourcePolicy& policy);

// ---- agent -----------------------------------------------------------------------

enum class ControlState { kStandalone, kRegistering, kConnected, kDisconnected };
const char* to_string(ControlState s);  // "standalone" | "registering" | "connected" | "disconnected"

struct AgentOptions {
    cv1::RegisterRequest registration;           // name, version, addresses, resources, capabilities
    jobs::Capacity local_capacity;               // from tdr.conf (before policy)
    fs::path policy_cache;                       // <state>/state/policy.json
    std::chrono::seconds peer_refresh{30};
    std::chrono::milliseconds backoff_min{1000};
    std::chrono::milliseconds backoff_max{60000};
    std::chrono::milliseconds offline_timeout{2000};
};

struct AgentHooks {
    // Apply new effective node capacity (JobManager::set_capacity).
    std::function<void(const jobs::Capacity&)> apply_capacity;
    // Fill the live stats of a heartbeat (allocated, running/queued/failed, memory/disk
    // available, cpu_utilization, capacity). The agent sets node_id, sent time,
    // version, policy_version and addresses itself.
    std::function<void(cv1::Heartbeat&)> fill_stats;
    // Called after every successful peer-directory refresh (Phase 3: PeerManager).
    std::function<void(const cv1::NodeList&)> on_directory;
};

class NodeAgent {
public:
    NodeAgent(boost::asio::io_context& io, IControlClient& client, AgentOptions options, AgentHooks hooks);
    ~NodeAgent();
    NodeAgent(const NodeAgent&) = delete;
    NodeAgent& operator=(const NodeAgent&) = delete;

    // Loads the cached policy (if any) and applies it immediately, then starts
    // registering in the background.
    void start();
    // Best-effort POST /offline (bounded by offline_timeout), cancels timers,
    // then calls `done` (always, exactly once).
    void stop(std::function<void()> done);
    // Fetch the peer directory now (no-op before registration or while a fetch
    // is in flight). Used when an unknown peer says hello (phase3.md §2).
    void refresh_peers();

    ControlState state() const;
    const std::string& node_id() const;               // "" until registered
    std::optional<cv1::ResourcePolicy> policy() const;  // applied policy
    std::int64_t last_heartbeat_ok_ms() const;        // 0 = never
    const std::string& last_error() const;
    const cv1::NodeList& peers() const;
    std::int64_t peers_refreshed_ms() const;          // 0 = never

private:
    struct Impl;
    std::shared_ptr<Impl> impl_;
};

}  // namespace tdr::agent
