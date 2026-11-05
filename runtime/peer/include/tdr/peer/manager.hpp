// PeerManager (docs/design/phase3.md §2-§5): one authenticated link per peer
// from the control-plane directory, LAN-then-Tailscale path choice, backoff,
// UNAVAILABLE marking, duplicate-link resolution, ping/RTT, goodbye.
//
// Single-threaded: everything on the io_context thread.
#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <boost/asio/io_context.hpp>

#include "fireforge/control/v1/control.pb.h"
#include "tdr/peer/transport.hpp"
#include "tdr/protocol/envelope.hpp"

namespace tdr::peer {

namespace cv1 = ::fireforge::control::v1;

enum class PeerState { kConnecting, kConnected, kUnavailable, kIdle };
const char* to_string(PeerState s);  // "CONNECTING" | "CONNECTED" | "UNAVAILABLE" | "IDLE"

struct PeerOptions {
    std::string self_node_id;
    std::string self_name;
    std::string daemon_version;
    std::chrono::milliseconds ping_interval{5000};
    std::chrono::milliseconds ping_timeout{15000};    // no frame received -> close
    std::chrono::milliseconds hello_timeout{5000};
    std::chrono::milliseconds backoff_min{1000};
    std::chrono::milliseconds backoff_max{60000};
    std::uint32_t unavailable_after_rounds = 3;
};

// Order in which a peer's addresses are tried: "lan", then "tailscale", then
// any other type; stable within a type. Exposed for tests.
std::vector<cv1::NodeAddress> dial_order(const cv1::NodeInfo& node);

class PeerManager {
public:
    PeerManager(boost::asio::io_context& io, ITransport& transport, PeerOptions options);
    ~PeerManager();
    PeerManager(const PeerManager&) = delete;
    PeerManager& operator=(const PeerManager&) = delete;

    // Installs transport handlers. Call once before update_directory.
    void start();
    // Goodbye{"shutdown"} on every link, close them, stop timers and dialing.
    void stop();

    // Replace the directory (from the NodeAgent). Self is ignored. New
    // ONLINE/STALE nodes start dialing; OFFLINE nodes are not dialed (existing
    // links are kept); missing nodes are closed (goodbye "removed") and dropped.
    void update_directory(const cv1::NodeList& nodes);

    // Snapshot for tdrctl peers and heartbeats, ordered by name.
    std::vector<cv1::PeerReachability> snapshot() const;

    // ---- for Phase 4 ----------------------------------------------------------
    // Sends on the peer's live link (sender_id is filled in). False if not connected.
    bool send(const std::string& node_id, protocol::Frame frame);
    // Frames other than HELLO / HEARTBEAT / PEER_ADVERTISE after the hello.
    void set_frame_handler(std::function<void(const std::string& node_id, protocol::Frame)> fn);
    // Fired on every state change of a peer.
    void set_state_listener(std::function<void(const std::string& node_id, PeerState)> fn);
    // Asked when an inbound hello names a node the directory doesn't know.
    void set_directory_refresh(std::function<void()> fn);

private:
    struct Impl;
    std::shared_ptr<Impl> impl_;
};

}  // namespace tdr::peer
