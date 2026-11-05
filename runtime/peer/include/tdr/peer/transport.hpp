// Peer transport (docs/design/phase3.md §1-§3): mutually authenticated TLS 1.3
// links carrying 48-byte envelopes. No protocol logic here: hello, ping,
// dedup and state live in PeerManager.
//
// Single-threaded: all callbacks run on the io_context thread.
#pragma once

#include <chrono>
#include <cstdint>
#include <expected>
#include <functional>
#include <memory>
#include <string>

#include <boost/asio/io_context.hpp>
#include <boost/asio/ssl/context.hpp>

#include "tdr/protocol/envelope.hpp"
#include "tdr/tls/tls.hpp"

namespace tdr::peer {

// One established, authenticated link. The remote certificate has been
// verified against the CA and is a node certificate.
class IFrameLink {
public:
    virtual ~IFrameLink() = default;
    virtual std::uint64_t id() const = 0;              // unique per transport
    virtual bool outbound() const = 0;                 // we dialed it
    virtual const tls::Identity& identity() const = 0; // remote node cert
    virtual std::string remote() const = 0;            // "ip:port"
    virtual bool is_open() const = 0;
    // Queues the frame (never blocks). No-op after close.
    virtual void send(const protocol::Frame& frame) = 0;
    // Flushes queued frames (bounded by a short timeout), then closes.
    // on_close fires once, later, with reason "closed locally".
    virtual void close() = 0;
};

struct TransportHandlers {
    std::function<void(const std::shared_ptr<IFrameLink>&)> on_accept;  // inbound link ready
    std::function<void(const std::shared_ptr<IFrameLink>&, protocol::Frame)> on_frame;
    // Fires exactly once per link (inbound or outbound) after it was handed
    // out, for any reason (peer EOF, error, protocol error, close()).
    std::function<void(const std::shared_ptr<IFrameLink>&, const std::string& reason)> on_close;
};

using DialResult = std::expected<std::shared_ptr<IFrameLink>, std::string>;

class ITransport {
public:
    virtual ~ITransport() = default;
    virtual void set_handlers(TransportHandlers handlers) = 0;
    // TCP connect + TLS handshake to host:port, then require the server cert
    // to be a node cert with CN == expected_name ("identity mismatch: ..."
    // otherwise). cb is called exactly once, later, on the io thread (not
    // after the transport is destroyed).
    virtual void dial(const std::string& host, std::uint16_t port, const std::string& expected_name,
                      std::function<void(DialResult)> cb) = 0;
};

struct TransportOptions {
    std::string listen_address = "0.0.0.0";
    std::uint16_t listen_port = 7400;            // 0 = ephemeral (tests)
    std::chrono::milliseconds connect_timeout{3000};   // TCP + TLS, per dial
    std::chrono::milliseconds handshake_timeout{5000}; // inbound TLS
    std::size_t max_links = 512;                 // inbound beyond this are closed
    std::size_t max_queued_bytes = 64u << 20;    // per link; exceeding closes the link
};

// TLS transport. `ctx` from tls::make_peer_context. Inbound clients must
// present a node certificate (others are closed after the handshake).
class TlsTransport : public ITransport {
public:
    TlsTransport(boost::asio::io_context& io, std::shared_ptr<boost::asio::ssl::context> ctx, TransportOptions options);
    ~TlsTransport() override;
    TlsTransport(const TlsTransport&) = delete;
    TlsTransport& operator=(const TlsTransport&) = delete;

    void set_handlers(TransportHandlers handlers) override;
    void dial(const std::string& host, std::uint16_t port, const std::string& expected_name,
              std::function<void(DialResult)> cb) override;

    void start();                    // listen; throws std::system_error on bind failure
    void stop();                     // close listener and all links (on_close fires for each)
    std::uint16_t bound_port() const;

private:
    struct Impl;
    std::shared_ptr<Impl> impl_;
};

}  // namespace tdr::peer
