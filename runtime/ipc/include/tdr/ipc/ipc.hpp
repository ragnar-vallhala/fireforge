// Local IPC over Unix domain sockets using the TDR envelope.
// Server side is asynchronous (Boost.Asio, single io_context thread).
// Client side is blocking and meant for tdrctl and tests.
#pragma once

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <optional>
#include <sys/types.h>

#include <boost/asio/io_context.hpp>

#include "tdr/protocol/envelope.hpp"

namespace tdr::ipc {

// One accepted client connection. All methods must be called on the server's
// io_context thread, except send(), which is also safe from that thread only
// (the daemon is single-threaded by design).
class Connection {
public:
    virtual ~Connection() = default;
    virtual std::uint64_t id() const = 0;           // unique per server, starts at 1
    virtual void send(protocol::Frame frame) = 0;   // queued; writes stay in order
    virtual void close() = 0;                       // idempotent; on_close fires once
    virtual bool is_open() const = 0;
    virtual std::optional<pid_t> peer_pid() const = 0;  // SO_PEERCRED
    virtual std::optional<uid_t> peer_uid() const = 0;
};

struct Handlers {
    // Called once per complete frame, in arrival order.
    std::function<void(const std::shared_ptr<Connection>&, protocol::Frame)> on_frame;
    // Called once when the connection closes for any reason (peer EOF,
    // protocol error, close()). Optional.
    std::function<void(const std::shared_ptr<Connection>&)> on_close;
};

class UnixServer {
public:
    // Removes a stale socket file at `path` before binding. Creates parent dirs.
    // `mode` is applied to the socket file with chmod after bind.
    UnixServer(boost::asio::io_context& io, std::filesystem::path path, Handlers handlers,
               unsigned mode = 0660);
    ~UnixServer();  // stops and unlinks the socket file

    UnixServer(const UnixServer&) = delete;
    UnixServer& operator=(const UnixServer&) = delete;

    void start();   // throws std::system_error if bind/listen fails
    void stop();    // closes listener and all connections
    std::size_t connection_count() const;
    const std::filesystem::path& path() const;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

// Blocking client.
class UnixClient {
public:
    // Throws std::system_error if the socket cannot be connected.
    explicit UnixClient(const std::filesystem::path& path);
    ~UnixClient();

    UnixClient(const UnixClient&) = delete;
    UnixClient& operator=(const UnixClient&) = delete;

    void send(const protocol::Frame& frame);
    // nullopt on clean EOF or timeout. timeout zero = wait forever.
    std::optional<protocol::Frame> receive(std::chrono::milliseconds timeout = std::chrono::milliseconds{0});

    // send() then receive() until a frame whose correlation_id equals the
    // request's message_id arrives (other frames are discarded).
    // Throws std::runtime_error on EOF or timeout.
    protocol::Frame request(const protocol::Frame& frame,
                            std::chrono::milliseconds timeout = std::chrono::seconds{30});

    int fd() const { return fd_; }

private:
    int fd_ = -1;
};

}  // namespace tdr::ipc
