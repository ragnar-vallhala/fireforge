#include "tdr/ipc/ipc.hpp"

#include <array>
#include <cerrno>
#include <cstring>
#include <deque>
#include <map>
#include <stdexcept>
#include <system_error>
#include <vector>

#include <poll.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <unistd.h>

#include <boost/asio/local/stream_protocol.hpp>
#include <boost/asio/write.hpp>

namespace tdr::ipc {

namespace asio = boost::asio;
using local = asio::local::stream_protocol;

namespace {

constexpr std::size_t kReadChunk = 64 * 1024;

[[noreturn]] void throw_errno(int err, const std::string& what) {
    throw std::system_error(err, std::generic_category(), what);
}

sockaddr_un make_addr(const std::filesystem::path& path) {
    sockaddr_un addr{};
    addr.sun_family = AF_UNIX;
    const std::string& s = path.native();
    if (s.size() >= sizeof(addr.sun_path)) throw_errno(ENAMETOOLONG, "unix socket path too long: " + s);
    std::memcpy(addr.sun_path, s.c_str(), s.size() + 1);
    return addr;
}

// True if something is accepting connections on `path`.
bool socket_is_live(const std::filesystem::path& path) {
    int fd = ::socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (fd < 0) return false;
    sockaddr_un addr = make_addr(path);
    bool live = ::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0;
    ::close(fd);
    return live;
}

}  // namespace

// ---- Server -----------------------------------------------------------------

struct ServerState;

class ConnectionImpl final : public Connection, public std::enable_shared_from_this<ConnectionImpl> {
public:
    ConnectionImpl(std::uint64_t id, local::socket sock, std::weak_ptr<ServerState> server)
        : id_(id), sock_(std::move(sock)), server_(std::move(server)) {
        ucred cred{};
        socklen_t len = sizeof(cred);
        if (::getsockopt(sock_.native_handle(), SOL_SOCKET, SO_PEERCRED, &cred, &len) == 0 &&
            len == sizeof(cred)) {
            pid_ = cred.pid;
            uid_ = cred.uid;
        }
    }

    std::uint64_t id() const override { return id_; }
    bool is_open() const override { return open_; }
    std::optional<pid_t> peer_pid() const override { return pid_; }
    std::optional<uid_t> peer_uid() const override { return uid_; }

    void start() { do_read(); }

    void send(protocol::Frame frame) override {
        if (!open_) return;
        std::string bytes;
        try {
            bytes = protocol::encode_frame(frame);
        } catch (const protocol::ProtocolError&) {
            close();
            return;
        }
        queue_.push_back(std::move(bytes));
        if (queue_.size() == 1) do_write();
    }

    void close() override;

private:
    void do_read() {
        auto self = shared_from_this();
        sock_.async_read_some(asio::buffer(rbuf_), [self](const boost::system::error_code& ec, std::size_t n) {
            if (!self->open_) return;
            if (ec) {
                self->close();
                return;
            }
            self->on_bytes(n);
        });
    }

    void on_bytes(std::size_t n);

    void do_write() {
        auto self = shared_from_this();
        asio::async_write(sock_, asio::buffer(queue_.front()),
                          [self](const boost::system::error_code& ec, std::size_t) {
                              if (!self->open_) return;
                              if (ec) {
                                  self->close();
                                  return;
                              }
                              self->queue_.pop_front();
                              if (!self->queue_.empty()) self->do_write();
                          });
    }

    std::uint64_t id_;
    local::socket sock_;
    std::weak_ptr<ServerState> server_;
    std::optional<pid_t> pid_;
    std::optional<uid_t> uid_;
    bool open_ = true;
    std::array<char, kReadChunk> rbuf_{};
    protocol::FrameDecoder decoder_;
    std::deque<std::string> queue_;
};

struct ServerState : std::enable_shared_from_this<ServerState> {
    ServerState(asio::io_context& io, std::filesystem::path p, Handlers h, unsigned m)
        : acceptor(io), path(std::move(p)), handlers(std::move(h)), mode(m) {}

    local::acceptor acceptor;
    std::filesystem::path path;
    Handlers handlers;
    unsigned mode;
    bool running = false;
    bool bound = false;
    std::uint64_t next_id = 1;
    std::map<std::uint64_t, std::shared_ptr<ConnectionImpl>> conns;

    void do_accept() {
        auto self = shared_from_this();
        acceptor.async_accept([self](const boost::system::error_code& ec, local::socket sock) {
            if (!self->running) return;
            if (!ec) {
                auto c = std::make_shared<ConnectionImpl>(self->next_id++, std::move(sock), self);
                self->conns.emplace(c->id(), c);
                c->start();
            } else if (ec == asio::error::operation_aborted) {
                return;
            }
            // Transient accept errors (EMFILE, ECONNABORTED...) just retry.
            self->do_accept();
        });
    }
};

void ConnectionImpl::close() {
    if (!open_) return;
    open_ = false;
    boost::system::error_code ec;
    sock_.shutdown(local::socket::shutdown_both, ec);
    sock_.close(ec);
    queue_.clear();
    auto self = shared_from_this();
    if (auto srv = server_.lock()) {
        srv->conns.erase(id_);
        if (srv->handlers.on_close) srv->handlers.on_close(self);
    }
}

void ConnectionImpl::on_bytes(std::size_t n) {
    auto self = shared_from_this();
    decoder_.feed(std::string_view(rbuf_.data(), n));
    try {
        while (open_) {
            auto frame = decoder_.next();
            if (!frame) break;
            auto srv = server_.lock();
            if (!srv) {
                close();
                return;
            }
            if (srv->handlers.on_frame) {
                std::shared_ptr<Connection> base = self;
                srv->handlers.on_frame(base, std::move(*frame));
            }
        }
    } catch (const protocol::ProtocolError&) {
        close();
        return;
    }
    if (open_) do_read();
}

struct UnixServer::Impl {
    std::shared_ptr<ServerState> state;
};

UnixServer::UnixServer(asio::io_context& io, std::filesystem::path path, Handlers handlers, unsigned mode)
    : impl_(std::make_unique<Impl>()) {
    impl_->state = std::make_shared<ServerState>(io, std::move(path), std::move(handlers), mode);
}

UnixServer::~UnixServer() {
    stop();
    auto& s = *impl_->state;
    if (s.bound) {
        ::unlink(s.path.c_str());
        s.bound = false;
    }
}

void UnixServer::start() {
    auto& s = *impl_->state;
    if (s.running) return;

    std::error_code fec;
    if (s.path.has_parent_path()) {
        std::filesystem::create_directories(s.path.parent_path(), fec);
        if (fec) throw std::system_error(fec, "create socket dir " + s.path.parent_path().string());
    }

    struct stat st{};
    if (::lstat(s.path.c_str(), &st) == 0) {
        if (!S_ISSOCK(st.st_mode)) throw_errno(EEXIST, "not a socket: " + s.path.string());
        if (socket_is_live(s.path)) throw_errno(EADDRINUSE, "socket in use: " + s.path.string());
        if (::unlink(s.path.c_str()) != 0 && errno != ENOENT)
            throw_errno(errno, "unlink stale socket " + s.path.string());
    }

    boost::system::error_code ec;
    local::endpoint ep(s.path.string());
    s.acceptor.open(ep.protocol(), ec);
    if (!ec) s.acceptor.bind(ep, ec);
    if (ec) {
        s.acceptor.close();
        throw std::system_error(ec.value(), std::generic_category(), "bind " + s.path.string());
    }
    s.bound = true;
    if (::chmod(s.path.c_str(), static_cast<mode_t>(s.mode)) != 0) {
        int err = errno;
        s.acceptor.close();
        throw_errno(err, "chmod " + s.path.string());
    }
    s.acceptor.listen(asio::socket_base::max_listen_connections, ec);
    if (ec) {
        s.acceptor.close();
        throw std::system_error(ec.value(), std::generic_category(), "listen " + s.path.string());
    }
    s.running = true;
    s.do_accept();
}

void UnixServer::stop() {
    auto& s = *impl_->state;
    s.running = false;
    boost::system::error_code ec;
    s.acceptor.close(ec);
    // close() erases from the map and runs on_close; iterate over a copy.
    std::vector<std::shared_ptr<ConnectionImpl>> conns;
    conns.reserve(s.conns.size());
    for (auto& [id, c] : s.conns) conns.push_back(c);
    for (auto& c : conns) c->close();
    s.conns.clear();
}

std::size_t UnixServer::connection_count() const { return impl_->state->conns.size(); }

const std::filesystem::path& UnixServer::path() const { return impl_->state->path; }

// ---- Client -----------------------------------------------------------------

namespace {

using Clock = std::chrono::steady_clock;

// Wait until fd is readable or the deadline passes. False on timeout.
bool wait_readable(int fd, std::optional<Clock::time_point> deadline) {
    for (;;) {
        int ms = -1;
        if (deadline) {
            auto left = std::chrono::ceil<std::chrono::milliseconds>(*deadline - Clock::now());
            if (left.count() <= 0) return false;
            ms = left.count() > 1'000'000'000 ? 1'000'000'000 : static_cast<int>(left.count());
        }
        pollfd p{fd, POLLIN, 0};
        int r = ::poll(&p, 1, ms);
        if (r > 0) return true;
        if (r == 0) {
            if (!deadline) continue;
            return false;
        }
        if (errno != EINTR) throw_errno(errno, "poll");
    }
}

enum class ReadResult { kOk, kEof, kTimeout };

// Read exactly len bytes, polling with the deadline before each read.
ReadResult read_exact(int fd, char* data, std::size_t len, std::optional<Clock::time_point> deadline,
                      std::size_t& got) {
    got = 0;
    while (got < len) {
        if (!wait_readable(fd, deadline)) return ReadResult::kTimeout;
        ssize_t n = ::recv(fd, data + got, len - got, 0);
        if (n < 0) {
            if (errno == EINTR || errno == EAGAIN) continue;
            throw_errno(errno, "recv");
        }
        if (n == 0) return ReadResult::kEof;
        got += static_cast<std::size_t>(n);
    }
    return ReadResult::kOk;
}

std::optional<protocol::Frame> receive_until(int fd, std::optional<Clock::time_point> deadline,
                                             bool* timed_out) {
    std::array<std::uint8_t, protocol::kHeaderSize> hdr{};
    std::size_t got = 0;
    auto r = read_exact(fd, reinterpret_cast<char*>(hdr.data()), hdr.size(), deadline, got);
    if (r == ReadResult::kTimeout && got == 0) {
        if (timed_out) *timed_out = true;
        return std::nullopt;
    }
    if (r == ReadResult::kEof && got == 0) return std::nullopt;
    if (r == ReadResult::kEof) throw protocol::ProtocolError("unexpected EOF in frame header");
    if (r == ReadResult::kTimeout) throw protocol::ProtocolError("timeout in the middle of a frame");
    auto h = protocol::decode_header(hdr);
    if (!h) throw protocol::ProtocolError("protocol decode error: " + std::string(protocol::to_string(h.error())));
    protocol::Frame f;
    f.header = *h;
    f.payload.resize(h->payload_length);
    r = read_exact(fd, f.payload.data(), f.payload.size(), deadline, got);
    if (r == ReadResult::kEof) throw protocol::ProtocolError("unexpected EOF in frame payload");
    if (r == ReadResult::kTimeout) throw protocol::ProtocolError("timeout in the middle of a frame");
    return f;
}

}  // namespace

UnixClient::UnixClient(const std::filesystem::path& path) {
    sockaddr_un addr = make_addr(path);
    fd_ = ::socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (fd_ < 0) throw_errno(errno, "socket");
    int r;
    do {
        r = ::connect(fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    } while (r != 0 && errno == EINTR);
    if (r != 0) {
        int err = errno;
        ::close(fd_);
        fd_ = -1;
        throw_errno(err, "connect " + path.string());
    }
}

UnixClient::~UnixClient() {
    if (fd_ >= 0) ::close(fd_);
}

void UnixClient::send(const protocol::Frame& frame) { protocol::write_frame(fd_, frame); }

std::optional<protocol::Frame> UnixClient::receive(std::chrono::milliseconds timeout) {
    std::optional<Clock::time_point> deadline;
    if (timeout.count() > 0) deadline = Clock::now() + timeout;
    return receive_until(fd_, deadline, nullptr);
}

protocol::Frame UnixClient::request(const protocol::Frame& frame, std::chrono::milliseconds timeout) {
    std::optional<Clock::time_point> deadline;
    if (timeout.count() > 0) deadline = Clock::now() + timeout;
    send(frame);
    for (;;) {
        bool timed_out = false;
        auto reply = receive_until(fd_, deadline, &timed_out);
        if (!reply) throw std::runtime_error(timed_out ? "request timed out" : "connection closed by peer");
        if (reply->header.correlation_id == frame.header.message_id) return std::move(*reply);
    }
}

}  // namespace tdr::ipc
