// TlsTransport: mTLS 1.3 peer links carrying envelopes (docs/design/phase3.md §1-§3).
//
// Lifetime: the Impl is owned by TlsTransport only; every async op holds a
// weak_ptr to it (or a shared_ptr to its own Link/Dial/Inbound object), so
// destroying the transport cancels everything and no callback runs afterwards.
// User callbacks are never called synchronously from send()/close()/dial()/stop().
#include "tdr/peer/transport.hpp"

#include <array>
#include <deque>
#include <format>
#include <system_error>
#include <unordered_map>
#include <vector>

#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/write.hpp>

#include <openssl/ssl.h>

#include "tdr/common/common.hpp"

namespace tdr::peer {

namespace net = boost::asio;
using tcp = net::ip::tcp;
using error_code = boost::system::error_code;
using SslStream = net::ssl::stream<tcp::socket>;

namespace {

constexpr std::chrono::milliseconds kFlushTimeout{1000};     // close(): drain the write queue
constexpr std::chrono::milliseconds kShutdownTimeout{1000};  // close(): TLS close_notify exchange
constexpr std::chrono::milliseconds kAcceptRetry{100};       // after a failed accept (EMFILE, ...)
constexpr std::size_t kReadChunk = 64 * 1024;

std::string endpoint_string(const tcp::endpoint& ep) {
    auto addr = ep.address();
    if (addr.is_v6()) return std::format("[{}]:{}", addr.to_string(), ep.port());
    return std::format("{}:{}", addr.to_string(), ep.port());
}

std::string host_port(const std::string& host, std::uint16_t port) {
    if (host.find(':') != std::string::npos && host.front() != '[') return std::format("[{}]:{}", host, port);
    return std::format("{}:{}", host, port);
}

std::string remote_of(const net::basic_socket<tcp>& s) {
    error_code ec;
    auto ep = s.remote_endpoint(ec);
    return ec ? std::string("?") : endpoint_string(ep);
}

bool is_peer_eof(const error_code& ec) {
    return ec == net::error::eof || ec == net::ssl::error::stream_truncated ||
           ec == net::error::connection_reset || ec == net::error::broken_pipe;
}

}  // namespace

struct Core;
class Link;
struct Dial;
struct Inbound;

struct Core : std::enable_shared_from_this<Core> {
    Core(net::io_context& io_, std::shared_ptr<net::ssl::context> ctx_, TransportOptions opts)
        : io(io_), ctx(std::move(ctx_)), options(std::move(opts)), acceptor(io_), accept_retry(io_) {}

    net::io_context& io;
    std::shared_ptr<net::ssl::context> ctx;
    TransportOptions options;
    TransportHandlers handlers;
    tcp::acceptor acceptor;
    net::steady_timer accept_retry;
    std::uint16_t port = 0;
    std::uint64_t next_link_id = 1;
    bool stopped = false;

    std::unordered_map<std::uint64_t, std::shared_ptr<Link>> links;  // handed out, not yet closed
    std::unordered_map<Dial*, std::shared_ptr<Dial>> dials;          // in progress
    std::unordered_map<Inbound*, std::shared_ptr<Inbound>> inbound;  // accepted, handshaking
    std::size_t inbound_links = 0;                                    // open inbound links

    void do_accept();
    void on_accepted(tcp::socket sock);
    void on_inbound_handshake(const std::shared_ptr<Inbound>& in, error_code ec);
    std::shared_ptr<Link> make_link(SslStream stream, bool outbound, tls::Identity identity, std::string remote);
    void stop_all(bool notify);
};

struct TlsTransport::Impl : Core {
    using Core::Core;
};

// ---- Link ------------------------------------------------------------------------

class Link final : public IFrameLink, public std::enable_shared_from_this<Link> {
public:
    Link(SslStream stream, std::weak_ptr<Core> owner, std::uint64_t id, bool outbound,
         tls::Identity identity, std::string remote, std::size_t max_queued)
        : stream_(std::move(stream)),
          owner_(std::move(owner)),
          id_(id),
          outbound_(outbound),
          identity_(std::move(identity)),
          remote_(std::move(remote)),
          max_queued_(max_queued),
          timer_(stream_.get_executor()) {}

    std::uint64_t id() const override { return id_; }
    bool outbound() const override { return outbound_; }
    const tls::Identity& identity() const override { return identity_; }
    std::string remote() const override { return remote_; }
    bool is_open() const override { return phase_ == Phase::kOpen; }

    void send(const protocol::Frame& frame) override {
        if (phase_ != Phase::kOpen) return;
        std::string bytes;
        try {
            bytes = protocol::encode_frame(frame);
        } catch (const std::exception& e) {
            fail(std::string("send failed: ") + e.what());
            return;
        }
        if (queued_bytes_ + bytes.size() > max_queued_) {
            fail("send queue overflow");
            return;
        }
        queued_bytes_ += bytes.size();
        queue_.push_back(std::move(bytes));
        write_next();
    }

    void close() override { close_with("closed locally"); }

    // Graceful local close: flush (bounded), TLS shutdown (bounded), socket close.
    void close_with(const std::string& reason) {
        if (phase_ != Phase::kOpen) return;
        reason_ = reason;
        phase_ = Phase::kFlushing;
        if (queue_.empty() && !writing_) return begin_shutdown();
        timer_.expires_after(kFlushTimeout);
        timer_.async_wait([self = shared_from_this()](error_code ec) {
            if (!ec && self->phase_ == Phase::kFlushing) self->begin_shutdown();
        });
    }

    // Immediate close (errors, transport destruction). on_close is posted.
    void fail(const std::string& reason) {
        if (phase_ == Phase::kClosed) return;
        if (reason_.empty()) reason_ = reason;
        finalize();
    }

    void start_read() {
        if ((phase_ != Phase::kOpen && phase_ != Phase::kFlushing) || reading_) return;
        reading_ = true;
        stream_.async_read_some(net::buffer(rbuf_), [self = shared_from_this()](error_code ec, std::size_t n) {
            self->on_read(ec, n);
        });
    }

private:
    enum class Phase { kOpen, kFlushing, kShutdown, kClosed };

    void on_read(error_code ec, std::size_t n) {
        reading_ = false;
        if (phase_ == Phase::kClosed) return;
        if (phase_ == Phase::kShutdown) return maybe_shutdown();
        if (ec) {
            if (is_peer_eof(ec)) return fail("closed by peer");
            return fail("read error: " + ec.message());
        }
        if (phase_ == Phase::kOpen) {
            decoder_.feed(std::string_view(rbuf_.data(), n));
            try {
                while (phase_ == Phase::kOpen) {
                    auto frame = decoder_.next();
                    if (!frame) break;
                    auto owner = owner_.lock();
                    if (!owner) break;
                    auto on_frame = owner->handlers.on_frame;  // copy: the handler may replace handlers
                    owner.reset();
                    if (on_frame) on_frame(shared_from_this(), std::move(*frame));
                }
            } catch (const protocol::ProtocolError& e) {
                return fail(std::string("protocol error: ") + e.what());
            }
        }
        // While flushing, keep reading (and discarding) so the shutdown can cancel it.
        if (phase_ == Phase::kOpen || phase_ == Phase::kFlushing) start_read();
        else if (phase_ == Phase::kShutdown) maybe_shutdown();
    }

    void write_next() {
        if (writing_ || queue_.empty() || phase_ == Phase::kClosed || phase_ == Phase::kShutdown) return;
        writing_ = true;
        net::async_write(stream_, net::buffer(queue_.front()),
                         [self = shared_from_this()](error_code ec, std::size_t) { self->on_write(ec); });
    }

    void on_write(error_code ec) {
        writing_ = false;
        if (phase_ == Phase::kClosed) return;
        if (!queue_.empty()) {
            queued_bytes_ -= queue_.front().size();
            queue_.pop_front();
        }
        if (phase_ == Phase::kShutdown) return maybe_shutdown();
        if (ec) {
            if (is_peer_eof(ec)) return fail("closed by peer");
            return fail("write error: " + ec.message());
        }
        if (!queue_.empty()) return write_next();
        if (phase_ == Phase::kFlushing) begin_shutdown();
    }

    void begin_shutdown() {
        if (phase_ != Phase::kOpen && phase_ != Phase::kFlushing) return;
        phase_ = Phase::kShutdown;
        timer_.expires_after(kShutdownTimeout);
        timer_.async_wait([self = shared_from_this()](error_code ec) {
            if (!ec && self->phase_ == Phase::kShutdown) self->finalize();
        });
        // The pending read (and a write still stuck after the flush timeout)
        // must finish before async_shutdown may use the stream.
        error_code ignored;
        stream_.lowest_layer().cancel(ignored);
        maybe_shutdown();
    }

    void maybe_shutdown() {
        if (phase_ != Phase::kShutdown || reading_ || writing_ || shutdown_started_) return;
        shutdown_started_ = true;
        stream_.async_shutdown([self = shared_from_this()](error_code) { self->finalize(); });
    }

    void finalize() {
        if (phase_ == Phase::kClosed) return;
        phase_ = Phase::kClosed;
        timer_.cancel();
        error_code ignored;
        stream_.lowest_layer().cancel(ignored);
        stream_.lowest_layer().close(ignored);
        // An in-flight async_write still references queue_.front(): keep it
        // until the (aborted) write completes; the op holds a ref to us.
        while (queue_.size() > (writing_ ? 1u : 0u)) queue_.pop_back();
        queued_bytes_ = 0;

        auto owner = owner_.lock();
        if (!owner) return;
        if (owner->links.erase(id_) > 0 && !outbound_ && owner->inbound_links > 0) --owner->inbound_links;
        net::post(owner->io, [w = owner_, self = shared_from_this()] {
            auto o = w.lock();
            if (!o) return;
            auto on_close = o->handlers.on_close;
            o.reset();
            if (on_close) on_close(self, self->reason_);
        });
    }

    SslStream stream_;
    std::weak_ptr<Core> owner_;
    std::uint64_t id_;
    bool outbound_;
    tls::Identity identity_;
    std::string remote_;
    std::size_t max_queued_;
    net::steady_timer timer_;

    Phase phase_ = Phase::kOpen;
    std::string reason_;
    protocol::FrameDecoder decoder_;
    std::array<char, kReadChunk> rbuf_{};
    std::deque<std::string> queue_;
    std::size_t queued_bytes_ = 0;
    bool reading_ = false;
    bool writing_ = false;
    bool shutdown_started_ = false;
};

// ---- Inbound handshake -------------------------------------------------------------

struct Inbound {
    Inbound(tcp::socket sock, net::ssl::context& ctx)
        : remote(remote_of(sock)), stream(std::move(sock), ctx), deadline(stream.get_executor()) {}
    std::string remote;
    SslStream stream;
    net::steady_timer deadline;
    bool done = false;

    void abort() {
        done = true;
        deadline.cancel();
        error_code ignored;
        stream.lowest_layer().close(ignored);
    }
};

// ---- Dial ------------------------------------------------------------------------

struct Dial : std::enable_shared_from_this<Dial> {
    Dial(std::weak_ptr<Core> owner_, net::io_context& io, net::ssl::context& ctx, std::string host_,
         std::uint16_t port_, std::string expected_, std::function<void(DialResult)> cb_)
        : owner(std::move(owner_)),
          host(std::move(host_)),
          port(port_),
          expected(std::move(expected_)),
          cb(std::move(cb_)),
          resolver(io),
          stream(io, ctx),
          deadline(io) {
        if (host.size() >= 2 && host.front() == '[' && host.back() == ']') host = host.substr(1, host.size() - 2);
    }

    std::weak_ptr<Core> owner;
    std::string host;
    std::uint16_t port;
    std::string expected;
    std::function<void(DialResult)> cb;
    tcp::resolver resolver;
    SslStream stream;
    net::steady_timer deadline;
    bool done = false;

    std::string where() const { return host_port(host, port); }

    void start(std::chrono::milliseconds timeout) {
        deadline.expires_after(timeout);
        deadline.async_wait([w = weak_from_this(), timeout](error_code ec) {
            if (ec) return;
            if (auto self = w.lock())
                self->finish(std::unexpected(
                    std::format("connect {}: timeout after {} ms", self->where(), timeout.count())));
        });
        resolver.async_resolve(host, std::to_string(port),
                               [self = shared_from_this()](error_code ec, tcp::resolver::results_type results) {
                                   if (self->done) return;
                                   if (ec)
                                       return self->finish(std::unexpected(
                                           std::format("resolve {}: {}", self->host, ec.message())));
                                   net::async_connect(self->stream.lowest_layer(), results,
                                                      [self](error_code ec2, const tcp::endpoint&) {
                                                          self->on_connect(ec2);
                                                      });
                               });
    }

    void on_connect(error_code ec) {
        if (done) return;
        if (ec) return finish(std::unexpected(std::format("connect {}: {}", where(), ec.message())));
        error_code ignored;
        stream.lowest_layer().set_option(tcp::no_delay(true), ignored);
        // SNI only (no hostname verification: the CN is checked below).
        SSL_set_tlsext_host_name(stream.native_handle(), expected.c_str());
        stream.async_handshake(net::ssl::stream_base::client,
                               [self = shared_from_this()](error_code ec2) { self->on_handshake(ec2); });
    }

    void on_handshake(error_code ec) {
        if (done) return;
        if (ec) return finish(std::unexpected(std::format("tls: handshake with {} failed: {}", where(), ec.message())));
        auto id = tls::peer_identity(stream.native_handle());
        if (!id) return finish(std::unexpected(std::format("tls: {} presented no valid certificate", where())));
        if (id->role != tls::Role::kNode || id->name != expected)
            return finish(std::unexpected(std::format("identity mismatch: expected {}, got {}", expected,
                                                      id->role == tls::Role::kNode ? id->name : id->actor())));
        auto o = owner.lock();
        if (!o) return abort();
        done = true;
        deadline.cancel();
        resolver.cancel();
        o->dials.erase(this);
        std::string remote = remote_of(stream.lowest_layer());
        auto link = o->make_link(std::move(stream), true, std::move(*id), std::move(remote));
        net::post(o->io, [w = owner, link, cb = std::move(cb)] {
            if (w.expired()) return;
            if (cb) cb(std::shared_ptr<IFrameLink>(link));
            if (!w.expired()) link->start_read();
        });
    }

    // Reports an error (posted) and tears down.
    void finish(DialResult r) {
        if (done) return;
        auto o = owner.lock();
        abort();
        if (!o) return;
        net::post(o->io, [w = owner, cb = std::move(cb), r = std::move(r)]() mutable {
            if (!w.expired() && cb) cb(std::move(r));
        });
        cb = nullptr;
        o->dials.erase(this);  // may destroy *this: last statement
    }

    // Silent teardown (no callback).
    void abort() {
        done = true;
        deadline.cancel();
        resolver.cancel();
        error_code ignored;
        stream.lowest_layer().close(ignored);
    }
};

// ---- Impl --------------------------------------------------------------------------

std::shared_ptr<Link> Core::make_link(SslStream stream, bool outbound, tls::Identity identity,
                                                    std::string remote) {
    auto link = std::make_shared<Link>(std::move(stream), weak_from_this(), next_link_id++, outbound,
                                       std::move(identity), std::move(remote), options.max_queued_bytes);
    links.emplace(link->id(), link);
    if (!outbound) ++inbound_links;
    return link;
}

void Core::do_accept() {
    if (stopped || !acceptor.is_open()) return;
    acceptor.async_accept(io, [w = weak_from_this()](error_code ec, tcp::socket sock) {
        auto self = w.lock();
        if (!self || self->stopped) return;
        if (ec) {
            if (ec == net::error::operation_aborted) return;
            log_warn("peer listener: accept failed: {}", ec.message());
            self->accept_retry.expires_after(kAcceptRetry);
            self->accept_retry.async_wait([w](error_code ec2) {
                if (ec2) return;
                if (auto s = w.lock()) s->do_accept();
            });
            return;
        }
        self->on_accepted(std::move(sock));
        self->do_accept();
    });
}

void Core::on_accepted(tcp::socket sock) {
    if (inbound_links + inbound.size() >= options.max_links) {
        log_warn("peer {}: rejected: too many inbound links (max {})", remote_of(sock), options.max_links);
        error_code ignored;
        sock.close(ignored);
        return;
    }
    error_code ignored;
    sock.set_option(tcp::no_delay(true), ignored);
    auto in = std::make_shared<Inbound>(std::move(sock), *ctx);
    inbound.emplace(in.get(), in);
    in->deadline.expires_after(options.handshake_timeout);
    in->deadline.async_wait([w = weak_from_this(), wi = std::weak_ptr<Inbound>(in)](error_code ec) {
        if (ec) return;
        auto self = w.lock();
        auto i = wi.lock();
        if (!self || !i || i->done) return;
        log_warn("peer {}: rejected: tls handshake timeout", i->remote);
        i->abort();
        self->inbound.erase(i.get());
    });
    in->stream.async_handshake(net::ssl::stream_base::server, [w = weak_from_this(), in](error_code ec) {
        if (auto self = w.lock()) self->on_inbound_handshake(in, ec);
    });
}

void Core::on_inbound_handshake(const std::shared_ptr<Inbound>& in, error_code ec) {
    if (in->done) return;
    auto reject = [&](const std::string& why) {
        log_warn("peer {}: rejected: {}", in->remote, why);
        in->abort();
        inbound.erase(in.get());
    };
    if (ec) return reject("tls handshake failed: " + ec.message());
    auto id = tls::peer_identity(in->stream.native_handle());
    if (!id) return reject("no valid client certificate");
    if (id->role != tls::Role::kNode) return reject("not a node certificate: " + id->actor());
    in->done = true;
    in->deadline.cancel();
    inbound.erase(in.get());
    auto link = make_link(std::move(in->stream), false, std::move(*id), in->remote);
    auto on_accept = handlers.on_accept;
    if (on_accept) on_accept(link);
    link->start_read();  // no-op if on_accept closed it or stopped the transport
}

void Core::stop_all(bool notify) {
    stopped = true;
    error_code ignored;
    acceptor.close(ignored);
    accept_retry.cancel();
    for (auto& [_, in] : inbound) in->abort();
    inbound.clear();

    std::vector<std::shared_ptr<Dial>> pending;
    for (auto& [_, d] : dials) pending.push_back(d);
    for (auto& d : pending) {
        if (notify) d->finish(std::unexpected(std::string("transport stopped")));
        else d->abort();
    }
    dials.clear();

    std::vector<std::shared_ptr<Link>> open;
    for (auto& [_, l] : links) open.push_back(l);
    for (auto& l : open) {
        if (notify) l->close_with("transport stopped");
        else l->fail("transport destroyed");
    }
}

// ---- TlsTransport ----------------------------------------------------------------

TlsTransport::TlsTransport(net::io_context& io, std::shared_ptr<net::ssl::context> ctx, TransportOptions options)
    : impl_(std::make_shared<Impl>(io, std::move(ctx), std::move(options))) {}

TlsTransport::~TlsTransport() {
    impl_->handlers = {};
    impl_->stop_all(false);
    impl_->links.clear();
}

void TlsTransport::set_handlers(TransportHandlers handlers) { impl_->handlers = std::move(handlers); }

void TlsTransport::dial(const std::string& host, std::uint16_t port, const std::string& expected_name,
                        std::function<void(DialResult)> cb) {
    auto& impl = *impl_;
    if (impl.stopped) {
        net::post(impl.io, [w = std::weak_ptr<Core>(impl_), cb = std::move(cb)] {
            if (!w.expired() && cb) cb(std::unexpected(std::string("transport stopped")));
        });
        return;
    }
    auto d = std::make_shared<Dial>(impl_, impl.io, *impl.ctx, host, port, expected_name, std::move(cb));
    impl.dials.emplace(d.get(), d);
    // Start on a later turn so the callback can never run inside dial().
    net::post(impl.io, [wd = std::weak_ptr<Dial>(d), timeout = impl.options.connect_timeout] {
        if (auto dd = wd.lock(); dd && !dd->done) dd->start(timeout);
    });
}

void TlsTransport::start() {
    auto& impl = *impl_;
    error_code ec;
    auto addr = net::ip::make_address(impl.options.listen_address, ec);
    if (ec)
        throw std::system_error(ec, "invalid peer listen_address '" + impl.options.listen_address + "'");
    tcp::endpoint ep(addr, impl.options.listen_port);
    std::string where = endpoint_string(ep);
    impl.acceptor.open(ep.protocol(), ec);
    if (!ec) impl.acceptor.set_option(tcp::acceptor::reuse_address(true), ec);
    if (!ec) impl.acceptor.bind(ep, ec);
    if (!ec) impl.acceptor.listen(net::socket_base::max_listen_connections, ec);
    if (ec) {
        error_code ignored;
        impl.acceptor.close(ignored);
        throw std::system_error(ec, "peer listener " + where);
    }
    impl.port = impl.acceptor.local_endpoint().port();
    impl.stopped = false;
    log_info("peer listener on {}", endpoint_string(impl.acceptor.local_endpoint()));
    impl.do_accept();
}

void TlsTransport::stop() { impl_->stop_all(true); }

std::uint16_t TlsTransport::bound_port() const { return impl_->port; }

}  // namespace tdr::peer
