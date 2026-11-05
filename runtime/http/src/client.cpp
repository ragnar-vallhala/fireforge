// AsyncClient and request_sync: one TLS connection per request, whole-request deadline.
#include <unordered_map>

#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/ssl.hpp>

#include "tdr/common/common.hpp"
#include "tdr/http/http.hpp"

namespace tdr::http {

namespace net = boost::asio;
namespace beast = boost::beast;
namespace bhttp = boost::beast::http;
using tcp = net::ip::tcp;
using error_code = boost::system::error_code;

namespace {

constexpr std::size_t kMaxResponseBody = 64u << 20;

using Result = std::expected<ClientResponse, std::string>;

// Liveness token shared by an owner (AsyncClient / request_sync) and its ops.
struct Owner {
    std::unordered_map<void*, std::function<void()>> cancel;  // live ops -> abort
};

class Op : public std::enable_shared_from_this<Op> {
public:
    Op(net::io_context& io, std::shared_ptr<net::ssl::context> ctx, const ClientOptions& options,
       std::weak_ptr<Owner> owner, AsyncClient::Callback cb)
        : ctx_(std::move(ctx)),
          options_(options),
          owner_(std::move(owner)),
          cb_(std::move(cb)),
          resolver_(io),
          stream_(io, *ctx_),
          deadline_(io) {}

    ~Op() {
        if (auto o = owner_.lock()) o->cancel.erase(this);
    }

    void start(const std::string& method, const std::string& target, std::string body) {
        auto o = owner_.lock();
        if (!o) return;  // owner destroyed before the op started
        o->cancel.emplace(this, [w = weak_from_this()] {
            if (auto self = w.lock()) self->abort();
        });
        o.reset();

        auto verb = bhttp::string_to_verb(method);
        if (verb == bhttp::verb::unknown) req_.method_string(method);
        else req_.method(verb);
        req_.target(target.empty() ? std::string("/") : target);
        req_.version(11);
        const bool v6 = options_.host.find(':') != std::string::npos;
        req_.set(bhttp::field::host,
                 v6 ? std::format("[{}]:{}", options_.host, options_.port) : std::format("{}:{}", options_.host, options_.port));
        req_.set(bhttp::field::user_agent, "fireforge/" TDR_VERSION);
        if (!body.empty()) req_.set(bhttp::field::content_type, "application/json");
        req_.body() = std::move(body);
        req_.keep_alive(false);
        req_.prepare_payload();

        parser_.body_limit(kMaxResponseBody);

        deadline_.expires_after(options_.timeout);
        deadline_.async_wait([w = weak_from_this()](error_code ec) {
            if (ec) return;
            if (auto self = w.lock()) self->finish(std::unexpected(std::format(
                                          "request to {}:{} timed out after {} ms", self->options_.host,
                                          self->options_.port, self->options_.timeout.count())));
        });

        resolver_.async_resolve(options_.host, std::to_string(options_.port),
                                [self = shared_from_this()](error_code ec, tcp::resolver::results_type results) {
                                    self->on_resolve(ec, std::move(results));
                                });
    }

    // Owner destroyed: stop everything, never call back.
    void abort() {
        cb_ = nullptr;
        done_ = true;
        teardown();
    }

private:
    std::string where() const { return std::format("{}:{}", options_.host, options_.port); }

    void on_resolve(error_code ec, tcp::resolver::results_type results) {
        if (done_) return;
        if (ec) return finish(std::unexpected(std::format("cannot resolve {}: {}", options_.host, ec.message())));
        beast::get_lowest_layer(stream_).async_connect(
            results, [self = shared_from_this()](error_code ec2, const tcp::endpoint&) { self->on_connect(ec2); });
    }

    void on_connect(error_code ec) {
        if (done_) return;
        if (ec) return finish(std::unexpected(std::format("cannot connect to {}: {}", where(), ec.message())));
        const std::string& vh = options_.verify_host.empty() ? options_.host : options_.verify_host;
        if (!tls::set_verify_host(stream_.native_handle(), vh))
            return finish(std::unexpected("cannot configure TLS host verification for " + vh));
        stream_.async_handshake(net::ssl::stream_base::client,
                                [self = shared_from_this()](error_code ec2) { self->on_handshake(ec2); });
    }

    void on_handshake(error_code ec) {
        if (done_) return;
        if (ec) return finish(std::unexpected(std::format("TLS handshake with {} failed: {}", where(), ec.message())));
        bhttp::async_write(stream_, req_,
                           [self = shared_from_this()](error_code ec2, std::size_t) { self->on_write(ec2); });
    }

    void on_write(error_code ec) {
        if (done_) return;
        if (ec) return finish(std::unexpected(std::format("sending request to {} failed: {}", where(), ec.message())));
        bhttp::async_read(stream_, buffer_, parser_,
                          [self = shared_from_this()](error_code ec2, std::size_t) { self->on_read(ec2); });
    }

    void on_read(error_code ec) {
        if (done_) return;
        if (ec) return finish(std::unexpected(std::format("reading response from {} failed: {}", where(), ec.message())));
        auto& msg = parser_.get();
        ClientResponse r;
        r.status = static_cast<int>(msg.result_int());
        r.body = std::move(msg.body());
        deliver(std::move(r));
        deadline_.cancel();
        beast::get_lowest_layer(stream_).expires_after(std::chrono::seconds(1));
        // Graceful TLS close, bounded to 1 s (errors ignored:
        // the server may already have closed after "Connection: close").
        stream_.async_shutdown([self = shared_from_this()](error_code) { self->teardown(); });
    }

    void deliver(Result r) {
        if (done_) return;
        done_ = true;
        auto cb = std::move(cb_);
        cb_ = nullptr;
        if (cb && !owner_.expired()) cb(std::move(r));
    }

    void finish(Result r) {
        deliver(std::move(r));
        teardown();
    }

    void teardown() {
        if (torn_down_) return;
        torn_down_ = true;
        deadline_.cancel();
        resolver_.cancel();
        error_code ec;
        beast::get_lowest_layer(stream_).socket().close(ec);
    }

    std::shared_ptr<net::ssl::context> ctx_;
    ClientOptions options_;
    std::weak_ptr<Owner> owner_;
    AsyncClient::Callback cb_;
    tcp::resolver resolver_;
    beast::ssl_stream<beast::tcp_stream> stream_;
    net::steady_timer deadline_;
    bhttp::request<bhttp::string_body> req_;
    bhttp::response_parser<bhttp::string_body> parser_;
    beast::flat_buffer buffer_;
    bool done_ = false;
    bool torn_down_ = false;
};

}  // namespace

struct AsyncClient::Impl {
    net::io_context& io;
    std::shared_ptr<net::ssl::context> ctx;
    ClientOptions options;
    std::shared_ptr<Owner> owner = std::make_shared<Owner>();
};

AsyncClient::AsyncClient(net::io_context& io, std::shared_ptr<net::ssl::context> ctx, ClientOptions options)
    : impl_(std::make_shared<Impl>(Impl{io, std::move(ctx), std::move(options)})) {}

AsyncClient::~AsyncClient() {
    // Abort live ops; their callbacks are dropped because the owner is gone.
    auto owner = std::move(impl_->owner);
    std::vector<std::function<void()>> aborts;
    for (auto& [_, f] : owner->cancel) aborts.push_back(f);
    owner.reset();
    for (auto& f : aborts) f();
}

void AsyncClient::request(const std::string& method, const std::string& target, std::string body, Callback cb) {
    auto op = std::make_shared<Op>(impl_->io, impl_->ctx, impl_->options, impl_->owner, std::move(cb));
    // Start on a later turn of the io loop so the callback never runs inside request().
    net::post(impl_->io, [op, method, target, body = std::move(body)]() mutable {
        op->start(method, target, std::move(body));
    });
}

std::expected<ClientResponse, std::string> request_sync(net::ssl::context& ctx, const ClientOptions& options,
                                                         const std::string& method, const std::string& target,
                                                         const std::string& body) {
    net::io_context io;
    auto owner = std::make_shared<Owner>();
    // Non-owning shared_ptr: ctx outlives this call.
    std::shared_ptr<net::ssl::context> ctx_ref(std::shared_ptr<void>{}, &ctx);
    std::optional<Result> result;
    auto op = std::make_shared<Op>(io, ctx_ref, options, owner, [&result](Result r) { result = std::move(r); });
    op->start(method, target, body);
    op.reset();
    io.run();
    if (!result) return std::unexpected("request to " + options.host + " did not complete");
    return std::move(*result);
}

}  // namespace tdr::http
