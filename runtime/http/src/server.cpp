// HttpsServer: Boost.Beast HTTP/1.1 over TLS (mTLS), keep-alive, single io thread.
#include <cctype>
#include <optional>
#include <system_error>
#include <unordered_map>

#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl.hpp>
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

int hex_value(char c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
}

std::string percent_decode(std::string_view s) {
    std::string out;
    out.reserve(s.size());
    for (std::size_t i = 0; i < s.size(); ++i) {
        char c = s[i];
        if (c == '+') {
            out.push_back(' ');
        } else if (c == '%' && i + 2 < s.size() && hex_value(s[i + 1]) >= 0 &&
                   hex_value(s[i + 2]) >= 0) {
            out.push_back(static_cast<char>(hex_value(s[i + 1]) * 16 + hex_value(s[i + 2])));
            i += 2;
        } else {
            out.push_back(c);
        }
    }
    return out;
}

std::map<std::string, std::string> parse_query(std::string_view q) {
    std::map<std::string, std::string> out;
    while (!q.empty()) {
        auto amp = q.find('&');
        std::string_view part = q.substr(0, amp);
        q = amp == std::string_view::npos ? std::string_view{} : q.substr(amp + 1);
        if (part.empty()) continue;
        auto eq = part.find('=');
        std::string key = percent_decode(part.substr(0, eq));
        std::string val = eq == std::string_view::npos ? std::string() : percent_decode(part.substr(eq + 1));
        if (!key.empty()) out.insert_or_assign(std::move(key), std::move(val));
    }
    return out;
}

std::string endpoint_string(const tcp::endpoint& ep) {
    auto addr = ep.address();
    if (addr.is_v6()) return std::format("[{}]:{}", addr.to_string(), ep.port());
    return std::format("{}:{}", addr.to_string(), ep.port());
}

}  // namespace

namespace {

class Session;

struct ServerCore : std::enable_shared_from_this<ServerCore> {
    ServerCore(net::io_context& io_, std::shared_ptr<net::ssl::context> ctx_, ServerOptions opts, Handler h)
        : io(io_), ctx(std::move(ctx_)), options(std::move(opts)), handler(std::move(h)), acceptor(io_) {}

    void do_accept();
    void stop();

    net::io_context& io;
    std::shared_ptr<net::ssl::context> ctx;
    ServerOptions options;
    Handler handler;
    tcp::acceptor acceptor;
    std::uint16_t port = 0;
    bool stopped = false;
    std::unordered_map<Session*, std::weak_ptr<Session>> sessions;
};

class Session : public std::enable_shared_from_this<Session> {
public:
    Session(std::shared_ptr<ServerCore> server, tcp::socket socket, std::string remote)
        : server_(std::move(server)), stream_(std::move(socket), *server_->ctx), remote_(std::move(remote)) {}

    ~Session() { server_->sessions.erase(this); }

    void start() {
        server_->sessions.emplace(this, weak_from_this());
        beast::get_lowest_layer(stream_).expires_after(server_->options.handshake_timeout);
        stream_.async_handshake(net::ssl::stream_base::server,
                                [self = shared_from_this()](error_code ec) { self->on_handshake(ec); });
    }

    void close() {
        if (closed_) return;
        closed_ = true;
        error_code ec;
        beast::get_lowest_layer(stream_).socket().shutdown(tcp::socket::shutdown_both, ec);
        beast::get_lowest_layer(stream_).close();
    }

private:
    void on_handshake(error_code ec) {
        if (closed_) return;
        if (ec) {
            log_debug("https: TLS handshake with {} failed: {}", remote_, ec.message());
            return close();
        }
        peer_ = tls::peer_identity(stream_.native_handle());
        do_read();
    }

    void do_read() {
        if (closed_ || server_->stopped) return close();
        parser_.emplace();
        parser_->body_limit(server_->options.max_body_bytes);
        beast::get_lowest_layer(stream_).expires_after(first_ ? server_->options.read_timeout
                                                              : server_->options.idle_timeout);
        bhttp::async_read_header(stream_, buffer_, *parser_,
                                 [self = shared_from_this()](error_code ec, std::size_t) { self->on_header(ec); });
    }

    void on_header(error_code ec) {
        if (closed_) return;
        if (ec) return on_read_error(ec);
        beast::get_lowest_layer(stream_).expires_after(server_->options.read_timeout);
        bhttp::async_read(stream_, buffer_, *parser_,
                          [self = shared_from_this()](error_code ec2, std::size_t) { self->on_read(ec2); });
    }

    void on_read_error(error_code ec) {
        if (ec == bhttp::error::body_limit) {
            log_warn("https: request body from {} exceeds {} bytes", remote_, server_->options.max_body_bytes);
            auto r = error_response(413, "payload_too_large",
                                    std::format("body exceeds {} bytes", server_->options.max_body_bytes));
            return send(std::move(r), /*keep_alive=*/false, /*drain=*/true);
        }
        if (ec == bhttp::error::end_of_stream || ec == net::ssl::error::stream_truncated || ec == net::error::eof) {
            return do_shutdown();
        }
        if (ec == beast::error::timeout) {
            log_debug("https: connection from {} timed out", remote_);
        } else if (ec != net::error::operation_aborted) {
            log_debug("https: read from {} failed: {}", remote_, ec.message());
            if (ec.category() == bhttp::make_error_code(bhttp::error::bad_target).category()) {
                // Malformed HTTP: tell the client before closing.
                return send(error_response(400, "bad_request", "malformed HTTP request: " + ec.message()), false,
                            false);
            }
        }
        close();
    }

    void on_read(error_code ec) {
        if (closed_) return;
        if (ec) return on_read_error(ec);
        first_ = false;
        auto msg = parser_->release();

        Request req;
        req.method = std::string(msg.method_string());
        std::string_view target = msg.target();
        auto qpos = target.find('?');
        req.path = std::string(target.substr(0, qpos));
        if (qpos != std::string_view::npos) req.query = parse_query(target.substr(qpos + 1));
        req.body = std::move(msg.body());
        if (auto it = msg.find(bhttp::field::content_type); it != msg.end()) req.content_type = std::string(it->value());
        for (const auto& f : msg) {
            std::string name(f.name_string());
            for (auto& ch : name) ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
            auto [it, fresh] = req.headers.try_emplace(std::move(name), std::string(f.value()));
            if (!fresh) it->second += ", " + std::string(f.value());
        }
        req.peer = peer_;
        req.remote = remote_;
        head_ = req.method == "HEAD";

        Response res;
        try {
            res = server_->handler(req);
        } catch (const std::exception& e) {
            log_error("https: handler for {} {} from {} threw: {}", req.method, req.path, remote_, e.what());
            res = error_response(500, "internal", "internal server error");
        } catch (...) {
            log_error("https: handler for {} {} from {} threw an unknown exception", req.method, req.path, remote_);
            res = error_response(500, "internal", "internal server error");
        }
        send(std::move(res), msg.keep_alive() && !server_->stopped, false);
    }

    void send(Response r, bool keep_alive, bool drain) {
        if (closed_) return;
        auto res = std::make_shared<bhttp::response<bhttp::string_body>>();
        res->version(11);
        if (r.status < 100 || r.status > 999) r.status = 500;
        res->result(static_cast<unsigned>(r.status));
        res->set(bhttp::field::server, "fireforge/" TDR_VERSION);
        if (!r.body.empty() || !r.content_type.empty()) res->set(bhttp::field::content_type, r.content_type);
        for (const auto& [k, v] : r.headers) res->set(k, v);
        res->keep_alive(keep_alive);
        if (head_) {
            // HEAD: the GET headers (Content-Length of the body the handler built), no body.
            const auto n = r.body.size();
            res->prepare_payload();
            res->content_length(n);
            head_ = false;
        } else {
            res->body() = std::move(r.body);
            res->prepare_payload();
        }
        beast::get_lowest_layer(stream_).expires_after(server_->options.read_timeout);
        bhttp::async_write(stream_, *res,
                           [self = shared_from_this(), res, keep_alive, drain](error_code ec, std::size_t) {
                               self->on_write(ec, keep_alive, drain);
                           });
    }

    void on_write(error_code ec, bool keep_alive, bool drain) {
        if (closed_) return;
        if (ec) {
            if (ec != net::error::operation_aborted) log_debug("https: write to {} failed: {}", remote_, ec.message());
            return close();
        }
        if (drain) return do_drain();
        if (!keep_alive) return do_shutdown();
        do_read();
    }

    // After an early error response (413) the client may still be sending its
    // body. Discard input until it closes, so closing does not RST away the
    // response it has not read yet.
    void do_drain() {
        if (!drain_buf_) {
            drain_buf_ = std::make_unique<std::array<char, 16384>>();
            beast::get_lowest_layer(stream_).expires_after(
                std::min<std::chrono::steady_clock::duration>(server_->options.read_timeout, std::chrono::seconds(5)));
        }
        stream_.async_read_some(net::buffer(*drain_buf_), [self = shared_from_this()](error_code ec, std::size_t) {
            if (self->closed_) return;
            if (ec) return self->close();
            self->do_drain();
        });
    }

    void do_shutdown() {
        if (closed_) return;
        beast::get_lowest_layer(stream_).expires_after(std::chrono::seconds(2));
        stream_.async_shutdown([self = shared_from_this()](error_code) { self->close(); });
    }

    std::shared_ptr<ServerCore> server_;
    beast::ssl_stream<beast::tcp_stream> stream_;
    std::string remote_;
    beast::flat_buffer buffer_;
    std::optional<bhttp::request_parser<bhttp::string_body>> parser_;
    std::optional<tls::Identity> peer_;
    std::unique_ptr<std::array<char, 16384>> drain_buf_;
    bool first_ = true;
    bool closed_ = false;
    bool head_ = false;  // the request being answered is HEAD
};

void ServerCore::do_accept() {
    if (stopped) return;
    acceptor.async_accept([self = shared_from_this()](error_code ec, tcp::socket socket) {
        if (self->stopped) return;
        if (ec) {
            if (ec == net::error::operation_aborted) return;
            log_warn("https: accept failed: {}", ec.message());
            // Transient (EMFILE etc.): keep accepting.
            return self->do_accept();
        }
        error_code ep_ec;
        auto ep = socket.remote_endpoint(ep_ec);
        std::string remote = ep_ec ? std::string("?") : endpoint_string(ep);
        if (self->sessions.size() >= self->options.max_connections) {
            log_warn("https: refusing connection from {}: {} connections open", remote, self->sessions.size());
            error_code ignored;
            socket.close(ignored);
        } else {
            std::make_shared<Session>(self, std::move(socket), std::move(remote))->start();
        }
        self->do_accept();
    });
}

void ServerCore::stop() {
    if (stopped) return;
    stopped = true;
    error_code ec;
    acceptor.close(ec);
    // Copy: close() may run destructors that erase from the map.
    std::vector<std::shared_ptr<Session>> live;
    for (auto& [_, w] : sessions)
        if (auto s = w.lock()) live.push_back(std::move(s));
    for (auto& s : live) s->close();
}

}  // namespace

struct HttpsServer::Impl {
    std::shared_ptr<ServerCore> core;
};

HttpsServer::HttpsServer(net::io_context& io, std::shared_ptr<net::ssl::context> ctx, ServerOptions options,
                         Handler handler)
    : impl_(std::make_shared<Impl>(Impl{std::make_shared<ServerCore>(io, std::move(ctx), std::move(options), std::move(handler))})) {}

HttpsServer::~HttpsServer() { impl_->core->stop(); }

void HttpsServer::start() {
    auto& a = impl_->core->acceptor;
    const std::string where = std::format("{}:{}", impl_->core->options.listen_address, impl_->core->options.port);
    try {
        auto addr = net::ip::make_address(impl_->core->options.listen_address);
        tcp::endpoint ep(addr, impl_->core->options.port);
        a.open(ep.protocol());
        a.set_option(net::socket_base::reuse_address(true));
        a.bind(ep);
        a.listen(net::socket_base::max_listen_connections);
    } catch (const boost::system::system_error& e) {
        error_code ignored;
        a.close(ignored);
        throw std::system_error(static_cast<std::error_code>(e.code()), "cannot listen on " + where);
    }
    impl_->core->port = a.local_endpoint().port();
    impl_->core->stopped = false;
    log_info("https: listening on {}", endpoint_string(a.local_endpoint()));
    impl_->core->do_accept();
}

void HttpsServer::stop() { impl_->core->stop(); }

std::uint16_t HttpsServer::bound_port() const { return impl_->core->port; }

}  // namespace tdr::http
