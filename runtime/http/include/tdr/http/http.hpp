// Minimal HTTPS (HTTP/1.1) server and clients on Boost.Beast + Asio SSL.
// Used by tdrcp (server), tdrd (async client) and tdrctl (sync client).
//
// Threading: the server and AsyncClient run on the io_context thread given to
// them; handlers and callbacks are invoked on that thread.
#pragma once

#include <chrono>
#include <cstdint>
#include <expected>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <boost/asio/io_context.hpp>
#include <boost/asio/ssl/context.hpp>

#include "tdr/tls/tls.hpp"

namespace tdr::http {

struct Request {
    std::string method;                               // "GET", "POST", "PATCH", ...
    std::string path;                                 // target without the query string
    std::map<std::string, std::string> query;         // decoded ?a=b&c=d
    std::map<std::string, std::string> params;        // filled by Router from {name} segments
    std::string body;
    std::string content_type;
    std::optional<tls::Identity> peer;                // verified client identity
    std::string remote;                               // "ip:port"
};

struct Response {
    int status = 200;
    std::string body;
    std::string content_type = "application/json";
};

using Handler = std::function<Response(Request&)>;

// Routes "METHOD /a/{id}/b" patterns. Exact segment match; {name} captures one
// segment into Request::params. Unknown path -> 404, known path with another
// method -> 405 (both with a JSON ApiError body).
class Router {
public:
    void add(const std::string& method, const std::string& pattern, Handler handler);
    Response dispatch(Request& req) const;

private:
    struct Route {
        std::string method;
        std::vector<std::string> segments;
        Handler handler;
    };
    std::vector<Route> routes_;
};

// JSON error body: {"code": code, "message": message}
Response error_response(int status, const std::string& code, const std::string& message);

struct ServerOptions {
    std::string listen_address = "0.0.0.0";
    std::uint16_t port = 8080;                        // 0 = ephemeral (tests); see bound_port()
    std::size_t max_body_bytes = 1 << 20;             // larger -> 413
    std::chrono::seconds handshake_timeout{10};
    std::chrono::seconds read_timeout{30};            // per request
    std::chrono::seconds idle_timeout{60};            // keep-alive
    std::size_t max_connections = 1024;
};

// HTTPS server with keep-alive. Every connection must present a client
// certificate accepted by `ctx` (use tls::make_server_context). The peer
// identity is attached to each Request. Exceptions thrown by the handler
// become 500 responses (logged).
class HttpsServer {
public:
    HttpsServer(boost::asio::io_context& io, std::shared_ptr<boost::asio::ssl::context> ctx,
                ServerOptions options, Handler handler);
    ~HttpsServer();
    HttpsServer(const HttpsServer&) = delete;
    HttpsServer& operator=(const HttpsServer&) = delete;

    void start();                       // throws std::system_error on bind failure
    void stop();                        // closes listener and connections
    std::uint16_t bound_port() const;

private:
    struct Impl;
    std::shared_ptr<Impl> impl_;
};

struct ClientOptions {
    std::string host;                   // DNS name or IP
    std::uint16_t port = 8080;
    std::string verify_host;            // name checked against the server cert; default = host
    std::chrono::milliseconds timeout{10000};  // whole request (connect + TLS + exchange)
};

struct ClientResponse {
    int status = 0;
    std::string body;
};

// Parse "https://host:port" (port default 8080). IPv6 as [::1].
std::expected<ClientOptions, std::string> parse_url(const std::string& url);

// Asynchronous client. One TLS connection per request (simple and robust;
// control-plane traffic is a request every few seconds). Requests may overlap.
class AsyncClient {
public:
    using Callback = std::function<void(std::expected<ClientResponse, std::string>)>;

    AsyncClient(boost::asio::io_context& io, std::shared_ptr<boost::asio::ssl::context> ctx, ClientOptions options);
    ~AsyncClient();  // pending callbacks are not called after destruction

    // body is sent as application/json when non-empty. The callback is always
    // called exactly once (later, on the io thread) unless the client is destroyed.
    void request(const std::string& method, const std::string& target, std::string body, Callback cb);

private:
    struct Impl;
    std::shared_ptr<Impl> impl_;
};

// Blocking client for CLIs. Error string on transport/TLS failure.
std::expected<ClientResponse, std::string> request_sync(boost::asio::ssl::context& ctx, const ClientOptions& options,
                                                         const std::string& method, const std::string& target,
                                                         const std::string& body = {});

}  // namespace tdr::http
