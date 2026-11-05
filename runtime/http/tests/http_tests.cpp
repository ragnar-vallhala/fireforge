#include <boost/test/unit_test.hpp>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>

#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/json.hpp>

#include "tdr/common/common.hpp"
#include "tdr/http/http.hpp"
#include "tdr/tls/tls.hpp"

namespace fs = std::filesystem;
namespace net = boost::asio;
namespace beast = boost::beast;
namespace bhttp = boost::beast::http;
namespace json = boost::json;
using tcp = net::ip::tcp;
using namespace tdr;
using namespace tdr::http;
using namespace std::chrono_literals;

namespace {

struct Pki {
    fs::path dir = fs::path("/tmp/claude-1000") / tdr::new_id("http-test");
    fs::path other = dir / "other";

    Pki() {
        fs::create_directories(other);
        must(tls::create_ca(dir, "ca"));
        tls::IssueOptions s;
        s.role = tls::Role::kServer;
        s.name = "cp";
        s.dns_sans = {"localhost"};
        s.ip_sans = {"127.0.0.1"};
        must(tls::issue(dir, s, dir / "cp.key", dir / "cp.crt"));
        tls::IssueOptions n;
        n.name = "node-1";
        must(tls::issue(dir, n, dir / "node-1.key", dir / "node-1.crt"));
        tls::IssueOptions a;
        a.role = tls::Role::kAdmin;
        a.name = "alice";
        must(tls::issue(dir, a, dir / "alice.key", dir / "alice.crt"));
        must(tls::create_ca(other, "other-ca"));
        tls::IssueOptions e;
        e.name = "node-1";
        must(tls::issue(other, e, other / "evil.key", other / "evil.crt"));
    }
    ~Pki() {
        std::error_code ec;
        fs::remove_all(dir, ec);
    }
    static void must(const std::expected<void, std::string>& r) {
        if (!r) throw std::runtime_error(r.error());
    }

    tls::Credentials server() const { return {dir / "ca.crt", dir / "cp.crt", dir / "cp.key"}; }
    tls::Credentials node() const { return {dir / "ca.crt", dir / "node-1.crt", dir / "node-1.key"}; }
    tls::Credentials admin() const { return {dir / "ca.crt", dir / "alice.crt", dir / "alice.key"}; }
    tls::Credentials evil() const { return {dir / "ca.crt", other / "evil.crt", other / "evil.key"}; }
};

const Pki& pki() {
    static Pki p;
    return p;
}

Router make_router() {
    Router r;
    r.add("GET", "/whoami", [](Request& req) {
        json::object o;
        o["actor"] = req.peer ? req.peer->actor() : "";
        o["fingerprint"] = req.peer ? req.peer->fingerprint_sha256 : "";
        o["remote"] = req.remote;
        return Response{200, json::serialize(o)};
    });
    r.add("GET", "/query", [](Request& req) {
        json::object o;
        for (const auto& [k, v] : req.query) o[k] = v;
        o["__path"] = req.path;
        return Response{200, json::serialize(o)};
    });
    r.add("POST", "/echo", [](Request& req) {
        json::object o;
        o["body"] = req.body;
        o["content_type"] = req.content_type;
        return Response{201, json::serialize(o)};
    });
    r.add("GET", "/items/{id}", [](Request& req) { return Response{200, req.params.at("id"), "text/plain"}; });
    r.add("GET", "/throw", [](Request&) -> Response { throw std::runtime_error("boom"); });
    r.add("GET", "/slow", [](Request&) {
        std::this_thread::sleep_for(50ms);
        return Response{200, "{}"};
    });
    return r;
}

struct TestServer {
    net::io_context io;
    std::shared_ptr<Router> router = std::make_shared<Router>(make_router());
    std::unique_ptr<HttpsServer> server;
    std::thread thread;

    explicit TestServer(ServerOptions opts = {}) {
        opts.listen_address = "127.0.0.1";
        opts.port = 0;
        if (opts.max_body_bytes == ServerOptions{}.max_body_bytes) opts.max_body_bytes = 1024;
        auto rt = router;
        server = std::make_unique<HttpsServer>(io, tls::make_server_context(pki().server()), opts,
                                               [rt](Request& req) { return rt->dispatch(req); });
        server->start();
        thread = std::thread([this] { io.run(); });
    }
    void stop() {
        if (!thread.joinable()) return;
        net::post(io, [this] { server->stop(); });
        thread.join();
    }
    ~TestServer() { stop(); }

    ClientOptions client(std::string host = "127.0.0.1") const {
        ClientOptions o;
        o.host = std::move(host);
        o.port = server->bound_port();
        o.verify_host = o.host;
        o.timeout = 3000ms;
        return o;
    }
};

json::object parse(const std::string& s) { return json::parse(s).as_object(); }

}  // namespace

BOOST_AUTO_TEST_CASE(http_valid_cert_and_peer_identity) {
    TestServer ts;
    auto ctx = tls::make_client_context(pki().node());
    auto r = request_sync(*ctx, ts.client(), "GET", "/whoami");
    BOOST_REQUIRE_MESSAGE(r, (r ? "" : r.error()));
    BOOST_TEST(r->status == 200);
    auto o = parse(r->body);
    BOOST_TEST(o["actor"].as_string() == "node:node-1");
    auto id = tls::read_identity(pki().dir / "node-1.crt");
    BOOST_REQUIRE(id);
    BOOST_TEST(o["fingerprint"].as_string() == id->fingerprint_sha256);
    BOOST_TEST(std::string(o["remote"].as_string()).starts_with("127.0.0.1:"));

    auto actx = tls::make_client_context(pki().admin());
    auto r2 = request_sync(*actx, ts.client("localhost"), "GET", "/whoami");
    BOOST_REQUIRE_MESSAGE(r2, (r2 ? "" : r2.error()));
    BOOST_TEST(parse(r2->body)["actor"].as_string() == "admin:alice");
}

BOOST_AUTO_TEST_CASE(http_client_without_cert_rejected) {
    TestServer ts;
    net::ssl::context ctx(net::ssl::context::tls_client);
    ctx.set_options(net::ssl::context::no_tlsv1 | net::ssl::context::no_tlsv1_1);
    ctx.load_verify_file((pki().dir / "ca.crt").string());
    ctx.set_verify_mode(net::ssl::verify_peer);
    auto r = request_sync(ctx, ts.client(), "GET", "/whoami");
    BOOST_REQUIRE(!r.has_value());
    BOOST_TEST_MESSAGE("no cert: " << r.error());
}

BOOST_AUTO_TEST_CASE(http_client_cert_from_other_ca_rejected) {
    TestServer ts;
    auto ctx = tls::make_client_context(pki().evil());
    auto r = request_sync(*ctx, ts.client(), "GET", "/whoami");
    BOOST_REQUIRE(!r.has_value());
    BOOST_TEST_MESSAGE("other CA: " << r.error());
}

BOOST_AUTO_TEST_CASE(http_server_host_mismatch_rejected) {
    TestServer ts;
    auto ctx = tls::make_client_context(pki().node());
    auto o = ts.client();
    o.verify_host = "wrong.example";
    auto r = request_sync(*ctx, o, "GET", "/whoami");
    BOOST_REQUIRE(!r.has_value());
    BOOST_TEST_MESSAGE("host mismatch: " << r.error());
    BOOST_TEST(r.error().find("handshake") != std::string::npos);
    o.verify_host = "10.9.9.9";
    BOOST_TEST(!request_sync(*ctx, o, "GET", "/whoami").has_value());
}

BOOST_AUTO_TEST_CASE(http_router_statuses_query_body) {
    TestServer ts;
    auto ctx = tls::make_client_context(pki().node());
    auto o = ts.client();

    auto nf = request_sync(*ctx, o, "GET", "/nope");
    BOOST_REQUIRE(nf);
    BOOST_TEST(nf->status == 404);
    BOOST_TEST(parse(nf->body)["code"].as_string() == "not_found");

    auto na = request_sync(*ctx, o, "DELETE", "/whoami");
    BOOST_REQUIRE(na);
    BOOST_TEST(na->status == 405);

    auto item = request_sync(*ctx, o, "GET", "/items/abc-1");
    BOOST_REQUIRE(item);
    BOOST_TEST(item->body == "abc-1");

    auto q = request_sync(*ctx, o, "GET", "/query?health=ONLINE&x=a%20b+c&flag&limit=%3D5&&bad=%zz");
    BOOST_REQUIRE(q);
    auto qo = parse(q->body);
    BOOST_TEST(qo["__path"].as_string() == "/query");
    BOOST_TEST(qo["health"].as_string() == "ONLINE");
    BOOST_TEST(qo["x"].as_string() == "a b c");
    BOOST_TEST(qo["flag"].as_string() == "");
    BOOST_TEST(qo["limit"].as_string() == "=5");
    BOOST_TEST(qo["bad"].as_string() == "%zz");

    auto e = request_sync(*ctx, o, "POST", "/echo", R"({"a":1})");
    BOOST_REQUIRE(e);
    BOOST_TEST(e->status == 201);
    auto eo = parse(e->body);
    BOOST_TEST(eo["body"].as_string() == R"({"a":1})");
    BOOST_TEST(eo["content_type"].as_string() == "application/json");

    auto ok = request_sync(*ctx, o, "POST", "/echo", std::string(1000, 'x'));
    BOOST_REQUIRE(ok);
    BOOST_TEST(ok->status == 201);
    auto big = request_sync(*ctx, o, "POST", "/echo", std::string(4096, 'x'));
    BOOST_REQUIRE_MESSAGE(big, (big ? "" : big.error()));
    BOOST_TEST(big->status == 413);
    BOOST_TEST(parse(big->body)["code"].as_string() == "payload_too_large");

    auto th = request_sync(*ctx, o, "GET", "/throw");
    BOOST_REQUIRE(th);
    BOOST_TEST(th->status == 500);
    BOOST_TEST(parse(th->body)["code"].as_string() == "internal");
    BOOST_TEST(th->body.find("boom") == std::string::npos);

    // Server still healthy afterwards.
    auto again = request_sync(*ctx, o, "GET", "/whoami");
    BOOST_REQUIRE(again);
    BOOST_TEST(again->status == 200);
}

BOOST_AUTO_TEST_CASE(http_keep_alive_multiple_requests_one_connection) {
    TestServer ts;
    auto ctx = tls::make_client_context(pki().node());
    net::io_context io;
    beast::ssl_stream<beast::tcp_stream> s(io, *ctx);
    beast::get_lowest_layer(s).expires_after(3s);
    beast::get_lowest_layer(s).connect(tcp::endpoint(net::ip::make_address("127.0.0.1"), ts.server->bound_port()));
    BOOST_REQUIRE(tls::set_verify_host(s.native_handle(), "127.0.0.1"));
    s.handshake(net::ssl::stream_base::client);
    beast::flat_buffer buf;
    for (int i = 0; i < 3; ++i) {
        bhttp::request<bhttp::string_body> req{bhttp::verb::get, "/items/" + std::to_string(i), 11};
        req.set(bhttp::field::host, "127.0.0.1");
        req.keep_alive(true);
        bhttp::write(s, req);
        bhttp::response<bhttp::string_body> res;
        bhttp::read(s, buf, res);
        BOOST_TEST(res.result_int() == 200u);
        BOOST_TEST(res.body() == std::to_string(i));
        BOOST_TEST(res.keep_alive());
    }
    boost::system::error_code ec;
    s.shutdown(ec);
}

BOOST_AUTO_TEST_CASE(http_async_client_concurrent_requests) {
    TestServer ts;
    net::io_context io;
    AsyncClient client(io, tls::make_client_context(pki().node()), ts.client());
    int done = 0, ok = 0;
    constexpr int kN = 8;
    for (int i = 0; i < kN; ++i) {
        client.request("GET", "/items/" + std::to_string(i), {},
                       [&, i](std::expected<ClientResponse, std::string> r) {
                           ++done;
                           if (r && r->status == 200 && r->body == std::to_string(i)) ++ok;
                       });
    }
    BOOST_TEST(done == 0);  // never called inline
    io.run();
    BOOST_TEST(done == kN);
    BOOST_TEST(ok == kN);

    // POST with body over the async client.
    std::optional<std::expected<ClientResponse, std::string>> res;
    client.request("POST", "/echo", R"({"k":"v"})", [&](auto r) { res = std::move(r); });
    io.restart();
    io.run();
    BOOST_REQUIRE(res && res->has_value());
    BOOST_TEST((*res)->status == 201);
}

BOOST_AUTO_TEST_CASE(http_async_client_destroyed_drops_callbacks) {
    TestServer ts;
    net::io_context io;
    bool called = false;
    {
        AsyncClient client(io, tls::make_client_context(pki().node()), ts.client());
        client.request("GET", "/slow", {}, [&](auto) { called = true; });
        io.run_for(5ms);  // let it get in flight
    }
    io.restart();
    io.run_for(2s);
    BOOST_TEST(!called);
    BOOST_TEST(io.stopped());  // no leftover work
}

BOOST_AUTO_TEST_CASE(http_client_timeout_against_silent_listener) {
    net::io_context lio;
    tcp::acceptor acc(lio, tcp::endpoint(net::ip::make_address("127.0.0.1"), 0));  // never accepts
    auto ctx = tls::make_client_context(pki().node());
    ClientOptions o;
    o.host = "127.0.0.1";
    o.port = acc.local_endpoint().port();
    o.timeout = 300ms;
    auto t0 = std::chrono::steady_clock::now();
    auto r = request_sync(*ctx, o, "GET", "/whoami");
    auto took = std::chrono::steady_clock::now() - t0;
    BOOST_REQUIRE(!r.has_value());
    BOOST_TEST(r.error().find("timed out") != std::string::npos);
    BOOST_TEST((took < 2s));

    // Async variant.
    net::io_context io;
    AsyncClient client(io, ctx, o);
    std::optional<std::expected<ClientResponse, std::string>> res;
    client.request("GET", "/", {}, [&](auto x) { res = std::move(x); });
    io.run();
    BOOST_REQUIRE(res.has_value());
    BOOST_TEST(!res->has_value());

    // Connection refused is an error, not a hang.
    acc.close();
    auto refused = request_sync(*ctx, o, "GET", "/");
    BOOST_TEST(!refused.has_value());
}

BOOST_AUTO_TEST_CASE(http_connection_limit) {
    ServerOptions opts;
    opts.max_connections = 1;
    TestServer ts(opts);
    net::io_context io;
    tcp::socket idle(io);
    idle.connect(tcp::endpoint(net::ip::make_address("127.0.0.1"), ts.server->bound_port()));
    std::this_thread::sleep_for(100ms);  // server accepts it and starts waiting for the handshake
    auto ctx = tls::make_client_context(pki().node());
    auto refused = request_sync(*ctx, ts.client(), "GET", "/whoami");
    BOOST_REQUIRE(!refused.has_value());
    BOOST_TEST_MESSAGE("over limit: " << refused.error());
    idle.close();
    std::this_thread::sleep_for(100ms);
    auto r = request_sync(*ctx, ts.client(), "GET", "/whoami");
    BOOST_TEST(r.has_value());
}

BOOST_AUTO_TEST_CASE(http_stop_lets_io_run_return) {
    TestServer ts;
    // An idle keep-alive connection and a half-open TCP connection stay open.
    net::io_context io;
    tcp::socket idle(io);
    idle.connect(tcp::endpoint(net::ip::make_address("127.0.0.1"), ts.server->bound_port()));
    auto ctx = tls::make_client_context(pki().node());
    beast::ssl_stream<beast::tcp_stream> s(io, *ctx);
    beast::get_lowest_layer(s).connect(tcp::endpoint(net::ip::make_address("127.0.0.1"), ts.server->bound_port()));
    BOOST_REQUIRE(tls::set_verify_host(s.native_handle(), "127.0.0.1"));
    s.handshake(net::ssl::stream_base::client);
    std::this_thread::sleep_for(50ms);

    auto t0 = std::chrono::steady_clock::now();
    ts.stop();  // posts stop() and joins the io thread
    BOOST_TEST((std::chrono::steady_clock::now() - t0 < 2s));
    // A new connection is refused.
    tcp::socket late(io);
    boost::system::error_code ec;
    late.connect(tcp::endpoint(net::ip::make_address("127.0.0.1"), ts.server->bound_port()), ec);
    BOOST_TEST(ec.failed());
}

BOOST_AUTO_TEST_CASE(http_start_bind_failure_throws) {
    net::io_context io;
    tcp::acceptor busy(io, tcp::endpoint(net::ip::make_address("127.0.0.1"), 0));
    ServerOptions o;
    o.listen_address = "127.0.0.1";
    o.port = busy.local_endpoint().port();
    HttpsServer s(io, tls::make_server_context(pki().server()), o, [](Request&) { return Response{}; });
    BOOST_CHECK_THROW(s.start(), std::system_error);
}

BOOST_AUTO_TEST_CASE(http_parse_url) {
    auto a = parse_url("https://cp.example:9443");
    BOOST_REQUIRE(a);
    BOOST_TEST(a->host == "cp.example");
    BOOST_TEST(a->port == 9443);
    auto b = parse_url("https://[::1]/");
    BOOST_REQUIRE(b);
    BOOST_TEST(b->host == "::1");
    BOOST_TEST(b->port == 8080);
    BOOST_TEST(!parse_url("http://x"));
    BOOST_TEST(!parse_url("https://x:0"));
    BOOST_TEST(!parse_url("https://x/path"));
}
