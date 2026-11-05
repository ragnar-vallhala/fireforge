// TlsTransport over real TLS on 127.0.0.1 (docs/design/phase3.md §7).
#include <boost/test/unit_test.hpp>

#include <array>
#include <chrono>
#include <filesystem>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <boost/asio/connect.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/asio/write.hpp>

#include "tdr/common/common.hpp"
#include "tdr/peer/transport.hpp"
#include "tdr/protocol/envelope.hpp"
#include "tdr/tls/tls.hpp"

namespace fs = std::filesystem;
namespace net = boost::asio;
using tcp = net::ip::tcp;
using namespace std::chrono_literals;
using namespace tdr::peer;
using tdr::protocol::Frame;

namespace {

// Test PKI, generated once per process: CA with node-a, node-b, admin
// "alice"; a second CA with node-x.
struct Pki {
    fs::path dir = fs::path("/tmp/claude-1000") / tdr::new_id("peer-test");
    fs::path ca = dir / "ca", ca2 = dir / "ca2";

    Pki() {
        fs::create_directories(dir);
        BOOST_REQUIRE(tdr::tls::create_ca(ca, "ca-a"));
        BOOST_REQUIRE(tdr::tls::create_ca(ca2, "ca-b"));
        issue(ca, tdr::tls::Role::kNode, "node-a");
        issue(ca, tdr::tls::Role::kNode, "node-b");
        issue(ca, tdr::tls::Role::kAdmin, "alice");
        issue(ca2, tdr::tls::Role::kNode, "node-x");
    }
    ~Pki() {
        std::error_code ec;
        fs::remove_all(dir, ec);
    }

    void issue(const fs::path& ca_dir, tdr::tls::Role role, const std::string& name) {
        tdr::tls::IssueOptions o;
        o.role = role;
        o.name = name;
        auto r = tdr::tls::issue(ca_dir, o, ca_dir / (name + ".key"), ca_dir / (name + ".crt"));
        BOOST_REQUIRE_MESSAGE(r, (r ? "" : r.error()));
    }

    // name's cert/key from cert_ca, trusting trust_ca.
    std::shared_ptr<net::ssl::context> ctx(const std::string& name, const fs::path* cert_ca = nullptr,
                                           const fs::path* trust_ca = nullptr) const {
        const fs::path& cd = cert_ca ? *cert_ca : ca;
        const fs::path& td = trust_ca ? *trust_ca : cd;
        return tdr::tls::make_peer_context({td / "ca.crt", cd / (name + ".crt"), cd / (name + ".key")});
    }
};

const Pki& pki() {
    static Pki p;
    return p;
}

bool run_until(net::io_context& io, const std::function<bool()>& pred, std::chrono::milliseconds timeout = 3s) {
    auto end = std::chrono::steady_clock::now() + timeout;
    while (!pred()) {
        if (std::chrono::steady_clock::now() > end) return false;
        if (io.stopped()) io.restart();
        io.run_one_for(10ms);
    }
    return true;
}

void run_for(net::io_context& io, std::chrono::milliseconds d) {
    run_until(io, [] { return false; }, d);
}

Frame frame(std::uint16_t type, std::string payload, std::uint64_t id) {
    Frame f;
    f.header.message_type = type;
    f.header.message_id = id;
    f.payload = std::move(payload);
    f.header.payload_length = static_cast<std::uint32_t>(f.payload.size());
    return f;
}

// A transport plus everything its handlers saw.
struct Node {
    std::unique_ptr<TlsTransport> t;
    std::vector<std::shared_ptr<IFrameLink>> accepted;
    std::vector<std::pair<std::uint64_t, Frame>> frames;
    std::vector<std::pair<std::uint64_t, std::string>> closes;

    Node(net::io_context& io, std::shared_ptr<net::ssl::context> ctx, TransportOptions o = {}, bool listen = true) {
        o.listen_address = "127.0.0.1";
        o.listen_port = 0;
        t = std::make_unique<TlsTransport>(io, std::move(ctx), o);
        t->set_handlers({
            .on_accept = [this](const std::shared_ptr<IFrameLink>& l) { accepted.push_back(l); },
            .on_frame = [this](const std::shared_ptr<IFrameLink>& l, Frame f) { frames.emplace_back(l->id(), std::move(f)); },
            .on_close = [this](const std::shared_ptr<IFrameLink>& l, const std::string& r) { closes.emplace_back(l->id(), r); },
        });
        if (listen) t->start();
    }
    Node(const Node&) = delete;
    Node& operator=(const Node&) = delete;

    std::uint16_t port() const { return t->bound_port(); }
    int closes_of(std::uint64_t id) const {
        int n = 0;
        for (auto& [i, _] : closes) n += i == id;
        return n;
    }
    std::string reason_of(std::uint64_t id) const {
        for (auto& [i, r] : closes)
            if (i == id) return r;
        return {};
    }
};

std::optional<DialResult> dial(net::io_context& io, Node& from, std::uint16_t port, const std::string& name,
                               std::chrono::milliseconds timeout = 3s) {
    std::optional<DialResult> r;
    from.t->dial("127.0.0.1", port, name, [&r](DialResult d) { r = std::move(d); });
    BOOST_TEST(!r.has_value());  // never inline
    run_until(io, [&] { return r.has_value(); }, timeout);
    return r;
}

bool contains(const std::string& s, std::string_view needle) { return s.find(needle) != std::string::npos; }

// A port with nothing listening.
std::uint16_t closed_port(net::io_context& io) {
    tcp::acceptor a(io, tcp::endpoint(net::ip::make_address("127.0.0.1"), 0));
    return a.local_endpoint().port();
}

}  // namespace

BOOST_AUTO_TEST_CASE(peer_transport_dial_identity_and_frames) {
    net::io_context io;
    Node a(io, pki().ctx("node-a")), b(io, pki().ctx("node-b"));
    BOOST_TEST(a.port() != 0);

    auto r = dial(io, a, b.port(), "node-b");
    BOOST_REQUIRE(r.has_value());
    BOOST_REQUIRE_MESSAGE(r->has_value(), (r->has_value() ? "" : r->error()));
    auto la = **r;
    BOOST_REQUIRE(run_until(io, [&] { return b.accepted.size() == 1; }));
    auto lb = b.accepted[0];

    BOOST_TEST(la->outbound());
    BOOST_TEST(!lb->outbound());
    BOOST_TEST(la->identity().name == "node-b");
    BOOST_TEST(lb->identity().name == "node-a");
    BOOST_TEST((la->identity().role == tdr::tls::Role::kNode));
    BOOST_TEST((lb->identity().role == tdr::tls::Role::kNode));
    BOOST_TEST(la->is_open());
    BOOST_TEST(lb->is_open());
    BOOST_TEST(contains(la->remote(), "127.0.0.1:"));
    BOOST_TEST(la->remote() == "127.0.0.1:" + std::to_string(b.port()));
    BOOST_TEST(contains(lb->remote(), "127.0.0.1:"));

    // Many small frames plus a 1 MiB one, in order, both directions.
    const int kSmall = 1000;
    std::string big(1u << 20, '\0');
    for (std::size_t i = 0; i < big.size(); ++i) big[i] = static_cast<char>(i * 31 + 7);
    for (int i = 1; i <= kSmall; ++i) la->send(frame(0x0102, "ping-" + std::to_string(i), static_cast<std::uint64_t>(i)));
    la->send(frame(0x0201, big, kSmall + 1));
    la->send(frame(0x0102, "", kSmall + 2));  // empty payload
    lb->send(frame(0x0101, "hello", 77));

    BOOST_REQUIRE(run_until(io, [&] { return b.frames.size() == kSmall + 2 && a.frames.size() == 1; }));
    for (int i = 0; i < kSmall; ++i) {
        BOOST_TEST(b.frames[static_cast<std::size_t>(i)].first == lb->id());
        BOOST_TEST(b.frames[static_cast<std::size_t>(i)].second.header.message_id == static_cast<std::uint64_t>(i + 1));
        BOOST_TEST(b.frames[static_cast<std::size_t>(i)].second.payload == "ping-" + std::to_string(i + 1));
    }
    BOOST_TEST(b.frames[kSmall].second.header.message_type == 0x0201);
    BOOST_TEST((b.frames[kSmall].second.payload == big));
    BOOST_TEST(b.frames[kSmall + 1].second.payload.empty());
    BOOST_TEST(a.frames[0].second.payload == "hello");
    BOOST_TEST(a.frames[0].first == la->id());
    BOOST_TEST(a.closes.empty());
    BOOST_TEST(b.closes.empty());
}

BOOST_AUTO_TEST_CASE(peer_transport_identity_mismatch) {
    net::io_context io;
    Node a(io, pki().ctx("node-a")), b(io, pki().ctx("node-b"));
    auto r = dial(io, a, b.port(), "node-c");
    BOOST_REQUIRE(r.has_value());
    BOOST_REQUIRE(!r->has_value());
    BOOST_TEST(r->error() == "identity mismatch: expected node-c, got node-b");
    // The acceptor either never handed the link out or sees it close.
    BOOST_TEST(run_until(io, [&] { return b.accepted.size() == b.closes.size(); }));
    BOOST_TEST(a.closes.empty());  // never handed out on the dialing side
}

BOOST_AUTO_TEST_CASE(peer_transport_rejects_admin_and_foreign_certs) {
    net::io_context io;
    Node a(io, pki().ctx("node-a"));

    // Admin cert as client: TLS succeeds, but the acceptor only takes node certs.
    {
        Node adm(io, pki().ctx("alice"), {}, false);
        auto r = dial(io, adm, a.port(), "node-a");
        BOOST_REQUIRE(r.has_value());
        if (r->has_value()) BOOST_TEST(run_until(io, [&] { return adm.closes.size() == 1; }));
        run_for(io, 100ms);
        BOOST_TEST(a.accepted.empty());
    }
    // Foreign CA, both directions.
    const fs::path& ca2 = pki().ca2;
    Node x(io, pki().ctx("node-x", &ca2));
    {
        auto r = dial(io, x, a.port(), "node-a");
        BOOST_REQUIRE(r.has_value());
        BOOST_REQUIRE(!r->has_value());
        BOOST_TEST(contains(r->error(), "tls"));
        auto r2 = dial(io, a, x.port(), "node-x");
        BOOST_REQUIRE(r2.has_value());
        BOOST_REQUIRE(!r2->has_value());
        BOOST_TEST(contains(r2->error(), "tls"));
        run_for(io, 100ms);
        BOOST_TEST(a.accepted.empty());
        BOOST_TEST(x.accepted.empty());
    }
    // Foreign client cert that trusts our CA: the acceptor rejects it.
    {
        const fs::path& ca = pki().ca;
        Node y(io, pki().ctx("node-x", &ca2, &ca), {}, false);
        auto r = dial(io, y, a.port(), "node-a");
        BOOST_REQUIRE(r.has_value());
        if (r->has_value()) BOOST_TEST(run_until(io, [&] { return y.closes.size() == 1; }));
        run_for(io, 100ms);
        BOOST_TEST(a.accepted.empty());
    }
}

BOOST_AUTO_TEST_CASE(peer_transport_connect_refused_and_timeout) {
    net::io_context io;
    TransportOptions o;
    o.connect_timeout = 300ms;
    Node a(io, pki().ctx("node-a"), o);

    std::uint16_t port = closed_port(io);
    auto r = dial(io, a, port, "node-b");
    BOOST_REQUIRE(r.has_value());
    BOOST_REQUIRE(!r->has_value());
    BOOST_TEST(r->error() == "connect 127.0.0.1:" + std::to_string(port) + ": Connection refused");

    // Listening TCP socket that never accepts: the TLS handshake stalls.
    tcp::acceptor silent(io, tcp::endpoint(net::ip::make_address("127.0.0.1"), 0));
    auto t0 = std::chrono::steady_clock::now();
    auto r2 = dial(io, a, silent.local_endpoint().port(), "node-b");
    auto took = std::chrono::steady_clock::now() - t0;
    BOOST_REQUIRE(r2.has_value());
    BOOST_REQUIRE(!r2->has_value());
    BOOST_TEST(contains(r2->error(), "timeout"));
    BOOST_TEST((took >= 250ms));
    BOOST_TEST((took < 2s));
}

BOOST_AUTO_TEST_CASE(peer_transport_inbound_handshake_timeout) {
    net::io_context io;
    TransportOptions o;
    o.handshake_timeout = 200ms;
    Node a(io, pki().ctx("node-a"), o);
    tcp::socket raw(io);
    raw.connect(tcp::endpoint(net::ip::make_address("127.0.0.1"), a.port()));
    std::array<char, 16> buf{};
    bool eof = false;
    raw.async_read_some(net::buffer(buf), [&](boost::system::error_code ec, std::size_t) { eof = ec.failed(); });
    BOOST_TEST(run_until(io, [&] { return eof; }, 2s));
    BOOST_TEST(a.accepted.empty());
}

BOOST_AUTO_TEST_CASE(peer_transport_close_flushes_and_reports_once) {
    net::io_context io;
    Node a(io, pki().ctx("node-a")), b(io, pki().ctx("node-b"));
    auto r = dial(io, a, b.port(), "node-b");
    BOOST_REQUIRE(r && r->has_value());
    auto la = **r;
    BOOST_REQUIRE(run_until(io, [&] { return b.accepted.size() == 1; }));
    auto lb = b.accepted[0];

    const int kN = 200;
    for (int i = 1; i <= kN; ++i) la->send(frame(0x0102, std::string(10000, 'x'), static_cast<std::uint64_t>(i)));
    la->close();
    BOOST_TEST(!la->is_open());
    BOOST_TEST(a.closes.empty());  // posted, not inline
    la->send(frame(0x0102, "after close", 9999));  // no-op
    la->close();                                   // idempotent

    BOOST_REQUIRE(run_until(io, [&] { return a.closes.size() == 1 && b.closes.size() == 1; }));
    BOOST_TEST(b.frames.size() == static_cast<std::size_t>(kN));
    for (std::size_t i = 0; i < b.frames.size(); ++i)
        BOOST_TEST(b.frames[i].second.header.message_id == i + 1);
    BOOST_TEST(a.reason_of(la->id()) == "closed locally");
    BOOST_TEST(b.reason_of(lb->id()) == "closed by peer");
    BOOST_TEST(!lb->is_open());
    run_for(io, 200ms);
    BOOST_TEST(a.closes_of(la->id()) == 1);
    BOOST_TEST(b.closes_of(lb->id()) == 1);
    BOOST_TEST(a.closes.size() == 1u);
    BOOST_TEST(b.closes.size() == 1u);
}

BOOST_AUTO_TEST_CASE(peer_transport_send_queue_overflow) {
    net::io_context io;
    TransportOptions o;
    o.max_queued_bytes = 64 * 1024;
    Node a(io, pki().ctx("node-a"), o), b(io, pki().ctx("node-b"));
    auto r = dial(io, a, b.port(), "node-b");
    BOOST_REQUIRE(r && r->has_value());
    auto la = **r;
    la->send(frame(0x0102, std::string(128 * 1024, 'y'), 1));
    BOOST_TEST(!la->is_open());
    BOOST_TEST(a.closes.empty());
    BOOST_REQUIRE(run_until(io, [&] { return a.closes.size() == 1; }));
    BOOST_TEST(a.closes[0].second == "send queue overflow");
    BOOST_TEST(run_until(io, [&] { return b.accepted.size() == b.closes.size() && !b.accepted.empty(); }));
}

BOOST_AUTO_TEST_CASE(peer_transport_protocol_errors_close_link) {
    net::io_context io;
    Node a(io, pki().ctx("node-a"));
    auto ctx = pki().ctx("node-b");

    std::array<std::uint8_t, tdr::protocol::kHeaderSize> oversize{};
    tdr::protocol::Header h;
    h.payload_length = tdr::protocol::kMaxPayload + 1;
    tdr::protocol::encode_header(h, oversize);
    std::string bad_magic(tdr::protocol::kHeaderSize, 'Z');
    const std::string inputs[] = {bad_magic, std::string(oversize.begin(), oversize.end())};

    for (const auto& bytes : inputs) {
        net::ssl::stream<tcp::socket> raw(io, *ctx);
        raw.lowest_layer().connect(tcp::endpoint(net::ip::make_address("127.0.0.1"), a.port()));
        bool hs = false;
        raw.async_handshake(net::ssl::stream_base::client, [&](boost::system::error_code ec) {
            BOOST_TEST(!ec.failed());
            hs = true;
        });
        BOOST_REQUIRE(run_until(io, [&] { return hs; }));
        std::size_t before = a.closes.size();
        BOOST_REQUIRE(run_until(io, [&] { return a.accepted.size() == before + 1; }));
        net::async_write(raw, net::buffer(bytes), [](boost::system::error_code, std::size_t) {});
        BOOST_REQUIRE(run_until(io, [&] { return a.closes.size() == before + 1; }));
        BOOST_TEST(contains(a.closes.back().second, "protocol error"), a.closes.back().second);
        BOOST_TEST(!a.accepted.back()->is_open());
    }
    BOOST_TEST(a.frames.empty());
}

BOOST_AUTO_TEST_CASE(peer_transport_stop_closes_everything) {
    net::io_context io;
    TransportOptions o;
    o.connect_timeout = 2s;
    Node a(io, pki().ctx("node-a"), o), b(io, pki().ctx("node-b"));
    auto r = dial(io, a, b.port(), "node-b");
    BOOST_REQUIRE(r && r->has_value());
    auto la = **r;
    BOOST_REQUIRE(run_until(io, [&] { return b.accepted.size() == 1; }));

    // A dial stuck in the TLS handshake while we stop.
    tcp::acceptor silent(io, tcp::endpoint(net::ip::make_address("127.0.0.1"), 0));
    std::optional<DialResult> pending;
    a.t->dial("127.0.0.1", silent.local_endpoint().port(), "node-b", [&](DialResult d) { pending = std::move(d); });
    run_for(io, 50ms);
    BOOST_TEST(!pending.has_value());

    a.t->stop();
    BOOST_TEST(!pending.has_value());  // posted
    BOOST_TEST(a.closes.empty());
    BOOST_REQUIRE(run_until(io, [&] { return pending.has_value() && a.closes.size() == 1 && b.closes.size() == 1; }));
    BOOST_TEST(!pending->has_value());
    BOOST_TEST(pending->error() == "transport stopped");
    BOOST_TEST(a.closes[0].second == "transport stopped");
    BOOST_TEST(b.closes[0].second == "closed by peer");

    // Dial after stop fails (posted); the listener is gone.
    auto r2 = dial(io, a, b.port(), "node-b");
    BOOST_REQUIRE(r2.has_value());
    BOOST_TEST(r2->error() == "transport stopped");
    auto r3 = dial(io, b, a.port(), "node-a");
    BOOST_REQUIRE(r3.has_value());
    BOOST_TEST(!r3->has_value());

    b.t->stop();
    silent.close();
    io.restart();
    auto t0 = std::chrono::steady_clock::now();
    io.run_for(5s);
    BOOST_TEST((std::chrono::steady_clock::now() - t0 < 3s));  // ran out of work
}

BOOST_AUTO_TEST_CASE(peer_transport_destroy_with_pending_work) {
    net::io_context io;
    Node b(io, pki().ctx("node-b"));
    tcp::acceptor silent(io, tcp::endpoint(net::ip::make_address("127.0.0.1"), 0));
    bool called = false;
    std::shared_ptr<IFrameLink> held;
    {
        TransportOptions o;
        o.connect_timeout = 2s;
        auto c = std::make_unique<Node>(io, pki().ctx("node-a"), o);
        auto r = dial(io, *c, b.port(), "node-b");
        BOOST_REQUIRE(r && r->has_value());
        held = **r;
        held->send(frame(0x0102, std::string(1u << 20, 'q'), 1));  // still queued at destruction
        c->t->dial("127.0.0.1", silent.local_endpoint().port(), "node-b", [&](DialResult) { called = true; });
        c->t->dial("127.0.0.1", b.port(), "node-b", [&](DialResult) { called = true; });  // not started yet
        c.reset();
    }
    BOOST_TEST(!held->is_open());
    held->send(frame(0x0102, "x", 2));
    held->close();
    run_for(io, 300ms);
    BOOST_TEST(!called);
    BOOST_TEST(run_until(io, [&] { return b.accepted.size() == 1 && b.closes.size() == 1; }));
}

BOOST_AUTO_TEST_CASE(peer_transport_max_links) {
    net::io_context io;
    TransportOptions o;
    o.max_links = 1;
    Node a(io, pki().ctx("node-a"), o), b(io, pki().ctx("node-b"));
    auto r1 = dial(io, b, a.port(), "node-a");
    BOOST_REQUIRE(r1 && r1->has_value());
    auto r2 = dial(io, b, a.port(), "node-a");
    BOOST_REQUIRE(r2.has_value());
    BOOST_TEST(!r2->has_value());
    BOOST_TEST(a.accepted.size() == 1u);
    // Outbound links don't count against the inbound limit.
    auto r3 = dial(io, a, b.port(), "node-b");
    BOOST_REQUIRE(r3 && r3->has_value());
    // Once the inbound link closes, a new one is accepted.
    (**r1)->close();
    BOOST_REQUIRE(run_until(io, [&] { return a.closes.size() == 1; }));
    auto r4 = dial(io, b, a.port(), "node-a");
    BOOST_REQUIRE(r4 && r4->has_value());
    BOOST_TEST(run_until(io, [&] { return a.accepted.size() == 2; }));
}
