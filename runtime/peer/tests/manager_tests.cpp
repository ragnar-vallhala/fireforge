// PeerManager tests over the in-memory FakeTransport (fake_transport.hpp).
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/test/unit_test.hpp>
#include <deque>
#include <optional>

#include "fake_transport.hpp"
#include "fireforge/tdr/v1/local.pb.h"
#include "fireforge/tdr/v1/peer.pb.h"
#include "tdr/common/common.hpp"
#include "tdr/peer/manager.hpp"

namespace {

using namespace std::chrono_literals;
using namespace tdr::peer;
using namespace tdr::peer::testing;
namespace v1 = ::fireforge::tdr::v1;
using tdr::protocol::Frame;
using tdr::protocol::make_frame;
using tdr::protocol::next_message_id;

const std::string kAlphaId = "ff-node-1111";
const std::string kBetaId = "ff-node-2222";

PeerOptions fast_opts(const std::string& name, const std::string& id) {
    PeerOptions o;
    o.self_name = name;
    o.self_node_id = id;
    o.daemon_version = "test";
    o.ping_interval = 50ms;
    o.ping_timeout = 200ms;
    o.hello_timeout = 100ms;
    o.backoff_min = 20ms;
    o.backoff_max = 100ms;
    o.unavailable_after_rounds = 3;
    return o;
}

cv1::NodeAddress address(const std::string& type, const std::string& hp) {
    cv1::NodeAddress a;
    a.set_type(type);
    auto pos = hp.rfind(':');
    a.set_address(hp.substr(0, pos));
    a.set_port(static_cast<std::uint32_t>(std::stoul(hp.substr(pos + 1))));
    return a;
}

cv1::NodeInfo node_info(const std::string& id, const std::string& name, cv1::NodeHealth health,
                        std::vector<cv1::NodeAddress> addrs) {
    cv1::NodeInfo n;
    n.set_node_id(id);
    n.set_name(name);
    n.set_health(health);
    for (auto& a : addrs) *n.add_addresses() = a;
    return n;
}

cv1::NodeList node_list(std::vector<cv1::NodeInfo> nodes) {
    cv1::NodeList l;
    for (auto& n : nodes) *l.add_nodes() = n;
    return l;
}

template <class Pred>
bool run_until(boost::asio::io_context& io, Pred pred, std::chrono::milliseconds timeout = 1000ms) {
    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (!pred()) {
        if (std::chrono::steady_clock::now() >= deadline) return false;
        io.restart();
        io.run_for(2ms);
    }
    return true;
}

void run_for(boost::asio::io_context& io, std::chrono::milliseconds d) {
    auto deadline = std::chrono::steady_clock::now() + d;
    while (std::chrono::steady_clock::now() < deadline) {
        io.restart();
        io.run_for(2ms);
    }
}

std::optional<cv1::PeerReachability> peer_of(const PeerManager& m, const std::string& node_id) {
    for (auto& r : m.snapshot())
        if (r.node_id() == node_id) return r;
    return std::nullopt;
}

std::string state_of(const PeerManager& m, const std::string& node_id) {
    auto r = peer_of(m, node_id);
    return r ? r->state() : "<none>";
}

// A remote node driven by the test: answers hellos and pings per its config.
struct ScriptedPeer {
    ScriptedPeer(boost::asio::io_context& io_, FakeNetwork& net_, std::string name_, std::string id_,
                 std::vector<std::string> addrs_ = {})
        : io(io_), net(net_), name(name_), node_id(id_), addrs(addrs_),
          t(net_, name_, addrs_.empty() ? std::string() : addrs_.front()), sender(tdr::node_id_from_string(id_)),
          reply_name(name_), reply_node_id(id_) {
        for (auto& a : addrs) net.listen(a, &t);
        TransportHandlers h;
        h.on_accept = [this](const std::shared_ptr<IFrameLink>& l) {
            ++accepts;
            links.push_back(l);
        };
        h.on_frame = [this](const std::shared_ptr<IFrameLink>& l, Frame f) { on_frame(l, std::move(f)); };
        h.on_close = [this](const std::shared_ptr<IFrameLink>&, const std::string& r) {
            ++closes;
            close_reasons.push_back(r);
        };
        t.set_handlers(std::move(h));
    }
    ~ScriptedPeer() {
        for (auto& a : addrs) net.unlisten(a);
    }

    void send(const std::shared_ptr<IFrameLink>& l, Frame f) {
        f.header.sender_id = sender;
        l->send(f);
    }

    void on_frame(const std::shared_ptr<IFrameLink>& l, Frame f) {
        frames.push_back(f);
        if (f.type() == v1::NODE_HELLO && f.header.correlation_id == 0 && reply_hello) {
            v1::PeerHello h;
            h.set_name(reply_name);
            h.set_node_id(reply_node_id);
            h.set_protocol_version(reply_version);
            send(l, make_frame(v1::NODE_HELLO, h, next_message_id(), f.header.message_id));
        } else if (f.type() == v1::NODE_HEARTBEAT && f.header.correlation_id == 0 && echo_pings) {
            v1::PeerPing p;
            BOOST_REQUIRE(f.parse(p));
            auto reply = make_frame(v1::NODE_HEARTBEAT, p, next_message_id(), f.header.message_id);
            std::chrono::milliseconds delay{0};
            if (!echo_delays.empty()) {
                delay = echo_delays.front();
                echo_delays.pop_front();
            }
            if (delay.count() == 0) {
                send(l, reply);
                ++echoes;
                return;
            }
            auto timer = std::make_shared<boost::asio::steady_timer>(io, delay);
            timer->async_wait([this, timer, l, reply](const boost::system::error_code& ec) {
                if (ec) return;
                send(l, reply);
                ++echoes;
            });
        }
    }

    // Dial a manager and send our hello.
    void connect_to(const std::string& hp, const std::string& expected, v1::PeerHello hello) {
        auto pos = hp.rfind(':');
        t.dial(hp.substr(0, pos), static_cast<std::uint16_t>(std::stoul(hp.substr(pos + 1))), expected,
               [this, hello](DialResult r) {
                   if (!r) {
                       dial_error = r.error();
                       return;
                   }
                   links.push_back(*r);
                   out = *r;
                   hello_id = next_message_id();
                   send(*r, make_frame(v1::NODE_HELLO, hello, hello_id));
               });
    }
    v1::PeerHello hello() const {
        v1::PeerHello h;
        h.set_name(name);
        h.set_node_id(node_id);
        h.set_protocol_version(1);
        return h;
    }

    std::size_t count(v1::MessageType type) const {
        std::size_t n = 0;
        for (auto& f : frames)
            if (f.type() == type) ++n;
        return n;
    }
    std::vector<std::string> goodbyes() const {
        std::vector<std::string> reasons;
        for (auto& f : frames) {
            if (f.type() != v1::PEER_ADVERTISE) continue;
            v1::PeerGoodbye g;
            if (f.parse(g)) reasons.push_back(g.reason());
        }
        return reasons;
    }
    std::size_t open_links() const {
        std::size_t n = 0;
        for (auto& l : links)
            if (l->is_open()) ++n;
        return n;
    }

    boost::asio::io_context& io;
    FakeNetwork& net;
    std::string name, node_id;
    std::vector<std::string> addrs;
    FakeTransport t;
    tdr::NodeId sender;

    bool reply_hello = true;
    std::string reply_name, reply_node_id;
    std::uint32_t reply_version = 1;
    bool echo_pings = true;
    std::deque<std::chrono::milliseconds> echo_delays;

    std::vector<Frame> frames;
    std::vector<std::shared_ptr<IFrameLink>> links;
    std::shared_ptr<IFrameLink> out;
    std::uint64_t hello_id = 0;
    std::string dial_error;
    int accepts = 0, closes = 0, echoes = 0;
    std::vector<std::string> close_reasons;
};

// A real PeerManager on a FakeTransport.
struct Node {
    Node(boost::asio::io_context& io, FakeNetwork& net, std::string name_, std::string id_, std::string addr_)
        : name(name_), id(id_), addr(addr_), t(net, name_, addr_), mgr(io, t, fast_opts(name_, id_)) {
        net.listen(addr, &t);
        mgr.set_state_listener([this](const std::string& nid, PeerState s) { states.emplace_back(nid, s); });
        mgr.set_directory_refresh([this] { ++refreshes; });
        mgr.start();
    }
    cv1::NodeInfo info(cv1::NodeHealth h = cv1::ONLINE) const {
        return node_info(id, name, h, {address("lan", addr)});
    }

    std::string name, id, addr;
    FakeTransport t;
    PeerManager mgr;
    std::vector<std::pair<std::string, PeerState>> states;
    int refreshes = 0;
};

struct Fixture {
    boost::asio::io_context io;
    FakeNetwork net{io};
};

}  // namespace

BOOST_AUTO_TEST_SUITE(peer_manager)

BOOST_AUTO_TEST_CASE(dial_order_lan_then_tailscale_then_rest_stable) {
    auto n = node_info("x", "x", cv1::ONLINE,
                       {address("public", "1.1.1.1:1"), address("tailscale", "100.64.0.1:2"),
                        address("lan", "10.0.0.1:3"), address("other", "9.9.9.9:4"), address("lan", "10.0.0.2:5"),
                        address("tailscale", "100.64.0.2:6")});
    auto o = dial_order(n);
    std::vector<std::uint32_t> ports;
    for (auto& a : o) ports.push_back(a.port());
    std::vector<std::uint32_t> want{3, 5, 2, 6, 1, 4};
    BOOST_TEST(ports == want, boost::test_tools::per_element());
    BOOST_TEST(std::string(to_string(PeerState::kUnavailable)) == "UNAVAILABLE");
}

BOOST_FIXTURE_TEST_CASE(falls_back_to_tailscale_and_records_path, Fixture) {
    Node a(io, net, "alpha", kAlphaId, "10.0.0.1:7400");
    ScriptedPeer b(io, net, "beta", kBetaId, {"100.64.0.2:7400"});
    a.t.scripts["10.0.0.2:7400"] = {DialScript::kFail, "connect timeout"};
    a.mgr.update_directory(node_list(
        {a.info(), node_info(kBetaId, "beta", cv1::ONLINE,
                             {address("tailscale", "100.64.0.2:7400"), address("lan", "10.0.0.2:7400")})}));
    BOOST_REQUIRE(run_until(io, [&] { return state_of(a.mgr, kBetaId) == "CONNECTED"; }));
    std::vector<std::string> want{"10.0.0.2:7400", "100.64.0.2:7400"};
    BOOST_TEST(a.t.dials == want, boost::test_tools::per_element());
    auto r = *peer_of(a.mgr, kBetaId);
    BOOST_TEST(r.path_type() == "tailscale");
    BOOST_TEST(r.address() == "100.64.0.2:7400");
    BOOST_TEST(r.outbound());
    BOOST_TEST(r.name() == "beta");
    BOOST_TEST(r.connected_since_unix_ms() > 0);
    BOOST_TEST(r.last_error().find("lan 10.0.0.2:7400: connect timeout") != std::string::npos);
    BOOST_REQUIRE(a.states.size() == 2u);
    BOOST_TEST((a.states[0].second == PeerState::kConnecting));
    BOOST_TEST((a.states[1].second == PeerState::kConnected));
    // Our hello carried our identity and sender_id.
    BOOST_REQUIRE(b.count(v1::NODE_HELLO) == 1u);
    v1::PeerHello h;
    BOOST_REQUIRE(b.frames[0].parse(h));
    BOOST_TEST(h.name() == "alpha");
    BOOST_TEST(h.node_id() == kAlphaId);
    BOOST_TEST(h.protocol_version() == 1u);
    BOOST_TEST((b.frames[0].header.sender_id == tdr::node_id_from_string(kAlphaId)));
    // Only one listing entry (self skipped).
    BOOST_TEST(a.mgr.snapshot().size() == 1u);
}

BOOST_FIXTURE_TEST_CASE(backoff_then_unavailable_then_success_resets, Fixture) {
    Node a(io, net, "alpha", kAlphaId, "10.0.0.1:7400");
    std::size_t dials_at_unavailable = 0;
    a.mgr.set_state_listener([&](const std::string&, PeerState s) {
        a.states.emplace_back(kBetaId, s);
        if (s == PeerState::kUnavailable && dials_at_unavailable == 0) dials_at_unavailable = a.t.dials.size();
    });
    a.mgr.update_directory(node_list({node_info(
        kBetaId, "beta", cv1::ONLINE, {address("lan", "10.0.0.2:7400"), address("tailscale", "100.64.0.2:7400")})}));
    BOOST_REQUIRE(run_until(io, [&] { return state_of(a.mgr, kBetaId) == "UNAVAILABLE"; }));
    BOOST_TEST(dials_at_unavailable == 6u);  // 3 rounds x 2 addresses
    BOOST_TEST(peer_of(a.mgr, kBetaId)->last_error().find("tailscale 100.64.0.2:7400: connection refused") !=
               std::string::npos);
    run_for(io, 200ms);
    // Backoff (20, 40, 80, 100, ... ms) bounds the dial rate.
    BOOST_TEST(a.t.dials.size() < 20u);
    BOOST_TEST(state_of(a.mgr, kBetaId) == "UNAVAILABLE");

    ScriptedPeer b(io, net, "beta", kBetaId, {"100.64.0.2:7400"});
    BOOST_REQUIRE(run_until(io, [&] { return state_of(a.mgr, kBetaId) == "CONNECTED"; }, 500ms));
    // Losing the link now starts again from CONNECTING, not UNAVAILABLE.
    a.states.clear();
    std::static_pointer_cast<FakeLink>(b.links.back())->reset();
    BOOST_REQUIRE(run_until(io, [&] { return !a.states.empty(); }));
    BOOST_TEST((a.states[0].second == PeerState::kConnecting));
    BOOST_REQUIRE(run_until(io, [&] { return state_of(a.mgr, kBetaId) == "CONNECTED"; }, 500ms));
    BOOST_TEST(b.accepts == 2);
}

BOOST_AUTO_TEST_CASE(outbound_hello_validation_failures) {
    struct Case {
        std::string what;
        std::function<void(ScriptedPeer&)> setup;
        std::string expect;
    };
    std::vector<Case> cases{
        {"wrong name", [](ScriptedPeer& b) { b.reply_name = "gamma"; }, "hello name mismatch"},
        {"wrong node_id", [](ScriptedPeer& b) { b.reply_node_id = "ff-node-9999"; }, "hello node_id mismatch"},
        {"wrong version", [](ScriptedPeer& b) { b.reply_version = 2; }, "unsupported protocol_version 2"},
        {"no reply", [](ScriptedPeer& b) { b.reply_hello = false; }, "hello timeout"},
    };
    for (auto& c : cases) {
        BOOST_TEST_CONTEXT(c.what) {
            Fixture f;
            Node a(f.io, f.net, "alpha", kAlphaId, "10.0.0.1:7400");
            ScriptedPeer b(f.io, f.net, "beta", kBetaId, {"10.0.0.2:7400"});
            c.setup(b);
            a.mgr.update_directory(
                node_list({node_info(kBetaId, "beta", cv1::ONLINE, {address("lan", "10.0.0.2:7400")})}));
            BOOST_TEST(run_until(f.io, [&] {
                auto r = peer_of(a.mgr, kBetaId);
                return r && r->last_error().find(c.expect) != std::string::npos;
            }));
            BOOST_TEST(state_of(a.mgr, kBetaId) != "CONNECTED");
            for (auto& s : a.states) BOOST_TEST((s.second != PeerState::kConnected));
            BOOST_TEST(run_until(f.io, [&] { return b.closes >= 1; }));
        }
    }
    // Transport-level identity check (cert CN != directory name).
    Fixture f;
    Node a(f.io, f.net, "alpha", kAlphaId, "10.0.0.1:7400");
    ScriptedPeer g(f.io, f.net, "gamma", kBetaId, {"10.0.0.2:7400"});
    a.mgr.update_directory(node_list({node_info(kBetaId, "beta", cv1::ONLINE, {address("lan", "10.0.0.2:7400")})}));
    BOOST_TEST(run_until(f.io, [&] {
        auto r = peer_of(a.mgr, kBetaId);
        return r && r->last_error().find("identity mismatch") != std::string::npos;
    }));
}

BOOST_FIXTURE_TEST_CASE(inbound_unknown_node_is_accepted_and_refresh_requested, Fixture) {
    Node a(io, net, "alpha", kAlphaId, "10.0.0.1:7400");
    ScriptedPeer b(io, net, "beta", kBetaId, {"10.0.0.2:7400"});
    b.connect_to("10.0.0.1:7400", "alpha", b.hello());
    BOOST_REQUIRE(run_until(io, [&] { return b.count(v1::NODE_HELLO) == 1u && a.refreshes == 1; }));
    const Frame& reply = b.frames[0];
    BOOST_TEST(reply.header.correlation_id == b.hello_id);
    BOOST_TEST((reply.header.sender_id == tdr::node_id_from_string(kAlphaId)));
    v1::PeerHello h;
    BOOST_REQUIRE(reply.parse(h));
    BOOST_TEST(h.name() == "alpha");
    BOOST_TEST(h.node_id() == kAlphaId);
    auto r = *peer_of(a.mgr, kBetaId);
    BOOST_TEST(r.state() == "CONNECTED");
    BOOST_TEST(r.name() == "beta");
    BOOST_TEST(!r.outbound());
    BOOST_TEST(r.path_type() == "inbound");
    BOOST_TEST(a.t.dials.empty());

    // A directory update that predates the refresh keeps the live peer.
    a.mgr.update_directory(node_list({}));
    BOOST_TEST(state_of(a.mgr, kBetaId) == "CONNECTED");
    // Once listed it becomes a dial target, but is not dialed while connected.
    a.mgr.update_directory(node_list({node_info(kBetaId, "beta", cv1::ONLINE, {address("lan", "10.0.0.2:7400")})}));
    run_for(io, 100ms);
    BOOST_TEST(a.t.dials.empty());
    BOOST_TEST(state_of(a.mgr, kBetaId) == "CONNECTED");
    BOOST_TEST(b.open_links() == 1u);
    BOOST_TEST(b.count(v1::NODE_HEARTBEAT) > 0u);  // pings flow on inbound links too
}

BOOST_AUTO_TEST_CASE(inbound_hello_rejections) {
    struct Case {
        std::string what;
        std::string cert_name;
        std::function<void(v1::PeerHello&)> edit;
        std::optional<v1::ErrorCode> code;  // nullopt: closed without ERROR
        bool send_ping_first = false;
    };
    std::vector<Case> cases{
        {"node_id differs from directory", "beta", [](v1::PeerHello& h) { h.set_node_id("ff-node-9999"); },
         v1::UNAUTHORIZED},
        {"cert name differs from hello", "gamma", [](v1::PeerHello& h) { h.set_name("beta"); }, v1::UNAUTHORIZED},
        {"wrong version", "beta", [](v1::PeerHello& h) { h.set_protocol_version(7); }, v1::UNSUPPORTED},
        {"claims to be us", "alpha", [](v1::PeerHello& h) {
             h.set_name("alpha");
             h.set_node_id(kAlphaId);
         }, v1::UNAUTHORIZED},
        {"frame before hello", "beta", [](v1::PeerHello&) {}, std::nullopt, true},
    };
    for (auto& c : cases) {
        BOOST_TEST_CONTEXT(c.what) {
            Fixture f;
            Node a(f.io, f.net, "alpha", kAlphaId, "10.0.0.1:7400");
            // Known in the directory, OFFLINE so it isn't dialed.
            a.mgr.update_directory(
                node_list({node_info(kBetaId, "beta", cv1::OFFLINE, {address("lan", "10.0.0.2:7400")})}));
            ScriptedPeer b(f.io, f.net, c.cert_name, kBetaId);
            auto h = b.hello();
            c.edit(h);
            b.sender = tdr::node_id_from_string(h.node_id());
            if (c.send_ping_first) {
                b.t.dial("10.0.0.1", 7400, "alpha", [&](DialResult r) {
                    BOOST_REQUIRE(r.has_value());
                    b.links.push_back(*r);
                    v1::PeerPing p;
                    b.send(*r, make_frame(v1::NODE_HEARTBEAT, p, next_message_id()));
                });
            } else {
                b.connect_to("10.0.0.1:7400", "alpha", h);
            }
            BOOST_TEST(run_until(f.io, [&] { return b.closes == 1; }));
            BOOST_TEST(b.count(v1::NODE_HELLO) == 0u);
            if (c.code) {
                BOOST_REQUIRE(b.count(v1::ERROR) == 1u);
                v1::Error e;
                BOOST_REQUIRE(b.frames.back().parse(e));
                BOOST_TEST(v1::ErrorCode_Name(e.code()) == v1::ErrorCode_Name(*c.code));
            } else {
                BOOST_TEST(b.count(v1::ERROR) == 0u);
            }
            BOOST_TEST(state_of(a.mgr, kBetaId) == "IDLE");
            BOOST_TEST(a.refreshes == 0);
        }
    }
    // Correct hello from a node listed OFFLINE is accepted.
    Fixture f;
    Node a(f.io, f.net, "alpha", kAlphaId, "10.0.0.1:7400");
    a.mgr.update_directory(node_list({node_info(kBetaId, "beta", cv1::OFFLINE, {address("lan", "10.0.0.2:7400")})}));
    ScriptedPeer b(f.io, f.net, "beta", kBetaId, {});
    b.connect_to("10.0.0.1:7400", "alpha", b.hello());
    BOOST_TEST(run_until(f.io, [&] { return state_of(a.mgr, kBetaId) == "CONNECTED"; }));
    BOOST_TEST(peer_of(a.mgr, kBetaId)->path_type() == "inbound");
    BOOST_TEST(a.refreshes == 0);
}

BOOST_AUTO_TEST_CASE(duplicate_links_converge_to_the_one_dialed_by_lower_id) {
    struct Case {
        std::string alpha_id;
        std::chrono::milliseconds alpha_delay, beta_delay;
    };
    // alpha lower and higher; alpha's link first, beta's first, simultaneous.
    std::vector<Case> cases{
        {kAlphaId, 0ms, 30ms}, {kAlphaId, 30ms, 0ms}, {kAlphaId, 0ms, 0ms},
        {"ff-node-9999", 0ms, 30ms}, {"ff-node-9999", 30ms, 0ms}, {"ff-node-9999", 0ms, 0ms},
    };
    for (auto& c : cases) {
        BOOST_TEST_CONTEXT(c.alpha_id << " delays " << c.alpha_delay.count() << "/" << c.beta_delay.count()) {
            Fixture f;
            Node a(f.io, f.net, "alpha", c.alpha_id, "10.0.0.1:7400");
            Node b(f.io, f.net, "beta", kBetaId, "10.0.0.2:7400");
            a.t.dial_delay = c.alpha_delay;
            b.t.dial_delay = c.beta_delay;
            auto dir = node_list({a.info(), b.info()});
            a.mgr.update_directory(dir);
            b.mgr.update_directory(dir);
            run_for(f.io, 250ms);
            BOOST_TEST(a.t.dials.size() == 1u);  // both dialed: a real duplicate
            BOOST_TEST(b.t.dials.size() == 1u);
            BOOST_TEST(a.t.open_links() == 1u);
            BOOST_TEST(b.t.open_links() == 1u);
            auto ra = *peer_of(a.mgr, b.id);
            auto rb = *peer_of(b.mgr, a.id);
            BOOST_TEST(ra.state() == "CONNECTED");
            BOOST_TEST(rb.state() == "CONNECTED");
            bool alpha_lower = c.alpha_id < kBetaId;
            BOOST_TEST(ra.outbound() == alpha_lower);
            BOOST_TEST(rb.outbound() == !alpha_lower);
            // The surviving links are the two ends of the same pair.
            std::shared_ptr<FakeLink> la, lb;
            for (auto& l : a.t.links)
                if (l->is_open()) la = l;
            for (auto& l : b.t.links)
                if (l->is_open()) lb = l;
            BOOST_REQUIRE(la && lb);
            BOOST_TEST((la->other.lock() == lb));
            // Stable: no further dials or link changes, pings keep it alive.
            run_for(f.io, 250ms);
            BOOST_TEST(a.t.dials.size() == 1u);
            BOOST_TEST(b.t.dials.size() == 1u);
            BOOST_TEST(la->is_open());
            BOOST_TEST(ra.connected_since_unix_ms() == peer_of(a.mgr, b.id)->connected_since_unix_ms());
            BOOST_TEST(peer_of(a.mgr, b.id)->rtt_ms() > 0);
        }
    }
}

BOOST_FIXTURE_TEST_CASE(ping_rtt_is_an_ewma, Fixture) {
    Node a(io, net, "alpha", kAlphaId, "10.0.0.1:7400");
    ScriptedPeer b(io, net, "beta", kBetaId, {"10.0.0.2:7400"});
    b.echo_delays = {40ms, 5ms};
    a.mgr.update_directory(node_list({node_info(kBetaId, "beta", cv1::ONLINE, {address("lan", "10.0.0.2:7400")})}));
    BOOST_REQUIRE(run_until(io, [&] { return peer_of(a.mgr, kBetaId) && peer_of(a.mgr, kBetaId)->rtt_ms() > 0; }));
    double first = peer_of(a.mgr, kBetaId)->rtt_ms();
    BOOST_TEST(first >= 40.0);
    BOOST_REQUIRE(run_until(io, [&] { return peer_of(a.mgr, kBetaId)->rtt_ms() != first; }));
    double second = peer_of(a.mgr, kBetaId)->rtt_ms();
    // 0.3 * ~5 + 0.7 * first: well below first, well above the last sample.
    BOOST_TEST(second < first);
    BOOST_TEST(second > 0.65 * first);
}

BOOST_FIXTURE_TEST_CASE(ping_timeout_closes_and_redials, Fixture) {
    Node a(io, net, "alpha", kAlphaId, "10.0.0.1:7400");
    ScriptedPeer b(io, net, "beta", kBetaId, {"10.0.0.2:7400"});
    b.echo_pings = false;
    std::vector<std::string> errors_on_drop;
    a.mgr.set_state_listener([&](const std::string& id, PeerState s) {
        if (s == PeerState::kConnecting) errors_on_drop.push_back(peer_of(a.mgr, id)->last_error());
    });
    a.mgr.update_directory(node_list({node_info(kBetaId, "beta", cv1::ONLINE, {address("lan", "10.0.0.2:7400")})}));
    BOOST_REQUIRE(run_until(io, [&] { return state_of(a.mgr, kBetaId) == "CONNECTED"; }));
    auto t0 = std::chrono::steady_clock::now();
    BOOST_REQUIRE(run_until(io, [&] { return b.closes >= 1; }));
    auto took = std::chrono::steady_clock::now() - t0;
    BOOST_TEST(took >= 190ms);
    BOOST_REQUIRE(errors_on_drop.size() >= 2u);
    BOOST_TEST(errors_on_drop[1] == "ping timeout");
    BOOST_REQUIRE(run_until(io, [&] { return b.accepts == 2 && state_of(a.mgr, kBetaId) == "CONNECTED"; }));
}

BOOST_FIXTURE_TEST_CASE(bad_sender_id_closes_link, Fixture) {
    Node a(io, net, "alpha", kAlphaId, "10.0.0.1:7400");
    ScriptedPeer b(io, net, "beta", kBetaId, {"10.0.0.2:7400"});
    int handled = 0;
    a.mgr.set_frame_handler([&](const std::string&, Frame) { ++handled; });
    a.mgr.update_directory(node_list({node_info(kBetaId, "beta", cv1::ONLINE, {address("lan", "10.0.0.2:7400")})}));
    BOOST_REQUIRE(run_until(io, [&] { return state_of(a.mgr, kBetaId) == "CONNECTED"; }));
    Frame f;
    f.header.message_type = v1::JOB_SUBMIT;
    f.header.message_id = next_message_id();
    f.payload = "x";
    f.header.payload_length = 1;
    f.header.sender_id = tdr::node_id_from_string("ff-node-evil");
    b.links.back()->send(f);  // raw, without ScriptedPeer::send fixing sender_id
    BOOST_REQUIRE(run_until(io, [&] { return b.closes == 1; }));
    BOOST_TEST(handled == 0);
    BOOST_TEST(peer_of(a.mgr, kBetaId)->last_error() == "bad sender_id");
}

BOOST_FIXTURE_TEST_CASE(goodbye_is_recorded_and_peer_redialed, Fixture) {
    Node a(io, net, "alpha", kAlphaId, "10.0.0.1:7400");
    ScriptedPeer b(io, net, "beta", kBetaId, {"10.0.0.2:7400"});
    a.mgr.update_directory(node_list({node_info(kBetaId, "beta", cv1::ONLINE, {address("lan", "10.0.0.2:7400")})}));
    BOOST_REQUIRE(run_until(io, [&] { return state_of(a.mgr, kBetaId) == "CONNECTED"; }));
    a.states.clear();
    v1::PeerGoodbye g;
    g.set_reason("shutdown");
    b.send(b.links.back(), make_frame(v1::PEER_ADVERTISE, g, next_message_id()));
    BOOST_REQUIRE(run_until(io, [&] { return !a.states.empty(); }));
    BOOST_TEST((a.states[0].second == PeerState::kConnecting));
    BOOST_TEST(peer_of(a.mgr, kBetaId)->last_error() == "peer said goodbye: shutdown");
    BOOST_REQUIRE(run_until(io, [&] { return b.accepts == 2 && state_of(a.mgr, kBetaId) == "CONNECTED"; }));
}

BOOST_FIXTURE_TEST_CASE(offline_peer_is_not_dialed_and_idle, Fixture) {
    Node a(io, net, "alpha", kAlphaId, "10.0.0.1:7400");
    ScriptedPeer b(io, net, "beta", kBetaId, {"10.0.0.2:7400"});
    auto info = [&](cv1::NodeHealth h) {
        return node_list({node_info(kBetaId, "beta", h, {address("lan", "10.0.0.2:7400")})});
    };
    a.mgr.update_directory(info(cv1::OFFLINE));
    run_for(io, 60ms);
    BOOST_TEST(a.t.dials.empty());
    BOOST_TEST(state_of(a.mgr, kBetaId) == "IDLE");
    BOOST_TEST(a.states.empty());

    a.mgr.update_directory(info(cv1::STALE));
    BOOST_REQUIRE(run_until(io, [&] { return state_of(a.mgr, kBetaId) == "CONNECTED"; }));
    // OFFLINE keeps the existing link.
    a.mgr.update_directory(info(cv1::OFFLINE));
    run_for(io, 60ms);
    BOOST_TEST(state_of(a.mgr, kBetaId) == "CONNECTED");
    BOOST_TEST(b.open_links() == 1u);
    // When it drops, the peer goes IDLE and is not redialed.
    std::static_pointer_cast<FakeLink>(b.links.back())->reset();
    BOOST_REQUIRE(run_until(io, [&] { return state_of(a.mgr, kBetaId) == "IDLE"; }));
    run_for(io, 100ms);
    BOOST_TEST(a.t.dials.size() == 1u);
    BOOST_TEST(state_of(a.mgr, kBetaId) == "IDLE");
}

BOOST_FIXTURE_TEST_CASE(removed_peer_gets_goodbye_and_is_dropped, Fixture) {
    Node a(io, net, "alpha", kAlphaId, "10.0.0.1:7400");
    ScriptedPeer b(io, net, "beta", kBetaId, {"10.0.0.2:7400"});
    a.mgr.update_directory(node_list({node_info(kBetaId, "beta", cv1::ONLINE, {address("lan", "10.0.0.2:7400")})}));
    BOOST_REQUIRE(run_until(io, [&] { return state_of(a.mgr, kBetaId) == "CONNECTED"; }));
    a.mgr.update_directory(node_list({a.info()}));
    BOOST_TEST(a.mgr.snapshot().empty());
    BOOST_REQUIRE(run_until(io, [&] { return b.closes == 1; }));
    std::vector<std::string> want{"removed"};
    BOOST_TEST(b.goodbyes() == want, boost::test_tools::per_element());
    run_for(io, 60ms);
    BOOST_TEST(a.t.dials.size() == 1u);
}

BOOST_FIXTURE_TEST_CASE(send_and_frame_handler_between_two_managers, Fixture) {
    Node a(io, net, "alpha", kAlphaId, "10.0.0.1:7400");
    Node b(io, net, "beta", kBetaId, "10.0.0.2:7400");
    std::vector<std::pair<std::string, Frame>> got;
    b.mgr.set_frame_handler([&](const std::string& id, Frame f) { got.emplace_back(id, std::move(f)); });
    BOOST_TEST(!a.mgr.send(kBetaId, Frame{}));
    a.mgr.update_directory(node_list({a.info(), b.info()}));
    BOOST_REQUIRE(run_until(io, [&] {
        return state_of(a.mgr, kBetaId) == "CONNECTED" && state_of(b.mgr, kAlphaId) == "CONNECTED";
    }));
    BOOST_TEST(b.refreshes == 1);  // alpha was unknown to beta
    Frame f;
    f.header.message_type = v1::JOB_SUBMIT;
    f.payload = "dummy";
    BOOST_TEST(a.mgr.send(kBetaId, f));
    BOOST_TEST(!a.mgr.send("ff-node-nobody", f));
    BOOST_REQUIRE(run_until(io, [&] { return !got.empty(); }));
    BOOST_TEST(got[0].first == kAlphaId);
    BOOST_TEST((got[0].second.type() == v1::JOB_SUBMIT));
    BOOST_TEST(got[0].second.payload == "dummy");
    BOOST_TEST(got[0].second.header.message_id != 0u);
    BOOST_TEST((got[0].second.header.sender_id == tdr::node_id_from_string(kAlphaId)));
    // And back, on the inbound link.
    std::vector<std::string> from;
    a.mgr.set_frame_handler([&](const std::string& id, Frame) { from.push_back(id); });
    BOOST_TEST(b.mgr.send(kAlphaId, f));
    BOOST_REQUIRE(run_until(io, [&] { return !from.empty(); }));
    BOOST_TEST(from[0] == kBetaId);
}

BOOST_FIXTURE_TEST_CASE(stop_sends_goodbye_and_never_redials, Fixture) {
    Node a(io, net, "alpha", kAlphaId, "10.0.0.1:7400");
    ScriptedPeer b(io, net, "beta", kBetaId, {"10.0.0.2:7400"});
    a.mgr.update_directory(node_list({node_info(kBetaId, "beta", cv1::ONLINE, {address("lan", "10.0.0.2:7400")})}));
    BOOST_REQUIRE(run_until(io, [&] { return state_of(a.mgr, kBetaId) == "CONNECTED"; }));
    a.mgr.stop();
    BOOST_REQUIRE(run_until(io, [&] { return b.closes == 1; }));
    std::vector<std::string> want{"shutdown"};
    BOOST_TEST(b.goodbyes() == want, boost::test_tools::per_element());
    Frame f;
    f.header.message_type = v1::JOB_SUBMIT;
    BOOST_TEST(!a.mgr.send(kBetaId, f));
    a.mgr.update_directory(node_list({node_info(kBetaId, "beta", cv1::ONLINE, {address("lan", "10.0.0.2:7400")})}));
    // Inbound is refused too.
    ScriptedPeer c(io, net, "gamma", "ff-node-3333");
    c.connect_to("10.0.0.1:7400", "alpha", c.hello());
    BOOST_REQUIRE(run_until(io, [&] { return c.closes == 1; }));
    run_for(io, 150ms);
    BOOST_TEST(a.t.dials.size() == 1u);
    BOOST_TEST(c.count(v1::NODE_HELLO) == 0u);
}

BOOST_FIXTURE_TEST_CASE(destroying_with_pending_work_is_safe, Fixture) {
    ScriptedPeer b(io, net, "beta", kBetaId, {"10.0.0.2:7400"});
    ScriptedPeer c(io, net, "gamma", "ff-node-3333", {"10.0.0.3:7400"});
    FakeTransport t(net, "alpha", "10.0.0.1:7400");
    net.listen("10.0.0.1:7400", &t);
    {
        // Hung dial, delayed dial, a connected link with timers armed.
        auto m = std::make_unique<PeerManager>(io, t, fast_opts("alpha", kAlphaId));
        m->start();
        t.scripts["10.0.0.9:7400"] = {DialScript::kHang, ""};
        m->update_directory(node_list(
            {node_info(kBetaId, "beta", cv1::ONLINE, {address("lan", "10.0.0.2:7400")}),
             node_info("ff-node-3333", "gamma", cv1::ONLINE, {address("lan", "10.0.0.3:7400")}),
             node_info("ff-node-4444", "delta", cv1::ONLINE, {address("lan", "10.0.0.9:7400")})}));
        BOOST_REQUIRE(run_until(io, [&] { return state_of(*m, kBetaId) == "CONNECTED"; }));
        t.dial_delay = 30ms;
        std::static_pointer_cast<FakeLink>(c.links.back())->reset();  // gamma redials after backoff, delayed
        run_for(io, 25ms);
        m.reset();
    }
    run_for(io, 100ms);
    BOOST_TEST(t.open_links() == 0u);
    BOOST_TEST(b.goodbyes().size() == 1u);
    // A hung dial completing after destruction is harmless.
    for (auto& cb : t.hung) cb(std::unexpected(std::string("late")));
    run_for(io, 10ms);
}

BOOST_AUTO_TEST_SUITE_END()
