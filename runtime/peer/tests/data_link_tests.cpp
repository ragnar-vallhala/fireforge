// PeerManager data links (docs/design/phase5.md §2, §9) over the in-memory
// FakeTransport.
#include <boost/asio/io_context.hpp>
#include <boost/test/unit_test.hpp>
#include <optional>

#include "fake_transport.hpp"
#include "fireforge/tdr/v1/artifact.pb.h"
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

PeerOptions opts(const std::string& name, const std::string& id) {
    PeerOptions o;
    o.self_name = name;
    o.self_node_id = id;
    o.daemon_version = "test";
    o.ping_interval = 50ms;
    o.ping_timeout = 200ms;
    o.hello_timeout = 100ms;
    o.backoff_min = 20ms;
    o.backoff_max = 100ms;
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

std::string state_of(const PeerManager& m, const std::string& node_id) {
    for (auto& r : m.snapshot())
        if (r.node_id() == node_id) return r.state();
    return "<none>";
}

Frame request(const std::string& id = "artifact://sha256/x", std::uint64_t offset = 0) {
    v1::ArtifactRequest r;
    r.set_artifact_id(id);
    r.set_offset(offset);
    return make_frame(v1::ARTIFACT_REQUEST, r, next_message_id());
}

// What a DataLinkHandlers pair saw.
struct Sink {
    std::vector<Frame> frames;
    std::vector<std::string> closes;
    DataLinkHandlers handlers() {
        return {
            .on_frame = [this](const std::shared_ptr<IFrameLink>&, Frame f) { frames.push_back(std::move(f)); },
            .on_close = [this](const std::shared_ptr<IFrameLink>&, const std::string& r) { closes.push_back(r); },
        };
    }
};

// A real PeerManager on a FakeTransport with a recording data acceptor.
struct Node {
    Node(boost::asio::io_context& io, FakeNetwork& net, std::string name_, std::string id_,
         std::vector<std::string> addrs_, bool acceptor = true)
        : name(name_), id(id_), addrs(addrs_), t(net, name_, addrs_.empty() ? "" : addrs_.front()),
          mgr(io, t, opts(name_, id_)), net_(net) {
        for (auto& a : addrs) net.listen(a, &t);
        if (acceptor) {
            mgr.set_data_acceptor([this](const std::string& from, const std::shared_ptr<IFrameLink>& l,
                                         Frame first) -> std::optional<DataLinkHandlers> {
                accepted.emplace_back(from, std::move(first));
                inbound.push_back(l);
                if (!accept) return std::nullopt;
                return sink.handlers();
            });
        }
        mgr.start();
    }
    ~Node() {
        for (auto& a : addrs) net_.unlisten(a);
    }
    cv1::NodeInfo info(cv1::NodeHealth h = cv1::ONLINE) const {
        std::vector<cv1::NodeAddress> as;
        for (auto& a : addrs) as.push_back(address("lan", a));
        return node_info(id, name, h, as);
    }

    std::string name, id;
    std::vector<std::string> addrs;
    FakeTransport t;
    PeerManager mgr;
    bool accept = true;
    std::vector<std::pair<std::string, Frame>> accepted;
    std::vector<std::shared_ptr<IFrameLink>> inbound;
    Sink sink;

private:
    FakeNetwork& net_;
};

// A bare remote end: a FakeTransport the test drives by hand.
struct Raw {
    Raw(FakeNetwork& net, std::string name, std::string id)
        : t(net, name), sender(tdr::node_id_from_string(id)) {
        TransportHandlers h;
        h.on_frame = [this](const std::shared_ptr<IFrameLink>&, Frame f) { frames.push_back(std::move(f)); };
        h.on_close = [this](const std::shared_ptr<IFrameLink>&, const std::string& r) { closes.push_back(r); };
        t.set_handlers(std::move(h));
    }
    void connect(const std::string& host, std::uint16_t port, const std::string& expected) {
        t.dial(host, port, expected, [this](DialResult r) {
            BOOST_REQUIRE(r.has_value());
            link = *r;
        });
    }
    void send(Frame f) {
        f.header.sender_id = sender;
        link->send(f);
    }
    FakeTransport t;
    tdr::NodeId sender;
    std::shared_ptr<IFrameLink> link;
    std::vector<Frame> frames;
    std::vector<std::string> closes;
};

struct Fixture {
    boost::asio::io_context io;
    FakeNetwork net{io};
};

}  // namespace

BOOST_AUTO_TEST_SUITE(peer_data_links)

BOOST_FIXTURE_TEST_CASE(inbound_data_link_is_adopted_and_routed, Fixture) {
    Node a(io, net, "alpha", kAlphaId, {"10.0.0.1:7400"});
    a.mgr.update_directory(node_list({node_info(kBetaId, "beta", cv1::OFFLINE, {address("lan", "10.0.0.2:7400")})}));
    Raw b(net, "beta", kBetaId);
    b.connect("10.0.0.1", 7400, "alpha");
    BOOST_REQUIRE(run_until(io, [&] { return b.link != nullptr; }));
    b.send(request("artifact://sha256/first", 7));
    Frame second;
    second.header.message_type = v1::ARTIFACT_CHUNK_ACK;
    second.header.message_id = next_message_id();
    second.payload = "ack";
    b.send(second);
    BOOST_REQUIRE(run_until(io, [&] { return a.sink.frames.size() == 1; }));
    BOOST_REQUIRE(a.accepted.size() == 1u);
    BOOST_TEST(a.accepted[0].first == kBetaId);
    v1::ArtifactRequest r;
    BOOST_REQUIRE(a.accepted[0].second.parse(r));
    BOOST_TEST(r.artifact_id() == "artifact://sha256/first");
    BOOST_TEST(r.offset() == 7u);
    BOOST_TEST(a.sink.frames[0].payload == "ack");  // first frame not re-delivered

    // send_data fills sender and message id.
    Frame reply;
    reply.header.message_type = v1::ARTIFACT_OFFER;
    reply.payload = "offer";
    a.mgr.send_data(a.inbound[0], reply);
    BOOST_REQUIRE(run_until(io, [&] { return b.frames.size() == 1; }));
    BOOST_TEST((b.frames[0].type() == v1::ARTIFACT_OFFER));
    BOOST_TEST((b.frames[0].header.sender_id == tdr::node_id_from_string(kAlphaId)));
    BOOST_TEST(b.frames[0].header.message_id != 0u);
    BOOST_TEST(b.frames[0].header.payload_length == 5u);

    // Not a peer link: beta stays IDLE, the link outlives the hello timeout.
    run_for(io, 150ms);
    BOOST_TEST(state_of(a.mgr, kBetaId) == "IDLE");
    BOOST_TEST(a.inbound[0]->is_open());
    BOOST_TEST(b.frames.size() == 1u);  // no hello, no ping

    b.link->close();
    BOOST_REQUIRE(run_until(io, [&] { return !a.sink.closes.empty(); }));
    run_for(io, 20ms);
    BOOST_TEST(a.sink.closes.size() == 1u);
}

BOOST_FIXTURE_TEST_CASE(inbound_data_link_rejections, Fixture) {
    struct Case {
        std::string what;
        std::string cert;
        std::string sender_id;
        bool acceptor = true;
        bool accept = true;
    };
    std::vector<Case> cases{
        {"cert not in directory", "gamma", "ff-node-3333"},
        {"bad sender_id", "beta", "ff-node-9999"},
        {"no acceptor", "beta", kBetaId, false},
        {"acceptor refuses", "beta", kBetaId, true, false},
        {"claims to be us", "alpha", kAlphaId},
    };
    for (auto& c : cases) {
        BOOST_TEST_CONTEXT(c.what) {
            Fixture f;
            Node a(f.io, f.net, "alpha", kAlphaId, {"10.0.0.1:7400"}, c.acceptor);
            a.accept = c.accept;
            a.mgr.update_directory(
                node_list({node_info(kBetaId, "beta", cv1::OFFLINE, {address("lan", "10.0.0.2:7400")})}));
            Raw b(f.net, c.cert, c.sender_id);
            b.connect("10.0.0.1", 7400, "alpha");
            BOOST_REQUIRE(run_until(f.io, [&] { return b.link != nullptr; }));
            b.send(request());
            BOOST_TEST(run_until(f.io, [&] { return b.closes.size() == 1; }));
            BOOST_TEST(a.sink.closes.empty());
            BOOST_TEST(a.accepted.size() == (c.acceptor && c.what == "acceptor refuses" ? 1u : 0u));
            BOOST_TEST(state_of(a.mgr, kBetaId) == "IDLE");
        }
    }
}

BOOST_FIXTURE_TEST_CASE(bad_sender_on_later_frame_closes_data_link, Fixture) {
    Node a(io, net, "alpha", kAlphaId, {"10.0.0.1:7400"});
    a.mgr.update_directory(node_list({node_info(kBetaId, "beta", cv1::OFFLINE, {address("lan", "10.0.0.2:7400")})}));
    Raw b(net, "beta", kBetaId);
    b.connect("10.0.0.1", 7400, "alpha");
    BOOST_REQUIRE(run_until(io, [&] { return b.link != nullptr; }));
    b.send(request());
    BOOST_REQUIRE(run_until(io, [&] { return a.accepted.size() == 1; }));
    b.sender = tdr::node_id_from_string("ff-node-9999");
    Frame f;
    f.header.message_type = v1::ARTIFACT_CHUNK_ACK;
    b.send(f);
    b.send(f);
    BOOST_REQUIRE(run_until(io, [&] { return a.sink.closes.size() == 1 && b.closes.size() == 1; }));
    BOOST_TEST(a.sink.frames.empty());
    run_for(io, 20ms);
    BOOST_TEST(a.sink.closes.size() == 1u);
}

BOOST_FIXTURE_TEST_CASE(dial_data_falls_back_and_coexists_with_peer_link, Fixture) {
    Node a(io, net, "alpha", kAlphaId, {"10.0.0.1:7400"});
    Node b(io, net, "beta", kBetaId, {"100.64.0.2:7400"});
    auto beta_info = node_info(kBetaId, "beta", cv1::ONLINE,
                               {address("tailscale", "100.64.0.2:7400"), address("lan", "10.0.0.2:7400")});
    a.mgr.update_directory(node_list({a.info(), beta_info}));
    b.mgr.update_directory(node_list({a.info(), b.info()}));
    BOOST_REQUIRE(run_until(io, [&] {
        return state_of(a.mgr, kBetaId) == "CONNECTED" && state_of(b.mgr, kAlphaId) == "CONNECTED";
    }));
    auto peer_dials = a.t.dials.size();

    Sink sink;
    std::optional<DataDialResult> res;
    a.mgr.dial_data(kBetaId, sink.handlers(), [&](DataDialResult r) { res = std::move(r); });
    BOOST_TEST(!res.has_value());
    BOOST_REQUIRE(run_until(io, [&] { return res.has_value(); }));
    BOOST_REQUIRE_MESSAGE(res->has_value(), (res->has_value() ? "" : res->error()));
    std::vector<std::string> want{"10.0.0.2:7400", "100.64.0.2:7400"};
    std::vector<std::string> got(a.t.dials.begin() + static_cast<std::ptrdiff_t>(peer_dials), a.t.dials.end());
    BOOST_TEST(got == want, boost::test_tools::per_element());
    auto link = **res;
    BOOST_TEST(link->outbound());

    a.mgr.send_data(link, request("artifact://sha256/y"));
    BOOST_REQUIRE(run_until(io, [&] { return b.accepted.size() == 1; }));
    BOOST_TEST(b.accepted[0].first == kAlphaId);
    Frame back;
    back.header.message_type = v1::ARTIFACT_TRANSFER;
    back.payload = "chunk";
    b.mgr.send_data(b.inbound[0], back);
    BOOST_REQUIRE(run_until(io, [&] { return sink.frames.size() == 1; }));
    BOOST_TEST(sink.frames[0].payload == "chunk");

    // The peer link is untouched; snapshots list one peer each.
    run_for(io, 150ms);
    BOOST_TEST(state_of(a.mgr, kBetaId) == "CONNECTED");
    BOOST_TEST(state_of(b.mgr, kAlphaId) == "CONNECTED");
    BOOST_TEST(a.mgr.snapshot().size() == 1u);
    BOOST_TEST(b.mgr.snapshot().size() == 1u);
    BOOST_TEST(link->is_open());
    BOOST_TEST(b.inbound[0]->is_open());

    // Closing the data link leaves the peer link up.
    link->close();
    BOOST_REQUIRE(run_until(io, [&] { return sink.closes.size() == 1 && b.sink.closes.size() == 1; }));
    BOOST_TEST(sink.closes[0] == "closed locally");
    run_for(io, 30ms);
    BOOST_TEST(state_of(a.mgr, kBetaId) == "CONNECTED");
}

BOOST_FIXTURE_TEST_CASE(dial_data_errors, Fixture) {
    Node a(io, net, "alpha", kAlphaId, {"10.0.0.1:7400"});
    a.mgr.update_directory(node_list(
        {a.info(), node_info(kBetaId, "beta", cv1::OFFLINE, {address("lan", "10.0.0.2:7400")}),
         node_info("ff-node-3333", "gamma", cv1::OFFLINE, {})}));
    auto dial = [&](const std::string& id) {
        std::optional<DataDialResult> r;
        Sink s;
        a.mgr.dial_data(id, s.handlers(), [&](DataDialResult x) { r = std::move(x); });
        BOOST_TEST(!r.has_value());  // never inline
        BOOST_REQUIRE(run_until(io, [&] { return r.has_value(); }));
        BOOST_REQUIRE(!r->has_value());
        return r->error();
    };
    BOOST_TEST(dial("ff-node-nobody").find("unknown node") != std::string::npos);
    BOOST_TEST(dial(kAlphaId) == "cannot dial self");
    BOOST_TEST(dial(kBetaId) == "lan 10.0.0.2:7400: connection refused");  // nobody listens
    BOOST_TEST(dial("ff-node-3333") == "no addresses");
    a.mgr.stop();
    BOOST_TEST(dial(kBetaId) == "peer manager stopped");
}

BOOST_FIXTURE_TEST_CASE(stop_closes_data_links_and_fails_pending_dials, Fixture) {
    Node a(io, net, "alpha", kAlphaId, {"10.0.0.1:7400"});
    Node b(io, net, "beta", kBetaId, {"10.0.0.2:7400"});
    a.mgr.update_directory(node_list(
        {a.info(cv1::OFFLINE), b.info(cv1::OFFLINE),
         node_info("ff-node-3333", "gamma", cv1::OFFLINE, {address("lan", "10.0.0.3:7400")})}));
    b.mgr.update_directory(node_list({a.info(cv1::OFFLINE), b.info(cv1::OFFLINE)}));
    Sink out;
    std::optional<DataDialResult> res;
    a.mgr.dial_data(kBetaId, out.handlers(), [&](DataDialResult r) { res = std::move(r); });
    BOOST_REQUIRE(run_until(io, [&] { return res.has_value(); }));
    BOOST_REQUIRE(res->has_value());
    a.mgr.send_data(**res, request());
    BOOST_REQUIRE(run_until(io, [&] { return b.accepted.size() == 1; }));

    a.t.scripts["10.0.0.3:7400"] = {DialScript::kHang, ""};
    std::optional<DataDialResult> hung;
    a.mgr.dial_data("ff-node-3333", out.handlers(), [&](DataDialResult r) { hung = std::move(r); });
    run_for(io, 10ms);
    BOOST_TEST(!hung.has_value());

    a.mgr.stop();
    BOOST_REQUIRE(run_until(io, [&] { return out.closes.size() == 1 && b.sink.closes.size() == 1 && hung; }));
    BOOST_TEST(!hung->has_value());
    BOOST_TEST(hung->error() == "peer manager stopped");
    // The hung dial completing later is closed and not reported again.
    auto late = a.t.hung;
    for (auto& cb : late) cb(std::unexpected(std::string("late")));
    run_for(io, 20ms);
    BOOST_TEST(out.closes.size() == 1u);
}

BOOST_FIXTURE_TEST_CASE(destroying_with_pending_data_dial_calls_nothing, Fixture) {
    FakeTransport t(net, "alpha", "10.0.0.1:7400");
    bool called = false;
    {
        PeerManager m(io, t, opts("alpha", kAlphaId));
        m.start();
        m.update_directory(
            node_list({node_info(kBetaId, "beta", cv1::OFFLINE, {address("lan", "10.0.0.2:7400")})}));
        t.scripts["10.0.0.2:7400"] = {DialScript::kHang, ""};
        Sink s;
        m.dial_data(kBetaId, s.handlers(), [&](DataDialResult) { called = true; });
        run_for(io, 10ms);
    }
    run_for(io, 20ms);
    for (auto& cb : t.hung) cb(std::unexpected(std::string("late")));
    run_for(io, 10ms);
    BOOST_TEST(!called);
}

BOOST_AUTO_TEST_SUITE_END()
