// PeerManager (docs/design/phase3.md §2-§5).
//
// Everything runs on the io_context thread. All asynchronous work (dial
// callbacks, timers, transport handlers) captures a weak_ptr to Impl, so it is
// ignored once the manager is gone. Dial callbacks also carry the peer's dial
// round generation, so results of cancelled or superseded rounds are dropped
// (a link they deliver is closed).
//
// Links are tracked by IFrameLink::id(). A link is "detached" once the
// manager has stopped using it (closed locally, closed by the transport, or
// its peer was removed); its on_close then only erases it.
//
// Behaviour choices beyond the design doc:
// - A dial round already running when the peer connects inbound is allowed to
//   finish; if its link completes the hello, the duplicate rule picks one.
//   Failed attempts of such a round end the round quietly (no backoff).
// - Two links in the same direction (e.g. the peer redialed while an old link
//   was still up on our side): the newer one is kept.
// - A peer created from an unknown inbound hello is kept while its link is up,
//   even when a directory update doesn't list it (the update may predate the
//   refresh it triggered). It is dropped by the next update after its link has
//   closed. Peers that were listed and disappear are removed immediately.
// - Callbacks (state listener, frame handler) are called synchronously and
//   must not call update_directory() or stop() re-entrantly; the directory
//   refresh callback is posted and may do anything.
#include <algorithm>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>
#include <map>
#include <random>
#include <set>
#include <unordered_map>

#include "fireforge/tdr/v1/local.pb.h"
#include "fireforge/tdr/v1/peer.pb.h"
#include "tdr/common/common.hpp"
#include "tdr/peer/manager.hpp"

namespace tdr::peer {

namespace asio = boost::asio;
namespace v1 = ::fireforge::tdr::v1;
using Clock = std::chrono::steady_clock;
using namespace std::chrono_literals;

namespace {

constexpr std::uint32_t kProtocolVersion = 1;
constexpr double kRttAlpha = 0.3;
constexpr std::size_t kMaxOutstandingPings = 64;

int address_rank(const cv1::NodeAddress& a) {
    if (a.type() == "lan") return 0;
    if (a.type() == "tailscale") return 1;
    return 2;
}

std::string host_port(const std::string& host, std::uint32_t port) {
    if (host.find(':') != std::string::npos) return std::format("[{}]:{}", host, port);
    return std::format("{}:{}", host, port);
}

// "ip:port" / "[v6]:port" -> "ip".
std::string host_of(const std::string& remote) {
    auto pos = remote.rfind(':');
    std::string h = pos == std::string::npos ? remote : remote.substr(0, pos);
    if (h.size() >= 2 && h.front() == '[' && h.back() == ']') h = h.substr(1, h.size() - 2);
    return h;
}

}  // namespace

const char* to_string(PeerState s) {
    switch (s) {
        case PeerState::kConnecting: return "CONNECTING";
        case PeerState::kConnected: return "CONNECTED";
        case PeerState::kUnavailable: return "UNAVAILABLE";
        case PeerState::kIdle: return "IDLE";
    }
    return "UNKNOWN";
}

std::vector<cv1::NodeAddress> dial_order(const cv1::NodeInfo& node) {
    std::vector<cv1::NodeAddress> out(node.addresses().begin(), node.addresses().end());
    std::stable_sort(out.begin(), out.end(),
                     [](const cv1::NodeAddress& a, const cv1::NodeAddress& b) { return address_rank(a) < address_rank(b); });
    return out;
}

namespace {

struct Link {
    Link(asio::io_context& io, std::shared_ptr<IFrameLink> l)
        : link(std::move(l)), id(link->id()), outbound(link->outbound()), hello_timer(io), ping_timer(io) {}

    std::shared_ptr<IFrameLink> link;
    std::uint64_t id;
    bool outbound;
    std::string peer_node_id;      // outbound: dial target; inbound: set at hello
    NodeId peer_sender{};          // node_id_from_string(peer_node_id), after hello
    std::uint64_t round_gen = 0;   // outbound: the dial round that produced it
    std::uint64_t hello_msg_id = 0;
    bool hello_done = false;
    bool detached = false;
    std::string path_type;
    std::string address;
    asio::steady_timer hello_timer;
    asio::steady_timer ping_timer;
    Clock::time_point last_received = Clock::now();
    std::uint64_t next_seq = 1;
    std::map<std::uint64_t, Clock::time_point> pings;  // seq -> sent
    double rtt_ms = 0;
    std::int64_t connected_since = 0;
};

struct Peer {
    explicit Peer(asio::io_context& io) : dial_timer(io) {}

    std::string node_id;
    std::string name;
    cv1::NodeInfo info;            // addresses for the next dial round
    bool in_directory = false;
    bool dial_target = false;
    PeerState state = PeerState::kIdle;
    std::uint64_t active_link = 0; // id of the CONNECTED link, 0 = none
    bool dialing = false;          // a round is in progress
    std::uint64_t dial_gen = 0;    // bumped per round and on cancel
    std::uint64_t timer_gen = 0;
    bool dial_scheduled = false;
    std::vector<cv1::NodeAddress> round;
    std::size_t round_idx = 0;
    std::string attempt_label;     // "lan 10.0.0.1:7400"
    std::chrono::milliseconds backoff{0};
    std::uint32_t failed_rounds = 0;
    std::string last_error;
    asio::steady_timer dial_timer;
};

}  // namespace

struct PeerManager::Impl : std::enable_shared_from_this<Impl> {
    Impl(asio::io_context& io_, ITransport& t, PeerOptions o)
        : io(io_), transport(t), opt(std::move(o)), self_sender(node_id_from_string(opt.self_node_id)),
          rng(std::random_device{}()) {}

    asio::io_context& io;
    ITransport& transport;
    PeerOptions opt;
    NodeId self_sender;
    std::mt19937 rng;
    bool stopped = false;
    std::map<std::string, std::unique_ptr<Peer>> peers;                 // by node_id
    std::unordered_map<std::uint64_t, std::unique_ptr<Link>> links;     // by link id
    std::function<void(const std::string&, protocol::Frame)> frame_handler;
    std::function<void(const std::string&, PeerState)> state_listener;
    std::function<void()> refresh;

    // ---- lookup -------------------------------------------------------------

    Peer* find_peer(const std::string& id) {
        auto it = peers.find(id);
        return it == peers.end() ? nullptr : it->second.get();
    }
    Peer* find_peer_by_name(const std::string& name) {
        for (auto& [_, p] : peers)
            if (p->name == name) return p.get();
        return nullptr;
    }
    Link* find_link(std::uint64_t id) {
        auto it = links.find(id);
        return it == links.end() ? nullptr : it->second.get();
    }
    Link* active(const Peer& p) { return p.active_link ? find_link(p.active_link) : nullptr; }

    void set_state(Peer& p, PeerState s) {
        if (p.state == s) return;
        log_info("peer {} ({}): {} -> {}", p.name, p.node_id, to_string(p.state), to_string(s));
        p.state = s;
        if (state_listener) {
            auto fn = state_listener;
            std::string id = p.node_id;
            fn(id, s);
        }
    }

    // ---- sending --------------------------------------------------------------

    void send_frame(Link& l, protocol::Frame f) {
        f.header.sender_id = self_sender;
        l.link->send(f);
    }
    void send_goodbye(Link& l, const std::string& reason) {
        v1::PeerGoodbye g;
        g.set_reason(reason);
        send_frame(l, protocol::make_frame(v1::PEER_ADVERTISE, g, protocol::next_message_id()));
    }
    void send_error(Link& l, v1::ErrorCode code, const std::string& msg, std::uint64_t correlation) {
        v1::Error e;
        e.set_code(code);
        e.set_message(msg);
        send_frame(l, protocol::make_frame(v1::ERROR, e, protocol::next_message_id(), correlation));
    }
    v1::PeerHello my_hello() const {
        v1::PeerHello h;
        h.set_node_id(opt.self_node_id);
        h.set_name(opt.self_name);
        h.set_daemon_version(opt.daemon_version);
        h.set_protocol_version(kProtocolVersion);
        h.set_sent_unix_ms(unix_ms());
        return h;
    }

    // ---- link teardown --------------------------------------------------------

    // Stop using the link; update its peer. Idempotent.
    void detach(Link& l, const std::string& reason) {
        if (l.detached) return;
        l.detached = true;
        l.hello_timer.cancel();
        l.ping_timer.cancel();
        Peer* p = l.peer_node_id.empty() ? nullptr : find_peer(l.peer_node_id);
        if (!p) return;
        if (p->active_link == l.id) {
            p->active_link = 0;
            p->last_error = reason;
            log_info("peer {}: link {} lost: {}", p->name, l.id, reason);
            on_active_lost(*p);
        } else if (!l.hello_done && l.outbound && p->dialing && l.round_gen == p->dial_gen) {
            attempt_failed(*p, reason);
        }
    }

    void close_link(Link& l, const std::string& reason) {
        detach(l, reason);
        l.link->close();
    }

    void on_active_lost(Peer& p) {
        if (stopped) return;
        p.backoff = 0ms;
        p.failed_rounds = 0;
        if (p.dial_target) {
            if (!p.dialing) schedule_round(p, opt.backoff_min);
            set_state(p, PeerState::kConnecting);
        } else {
            set_state(p, PeerState::kIdle);
        }
    }

    // ---- dialing --------------------------------------------------------------

    std::chrono::milliseconds jitter(std::chrono::milliseconds base) {
        std::uniform_real_distribution<double> d(0.8, 1.2);
        return std::chrono::milliseconds(static_cast<std::int64_t>(static_cast<double>(base.count()) * d(rng)));
    }

    void cancel_schedule(Peer& p) {
        ++p.timer_gen;
        p.dial_scheduled = false;
        p.dial_timer.cancel();
    }

    void cancel_round(Peer& p) {
        cancel_schedule(p);
        if (p.dialing) {
            p.dialing = false;
            ++p.dial_gen;
        }
    }

    void schedule_round(Peer& p, std::chrono::milliseconds delay) {
        if (stopped) return;
        p.dial_scheduled = true;
        auto gen = ++p.timer_gen;
        p.dial_timer.expires_after(delay);
        p.dial_timer.async_wait([w = weak_from_this(), id = p.node_id, gen](const boost::system::error_code& ec) {
            if (ec) return;
            auto self = w.lock();
            if (!self) return;
            Peer* peer = self->find_peer(id);
            if (!peer || peer->timer_gen != gen) return;
            peer->dial_scheduled = false;
            self->start_round(*peer);
        });
    }

    void start_round(Peer& p) {
        if (stopped || !p.dial_target || p.active_link || p.dialing) return;
        cancel_schedule(p);
        p.dialing = true;
        ++p.dial_gen;
        p.round = dial_order(p.info);
        p.round_idx = 0;
        log_debug("peer {}: dial round {} ({} addresses)", p.name, p.dial_gen, p.round.size());
        if (p.state != PeerState::kUnavailable) set_state(p, PeerState::kConnecting);
        try_next(p);
    }

    void attempt_failed(Peer& p, const std::string& reason) {
        p.last_error = p.attempt_label.empty() ? reason : std::format("{}: {}", p.attempt_label, reason);
        log_debug("peer {}: dial failed: {}", p.name, p.last_error);
        try_next(p);
    }

    void try_next(Peer& p) {
        while (true) {
            if (stopped || !p.dialing) return;
            if (p.active_link) {  // connected inbound meanwhile: end the round quietly
                p.dialing = false;
                return;
            }
            if (p.round_idx >= p.round.size()) {
                round_failed(p);
                return;
            }
            cv1::NodeAddress a = p.round[p.round_idx++];
            p.attempt_label = std::format("{} {}", a.type(), host_port(a.address(), a.port()));
            if (a.address().empty() || a.port() == 0 || a.port() > 65535) {
                p.last_error = p.attempt_label + ": invalid address";
                continue;
            }
            auto gen = p.dial_gen;
            transport.dial(a.address(), static_cast<std::uint16_t>(a.port()), p.name,
                           [w = weak_from_this(), id = p.node_id, gen, a](DialResult r) {
                               auto self = w.lock();
                               if (!self) {
                                   if (r) (*r)->close();
                                   return;
                               }
                               self->on_dial_result(id, gen, a, std::move(r));
                           });
            return;
        }
    }

    void round_failed(Peer& p) {
        p.dialing = false;
        ++p.failed_rounds;
        if (p.round.empty()) p.last_error = "no addresses";
        p.backoff = p.backoff.count() <= 0 ? std::min(opt.backoff_min, opt.backoff_max)
                                           : std::min(p.backoff * 2, opt.backoff_max);
        auto delay = jitter(p.backoff);
        log_debug("peer {}: round failed ({} in a row), retry in {} ms", p.name, p.failed_rounds, delay.count());
        schedule_round(p, delay);
        set_state(p, p.failed_rounds >= opt.unavailable_after_rounds ? PeerState::kUnavailable
                                                                     : PeerState::kConnecting);
    }

    void on_dial_result(const std::string& id, std::uint64_t gen, const cv1::NodeAddress& a, DialResult r) {
        Peer* p = find_peer(id);
        if (stopped || !p || !p->dialing || p->dial_gen != gen) {
            if (r) (*r)->close();
            return;
        }
        if (!r) {
            attempt_failed(*p, r.error());
            return;
        }
        auto owned = std::make_unique<Link>(io, *r);
        Link& l = *owned;
        l.peer_node_id = id;
        l.round_gen = gen;
        l.path_type = a.type();
        l.address = host_port(a.address(), a.port());
        if (!links.emplace(l.id, std::move(owned)).second) {
            log_warn("peer {}: transport reused link id {}", p->name, (*r)->id());
            (*r)->close();
            attempt_failed(*p, "duplicate link id");
            return;
        }
        l.hello_msg_id = protocol::next_message_id();
        send_frame(l, protocol::make_frame(v1::NODE_HELLO, my_hello(), l.hello_msg_id));
        arm_hello_timer(l);
    }

    // ---- hello ----------------------------------------------------------------

    void arm_hello_timer(Link& l) {
        l.hello_timer.expires_after(opt.hello_timeout);
        l.hello_timer.async_wait([w = weak_from_this(), id = l.id](const boost::system::error_code& ec) {
            if (ec) return;
            auto self = w.lock();
            if (!self) return;
            Link* link = self->find_link(id);
            if (!link || link->detached || link->hello_done) return;
            self->close_link(*link, "hello timeout");
        });
    }

    void on_accept(const std::shared_ptr<IFrameLink>& link) {
        if (stopped) {
            link->close();
            return;
        }
        auto owned = std::make_unique<Link>(io, link);
        Link& l = *owned;
        l.address = link->remote();
        if (!links.emplace(l.id, std::move(owned)).second) {
            link->close();
            return;
        }
        log_debug("peer link {} accepted from {} ({})", l.id, l.address, link->identity().name);
        arm_hello_timer(l);
    }

    void outbound_pre_hello(Link& l, const protocol::Frame& f) {
        Peer* p = find_peer(l.peer_node_id);
        if (!p) {
            close_link(l, "peer removed");
            return;
        }
        switch (f.type()) {
            case v1::NODE_HELLO: {
                if (f.header.correlation_id != l.hello_msg_id) {
                    close_link(l, "protocol error: unexpected hello");
                    return;
                }
                v1::PeerHello h;
                if (!f.parse(h)) {
                    close_link(l, "protocol error: bad hello");
                    return;
                }
                if (h.protocol_version() != kProtocolVersion) {
                    send_error(l, v1::UNSUPPORTED, "unsupported protocol_version", f.header.message_id);
                    close_link(l, std::format("unsupported protocol_version {}", h.protocol_version()));
                    return;
                }
                const std::string& cert = l.link->identity().name;
                if (h.name() != cert || h.name() != p->name) {
                    close_link(l, std::format("hello name mismatch: '{}' (certificate '{}', directory '{}')",
                                              h.name(), cert, p->name));
                    return;
                }
                if (h.node_id() != p->node_id) {
                    close_link(l, std::format("hello node_id mismatch: '{}' (directory '{}')", h.node_id(),
                                              p->node_id));
                    return;
                }
                if (f.header.sender_id != node_id_from_string(h.node_id())) {
                    close_link(l, "bad sender_id");
                    return;
                }
                complete_hello(l, *p);
                return;
            }
            case v1::ERROR: {
                v1::Error e;
                (void)f.parse(e);
                close_link(l, std::format("rejected by peer: {} ({})", e.message(), v1::ErrorCode_Name(e.code())));
                return;
            }
            case v1::PEER_ADVERTISE: {
                v1::PeerGoodbye g;
                (void)f.parse(g);
                close_link(l, "peer said goodbye: " + g.reason());
                return;
            }
            default:
                close_link(l, std::format("protocol error: message type {:#x} before hello", f.header.message_type));
                return;
        }
    }

    void reject(Link& l, v1::ErrorCode code, const std::string& msg, std::uint64_t correlation) {
        log_warn("peer link {} from {} ({}) rejected: {}", l.id, l.address, l.link->identity().name, msg);
        send_error(l, code, msg, correlation);
        close_link(l, msg);
    }

    void inbound_pre_hello(Link& l, const protocol::Frame& f) {
        if (f.type() != v1::NODE_HELLO || f.header.correlation_id != 0) {
            log_warn("peer link {} from {}: message type {:#x} before hello", l.id, l.address, f.header.message_type);
            close_link(l, "protocol error before hello");
            return;
        }
        const auto corr = f.header.message_id;
        v1::PeerHello h;
        if (!f.parse(h)) return reject(l, v1::BAD_REQUEST, "bad hello", corr);
        if (h.protocol_version() != kProtocolVersion)
            return reject(l, v1::UNSUPPORTED, std::format("unsupported protocol_version {}", h.protocol_version()),
                          corr);
        const std::string& cert = l.link->identity().name;
        if (h.name() != cert)
            return reject(l, v1::UNAUTHORIZED,
                          std::format("hello name '{}' does not match certificate '{}'", h.name(), cert), corr);
        if (h.node_id().empty()) return reject(l, v1::BAD_REQUEST, "hello without node_id", corr);
        if (h.node_id() == opt.self_node_id || h.name() == opt.self_name)
            return reject(l, v1::UNAUTHORIZED, "hello claims to be this node", corr);
        if (f.header.sender_id != node_id_from_string(h.node_id()))
            return reject(l, v1::BAD_REQUEST, "bad sender_id", corr);
        if (Peer* by_name = find_peer_by_name(h.name()); by_name && by_name->node_id != h.node_id())
            return reject(l, v1::UNAUTHORIZED,
                          std::format("node_id '{}' does not match directory for '{}'", h.node_id(), h.name()), corr);
        Peer* p = find_peer(h.node_id());
        if (p && p->name != h.name())
            return reject(l, v1::UNAUTHORIZED, std::format("node_id '{}' belongs to '{}'", h.node_id(), p->name),
                          corr);
        bool unknown = false;
        if (!p) {
            auto owned = std::make_unique<Peer>(io);
            owned->node_id = h.node_id();
            owned->name = h.name();
            p = owned.get();
            peers.emplace(p->node_id, std::move(owned));
            unknown = true;
            log_info("peer {} ({}) connected inbound but is not in the directory; requesting refresh", h.name(),
                     h.node_id());
        }
        l.peer_node_id = h.node_id();
        send_frame(l, protocol::make_frame(v1::NODE_HELLO, my_hello(), protocol::next_message_id(), corr));
        complete_hello(l, *p);
        if (unknown) {
            asio::post(io, [w = weak_from_this()] {
                auto self = w.lock();
                if (self && !self->stopped && self->refresh) {
                    auto fn = self->refresh;
                    fn();
                }
            });
        }
    }

    std::string classify_inbound(const Peer& p, const std::string& remote) const {
        auto host = host_of(remote);
        for (const auto& a : p.info.addresses())
            if (a.address() == host) return a.type();
        return "inbound";
    }

    // Dedup rule: keep the link dialed by the node with the lower node_id;
    // two links in the same direction: keep the newer.
    bool keep_new(const Link& cur, const Link& nw, const Peer& p) const {
        if (cur.outbound == nw.outbound) return true;
        bool self_lower = opt.self_node_id < p.node_id;
        return self_lower ? nw.outbound : !nw.outbound;
    }

    void complete_hello(Link& l, Peer& p) {
        l.hello_done = true;
        l.hello_timer.cancel();
        l.peer_sender = node_id_from_string(p.node_id);
        l.connected_since = unix_ms();
        l.last_received = Clock::now();
        if (!l.outbound) l.path_type = classify_inbound(p, l.address);
        arm_ping(l);
        if (l.outbound && p.dialing && l.round_gen == p.dial_gen) p.dialing = false;  // round succeeded

        if (Link* cur = active(p); cur && cur != &l) {
            bool keep = keep_new(*cur, l, p);
            Link& drop = keep ? *cur : l;
            log_info("peer {}: duplicate link, keeping {} link {}, closing {} link {}", p.name,
                     keep ? (l.outbound ? "outbound" : "inbound") : (cur->outbound ? "outbound" : "inbound"),
                     keep ? l.id : cur->id, drop.outbound ? "outbound" : "inbound", drop.id);
            if (keep) p.active_link = l.id;
            send_goodbye(drop, "duplicate");
            close_link(drop, "duplicate");
            return;
        }
        p.active_link = l.id;
        p.failed_rounds = 0;
        p.backoff = 0ms;
        cancel_schedule(p);
        log_info("peer {} connected ({} {} {})", p.name, l.outbound ? "outbound" : "inbound", l.path_type, l.address);
        set_state(p, PeerState::kConnected);
    }

    // ---- connected links ----------------------------------------------------

    void arm_ping(Link& l) {
        l.ping_timer.expires_after(opt.ping_interval);
        l.ping_timer.async_wait([w = weak_from_this(), id = l.id](const boost::system::error_code& ec) {
            if (ec) return;
            auto self = w.lock();
            if (!self) return;
            Link* link = self->find_link(id);
            if (!link || link->detached) return;
            self->ping_tick(*link);
        });
    }

    void ping_tick(Link& l) {
        auto now = Clock::now();
        if (now - l.last_received > opt.ping_timeout) {
            close_link(l, "ping timeout");
            return;
        }
        v1::PeerPing ping;
        auto seq = l.next_seq++;
        ping.set_seq(seq);
        ping.set_sent_unix_ms(unix_ms());
        l.pings[seq] = now;
        while (l.pings.size() > kMaxOutstandingPings) l.pings.erase(l.pings.begin());
        send_frame(l, protocol::make_frame(v1::NODE_HEARTBEAT, ping, protocol::next_message_id()));
        arm_ping(l);
    }

    void on_frame(const std::shared_ptr<IFrameLink>& link, protocol::Frame f) {
        Link* l = find_link(link->id());
        if (!l || l->detached) return;
        if (!l->hello_done) {
            if (l->outbound)
                outbound_pre_hello(*l, f);
            else
                inbound_pre_hello(*l, f);
            return;
        }
        Peer* p = find_peer(l->peer_node_id);
        if (!p) {
            close_link(*l, "peer removed");
            return;
        }
        if (f.header.sender_id != l->peer_sender) {
            log_warn("peer {}: frame type {:#x} with bad sender_id, closing link", p->name, f.header.message_type);
            close_link(*l, "bad sender_id");
            return;
        }
        auto now = Clock::now();
        l->last_received = now;
        switch (f.type()) {
            case v1::NODE_HEARTBEAT: {
                v1::PeerPing ping;
                if (!f.parse(ping)) {
                    log_debug("peer {}: unparsable ping", p->name);
                    return;
                }
                if (f.header.correlation_id == 0) {
                    send_frame(*l, protocol::make_frame(v1::NODE_HEARTBEAT, ping, protocol::next_message_id(),
                                                        f.header.message_id));
                    return;
                }
                auto it = l->pings.find(ping.seq());
                if (it == l->pings.end()) return;
                double sample = std::chrono::duration<double, std::milli>(now - it->second).count();
                sample = std::max(sample, 0.001);
                l->pings.erase(it);
                l->rtt_ms = l->rtt_ms == 0 ? sample : kRttAlpha * sample + (1 - kRttAlpha) * l->rtt_ms;
                return;
            }
            case v1::NODE_HELLO:
                log_debug("peer {}: ignoring hello after handshake", p->name);
                return;
            case v1::PEER_ADVERTISE: {
                v1::PeerGoodbye g;
                (void)f.parse(g);
                std::string reason = "peer said goodbye: " + g.reason();
                log_info("peer {}: {}", p->name, reason);
                p->last_error = reason;
                close_link(*l, reason);
                return;
            }
            case v1::ERROR: {
                v1::Error e;
                (void)f.parse(e);
                std::string reason = std::format("peer error: {} ({})", e.message(), v1::ErrorCode_Name(e.code()));
                log_warn("peer {}: {}", p->name, reason);
                close_link(*l, reason);
                return;
            }
            default:
                if (frame_handler) {
                    auto fn = frame_handler;
                    std::string id = p->node_id;
                    fn(id, std::move(f));
                } else {
                    log_debug("peer {}: no handler for message type {:#x}", p->name, f.header.message_type);
                }
                return;
        }
    }

    void on_close(const std::shared_ptr<IFrameLink>& link, const std::string& reason) {
        auto it = links.find(link->id());
        if (it == links.end()) return;
        auto holder = std::move(it->second);
        links.erase(it);
        detach(*holder, "link closed: " + reason);
    }

    // ---- directory ------------------------------------------------------------

    void remove_peer(const std::string& id, const std::string& reason) {
        auto it = peers.find(id);
        if (it == peers.end()) return;
        log_info("peer {} ({}) removed from directory", it->second->name, id);
        cancel_round(*it->second);
        peers.erase(it);
        std::vector<std::uint64_t> ids;
        for (auto& [lid, l] : links)
            if (l->peer_node_id == id && !l->detached) ids.push_back(lid);
        for (auto lid : ids) {
            if (Link* l = find_link(lid)) {
                send_goodbye(*l, reason);
                close_link(*l, reason);
            }
        }
    }

    void update_directory(const cv1::NodeList& nodes) {
        if (stopped) return;
        std::set<std::string> seen;
        for (const auto& n : nodes.nodes()) {
            if (n.node_id().empty() || n.node_id() == opt.self_node_id || n.name() == opt.self_name) continue;
            if (!seen.insert(n.node_id()).second) continue;
            Peer* p = find_peer(n.node_id());
            if (p && p->name != n.name()) {  // same id, new name: start over
                remove_peer(n.node_id(), "removed");
                p = nullptr;
            }
            if (Peer* other = find_peer_by_name(n.name()); other && other != p) {
                remove_peer(other->node_id, "removed");  // name now belongs to another id
            }
            if (!p) {
                auto owned = std::make_unique<Peer>(io);
                owned->node_id = n.node_id();
                owned->name = n.name();
                p = owned.get();
                peers.emplace(p->node_id, std::move(owned));
            }
            p->info = n;
            p->in_directory = true;
            bool was_target = p->dial_target;
            bool target = n.health() == cv1::ONLINE || n.health() == cv1::STALE;
            p->dial_target = target;
            if (target) {
                if (!p->active_link && !p->dialing && (!was_target || !p->dial_scheduled)) start_round(*p);
            } else {
                cancel_round(*p);
                if (!p->active_link) set_state(*p, PeerState::kIdle);
            }
        }
        std::vector<std::string> gone;
        for (auto& [id, p] : peers)
            if (!seen.contains(id) && (p->in_directory || !p->active_link)) gone.push_back(id);
        for (const auto& id : gone) remove_peer(id, "removed");
    }

    // ---- stop / snapshot --------------------------------------------------------

    void stop() {
        if (stopped) return;
        stopped = true;
        for (auto& [_, p] : peers) cancel_round(*p);
        std::vector<std::uint64_t> ids;
        for (auto& [lid, l] : links)
            if (!l->detached) ids.push_back(lid);
        for (auto lid : ids) {
            if (Link* l = find_link(lid)) {
                send_goodbye(*l, "shutdown");
                close_link(*l, "shutdown");
            }
        }
        std::vector<std::string> pids;
        for (auto& [id, _] : peers) pids.push_back(id);
        for (const auto& id : pids)
            if (Peer* p = find_peer(id)) set_state(*p, PeerState::kIdle);
    }

    std::vector<cv1::PeerReachability> snapshot() {
        std::vector<const Peer*> order;
        for (auto& [_, p] : peers) order.push_back(p.get());
        std::sort(order.begin(), order.end(), [](const Peer* a, const Peer* b) {
            return a->name != b->name ? a->name < b->name : a->node_id < b->node_id;
        });
        std::vector<cv1::PeerReachability> out;
        out.reserve(order.size());
        for (const Peer* p : order) {
            cv1::PeerReachability r;
            r.set_node_id(p->node_id);
            r.set_name(p->name);
            r.set_state(to_string(p->state));
            r.set_last_error(p->last_error);
            if (Link* l = active(*p)) {
                r.set_path_type(l->path_type);
                r.set_address(l->address);
                r.set_rtt_ms(l->rtt_ms);
                r.set_connected_since_unix_ms(l->connected_since);
                r.set_outbound(l->outbound);
            }
            out.push_back(std::move(r));
        }
        return out;
    }

    bool send(const std::string& node_id, protocol::Frame f) {
        if (stopped) return false;
        Peer* p = find_peer(node_id);
        if (!p || p->state != PeerState::kConnected) return false;
        Link* l = active(*p);
        if (!l || l->detached || !l->link->is_open()) return false;
        if (f.header.message_id == 0) f.header.message_id = protocol::next_message_id();
        f.header.payload_length = static_cast<std::uint32_t>(f.payload.size());
        send_frame(*l, std::move(f));
        return true;
    }
};

// ---- PeerManager ----------------------------------------------------------------

PeerManager::PeerManager(asio::io_context& io, ITransport& transport, PeerOptions options)
    : impl_(std::make_shared<Impl>(io, transport, std::move(options))) {}

PeerManager::~PeerManager() {
    if (!impl_) return;
    // The owner's callbacks may already be gone; don't call them while tearing down.
    impl_->state_listener = nullptr;
    impl_->frame_handler = nullptr;
    impl_->refresh = nullptr;
    impl_->stop();
}

void PeerManager::start() {
    std::weak_ptr<Impl> w = impl_;
    TransportHandlers h;
    h.on_accept = [w](const std::shared_ptr<IFrameLink>& link) {
        if (auto self = w.lock())
            self->on_accept(link);
        else
            link->close();
    };
    h.on_frame = [w](const std::shared_ptr<IFrameLink>& link, protocol::Frame f) {
        if (auto self = w.lock()) self->on_frame(link, std::move(f));
    };
    h.on_close = [w](const std::shared_ptr<IFrameLink>& link, const std::string& reason) {
        if (auto self = w.lock()) self->on_close(link, reason);
    };
    impl_->transport.set_handlers(std::move(h));
}

void PeerManager::stop() { impl_->stop(); }

void PeerManager::update_directory(const cv1::NodeList& nodes) { impl_->update_directory(nodes); }

std::vector<cv1::PeerReachability> PeerManager::snapshot() const { return impl_->snapshot(); }

bool PeerManager::send(const std::string& node_id, protocol::Frame frame) {
    return impl_->send(node_id, std::move(frame));
}

void PeerManager::set_frame_handler(std::function<void(const std::string& node_id, protocol::Frame)> fn) {
    impl_->frame_handler = std::move(fn);
}

void PeerManager::set_state_listener(std::function<void(const std::string& node_id, PeerState)> fn) {
    impl_->state_listener = std::move(fn);
}

void PeerManager::set_directory_refresh(std::function<void()> fn) { impl_->refresh = std::move(fn); }

}  // namespace tdr::peer
