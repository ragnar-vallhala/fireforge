// NodeAgent: registration, heartbeats, live policy, peer cache, graceful offline
// (phase2.md §7). Everything runs on the io_context thread.
//
// Lifetime: all asynchronous work (client callbacks, timer handlers) captures a
// weak_ptr to Impl, so callbacks arriving after the agent is destroyed are
// ignored. Each request kind has at most one request in flight; a generation
// counter per kind discards replies that were superseded.
#include <fcntl.h>
#include <unistd.h>

#include <google/protobuf/util/json_util.h>

#include <algorithm>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>
#include <cerrno>
#include <cstring>
#include <fstream>
#include <sstream>
#include <system_error>

#include "agent_internal.hpp"
#include "tdr/agent/agent.hpp"
#include "tdr/common/common.hpp"

namespace tdr::agent {

namespace asio = boost::asio;
using namespace std::chrono_literals;

namespace detail {

std::chrono::milliseconds g_interval_unit{1000};

std::chrono::milliseconds jitter(std::chrono::milliseconds base, std::mt19937& rng) {
    std::uniform_real_distribution<double> d(0.8, 1.2);
    return std::chrono::milliseconds(static_cast<std::int64_t>(static_cast<double>(base.count()) * d(rng)));
}

std::chrono::milliseconds next_backoff_base(std::chrono::milliseconds current, std::chrono::milliseconds min,
                                            std::chrono::milliseconds max) {
    if (current.count() <= 0) return std::min(min, max);
    return std::min(current * 2, max);
}

}  // namespace detail

const char* to_string(ControlState s) {
    switch (s) {
        case ControlState::kStandalone: return "standalone";
        case ControlState::kRegistering: return "registering";
        case ControlState::kConnected: return "connected";
        case ControlState::kDisconnected: return "disconnected";
    }
    return "unknown";
}

namespace {

namespace pbu = google::protobuf::util;

std::optional<cv1::ResourcePolicy> load_policy(const fs::path& path) {
    if (path.empty()) return std::nullopt;
    std::ifstream in(path);
    if (!in) return std::nullopt;
    std::stringstream ss;
    ss << in.rdbuf();
    cv1::ResourcePolicy p;
    pbu::JsonParseOptions po;
    po.ignore_unknown_fields = true;
    if (!pbu::JsonStringToMessage(ss.str(), &p, po).ok()) {
        log_warn("control: ignoring unreadable policy cache {}", path.string());
        return std::nullopt;
    }
    return p;
}

// Atomic replace: write tmp, fsync, rename.
void save_policy(const fs::path& path, const cv1::ResourcePolicy& p) {
    if (path.empty()) return;
    std::string json;
    pbu::JsonPrintOptions opts;
    opts.preserve_proto_field_names = true;
    opts.always_print_primitive_fields = true;
    (void)pbu::MessageToJsonString(p, &json, opts);
    json += '\n';

    std::error_code ec;
    if (path.has_parent_path()) fs::create_directories(path.parent_path(), ec);
    const fs::path tmp = path.string() + ".tmp";
    int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    if (fd < 0) {
        log_warn("control: cannot write policy cache {}: {}", tmp.string(), std::strerror(errno));
        return;
    }
    const char* data = json.data();
    std::size_t left = json.size();
    bool ok = true;
    while (left > 0) {
        ssize_t n = ::write(fd, data, left);
        if (n < 0) {
            if (errno == EINTR) continue;
            ok = false;
            break;
        }
        data += n;
        left -= static_cast<std::size_t>(n);
    }
    if (ok && ::fsync(fd) != 0) ok = false;
    if (::close(fd) != 0) ok = false;
    if (ok) {
        fs::rename(tmp, path, ec);
        ok = !ec;
    }
    if (!ok) {
        log_warn("control: cannot write policy cache {}", path.string());
        fs::remove(tmp, ec);
    }
}

std::string describe(const cv1::ResourcePolicy& p) {
    return std::format("version {} (profile '{}', cores {}, memory {}, jobs {})", p.version(), p.profile(),
                       p.max_cpu_cores(), p.max_memory_bytes(), p.max_jobs());
}

std::string describe(const ClientError& e) {
    return e.http_status == 0 ? e.message : std::format("HTTP {}: {}", e.http_status, e.message);
}

}  // namespace

struct NodeAgent::Impl : std::enable_shared_from_this<Impl> {
    asio::io_context& io;
    IControlClient& client;
    AgentOptions opts;
    AgentHooks hooks;

    ControlState state = ControlState::kStandalone;
    bool started = false;
    bool stopped = false;
    std::string node_id;
    std::optional<cv1::ResourcePolicy> applied;
    std::int64_t last_hb_ok_ms = 0;
    std::string last_error;
    cv1::NodeList peers;
    std::int64_t peers_ms = 0;

    std::chrono::milliseconds interval{10s};
    std::chrono::milliseconds backoff{0};  // current base (before jitter); 0 = reset
    std::mt19937 rng{std::random_device{}()};

    asio::steady_timer reg_timer{io};
    asio::steady_timer hb_timer{io};
    asio::steady_timer peers_timer{io};
    asio::steady_timer offline_timer{io};

    bool reg_in_flight = false, hb_in_flight = false, peers_in_flight = false;
    std::uint64_t reg_gen = 0, hb_gen = 0, peers_gen = 0;

    Impl(asio::io_context& io_, IControlClient& c, AgentOptions o, AgentHooks h)
        : io(io_), client(c), opts(std::move(o)), hooks(std::move(h)) {}

    std::weak_ptr<Impl> weak() { return weak_from_this(); }

    std::chrono::milliseconds next_backoff() {
        backoff = detail::next_backoff_base(backoff, opts.backoff_min, opts.backoff_max);
        return detail::jitter(backoff, rng);
    }

    void apply_capacity(const jobs::Capacity& c) {
        if (hooks.apply_capacity) hooks.apply_capacity(c);
    }

    void apply_policy(const cv1::ResourcePolicy& p) {
        applied = p;
        const auto cap = effective_capacity(opts.local_capacity, p);
        apply_capacity(cap);
        save_policy(opts.policy_cache, p);
        log_info("control: applied policy {} -> capacity {} cores, {} bytes, {} jobs", describe(p), cap.cpu_cores,
                 cap.memory_bytes, cap.jobs);
    }

    void maybe_apply(const cv1::ResourcePolicy& p) {
        if (!applied || applied->version() != p.version()) apply_policy(p);
    }

    void set_interval(std::uint32_t seconds) {
        if (seconds == 0) return;  // keep current
        interval = detail::g_interval_unit * std::max<std::uint32_t>(seconds, 1);
    }

    // ---- lifecycle ---------------------------------------------------------

    void start() {
        if (started || stopped) return;
        started = true;
        if (auto cached = load_policy(opts.policy_cache)) {
            applied = *cached;
            const auto cap = effective_capacity(opts.local_capacity, *cached);
            apply_capacity(cap);
            log_info("control: using cached policy {}", describe(*cached));
        } else {
            apply_capacity(opts.local_capacity);
        }
        state = ControlState::kRegistering;
        send_register();
    }

    void cancel_timers() {
        reg_timer.cancel();
        hb_timer.cancel();
        peers_timer.cancel();
    }

    void stop(std::function<void()> done) {
        const bool first = !stopped;
        stopped = true;
        cancel_timers();
        if (!first || node_id.empty()) {
            asio::post(io, std::move(done));
            return;
        }
        // Whichever of {offline reply, timeout} comes first calls done; the timer
        // handler also runs (operation_aborted) if the agent is destroyed meanwhile.
        struct Finish {
            bool fired = false;
            std::function<void()> done;
        };
        auto shared_finish = std::make_shared<Finish>(Finish{false, std::move(done)});
        auto finish = [shared_finish, w = weak(), ex = io.get_executor()] {
            if (shared_finish->fired) return;
            shared_finish->fired = true;
            if (auto self = w.lock()) self->offline_timer.cancel();
            asio::post(ex, std::move(shared_finish->done));
        };
        offline_timer.expires_after(opts.offline_timeout);
        offline_timer.async_wait([finish](const boost::system::error_code&) { finish(); });
        client.offline(node_id, "shutdown", [finish, id = node_id](Result<cv1::NodeInfo> r) {
            if (r) {
                log_info("control: reported offline ({})", id);
            } else {
                log_warn("control: offline report failed: {}", describe(r.error()));
            }
            finish();
        });
    }

    // ---- register ------------------------------------------------------------

    void send_register() {
        if (stopped || reg_in_flight) return;
        reg_timer.cancel();
        reg_in_flight = true;
        const auto gen = ++reg_gen;
        client.register_node(opts.registration, [w = weak(), gen](Result<cv1::RegisterReply> r) {
            auto self = w.lock();
            if (!self || self->stopped || gen != self->reg_gen) return;
            self->reg_in_flight = false;
            self->on_register(std::move(r));
        });
    }

    void on_register(Result<cv1::RegisterReply> r) {
        if (!r) {
            last_error = describe(r.error());
            const auto delay = next_backoff();
            log_warn("control: registration failed: {} (retry in {} ms)", last_error, delay.count());
            reg_timer.expires_after(delay);
            reg_timer.async_wait([w = weak()](const boost::system::error_code& ec) {
                if (ec) return;
                if (auto self = w.lock(); self && !self->stopped) self->send_register();
            });
            return;
        }
        const bool again = !node_id.empty();
        node_id = r->node_id();
        if (again) {
            log_info("control: reconnected to control plane (node {})", node_id);
        } else {
            log_info("control: registered as {}", node_id);
        }
        if (r->has_policy()) maybe_apply(r->policy());
        state = ControlState::kConnected;
        last_error.clear();
        backoff = 0ms;
        set_interval(r->heartbeat_interval_seconds() ? r->heartbeat_interval_seconds() : 10);
        schedule_heartbeat(interval);
        refresh_peers_now();
    }

    // ---- heartbeat -------------------------------------------------------------

    void schedule_heartbeat(std::chrono::milliseconds delay) {
        hb_timer.expires_after(delay);
        hb_timer.async_wait([w = weak()](const boost::system::error_code& ec) {
            if (ec) return;
            if (auto self = w.lock(); self && !self->stopped) self->send_heartbeat();
        });
    }

    void send_heartbeat() {
        if (stopped || hb_in_flight || node_id.empty() || state == ControlState::kRegistering) return;
        cv1::Heartbeat hb;
        if (hooks.fill_stats) hooks.fill_stats(hb);
        hb.set_node_id(node_id);
        hb.set_sent_unix_ms(unix_ms());
        hb.set_daemon_version(opts.registration.daemon_version());
        hb.set_policy_version(applied ? applied->version() : 0);
        *hb.mutable_addresses() = opts.registration.addresses();
        hb_in_flight = true;
        const auto gen = ++hb_gen;
        client.heartbeat(hb, [w = weak(), gen](Result<cv1::HeartbeatReply> r) {
            auto self = w.lock();
            if (!self || self->stopped || gen != self->hb_gen) return;
            self->hb_in_flight = false;
            self->on_heartbeat(std::move(r));
        });
    }

    void on_heartbeat(Result<cv1::HeartbeatReply> r) {
        if (r) {
            last_hb_ok_ms = unix_ms();
            if (r->has_policy()) maybe_apply(r->policy());
            if (state == ControlState::kDisconnected) log_info("control: reconnected to control plane");
            state = ControlState::kConnected;
            last_error.clear();
            backoff = 0ms;
            set_interval(r->heartbeat_interval_seconds());
            schedule_heartbeat(interval);
            return;
        }
        last_error = describe(r.error());
        if (r.error().http_status == 404) {
            log_warn("control: control plane does not know node {}; registering again", node_id);
            state = ControlState::kRegistering;
            backoff = 0ms;
            hb_timer.cancel();
            send_register();
            return;
        }
        const auto delay = std::max(interval, next_backoff());
        if (state != ControlState::kDisconnected) {
            log_warn("control: heartbeat failed: {}; disconnected, keeping current policy", last_error);
        } else {
            log_debug("control: heartbeat failed: {} (retry in {} ms)", last_error, delay.count());
        }
        state = ControlState::kDisconnected;
        schedule_heartbeat(delay);
    }

    // ---- peers -------------------------------------------------------------------

    void refresh_peers_now() {
        peers_timer.cancel();
        send_peers();
    }

    void send_peers() {
        if (stopped || peers_in_flight) return;
        peers_in_flight = true;
        const auto gen = ++peers_gen;
        client.list_nodes([w = weak(), gen](Result<cv1::NodeList> r) {
            auto self = w.lock();
            if (!self || self->stopped || gen != self->peers_gen) return;
            self->peers_in_flight = false;
            self->on_peers(std::move(r));
        });
    }

    void on_peers(Result<cv1::NodeList> r) {
        if (r) {
            peers = std::move(*r);
            peers_ms = unix_ms();
        } else {
            log_debug("control: peer refresh failed: {}", describe(r.error()));
        }
        peers_timer.expires_after(opts.peer_refresh);
        peers_timer.async_wait([w = weak()](const boost::system::error_code& ec) {
            if (ec) return;
            if (auto self = w.lock(); self && !self->stopped) self->send_peers();
        });
    }
};

NodeAgent::NodeAgent(asio::io_context& io, IControlClient& client, AgentOptions options, AgentHooks hooks)
    : impl_(std::make_shared<Impl>(io, client, std::move(options), std::move(hooks))) {}

NodeAgent::~NodeAgent() {
    if (!impl_) return;
    impl_->stopped = true;
    impl_->cancel_timers();
}

void NodeAgent::start() { impl_->start(); }
void NodeAgent::stop(std::function<void()> done) { impl_->stop(std::move(done)); }

ControlState NodeAgent::state() const { return impl_->state; }
const std::string& NodeAgent::node_id() const { return impl_->node_id; }
std::optional<cv1::ResourcePolicy> NodeAgent::policy() const { return impl_->applied; }
std::int64_t NodeAgent::last_heartbeat_ok_ms() const { return impl_->last_hb_ok_ms; }
const std::string& NodeAgent::last_error() const { return impl_->last_error; }
const cv1::NodeList& NodeAgent::peers() const { return impl_->peers; }
std::int64_t NodeAgent::peers_refreshed_ms() const { return impl_->peers_ms; }

}  // namespace tdr::agent
