// In-memory ITransport for PeerManager tests.
//
// FakeNetwork maps "host:port" to a listening FakeTransport. A dial connects
// to the listener at that address (unless the dialer has a script for it),
// creating a pair of FakeLinks. Frames and closes are delivered to the other
// side with asio::post (FIFO on one io thread, so order is preserved).
#pragma once

#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>
#include <chrono>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "tdr/peer/transport.hpp"

namespace tdr::peer::testing {

namespace asio = boost::asio;

struct FakeTransportState {
    TransportHandlers handlers;
};

class FakeLink : public IFrameLink, public std::enable_shared_from_this<FakeLink> {
public:
    FakeLink(asio::io_context& io, std::uint64_t id, bool outbound, std::string remote_name, std::string remote,
             std::weak_ptr<FakeTransportState> owner)
        : io_(io), id_(id), outbound_(outbound), remote_(std::move(remote)), owner_(std::move(owner)) {
        identity_.role = tls::Role::kNode;
        identity_.name = std::move(remote_name);
    }

    std::uint64_t id() const override { return id_; }
    bool outbound() const override { return outbound_; }
    const tls::Identity& identity() const override { return identity_; }
    std::string remote() const override { return remote_; }
    bool is_open() const override { return open_; }

    void send(const protocol::Frame& frame) override {
        if (!open_) return;
        sent.push_back(frame);
        asio::post(io_, [o = other, frame] {
            auto p = o.lock();
            if (p && p->open_) p->deliver(frame);
        });
    }

    void close() override { shutdown("closed locally", "peer closed"); }

    // Abrupt failure seen by both sides.
    void reset() { shutdown("connection reset", "connection reset"); }

    std::weak_ptr<FakeLink> other;
    std::vector<protocol::Frame> sent;

private:
    void shutdown(const std::string& local_reason, const std::string& remote_reason) {
        if (!open_) return;
        open_ = false;
        asio::post(io_, [self = shared_from_this(), local_reason] { self->notify_close(local_reason); });
        asio::post(io_, [o = other, remote_reason] {
            auto p = o.lock();
            if (p && p->open_) {
                p->open_ = false;
                p->notify_close(remote_reason);
            }
        });
    }

    void deliver(const protocol::Frame& f) {
        auto s = owner_.lock();
        if (s && s->handlers.on_frame) s->handlers.on_frame(shared_from_this(), f);
    }

    void notify_close(const std::string& reason) {
        if (close_notified_) return;
        close_notified_ = true;
        auto s = owner_.lock();
        if (s && s->handlers.on_close) s->handlers.on_close(shared_from_this(), reason);
    }

    asio::io_context& io_;
    std::uint64_t id_;
    bool outbound_;
    tls::Identity identity_;
    std::string remote_;
    std::weak_ptr<FakeTransportState> owner_;
    bool open_ = true;
    bool close_notified_ = false;
};

class FakeTransport;

class FakeNetwork {
public:
    explicit FakeNetwork(asio::io_context& io_) : io(io_) {}
    void listen(const std::string& addr, FakeTransport* t) { listeners[addr] = t; }
    void unlisten(const std::string& addr) { listeners.erase(addr); }

    asio::io_context& io;
    std::map<std::string, FakeTransport*> listeners;
    std::uint64_t next_link_id = 1;
};

struct DialScript {
    enum Kind { kFail, kHang } kind = kFail;
    std::string error = "connection refused";
};

class FakeTransport : public ITransport {
public:
    FakeTransport(FakeNetwork& net, std::string name, std::string self_addr = "")
        : net_(net), name_(std::move(name)), self_addr_(std::move(self_addr)),
          state_(std::make_shared<FakeTransportState>()) {}

    void set_handlers(TransportHandlers handlers) override { state_->handlers = std::move(handlers); }

    void dial(const std::string& host, std::uint16_t port, const std::string& expected_name,
              std::function<void(DialResult)> cb) override {
        std::string key = host + ":" + std::to_string(port);
        dials.push_back(key);
        auto& io = net_.io;
        if (auto it = scripts.find(key); it != scripts.end()) {
            if (it->second.kind == DialScript::kHang) {
                hung.push_back(std::move(cb));
                return;
            }
            asio::post(io, [cb = std::move(cb), err = it->second.error] { cb(std::unexpected(err)); });
            return;
        }
        auto lit = net_.listeners.find(key);
        if (lit == net_.listeners.end()) {
            asio::post(io, [cb = std::move(cb)] { cb(std::unexpected(std::string("connection refused"))); });
            return;
        }
        FakeTransport* target = lit->second;
        if (target->name_ != expected_name) {
            asio::post(io, [cb = std::move(cb), n = target->name_, expected_name] {
                cb(std::unexpected("identity mismatch: expected " + expected_name + ", got " + n));
            });
            return;
        }
        auto mine = std::make_shared<FakeLink>(io, net_.next_link_id++, true, target->name_, key, state_);
        std::string from = self_addr_.empty() ? "fake:" + std::to_string(net_.next_link_id) : self_addr_;
        auto theirs =
            std::make_shared<FakeLink>(io, net_.next_link_id++, false, name_, from, target->state_);
        mine->other = theirs;
        theirs->other = mine;
        links.push_back(mine);
        target->links.push_back(theirs);
        auto complete = [target_state = std::weak_ptr<FakeTransportState>(target->state_), mine, theirs,
                         cb = std::move(cb)] {
            auto ts = target_state.lock();
            if (ts && ts->handlers.on_accept) ts->handlers.on_accept(theirs);
            cb(mine);
        };
        if (dial_delay.count() > 0) {
            auto timer = std::make_shared<asio::steady_timer>(io, dial_delay);
            timer->async_wait([timer, complete = std::move(complete)](const boost::system::error_code&) mutable {
                complete();
            });
        } else {
            asio::post(io, std::move(complete));
        }
    }

    std::size_t open_links() const {
        std::size_t n = 0;
        for (const auto& l : links)
            if (l->is_open()) ++n;
        return n;
    }

    const std::string& name() const { return name_; }

    std::map<std::string, DialScript> scripts;
    std::chrono::milliseconds dial_delay{0};
    std::vector<std::string> dials;
    std::vector<std::function<void(DialResult)>> hung;
    std::vector<std::shared_ptr<FakeLink>> links;  // every link of this side

private:
    FakeNetwork& net_;
    std::string name_;
    std::string self_addr_;
    std::shared_ptr<FakeTransportState> state_;
};

}  // namespace tdr::peer::testing
