// ArtifactService (docs/design/phase5.md §2-§3).
//
// Everything runs on the io_context thread. Async work (timers, peer
// callbacks, holder lookups) captures a weak_ptr to Impl and is ignored once
// the service is gone. Data-link handlers also carry a generation number:
// a link the service has dropped is "stale" and its late frames/close are
// ignored.
//
// Server: one Serve per adopted data link. Errors (bad request, not found,
// busy) are sent and the link refused (the PeerManager closes it after
// flushing). The stream keeps <= window chunks unacknowledged. With a rate
// cap, every chunk books its slot on one node-wide schedule (next_free) before
// it is sent, so the total serving rate is <= max_serve_bytes_per_sec.
//
// Fetcher: one Fetch per artifact id (waiters share it), at most max_fetching
// active, the rest FIFO-queued. Each holder attempt is a fresh data link.
//
// Behaviour choices beyond the design doc:
// - A request with offset > size gets ERROR BAD_REQUEST. A fetcher whose
//   resume request (offset > 0) is refused with BAD_REQUEST, or offered a size
//   smaller than its .part, discards the .part and retries the same holder
//   once from 0.
// - A local write/ingest failure fails the fetch at once (another holder
//   would not help); "store full" likewise.
// - stats().resumed counts fetches that resumed (an attempt started at
//   offset > 0) at least once.
// - stop() posts the "transfer stopped" callbacks; the destructor drops them
//   (nothing is called after destruction).
// - Serves count against max_serving until their link has closed.
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>
#include <cerrno>
#include <cstring>
#include <deque>
#include <map>
#include <set>
#include <unordered_map>

#include "fireforge/tdr/v1/artifact.pb.h"
#include "fireforge/tdr/v1/local.pb.h"
#include "tdr/common/common.hpp"
#include "tdr/transfer/transfer.hpp"

namespace tdr::transfer {

namespace asio = boost::asio;
namespace v1 = ::fireforge::tdr::v1;
namespace fs = std::filesystem;
using Clock = std::chrono::steady_clock;
using Result = std::expected<artifacts::ArtifactMeta, std::string>;
using Callback = std::function<void(Result)>;
using peer::IFrameLink;

namespace {

constexpr const char* kDefaultMediaType = "application/octet-stream";

std::string errno_str(int e) { return std::strerror(e); }

struct Fd {
    int fd = -1;
    Fd() = default;
    explicit Fd(int f) : fd(f) {}
    Fd(const Fd&) = delete;
    Fd& operator=(const Fd&) = delete;
    Fd(Fd&& o) noexcept : fd(std::exchange(o.fd, -1)) {}
    Fd& operator=(Fd&& o) noexcept {
        if (this != &o) {
            reset();
            fd = std::exchange(o.fd, -1);
        }
        return *this;
    }
    ~Fd() { reset(); }
    void reset() {
        if (fd >= 0) ::close(fd);
        fd = -1;
    }
    explicit operator bool() const { return fd >= 0; }
};

bool pread_all(int fd, char* buf, std::size_t n, std::uint64_t off) {
    while (n > 0) {
        ssize_t r = ::pread(fd, buf, n, static_cast<off_t>(off));
        if (r < 0 && errno == EINTR) continue;
        if (r <= 0) return false;
        buf += r;
        n -= static_cast<std::size_t>(r);
        off += static_cast<std::uint64_t>(r);
    }
    return true;
}

bool pwrite_all(int fd, const char* buf, std::size_t n, std::uint64_t off) {
    while (n > 0) {
        ssize_t r = ::pwrite(fd, buf, n, static_cast<off_t>(off));
        if (r < 0 && errno == EINTR) continue;
        if (r <= 0) return false;
        buf += r;
        n -= static_cast<std::size_t>(r);
        off += static_cast<std::uint64_t>(r);
    }
    return true;
}

template <class Msg>
protocol::Frame frame(v1::MessageType t, const Msg& m, std::uint64_t correlation = 0) {
    return protocol::make_frame(t, m, 0, correlation);
}

protocol::Frame error_frame(v1::ErrorCode code, const std::string& msg, std::uint64_t correlation) {
    v1::Error e;
    e.set_code(code);
    e.set_message(msg);
    return frame(v1::ERROR, e, correlation);
}

}  // namespace

struct ArtifactService::Impl : std::enable_shared_from_this<Impl> {
    Impl(asio::io_context& io_, peer::PeerManager& p, artifacts::IArtifactStore& s, journal::IJournal& j,
         TransferOptions o, TransferHooks h)
        : io(io_), peers(p), store(s), journal(j), opt(std::move(o)), hooks(std::move(h)) {
        if (opt.chunk_bytes == 0) opt.chunk_bytes = 1u << 20;
        if (opt.window == 0) opt.window = 8;
    }

    asio::io_context& io;
    peer::PeerManager& peers;
    artifacts::IArtifactStore& store;
    journal::IJournal& journal;
    TransferOptions opt;
    TransferHooks hooks;
    bool stopped = false;
    bool destroyed = false;
    TransferStats st;

    // ---- server ---------------------------------------------------------------

    struct Serve {
        explicit Serve(asio::io_context& io) : pace_timer(io), idle_timer(io) {}
        std::uint64_t id = 0;
        std::shared_ptr<IFrameLink> link;
        std::string node_id;
        std::string artifact_id;
        Fd fd;
        std::uint64_t size = 0;
        std::uint64_t next = 0;               // next offset to send
        bool sent_last = false;
        std::uint32_t window = 8;
        std::deque<std::uint64_t> unacked;    // end offsets of sent chunks
        bool booked = false;                  // waiting for its rate-limit slot
        std::size_t booked_bytes = 0;
        asio::steady_timer pace_timer;
        asio::steady_timer idle_timer;
        bool closing = false;
    };
    std::map<std::uint64_t, std::unique_ptr<Serve>> serves;
    std::uint64_t next_serve = 1;
    Clock::time_point next_free = Clock::time_point::min();  // rate-limit schedule

    std::optional<peer::DataLinkHandlers> accept(const std::string& node_id, const std::shared_ptr<IFrameLink>& link,
                                                 protocol::Frame first) {
        const auto corr = first.header.message_id;
        auto refuse = [&](v1::ErrorCode code, const std::string& msg) -> std::optional<peer::DataLinkHandlers> {
            log_debug("artifact serve to {}: refused: {}", node_id, msg);
            peers.send_data(link, error_frame(code, msg, corr));
            return std::nullopt;
        };
        if (stopped) return refuse(v1::UNAVAILABLE, "stopped");
        v1::ArtifactRequest req;
        if (!first.parse(req)) return refuse(v1::BAD_REQUEST, "bad request: unparsable");
        if (!artifacts::parse_artifact_id(req.artifact_id()))
            return refuse(v1::BAD_REQUEST, "bad request: invalid artifact id");
        auto path = store.path_of(req.artifact_id());
        if (!path || !store.contains(req.artifact_id())) return refuse(v1::NOT_FOUND, "not found");
        if (serves.size() >= opt.max_serving) return refuse(v1::UNAVAILABLE, "busy");
        Fd fd(::open(path->c_str(), O_RDONLY | O_CLOEXEC));
        if (!fd) return refuse(v1::NOT_FOUND, "not found");
        struct stat sb{};
        if (::fstat(fd.fd, &sb) != 0) return refuse(v1::INTERNAL, "stat failed");
        auto size = static_cast<std::uint64_t>(sb.st_size);
        if (req.offset() > size) return refuse(v1::BAD_REQUEST, "bad request: offset beyond size");

        auto s = std::make_unique<Serve>(io);
        s->id = next_serve++;
        s->link = link;
        s->node_id = node_id;
        s->artifact_id = req.artifact_id();
        s->fd = std::move(fd);
        s->size = size;
        s->next = req.offset();
        s->window = req.window() == 0 ? 8 : req.window();

        v1::ArtifactOffer offer;
        offer.set_artifact_id(req.artifact_id());
        offer.set_size(size);
        offer.set_sha256(*artifacts::parse_artifact_id(req.artifact_id()));
        std::string media;
        if (auto row = journal.get_artifact(req.artifact_id()); row && !row->media_type.empty()) media = row->media_type;
        offer.set_media_type(media.empty() ? kDefaultMediaType : media);
        offer.set_offset(req.offset());
        peers.send_data(link, frame(v1::ARTIFACT_OFFER, offer, corr));
        log_info("serving {} to {} from offset {} ({} bytes)", req.artifact_id(), node_id, req.offset(), size);
        if (hooks.touch) {
            auto fn = hooks.touch;
            fn(req.artifact_id());
        }

        auto sid = s->id;
        serves.emplace(sid, std::move(s));
        arm_serve_idle(*serves.at(sid));
        asio::post(io, [w = weak_from_this(), sid] {
            if (auto self = w.lock())
                if (Serve* sv = self->find_serve(sid)) self->pump(*sv);
        });

        std::weak_ptr<Impl> w = weak_from_this();
        peer::DataLinkHandlers h;
        h.on_frame = [w, sid](const std::shared_ptr<IFrameLink>&, protocol::Frame f) {
            if (auto self = w.lock()) self->on_serve_frame(sid, std::move(f));
        };
        h.on_close = [w, sid](const std::shared_ptr<IFrameLink>&, const std::string& reason) {
            if (auto self = w.lock()) self->on_serve_close(sid, reason);
        };
        return h;
    }

    Serve* find_serve(std::uint64_t id) {
        auto it = serves.find(id);
        return it == serves.end() ? nullptr : it->second.get();
    }

    void arm_serve_idle(Serve& s) {
        s.idle_timer.expires_after(opt.idle_timeout);
        s.idle_timer.async_wait([w = weak_from_this(), sid = s.id](const boost::system::error_code& ec) {
            if (ec) return;
            auto self = w.lock();
            if (!self) return;
            if (Serve* sv = self->find_serve(sid)) {
                log_warn("serving {} to {}: idle timeout", sv->artifact_id, sv->node_id);
                self->close_serve(*sv);
            }
        });
    }

    void close_serve(Serve& s) {
        if (s.closing) return;
        s.closing = true;
        s.pace_timer.cancel();
        s.idle_timer.cancel();
        s.link->close();  // on_serve_close erases it
    }

    void pump(Serve& s) {
        while (!s.closing && !s.booked && !s.sent_last && s.unacked.size() < s.window) {
            std::size_t n = static_cast<std::size_t>(std::min<std::uint64_t>(opt.chunk_bytes, s.size - s.next));
            if (opt.max_serve_bytes_per_sec > 0 && n > 0) {
                auto now = Clock::now();
                auto at = std::max(now, next_free);
                next_free = at + std::chrono::duration_cast<Clock::duration>(std::chrono::duration<double>(
                                     static_cast<double>(n) / static_cast<double>(opt.max_serve_bytes_per_sec)));
                if (at > now) {
                    s.booked = true;
                    s.booked_bytes = n;
                    s.pace_timer.expires_at(at);
                    s.pace_timer.async_wait([w = weak_from_this(), sid = s.id](const boost::system::error_code& ec) {
                        if (ec) return;
                        auto self = w.lock();
                        if (!self) return;
                        Serve* sv = self->find_serve(sid);
                        if (!sv || sv->closing) return;
                        sv->booked = false;
                        if (!self->send_chunk(*sv, sv->booked_bytes)) return;
                        self->pump(*sv);
                    });
                    return;
                }
            }
            if (!send_chunk(s, n)) return;
        }
    }

    // False if the serve was closed.
    bool send_chunk(Serve& s, std::size_t n) {
        v1::ArtifactChunk c;
        c.set_offset(s.next);
        std::string* data = c.mutable_data();
        data->resize(n);
        if (n > 0 && !pread_all(s.fd.fd, data->data(), n, s.next)) {
            log_warn("serving {}: read failed at {}: {}", s.artifact_id, s.next, errno_str(errno));
            peers.send_data(s.link, error_frame(v1::INTERNAL, "read failed", 0));
            close_serve(s);
            return false;
        }
        s.next += n;
        s.sent_last = s.next >= s.size;
        c.set_last(s.sent_last);
        s.unacked.push_back(s.next);
        st.bytes_served += n;
        peers.send_data(s.link, frame(v1::ARTIFACT_TRANSFER, c));
        arm_serve_idle(s);
        return true;
    }

    void on_serve_frame(std::uint64_t sid, protocol::Frame f) {
        Serve* s = find_serve(sid);
        if (!s || s->closing) return;
        if (f.type() != v1::ARTIFACT_CHUNK_ACK) {
            log_debug("serving {}: unexpected message type {:#x}, closing", s->artifact_id, f.header.message_type);
            close_serve(*s);
            return;
        }
        v1::ArtifactChunkAck ack;
        if (!f.parse(ack)) {
            close_serve(*s);
            return;
        }
        while (!s->unacked.empty() && s->unacked.front() <= ack.next_offset()) s->unacked.pop_front();
        arm_serve_idle(*s);
        pump(*s);
    }

    void on_serve_close(std::uint64_t sid, const std::string& reason) {
        auto it = serves.find(sid);
        if (it == serves.end()) return;
        auto s = std::move(it->second);
        serves.erase(it);
        s->pace_timer.cancel();
        s->idle_timer.cancel();
        log_debug("serving {} to {} ended at {}: {}", s->artifact_id, s->node_id, s->next, reason);
    }

    // ---- fetcher --------------------------------------------------------------

    struct Fetch {
        explicit Fetch(asio::io_context& io) : idle_timer(io) {}
        std::string id, hex;
        std::vector<Callback> waiters;
        bool active = false;
        std::vector<std::string> holders;
        std::size_t idx = 0;
        bool restarted = false;             // .part discarded for this holder already
        std::uint64_t gen = 0;              // current attempt
        std::shared_ptr<IFrameLink> link;
        bool offered = false;
        std::uint64_t req_offset = 0;
        std::uint64_t next = 0;             // next expected offset
        std::uint64_t size = 0;
        std::string media_type;
        Fd fd;
        bool resumed = false;
        std::string last_error;
        asio::steady_timer idle_timer;
    };
    std::map<std::string, std::unique_ptr<Fetch>> fetches;
    std::deque<std::string> queue;
    std::uint32_t active = 0;
    std::uint64_t next_gen = 1;

    Fetch* find_fetch(const std::string& id) {
        auto it = fetches.find(id);
        return it == fetches.end() ? nullptr : it->second.get();
    }

    fs::path part_dir() const { return store.root() / "partial"; }
    fs::path part_path(const Fetch& f) const { return part_dir() / (f.hex + ".part"); }

    void post_result(Callback cb, Result r) {
        asio::post(io, [w = weak_from_this(), cb = std::move(cb), r = std::move(r)]() mutable {
            auto self = w.lock();
            if (!self || self->destroyed) return;
            cb(std::move(r));
        });
    }

    std::optional<artifacts::ArtifactMeta> stored_meta(const std::string& id) {
        auto path = store.path_of(id);
        if (!path || !store.contains(id)) return std::nullopt;
        artifacts::ArtifactMeta m;
        m.id = id;
        m.sha256_hex = artifacts::parse_artifact_id(id).value_or("");
        m.local_path = *path;
        if (auto row = journal.get_artifact(id)) {
            m.size = row->size;
            m.media_type = row->media_type;
            m.producer_job = row->producer_job;
            m.created_unix_ms = row->created_unix_ms;
        } else {
            std::error_code ec;
            m.size = fs::file_size(*path, ec);
        }
        if (m.media_type.empty()) m.media_type = kDefaultMediaType;
        return m;
    }

    void fetch(const std::string& id, Callback cb) {
        if (!cb) cb = [](Result) {};
        if (stopped) return post_result(std::move(cb), std::unexpected(std::string("transfer stopped")));
        auto hex = artifacts::parse_artifact_id(id);
        if (!hex) return post_result(std::move(cb), std::unexpected("invalid artifact id: " + id));
        if (Fetch* f = find_fetch(id)) {
            f->waiters.push_back(std::move(cb));
            return;
        }
        if (auto m = stored_meta(id)) return post_result(std::move(cb), *m);
        auto f = std::make_unique<Fetch>(io);
        f->id = id;
        f->hex = *hex;
        f->waiters.push_back(std::move(cb));
        fetches.emplace(id, std::move(f));
        queue.push_back(id);
        asio::post(io, [w = weak_from_this()] {
            if (auto self = w.lock()) self->start_queued();
        });
    }

    void start_queued() {
        while (!stopped && active < opt.max_fetching && !queue.empty()) {
            std::string id = queue.front();
            queue.pop_front();
            Fetch* f = find_fetch(id);
            if (!f) continue;
            f->active = true;
            ++active;
            begin(*f);
        }
    }

    void begin(Fetch& f) {
        if (auto m = stored_meta(f.id)) return finish(f.id, *m);  // arrived meanwhile
        if (!hooks.holders) return finish(f.id, std::unexpected(std::string("no holder")));
        auto fn = hooks.holders;
        auto gen = f.gen = next_gen++;
        fn(f.id, [w = weak_from_this(), id = f.id, gen](std::expected<std::vector<std::string>, std::string> r) {
            auto self = w.lock();
            if (!self) return;
            Fetch* fp = self->find_fetch(id);
            if (!fp || fp->gen != gen || self->stopped) return;
            if (!r) return self->finish(id, std::unexpected("holders: " + r.error()));
            self->got_holders(*fp, std::move(*r));
        });
    }

    void got_holders(Fetch& f, std::vector<std::string> ids) {
        const std::string& self_id = peers.self_node_id();
        std::vector<std::string> list;
        std::set<std::string> seen;
        for (auto& h : ids)
            if (!h.empty() && h != self_id && seen.insert(h).second) list.push_back(h);
        // CONNECTED peers by RTT, then the rest in the given order.
        std::map<std::string, double> rtt;
        for (const auto& r : peers.snapshot())
            if (r.state() == "CONNECTED") rtt[r.node_id()] = r.rtt_ms();
        std::stable_sort(list.begin(), list.end(), [&](const std::string& a, const std::string& b) {
            auto ia = rtt.find(a), ib = rtt.find(b);
            bool ca = ia != rtt.end(), cb = ib != rtt.end();
            if (ca != cb) return ca;
            return ca && ia->second < ib->second;
        });
        if (list.empty()) return finish(f.id, std::unexpected(std::string("no holder")));
        f.holders = std::move(list);
        f.idx = 0;
        f.restarted = false;
        try_holder(f);
    }

    std::string holder_name(const std::string& node_id) const {
        for (const auto& r : peers.snapshot())
            if (r.node_id() == node_id && !r.name().empty()) return r.name();
        return node_id;
    }

    void try_holder(Fetch& f) {
        if (f.idx >= f.holders.size())
            return finish(f.id, std::unexpected(f.last_error.empty() ? std::string("no holder") : f.last_error));
        std::error_code ec;
        fs::create_directories(part_dir(), ec);
        if (ec) return finish(f.id, std::unexpected("create " + part_dir().string() + ": " + ec.message()));
        auto part = part_path(f);
        std::uint64_t offset = 0;
        if (fs::exists(part, ec)) offset = fs::file_size(part, ec);
        if (ec) offset = 0;
        auto gen = f.gen = next_gen++;
        f.req_offset = offset;
        f.offered = false;
        std::weak_ptr<Impl> w = weak_from_this();
        peer::DataLinkHandlers h;
        h.on_frame = [w, id = f.id, gen](const std::shared_ptr<IFrameLink>&, protocol::Frame fr) {
            auto self = w.lock();
            if (!self) return;
            Fetch* fp = self->find_fetch(id);
            if (fp && fp->gen == gen) self->on_fetch_frame(*fp, std::move(fr));
        };
        h.on_close = [w, id = f.id, gen](const std::shared_ptr<IFrameLink>&, const std::string& reason) {
            auto self = w.lock();
            if (!self) return;
            Fetch* fp = self->find_fetch(id);
            if (fp && fp->gen == gen) {
                fp->link.reset();
                self->next_holder(*fp, "link closed: " + reason);
            }
        };
        log_debug("fetch {}: trying {} from offset {}", f.id, f.holders[f.idx], offset);
        peers.dial_data(f.holders[f.idx], std::move(h), [w, id = f.id, gen](peer::DataDialResult r) {
            auto self = w.lock();
            if (!self) {
                if (r) (*r)->close();
                return;
            }
            Fetch* fp = self->find_fetch(id);
            if (!fp || fp->gen != gen || self->stopped) {
                if (r) (*r)->close();
                return;
            }
            if (!r) return self->next_holder(*fp, r.error());
            self->connected(*fp, *r);
        });
    }

    void connected(Fetch& f, const std::shared_ptr<IFrameLink>& link) {
        f.link = link;
        v1::ArtifactRequest req;
        req.set_artifact_id(f.id);
        req.set_offset(f.req_offset);
        req.set_window(opt.window);
        peers.send_data(link, frame(v1::ARTIFACT_REQUEST, req));
        arm_fetch_idle(f);
    }

    void arm_fetch_idle(Fetch& f) {
        f.idle_timer.expires_after(opt.idle_timeout);
        f.idle_timer.async_wait([w = weak_from_this(), id = f.id, gen = f.gen](const boost::system::error_code& ec) {
            if (ec) return;
            auto self = w.lock();
            if (!self) return;
            Fetch* fp = self->find_fetch(id);
            if (fp && fp->gen == gen) self->next_holder(*fp, "idle timeout");
        });
    }

    // Stop using the current attempt (its late frames/close are ignored).
    void drop_attempt(Fetch& f) {
        f.gen = next_gen++;
        f.idle_timer.cancel();
        f.fd.reset();
        f.offered = false;
        if (f.link) {
            f.link->close();
            f.link.reset();
        }
    }

    void next_holder(Fetch& f, const std::string& reason) {
        std::string who = f.idx < f.holders.size() ? holder_name(f.holders[f.idx]) : std::string("?");
        drop_attempt(f);
        f.last_error = std::format("{}: {}", who, reason);
        log_info("fetch {}: {}", f.id, f.last_error);
        ++f.idx;
        f.restarted = false;
        try_holder(f);
    }

    void restart_same_holder(Fetch& f, const std::string& why) {
        log_info("fetch {}: {}; discarding .part and restarting from 0", f.id, why);
        drop_attempt(f);
        std::error_code ec;
        fs::remove(part_path(f), ec);
        f.restarted = true;
        try_holder(f);
    }

    void on_fetch_frame(Fetch& f, protocol::Frame fr) {
        arm_fetch_idle(f);
        switch (fr.type()) {
            case v1::ERROR: {
                v1::Error e;
                (void)fr.parse(e);
                if (e.code() == v1::BAD_REQUEST && f.req_offset > 0 && !f.restarted && !f.offered)
                    return restart_same_holder(f, "resume refused: " + e.message());
                return next_holder(f, e.message().empty() ? v1::ErrorCode_Name(e.code()) : e.message());
            }
            case v1::ARTIFACT_OFFER: {
                if (f.offered) return next_holder(f, "protocol error: second offer");
                v1::ArtifactOffer o;
                if (!fr.parse(o)) return next_holder(f, "protocol error: bad offer");
                if (o.artifact_id() != f.id || o.sha256() != f.hex) return next_holder(f, "offer for another artifact");
                if (o.size() < f.req_offset && !f.restarted)
                    return restart_same_holder(f, ".part larger than the artifact");
                if (o.offset() != f.req_offset || o.offset() > o.size())
                    return next_holder(f, std::format("offer at offset {} (requested {})", o.offset(), f.req_offset));
                if (hooks.has_room) {
                    auto fn = hooks.has_room;
                    if (!fn(o.size() - o.offset())) {
                        drop_attempt(f);
                        return finish(f.id, std::unexpected(std::string("store full")));
                    }
                    if (find_fetch(f.id) != &f || stopped) return;
                }
                f.fd = Fd(::open(part_path(f).c_str(), O_WRONLY | O_CREAT | O_CLOEXEC, 0600));
                if (!f.fd) {
                    std::string err = "open .part: " + errno_str(errno);
                    drop_attempt(f);
                    return finish(f.id, std::unexpected(err));
                }
                if (::ftruncate(f.fd.fd, static_cast<off_t>(o.offset())) != 0) {
                    std::string err = "truncate .part: " + errno_str(errno);
                    drop_attempt(f);
                    return finish(f.id, std::unexpected(err));
                }
                f.offered = true;
                f.size = o.size();
                f.next = o.offset();
                f.media_type = o.media_type().empty() ? kDefaultMediaType : o.media_type();
                if (o.offset() > 0 && !f.resumed) {
                    f.resumed = true;
                    ++st.resumed;
                }
                return;
            }
            case v1::ARTIFACT_TRANSFER: {
                if (!f.offered) return next_holder(f, "protocol error: chunk before offer");
                v1::ArtifactChunk c;
                if (!fr.parse(c)) return next_holder(f, "protocol error: bad chunk");
                if (c.offset() != f.next)
                    return next_holder(f, std::format("out-of-order chunk at {} (expected {})", c.offset(), f.next));
                const auto& data = c.data();
                if (data.size() > f.size - f.next) return next_holder(f, "chunk beyond the offered size");
                if (!data.empty() && !pwrite_all(f.fd.fd, data.data(), data.size(), f.next)) {
                    std::string err = "write .part: " + errno_str(errno);
                    drop_attempt(f);
                    return finish(f.id, std::unexpected(err));
                }
                f.next += data.size();
                st.bytes_received += data.size();
                v1::ArtifactChunkAck ack;
                ack.set_next_offset(f.next);
                peers.send_data(f.link, frame(v1::ARTIFACT_CHUNK_ACK, ack));
                if (c.last()) {
                    if (f.next != f.size) return next_holder(f, "last chunk before the end");
                    return complete(f);
                }
                return;
            }
            default:
                return next_holder(f, std::format("protocol error: message type {:#x}", fr.header.message_type));
        }
    }

    void complete(Fetch& f) {
        int fd = f.fd.fd;
        bool synced = fd >= 0 && ::fsync(fd) == 0;
        int sync_err = errno;
        drop_attempt(f);  // closes the .part fd and the link
        if (!synced) return finish(f.id, std::unexpected("fsync .part: " + errno_str(sync_err)));
        auto part = part_path(f);
        auto h = artifacts::sha256_file(part);
        if (!h || *h != f.hex) {
            std::error_code ec;
            fs::remove(part, ec);
            std::string who = holder_name(f.holders[f.idx]);
            f.last_error = std::format("{}: checksum mismatch", who);
            log_warn("fetch {}: {}; .part discarded", f.id, f.last_error);
            ++f.idx;
            f.restarted = false;
            return try_holder(f);
        }
        auto meta = store.ingest(part, f.media_type, "", true);
        if (!meta) return finish(f.id, std::unexpected("ingest: " + meta.error()));
        journal::ArtifactRow row;
        row.id = meta->id;
        row.sha256_hex = meta->sha256_hex;
        row.size = meta->size;
        row.media_type = meta->media_type;
        row.producer_job = meta->producer_job;
        row.local_path = meta->local_path.string();
        row.created_unix_ms = meta->created_unix_ms;
        try {
            journal.record_artifact(row);
        } catch (const std::exception& e) {
            return finish(f.id, std::unexpected(std::string("journal: ") + e.what()));
        }
        log_info("fetched {} ({} bytes) from {}", f.id, meta->size, holder_name(f.holders[f.idx]));
        if (hooks.on_stored) {
            auto fn = hooks.on_stored;
            fn(*meta);
        }
        finish(f.id, *meta);
    }

    // Remove the fetch and call its waiters (synchronously; we're always in a
    // callback here).
    void finish(const std::string& id, Result r) {
        auto it = fetches.find(id);
        if (it == fetches.end()) return;
        auto f = std::move(it->second);
        fetches.erase(it);
        drop_attempt(*f);
        if (f->active) --active;
        if (r)
            ++st.fetched;
        else {
            ++st.fetch_failures;
            log_warn("fetch {} failed: {}", id, r.error());
        }
        auto keep = shared_from_this();  // a waiter may destroy the service
        for (auto& cb : f->waiters) {
            if (destroyed) break;
            cb(r);
        }
        if (!destroyed && !stopped) start_queued();
    }

    // ---- stop -------------------------------------------------------------------

    void stop() {
        if (stopped) return;
        stopped = true;
        auto all = std::move(fetches);
        fetches.clear();
        queue.clear();
        active = 0;
        for (auto& [id, f] : all) {
            drop_attempt(*f);
            ++st.fetch_failures;
            if (destroyed) continue;
            for (auto& cb : f->waiters) post_result(std::move(cb), std::unexpected(std::string("transfer stopped")));
        }
        std::vector<Serve*> ss;
        for (auto& [_, s] : serves) ss.push_back(s.get());
        for (Serve* s : ss) close_serve(*s);
    }

    bool busy(const std::string& id) const {
        if (fetches.contains(id)) return true;
        for (const auto& [_, s] : serves)
            if (s->artifact_id == id) return true;
        return false;
    }

    TransferStats stats() const {
        TransferStats s = st;
        s.fetching = active;
        s.queued = static_cast<std::uint32_t>(queue.size());
        s.serving = static_cast<std::uint32_t>(serves.size());
        return s;
    }
};

ArtifactService::ArtifactService(asio::io_context& io, peer::PeerManager& peers, artifacts::IArtifactStore& store,
                                 journal::IJournal& journal, TransferOptions options, TransferHooks hooks)
    : impl_(std::make_shared<Impl>(io, peers, store, journal, std::move(options), std::move(hooks))) {
    std::weak_ptr<Impl> w = impl_;
    peers.set_data_acceptor([w](const std::string& node_id, const std::shared_ptr<IFrameLink>& link,
                                protocol::Frame first) -> std::optional<peer::DataLinkHandlers> {
        auto self = w.lock();
        if (!self) return std::nullopt;
        return self->accept(node_id, link, std::move(first));
    });
}

ArtifactService::~ArtifactService() {
    if (!impl_) return;
    impl_->destroyed = true;
    impl_->peers.set_data_acceptor(nullptr);
    impl_->stop();
}

void ArtifactService::fetch(const std::string& artifact_id, Callback cb) { impl_->fetch(artifact_id, std::move(cb)); }

void ArtifactService::stop() { impl_->stop(); }

bool ArtifactService::busy(const std::string& artifact_id) const { return impl_->busy(artifact_id); }

TransferStats ArtifactService::stats() const { return impl_->stats(); }

}  // namespace tdr::transfer
