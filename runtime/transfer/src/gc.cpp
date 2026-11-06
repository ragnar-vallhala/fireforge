// StoreGc (docs/design/phase5.md §6). Everything runs on the io_context thread.
//
// A pass: scan <store>/sha256 (size + mtime of every object), and when above
// high_pct% of max_bytes (or forced) walk the files oldest mtime first,
// releasing them one at a time through the control plane until the store is
// at or below low_pct%. Only a released artifact's file is deleted. Protection
// (is_protected, min_age) is checked when a candidate is reached and again
// before deleting a released file (a job may have started using it while the
// release was in flight; the file is then kept and re-announced by the next
// full sync).
//
// Lifetime: async work captures a weak_ptr to Impl, so a release reply after
// the destructor is a no-op. After stop() the reply of the release in flight is
// still applied, then the pass ends and its report is delivered.
#include "tdr/transfer/gc.hpp"

#include <fcntl.h>
#include <sys/stat.h>

#include <algorithm>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>
#include <cerrno>
#include <cstring>
#include <system_error>

#include "tdr/common/common.hpp"

namespace tdr::transfer {

namespace asio = boost::asio;
namespace fs = std::filesystem;
namespace tv1 = ::fireforge::tdr::v1;
namespace cv1 = ::fireforge::control::v1;

namespace {

bool is_hex64(const std::string& s) {
    return s.size() == 64 && std::ranges::all_of(s, [](char c) { return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'); });
}

}  // namespace

struct StoreGc::Impl : std::enable_shared_from_this<Impl> {
    struct Entry {
        std::string id;
        fs::path path;
        std::uint64_t size = 0;
        fs::file_time_type mtime;
    };
    using ReportCb = std::function<void(const tv1::GcReport&)>;

    asio::io_context& io;
    artifacts::IArtifactStore& store;
    GcOptions opts;
    GcHooks hooks;
    asio::steady_timer timer{io};
    bool started = false, stopped = false;
    std::uint64_t bytes = 0;  // cached store size

    // current pass
    bool running = false;
    std::vector<ReportCb> waiters;
    std::vector<Entry> candidates;
    std::size_t next_idx = 0;
    std::uint64_t low_bytes = 0;
    tv1::GcReport report;

    Impl(asio::io_context& io_, artifacts::IArtifactStore& s, GcOptions o, GcHooks h)
        : io(io_), store(s), opts(std::move(o)), hooks(std::move(h)) {
        scan(nullptr);
    }

    std::weak_ptr<Impl> weak() { return weak_from_this(); }

    // Total size of <store>/sha256; fills `out` (if given) with every object.
    void scan(std::vector<Entry>* out) {
        std::uint64_t total = 0;
        std::error_code ec;
        const fs::path dir = store.root() / "sha256";
        for (fs::recursive_directory_iterator it(dir, fs::directory_options::skip_permission_denied, ec), end;
             !ec && it != end; it.increment(ec)) {
            std::error_code fec;
            if (!it->is_regular_file(fec)) continue;
            const std::uint64_t size = it->file_size(fec);
            if (fec) continue;
            total += size;
            if (!out) continue;
            const std::string name = it->path().filename().string();
            if (!is_hex64(name)) continue;
            auto mtime = it->last_write_time(fec);
            if (fec) continue;
            out->push_back(Entry{artifacts::make_artifact_id(name), it->path(), size, mtime});
        }
        if (ec && ec != std::errc::no_such_file_or_directory) {
            log_warn("gc: scanning {}: {}", dir.string(), ec.message());
        }
        bytes = total;
    }

    bool young(const Entry& e) const {
        if (opts.min_age.count() <= 0) return false;
        const auto now = fs::file_time_type::clock::now();
        return now - e.mtime < opts.min_age;
    }

    bool is_protected(const Entry& e) const {
        return young(e) || (hooks.is_protected && hooks.is_protected(e.id));
    }

    void start() {
        if (started || stopped) return;
        started = true;
        arm();
    }

    void arm() {
        if (stopped || opts.interval.count() <= 0) return;
        timer.expires_after(opts.interval);
        timer.async_wait([w = weak()](const boost::system::error_code& ec) {
            if (ec) return;
            auto self = w.lock();
            if (!self || self->stopped) return;
            self->run([](const tv1::GcReport&) {}, false);
            self->arm();
        });
    }

    void stop() {
        stopped = true;
        timer.cancel();
    }

    void run(ReportCb cb, bool force) {
        if (cb) waiters.push_back(std::move(cb));
        if (running) return;  // coalesced: delivered when the running pass ends
        running = true;
        report.Clear();
        candidates.clear();
        next_idx = 0;
        if (stopped) {
            report.set_store_bytes(bytes);
            report.set_max_bytes(opts.max_bytes);
            return finish();
        }
        std::vector<Entry> all;
        scan(&all);
        report.set_max_bytes(opts.max_bytes);
        if (opts.max_bytes == 0) return finish();
        const std::uint64_t high = opts.max_bytes / 100 * opts.high_pct + opts.max_bytes % 100 * opts.high_pct / 100;
        low_bytes = opts.max_bytes / 100 * opts.low_pct + opts.max_bytes % 100 * opts.low_pct / 100;
        if (!force && bytes <= high) return finish();
        if (bytes <= low_bytes) return finish();
        std::ranges::sort(all, [](const Entry& a, const Entry& b) {
            if (a.mtime != b.mtime) return a.mtime < b.mtime;
            return a.id < b.id;
        });
        candidates = std::move(all);
        log_info("gc: store {} bytes over {}% of {} bytes; evicting toward {}%", bytes,
                 force ? opts.low_pct : opts.high_pct, opts.max_bytes, opts.low_pct);
        next();
    }

    void next() {
        while (!stopped && bytes > low_bytes && next_idx < candidates.size()) {
            const Entry& e = candidates[next_idx++];
            if (is_protected(e)) {
                report.set_kept_protected(report.kept_protected() + 1);
                continue;
            }
            if (!hooks.release) {
                report.set_kept_last_copy(report.kept_last_copy() + 1);
                continue;
            }
            const std::size_t idx = next_idx - 1;
            hooks.release(e.id, [w = weak(), idx](std::expected<cv1::ReleaseReply, std::string> r) {
                auto self = w.lock();
                if (!self) return;
                self->on_release(idx, std::move(r));
            });
            return;  // one release at a time
        }
        finish();
    }

    void on_release(std::size_t idx, std::expected<cv1::ReleaseReply, std::string> r) {
        if (!running || idx >= candidates.size()) return;
        const Entry& e = candidates[idx];
        if (!r) {
            log_debug("gc: release {}: {}", e.id, r.error());
            report.set_kept_last_copy(report.kept_last_copy() + 1);
        } else if (!r->released()) {
            if (r->reason() == "pinned") {
                report.set_kept_pinned(report.kept_pinned() + 1);
            } else {
                report.set_kept_last_copy(report.kept_last_copy() + 1);
            }
        } else if (hooks.is_protected && hooks.is_protected(e.id)) {
            // Became in use while the release was in flight: keep the file.
            log_info("gc: {} released but now in use; kept", e.id);
            report.set_kept_protected(report.kept_protected() + 1);
        } else {
            std::error_code ec;
            if (fs::remove(e.path, ec) && !ec) {
                bytes = bytes > e.size ? bytes - e.size : 0;
                report.add_evicted(e.id);
                report.set_freed_bytes(report.freed_bytes() + e.size);
                log_debug("gc: evicted {} ({} bytes)", e.id, e.size);
            } else if (ec) {
                log_warn("gc: cannot delete {}: {}", e.path.string(), ec.message());
            }
        }
        // Continue on a fresh stack (a release hook may answer synchronously).
        asio::post(io, [w = weak()] {
            if (auto self = w.lock(); self && self->running) self->next();
        });
    }

    void finish() {
        report.set_store_bytes(bytes);
        if (report.evicted_size() > 0 || report.kept_last_copy() + report.kept_pinned() > 0) {
            log_info("gc: evicted {} ({} bytes freed), kept {} last copies, {} pinned, {} in use; store {} bytes",
                     report.evicted_size(), report.freed_bytes(), report.kept_last_copy(), report.kept_pinned(),
                     report.kept_protected(), bytes);
        }
        running = false;
        candidates.clear();
        auto cbs = std::move(waiters);
        waiters.clear();
        if (cbs.empty()) return;
        asio::post(io, [cbs = std::move(cbs), rep = report]() {
            for (const auto& cb : cbs) cb(rep);
        });
    }

    void touch(const std::string& id) {
        auto p = store.path_of(id);
        if (!p) return;
        struct timespec ts[2];
        ts[0].tv_sec = 0;
        ts[0].tv_nsec = UTIME_NOW;
        ts[1] = ts[0];
        if (::utimensat(AT_FDCWD, p->c_str(), ts, 0) != 0 && errno != ENOENT) {
            log_debug("gc: touch {}: {}", p->string(), std::strerror(errno));
        }
    }
};

StoreGc::StoreGc(asio::io_context& io, artifacts::IArtifactStore& store, GcOptions options, GcHooks hooks)
    : impl_(std::make_shared<Impl>(io, store, std::move(options), std::move(hooks))) {}

StoreGc::~StoreGc() {
    if (impl_) impl_->stop();
}

void StoreGc::start() { impl_->start(); }
void StoreGc::stop() { impl_->stop(); }

void StoreGc::run(std::function<void(const tv1::GcReport&)> cb, bool force_to_low) {
    impl_->run(std::move(cb), force_to_low);
}

void StoreGc::touch(const std::string& artifact_id) { impl_->touch(artifact_id); }
std::uint64_t StoreGc::store_bytes() const { return impl_->bytes; }
void StoreGc::note_added(std::uint64_t bytes) { impl_->bytes += bytes; }

bool StoreGc::has_room(std::uint64_t bytes) const {
    if (impl_->opts.max_bytes == 0) return true;
    return impl_->bytes + bytes <= impl_->opts.max_bytes;
}

const GcOptions& StoreGc::options() const { return impl_->opts; }

}  // namespace tdr::transfer
