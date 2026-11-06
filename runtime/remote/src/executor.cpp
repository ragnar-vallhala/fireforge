// RemoteExecutor (docs/design/phase4.md §5). Everything runs on the io_context
// thread.
//
// Lifetime: asynchronous work (client callbacks, timers, posted handlers)
// captures a weak_ptr to Impl, so anything arriving after stop() or the
// destructor is a no-op. Each request kind (lease, status update, full sync,
// incremental announce) has at most one request in flight; a generation counter
// per kind discards superseded replies.
//
// Outbox: one FIFO for all jobs, sent strictly one update at a time, so the
// updates of a job always go out in order and a later one is never sent before
// the earlier one is acknowledged.
//
// State file (RemoteOptions::state_file, replaced atomically: tmp + fsync +
// rename), version 1:
//
//   {
//     "version": 1,
//     "leases": [                      // mapping, keyed by local job id
//       {"local_job_id": "job-x.a2", "job_id": "job-x", "attempt_id": "att-...",
//        "fencing_token": 2, "attempt_number": 2,
//        "terminal": false}            // true once the outcome update is queued
//     ],
//     "outbox": [                      // pending status updates, oldest first
//       {"job_id": "job-x", "local_job_id": "job-x.a2", "terminal": false,
//        "update": { ...JobUpdate as proto3 JSON (proto field names)... }}
//     ]
//   }
//
// A mapping entry is removed once its terminal update is acknowledged (2xx),
// or dropped (409 / 404 / other 4xx). Missing file = empty state; an
// unreadable file is renamed to <file>.corrupt and the executor starts empty.
#include "tdr/remote/executor.hpp"

#include <fcntl.h>
#include <unistd.h>

#include <google/protobuf/util/json_util.h>

#include <algorithm>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/json.hpp>
#include <cerrno>
#include <cstring>
#include <deque>
#include <format>
#include <fstream>
#include <map>
#include <sstream>
#include <system_error>

#include "tdr/common/common.hpp"

namespace tdr::remote {

namespace asio = boost::asio;
namespace json = boost::json;
namespace pbu = google::protobuf::util;
using namespace std::chrono_literals;

std::string local_job_id(const std::string& central_job_id, std::uint32_t attempt_number) {
    return std::format("{}.a{}", central_job_id, attempt_number);
}

namespace {

std::string describe(const agent::ClientError& e) {
    return e.http_status == 0 ? e.message : std::format("HTTP {}: {}", e.http_status, e.message);
}

bool is_transient(const agent::ClientError& e) { return e.http_status == 0 || e.http_status >= 500; }

// A local submit failure caused by this node (capacity, id clash, journal) is
// retryable elsewhere; anything about the spec itself is not.
bool submit_error_retryable(const std::string& err) {
    return err.find("capacity") != std::string::npos || err.find("already exists") != std::string::npos ||
           err.starts_with("journal");
}

std::chrono::milliseconds next_backoff(std::chrono::milliseconds cur, std::chrono::milliseconds min,
                                       std::chrono::milliseconds max) {
    if (cur.count() <= 0) return std::min(min, max);
    return std::min(cur * 2, max);
}

bool write_atomic(const fs::path& path, const std::string& data) {
    std::error_code ec;
    if (path.has_parent_path()) fs::create_directories(path.parent_path(), ec);
    const fs::path tmp = path.string() + ".tmp";
    int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    if (fd < 0) {
        log_error("remote: cannot write {}: {}", tmp.string(), std::strerror(errno));
        return false;
    }
    const char* p = data.data();
    std::size_t left = data.size();
    bool ok = true;
    while (left > 0) {
        ssize_t n = ::write(fd, p, left);
        if (n < 0) {
            if (errno == EINTR) continue;
            ok = false;
            break;
        }
        p += n;
        left -= static_cast<std::size_t>(n);
    }
    if (ok && ::fsync(fd) != 0) ok = false;
    if (::close(fd) != 0) ok = false;
    if (ok) {
        fs::rename(tmp, path, ec);
        ok = !ec;
    }
    if (!ok) {
        log_error("remote: cannot write state file {}", path.string());
        fs::remove(tmp, ec);
    }
    return ok;
}

std::string str_field(const json::object& o, std::string_view key) {
    const auto* v = o.if_contains(key);
    if (!v || !v->is_string()) throw std::runtime_error(std::format("missing string field '{}'", key));
    return std::string(v->as_string());
}

std::uint64_t uint_field(const json::object& o, std::string_view key) {
    const auto* v = o.if_contains(key);
    if (!v) throw std::runtime_error(std::format("missing field '{}'", key));
    return json::value_to<std::uint64_t>(*v);
}

bool bool_field(const json::object& o, std::string_view key) {
    const auto* v = o.if_contains(key);
    return v && v->is_bool() && v->as_bool();
}

}  // namespace

struct RemoteExecutor::Impl : std::enable_shared_from_this<Impl> {
    struct Lease {
        std::string job_id;  // central
        std::string attempt_id;
        std::uint64_t token = 0;
        std::uint32_t attempt_number = 0;
        bool terminal = false;  // outcome update queued
    };
    struct Pending {
        std::string job_id;
        std::string local_id;
        bool terminal = false;
        cv1::JobUpdate update;
    };

    asio::io_context& io;
    agent::IControlClient& client;
    jobs::JobManager& manager;
    RemoteOptions opts;
    RemoteHooks hooks;

    bool started = false, stopped = false;
    std::string node_id;  // "" until on_registered

    std::map<std::string, Lease> leases;  // by local job id
    std::deque<Pending> outbox;

    std::uint64_t leases_received = 0, stale_updates = 0;
    std::string last_error;

    // pull loop
    asio::steady_timer poll_timer{io};
    bool lease_in_flight = false, poll_again = false;
    std::uint64_t lease_gen = 0;

    // outbox
    asio::steady_timer outbox_timer{io};
    bool update_in_flight = false;
    std::uint64_t update_gen = 0;
    std::chrono::milliseconds outbox_backoff{0};

    // full artifact sync
    asio::steady_timer sync_timer{io};
    std::uint64_t sync_gen = 0;
    std::chrono::milliseconds sync_backoff{0};

    // incremental announcements (memory only: a restart does a full sync)
    asio::steady_timer announce_timer{io};
    std::vector<cv1::ArtifactRef> to_announce;
    bool announce_in_flight = false;
    std::uint64_t announce_gen = 0;
    std::chrono::milliseconds announce_backoff{0};

    Impl(asio::io_context& io_, agent::IControlClient& c, jobs::JobManager& m, RemoteOptions o, RemoteHooks h)
        : io(io_), client(c), manager(m), opts(std::move(o)), hooks(std::move(h)) {}

    std::weak_ptr<Impl> weak() { return weak_from_this(); }

    bool connected() const { return hooks.connected ? hooks.connected() : false; }
    bool can_send() const { return started && !stopped && !node_id.empty() && connected(); }

    void cancel_timers() {
        poll_timer.cancel();
        outbox_timer.cancel();
        sync_timer.cancel();
        announce_timer.cancel();
    }

    // ---- persistence -----------------------------------------------------------

    void save() {
        if (opts.state_file.empty()) return;
        json::array ls;
        for (const auto& [local, l] : leases) {
            ls.push_back(json::object{{"local_job_id", local},
                                      {"job_id", l.job_id},
                                      {"attempt_id", l.attempt_id},
                                      {"fencing_token", l.token},
                                      {"attempt_number", l.attempt_number},
                                      {"terminal", l.terminal}});
        }
        json::array ob;
        pbu::JsonPrintOptions po;
        po.preserve_proto_field_names = true;
        for (const auto& p : outbox) {
            std::string u;
            (void)pbu::MessageToJsonString(p.update, &u, po);
            ob.push_back(json::object{{"job_id", p.job_id},
                                      {"local_job_id", p.local_id},
                                      {"terminal", p.terminal},
                                      {"update", json::parse(u)}});
        }
        json::object root{{"version", 1}, {"leases", std::move(ls)}, {"outbox", std::move(ob)}};
        write_atomic(opts.state_file, json::serialize(root) + "\n");
    }

    void load() {
        if (opts.state_file.empty()) return;
        std::ifstream in(opts.state_file);
        if (!in) return;  // missing = empty
        std::stringstream ss;
        ss << in.rdbuf();
        in.close();
        try {
            std::map<std::string, Lease> ls;
            std::deque<Pending> ob;
            const json::value doc = json::parse(ss.str());
            const json::object& root = doc.as_object();
            if (uint_field(root, "version") != 1) throw std::runtime_error("unsupported version");
            for (const auto& v : root.at("leases").as_array()) {
                const auto& o = v.as_object();
                Lease l;
                l.job_id = str_field(o, "job_id");
                l.attempt_id = str_field(o, "attempt_id");
                l.token = uint_field(o, "fencing_token");
                l.attempt_number = static_cast<std::uint32_t>(uint_field(o, "attempt_number"));
                l.terminal = bool_field(o, "terminal");
                ls[str_field(o, "local_job_id")] = std::move(l);
            }
            pbu::JsonParseOptions po;
            po.ignore_unknown_fields = true;
            for (const auto& v : root.at("outbox").as_array()) {
                const auto& o = v.as_object();
                Pending p;
                p.job_id = str_field(o, "job_id");
                p.local_id = str_field(o, "local_job_id");
                p.terminal = bool_field(o, "terminal");
                auto st = pbu::JsonStringToMessage(json::serialize(o.at("update")), &p.update, po);
                if (!st.ok()) throw std::runtime_error("bad update: " + st.ToString());
                ob.push_back(std::move(p));
            }
            leases = std::move(ls);
            outbox = std::move(ob);
        } catch (const std::exception& e) {
            const fs::path bad = opts.state_file.string() + ".corrupt";
            log_error("remote: state file {} is unreadable ({}); moved to {}, starting empty",
                      opts.state_file.string(), e.what(), bad.string());
            std::error_code ec;
            fs::rename(opts.state_file, bad, ec);
            leases.clear();
            outbox.clear();
        }
    }

    // ---- lifecycle ---------------------------------------------------------------

    void start() {
        if (started || stopped) return;
        started = true;
        load();
        // Reconcile with the JobManager (after its recover()).
        for (auto it = leases.begin(); it != leases.end();) {
            const std::string& local = it->first;
            Lease& l = it->second;
            if (l.terminal) {
                const bool queued = std::ranges::any_of(
                    outbox, [&](const Pending& p) { return p.terminal && p.local_id == local; });
                if (!queued) {
                    it = leases.erase(it);  // already reported
                    continue;
                }
            } else if (auto st = manager.status(local); !st) {
                log_warn("remote: local job {} of lease {} is gone; reporting FAILED", local, l.job_id);
                queue_outcome(local, l, tv1::FAILED, "local job lost", /*retryable=*/true, {});
            } else if (jobs::is_terminal(st->state())) {
                queue_terminal_from_status(local, l, *st);
            }
            ++it;
        }
        save();
        if (!leases.empty() || !outbox.empty()) {
            log_info("remote: recovered {} leases, {} pending updates", leases.size(), outbox.size());
        }
    }

    void stop() {
        stopped = true;
        cancel_timers();
    }

    void on_registered(const std::string& id) {
        if (!started || stopped) return;
        node_id = id;
        // Outbox: retry now.
        outbox_timer.cancel();
        outbox_backoff = 0ms;
        send_next_update();
        // Pull loop: (re)start.
        if (opts.enabled) poll_now();
        // Full artifact sync; also flush pending incremental announcements.
        sync_backoff = 0ms;
        start_sync();
        announce_timer.cancel();
        announce_backoff = 0ms;
        send_announce();
    }

    // ---- pull loop -----------------------------------------------------------------

    void schedule_poll(std::chrono::milliseconds delay) {
        poll_timer.expires_after(delay);
        poll_timer.async_wait([w = weak()](const boost::system::error_code& ec) {
            if (ec) return;
            if (auto self = w.lock(); self && !self->stopped) self->do_poll();
        });
    }

    void poll_now() {
        if (stopped || !opts.enabled || node_id.empty()) return;
        if (lease_in_flight) {
            poll_again = true;
            return;
        }
        poll_timer.cancel();
        do_poll();
    }

    void request_poll() {
        asio::post(io, [w = weak()] {
            if (auto self = w.lock(); self && !self->stopped) self->poll_now();
        });
    }

    void do_poll() {
        if (stopped || !opts.enabled || node_id.empty() || lease_in_flight) return;
        if (!connected()) {
            schedule_poll(opts.poll_idle);
            return;
        }
        const auto cap = manager.capacity();
        const auto use = manager.usage();
        const std::uint32_t busy = use.running + use.queued;
        const std::uint32_t slots = cap.jobs > busy ? cap.jobs - busy : 0;
        const std::uint32_t cpu = cap.cpu_cores > use.allocated.cpu_cores ? cap.cpu_cores - use.allocated.cpu_cores : 0;
        const std::uint64_t mem =
            cap.memory_bytes > use.allocated.memory_bytes ? cap.memory_bytes - use.allocated.memory_bytes : 0;
        if (use.queued > 0 || slots == 0 || cpu == 0 || mem == 0) {
            // Busy: a terminal local job triggers a prompt poll; this is the fallback.
            schedule_poll(opts.poll_interval);
            return;
        }
        cv1::LeaseRequest req;
        req.set_node_id(node_id);
        req.mutable_free()->set_cpu_cores(cpu);
        req.mutable_free()->set_memory_bytes(mem);
        req.mutable_free()->set_gpu_count(0);
        req.set_max_jobs(slots);
        lease_in_flight = true;
        poll_again = false;
        const auto gen = ++lease_gen;
        client.lease(req, [w = weak(), gen](agent::Result<cv1::LeaseReply> r) {
            auto self = w.lock();
            if (!self || self->stopped || gen != self->lease_gen) return;
            self->lease_in_flight = false;
            self->on_lease_reply(std::move(r));
        });
    }

    void on_lease_reply(agent::Result<cv1::LeaseReply> r) {
        bool granted = false;
        if (!r) {
            last_error = "lease: " + describe(r.error());
            log_debug("remote: lease request failed: {}", describe(r.error()));
        } else {
            granted = r->leases_size() > 0;
            for (const auto& lease : r->leases()) accept(lease);
        }
        if (poll_again) {
            poll_again = false;
            request_poll();
            return;
        }
        schedule_poll(granted ? opts.poll_interval : opts.poll_idle);
    }

    void accept(const cv1::Lease& lease) {
        ++leases_received;
        const std::string local = local_job_id(lease.job_id(), lease.attempt_number());
        if (leases.contains(local)) {
            log_warn("remote: duplicate lease for {} ignored", local);
            return;
        }
        Lease l;
        l.job_id = lease.job_id();
        l.attempt_id = lease.attempt_id();
        l.token = lease.fencing_token();
        l.attempt_number = lease.attempt_number();
        leases[local] = l;
        save();  // mapping first: a crash after submit must still find it

        tv1::JobSpec spec = lease.spec();
        spec.set_job_id(local);
        spec.mutable_policy()->set_max_attempts(1);  // the control plane owns retries
        log_info("remote: lease {} attempt {} -> local job {}", lease.job_id(), lease.attempt_number(), local);
        auto sub = manager.submit(std::move(spec));  // may call on_job_state re-entrantly
        if (!sub) {
            const bool retryable = submit_error_retryable(sub.error());
            log_warn("remote: lease {} rejected locally ({}): {}", lease.job_id(),
                     retryable ? "retryable" : "not retryable", sub.error());
            auto it = leases.find(local);
            if (it != leases.end() && !it->second.terminal) {
                queue_outcome(local, it->second, tv1::FAILED, sub.error(), retryable, {});
                save();
                send_next_update();
            }
        }
    }

    // ---- job state -> outbox ------------------------------------------------------

    void queue_outcome(const std::string& local, Lease& l, tv1::JobState state, const std::string& error,
                       bool retryable, std::vector<cv1::ArtifactRef> outputs) {
        Pending p;
        p.job_id = l.job_id;
        p.local_id = local;
        p.terminal = jobs::is_terminal(state);
        p.update.set_attempt_id(l.attempt_id);
        p.update.set_fencing_token(l.token);
        p.update.set_state(state);
        if (!error.empty()) p.update.set_error(error);
        if (state == tv1::FAILED) p.update.set_retryable(retryable);
        for (auto& o : outputs) *p.update.add_outputs() = std::move(o);
        if (p.terminal) l.terminal = true;
        outbox.push_back(std::move(p));
    }

    std::vector<cv1::ArtifactRef> outputs_of(const tv1::JobStatus& st) const {
        std::vector<cv1::ArtifactRef> out;
        for (const auto& id : st.output_artifacts()) {
            std::optional<cv1::ArtifactRef> meta;
            if (hooks.artifact_meta) meta = hooks.artifact_meta(id);
            if (!meta) {
                meta.emplace();
                meta->set_artifact_id(id);
            }
            out.push_back(std::move(*meta));
        }
        return out;
    }

    void queue_terminal_from_status(const std::string& local, Lease& l, const tv1::JobStatus& st) {
        switch (st.state()) {
            case tv1::COMPLETED: queue_outcome(local, l, tv1::COMPLETED, {}, false, outputs_of(st)); break;
            case tv1::FAILED: queue_outcome(local, l, tv1::FAILED, st.last_error(), true, {}); break;
            case tv1::CANCELLED: queue_outcome(local, l, tv1::CANCELLED, st.last_error(), false, {}); break;
            case tv1::EXPIRED: queue_outcome(local, l, tv1::EXPIRED, st.last_error(), false, {}); break;
            case tv1::VALIDATION_FAILED:
                queue_outcome(local, l, tv1::VALIDATION_FAILED, st.last_error(), false, {});
                break;
            default: break;
        }
    }

    void on_job_state(const std::string& local, tv1::JobState state) {
        if (stopped) return;
        const bool terminal = jobs::is_terminal(state);
        if (auto it = leases.find(local); started && it != leases.end() && !it->second.terminal) {
            if (state == tv1::RUNNING) {
                queue_outcome(local, it->second, tv1::RUNNING, {}, false, {});
                save();
                send_next_update();
            } else if (terminal) {
                if (auto st = manager.status(local)) {
                    queue_terminal_from_status(local, it->second, *st);
                } else {
                    queue_outcome(local, it->second, state, {}, true, {});
                }
                save();
                send_next_update();
            }
        }
        if (state == tv1::COMPLETED) {
            if (auto st = manager.status(local)) {
                auto outs = outputs_of(*st);
                if (!outs.empty()) {
                    for (auto& o : outs) to_announce.push_back(std::move(o));
                    send_announce();
                }
            }
        }
        if (terminal && started) request_poll();  // capacity freed
    }

    // ---- outbox sender ---------------------------------------------------------------

    void schedule_outbox_retry() {
        outbox_backoff = next_backoff(outbox_backoff, opts.retry_min, opts.retry_max);
        outbox_timer.expires_after(outbox_backoff);
        outbox_timer.async_wait([w = weak()](const boost::system::error_code& ec) {
            if (ec) return;
            if (auto self = w.lock(); self && !self->stopped) self->send_next_update();
        });
    }

    void send_next_update() {
        if (stopped || update_in_flight || outbox.empty() || node_id.empty()) return;
        if (!connected()) {
            // Cheap local check, no request: look again after retry_min.
            if (outbox_timer.expiry() <= asio::steady_timer::clock_type::now()) {
                outbox_timer.expires_after(opts.retry_min);
                outbox_timer.async_wait([w = weak()](const boost::system::error_code& ec) {
                    if (ec) return;
                    if (auto self = w.lock(); self && !self->stopped) self->send_next_update();
                });
            }
            return;
        }
        const Pending& p = outbox.front();
        update_in_flight = true;
        const auto gen = ++update_gen;
        client.update_job(p.job_id, p.update, [w = weak(), gen](agent::Result<cv1::Job> r) {
            auto self = w.lock();
            if (!self || self->stopped || gen != self->update_gen) return;
            self->update_in_flight = false;
            self->on_update_reply(std::move(r));
        });
    }

    void on_update_reply(agent::Result<cv1::Job> r) {
        if (outbox.empty()) return;
        if (!r && is_transient(r.error())) {
            last_error = "status update: " + describe(r.error());
            log_debug("remote: status update for {} failed: {}", outbox.front().job_id, describe(r.error()));
            schedule_outbox_retry();
            return;
        }
        Pending p = std::move(outbox.front());
        outbox.pop_front();
        outbox_backoff = 0ms;
        const std::string sname = tv1::JobState_Name(p.update.state());
        if (!r) {
            const int s = r.error().http_status;
            if (s == 409 || s == 404) {
                ++stale_updates;
                log_warn("remote: {} update for job {} (attempt {}) is stale ({}); dropped", sname, p.job_id,
                         p.update.attempt_id(), describe(r.error()));
            } else {
                last_error = "status update: " + describe(r.error());
                log_error("remote: {} update for job {} rejected ({}); dropped", sname, p.job_id,
                          describe(r.error()));
            }
        } else {
            log_debug("remote: job {} {} reported", p.job_id, sname);
        }
        if (p.terminal) leases.erase(p.local_id);
        save();
        send_next_update();
    }

    // ---- artifact announcements --------------------------------------------------------

    void start_sync() {
        if (stopped || node_id.empty()) return;
        sync_timer.cancel();
        const auto gen = ++sync_gen;
        auto all = std::make_shared<std::vector<cv1::ArtifactRef>>(
            hooks.list_local_artifacts ? hooks.list_local_artifacts() : std::vector<cv1::ArtifactRef>{});
        const std::string sync_id = new_id("sync");
        send_sync_page(gen, sync_id, all, 0);
    }

    void send_sync_page(std::uint64_t gen, const std::string& sync_id,
                        std::shared_ptr<std::vector<cv1::ArtifactRef>> all, std::size_t offset) {
        if (stopped || gen != sync_gen) return;
        if (!connected()) return sync_failed(gen, "not connected");
        const std::size_t page = std::max<std::size_t>(opts.announce_page, 1);
        const std::size_t end = std::min(all->size(), offset + page);
        cv1::ArtifactAnnouncement a;
        a.set_node_id(node_id);
        a.set_sync_id(sync_id);
        for (std::size_t i = offset; i < end; ++i) *a.add_artifacts() = (*all)[i];
        const bool last = end >= all->size();
        a.set_full(last);
        client.announce_artifacts(a, [w = weak(), gen, sync_id, all, end, last](agent::Result<void> r) {
            auto self = w.lock();
            if (!self || self->stopped || gen != self->sync_gen) return;
            if (!r) return self->sync_failed(gen, describe(r.error()));
            if (last) {
                self->sync_backoff = 0ms;
                log_info("remote: announced {} local artifacts (full sync)", all->size());
                return;
            }
            self->send_sync_page(gen, sync_id, all, end);
        });
    }

    void sync_failed(std::uint64_t gen, const std::string& why) {
        last_error = "artifact sync: " + why;
        log_debug("remote: artifact sync failed: {}", why);
        sync_backoff = next_backoff(sync_backoff, opts.retry_min, opts.retry_max);
        sync_timer.expires_after(sync_backoff);
        sync_timer.async_wait([w = weak(), gen](const boost::system::error_code& ec) {
            if (ec) return;
            if (auto self = w.lock(); self && !self->stopped && gen == self->sync_gen) self->start_sync();
        });
    }

    void send_announce() {
        if (stopped || announce_in_flight || to_announce.empty() || node_id.empty()) return;
        if (!connected()) return announce_failed("not connected");
        cv1::ArtifactAnnouncement a;
        a.set_node_id(node_id);
        const std::size_t n = std::min(to_announce.size(), std::max<std::size_t>(opts.announce_page, 1));
        for (std::size_t i = 0; i < n; ++i) *a.add_artifacts() = to_announce[i];
        announce_in_flight = true;
        const auto gen = ++announce_gen;
        client.announce_artifacts(a, [w = weak(), gen, n](agent::Result<void> r) {
            auto self = w.lock();
            if (!self || self->stopped || gen != self->announce_gen) return;
            self->announce_in_flight = false;
            if (!r) return self->announce_failed(describe(r.error()));
            self->announce_backoff = 0ms;
            self->to_announce.erase(self->to_announce.begin(),
                                    self->to_announce.begin() + static_cast<std::ptrdiff_t>(n));
            self->send_announce();
        });
    }

    void announce_failed(const std::string& why) {
        log_debug("remote: artifact announcement failed: {}", why);
        if (announce_timer.expiry() > asio::steady_timer::clock_type::now()) return;  // retry already armed
        announce_backoff = next_backoff(announce_backoff, opts.retry_min, opts.retry_max);
        announce_timer.expires_after(announce_backoff);
        announce_timer.async_wait([w = weak()](const boost::system::error_code& ec) {
            if (ec) return;
            if (auto self = w.lock(); self && !self->stopped) self->send_announce();
        });
    }

    // ---- revocation ---------------------------------------------------------------------

    void on_revoked(const std::vector<cv1::LeaseRef>& revoked) {
        if (!started || stopped) return;
        for (const auto& ref : revoked) {
            std::string local;
            for (const auto& [id, l] : leases) {
                if (!l.terminal && l.job_id == ref.job_id() && l.attempt_id == ref.attempt_id() &&
                    l.token == ref.fencing_token()) {
                    local = id;
                    break;
                }
            }
            if (local.empty()) continue;
            log_info("remote: lease {} (attempt {}) revoked; cancelling local job {}", ref.job_id(),
                     ref.attempt_id(), local);
            if (auto r = manager.cancel(local); !r) log_debug("remote: cancel {}: {}", local, r.error());
        }
    }

    std::vector<cv1::LeaseRef> active_leases() const {
        std::vector<cv1::LeaseRef> out;
        for (const auto& [local, l] : leases) {
            if (l.terminal) continue;
            cv1::LeaseRef r;
            r.set_job_id(l.job_id);
            r.set_attempt_id(l.attempt_id);
            r.set_fencing_token(l.token);
            out.push_back(std::move(r));
        }
        return out;
    }
};

RemoteExecutor::RemoteExecutor(asio::io_context& io, agent::IControlClient& client, jobs::JobManager& manager,
                               RemoteOptions options, RemoteHooks hooks)
    : impl_(std::make_shared<Impl>(io, client, manager, std::move(options), std::move(hooks))) {}

RemoteExecutor::~RemoteExecutor() {
    if (impl_) impl_->stop();
}

void RemoteExecutor::start() { impl_->start(); }
void RemoteExecutor::stop() { impl_->stop(); }
void RemoteExecutor::on_registered(const std::string& node_id) { impl_->on_registered(node_id); }
void RemoteExecutor::on_job_state(const std::string& local_job_id, tv1::JobState state) {
    impl_->on_job_state(local_job_id, state);
}
void RemoteExecutor::on_revoked(const std::vector<cv1::LeaseRef>& revoked) { impl_->on_revoked(revoked); }
std::vector<cv1::LeaseRef> RemoteExecutor::active_leases() const { return impl_->active_leases(); }

std::optional<std::string> RemoteExecutor::central_job_id(const std::string& local_job_id) const {
    auto it = impl_->leases.find(local_job_id);
    if (it == impl_->leases.end()) return std::nullopt;
    return it->second.job_id;
}

RemoteStats RemoteExecutor::stats() const {
    RemoteStats s;
    for (const auto& [_, l] : impl_->leases) {
        if (!l.terminal) ++s.active_leases;
    }
    s.pending_updates = static_cast<std::uint32_t>(impl_->outbox.size());
    s.leases_received = impl_->leases_received;
    s.stale_updates = impl_->stale_updates;
    s.last_error = impl_->last_error;
    return s;
}

}  // namespace tdr::remote
