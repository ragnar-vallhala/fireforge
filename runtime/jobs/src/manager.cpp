// JobManager: FIFO admission, attempt lifecycle, retries, cancellation,
// output commit and crash recovery (docs/design/phase1.md sections 3-5).
//
// Single-threaded. Re-entrancy rules:
//  - Scheduling passes are always posted to the io_context (request_schedule),
//    never run from inside a supervisor callback or a public method.
//  - The state listener fires synchronously right after each journal
//    transition and may call back into the manager (e.g. cancel()); code
//    after a transition re-checks the in-memory attempt flags.
#include "tdr/jobs/jobs.hpp"

#include <algorithm>
#include <cerrno>
#include <compare>
#include <cstring>
#include <map>
#include <set>
#include <system_error>
#include <unordered_map>
#include <utility>

#include <sys/stat.h>
#include <unistd.h>

#include <boost/asio/post.hpp>

#include "tdr/common/common.hpp"

namespace tdr::jobs {

namespace {

using journal::AttemptState;
using supervisor::ExitKind;

constexpr std::string_view kArtifactPrefix = "artifact://sha256/";
constexpr std::string_view kDefaultMediaType = "application/octet-stream";
constexpr std::size_t kMaxJobIdLength = 128;
constexpr std::string_view kDefaultCheckpointName = "checkpoint";

// "artifact://sha256/<64 lowercase hex>" -> hex. Local re-implementation so
// this module does not depend on the artifacts library's free functions.
std::optional<std::string> artifact_hex(std::string_view id) {
    if (!id.starts_with(kArtifactPrefix)) return std::nullopt;
    std::string_view hex = id.substr(kArtifactPrefix.size());
    if (hex.size() != 64) return std::nullopt;
    for (char c : hex) {
        if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) return std::nullopt;
    }
    return std::string(hex);
}

// Job ids become directory names under work_root, so keep them tame.
bool valid_job_id(const std::string& id) {
    if (id.empty() || id.size() > kMaxJobIdLength || id == "." || id == "..") return false;
    return std::ranges::all_of(id, [](char c) {
        return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' ||
               c == '_' || c == '.';
    });
}

// Checkpoint names become file names (checkpoints/resume/<name>).
bool valid_checkpoint_name(const std::string& name) { return valid_job_id(name); }

bool constant_time_equal(std::string_view a, std::string_view b) {
    if (a.size() != b.size()) return false;
    unsigned char diff = 0;
    for (std::size_t i = 0; i < a.size(); ++i) {
        diff |= static_cast<unsigned char>(a[i] ^ b[i]);
    }
    return diff == 0;
}

// Restricted job user (phase1.md section 11): give `path` group `gid` and set
// `mode` explicitly (create_directories is subject to the process umask).
// Returns an error message, or "" on success.
std::string restrict_dir(const fs::path& path, gid_t gid, mode_t mode) {
    if (::chown(path.c_str(), static_cast<uid_t>(-1), gid) != 0) {
        int err = errno;
        return std::format("chown {}: {} (is tdrd in group {}?)", path.string(), std::strerror(err), gid);
    }
    if (::chmod(path.c_str(), mode) != 0) {
        int err = errno;
        return std::format("chmod {:o} {}: {}", static_cast<unsigned>(mode), path.string(), std::strerror(err));
    }
    return {};
}

std::uint32_t max_attempts(const v1::JobSpec& spec) {
    return spec.policy().max_attempts() == 0 ? 1 : spec.policy().max_attempts();
}

const char* exit_kind_name(ExitKind k) {
    switch (k) {
        case ExitKind::kExited: return "exited";
        case ExitKind::kSignaled: return "signaled";
        case ExitKind::kTimedOut: return "timed out";
        case ExitKind::kCancelled: return "cancelled";
        case ExitKind::kOomKilled: return "oom killed";
    }
    return "unknown";
}

std::string describe_exit(const supervisor::ExitInfo& info) {
    // The supervisor's message is already a full human-readable summary.
    if (!info.message.empty()) return info.message;
    std::string s;
    switch (info.kind) {
        case ExitKind::kExited: s = std::format("exited with code {}", info.exit_code); break;
        case ExitKind::kSignaled: s = std::format("killed by signal {}", info.signal); break;
        case ExitKind::kOomKilled: s = "killed: out of memory"; break;
        default: s = exit_kind_name(info.kind); break;
    }
    return s;
}

// True if `p` (canonical) is `base` (canonical) or below it.
bool is_within(const fs::path& base, const fs::path& p) {
    auto [bi, pi] = std::mismatch(base.begin(), base.end(), p.begin(), p.end());
    return bi == base.end();
}

struct QueueKey {
    std::int64_t created_ms = 0;
    std::uint64_t seq = 0;
    auto operator<=>(const QueueKey&) const = default;
};

struct OutputToIngest {
    fs::path path;       // canonical
    std::string media_type;
    std::string name;    // phase6.md §2
};

}  // namespace

struct JobManager::Impl {
    enum class Phase { kStarting, kRunning };

    struct Attempt {
        std::string id;
        std::string job_id;
        std::string token;
        std::uint32_t number = 0;
        fs::path dir;
        v1::JobSpec spec;
        Capacity reserved;
        std::vector<v1::InputFile> inputs;
        fs::path resume_path;  // checkpoints/resume/<name>, "" = no resume
        Phase phase = Phase::kStarting;
        bool cancel_requested = false;
        std::string cancel_reason;
        std::optional<v1::WorkerResult> result;
    };

    struct QueueEntry {
        std::string job_id;
        Capacity req;
    };

    Impl(boost::asio::io_context& io_, journal::IJournal& j, artifacts::IArtifactStore& s,
         supervisor::IProcessSupervisor& sp, ManagerOptions o)
        : io(io_), journal(j), store(s), sup(sp), opts(std::move(o)) {}

    boost::asio::io_context& io;
    journal::IJournal& journal;
    artifacts::IArtifactStore& store;
    supervisor::IProcessSupervisor& sup;
    ManagerOptions opts;

    // Guards posted handlers and exit callbacks against outliving the manager.
    std::shared_ptr<char> alive = std::make_shared<char>(0);

    std::map<QueueKey, QueueEntry> queue;                    // FIFO by creation order
    std::unordered_map<std::string, QueueKey> keys;          // non-terminal job -> queue key
    std::unordered_map<std::string, std::unique_ptr<Attempt>> attempts;  // live attempts
    std::unordered_map<std::string, std::string> job_attempt;            // job -> live attempt
    Capacity allocated;
    std::uint64_t next_seq = 0;
    bool schedule_pending = false;

    std::function<void(const std::string&, const std::string&)> cancel_notifier;
    std::function<void(const std::string&, v1::JobState)> state_listener;
    std::function<void(const std::string&, const v1::NamedArtifact&)> checkpoint_listener;

    // ---- helpers -----------------------------------------------------------

    void notify(const std::string& job_id, v1::JobState s) {
        if (is_terminal(s)) {
            if (auto it = keys.find(job_id); it != keys.end()) {
                queue.erase(it->second);
                keys.erase(it);
            }
        }
        if (state_listener) state_listener(job_id, s);
    }

    // Journal CAS transition + logging + listener.
    bool move(const std::string& job_id, v1::JobState from, v1::JobState to, const std::string& error = {}) {
        if (!is_valid_transition(from, to)) {
            log_error("job {}: invalid transition {} -> {}", job_id, to_string(from), to_string(to));
            return false;
        }
        if (!journal.transition(job_id, from, to, error)) {
            log_warn("job {}: transition {} -> {} lost (state changed)", job_id, to_string(from), to_string(to));
            return false;
        }
        if (is_terminal(to)) {
            log_info("job {}: {} -> {}{}{}", job_id, to_string(from), to_string(to), error.empty() ? "" : ": ",
                     error);
        } else {
            log_debug("job {}: {} -> {}{}{}", job_id, to_string(from), to_string(to), error.empty() ? "" : ": ",
                      error);
        }
        notify(job_id, to);
        return true;
    }

    QueueKey key_for(const std::string& job_id, std::int64_t created_ms) {
        auto [it, inserted] = keys.try_emplace(job_id, QueueKey{created_ms, next_seq});
        if (inserted) ++next_seq;
        return it->second;
    }

    void enqueue(const journal::JobRecord& rec) {
        QueueKey k = key_for(rec.job_id, rec.created_unix_ms);
        queue[k] = QueueEntry{rec.job_id, effective_request(rec.spec, opts.default_job_memory_bytes)};
    }

    void dequeue(const std::string& job_id) {
        if (auto it = keys.find(job_id); it != keys.end()) queue.erase(it->second);
    }

    bool fits(const Capacity& req) const {
        return static_cast<std::uint64_t>(allocated.cpu_cores) + req.cpu_cores <= opts.capacity.cpu_cores &&
               allocated.memory_bytes + req.memory_bytes <= opts.capacity.memory_bytes &&
               allocated.jobs + 1 <= opts.capacity.jobs;
    }

    void reserve(const Capacity& c) {
        allocated.cpu_cores += c.cpu_cores;
        allocated.memory_bytes += c.memory_bytes;
        allocated.jobs += 1;
    }

    void release(const Capacity& c) {
        allocated.cpu_cores -= std::min(allocated.cpu_cores, c.cpu_cores);
        allocated.memory_bytes -= std::min(allocated.memory_bytes, c.memory_bytes);
        allocated.jobs -= std::min<std::uint32_t>(allocated.jobs, 1);
    }

    // Removes the live attempt and releases its reservation. Returns ownership.
    std::unique_ptr<Attempt> retire(const std::string& attempt_id) {
        auto it = attempts.find(attempt_id);
        if (it == attempts.end()) return nullptr;
        std::unique_ptr<Attempt> a = std::move(it->second);
        attempts.erase(it);
        if (auto ja = job_attempt.find(a->job_id); ja != job_attempt.end() && ja->second == attempt_id) {
            job_attempt.erase(ja);
        }
        release(a->reserved);
        return a;
    }

    void request_schedule() {
        if (schedule_pending) return;
        schedule_pending = true;
        boost::asio::post(io, [this, w = std::weak_ptr<char>(alive)] {
            if (w.expired()) return;
            schedule_pending = false;
            schedule_now();
        });
    }

    // Head-of-line FIFO: stop at the first job that does not fit.
    void schedule_now() {
        while (!queue.empty()) {
            auto it = queue.begin();
            if (!fits(it->second.req)) break;
            QueueEntry e = std::move(it->second);
            queue.erase(it);
            start_attempt(e.job_id);
        }
    }

    // Failure of the current attempt: retry if allowed and attempts remain,
    // else FAILED (or EXPIRED for timeouts).
    void fail_or_retry(const std::string& job_id, v1::JobState from, const std::string& error, bool retryable,
                       v1::JobState exhausted_state = v1::FAILED) {
        auto rec = journal.get_job(job_id);
        if (!rec) return;
        bool attempts_left = rec->attempts_used < max_attempts(rec->spec);
        if (retryable && attempts_left) {
            if (!move(job_id, from, v1::RETRY_PENDING, error)) return;
            if (!move(job_id, v1::RETRY_PENDING, v1::QUEUED)) return;
            if (auto again = journal.get_job(job_id); again && again->state == v1::QUEUED) enqueue(*again);
            request_schedule();
        } else {
            move(job_id, from, exhausted_state, error);
        }
    }

    // ---- attempt start (section 4, steps 1-6) -------------------------------

    void start_attempt(const std::string& job_id) {
        auto rec = journal.get_job(job_id);
        if (!rec || rec->state != v1::QUEUED) {
            log_warn("job {}: dropped from queue (no longer QUEUED)", job_id);
            return;
        }

        auto owned = std::make_unique<Attempt>();
        Attempt& a = *owned;
        a.id = new_id("att");
        a.job_id = job_id;
        a.token = new_secret();
        a.number = rec->attempts_used + 1;
        a.dir = opts.work_root / job_id / a.id;
        a.spec = rec->spec;
        a.reserved = effective_request(rec->spec, opts.default_job_memory_bytes);

        // Steps 2-3: work dir + inputs. Errors are reported after the attempt
        // is recorded so they consume an attempt (no infinite requeue loop).
        std::string setup_error;
        try {
            fs::create_directories(a.dir / "inputs");
            fs::create_directories(a.dir / "outputs");
            fs::create_directories(a.dir / "checkpoints");
            std::set<std::string> seen;
            for (int i = 0; i < rec->spec.input_artifacts_size(); ++i) {
                const std::string& id = rec->spec.input_artifacts(i);
                auto hex = artifact_hex(id);
                if (!hex) {
                    setup_error = "invalid input artifact id: " + id;
                    break;
                }
                fs::path dest = a.dir / "inputs" / *hex;
                if (seen.insert(*hex).second) {
                    if (auto r = store.materialize(id, dest); !r) {
                        setup_error = "materialize " + id + ": " + r.error();
                        break;
                    }
                }
                v1::InputFile f;
                f.set_artifact_id(id);
                f.set_path(dest.string());
                if (i < rec->spec.input_names_size()) f.set_name(rec->spec.input_names(i));
                a.inputs.push_back(std::move(f));
            }
            // Resume (phase6.md §6): this job's latest local checkpoint wins
            // over the spec's (control-plane) resume checkpoint.
            if (setup_error.empty()) {
                std::string resume_id = rec->checkpoint.artifact_id();
                std::string resume_name = rec->checkpoint.name();
                if (resume_id.empty()) {
                    resume_id = rec->spec.resume_checkpoint();
                    resume_name.clear();
                }
                if (!valid_checkpoint_name(resume_name)) resume_name = kDefaultCheckpointName;
                if (!resume_id.empty()) {
                    const fs::path dest = a.dir / "checkpoints" / "resume" / resume_name;
                    if (auto r = store.materialize(resume_id, dest); !r) {
                        setup_error = "materialize resume checkpoint " + resume_id + ": " + r.error();
                    } else {
                        a.resume_path = dest;
                    }
                }
            }
            // Restricted job user: ownership and modes are applied after the
            // inputs are materialized, so inputs/ ends up 02750 (the job can
            // read but not add or remove files). The job dir may already exist
            // from an earlier attempt; re-applying is harmless.
            if (opts.run_as && setup_error.empty()) {
                const gid_t gid = opts.run_as->gid;
                std::vector<std::pair<fs::path, mode_t>> dirs = {
                    {a.dir.parent_path(), 02750},  {a.dir, 02770},
                    {a.dir / "outputs", 02770},    {a.dir / "checkpoints", 02770},
                    {a.dir / "inputs", 02750},
                };
                // Like inputs/: readable, but the job cannot add or remove files.
                if (!a.resume_path.empty()) dirs.emplace_back(a.resume_path.parent_path(), 02750);
                for (const auto& [path, mode] : dirs) {
                    setup_error = restrict_dir(path, gid, mode);
                    if (!setup_error.empty()) {
                        setup_error = "work dir setup: " + setup_error;
                        break;
                    }
                }
            }
        } catch (const fs::filesystem_error& e) {
            setup_error = std::string("work dir setup: ") + e.what();
        }

        // Step 4.
        journal::AttemptRecord ar;
        ar.attempt_id = a.id;
        ar.job_id = job_id;
        ar.number = a.number;
        ar.token = a.token;
        ar.state = AttemptState::kStarting;
        ar.work_dir = a.dir.string();
        ar.started_unix_ms = unix_ms();
        journal.begin_attempt(ar);

        reserve(a.reserved);
        const std::string attempt_id = a.id;
        attempts.emplace(attempt_id, std::move(owned));
        job_attempt[job_id] = attempt_id;

        if (!move(job_id, v1::QUEUED, v1::ASSIGNED)) {
            retire(attempt_id);
            journal.finish_attempt(attempt_id, AttemptState::kAbandoned, 0, 0, "job state changed before start");
            return;
        }

        // The listener may have cancelled the job while it was ASSIGNED.
        if (a.cancel_requested) {
            auto gone = retire(attempt_id);
            journal.finish_attempt(attempt_id, AttemptState::kCancelled, 0, 0, gone->cancel_reason);
            move(job_id, v1::ASSIGNED, v1::CANCELLED, gone->cancel_reason);
            request_schedule();
            return;
        }
        if (!setup_error.empty()) {
            launch_failed(attempt_id, setup_error);
            return;
        }

        // Step 5.
        supervisor::LaunchSpec ls;
        ls.attempt_id = attempt_id;
        ls.argv.assign(rec->spec.command().argv().begin(), rec->spec.command().argv().end());
        // Env precedence: HOME (restricted job user only; the job user cannot
        // write the daemon's home) is a default that the job's own env may
        // override. FIREFORGE_* are set last and always win.
        if (opts.run_as) ls.env["HOME"] = a.dir.string();
        for (const auto& [k, v] : rec->spec.command().env()) ls.env[k] = v;
        ls.env["FIREFORGE_IPC_SOCKET"] = opts.worker_socket.string();
        ls.env["FIREFORGE_JOB_ID"] = job_id;
        ls.env["FIREFORGE_ATTEMPT_ID"] = attempt_id;
        ls.env["FIREFORGE_ATTEMPT_TOKEN"] = a.token;
        ls.env["FIREFORGE_WORKDIR"] = a.dir.string();
        ls.env["FIREFORGE_INPUT_DIR"] = (a.dir / "inputs").string();
        ls.env["FIREFORGE_OUTPUT_DIR"] = (a.dir / "outputs").string();
        ls.env["FIREFORGE_CHECKPOINT_DIR"] = (a.dir / "checkpoints").string();
        ls.working_dir = a.dir;
        ls.stdout_path = a.dir / "stdout.log";
        ls.stderr_path = a.dir / "stderr.log";
        ls.limits.cpu_cores = a.reserved.cpu_cores;
        ls.limits.memory_bytes = a.reserved.memory_bytes;
        ls.timeout = std::chrono::seconds(static_cast<std::int64_t>(rec->spec.policy().timeout_seconds()));
        ls.run_as = opts.run_as;

        auto handle = sup.launch(ls, [this, w = std::weak_ptr<char>(alive), attempt_id](
                                         const supervisor::ExitInfo& info) {
            if (w.expired()) return;
            on_exit(attempt_id, info);
        });
        if (!handle) {
            launch_failed(attempt_id, "launch failed: " + handle.error());
            return;
        }

        // Step 6.
        a.phase = Phase::kRunning;
        journal.update_attempt_process(attempt_id, handle->pid, handle->start_ticks, handle->cgroup_path);
        log_info("job {}: attempt {} (#{}) started, pid {}", job_id, attempt_id, a.number, handle->pid);
        // After this, `a` may be cancelled by the listener; cancel() handles
        // the RUNNING phase itself, and the exit callback cannot run before
        // we return to the io loop, so `a` stays valid here.
        move(job_id, v1::ASSIGNED, v1::RUNNING);
    }

    void launch_failed(const std::string& attempt_id, const std::string& error) {
        auto a = retire(attempt_id);
        if (!a) return;
        log_warn("job {}: attempt {} {}", a->job_id, attempt_id, error);
        if (a->cancel_requested) {
            journal.finish_attempt(attempt_id, AttemptState::kCancelled, 0, 0, error);
            move(a->job_id, v1::ASSIGNED, v1::CANCELLED, a->cancel_reason);
        } else {
            journal.finish_attempt(attempt_id, AttemptState::kFailed, 0, 0, error);
            fail_or_retry(a->job_id, v1::ASSIGNED, error, /*retryable=*/true);
        }
        request_schedule();
    }

    // ---- exit handling (section 4 table) ------------------------------------

    void on_exit(const std::string& attempt_id, const supervisor::ExitInfo& info) {
        // Reservations are released on exit, before commit.
        auto a = retire(attempt_id);
        if (!a) {
            log_warn("exit for unknown attempt {}", attempt_id);
            return;
        }
        request_schedule();

        const std::string& job_id = a->job_id;
        auto rec = journal.get_job(job_id);
        if (!rec) return;
        const v1::JobState from = rec->state;
        log_debug("job {}: attempt {} {}", job_id, attempt_id, describe_exit(info));

        if (from != v1::RUNNING || rec->current_attempt_id != attempt_id) {
            // Not ours to drive any more (changed externally); record and stop.
            journal.finish_attempt(attempt_id, AttemptState::kAbandoned, info.exit_code, info.signal,
                                   "attempt no longer current: " + describe_exit(info));
            return;
        }

        if (a->cancel_requested || info.kind == ExitKind::kCancelled) {
            std::string reason = a->cancel_reason.empty() ? "cancelled" : a->cancel_reason;
            journal.finish_attempt(attempt_id, AttemptState::kCancelled, info.exit_code, info.signal, reason);
            move(job_id, from, v1::CANCELLED, reason);
            return;
        }

        if (info.kind == ExitKind::kTimedOut) {
            std::string err = std::format("timed out after {}s", a->spec.policy().timeout_seconds());
            journal.finish_attempt(attempt_id, AttemptState::kFailed, info.exit_code, info.signal, err);
            fail_or_retry(job_id, from, err, /*retryable=*/true, v1::EXPIRED);
            return;
        }

        if (info.kind == ExitKind::kExited && info.exit_code == 0) {
            if (!a->result || a->result->success()) {
                commit(*a);
                return;
            }
            std::string err = a->result->error().empty() ? "worker reported failure" : a->result->error();
            journal.finish_attempt(attempt_id, AttemptState::kFailed, 0, 0, err);
            fail_or_retry(job_id, from, err, a->result->retryable());
            return;
        }

        std::string err = describe_exit(info);
        if (a->result && !a->result->error().empty()) err += ": " + a->result->error();
        journal.finish_attempt(attempt_id, AttemptState::kFailed, info.exit_code, info.signal, err);
        fail_or_retry(job_id, from, err, /*retryable=*/true);
    }

    // ---- commit ------------------------------------------------------------

    // Output set per section 4 step 1, or an error (-> VALIDATION_FAILED).
    std::expected<std::vector<OutputToIngest>, std::string> collect_outputs(const Attempt& a) {
        const fs::path out_dir = a.dir / "outputs";
        std::error_code ec;
        const fs::path base = fs::canonical(out_dir, ec);
        if (ec) return std::unexpected("output dir missing: " + out_dir.string());

        std::vector<OutputToIngest> out;
        if (!a.result) {
            std::vector<fs::path> files;
            for (auto it = fs::recursive_directory_iterator(base, ec); !ec && it != fs::recursive_directory_iterator();
                 it.increment(ec)) {
                // symlink_status: symlinks are never followed or published.
                if (it->symlink_status().type() == fs::file_type::regular) files.push_back(it->path());
            }
            if (ec) return std::unexpected("scan outputs: " + ec.message());
            std::ranges::sort(files);
            for (auto& f : files) {
                std::string name = f.lexically_relative(base).generic_string();
                out.push_back({std::move(f), std::string(kDefaultMediaType), std::move(name)});
            }
            return out;
        }

        std::set<std::pair<fs::path, std::string>> seen;
        for (const auto& o : a.result->outputs()) {
            const fs::path rel(o.path());
            if (o.path().empty() || rel.is_absolute() || rel.has_root_name() || rel.has_root_directory()) {
                return std::unexpected("declared output path not relative: '" + o.path() + "'");
            }
            for (const auto& part : rel) {
                if (part == "..") return std::unexpected("declared output escapes output dir: " + o.path());
            }
            const fs::path full = base / rel;
            if (!fs::exists(fs::symlink_status(full, ec))) {
                return std::unexpected("declared output missing: " + o.path());
            }
            const fs::path real = fs::canonical(full, ec);
            if (ec) return std::unexpected("declared output unresolvable: " + o.path());
            if (!is_within(base, real) || real == base) {
                return std::unexpected("declared output escapes output dir: " + o.path());
            }
            if (!fs::is_regular_file(real, ec)) {
                return std::unexpected("declared output is not a regular file: " + o.path());
            }
            std::string name = o.name().empty() ? rel.lexically_normal().generic_string() : o.name();
            if (!seen.insert({real, name}).second) continue;  // same output declared twice
            out.push_back({real, o.media_type().empty() ? std::string(kDefaultMediaType) : o.media_type(),
                           std::move(name)});
        }
        return out;
    }

    void commit(Attempt& a) {
        const std::string& job_id = a.job_id;
        if (!move(job_id, v1::RUNNING, v1::COMMITTING)) return;

        auto files = collect_outputs(a);
        if (!files) {
            journal.finish_attempt(a.id, AttemptState::kFailed, 0, 0, files.error());
            move(job_id, v1::COMMITTING, v1::VALIDATION_FAILED, files.error());
            return;
        }

        // Names are unique per job (phase6.md §2); checked before anything is ingested.
        {
            std::set<std::string> names;
            for (const auto& f : *files) {
                if (names.insert(f.name).second) continue;
                const std::string err = "duplicate output name: " + f.name;
                journal.finish_attempt(a.id, AttemptState::kFailed, 0, 0, err);
                fail_or_retry(job_id, v1::COMMITTING, err, /*retryable=*/false);
                return;
            }
        }

        std::vector<v1::NamedArtifact> named;
        std::map<fs::path, artifacts::ArtifactMeta> ingested;  // a file declared under two names: ingest once
        std::size_t distinct = 0;
        for (const auto& f : *files) {
            auto it = ingested.find(f.path);
            if (it == ingested.end()) {
                // With a restricted job user the output file is owned by the job
                // user, so it is copied: the store file must be a new daemon-owned file.
                auto meta = store.ingest(f.path, f.media_type, job_id, /*move_source=*/!opts.run_as.has_value());
                if (!meta) {
                    std::string err = "ingest " + f.path.filename().string() + ": " + meta.error();
                    journal.finish_attempt(a.id, AttemptState::kFailed, 0, 0, err);
                    fail_or_retry(job_id, v1::COMMITTING, err, /*retryable=*/true);
                    return;
                }
                record(*meta);
                it = ingested.emplace(f.path, std::move(*meta)).first;
            }
            const auto& meta = it->second;
            if (std::ranges::none_of(named, [&](const auto& n) { return n.artifact_id() == meta.id; })) ++distinct;
            v1::NamedArtifact n;
            n.set_artifact_id(meta.id);
            n.set_name(f.name);
            n.set_size(meta.size);
            n.set_media_type(meta.media_type);
            named.push_back(std::move(n));
        }
        std::vector<v1::StepSpec> spawn;
        if (a.result && a.result->success()) spawn.assign(a.result->spawn().begin(), a.result->spawn().end());

        if (!journal.commit_outputs(job_id, a.id, named, spawn)) {
            // Fencing: the attempt is no longer current. Outputs stay in the
            // store unreferenced; the job is not touched.
            log_warn("job {}: commit by stale attempt {} rejected", job_id, a.id);
            journal.finish_attempt(a.id, AttemptState::kAbandoned, 0, 0, "commit fenced: attempt no longer current");
            return;
        }
        log_info("job {}: COMMITTING -> COMPLETED ({} outputs, {} artifacts{})", job_id, named.size(), distinct,
                 spawn.empty() ? std::string() : std::format(", {} spawned steps", spawn.size()));
        notify(job_id, v1::COMPLETED);

        if (!opts.keep_work_dirs) {
            // Not fatal: a job (especially under a restricted job user) can
            // leave files the daemon cannot remove.
            std::error_code ec;
            fs::remove_all(a.dir, ec);
            if (ec) log_warn("job {}: cleanup of {} failed: {}", job_id, a.dir.string(), ec.message());
            fs::remove(a.dir.parent_path(), ec);  // job dir, only if now empty
        }
    }

    void record(const artifacts::ArtifactMeta& meta) {
        journal::ArtifactRow row;
        row.id = meta.id;
        row.sha256_hex = meta.sha256_hex;
        row.size = meta.size;
        row.media_type = meta.media_type;
        row.producer_job = meta.producer_job;
        row.local_path = meta.local_path.string();
        row.created_unix_ms = meta.created_unix_ms;
        journal.record_artifact(row);
    }

    // ---- recovery ------------------------------------------------------------

    void recover() {
        for (const auto& att : journal.open_attempts()) {
            if (att.pid > 0) {
                supervisor::ProcessHandle h;
                h.pid = att.pid;
                h.start_ticks = att.pid_start_ticks;
                h.cgroup_path = att.cgroup_path;
                bool killed = sup.kill_orphan(h);
                log_info("recover: attempt {} pid {} {}", att.attempt_id, att.pid,
                         killed ? "killed" : "already gone");
            }
            journal.finish_attempt(att.attempt_id, AttemptState::kAbandoned, 0, 0, "abandoned: daemon restarted");
            auto rec = journal.get_job(att.job_id);
            if (rec && (rec->state == v1::ASSIGNED || rec->state == v1::RUNNING || rec->state == v1::COMMITTING) &&
                rec->current_attempt_id == att.attempt_id) {
                fail_or_retry_recovery(*rec, "attempt " + att.attempt_id + " abandoned after daemon restart");
            }
        }
        // In-flight states without an open attempt: same treatment.
        for (const auto& rec : journal.jobs_in_states({v1::ASSIGNED, v1::RUNNING, v1::COMMITTING})) {
            fail_or_retry_recovery(rec, "attempt lost after daemon restart");
        }
        for (const auto& rec : journal.jobs_in_states({v1::CREATED})) move(rec.job_id, v1::CREATED, v1::QUEUED);
        for (const auto& rec : journal.jobs_in_states({v1::RETRY_PENDING})) {
            move(rec.job_id, v1::RETRY_PENDING, v1::QUEUED);
        }
        // Rebuild the queue in creation order (journal returns oldest first).
        for (const auto& rec : journal.jobs_in_states({v1::QUEUED})) enqueue(rec);
        log_info("recover: {} job(s) queued", queue.size());
        request_schedule();
    }

    // Like fail_or_retry but without enqueuing (recover() rebuilds the queue).
    void fail_or_retry_recovery(const journal::JobRecord& rec, const std::string& error) {
        if (rec.attempts_used < max_attempts(rec.spec)) {
            if (move(rec.job_id, rec.state, v1::RETRY_PENDING, error)) move(rec.job_id, v1::RETRY_PENDING, v1::QUEUED);
        } else {
            move(rec.job_id, rec.state, v1::FAILED, error);
        }
    }

    // ---- submit / cancel ------------------------------------------------------

    std::expected<std::string, std::string> submit(v1::JobSpec spec) {
        if (spec.command().argv().empty() || spec.command().argv(0).empty()) {
            return std::unexpected("command.argv must not be empty");
        }
        const Capacity req = effective_request(spec, opts.default_job_memory_bytes);
        if (req.cpu_cores > opts.capacity.cpu_cores || req.memory_bytes > opts.capacity.memory_bytes ||
            opts.capacity.jobs == 0) {
            return std::unexpected(std::format(
                "request ({} cores, {} bytes) exceeds node capacity ({} cores, {} bytes, {} jobs)", req.cpu_cores,
                req.memory_bytes, opts.capacity.cpu_cores, opts.capacity.memory_bytes, opts.capacity.jobs));
        }
        for (const auto& id : spec.input_artifacts()) {
            if (!artifact_hex(id)) return std::unexpected("invalid input artifact id: " + id);
            if (!store.contains(id)) return std::unexpected("input artifact not found locally: " + id);
        }
        if (spec.input_names_size() > spec.input_artifacts_size()) {
            return std::unexpected(std::format("input_names has {} entries but there are only {} input artifacts",
                                               spec.input_names_size(), spec.input_artifacts_size()));
        }
        if (const auto& ck = spec.resume_checkpoint(); !ck.empty()) {
            if (!artifact_hex(ck)) return std::unexpected("invalid resume checkpoint id: " + ck);
            if (!store.contains(ck)) return std::unexpected("resume checkpoint not found locally: " + ck);
        }
        if (spec.job_id().empty()) {
            spec.set_job_id(new_id("job"));
        } else {
            if (!valid_job_id(spec.job_id())) {
                return std::unexpected("invalid job_id (allowed: [A-Za-z0-9._-], max 128 chars)");
            }
            if (journal.get_job(spec.job_id())) return std::unexpected("job already exists: " + spec.job_id());
        }

        journal::JobRecord rec;
        rec.job_id = spec.job_id();
        rec.state = v1::CREATED;
        rec.created_unix_ms = unix_ms();
        rec.updated_unix_ms = rec.created_unix_ms;
        rec.spec = std::move(spec);
        try {
            journal.insert_job(rec);
        } catch (const journal::JournalError& e) {
            return std::unexpected(std::string("journal: ") + e.what());
        }
        const std::string job_id = rec.job_id;
        log_info("job {}: submitted ({} cores, {} bytes)", job_id, req.cpu_cores, req.memory_bytes);
        // The listener sees CREATED too: it is the job's first state.
        notify(job_id, v1::CREATED);
        if (move(job_id, v1::CREATED, v1::QUEUED)) {
            rec.state = v1::QUEUED;
            enqueue(rec);
        }
        request_schedule();
        return job_id;
    }

    std::expected<void, std::string> cancel(const std::string& job_id) {
        auto rec = journal.get_job(job_id);
        if (!rec) return std::unexpected("unknown job: " + job_id);
        if (is_terminal(rec->state)) {
            return std::unexpected(std::format("job {} is already {}", job_id, to_string(rec->state)));
        }
        const std::string reason = "cancelled by request";
        switch (rec->state) {
            case v1::CREATED:
            case v1::QUEUED:
            case v1::LEASED:
            case v1::RETRY_PENDING:
                dequeue(job_id);
                if (!move(job_id, rec->state, v1::CANCELLED, reason)) {
                    return std::unexpected("job state changed concurrently");
                }
                request_schedule();  // head of queue may have changed
                return {};
            case v1::ASSIGNED:
            case v1::RUNNING: {
                auto ja = job_attempt.find(job_id);
                auto it = ja == job_attempt.end() ? attempts.end() : attempts.find(ja->second);
                if (it == attempts.end()) {
                    // No live process (should not happen); finish it here.
                    if (!move(job_id, rec->state, v1::CANCELLED, reason)) {
                        return std::unexpected("job state changed concurrently");
                    }
                    return {};
                }
                Attempt& a = *it->second;
                if (a.cancel_requested) return {};
                a.cancel_requested = true;
                a.cancel_reason = reason;
                log_info("job {}: cancel requested (attempt {})", job_id, a.id);
                if (a.phase == Phase::kRunning) {
                    if (cancel_notifier) cancel_notifier(a.id, reason);
                    sup.cancel(a.id, opts.cancel_grace);
                }
                // kStarting: start_attempt sees the flag and never launches.
                return {};
            }
            case v1::COMMITTING:
                return std::unexpected("job " + job_id + " is committing outputs");
            default:
                return std::unexpected(std::format("job {} cannot be cancelled in state {}", job_id,
                                                   to_string(rec->state)));
        }
    }

    // ---- worker hooks ----------------------------------------------------------

    Attempt* running_attempt(const std::string& attempt_id) {
        auto it = attempts.find(attempt_id);
        if (it == attempts.end() || it->second->phase != Phase::kRunning) return nullptr;
        return it->second.get();
    }

    std::expected<v1::WorkerTask, std::string> on_worker_hello(const v1::WorkerHello& hello) {
        auto it = attempts.find(hello.attempt_id());
        // Same message for unknown attempt and bad token: don't leak which.
        if (it == attempts.end() || !constant_time_equal(hello.token(), it->second->token)) {
            return std::unexpected("unknown attempt or bad token");
        }
        Attempt& a = *it->second;
        if (a.phase != Phase::kRunning) return std::unexpected("attempt is not running");
        if (a.cancel_requested) return std::unexpected("attempt is being cancelled");

        v1::WorkerTask t;
        t.set_job_id(a.job_id);
        t.set_attempt_id(a.id);
        t.set_attempt_number(a.number);
        t.set_operation(a.spec.operation());
        t.set_parameters_json(a.spec.parameters_json());
        for (const auto& f : a.inputs) *t.add_inputs() = f;
        t.set_output_dir((a.dir / "outputs").string());
        t.set_checkpoint_dir((a.dir / "checkpoints").string());
        t.set_campaign_id(a.spec.campaign_id());
        t.set_material_id(a.spec.material_id());
        if (!a.resume_path.empty()) t.set_resume_checkpoint(a.resume_path.string());
        t.set_workflow_id(a.spec.workflow_id());
        t.set_step(a.spec.step());
        t.set_checkpoint_enabled(a.spec.policy().checkpoint());
        log_debug("job {}: worker hello (attempt {}, pid {}, sdk {})", a.job_id, a.id, hello.pid(),
                  hello.sdk_version());
        return t;
    }

    // Same check as on_worker_hello, without binding anything (phase7.md §5).
    std::expected<std::string, std::string> verify_attempt(const std::string& attempt_id,
                                                           const std::string& token) const {
        auto it = attempts.find(attempt_id);
        if (it == attempts.end() || !constant_time_equal(token, it->second->token)) {
            return std::unexpected("unknown attempt or bad token");
        }
        const Attempt& a = *it->second;
        if (a.phase != Phase::kRunning) return std::unexpected("attempt is not running");
        if (a.cancel_requested) return std::unexpected("attempt is being cancelled");
        return a.job_id;
    }

    void on_worker_progress(const v1::WorkerProgress& p) {
        Attempt* a = running_attempt(p.attempt_id());
        if (!a) return;
        double f = p.fraction();
        if (!(f >= 0.0)) f = 0.0;  // also catches NaN
        if (f > 1.0) f = 1.0;
        journal.set_progress(a->job_id, f, p.message());
    }

    std::expected<void, std::string> on_worker_result(const v1::WorkerResult& r) {
        Attempt* a = running_attempt(r.attempt_id());
        if (!a) return std::unexpected("unknown or finished attempt: " + r.attempt_id());
        if (a->result) return std::unexpected("result already received for attempt " + r.attempt_id());
        a->result = r;
        log_debug("job {}: worker result success={} outputs={} spawn={}", a->job_id, r.success(), r.outputs_size(),
                  r.spawn_size());
        return {};
    }

    // phase6.md §6. Same path care as declared outputs.
    std::expected<v1::NamedArtifact, std::string> on_worker_checkpoint(const v1::WorkerCheckpoint& c) {
        Attempt* a = running_attempt(c.attempt_id());
        if (!a) return std::unexpected("unknown or finished attempt: " + c.attempt_id());
        if (!a->spec.policy().checkpoint()) return std::unexpected("checkpointing not enabled for this job");
        if (a->cancel_requested) return std::unexpected("attempt is being cancelled");
        auto rec = journal.get_job(a->job_id);
        if (!rec || rec->state != v1::RUNNING || rec->current_attempt_id != a->id) {
            return std::unexpected("attempt is not running");
        }
        const std::string name = c.name().empty() ? std::string(kDefaultCheckpointName) : c.name();
        if (!valid_checkpoint_name(name)) {
            return std::unexpected("invalid checkpoint name (allowed: [A-Za-z0-9._-], max 128 chars)");
        }
        if (c.path().empty()) return std::unexpected("checkpoint path is empty");

        std::error_code ec;
        const fs::path base = fs::canonical(a->dir / "checkpoints", ec);
        if (ec) return std::unexpected("checkpoint dir missing");
        const fs::path given(c.path());
        if (!given.is_absolute()) {
            for (const auto& part : given) {
                if (part == "..") return std::unexpected("checkpoint path escapes checkpoint dir: " + c.path());
            }
        }
        const fs::path full = given.is_absolute() ? given : base / given;
        const fs::path real = fs::canonical(full, ec);
        if (ec) return std::unexpected("checkpoint not found: " + c.path());
        if (!is_within(base, real) || real == base) {
            return std::unexpected("checkpoint path escapes checkpoint dir: " + c.path());
        }
        if (!fs::is_regular_file(real, ec)) return std::unexpected("checkpoint is not a regular file: " + c.path());

        // Always a copy: the worker may keep writing to the file (and under a
        // restricted job user it owns it).
        const std::string media = c.media_type().empty() ? std::string(kDefaultMediaType) : c.media_type();
        auto meta = store.ingest(real, media, a->job_id, /*move_source=*/false);
        if (!meta) return std::unexpected("ingest checkpoint: " + meta.error());
        record(*meta);

        v1::NamedArtifact n;
        n.set_artifact_id(meta->id);
        n.set_name(name);
        n.set_size(meta->size);
        n.set_media_type(meta->media_type);
        const std::string job_id = a->job_id;  // the listener may re-enter
        journal.set_checkpoint(job_id, n);
        log_info("job {}: checkpoint '{}' {} ({} bytes)", job_id, name, n.artifact_id(), n.size());
        if (checkpoint_listener) checkpoint_listener(job_id, n);
        return n;
    }
};

// ---- JobManager forwarding ------------------------------------------------------

JobManager::JobManager(boost::asio::io_context& io, journal::IJournal& journal, artifacts::IArtifactStore& store,
                       supervisor::IProcessSupervisor& supervisor, ManagerOptions options)
    : impl_(std::make_unique<Impl>(io, journal, store, supervisor, std::move(options))) {}

JobManager::~JobManager() = default;

void JobManager::recover() { impl_->recover(); }

std::expected<std::string, std::string> JobManager::submit(v1::JobSpec spec) { return impl_->submit(std::move(spec)); }

std::expected<void, std::string> JobManager::cancel(const std::string& job_id) { return impl_->cancel(job_id); }

std::optional<v1::JobStatus> JobManager::status(const std::string& job_id) const {
    auto rec = impl_->journal.get_job(job_id);
    if (!rec) return std::nullopt;
    return to_status(*rec);
}

std::vector<v1::JobStatus> JobManager::list(v1::JobState state, std::size_t limit) const {
    std::vector<v1::JobStatus> out;
    for (const auto& rec : impl_->journal.list_jobs(state, limit)) out.push_back(to_status(rec));
    return out;
}

Usage JobManager::usage() const {
    Usage u;
    u.allocated = impl_->allocated;
    u.running = static_cast<std::uint32_t>(impl_->attempts.size());
    u.queued = static_cast<std::uint32_t>(impl_->queue.size());
    return u;
}

void JobManager::set_capacity(const Capacity& capacity) {
    impl_->opts.capacity = capacity;
    impl_->request_schedule();
}

Capacity JobManager::capacity() const { return impl_->opts.capacity; }

std::expected<v1::WorkerTask, std::string> JobManager::on_worker_hello(const v1::WorkerHello& hello) {
    return impl_->on_worker_hello(hello);
}

std::expected<std::string, std::string> JobManager::verify_attempt(const std::string& attempt_id,
                                                                  const std::string& token) const {
    return impl_->verify_attempt(attempt_id, token);
}

void JobManager::on_worker_progress(const v1::WorkerProgress& progress) { impl_->on_worker_progress(progress); }

std::expected<void, std::string> JobManager::on_worker_result(const v1::WorkerResult& result) {
    return impl_->on_worker_result(result);
}

std::expected<v1::NamedArtifact, std::string> JobManager::on_worker_checkpoint(const v1::WorkerCheckpoint& c) {
    return impl_->on_worker_checkpoint(c);
}

void JobManager::set_checkpoint_listener(std::function<void(const std::string&, const v1::NamedArtifact&)> fn) {
    impl_->checkpoint_listener = std::move(fn);
}

void JobManager::set_cancel_notifier(std::function<void(const std::string&, const std::string&)> fn) {
    impl_->cancel_notifier = std::move(fn);
}

void JobManager::set_state_listener(std::function<void(const std::string&, v1::JobState)> fn) {
    impl_->state_listener = std::move(fn);
}

}  // namespace tdr::jobs
