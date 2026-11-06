// JobStore: central job queue, fenced leases, artifact locations
// (docs/design/phase4.md §2-§4). Every mutating method is one transaction.
//
// Lock order: a job row (jobs, FOR UPDATE) is always locked before its
// attempt rows, so update_job / expire_leases / cancel_job cannot deadlock
// with each other. renew_leases only touches attempt rows.
#include "jobs.hpp"

#include <algorithm>
#include <charconv>
#include <format>
#include <map>
#include <set>

#include "util.hpp"
#include "workflows.hpp"

namespace tdr::cpdb {

namespace {

using detail::clamp_limit;
using detail::from_json;
using detail::short_id;
using detail::to_json;
using pg::Result;
using pg::transact;

#define TS TDR_TS
#define MS TDR_MS

constexpr int kMaxLeaseScan = 200;  // phase4.md §3

// SET fragment for an UPDATE whose new state is $2: entering QUEUED restarts
// the locality wait (phase5.md §4).
// It also resets the ranked-placement state (phase6.md §7); the caller then runs refresh_strict.
#define QUEUED_AT(n)                                                                                    \
    " queued_at = CASE WHEN $2 = 'QUEUED' THEN " TS(n) " ELSE queued_at END,"                           \
    " reserved_node = CASE WHEN $2 = 'QUEUED' THEN NULL ELSE reserved_node END,"                        \
    " reserved_until = CASE WHEN $2 = 'QUEUED' THEN NULL ELSE reserved_until END,"                      \
    " reserve_tried = CASE WHEN $2 = 'QUEUED' THEN '{}'::text[] ELSE reserve_tried END,"                \
    " reserve_done = CASE WHEN $2 = 'QUEUED' THEN false ELSE reserve_done END"

// pg_advisory_xact_lock(kArtifactLockClass, hashtext(artifact_id)) serializes
// release and pin changes of one artifact (two-key space: no clash with the
// one-key migration lock).
constexpr std::int32_t kArtifactLockClass = 0x4646'4152;  // "FFAR"

std::string state_name(tv1::JobState s) { return tv1::JobState_Name(s); }

tv1::JobState parse_job_state(const std::string& s) {
    tv1::JobState v = tv1::JOB_STATE_UNSPECIFIED;
    if (!tv1::JobState_Parse(s, &v)) throw DbError("unknown job state '" + s + "'");
    return v;
}

bool is_terminal(tv1::JobState s) {
    return s == tv1::COMPLETED || s == tv1::FAILED || s == tv1::CANCELLED || s == tv1::EXPIRED ||
           s == tv1::VALIDATION_FAILED;
}

double parse_double(const std::string& s) {
    double v = 0;
    auto [p, ec] = std::from_chars(s.data(), s.data() + s.size(), v);
    if (ec != std::errc{}) throw DbError("bad float '" + s + "' from database");
    return v;
}

tv1::JobSpec parse_spec(const std::string& hex) {
    tv1::JobSpec spec;
    if (!spec.ParseFromString(detail::from_hex(hex))) throw DbError("stored job spec does not parse");
    return spec;
}

constexpr const char* kJobCols =
    "j.job_id, j.state, encode(j.spec, 'hex'), j.attempts_used, coalesce(j.current_attempt_id, ''),"
    " coalesce(j.node_id, ''), j.last_error, j.outputs::text, j.submitted_by, j.cancel_requested, " MS(
        "j.created_at") ", " MS("j.updated_at") ", j.progress_fraction::text, j.progress_message,"
    " coalesce(j.workflow_id, ''), coalesce(j.step, ''), coalesce(j.checkpoint::text, ''),"
    " coalesce(j.reserved_node, ''), coalesce(" MS("j.reserved_until") ", 0)";

cv1::Job job_from(const Result& r, int row) {
    cv1::Job j;
    j.set_job_id(r.str(row, 0));
    j.set_state(parse_job_state(r.str(row, 1)));
    *j.mutable_spec() = parse_spec(r.str(row, 2));
    j.set_attempts_used(static_cast<std::uint32_t>(r.u64(row, 3)));
    j.set_current_attempt_id(r.str(row, 4));
    j.set_node_id(r.str(row, 5));
    j.set_last_error(r.str(row, 6));
    cv1::Job carrier;
    from_json("{\"outputs\":" + r.str(row, 7) + "}", &carrier);
    *j.mutable_outputs() = std::move(*carrier.mutable_outputs());
    j.set_submitted_by(r.str(row, 8));
    j.set_cancel_requested(r.str(row, 9) == "t");
    j.set_created_unix_ms(r.i64(row, 10));
    j.set_updated_unix_ms(r.i64(row, 11));
    j.set_progress_fraction(parse_double(r.str(row, 12)));
    j.set_progress_message(r.str(row, 13));
    j.set_workflow_id(r.str(row, 14));
    j.set_step(r.str(row, 15));
    if (!r.str(row, 16).empty()) from_json(r.str(row, 16), j.mutable_checkpoint());
    j.set_reserved_node(r.str(row, 17));
    j.set_reserved_until_unix_ms(r.i64(row, 18));
    return j;
}

constexpr const char* kAttemptCols =
    "attempt_id, number, fencing_token, node_id, state, error, " MS("leased_at") ", " MS(
        "expires_at") ", coalesce(" MS("ended_at") ", 0)";

cv1::JobAttempt attempt_from(const Result& r, int row) {
    cv1::JobAttempt a;
    a.set_attempt_id(r.str(row, 0));
    a.set_number(static_cast<std::uint32_t>(r.u64(row, 1)));
    a.set_fencing_token(r.u64(row, 2));
    a.set_node_id(r.str(row, 3));
    a.set_state(r.str(row, 4));
    a.set_error(r.str(row, 5));
    a.set_leased_unix_ms(r.i64(row, 6));
    a.set_expires_unix_ms(r.i64(row, 7));
    a.set_ended_unix_ms(r.i64(row, 8));
    return a;
}

}  // namespace

// ---- jobs ---------------------------------------------------------------------

std::string JobStore::insert_job(tv1::JobSpec& spec, const std::string& submitted_by, std::int64_t now_ms) {
    if (spec.job_id().empty()) spec.set_job_id(short_id("job-"));
    std::string raw;
    if (!spec.SerializeToString(&raw)) throw DbError("job spec does not serialize");
    const auto& res = spec.resources();
    auto opt = [](const std::string& v) { return v.empty() ? std::nullopt : std::optional(v); };
    c_.exec("INSERT INTO jobs (job_id, state, spec, campaign_id, cpu_cores, memory_bytes, gpu_count,"
            " required_capabilities, input_artifacts, max_attempts, submitted_by, created_at, updated_at, queued_at,"
            " workflow_id, step)"
            " VALUES ($1, 'QUEUED', $2::bytea, $3, $4::bigint, $5::numeric, $6::bigint, $7::jsonb, $8::jsonb,"
            " $9::int, $10, " TS(11) ", " TS(11) ", " TS(11) ", $12, $13)",
            spec.job_id(), detail::to_bytea_hex(raw), opt(spec.campaign_id()), std::max<std::uint32_t>(res.cpu_cores(), 1),
            res.memory_bytes(), res.gpu_count(), detail::string_array_json(spec.required_capabilities()),
            detail::string_array_json(spec.input_artifacts()), std::max<std::uint32_t>(spec.policy().max_attempts(), 1),
            submitted_by, now_ms, opt(spec.workflow_id()), opt(spec.step()));
    refresh_strict(spec.job_id());
    return spec.job_id();
}

void JobStore::refresh_strict(const std::string& job_id) {
    // Input sizes as known now (largest announced copy); the checkpoint counts as an input.
    c_.exec("UPDATE jobs j SET strict = (j.gpu_count > 0 OR coalesce((SELECT sum(t.sz) FROM"
            "   (SELECT max(l.size) AS sz FROM artifact_locations l WHERE l.artifact_id IN"
            "      (SELECT jsonb_array_elements_text(j.input_artifacts) UNION SELECT j.checkpoint->>'artifact_id')"
            "    GROUP BY l.artifact_id) t), 0) >= $2::numeric)"
            " WHERE j.job_id = $1",
            job_id, strict_bytes_);
}

std::expected<cv1::Job, std::string> JobStore::create_job(const tv1::JobSpec& in, const std::string& submitted_by,
                                                          std::int64_t now_ms) {
    tv1::JobSpec spec = in;
    try {
        return transact(c_, [&] {
            tv1::JobSpec s = spec;
            insert_job(s, submitted_by, now_ms);
            auto j = load_job(s.job_id(), false);
            if (!j) throw DbError("create_job: job missing after insert");
            return *j;
        });
    } catch (const DbError& e) {
        if (e.sqlstate == "23505") return std::unexpected("job id already exists");
        if (e.sqlstate == "23503") return std::unexpected("unknown campaign");
        throw;
    }
}

std::optional<cv1::Job> JobStore::load_job(const std::string& job_id, bool with_attempts) {
    auto r = c_.exec(std::string("SELECT ") + kJobCols + " FROM jobs j WHERE j.job_id = $1", job_id);
    if (r.rows() != 1) return std::nullopt;
    auto j = job_from(r, 0);
    if (with_attempts) {
        auto a = c_.exec(std::string("SELECT ") + kAttemptCols + " FROM job_attempts WHERE job_id = $1 ORDER BY number",
                         job_id);
        for (int i = 0; i < a.rows(); ++i) *j.add_attempts() = attempt_from(a, i);
    }
    return j;
}

std::optional<cv1::Job> JobStore::get_job(const std::string& job_id, bool with_attempts) {
    // Two reads: run them in one (read-only) transaction for a consistent view.
    return transact(c_, [&] { return load_job(job_id, with_attempts); });
}

std::vector<cv1::Job> JobStore::list_jobs(tv1::JobState state, const std::string& campaign_id, std::size_t limit) {
    return c_.retry([&] {
        const std::optional<std::string> s =
            state == tv1::JOB_STATE_UNSPECIFIED ? std::nullopt : std::optional(state_name(state));
        auto r = c_.exec(std::string("SELECT ") + kJobCols +
                             " FROM jobs j WHERE ($1::text IS NULL OR j.state = $1)"
                             " AND ($2 = '' OR j.campaign_id = $2)"
                             " ORDER BY j.created_at DESC, j.seq DESC LIMIT $3::bigint",
                         s, campaign_id, clamp_limit(limit));
        std::vector<cv1::Job> out;
        out.reserve(static_cast<std::size_t>(r.rows()));
        for (int i = 0; i < r.rows(); ++i) out.push_back(job_from(r, i));
        return out;
    });
}

CancelOutcome JobStore::cancel_job(const std::string& job_id, const std::string& reason, std::int64_t now_ms) {
    const std::string why = reason.empty() ? std::string("cancelled") : reason;
    return transact(c_, [&] {
        CancelOutcome out;
        // Lock order: the workflow row before the job row.
        auto w = c_.exec("SELECT coalesce(workflow_id, ''), coalesce(step, '') FROM jobs WHERE job_id = $1", job_id);
        if (w.rows() != 1) return out;
        const std::string wf = w.str(0, 0), step = w.str(0, 1);
        if (!wf.empty()) wf_->lock(wf, false);
        auto r = c_.exec("SELECT state FROM jobs WHERE job_id = $1 FOR UPDATE", job_id);
        if (r.rows() != 1) return out;
        const auto st = parse_job_state(r.str(0, 0));
        if (is_terminal(st)) {
            out.result = CancelResult::kTerminal;
        } else if (st == tv1::LEASED || st == tv1::RUNNING) {
            // The holder learns via HeartbeatReply.revoked; its CANCELLED report
            // (or lease expiry) finishes the job.
            c_.exec("UPDATE jobs SET cancel_requested = true, last_error = $2, updated_at = " TS(3) " WHERE job_id = $1",
                    job_id, why, now_ms);
            out.result = CancelResult::kRequested;
        } else {
            c_.exec("UPDATE jobs SET state = 'CANCELLED', cancel_requested = true, last_error = $2,"
                    " updated_at = " TS(3) " WHERE job_id = $1",
                    job_id, why, now_ms);
            out.result = CancelResult::kCancelled;
            if (!wf.empty()) wf_->job_terminal(wf, step, tv1::CANCELLED, why, nullptr, now_ms);
        }
        out.job = load_job(job_id, false);
        return out;
    });
}

// ---- leases -------------------------------------------------------------------

std::vector<cv1::Lease> JobStore::lease_jobs(const cv1::LeaseRequest& req, std::int64_t ttl_ms,
                                             std::int64_t locality_wait_ms, std::int64_t now_ms) {
    if (req.max_jobs() == 0 || req.free().cpu_cores() == 0) return {};
    return transact(c_, [&] {
        std::vector<cv1::Lease> out;
        auto n = c_.exec("SELECT health, capabilities::text FROM nodes WHERE node_id = $1", req.node_id());
        if (n.rows() != 1 || n.str(0, 0) != "ONLINE") return out;

        // Eligibility in SQL (phase4.md §3, phase5.md §4, phase6.md §7): QUEUED,
        // campaign absent or ACTIVE, capabilities subset, fits the free capacity
        // on its own, not reserved for another node (live reservation), and either
        //  - reserved for this node (live): every input on some ONLINE node, or
        //  - not strict, or released by place_jobs: locality (every input on this
        //    node, or queued for at least locality_wait and every input on some
        //    ONLINE node).
        // A strict job is leasable only via place_jobs. Jobs reserved for this
        // node come first. Greedy packing happens below.
        // Locked candidates that are not granted stay QUEUED; other lease
        // transactions skip them until this one commits.
#define MINE "coalesce(j.reserved_node = $1 AND j.reserved_until > " TS(8) ", false)"
#define ON_ONLINE                                                                                  \
    "NOT EXISTS (SELECT 1 FROM jsonb_array_elements_text(j.input_artifacts) a(id)"                  \
    "  WHERE NOT EXISTS (SELECT 1 FROM artifact_locations l JOIN nodes h ON h.node_id = l.node_id" \
    "                    WHERE l.artifact_id = a.id AND h.health = 'ONLINE'))"
        auto c = c_.exec(
            "SELECT j.job_id, j.cpu_cores, j.memory_bytes::text, j.gpu_count, j.attempts_used, encode(j.spec, 'hex'),"
            " CASE WHEN j.checkpoint IS NOT NULL AND EXISTS (SELECT 1 FROM artifact_locations l"
            "   JOIN nodes h ON h.node_id = l.node_id"
            "   WHERE l.artifact_id = j.checkpoint->>'artifact_id' AND h.health = 'ONLINE')"
            " THEN j.checkpoint->>'artifact_id' ELSE '' END"
            " FROM jobs j LEFT JOIN campaigns c ON c.campaign_id = j.campaign_id"
            " WHERE j.state = 'QUEUED'"
            "   AND (j.campaign_id IS NULL OR c.state = 'ACTIVE')"
            "   AND j.required_capabilities <@ $2::jsonb"
            "   AND j.cpu_cores <= $3::bigint AND j.memory_bytes <= $4::numeric AND j.gpu_count <= $5::bigint"
            "   AND NOT coalesce(j.reserved_node <> $1 AND j.reserved_until > " TS(8) ", false)"
            "   AND ((" MINE " AND " ON_ONLINE ")"
            "     OR ((NOT j.strict OR j.reserve_done)"
            "       AND (NOT EXISTS (SELECT 1 FROM jsonb_array_elements_text(j.input_artifacts) a(id)"
            "            WHERE NOT EXISTS (SELECT 1 FROM artifact_locations l"
            "                              WHERE l.artifact_id = a.id AND l.node_id = $1))"
            "         OR (coalesce(j.queued_at, j.created_at) <= " TS(7) " AND " ON_ONLINE "))))"
            " ORDER BY " MINE " DESC, j.created_at, j.seq LIMIT $6::int"
            " FOR UPDATE OF j SKIP LOCKED",
            req.node_id(), n.str(0, 1), req.free().cpu_cores(), req.free().memory_bytes(), req.free().gpu_count(),
            kMaxLeaseScan, now_ms - std::max<std::int64_t>(locality_wait_ms, 0), now_ms);
#undef MINE
#undef ON_ONLINE

        std::uint64_t cpu = req.free().cpu_cores();
        std::uint64_t mem = req.free().memory_bytes();
        std::uint64_t gpu = req.free().gpu_count();
        const std::int64_t expires = now_ms + ttl_ms;
        for (int i = 0; i < c.rows() && out.size() < req.max_jobs(); ++i) {
            // cpu_cores is stored as max(1, requested). memory 0 means "node
            // default" (applied by the node), so it always fits here.
            const auto jc = c.u64(i, 1), jm = c.u64(i, 2), jg = c.u64(i, 3);
            if (jc > cpu || jm > mem || jg > gpu) continue;  // misfit: try the next one
            cpu -= jc;
            mem -= jm;
            gpu -= jg;

            const std::string job_id = c.str(i, 0);
            const auto number = static_cast<std::uint32_t>(c.u64(i, 4) + 1);
            const std::string attempt_id = short_id("att-");
            c_.exec("INSERT INTO job_attempts (attempt_id, job_id, number, fencing_token, node_id, state, leased_at,"
                    " expires_at) VALUES ($1, $2, $3::int, $3::bigint, $4, 'LEASED', " TS(5) ", " TS(6) ")",
                    attempt_id, job_id, number, req.node_id(), now_ms, expires);
            c_.exec("UPDATE jobs SET state = 'LEASED', attempts_used = $2::int, current_attempt_id = $3, node_id = $4,"
                    " progress_fraction = 0, progress_message = '', reserved_node = NULL, reserved_until = NULL,"
                    " updated_at = " TS(5) " WHERE job_id = $1",
                    job_id, number, attempt_id, req.node_id(), now_ms);
            cv1::Lease l;
            l.set_job_id(job_id);
            l.set_attempt_id(attempt_id);
            l.set_fencing_token(number);
            l.set_attempt_number(number);
            *l.mutable_spec() = parse_spec(c.str(i, 5));
            // Resume only from a checkpoint some ONLINE node can serve (phase6.md §6);
            // otherwise the attempt starts from scratch.
            l.mutable_spec()->set_resume_checkpoint(c.str(i, 6));
            l.set_expires_unix_ms(expires);
            out.push_back(std::move(l));
        }
        return out;
    });
}

UpdateOutcome JobStore::update_job(const std::string& job_id, const std::string& node_id, const cv1::JobUpdate& u,
                                   std::int64_t now_ms) {
    return transact(c_, [&] {
        UpdateOutcome out;
        // Lock order: the workflow row before the job row, for updates that may
        // end the job (phase6.md §4). RUNNING updates don't touch the workflow.
        auto w = c_.exec("SELECT coalesce(workflow_id, ''), coalesce(step, '') FROM jobs WHERE job_id = $1", job_id);
        if (w.rows() != 1) {
            out.message = "unknown job " + job_id;
            return out;
        }
        const std::string wf = w.str(0, 0), step = w.str(0, 1);
        if (!wf.empty() && u.state() != tv1::RUNNING) wf_->lock(wf, false);
        auto r = c_.exec(
            "SELECT j.state, coalesce(j.current_attempt_id, ''), coalesce(a.fencing_token, 0), coalesce(a.node_id, ''),"
            " j.attempts_used, j.max_attempts, j.cancel_requested"
            " FROM jobs j LEFT JOIN job_attempts a ON a.attempt_id = j.current_attempt_id"
            " WHERE j.job_id = $1 FOR UPDATE OF j",
            job_id);
        if (r.rows() != 1) {
            out.message = "unknown job " + job_id;
            return out;
        }
        switch (u.state()) {
            case tv1::RUNNING:
            case tv1::COMPLETED:
            case tv1::FAILED:
            case tv1::CANCELLED:
            case tv1::EXPIRED:
            case tv1::VALIDATION_FAILED: break;
            default:
                out.result = UpdateResult::kInvalid;
                out.message = "state must be RUNNING, COMPLETED, FAILED, CANCELLED, EXPIRED or VALIDATION_FAILED";
                return out;
        }
        const auto st = parse_job_state(r.str(0, 0));
        const std::string cur_attempt = r.str(0, 1);
        const auto token = r.u64(0, 2);
        const std::string holder = r.str(0, 3);
        const auto used = r.u64(0, 4), max_attempts = r.u64(0, 5);
        const bool cancel_requested = r.str(0, 6) == "t";

        out.result = UpdateResult::kStale;
        if (st != tv1::LEASED && st != tv1::RUNNING) {
            out.message = std::format("job {} is {}", job_id, state_name(st));
            return out;
        }
        if (cur_attempt != u.attempt_id() || token != u.fencing_token()) {
            out.message = std::format("attempt {} token {} is not the current attempt of {}", u.attempt_id(),
                                      u.fencing_token(), job_id);
            return out;
        }
        if (holder != node_id) {
            out.message = std::format("attempt {} is not held by {}", u.attempt_id(), node_id);
            return out;
        }

        auto end_attempt = [&](const char* state, const std::string& error) {
            c_.exec("UPDATE job_attempts SET state = $2, error = $3, ended_at = " TS(4) " WHERE attempt_id = $1",
                    cur_attempt, state, error, now_ms);
        };
        auto set_job = [&](tv1::JobState to, const std::string& error) {
            c_.exec("UPDATE jobs SET state = $2, last_error = $3, updated_at = " TS(4) "," QUEUED_AT(4)
                    " WHERE job_id = $1",
                    job_id, state_name(to), error, now_ms);
            if (to == tv1::QUEUED) refresh_strict(job_id);
        };
        // The workflow engine, when the job is now terminal.
        auto engine = [&](tv1::JobState to, const std::string& error, const WorkflowStore::Expansion* exp) {
            if (!wf.empty()) wf_->job_terminal(wf, step, to, error, exp, now_ms);
        };

        switch (u.state()) {
            case tv1::RUNNING: {
                const bool has_progress = u.progress_fraction() != 0 || !u.progress_message().empty();
                c_.exec("UPDATE jobs SET state = 'RUNNING', updated_at = " TS(2) ","
                        " progress_fraction = CASE WHEN $3::boolean THEN $4::double precision ELSE progress_fraction END,"
                        " progress_message = CASE WHEN $3::boolean THEN $5 ELSE progress_message END WHERE job_id = $1",
                        job_id, now_ms, has_progress, std::format("{}", std::clamp(u.progress_fraction(), 0.0, 1.0)),
                        u.progress_message());
                c_.exec("UPDATE job_attempts SET state = 'RUNNING' WHERE attempt_id = $1 AND state = 'LEASED'",
                        cur_attempt);
                if (u.has_checkpoint() && !u.checkpoint().artifact_id().empty()) {
                    // Latest checkpoint wins (phase6.md §6); the reporting node holds it.
                    cv1::JobUpdate carrier;
                    auto* ck = carrier.add_outputs();
                    *ck = u.checkpoint();
                    if (ck->producer_job().empty()) ck->set_producer_job(job_id);
                    if (ck->created_unix_ms() == 0) ck->set_created_unix_ms(now_ms);
                    if (ck->name().empty()) ck->set_name("checkpoint");
                    const std::string json = to_json(carrier);
                    c_.exec("UPDATE jobs SET checkpoint = ($2::jsonb) -> 'outputs' -> 0 WHERE job_id = $1", job_id, json);
                    upsert_locations(node_id, json, "outputs", "", now_ms);
                }
                break;
            }
            case tv1::COMPLETED: {
                cv1::JobUpdate carrier;
                for (const auto& o : u.outputs()) {
                    auto* x = carrier.add_outputs();
                    *x = o;
                    if (x->producer_job().empty()) x->set_producer_job(job_id);
                    if (x->created_unix_ms() == 0) x->set_created_unix_ms(now_ms);
                }
                const std::string json = to_json(carrier);
                // Fan-out (phase6.md §5), validated before anything is written.
                std::string invalid;
                WorkflowStore::Expansion exp;
                if (!u.spawn().empty()) {
                    if (wf.empty()) invalid = "only workflow steps may spawn";
                    else invalid = wf_->prepare_spawn(wf, step, u.spawn(), exp);
                }
                c_.exec("UPDATE jobs SET outputs = coalesce(($2::jsonb) -> 'outputs', '[]'::jsonb) WHERE job_id = $1",
                        job_id, json);
                upsert_locations(node_id, json, "outputs", "", now_ms);
                if (!invalid.empty()) {
                    // Outputs are kept; the job fails for good.
                    const std::string err = "invalid spawn: " + invalid;
                    end_attempt("FAILED", err);
                    set_job(tv1::FAILED, err);
                    engine(tv1::FAILED, err, nullptr);
                    out.result = UpdateResult::kInvalid;
                    out.message = err;
                    out.job = load_job(job_id, false);
                    return out;
                }
                end_attempt("SUCCEEDED", "");
                c_.exec("UPDATE jobs SET state = 'COMPLETED', last_error = '', progress_fraction = 1, updated_at = " TS(2)
                        " WHERE job_id = $1",
                        job_id, now_ms);
                engine(tv1::COMPLETED, "", &exp);
                break;
            }
            case tv1::FAILED: {
                const std::string err = u.error().empty() ? std::string("failed") : u.error();
                end_attempt("FAILED", err);
                const bool retry = u.retryable() && used < max_attempts && !cancel_requested;
                set_job(retry ? tv1::QUEUED : tv1::FAILED, err);  // RETRY_PENDING -> QUEUED at once (no backoff yet)
                if (!retry) engine(tv1::FAILED, err, nullptr);
                break;
            }
            case tv1::CANCELLED: {
                end_attempt("CANCELLED", u.error());
                c_.exec("UPDATE jobs SET state = 'CANCELLED',"
                        " last_error = CASE WHEN $2 = '' THEN last_error ELSE $2 END, updated_at = " TS(3)
                        " WHERE job_id = $1 RETURNING last_error",
                        job_id, u.error(), now_ms);
                engine(tv1::CANCELLED, u.error().empty() ? std::string("cancelled") : u.error(), nullptr);
                break;
            }
            default: {  // EXPIRED, VALIDATION_FAILED: terminal
                const std::string err = u.error().empty() ? state_name(u.state()) : u.error();
                end_attempt("FAILED", err);
                set_job(u.state(), err);
                engine(u.state(), err, nullptr);
                break;
            }
        }
        out.result = UpdateResult::kOk;
        out.message.clear();
        out.job = load_job(job_id, false);
        return out;
    });
}

std::vector<cv1::LeaseRef> JobStore::renew_leases(const std::string& node_id, const std::vector<cv1::LeaseRef>& leases,
                                                  std::int64_t ttl_ms, std::int64_t now_ms) {
    if (leases.empty()) return {};
    return transact(c_, [&] {
        std::vector<cv1::LeaseRef> revoked;
        for (const auto& l : leases) {
            auto r = c_.exec(
                "UPDATE job_attempts a SET expires_at = " TS(5) " FROM jobs j"
                " WHERE a.attempt_id = $2 AND a.job_id = $1 AND a.fencing_token = $3::bigint AND a.node_id = $4"
                "   AND a.state IN ('LEASED', 'RUNNING')"
                "   AND j.job_id = a.job_id AND j.current_attempt_id = a.attempt_id"
                "   AND j.state IN ('LEASED', 'RUNNING') AND NOT j.cancel_requested",
                l.job_id(), l.attempt_id(), l.fencing_token(), node_id, now_ms + ttl_ms);
            if (r.affected() != 1) revoked.push_back(l);
        }
        return revoked;
    });
}

std::vector<ExpiredLease> JobStore::expire_leases(std::int64_t now_ms) {
    return transact(c_, [&] {
        std::vector<ExpiredLease> out;
        // Candidates without locks; each is then locked with SKIP LOCKED (the
        // workflow row first, lock order), so this never waits: a job (or
        // workflow) busy elsewhere is looked at again next sweep.
        auto cand = c_.exec(
            "SELECT j.job_id, coalesce(j.workflow_id, ''), coalesce(j.step, '')"
            " FROM jobs j JOIN job_attempts a ON a.attempt_id = j.current_attempt_id"
            " WHERE j.state IN ('LEASED', 'RUNNING') AND a.expires_at < " TS(1) " ORDER BY a.expires_at, j.job_id",
            now_ms);
        for (int k = 0; k < cand.rows(); ++k) {
            const std::string job_id = cand.str(k, 0), wf = cand.str(k, 1), step = cand.str(k, 2);
            if (!wf.empty() && !wf_->lock(wf, true)) continue;
            auto r = c_.exec(
                "SELECT j.job_id, a.attempt_id, a.node_id, j.cancel_requested, j.attempts_used, j.max_attempts"
                " FROM jobs j JOIN job_attempts a ON a.attempt_id = j.current_attempt_id"
                " WHERE j.job_id = $1 AND j.state IN ('LEASED', 'RUNNING') AND a.expires_at < " TS(2)
                " FOR UPDATE OF j SKIP LOCKED",
                job_id, now_ms);
            if (r.rows() != 1) continue;
            // Re-check on the latest row version: a renewal may have committed
            // after this statement's snapshot.
            auto a = c_.exec("UPDATE job_attempts SET state = 'ABANDONED', error = 'lease expired', ended_at = " TS(2)
                             " WHERE attempt_id = $1 AND state IN ('LEASED', 'RUNNING') AND expires_at < " TS(2),
                             r.str(0, 1), now_ms);
            if (a.affected() != 1) continue;
            ExpiredLease e{r.str(0, 0), r.str(0, 1), r.str(0, 2), tv1::FAILED};
            if (r.str(0, 3) == "t") e.new_state = tv1::CANCELLED;
            else if (r.u64(0, 4) < r.u64(0, 5)) e.new_state = tv1::QUEUED;
            c_.exec("UPDATE jobs SET state = $2, last_error = 'lease expired', updated_at = " TS(3) "," QUEUED_AT(3)
                    " WHERE job_id = $1",
                    e.job_id, state_name(e.new_state), now_ms);
            if (e.new_state == tv1::QUEUED) refresh_strict(e.job_id);
            else if (!wf.empty()) wf_->job_terminal(wf, step, e.new_state, "lease expired", nullptr, now_ms);
            out.push_back(std::move(e));
        }
        return out;
    });
}

// ---- artifact locations ----------------------------------------------------------

// `json` holds an array of ArtifactRef (proto3 JSON) under `key`.
void JobStore::upsert_locations(const std::string& node_id, const std::string& json, const char* key,
                                const std::string& sync_id, std::int64_t now_ms) {
    c_.exec("INSERT INTO artifact_locations (artifact_id, node_id, size, media_type, producer_job, created_at, seen_at,"
            " sync_id)"
            " SELECT DISTINCT ON (x.artifact_id) x.artifact_id, $1, coalesce(x.size, 0), coalesce(x.media_type, ''),"
            "   coalesce(x.producer_job, ''),"
            "   CASE WHEN coalesce(x.created_unix_ms, 0) = 0 THEN " TS(4) " ELSE to_timestamp(x.created_unix_ms / 1000.0) END,"
            "   " TS(4) ", $5"
            " FROM jsonb_to_recordset(coalesce(($2::jsonb) -> ($3::text), '[]'::jsonb))"
            "   AS x(artifact_id text, size numeric, media_type text, producer_job text, created_unix_ms bigint)"
            " WHERE coalesce(x.artifact_id, '') <> ''"
            " ORDER BY x.artifact_id"
            " ON CONFLICT (artifact_id, node_id) DO UPDATE SET size = EXCLUDED.size, media_type = EXCLUDED.media_type,"
            "   producer_job = CASE WHEN EXCLUDED.producer_job = '' THEN artifact_locations.producer_job"
            "                       ELSE EXCLUDED.producer_job END,"
            "   seen_at = EXCLUDED.seen_at, sync_id = EXCLUDED.sync_id",
            node_id, json, key, now_ms, sync_id);
}

bool JobStore::announce_artifacts(const cv1::ArtifactAnnouncement& a, std::int64_t now_ms) {
    cv1::ArtifactAnnouncement carrier;
    *carrier.mutable_artifacts() = a.artifacts();
    const std::string json = to_json(carrier);
    return transact(c_, [&] {
        if (c_.exec("SELECT 1 FROM nodes WHERE node_id = $1 FOR SHARE", a.node_id()).rows() != 1) return false;
        if (!a.sync_id().empty()) {
            // First page of a new sync remembers when it started.
            c_.exec("INSERT INTO artifact_syncs (node_id, sync_id, started_at) VALUES ($1, $2, " TS(3) ")"
                    " ON CONFLICT (node_id) DO UPDATE SET sync_id = EXCLUDED.sync_id, started_at = EXCLUDED.started_at"
                    " WHERE artifact_syncs.sync_id <> EXCLUDED.sync_id",
                    a.node_id(), a.sync_id(), now_ms);
        }
        upsert_locations(a.node_id(), json, "artifacts", a.sync_id(), now_ms);
        if (a.full()) {
            // Drop rows neither seen in this sync nor announced (incrementally)
            // since it started. A full page without sync_id is a one-page sync.
            c_.exec("DELETE FROM artifact_locations WHERE node_id = $1 AND ($2 = '' OR sync_id <> $2)"
                    " AND seen_at < coalesce((SELECT started_at FROM artifact_syncs WHERE node_id = $1 AND sync_id = $2),"
                    " " TS(3) ")",
                    a.node_id(), a.sync_id(), now_ms);
            c_.exec("DELETE FROM artifact_syncs WHERE node_id = $1", a.node_id());
        }
        return true;
    });
}

std::optional<cv1::ArtifactInfo> JobStore::load_artifact(const std::string& artifact_id) {
    auto r = c_.exec("SELECT l.artifact_id, l.size::text, l.media_type, l.producer_job, " MS("l.created_at")
                     ", l.node_id FROM artifact_locations l JOIN nodes n ON n.node_id = l.node_id"
                     " WHERE l.artifact_id = $1 ORDER BY n.name, l.node_id",
                     artifact_id);
    const bool pinned = c_.exec("SELECT 1 FROM artifact_pins WHERE artifact_id = $1", artifact_id).rows() == 1;
    if (r.rows() == 0 && !pinned) return std::nullopt;
    cv1::ArtifactInfo info;
    info.set_pinned(pinned);
    auto* a = info.mutable_artifact();
    a->set_artifact_id(artifact_id);
    if (r.rows() == 0) return info;
    a->set_size(r.u64(0, 1));
    a->set_media_type(r.str(0, 2));
    a->set_created_unix_ms(r.i64(0, 4));
    for (int i = 0; i < r.rows(); ++i) {
        if (a->producer_job().empty()) a->set_producer_job(r.str(i, 3));
        info.add_node_ids(r.str(i, 5));
    }
    return info;
}

std::optional<cv1::ArtifactInfo> JobStore::get_artifact(const std::string& artifact_id) {
    // Two reads: one (read-only) transaction for a consistent view.
    return transact(c_, [&] { return load_artifact(artifact_id); });
}

void JobStore::lock_artifact(const std::string& artifact_id) {
    c_.exec("SELECT pg_advisory_xact_lock($1::int, hashtext($2))", kArtifactLockClass, artifact_id);
}

cv1::ReleaseReply JobStore::release_artifact(const std::string& artifact_id, const std::string& node_id, bool force,
                                             std::int64_t /*now_ms*/) {
    return transact(c_, [&] {
        // The advisory lock serializes releases of this artifact, including a
        // node without a row of its own (nothing to lock FOR UPDATE). Statements
        // after it see every release committed before it was granted.
        lock_artifact(artifact_id);
        c_.exec("SELECT 1 FROM artifact_locations WHERE artifact_id = $1 FOR UPDATE", artifact_id);
        cv1::ReleaseReply out;
        auto drop = [&] {
            c_.exec("DELETE FROM artifact_locations WHERE artifact_id = $1 AND node_id = $2", artifact_id, node_id);
            out.set_released(true);
        };
        if (force) {
            drop();
            return out;
        }
        if (c_.exec("SELECT 1 FROM artifact_pins WHERE artifact_id = $1", artifact_id).rows() == 1) {
            out.set_reason("pinned");
            return out;
        }
        // Another copy must be reachable soon: held by a node that isn't OFFLINE
        // (an OFFLINE node may never come back, so its copy doesn't count).
        if (c_.exec("SELECT 1 FROM artifact_locations l JOIN nodes h ON h.node_id = l.node_id "
                    "WHERE l.artifact_id = $1 AND l.node_id <> $2 AND h.health <> 'OFFLINE' LIMIT 1",
                    artifact_id, node_id)
                .rows() == 0) {
            out.set_reason("last copy");
            return out;
        }
        drop();
        return out;
    });
}

cv1::ArtifactInfo JobStore::set_artifact_pinned(const std::string& artifact_id, bool pinned, const std::string& actor,
                                                std::int64_t now_ms) {
    return transact(c_, [&] {
        lock_artifact(artifact_id);
        if (pinned)
            c_.exec("INSERT INTO artifact_pins (artifact_id, pinned_by, pinned_at) VALUES ($1, $2, " TS(3) ")"
                    " ON CONFLICT (artifact_id) DO NOTHING",
                    artifact_id, actor, now_ms);
        else
            c_.exec("DELETE FROM artifact_pins WHERE artifact_id = $1", artifact_id);
        if (auto info = load_artifact(artifact_id)) return *info;
        cv1::ArtifactInfo info;  // unpinned and held nowhere
        info.mutable_artifact()->set_artifact_id(artifact_id);
        return info;
    });
}

// ---- ranked placement (phase6.md §7) ----------------------------------------------

namespace {

std::vector<std::string> json_strings(const std::string& json) {
    cv1::RegisterRequest carrier;
    from_json("{\"capabilities\":" + (json.empty() ? std::string("[]") : json) + "}", &carrier);
    return {carrier.capabilities().begin(), carrier.capabilities().end()};
}

std::uint64_t sat_sub(std::uint64_t a, std::uint64_t b) { return a > b ? a - b : 0; }

}  // namespace

std::vector<Reservation> JobStore::place_jobs(const PlacementOptions& o, std::int64_t now_ms) {
    strict_bytes_ = o.strict_input_bytes;
    return transact(c_, [&] {
        std::vector<Reservation> out;
        // Strict QUEUED jobs not yet released, without a live reservation (or past max_reserve).
        auto c = c_.exec(
            "SELECT j.job_id, encode(j.spec, 'hex'), (j.campaign_id IS NULL OR c.state = 'ACTIVE'),"
            " " MS("coalesce(j.queued_at, j.created_at)") ", to_jsonb(j.reserve_tried)::text,"
            " coalesce(j.checkpoint->>'artifact_id', ''), j.cpu_cores, j.memory_bytes::text, j.gpu_count"
            " FROM jobs j LEFT JOIN campaigns c ON c.campaign_id = j.campaign_id"
            " WHERE j.state = 'QUEUED' AND j.strict AND NOT j.reserve_done"
            "   AND (j.reserved_until IS NULL OR j.reserved_until <= " TS(1)
            "        OR coalesce(j.queued_at, j.created_at) <= " TS(2) ")"
            " ORDER BY j.created_at, j.seq LIMIT 200 FOR UPDATE OF j SKIP LOCKED",
            now_ms, now_ms - o.max_reserve_ms);
        if (c.rows() == 0) return out;

        struct Node {
            std::string id;
            std::vector<std::string> caps;
            cv1::Heartbeat hb;
            bool has_hb = false;
            std::uint32_t max_jobs = 0;
        };
        std::vector<Node> nodes;
        auto nr = c_.exec(
            "SELECT n.node_id, n.capabilities::text, coalesce(n.last_heartbeat::text, ''), p.max_jobs"
            " FROM nodes n JOIN node_policies p ON p.node_id = n.node_id WHERE n.health = 'ONLINE'"
            " ORDER BY n.name, n.node_id");
        std::uint64_t max_cores = 0;
        for (int i = 0; i < nr.rows(); ++i) {
            Node n;
            n.id = nr.str(i, 0);
            n.caps = json_strings(nr.str(i, 1));
            if (!nr.str(i, 2).empty()) {
                from_json(nr.str(i, 2), &n.hb);
                n.has_hb = true;
            }
            n.max_jobs = static_cast<std::uint32_t>(nr.u64(i, 3));
            max_cores = std::max<std::uint64_t>(max_cores, n.hb.capacity().cpu_cores());
            nodes.push_back(std::move(n));
        }

        // Sizes and ONLINE holders of every input / checkpoint involved.
        std::vector<tv1::JobSpec> specs;
        std::vector<std::string> all_ids;
        for (int i = 0; i < c.rows(); ++i) {
            specs.push_back(parse_spec(c.str(i, 1)));
            for (const auto& a : specs.back().input_artifacts()) all_ids.push_back(a);
            if (!c.str(i, 5).empty()) all_ids.push_back(c.str(i, 5));
        }
        struct Loc {
            std::uint64_t size = 0;
            std::set<std::string> holders;  // ONLINE
        };
        std::map<std::string, Loc> locs;
        if (!all_ids.empty()) {
            std::string arr = "{";
            for (std::size_t i = 0; i < all_ids.size(); ++i) arr += (i ? ",\"" : "\"") + all_ids[i] + "\"";
            arr += "}";
            auto lr = c_.exec("SELECT l.artifact_id, l.node_id, l.size::text, n.health FROM artifact_locations l"
                              " JOIN nodes n ON n.node_id = l.node_id WHERE l.artifact_id = ANY($1::text[])",
                              arr);
            for (int i = 0; i < lr.rows(); ++i) {
                auto& l = locs[lr.str(i, 0)];
                l.size = std::max(l.size, lr.u64(i, 2));
                if (lr.str(i, 3) == "ONLINE") l.holders.insert(lr.str(i, 1));
            }
        }

        for (int i = 0; i < c.rows(); ++i) {
            const std::string job_id = c.str(i, 0);
            const auto& spec = specs[static_cast<std::size_t>(i)];
            auto release = [&] {
                c_.exec("UPDATE jobs SET reserved_node = NULL, reserved_until = NULL, reserve_done = true"
                        " WHERE job_id = $1",
                        job_id);
            };
            if (c.i64(i, 3) <= now_ms - o.max_reserve_ms) {
                release();
                continue;
            }
            if (c.str(i, 2) != "t") continue;  // campaign not ACTIVE: nobody may lease it yet

            std::set<std::string> ids(spec.input_artifacts().begin(), spec.input_artifacts().end());
            if (!c.str(i, 5).empty()) ids.insert(c.str(i, 5));
            bool fetchable = true;
            std::uint64_t total = 0;
            for (const auto& a : spec.input_artifacts()) {
                auto it = locs.find(a);
                if (it == locs.end() || it->second.holders.empty()) fetchable = false;
            }
            for (const auto& a : ids)
                if (auto it = locs.find(a); it != locs.end()) total += it->second.size;
            if (!fetchable) {  // no node could fetch the inputs: ranking can't help
                release();
                continue;
            }

            const auto tried_v = json_strings(c.str(i, 4));
            const std::set<std::string> tried(tried_v.begin(), tried_v.end());
            const std::uint64_t jc = c.u64(i, 6), jm = c.u64(i, 7), jg = c.u64(i, 8);
            const Node* best = nullptr;
            double best_cost = 0;
            for (const auto& n : nodes) {
                if (!n.has_hb) continue;
                if (!std::ranges::all_of(spec.required_capabilities(),
                                         [&](const std::string& cap) { return std::ranges::count(n.caps, cap) > 0; }))
                    continue;
                const auto& cap = n.hb.capacity();
                const auto& al = n.hb.allocated();
                const std::uint64_t fc = sat_sub(cap.cpu_cores(), al.cpu_cores());
                const std::uint64_t fm = sat_sub(cap.memory_bytes(), al.memory_bytes());
                const std::uint64_t fg = sat_sub(cap.gpu_count(), al.gpu_count());
                if (jc > fc || jg > fg || (cap.memory_bytes() > 0 && jm > fm)) continue;
                if (tried.contains(n.id)) continue;

                std::uint64_t missing = 0;
                for (const auto& a : ids)
                    if (auto it = locs.find(a); it != locs.end() && !it->second.holders.contains(n.id))
                        missing += it->second.size;
                const double transfer = total > 0 ? static_cast<double>(missing) / static_cast<double>(total) : 0.0;
                const double slots = std::max<double>(1.0, n.max_jobs > 0 ? n.max_jobs : cap.cpu_cores());
                const double queue =
                    std::min(1.0, static_cast<double>(n.hb.running_jobs() + n.hb.queued_jobs()) / slots);
                const double avail = static_cast<double>(n.hb.memory_available_bytes());
                const double memory = avail > 0 ? std::min(1.0, static_cast<double>(jm) / avail) : (jm > 0 ? 1.0 : 0.0);
                const double contention = std::clamp(n.hb.cpu_utilization(), 0.0, 1.0);
                const double compute =
                    max_cores > 0 ? 1.0 - static_cast<double>(cap.cpu_cores()) / static_cast<double>(max_cores) : 0.0;
                const double cost = o.w_compute * compute + o.w_transfer * transfer + o.w_queue * queue +
                                    o.w_memory * memory + o.w_contention * contention;
                if (!best || cost < best_cost) {
                    best = &n;
                    best_cost = cost;
                }
            }
            if (!best) {
                // Every eligible node had its window, or none is eligible: anyone may lease it.
                release();
                continue;
            }
            const std::int64_t until = now_ms + o.reserve_window_ms;
            c_.exec("UPDATE jobs SET reserved_node = $2, reserved_until = " TS(3)
                    ", reserve_tried = array_append(reserve_tried, $2) WHERE job_id = $1",
                    job_id, best->id, until);
            out.push_back(Reservation{job_id, best->id, best_cost, until});
        }
        return out;
    });
}

#undef QUEUED_AT
#undef TS
#undef MS

}  // namespace tdr::cpdb
