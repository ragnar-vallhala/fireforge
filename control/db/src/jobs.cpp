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

#include "materials.hpp"
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
    " reserve_done = CASE WHEN $2 = 'QUEUED' THEN false ELSE reserve_done END,"                         \
    " pending_reason = CASE WHEN $2 = 'QUEUED' THEN '' ELSE pending_reason END"

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
    " coalesce(j.reserved_node, ''), coalesce(" MS("j.reserved_until") ", 0), j.pending_reason, j.is_gang";

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
    if (j.state() == tv1::QUEUED) j.set_pending_reason(r.str(row, 19));
    return j;
}

bool row_is_gang(const Result& r, int row) { return r.str(row, 20) == "t"; }

cv1::GangRole parse_role(const std::string& s) { return s == "HEAD" ? cv1::GANG_HEAD : cv1::GANG_MEMBER; }

// SELECT columns for GangMember rows: node_id, name, member_index, rank_offset, ranks, role, state.
cv1::GangMember member_from(const Result& r, int row, int c = 0) {
    cv1::GangMember m;
    m.set_node_id(r.str(row, c));
    m.set_name(r.str(row, c + 1));
    m.set_member_index(static_cast<std::uint32_t>(r.u64(row, c + 2)));
    m.set_rank_offset(static_cast<std::uint32_t>(r.u64(row, c + 3)));
    m.set_ranks(static_cast<std::uint32_t>(r.u64(row, c + 4)));
    m.set_role(parse_role(r.str(row, c + 5)));
    m.set_state(r.str(row, c + 6));
    return m;
}

// A gang attempt's expiry is its head member's (members are renewed one by one).
constexpr const char* kAttemptCols =
    "attempt_id, number, fencing_token, node_id, state, error, " MS("leased_at") ", " MS(
        "coalesce((SELECT m.expires_at FROM job_attempt_members m WHERE m.attempt_id = job_attempts.attempt_id"
        " AND m.member_index = 0), expires_at)") ", coalesce(" MS("ended_at") ", 0)";

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
            " workflow_id, step, is_gang)"
            " VALUES ($1, 'QUEUED', $2::bytea, $3, $4::bigint, $5::numeric, $6::bigint, $7::jsonb, $8::jsonb,"
            " $9::int, $10, " TS(11) ", " TS(11) ", " TS(11) ", $12, $13, $14::boolean)",
            spec.job_id(), detail::to_bytea_hex(raw), opt(spec.campaign_id()), std::max<std::uint32_t>(res.cpu_cores(), 1),
            res.memory_bytes(), res.gpu_count(), detail::string_array_json(spec.required_capabilities()),
            detail::string_array_json(spec.input_artifacts()), std::max<std::uint32_t>(spec.policy().max_attempts(), 1),
            submitted_by, now_ms, opt(spec.workflow_id()), opt(spec.step()), spec.mpi().ranks() > 0);
    refresh_strict(spec.job_id());
    return spec.job_id();
}

void JobStore::refresh_strict(const std::string& job_id) {
    // Input sizes as known now (largest announced copy); the checkpoint counts as an input.
    // Gang jobs are never strict: place_jobs places them as gangs (phase7.md §3).
    c_.exec("UPDATE jobs j SET strict = NOT j.is_gang AND (j.gpu_count > 0 OR coalesce((SELECT sum(t.sz) FROM"
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
    const bool gang = row_is_gang(r, 0);
    if (gang) fill_gang(j);
    if (with_attempts) {
        auto a = c_.exec(std::string("SELECT ") + kAttemptCols + " FROM job_attempts WHERE job_id = $1 ORDER BY number",
                         job_id);
        for (int i = 0; i < a.rows(); ++i) {
            auto* at = j.add_attempts();
            *at = attempt_from(a, i);
            if (gang)
                for (auto& m : attempt_members(at->attempt_id())) *at->add_members() = std::move(m);
        }
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
        for (int i = 0; i < r.rows(); ++i) {
            out.push_back(job_from(r, i));
            if (row_is_gang(r, i)) fill_gang(out.back());
        }
        return out;
    });
}

// ---- gangs (phase7.md §3, §4) ---------------------------------------------------

std::vector<cv1::GangMember> JobStore::attempt_members(const std::string& attempt_id) {
    auto r = c_.exec("SELECT m.node_id, coalesce(n.name, ''), m.member_index, m.rank_offset, m.ranks, m.role, m.state"
                     " FROM job_attempt_members m LEFT JOIN nodes n ON n.node_id = m.node_id"
                     " WHERE m.attempt_id = $1 ORDER BY m.member_index",
                     attempt_id);
    std::vector<cv1::GangMember> out;
    for (int i = 0; i < r.rows(); ++i) out.push_back(member_from(r, i));
    return out;
}

void JobStore::fill_gang(cv1::Job& j) {
    j.clear_gang();
    if (j.state() == tv1::QUEUED) {
        // READY: the member's last poll had room for its hold (ready_at is cleared otherwise).
        auto r = c_.exec("SELECT h.node_id, coalesce(n.name, ''), h.member_index, h.rank_offset, h.ranks,"
                         " CASE WHEN h.member_index = 0 THEN 'HEAD' ELSE 'MEMBER' END,"
                         " CASE WHEN h.ready_at IS NULL THEN 'HELD' ELSE 'READY' END"
                         " FROM gang_holds h LEFT JOIN nodes n ON n.node_id = h.node_id"
                         " WHERE h.job_id = $1 ORDER BY h.member_index",
                         j.job_id());
        for (int i = 0; i < r.rows(); ++i) *j.add_gang() = member_from(r, i);
        return;
    }
    if (j.current_attempt_id().empty()) return;
    for (auto& m : attempt_members(j.current_attempt_id())) *j.add_gang() = std::move(m);
}

std::string JobStore::resume_checkpoint(const std::string& job_id) {
    // Resume only from a checkpoint some ONLINE node can serve (phase6.md §6).
    auto r = c_.exec("SELECT CASE WHEN j.checkpoint IS NOT NULL AND EXISTS (SELECT 1 FROM artifact_locations l"
                     "   JOIN nodes h ON h.node_id = l.node_id"
                     "   WHERE l.artifact_id = j.checkpoint->>'artifact_id' AND h.health = 'ONLINE')"
                     " THEN j.checkpoint->>'artifact_id' ELSE '' END FROM jobs j WHERE j.job_id = $1",
                     job_id);
    return r.rows() == 1 ? r.str(0, 0) : std::string();
}

cv1::Lease JobStore::gang_lease(const std::string& job_id, const std::string& attempt_id, std::uint32_t number,
                                const tv1::JobSpec& spec, const std::string& node_id, std::int64_t expires_ms) {
    cv1::Lease l;
    l.set_job_id(job_id);
    l.set_attempt_id(attempt_id);
    l.set_fencing_token(number);
    l.set_attempt_number(number);
    *l.mutable_spec() = spec;
    l.mutable_spec()->set_resume_checkpoint(resume_checkpoint(job_id));
    l.set_expires_unix_ms(expires_ms);
    auto* g = l.mutable_gang();
    g->set_ranks(spec.mpi().ranks());
    for (auto& m : attempt_members(attempt_id)) {
        if (m.member_index() == 0) g->set_head_node_id(m.node_id());
        if (m.node_id() == node_id) {
            g->set_role(m.role());
            g->set_member_index(m.member_index());
        }
        *g->add_members() = std::move(m);
    }
    return l;
}

namespace {
// Per-member resources: ranks x the per-rank request (cpu 0 counts as 1).
struct Need {
    std::uint64_t cpu = 0, mem = 0, gpu = 0;
};
Need member_need(const tv1::JobSpec& spec, std::uint64_t ranks) {
    const auto& r = spec.resources();
    return {ranks * std::max<std::uint64_t>(r.cpu_cores(), 1), ranks * r.memory_bytes(), ranks * r.gpu_count()};
}
}  // namespace

template <class Take>
void JobStore::deliver_members(const std::string& node_id, std::int64_t expires_ms, std::vector<cv1::Lease>& out,
                               Take&& take) {
    // SKIP LOCKED: a job busy elsewhere is delivered on the next poll (never waits,
    // so it cannot deadlock with the readiness pass).
    auto d = c_.exec("SELECT m.attempt_id, a.job_id, a.number, encode(j.spec, 'hex'), m.ranks"
                     " FROM job_attempt_members m JOIN job_attempts a ON a.attempt_id = m.attempt_id"
                     " JOIN jobs j ON j.job_id = a.job_id"
                     " WHERE m.node_id = $1 AND NOT m.delivered AND m.state = 'LEASED'"
                     "   AND j.current_attempt_id = m.attempt_id AND j.state IN ('LEASED', 'RUNNING')"
                     "   AND NOT j.cancel_requested"
                     " ORDER BY j.created_at, j.seq FOR UPDATE OF j SKIP LOCKED",
                     node_id);
    for (int i = 0; i < d.rows(); ++i) {
        const std::string attempt_id = d.str(i, 0), job_id = d.str(i, 1);
        auto u = c_.exec("UPDATE job_attempt_members SET delivered = true, expires_at = " TS(3)
                         " WHERE attempt_id = $1 AND node_id = $2 AND NOT delivered AND state = 'LEASED'",
                         attempt_id, node_id, expires_ms);
        if (u.affected() != 1) continue;
        const auto spec = parse_spec(d.str(i, 3));
        const auto need = member_need(spec, d.u64(i, 4));
        take(need.cpu, need.mem, need.gpu);
        out.push_back(gang_lease(job_id, attempt_id, static_cast<std::uint32_t>(d.u64(i, 2)), spec, node_id, expires_ms));
    }
}

cv1::Lease JobStore::create_gang_attempt(const std::string& job_id, std::uint32_t number, const tv1::JobSpec& spec,
                                         const std::string& caller, std::int64_t now_ms, std::int64_t expires_ms) {
    const std::string attempt_id = short_id("att-");
    auto h = c_.exec("SELECT node_id FROM gang_holds WHERE job_id = $1 AND member_index = 0", job_id);
    if (h.rows() != 1) throw DbError("gang " + job_id + " has no head hold");
    const std::string head = h.str(0, 0);
    c_.exec("INSERT INTO job_attempts (attempt_id, job_id, number, fencing_token, node_id, state, leased_at,"
            " expires_at) VALUES ($1, $2, $3::int, $3::bigint, $4, 'LEASED', " TS(5) ", " TS(6) ")",
            attempt_id, job_id, number, head, now_ms, expires_ms);
    c_.exec("INSERT INTO job_attempt_members (attempt_id, node_id, member_index, rank_offset, ranks, role, state,"
            " delivered, expires_at)"
            " SELECT $1, h.node_id, h.member_index, h.rank_offset, h.ranks,"
            "   CASE WHEN h.member_index = 0 THEN 'HEAD' ELSE 'MEMBER' END, 'LEASED', h.node_id = $3, " TS(4)
            " FROM gang_holds h WHERE h.job_id = $2",
            attempt_id, job_id, caller, expires_ms);
    c_.exec("UPDATE jobs SET state = 'LEASED', attempts_used = $2::int, current_attempt_id = $3, node_id = $4,"
            " progress_fraction = 0, progress_message = '', reserved_node = NULL, reserved_until = NULL,"
            " pending_reason = '', gang_tried = '{}', updated_at = " TS(5) " WHERE job_id = $1",
            job_id, number, attempt_id, head, now_ms);
    c_.exec("DELETE FROM gang_holds WHERE job_id = $1", job_id);
    return gang_lease(job_id, attempt_id, number, spec, caller, expires_ms);
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
            c_.exec("DELETE FROM gang_holds WHERE job_id = $1", job_id);
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
    return transact(c_, [&] {
        std::vector<cv1::Lease> out;
        auto n = c_.exec("SELECT health, capabilities::text FROM nodes WHERE node_id = $1", req.node_id());
        if (n.rows() != 1 || n.str(0, 0) != "ONLINE") return out;
        // Gang eligibility (phase7.md §3): this node takes remote work.
        c_.exec("UPDATE nodes SET last_lease_poll = " TS(2) " WHERE node_id = $1", req.node_id(), now_ms);

        const auto sub = [](std::uint64_t a, std::uint64_t b) { return a > b ? a - b : 0; };
        // What is left of req.free / max_jobs after gang leases handed out in this call.
        std::uint64_t cpu = req.free().cpu_cores();
        std::uint64_t mem = req.free().memory_bytes();
        std::uint64_t gpu = req.free().gpu_count();
        std::uint32_t slots = req.max_jobs();
        const std::int64_t expires = now_ms + ttl_ms;
        auto take = [&](std::uint64_t c, std::uint64_t m, std::uint64_t g) {
            cpu = sub(cpu, c);
            mem = sub(mem, m);
            gpu = sub(gpu, g);
            if (slots > 0) --slots;
        };

        // 1. Member leases of gang attempts created by another member's poll. They
        //    were accounted for when this member became ready, so they are handed
        //    out even without a free slot.
        deliver_members(req.node_id(), expires, out, take);

        // 2. Readiness of this node's holds, oldest gang first. A hold counts as
        //    ready when what is left of req.free (and a job slot) covers it.
        auto h = c_.exec("SELECT h.job_id, h.cpu, h.memory::text, h.gpus FROM gang_holds h"
                         " JOIN jobs j ON j.job_id = h.job_id"
                         " WHERE h.node_id = $1 AND j.state = 'QUEUED' ORDER BY j.created_at, j.seq",
                         req.node_id());
        std::uint64_t rc = cpu, rm = mem, rg = gpu;
        std::uint32_t rs = slots;
        for (int i = 0; i < h.rows(); ++i) {
            const std::string job_id = h.str(i, 0);
            const auto hc = h.u64(i, 1), hm = h.u64(i, 2), hg = h.u64(i, 3);
            // Hold rows are only written under their job's row lock (place_jobs
            // drops holds of jobs it has locked), so this cannot deadlock with it.
            if (rs == 0 || hc > rc || hm > rm || hg > rg) {
                // Not ready (any more). Best effort: skipped if the job is busy elsewhere.
                c_.exec("UPDATE gang_holds SET ready_at = NULL WHERE job_id = $1 AND node_id = $2 AND ready_at IS NOT NULL"
                        " AND EXISTS (SELECT 1 FROM jobs WHERE job_id = $1 AND state = 'QUEUED' FOR UPDATE SKIP LOCKED)",
                        job_id, req.node_id());
                continue;
            }
            // The job row serializes members polling at the same time: exactly one
            // of them creates the attempt; the others see it LEASED (and get their
            // member lease in pass 3 below).
            auto j = c_.exec("SELECT attempts_used, encode(spec, 'hex') FROM jobs"
                             " WHERE job_id = $1 AND state = 'QUEUED' AND is_gang FOR UPDATE",
                             job_id);
            if (j.rows() != 1) continue;
            if (c_.exec("UPDATE gang_holds SET ready_at = " TS(3) " WHERE job_id = $1 AND node_id = $2", job_id,
                        req.node_id(), now_ms)
                    .affected() != 1)
                continue;  // the hold was dropped meanwhile
            rc -= hc;
            rm -= hm;
            rg -= hg;
            --rs;
            auto ready = c_.exec("SELECT count(*) FILTER (WHERE ready_at IS NULL OR ready_at < " TS(2) ")"
                                 " FROM gang_holds WHERE job_id = $1",
                                 job_id, now_ms - std::max<std::int64_t>(gang_.ready_window_ms, 0));
            if (ready.i64(0, 0) != 0) continue;
            const auto number = static_cast<std::uint32_t>(j.u64(0, 0) + 1);
            out.push_back(create_gang_attempt(job_id, number, parse_spec(j.str(0, 1)), req.node_id(), now_ms, expires));
            take(hc, hm, hg);
        }

        // 3. Member leases of attempts another member created while pass 2 waited.
        deliver_members(req.node_id(), expires, out, take);

        // 4. Ordinary jobs, from what is left minus every hold on this node (drain).
        auto held = c_.exec("SELECT coalesce(sum(h.cpu), 0), coalesce(sum(h.memory), 0)::text, coalesce(sum(h.gpus), 0)"
                            " FROM gang_holds h JOIN jobs j ON j.job_id = h.job_id"
                            " WHERE h.node_id = $1 AND j.state = 'QUEUED'",
                            req.node_id());
        cpu = sub(cpu, held.u64(0, 0));
        mem = sub(mem, held.u64(0, 1));
        gpu = sub(gpu, held.u64(0, 2));
        if (slots == 0 || cpu == 0) return out;

        // Eligibility in SQL (phase4.md §3, phase5.md §4, phase6.md §7): QUEUED,
        // not a gang, campaign absent or ACTIVE, capabilities subset, fits the free
        // capacity on its own, not reserved for another node (live reservation), and either
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
            " WHERE j.state = 'QUEUED' AND NOT j.is_gang"
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
            req.node_id(), n.str(0, 1), cpu, mem, gpu, kMaxLeaseScan,
            now_ms - std::max<std::int64_t>(locality_wait_ms, 0), now_ms);
#undef MINE
#undef ON_ONLINE

        for (int i = 0; i < c.rows() && slots > 0; ++i) {
            // cpu_cores is stored as max(1, requested). memory 0 means "node
            // default" (applied by the node), so it always fits here.
            const auto jc = c.u64(i, 1), jm = c.u64(i, 2), jg = c.u64(i, 3);
            if (jc > cpu || jm > mem || jg > gpu) continue;  // misfit: try the next one
            cpu -= jc;
            mem -= jm;
            gpu -= jg;
            --slots;

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
            " j.attempts_used, j.max_attempts, j.cancel_requested, j.is_gang"
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
        const bool gang = r.str(0, 7) == "t";

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
        // Gangs (phase7.md §4): fenced by membership of the current attempt.
        int member = -1;
        std::string member_name;
        if (gang) {
            auto m = c_.exec("SELECT m.member_index, m.state, coalesce(n.name, m.node_id) FROM job_attempt_members m"
                             " LEFT JOIN nodes n ON n.node_id = m.node_id"
                             " WHERE m.attempt_id = $1 AND m.node_id = $2 FOR UPDATE OF m",
                             cur_attempt, node_id);
            if (m.rows() != 1) {
                out.message = std::format("attempt {} is not held by {}", u.attempt_id(), node_id);
                return out;
            }
            if (m.str(0, 1) != "LEASED" && m.str(0, 1) != "RUNNING") {
                out.message = std::format("member {} of attempt {} is already {}", m.str(0, 2), u.attempt_id(),
                                          m.str(0, 1));
                return out;
            }
            member = static_cast<int>(m.i64(0, 0));
            member_name = m.str(0, 2);
        } else if (holder != node_id) {
            out.message = std::format("attempt {} is not held by {}", u.attempt_id(), node_id);
            return out;
        }

        // Records come with the COMPLETED update only (phase8.md §2).
        if (!u.records().empty() && u.state() != tv1::COMPLETED) {
            out.result = UpdateResult::kInvalid;
            out.message = "records are allowed only with a COMPLETED update";
            return out;
        }

        // Ends the attempt. Gang: the reporting member takes the attempt's state,
        // every other live member is revoked (its node learns via renew_leases).
        auto end_attempt = [&](const char* state, const std::string& error) {
            c_.exec("UPDATE job_attempts SET state = $2, error = $3, ended_at = " TS(4) " WHERE attempt_id = $1",
                    cur_attempt, state, error, now_ms);
            if (gang)
                c_.exec("UPDATE job_attempt_members SET state = CASE WHEN node_id = $2 THEN $3 ELSE 'CANCELLED' END,"
                        " error = CASE WHEN node_id = $2 THEN $4 ELSE $5 END"
                        " WHERE attempt_id = $1 AND state IN ('LEASED', 'RUNNING')",
                        cur_attempt, node_id, state, error,
                        member == 0 ? std::format("revoked: head {} reported {}", member_name, state)
                                    : std::format("revoked: member {} failed", member_name));
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

        if (gang && u.state() == tv1::RUNNING)
            c_.exec("UPDATE job_attempt_members SET state = 'RUNNING' WHERE attempt_id = $1 AND node_id = $2"
                    " AND state = 'LEASED'",
                    cur_attempt, node_id);
        if (gang && member != 0) {
            // A non-head member: it never decides success; its failure fails the attempt.
            switch (u.state()) {
                case tv1::RUNNING: break;  // progress and checkpoints come from the head
                case tv1::COMPLETED:       // outputs come from the head only
                    c_.exec("UPDATE job_attempt_members SET state = 'SUCCEEDED', error = '' WHERE attempt_id = $1"
                            " AND node_id = $2",
                            cur_attempt, node_id);
                    break;
                default: {
                    const std::string merr = u.error().empty() ? state_name(u.state()) : u.error();
                    c_.exec("UPDATE job_attempt_members SET state = $3, error = $4 WHERE attempt_id = $1 AND node_id = $2",
                            cur_attempt, node_id, u.state() == tv1::CANCELLED ? "CANCELLED" : "FAILED", merr);
                    const std::string err = std::format("member {} failed: {}", member_name, merr);
                    end_attempt("FAILED", err);
                    if (cancel_requested) {
                        c_.exec("UPDATE jobs SET state = 'CANCELLED', updated_at = " TS(2) " WHERE job_id = $1", job_id,
                                now_ms);
                        engine(tv1::CANCELLED, "cancelled", nullptr);
                        break;
                    }
                    // Retryable unless the member said otherwise (a cancelled member
                    // job, e.g. killed locally, is retried).
                    const bool retryable = u.state() == tv1::FAILED ? u.retryable() : u.state() == tv1::CANCELLED;
                    const bool retry = retryable && used < max_attempts;
                    set_job(retry ? tv1::QUEUED : tv1::FAILED, err);
                    if (!retry) engine(tv1::FAILED, err, nullptr);
                    break;
                }
            }
            out.result = UpdateResult::kOk;
            out.message.clear();
            out.job = load_job(job_id, false);
            return out;
        }

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
                    if (!invalid.empty()) invalid = "invalid spawn: " + invalid;
                }
                // Records (phase8.md §2), validated before anything is written.
                MaterialStore::Context rctx;
                if (invalid.empty() && !u.records().empty()) {
                    rctx.job_id = job_id;
                    rctx.attempt_id = cur_attempt;
                    rctx.workflow_id = wf;
                    rctx.step = step;
                    rctx.node_id = node_id;
                    rctx.campaign_id =
                        c_.exec("SELECT coalesce(campaign_id, '') FROM jobs WHERE job_id = $1", job_id).str(0, 0);
                    for (const auto& o : carrier.outputs())
                        if (!o.name().empty()) rctx.outputs.try_emplace(o.name(), o.artifact_id());
                    if (auto e = mat_->validate(u.records(), rctx); !e.empty()) invalid = "invalid records: " + e;
                }
                c_.exec("UPDATE jobs SET outputs = coalesce(($2::jsonb) -> 'outputs', '[]'::jsonb) WHERE job_id = $1",
                        job_id, json);
                upsert_locations(node_id, json, "outputs", "", now_ms);
                if (!invalid.empty()) {
                    // Outputs are kept; the job fails for good (nothing spawned, no records).
                    const std::string& err = invalid;
                    end_attempt("FAILED", err);
                    set_job(tv1::FAILED, err);
                    engine(tv1::FAILED, err, nullptr);
                    out.result = UpdateResult::kInvalid;
                    out.message = err;
                    out.job = load_job(job_id, false);
                    return out;
                }
                end_attempt("SUCCEEDED", "");
                if (!u.records().empty()) mat_->store(u.records(), rctx, now_ms);
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
            // Gang attempt (phase7.md §4): each member renews its own row; a member
            // that is done, or of an attempt no longer current, is revoked.
            if (c_.exec("SELECT 1 FROM job_attempt_members WHERE attempt_id = $1 LIMIT 1", l.attempt_id()).rows() > 0) {
                auto g = c_.exec(
                    "UPDATE job_attempt_members m SET expires_at = " TS(5) " FROM job_attempts a, jobs j"
                    " WHERE m.attempt_id = $2 AND m.node_id = $4 AND m.state IN ('LEASED', 'RUNNING') AND m.delivered"
                    "   AND a.attempt_id = m.attempt_id AND a.job_id = $1 AND a.fencing_token = $3::bigint"
                    "   AND a.state IN ('LEASED', 'RUNNING')"
                    "   AND j.job_id = a.job_id AND j.current_attempt_id = a.attempt_id"
                    "   AND j.state IN ('LEASED', 'RUNNING') AND NOT j.cancel_requested",
                    l.job_id(), l.attempt_id(), l.fencing_token(), node_id, now_ms + ttl_ms);
                if (g.affected() != 1) revoked.push_back(l);
                continue;
            }
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
            " WHERE j.state IN ('LEASED', 'RUNNING') AND NOT j.is_gang AND a.expires_at < " TS(1)
            " ORDER BY a.expires_at, j.job_id",
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

        // Gang attempts (phase7.md §4): any member's lease expiring fails the whole
        // attempt; the other members are revoked.
        auto gc = c_.exec(
            "SELECT DISTINCT j.job_id, coalesce(j.workflow_id, ''), coalesce(j.step, ''), j.created_at, j.seq"
            " FROM jobs j JOIN job_attempt_members m ON m.attempt_id = j.current_attempt_id"
            " WHERE j.is_gang AND j.state IN ('LEASED', 'RUNNING') AND m.state IN ('LEASED', 'RUNNING')"
            "   AND m.expires_at < " TS(1) " ORDER BY j.created_at, j.seq",
            now_ms);
        for (int k = 0; k < gc.rows(); ++k) {
            const std::string job_id = gc.str(k, 0), wf = gc.str(k, 1), step = gc.str(k, 2);
            if (!wf.empty() && !wf_->lock(wf, true)) continue;
            auto r = c_.exec("SELECT j.current_attempt_id, j.cancel_requested, j.attempts_used, j.max_attempts"
                             " FROM jobs j WHERE j.job_id = $1 AND j.state IN ('LEASED', 'RUNNING')"
                             " FOR UPDATE OF j SKIP LOCKED",
                             job_id);
            if (r.rows() != 1) continue;
            const std::string attempt = r.str(0, 0);
            // Atomic re-check against renewals committed meanwhile.
            auto x = c_.exec("UPDATE job_attempt_members m SET state = 'ABANDONED', error = 'lease expired'"
                             " FROM (SELECT member_index FROM job_attempt_members WHERE attempt_id = $1"
                             "       AND state IN ('LEASED', 'RUNNING') AND expires_at < " TS(2) " FOR UPDATE) e"
                             " WHERE m.attempt_id = $1 AND m.member_index = e.member_index"
                             " RETURNING m.node_id, m.member_index",
                             attempt, now_ms);
            if (x.rows() == 0) continue;
            int first = 0;
            for (int i = 1; i < x.rows(); ++i)
                if (x.i64(i, 1) < x.i64(first, 1)) first = i;
            const std::string dead = x.str(first, 0);
            auto nm = c_.exec("SELECT name FROM nodes WHERE node_id = $1", dead);
            const std::string err =
                std::format("member {} failed: lease expired", nm.rows() == 1 ? nm.str(0, 0) : dead);
            c_.exec("UPDATE job_attempts SET state = 'ABANDONED', error = $2, ended_at = " TS(3) " WHERE attempt_id = $1",
                    attempt, err, now_ms);
            c_.exec("UPDATE job_attempt_members SET state = 'CANCELLED', error = $2"
                    " WHERE attempt_id = $1 AND state IN ('LEASED', 'RUNNING')",
                    attempt, "revoked: " + err);
            ExpiredLease e{job_id, attempt, dead, tv1::FAILED};
            if (r.str(0, 1) == "t") e.new_state = tv1::CANCELLED;
            else if (r.u64(0, 2) < r.u64(0, 3)) e.new_state = tv1::QUEUED;
            c_.exec("UPDATE jobs SET state = $2, last_error = $4, updated_at = " TS(3) "," QUEUED_AT(3)
                    " WHERE job_id = $1",
                    e.job_id, state_name(e.new_state), now_ms, err);
            if (e.new_state != tv1::QUEUED && !wf.empty()) wf_->job_terminal(wf, step, e.new_state, err, nullptr, now_ms);
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

// Phase 6 cost (phase6.md §7) of running `jm` bytes of memory on a node; lower is better.
double node_cost(const PlacementOptions& o, const cv1::Heartbeat& hb, std::uint32_t max_jobs, std::uint64_t max_cores,
                 std::uint64_t jm, double transfer) {
    const auto& cap = hb.capacity();
    const double slots = std::max<double>(1.0, max_jobs > 0 ? max_jobs : cap.cpu_cores());
    const double queue = std::min(1.0, static_cast<double>(hb.running_jobs() + hb.queued_jobs()) / slots);
    const double avail = static_cast<double>(hb.memory_available_bytes());
    const double memory = avail > 0 ? std::min(1.0, static_cast<double>(jm) / avail) : (jm > 0 ? 1.0 : 0.0);
    const double contention = std::clamp(hb.cpu_utilization(), 0.0, 1.0);
    const double compute =
        max_cores > 0 ? 1.0 - static_cast<double>(cap.cpu_cores()) / static_cast<double>(max_cores) : 0.0;
    return o.w_compute * compute + o.w_transfer * transfer + o.w_queue * queue + o.w_memory * memory +
           o.w_contention * contention;
}

std::string join_names(const std::vector<std::string>& v) {
    std::string out;
    for (const auto& s : v) out += (out.empty() ? "" : ", ") + s;
    return out;
}

}  // namespace

std::vector<Reservation> JobStore::place_jobs(const PlacementOptions& o, std::int64_t now_ms) {
    strict_bytes_ = o.strict_input_bytes;
    return transact(c_, [&] {
        std::vector<Reservation> out;
        // Holds outlive their job only by accident (e.g. a workflow cancel): drop them.
        c_.exec("DELETE FROM gang_holds h USING jobs j WHERE h.job_id = j.job_id AND j.state <> 'QUEUED'");
        // QUEUED gang jobs (phase7.md §3).
        auto g = c_.exec(
            "SELECT j.job_id, encode(j.spec, 'hex'), (j.campaign_id IS NULL OR c.state = 'ACTIVE'),"
            " to_jsonb(j.gang_tried)::text, coalesce(j.checkpoint->>'artifact_id', '')"
            " FROM jobs j LEFT JOIN campaigns c ON c.campaign_id = j.campaign_id"
            " WHERE j.state = 'QUEUED' AND j.is_gang"
            " ORDER BY j.created_at, j.seq LIMIT 200 FOR UPDATE OF j SKIP LOCKED");
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
        if (g.rows() == 0 && c.rows() == 0) return out;

        struct Node {
            std::string id, name;
            std::vector<std::string> caps;
            cv1::Heartbeat hb;
            bool has_hb = false;
            std::uint32_t max_jobs = 0;
            std::int64_t last_poll_ms = 0;
            std::set<std::string> connected;  // peers its last heartbeat reports CONNECTED
        };
        std::vector<Node> nodes;
        std::map<std::string, std::size_t> node_at;
        auto nr = c_.exec(
            "SELECT n.node_id, n.capabilities::text, coalesce(n.last_heartbeat::text, ''), p.max_jobs, n.name,"
            " coalesce(" MS("n.last_lease_poll") ", 0)"
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
            n.name = nr.str(i, 4);
            n.last_poll_ms = nr.i64(i, 5);
            for (const auto& p : n.hb.peers())
                if (p.state() == "CONNECTED") n.connected.insert(p.node_id());
            max_cores = std::max<std::uint64_t>(max_cores, n.hb.capacity().cpu_cores());
            node_at[n.id] = nodes.size();
            nodes.push_back(std::move(n));
        }
        auto has_caps = [](const Node& n, const tv1::JobSpec& spec) {
            return std::ranges::all_of(spec.required_capabilities(),
                                       [&](const std::string& cap) { return std::ranges::count(n.caps, cap) > 0; });
        };

        // Sizes and ONLINE holders of every input / checkpoint involved.
        std::vector<tv1::JobSpec> gspecs, specs;
        std::vector<std::string> all_ids;
        for (int i = 0; i < g.rows(); ++i) {
            gspecs.push_back(parse_spec(g.str(i, 1)));
            for (const auto& a : gspecs.back().input_artifacts()) all_ids.push_back(a);
            if (!g.str(i, 4).empty()) all_ids.push_back(g.str(i, 4));
        }
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
        // Missing bytes / total bytes of `ids` on node_id.
        auto transfer_of = [&](const std::set<std::string>& ids, const std::string& node_id) {
            std::uint64_t total = 0, missing = 0;
            for (const auto& a : ids)
                if (auto it = locs.find(a); it != locs.end()) {
                    total += it->second.size;
                    if (!it->second.holders.contains(node_id)) missing += it->second.size;
                }
            return total > 0 ? static_cast<double>(missing) / static_cast<double>(total) : 0.0;
        };

        // Capacity held by gangs, per node (kept current while gangs are placed below).
        struct Held {
            std::uint64_t cpu = 0, mem = 0, gpu = 0;
        };
        std::map<std::string, Held> held;
        struct HoldRow {
            std::string node_id, name;
            std::uint64_t cpu = 0, mem = 0, gpu = 0;
            std::int64_t created_ms = 0, ready_ms = 0;
        };
        std::map<std::string, std::vector<HoldRow>> holds;  // job_id -> by member_index
        auto hr = c_.exec("SELECT h.job_id, h.node_id, coalesce(n.name, h.node_id), h.cpu, h.memory::text, h.gpus, " MS(
            "h.created_at") ", coalesce(" MS("h.ready_at") ", 0)"
                          " FROM gang_holds h JOIN jobs j ON j.job_id = h.job_id LEFT JOIN nodes n ON n.node_id = h.node_id"
                          " WHERE j.state = 'QUEUED' ORDER BY h.job_id, h.member_index");
        for (int i = 0; i < hr.rows(); ++i) {
            HoldRow h{hr.str(i, 1), hr.str(i, 2), hr.u64(i, 3), hr.u64(i, 4), hr.u64(i, 5), hr.i64(i, 6), hr.i64(i, 7)};
            auto& t = held[h.node_id];
            t.cpu += h.cpu;
            t.mem += h.mem;
            t.gpu += h.gpu;
            holds[hr.str(i, 0)].push_back(std::move(h));
        }

        // ---- gangs (phase7.md §3), before strict jobs -------------------------------
        const std::int64_t drain_s = gang_.drain_timeout_ms / 1000;
        for (int i = 0; i < g.rows(); ++i) {
            const std::string job_id = g.str(i, 0);
            const auto& spec = gspecs[static_cast<std::size_t>(i)];
            auto set_reason = [&](const std::string& why) {
                c_.exec("UPDATE jobs SET pending_reason = $2 WHERE job_id = $1 AND pending_reason <> $2", job_id, why);
            };
            auto tried_v = json_strings(g.str(i, 3));
            std::set<std::string> tried(tried_v.begin(), tried_v.end());
            auto& mine = holds[job_id];
            auto drop_holds = [&](bool mark_tried) {
                if (mark_tried) {
                    c_.exec("UPDATE jobs SET gang_tried = ARRAY(SELECT DISTINCT x FROM unnest(gang_tried ||"
                            " ARRAY(SELECT node_id FROM gang_holds WHERE job_id = $1)) AS x ORDER BY x)"
                            " WHERE job_id = $1",
                            job_id);
                    for (const auto& h : mine) tried.insert(h.node_id);
                }
                c_.exec("DELETE FROM gang_holds WHERE job_id = $1", job_id);
                for (const auto& h : mine) {
                    auto& t = held[h.node_id];
                    t.cpu = sat_sub(t.cpu, h.cpu);
                    t.mem = sat_sub(t.mem, h.mem);
                    t.gpu = sat_sub(t.gpu, h.gpu);
                }
                mine.clear();
            };
            if (g.str(i, 2) != "t") {  // campaign not ACTIVE: nobody may lease it yet
                if (!mine.empty()) drop_holds(false);
                set_reason("campaign is not ACTIVE");
                continue;
            }
            if (!mine.empty()) {
                std::int64_t created = mine.front().created_ms;
                bool gone = false;
                for (const auto& h : mine) {
                    created = std::min(created, h.created_ms);
                    if (!node_at.contains(h.node_id)) gone = true;
                }
                if (gone) {
                    drop_holds(false);  // a member is no longer ONLINE: pick again now
                } else if (now_ms - created >= gang_.drain_timeout_ms) {
                    drop_holds(true);  // drain timeout: prefer another set
                } else {
                    std::vector<std::string> waiting, all;
                    for (const auto& h : mine) {
                        all.push_back(h.name);
                        if (h.ready_ms == 0) waiting.push_back(h.name);
                    }
                    set_reason(std::format("draining {} ({} s of {} s)", join_names(waiting.empty() ? all : waiting),
                                           (now_ms - created) / 1000, drain_s));
                    continue;
                }
            }

            // Slots per eligible node, from heartbeat capacity minus other gangs' holds.
            const std::uint32_t ranks = spec.mpi().ranks();
            const auto& res = spec.resources();
            const std::uint64_t pc = std::max<std::uint64_t>(res.cpu_cores(), 1), pm = res.memory_bytes(),
                                pg = res.gpu_count();
            const std::uint64_t cap_ranks = spec.mpi().max_ranks_per_node() > 0 ? spec.mpi().max_ranks_per_node() : ranks;
            std::set<std::string> ids(spec.input_artifacts().begin(), spec.input_artifacts().end());
            if (!g.str(i, 4).empty()) ids.insert(g.str(i, 4));
            struct Cand {
                const Node* n;
                std::uint32_t slots;
                double cost;
            };
            std::vector<Cand> elig;
            for (const auto& n : nodes) {
                if (!n.has_hb || !has_caps(n, spec)) continue;
                if (n.last_poll_ms == 0 || n.last_poll_ms < now_ms - gang_.poll_recent_ms) continue;
                const auto& cap = n.hb.capacity();
                const Held h = held.contains(n.id) ? held[n.id] : Held{};
                std::uint64_t slots = std::min<std::uint64_t>(sat_sub(cap.cpu_cores(), h.cpu) / pc, cap_ranks);
                if (pm > 0 && cap.memory_bytes() > 0) slots = std::min(slots, sat_sub(cap.memory_bytes(), h.mem) / pm);
                if (pg > 0) slots = std::min(slots, sat_sub(cap.gpu_count(), h.gpu) / pg);
                if (slots == 0) continue;
                const auto here = std::min<std::uint64_t>(slots, ranks);
                elig.push_back({&n, static_cast<std::uint32_t>(slots),
                                node_cost(o, n.hb, n.max_jobs, max_cores, here * pm, transfer_of(ids, n.id))});
            }
            std::ranges::stable_sort(elig, [](const Cand& a, const Cand& b) {
                if (a.slots != b.slots) return a.slots > b.slots;
                return a.cost < b.cost;
            });
            auto linked = [](const Node& a, const Node& b) {
                return a.connected.contains(b.id) && b.connected.contains(a.id);
            };
            // Greedy cover: from each possible head in order, add nodes linked to every
            // node chosen so far until the slots cover the ranks.
            std::uint64_t best = 0;
            auto cover = [&](bool untried_only) -> std::vector<std::size_t> {
                const auto max_nodes = std::max<std::size_t>(gang_.max_nodes, 1);
                for (std::size_t s = 0; s < elig.size(); ++s) {
                    if (untried_only && tried.contains(elig[s].n->id)) continue;
                    std::vector<std::size_t> chosen{s};
                    std::uint64_t got = elig[s].slots;
                    for (std::size_t k = 0; k < elig.size() && got < ranks && chosen.size() < max_nodes; ++k) {
                        if (k == s || (untried_only && tried.contains(elig[k].n->id))) continue;
                        if (!std::ranges::all_of(chosen, [&](std::size_t x) { return linked(*elig[x].n, *elig[k].n); }))
                            continue;
                        chosen.push_back(k);
                        got += elig[k].slots;
                    }
                    best = std::max(best, std::min<std::uint64_t>(got, ranks));
                    if (got >= ranks) return chosen;
                }
                return {};
            };
            std::vector<std::size_t> chosen;
            if (!tried.empty()) chosen = cover(true);
            if (chosen.empty()) chosen = cover(false);
            if (chosen.empty()) {
                set_reason(std::format("not enough capacity: {} of {} ranks on {} eligible nodes", best, ranks,
                                       elig.size()));
                continue;
            }
            std::uint32_t offset = 0;
            std::vector<std::string> names;
            for (std::size_t m = 0; m < chosen.size(); ++m) {
                const auto& cand = elig[chosen[m]];
                const std::uint32_t here = std::min(cand.slots, ranks - offset);
                const auto need = member_need(spec, here);
                c_.exec("INSERT INTO gang_holds (job_id, node_id, member_index, rank_offset, ranks, cpu, memory, gpus,"
                        " created_at) VALUES ($1, $2, $3::int, $4::int, $5::int, $6::bigint, $7::numeric, $8::bigint, " TS(9)
                        ")",
                        job_id, cand.n->id, static_cast<std::int64_t>(m), offset, here, need.cpu, need.mem, need.gpu,
                        now_ms);
                auto& t = held[cand.n->id];
                t.cpu += need.cpu;
                t.mem += need.mem;
                t.gpu += need.gpu;
                names.push_back(cand.n->name);
                offset += here;
            }
            set_reason(std::format("draining {} (0 s of {} s)", join_names(names), drain_s));
        }

        // ---- strict jobs (phase6.md §7) ---------------------------------------------
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
            for (const auto& a : spec.input_artifacts()) {
                auto it = locs.find(a);
                if (it == locs.end() || it->second.holders.empty()) fetchable = false;
            }
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
                if (!n.has_hb || !has_caps(n, spec)) continue;
                const auto& cap = n.hb.capacity();
                const auto& al = n.hb.allocated();
                // Free from the last heartbeat, minus capacity held for gangs (phase7.md §3).
                const Held h = held.contains(n.id) ? held[n.id] : Held{};
                const std::uint64_t fc = sat_sub(sat_sub(cap.cpu_cores(), al.cpu_cores()), h.cpu);
                const std::uint64_t fm = sat_sub(sat_sub(cap.memory_bytes(), al.memory_bytes()), h.mem);
                const std::uint64_t fg = sat_sub(sat_sub(cap.gpu_count(), al.gpu_count()), h.gpu);
                if (jc > fc || jg > fg || (cap.memory_bytes() > 0 && jm > fm)) continue;
                if (tried.contains(n.id)) continue;
                const double cost = node_cost(o, n.hb, n.max_jobs, max_cores, jm, transfer_of(ids, n.id));
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
