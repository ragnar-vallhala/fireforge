// WorkflowStore: workflows, steps, readiness, run_if rules, input wiring and
// dynamic fan-out (docs/design/phase6.md §3-§5).
//
// Step states in workflow_steps.state: WAITING (no job yet), ACTIVE (its job
// exists and is not terminal), then the job's terminal state, or SKIPPED /
// CANCELLED / FAILED without a job. Displayed state: the job's state while ACTIVE.
//
// Settledness (§3) is O(1): open_desc counts a step's spawned descendants that
// are not terminal (kept up to date along the ancestor chain, depth <= 8), so X
// is settled iff X is terminal and open_desc = 0. The subtree of X is the
// name range ['X/', 'X0') (names are COLLATE "C", static names have no '/').
#include "workflows.hpp"

#include <algorithm>
#include <format>
#include <map>
#include <set>
#include <unordered_map>
#include <unordered_set>

#include "jobs.hpp"
#include "util.hpp"

namespace tdr::cpdb {

namespace {

using detail::clamp_limit;
using detail::from_json;
using detail::short_id;
using detail::to_json;
using pg::transact;

#define TS TDR_TS
#define MS TDR_MS

// Subtree predicate on alias s for the step name bound at $n (X itself or a descendant).
#define SUBTREE(n) \
    "(s.name = $" #n " OR (s.name >= ($" #n " || '/') COLLATE \"C\" AND s.name < ($" #n " || '0') COLLATE \"C\"))"

bool terminal_step(const std::string& s) {
    return s == "COMPLETED" || s == "FAILED" || s == "CANCELLED" || s == "EXPIRED" || s == "VALIDATION_FAILED" ||
           s == "SKIPPED";
}
bool failure_step(const std::string& s) {
    return s == "FAILED" || s == "CANCELLED" || s == "EXPIRED" || s == "VALIDATION_FAILED";
}

// [A-Za-z0-9_.-]{1,64}
bool valid_step_name(const std::string& n) {
    if (n.empty() || n.size() > 64) return false;
    return std::all_of(n.begin(), n.end(),
                       [](unsigned char c) { return std::isalnum(c) || c == '_' || c == '.' || c == '-'; });
}

bool valid_artifact_id(const std::string& id) {
    static constexpr std::string_view p = "artifact://sha256/";
    if (!id.starts_with(p) || id.size() != p.size() + 64) return false;
    return std::all_of(id.begin() + static_cast<std::ptrdiff_t>(p.size()), id.end(),
                       [](char c) { return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'); });
}

// "a/b/c" -> {"a", "a/b"}
std::vector<std::string> ancestors(const std::string& name) {
    std::vector<std::string> out;
    for (std::size_t p = name.find('/'); p != std::string::npos; p = name.find('/', p + 1))
        out.push_back(name.substr(0, p));
    return out;
}

// PostgreSQL text[] literal.
std::string text_array(const std::vector<std::string>& v) {
    std::string out = "{";
    for (std::size_t i = 0; i < v.size(); ++i) {
        if (i) out += ',';
        out += '"';
        for (char ch : v[i]) {
            if (ch == '"' || ch == '\\') out += '\\';
            out += ch;
        }
        out += '"';
    }
    return out + "}";
}

std::string json_str(const std::string& s) {
    std::string out = "\"";
    for (unsigned char ch : s) {
        switch (ch) {
            case '"': out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\n': out += "\\n"; break;
            case '\r': out += "\\r"; break;
            case '\t': out += "\\t"; break;
            default:
                if (ch < 0x20) out += std::format("\\u{:04x}", ch);
                else out += static_cast<char>(ch);
        }
    }
    return out + "\"";
}

// JobSpec template rules shared by create_workflow and spawns (§3).
std::string check_template(const std::string& step, const tv1::JobSpec& j) {
    if (j.command().argv().empty() || j.command().argv(0).empty())
        return std::format("step {}: job.command.argv must not be empty", step);
    if (!j.job_id().empty() || !j.campaign_id().empty() || !j.workflow_id().empty() || !j.step().empty() ||
        !j.resume_checkpoint().empty())
        return std::format("step {}: job template must not set job_id, campaign_id, workflow_id, step or "
                           "resume_checkpoint",
                           step);
    for (const auto& a : j.input_artifacts())
        if (!valid_artifact_id(a))
            return std::format("step {}: input artifact '{}' is not artifact://sha256/<64 hex>", step, a);
    if (j.input_names_size() > j.input_artifacts_size())
        return std::format("step {}: more input_names than input_artifacts", step);
    return {};
}

std::string check_common(const std::string& step, const tv1::StepSpec& s) {
    if (!tv1::RunIf_IsValid(s.run_if())) return std::format("step {}: unknown run_if", step);
    return check_template(step, s.job());
}

// Kahn's algorithm over `deps` restricted to `nodes`. Returns a node on a cycle, or "".
std::string find_cycle(const std::vector<std::string>& nodes,
                       const std::unordered_map<std::string, std::vector<std::string>>& deps) {
    std::unordered_map<std::string, int> indeg;
    std::unordered_map<std::string, std::vector<std::string>> rev;
    for (const auto& n : nodes) indeg[n] = 0;
    for (const auto& n : nodes) {
        auto it = deps.find(n);
        if (it == deps.end()) continue;
        for (const auto& d : it->second)
            if (indeg.contains(d)) {
                ++indeg[n];
                rev[d].push_back(n);
            }
    }
    std::vector<std::string> q;
    for (const auto& n : nodes)
        if (indeg[n] == 0) q.push_back(n);
    std::size_t done = 0;
    while (!q.empty()) {
        auto n = q.back();
        q.pop_back();
        ++done;
        for (const auto& m : rev[n])
            if (--indeg[m] == 0) q.push_back(m);
    }
    if (done == nodes.size()) return {};
    for (const auto& n : nodes)
        if (indeg[n] > 0) return n;
    return {};
}

std::vector<cv1::ArtifactRef> parse_outputs(const std::string& json) {
    cv1::JobUpdate carrier;
    from_json("{\"outputs\":" + json + "}", &carrier);
    return {carrier.outputs().begin(), carrier.outputs().end()};
}

std::string spec_json(const tv1::StepSpec& s) { return to_json(s); }

tv1::StepSpec parse_step_spec(const std::string& json) {
    tv1::StepSpec s;
    from_json(json, &s);
    return s;
}

// Displayed step state: the job's state while the step is ACTIVE.
constexpr const char* kStepStateExpr = "CASE WHEN s.state = 'ACTIVE' AND j.state IS NOT NULL THEN j.state ELSE s.state END";

constexpr const char* kWfCols =
    "w.workflow_id, w.name, coalesce(w.campaign_id, ''), w.state, w.submitted_by, " MS("w.created_at") ", " MS(
        "w.updated_at") ", coalesce(" MS("w.finished_at") ", 0), w.error";

cv1::Workflow wf_from(const pg::Result& r, int i) {
    cv1::Workflow w;
    w.set_workflow_id(r.str(i, 0));
    w.set_name(r.str(i, 1));
    w.set_campaign_id(r.str(i, 2));
    w.set_state(r.str(i, 3));
    w.set_submitted_by(r.str(i, 4));
    w.set_created_unix_ms(r.i64(i, 5));
    w.set_updated_unix_ms(r.i64(i, 6));
    w.set_finished_unix_ms(r.i64(i, 7));
    w.set_error(r.str(i, 8));
    return w;
}

}  // namespace

// ---- rows -----------------------------------------------------------------------

bool WorkflowStore::lock(const std::string& id, bool skip_locked) {
    return c_.exec(skip_locked ? "SELECT 1 FROM workflows WHERE workflow_id = $1 FOR UPDATE SKIP LOCKED"
                               : "SELECT 1 FROM workflows WHERE workflow_id = $1 FOR UPDATE",
                   id)
               .rows() == 1;
}

std::optional<WorkflowStore::Wf> WorkflowStore::load_wf(const std::string& id) {
    auto r = c_.exec("SELECT state, coalesce(campaign_id, ''), submitted_by, error, step_count, open_steps"
                     " FROM workflows WHERE workflow_id = $1",
                     id);
    if (r.rows() != 1) return std::nullopt;
    Wf w;
    w.id = id;
    w.state = r.str(0, 0);
    w.campaign_id = r.str(0, 1);
    w.submitted_by = r.str(0, 2);
    w.error = r.str(0, 3);
    w.step_count = r.i64(0, 4);
    w.open_steps = r.i64(0, 5);
    return w;
}

void WorkflowStore::save_wf(const Run& r) {
    c_.exec("UPDATE workflows SET state = $2, error = $3, step_count = $4::int, open_steps = $5::int,"
            " updated_at = " TS(6) ","
            " finished_at = CASE WHEN $2 <> 'RUNNING' AND finished_at IS NULL THEN " TS(6) " ELSE finished_at END"
            " WHERE workflow_id = $1",
            r.wf.id, r.wf.state, r.wf.error, r.wf.step_count, r.wf.open_steps, r.now);
}

void WorkflowStore::insert_steps(Run& r, const std::vector<NewStep>& steps, bool eval_all) {
    if (steps.empty()) return;
    std::string rows = "[", deps = "[";
    bool first_dep = true;
    for (std::size_t i = 0; i < steps.size(); ++i) {
        const auto& s = steps[i];
        if (i) rows += ',';
        rows += std::format("{{\"name\":{},\"seq\":{},\"parent\":{},\"depth\":{},\"spec\":{}}}", json_str(s.name),
                            r.wf.step_count + static_cast<std::int64_t>(i) + 1,
                            s.parent.empty() ? std::string("null") : json_str(s.parent), s.depth, spec_json(s.spec));
        for (const auto& d : s.deps) {
            if (!first_dep) deps += ',';
            first_dep = false;
            deps += "[" + json_str(s.name) + "," + json_str(d) + "]";
        }
    }
    rows += "]";
    deps += "]";
    c_.exec("INSERT INTO workflow_steps (workflow_id, name, seq, parent_step, depth, spec, state, updated_at)"
            " SELECT $1, x->>'name', (x->>'seq')::int, x->>'parent', (x->>'depth')::int, x->'spec', 'WAITING', " TS(3)
            " FROM jsonb_array_elements($2::jsonb) x",
            r.wf.id, rows, r.now);
    c_.exec("INSERT INTO workflow_step_deps (workflow_id, step, dep)"
            " SELECT $1, x->>0, x->>1 FROM jsonb_array_elements($2::jsonb) x ON CONFLICT DO NOTHING",
            r.wf.id, deps);
    // Spawned steps share one parent: it and its ancestors gain open descendants.
    if (!steps[0].parent.empty()) {
        auto up = ancestors(steps[0].parent);
        up.push_back(steps[0].parent);
        c_.exec("UPDATE workflow_steps SET open_desc = open_desc + $3::int WHERE workflow_id = $1 AND name = ANY($2::text[])",
                r.wf.id, text_array(up), static_cast<std::int64_t>(steps.size()));
    }
    r.wf.step_count += static_cast<std::int64_t>(steps.size());
    r.wf.open_steps += static_cast<std::int64_t>(steps.size());
    r.changes += steps.size();
    for (const auto& s : steps)
        if (eval_all || s.deps.empty()) r.to_eval.push_back(s.name);
}

bool WorkflowStore::mark_terminal(Run& r, const std::string& name, const std::string& state, const std::string& error) {
    auto u = c_.exec("UPDATE workflow_steps SET state = $3, error = $4, updated_at = " TS(5)
                     " WHERE workflow_id = $1 AND name = $2 AND state IN ('WAITING', 'ACTIVE')"
                     " RETURNING coalesce((spec->>'allow_failure')::boolean, false)",
                     r.wf.id, name, state, error, r.now);
    if (u.rows() != 1) return false;  // already terminal (idempotent)
    const bool allow_failure = u.str(0, 0) == "t";
    if (auto up = ancestors(name); !up.empty())
        c_.exec("UPDATE workflow_steps SET open_desc = open_desc - 1 WHERE workflow_id = $1 AND name = ANY($2::text[])",
                r.wf.id, text_array(up));
    --r.wf.open_steps;
    if (failure_step(state) && !allow_failure && r.wf.error.empty())
        r.wf.error = std::format("step {} {}{}{}", name, state, error.empty() ? "" : ": ", error);
    r.finished.push_back(name);
    ++r.changes;
    return true;
}

void WorkflowStore::process(Run& r) {
    std::vector<std::string> first;
    first.swap(r.to_eval);
    for (const auto& n : first) evaluate(r, n);
    while (!r.finished.empty() && r.wf.state == "RUNNING") {
        const std::string x = r.finished.front();
        r.finished.pop_front();
        // Dependents of x, and of its ancestors (whose settledness may have changed).
        auto keys = ancestors(x);
        keys.push_back(x);
        auto d = c_.exec("SELECT DISTINCT s.name, s.seq FROM workflow_step_deps d"
                         " JOIN workflow_steps s ON s.workflow_id = d.workflow_id AND s.name = d.step"
                         " WHERE d.workflow_id = $1 AND d.dep = ANY($2::text[]) AND s.state = 'WAITING'"
                         " ORDER BY s.seq",
                         r.wf.id, text_array(keys));
        for (int i = 0; i < d.rows(); ++i) evaluate(r, d.str(i, 0));
    }
    r.finished.clear();
}

void WorkflowStore::evaluate(Run& r, const std::string& name) {
    if (r.wf.state != "RUNNING") return;
    auto row = c_.exec("SELECT state, spec::text FROM workflow_steps WHERE workflow_id = $1 AND name = $2", r.wf.id,
                       name);
    if (row.rows() != 1 || row.str(0, 0) != "WAITING") return;

    // Ready: every dependency settled.
    auto deps = c_.exec("SELECT x.name, x.state, x.open_desc FROM workflow_step_deps d"
                        " JOIN workflow_steps x ON x.workflow_id = d.workflow_id AND x.name = d.dep"
                        " WHERE d.workflow_id = $1 AND d.step = $2 ORDER BY x.seq",
                        r.wf.id, name);
    for (int i = 0; i < deps.rows(); ++i)
        if (!terminal_step(deps.str(i, 1)) || deps.i64(i, 2) != 0) return;

    const tv1::StepSpec spec = parse_step_spec(row.str(0, 1));

    // Rule over the dependencies and their spawned subtrees.
    bool all_ok = true, any_failed = false;
    for (int i = 0; i < deps.rows(); ++i) {
        auto st = c_.exec("SELECT DISTINCT s.state FROM workflow_steps s WHERE s.workflow_id = $1 AND " SUBTREE(2),
                          r.wf.id, deps.str(i, 0));
        for (int k = 0; k < st.rows(); ++k) {
            const auto s = st.str(k, 0);
            if (s != "COMPLETED") all_ok = false;
            if (failure_step(s)) any_failed = true;
        }
    }
    bool run = true;
    switch (spec.run_if()) {
        case tv1::ANY_FAILED: run = any_failed; break;
        case tv1::ALWAYS: run = true; break;
        default: run = all_ok; break;
    }
    if (!run) {
        mark_terminal(r, name, "SKIPPED", std::format("run_if {} not met", tv1::RunIf_Name(spec.run_if())));
        return;
    }

    // Wire inputs (§3).
    tv1::JobSpec job = spec.job();
    std::vector<std::string> names(job.input_names().begin(), job.input_names().end());
    names.resize(static_cast<std::size_t>(job.input_artifacts_size()));
    std::unordered_map<std::string, int> used;
    auto add = [&](const std::string& artifact, const std::string& want) {
        std::string n = want;
        if (!n.empty()) {
            auto [it, fresh] = used.try_emplace(n, 1);
            if (!fresh) {
                std::string cand;
                do cand = std::format("{}#{}", want, ++it->second);
                while (used.contains(cand));
                used.emplace(cand, 1);
                n = cand;
            }
        }
        job.add_input_artifacts(artifact);
        names.push_back(n);
    };
    for (const auto& n : names)
        if (!n.empty()) used.try_emplace(n, 1);
    for (const auto& in : spec.inputs()) {
        const bool all = in.output().empty() || in.output() == "*";
        auto src = c_.exec(std::string("SELECT s.name, j.outputs::text FROM workflow_steps s JOIN jobs j ON j.job_id = s.job_id"
                                       " WHERE s.workflow_id = $1 AND s.state = 'COMPLETED' AND ") +
                               (in.spawned() ? SUBTREE(2) : "s.name = $2") + " ORDER BY s.seq",
                           r.wf.id, in.step());
        std::size_t matched = 0;
        for (int i = 0; i < src.rows(); ++i)
            for (const auto& o : parse_outputs(src.str(i, 1))) {
                if (!all && o.name() != in.output()) continue;
                ++matched;
                add(o.artifact_id(), in.input_name().empty() ? o.name() : in.input_name());
            }
        // A named output must exist when its producer completed (with `spawned`: in
        // at least one completed step of the subtree). Producers that did not
        // complete (ANY_FAILED / ALWAYS steps) contribute nothing.
        if (!all && matched == 0 && src.rows() > 0) {
            mark_terminal(r, name, "FAILED", std::format("input {}.{} missing", in.step(), in.output()));
            return;
        }
    }
    // Trailing unnamed inputs need no names entry.
    while (!names.empty() && names.back().empty()) names.pop_back();
    job.clear_input_names();
    for (auto& n : names) job.add_input_names(n);
    job.set_campaign_id(r.wf.campaign_id);
    job.set_workflow_id(r.wf.id);
    job.set_step(name);
    job.clear_job_id();
    const std::string job_id = jobs_.insert_job(job, r.wf.submitted_by, r.now);
    c_.exec("UPDATE workflow_steps SET state = 'ACTIVE', job_id = $3, updated_at = " TS(4)
            " WHERE workflow_id = $1 AND name = $2",
            r.wf.id, name, job_id, r.now);
    ++r.changes;
}

void WorkflowStore::finish(Run& r) {
    if (r.wf.state == "RUNNING" && r.wf.open_steps <= 0) r.wf.state = r.wf.error.empty() ? "SUCCEEDED" : "FAILED";
}

// ---- create ---------------------------------------------------------------------------

std::expected<cv1::Workflow, std::string> WorkflowStore::create_workflow(const cv1::CreateWorkflowRequest& req,
                                                                         const std::string& submitted_by,
                                                                         std::int64_t now_ms) {
    if (req.steps().empty()) return std::unexpected("workflow has no steps");
    if (static_cast<std::size_t>(req.steps_size()) > limits_.max_steps)
        return std::unexpected(std::format("too many steps ({} > {})", req.steps_size(), limits_.max_steps));
    std::unordered_set<std::string> known;
    std::vector<std::string> order;
    for (const auto& s : req.steps()) {
        if (!valid_step_name(s.name()))
            return std::unexpected(std::format("step name '{}' must be 1-64 characters of [A-Za-z0-9_.-]", s.name()));
        if (!known.insert(s.name()).second) return std::unexpected("duplicate step name " + s.name());
        order.push_back(s.name());
    }
    std::vector<NewStep> steps;
    std::unordered_map<std::string, std::vector<std::string>> graph;
    for (const auto& s : req.steps()) {
        if (auto e = check_common(s.name(), s); !e.empty()) return std::unexpected(e);
        NewStep n;
        n.name = s.name();
        n.spec = s;
        std::vector<std::string> deps;
        for (const auto& d : s.depends_on()) {
            if (d == s.name()) return std::unexpected(std::format("step {} depends on itself", d));
            if (!known.contains(d)) return std::unexpected(std::format("step {}: unknown dependency {}", s.name(), d));
            deps.push_back(d);
        }
        for (const auto& in : s.inputs()) {
            if (in.step() == s.name()) return std::unexpected(std::format("step {} takes input from itself", s.name()));
            if (!known.contains(in.step()))
                return std::unexpected(std::format("step {}: input from unknown step '{}'", s.name(), in.step()));
            deps.push_back(in.step());
        }
        std::ranges::sort(deps);
        deps.erase(std::unique(deps.begin(), deps.end()), deps.end());
        n.deps = deps;
        graph[n.name] = std::move(deps);
        steps.push_back(std::move(n));
    }
    if (auto c = find_cycle(order, graph); !c.empty())
        return std::unexpected(std::format("dependency cycle through step {}", c));

    const std::string id = short_id("wf-");
    const std::optional<std::string> campaign =
        req.campaign_id().empty() ? std::nullopt : std::optional(req.campaign_id());
    try {
        return transact(c_, [&]() -> std::expected<cv1::Workflow, std::string> {
            if (campaign && c_.exec("SELECT 1 FROM campaigns WHERE campaign_id = $1", *campaign).rows() != 1)
                return std::unexpected("unknown campaign " + *campaign);
            c_.exec("INSERT INTO workflows (workflow_id, name, campaign_id, state, submitted_by, created_at, updated_at)"
                    " VALUES ($1, $2, $3, 'RUNNING', $4, " TS(5) ", " TS(5) ")",
                    id, req.name(), campaign, submitted_by, now_ms);
            Run r;
            r.wf = *load_wf(id);
            r.now = now_ms;
            insert_steps(r, steps, false);
            process(r);
            finish(r);
            save_wf(r);
            auto w = load_workflow(id, true);
            if (!w) throw DbError("create_workflow: workflow missing after insert");
            return *w;
        });
    } catch (const DbError& e) {
        if (e.sqlstate == "23503") return std::unexpected("unknown campaign " + req.campaign_id());
        throw;
    }
}

// ---- spawn (§5) -------------------------------------------------------------------------

std::string WorkflowStore::prepare_spawn(const std::string& wf_id, const std::string& step,
                                         const google::protobuf::RepeatedPtrField<tv1::StepSpec>& spawn, Expansion& out) {
    out.steps.clear();
    auto wf = load_wf(wf_id);
    if (!wf || wf->state != "RUNNING") return {};  // cancelled workflow: expansion dropped
    auto p = c_.exec("SELECT depth FROM workflow_steps WHERE workflow_id = $1 AND name = $2", wf_id, step);
    if (p.rows() != 1) return "spawning step " + step + " not found";
    const auto n = static_cast<std::size_t>(spawn.size());
    if (n > limits_.max_spawn) return std::format("too many steps in one expansion ({} > {})", n, limits_.max_spawn);
    if (static_cast<std::size_t>(wf->step_count) + n > limits_.max_steps)
        return std::format("workflow would exceed {} steps", limits_.max_steps);
    const int depth = static_cast<int>(p.i64(0, 0)) + 1;
    if (static_cast<std::size_t>(depth) > limits_.max_depth)
        return std::format("spawn depth {} exceeds {}", depth, limits_.max_depth);

    std::unordered_set<std::string> shorts;
    for (const auto& s : spawn) {
        if (!valid_step_name(s.name()))
            return std::format("step name '{}' must be 1-64 characters of [A-Za-z0-9_.-]", s.name());
        if (!shorts.insert(s.name()).second) return "duplicate step name " + s.name();
    }
    const auto full = [&](const std::string& s) { return step + "/" + s; };

    std::set<std::string> external;  // existing steps named by full name
    std::vector<std::string> fulls;
    std::unordered_map<std::string, std::vector<std::string>> sib;  // sibling deps (full names)
    for (const auto& s : spawn) {
        NewStep ns;
        ns.name = full(s.name());
        ns.parent = step;
        ns.depth = depth;
        ns.spec = s;
        ns.spec.set_name(ns.name);
        if (auto e = check_common(ns.name, s); !e.empty()) return e;
        // A short name that is also a sibling refers to the sibling.
        auto resolve = [&](const std::string& d) -> std::expected<std::string, std::string> {
            if (d == s.name()) return std::unexpected(std::format("step {} depends on itself", ns.name));
            if (shorts.contains(d)) return full(d);
            if (d == "." || d == step) return std::unexpected(std::format("step {} cannot depend on its spawning step", ns.name));
            external.insert(d);
            return d;
        };
        std::vector<std::string> deps;
        ns.spec.clear_depends_on();
        for (const auto& d : s.depends_on()) {
            auto r = resolve(d);
            if (!r) return r.error();
            deps.push_back(*r);
            ns.spec.add_depends_on(*r);
        }
        for (auto& in : *ns.spec.mutable_inputs()) {
            if (in.step() == ".") {  // the spawning step's own outputs: completed, not a dependency
                if (in.spawned()) return std::format("step {}: input '.' cannot be spawned", ns.name);
                in.set_step(step);
                continue;
            }
            auto r = resolve(in.step());
            if (!r) return r.error();
            in.set_step(*r);
            deps.push_back(*r);
        }
        std::ranges::sort(deps);
        deps.erase(std::unique(deps.begin(), deps.end()), deps.end());
        for (const auto& d : deps)
            if (d.starts_with(step + "/") && !shorts.contains(d.substr(step.size() + 1)))
                return std::format("step {}: unknown dependency {}", ns.name, d);
        ns.deps = deps;
        sib[ns.name] = deps;
        fulls.push_back(ns.name);
        out.steps.push_back(std::move(ns));
    }
    // Names must be new (a retried completion cannot re-add them).
    if (c_.exec("SELECT 1 FROM workflow_steps WHERE workflow_id = $1 AND name = ANY($2::text[]) LIMIT 1", wf_id,
                text_array(fulls))
            .rows() > 0) {
        out.steps.clear();
        return "step names already exist";
    }
    auto fail = [&](std::string e) {
        out.steps.clear();
        return e;
    };
    if (auto c = find_cycle(fulls, sib); !c.empty()) return fail("dependency cycle through step " + c);
    if (!external.empty()) {
        std::vector<std::string> ext(external.begin(), external.end());
        auto ex = c_.exec("SELECT name FROM workflow_steps WHERE workflow_id = $1 AND name = ANY($2::text[])", wf_id,
                          text_array(ext));
        if (static_cast<std::size_t>(ex.rows()) != ext.size()) {
            std::set<std::string> have;
            for (int i = 0; i < ex.rows(); ++i) have.insert(ex.str(i, 0));
            for (const auto& e : ext)
                if (!have.contains(e)) return fail("unknown dependency " + e);
        }
        // Deadlock check: an existing dependency that (through dependencies or
        // spawn-tree settlement) waits for the spawning step or one of its
        // ancestors would wait for these new steps, which wait for it.
        std::unordered_set<std::string> blocked{step};
        for (auto& a : ancestors(step)) blocked.insert(a);
        std::unordered_map<std::string, std::vector<std::string>> edges;  // waits-for
        auto g = c_.exec("SELECT step, dep FROM workflow_step_deps WHERE workflow_id = $1", wf_id);
        for (int i = 0; i < g.rows(); ++i) edges[g.str(i, 0)].push_back(g.str(i, 1));
        auto names = c_.exec("SELECT name, coalesce(parent_step, '') FROM workflow_steps WHERE workflow_id = $1", wf_id);
        for (int i = 0; i < names.rows(); ++i)
            if (!names.str(i, 1).empty()) edges[names.str(i, 1)].push_back(names.str(i, 0));
        std::vector<std::string> stack(ext.begin(), ext.end());
        std::unordered_set<std::string> seen(ext.begin(), ext.end());
        while (!stack.empty()) {
            auto v = stack.back();
            stack.pop_back();
            if (blocked.contains(v)) return fail(std::format("dependency on {} would wait for spawning step {}", v, step));
            for (const auto& w : edges[v])
                if (seen.insert(w).second) stack.push_back(w);
        }
    }
    return {};
}

void WorkflowStore::job_terminal(const std::string& wf_id, const std::string& step, tv1::JobState state,
                                 const std::string& error, const Expansion* exp, std::int64_t now_ms) {
    auto wf = load_wf(wf_id);
    if (!wf) return;
    Run r;
    r.wf = *wf;
    r.now = now_ms;
    if (exp && r.wf.state == "RUNNING") insert_steps(r, exp->steps, true);
    mark_terminal(r, step, tv1::JobState_Name(state), error);
    if (r.wf.state == "RUNNING") {
        process(r);
        finish(r);
    }
    save_wf(r);
}

// ---- read -------------------------------------------------------------------------------

std::optional<cv1::Workflow> WorkflowStore::load_workflow(const std::string& id, bool with_steps) {
    auto r = c_.exec(std::string("SELECT ") + kWfCols + " FROM workflows w WHERE w.workflow_id = $1", id);
    if (r.rows() != 1) return std::nullopt;
    auto w = wf_from(r, 0);
    auto counts = c_.exec(std::string("SELECT ") + kStepStateExpr +
                              " AS st, count(*) FROM workflow_steps s LEFT JOIN jobs j ON j.job_id = s.job_id"
                              " WHERE s.workflow_id = $1 GROUP BY st",
                          id);
    for (int i = 0; i < counts.rows(); ++i)
        (*w.mutable_step_counts())[counts.str(i, 0)] = static_cast<std::uint32_t>(counts.u64(i, 1));
    if (with_steps) {
        auto s = c_.exec(std::string("SELECT s.name, ") + kStepStateExpr +
                             ", coalesce(s.job_id, ''), coalesce(s.parent_step, ''), s.spec::text,"
                             " coalesce(j.node_id, ''), s.error, " MS("s.updated_at") ","
                             " CASE WHEN s.state = 'ACTIVE' THEN coalesce(j.last_error, '') ELSE '' END"
                             " FROM workflow_steps s LEFT JOIN jobs j ON j.job_id = s.job_id"
                             " WHERE s.workflow_id = $1 ORDER BY s.seq",
                         id);
        for (int i = 0; i < s.rows(); ++i) {
            auto* x = w.add_steps();
            x->set_name(s.str(i, 0));
            x->set_state(s.str(i, 1));
            x->set_job_id(s.str(i, 2));
            x->set_parent_step(s.str(i, 3));
            *x->mutable_spec() = parse_step_spec(s.str(i, 4));
            x->set_node_id(s.str(i, 5));
            x->set_error(s.str(i, 6).empty() ? s.str(i, 8) : s.str(i, 6));
            x->set_updated_unix_ms(s.i64(i, 7));
        }
    }
    return w;
}

std::optional<cv1::Workflow> WorkflowStore::get_workflow(const std::string& id, bool with_steps) {
    return transact(c_, [&] { return load_workflow(id, with_steps); });
}

std::vector<cv1::Workflow> WorkflowStore::list_workflows(const std::string& state, const std::string& campaign_id,
                                                         std::size_t limit) {
    return transact(c_, [&] {
        auto r = c_.exec("SELECT workflow_id FROM workflows WHERE ($1 = '' OR state = $1) AND ($2 = '' OR campaign_id = $2)"
                         " ORDER BY created_at DESC, seq DESC LIMIT $3::bigint",
                         state, campaign_id, clamp_limit(limit));
        std::vector<cv1::Workflow> out;
        for (int i = 0; i < r.rows(); ++i)
            if (auto w = load_workflow(r.str(i, 0), false)) out.push_back(std::move(*w));
        return out;
    });
}

// ---- cancel / sweep ----------------------------------------------------------------------

std::optional<cv1::Workflow> WorkflowStore::cancel_workflow(const std::string& id, const std::string& reason,
                                                            std::int64_t now_ms) {
    const std::string why = reason.empty() ? std::string("cancelled") : reason;
    return transact(c_, [&]() -> std::optional<cv1::Workflow> {
        if (!lock(id, false)) return std::nullopt;
        Run r;
        r.wf = *load_wf(id);
        r.now = now_ms;
        if (r.wf.state != "RUNNING") return load_workflow(id, true);
        r.wf.state = "CANCELLED";
        if (r.wf.error.empty()) r.wf.error = why;
        auto w = c_.exec("UPDATE workflow_steps SET state = 'CANCELLED', error = $2, updated_at = " TS(3)
                         " WHERE workflow_id = $1 AND state = 'WAITING' RETURNING name",
                         id, why, now_ms);
        r.wf.open_steps -= w.rows();
        // Jobs of ACTIVE steps (workflow row locked first: lock order).
        auto j = c_.exec("SELECT j.job_id, j.state, j.step FROM jobs j WHERE j.workflow_id = $1"
                         " AND j.state IN ('QUEUED', 'RETRY_PENDING', 'LEASED', 'RUNNING') ORDER BY j.job_id FOR UPDATE",
                         id);
        for (int i = 0; i < j.rows(); ++i) {
            const auto st = j.str(i, 1);
            if (st == "LEASED" || st == "RUNNING") {
                c_.exec("UPDATE jobs SET cancel_requested = true, last_error = $2, updated_at = " TS(3) " WHERE job_id = $1",
                        j.str(i, 0), why, now_ms);
            } else {
                c_.exec("UPDATE jobs SET state = 'CANCELLED', cancel_requested = true, last_error = $2,"
                        " updated_at = " TS(3) " WHERE job_id = $1",
                        j.str(i, 0), why, now_ms);
                mark_terminal(r, j.str(i, 2), "CANCELLED", why);
            }
        }
        save_wf(r);
        return load_workflow(id, true);
    });
}

std::size_t WorkflowStore::sweep_workflows(std::int64_t now_ms) {
    auto ids = c_.retry([&] {
        auto r = c_.exec("SELECT workflow_id FROM workflows WHERE state = 'RUNNING' ORDER BY created_at, seq");
        std::vector<std::string> v;
        for (int i = 0; i < r.rows(); ++i) v.push_back(r.str(i, 0));
        return v;
    });
    std::size_t total = 0;
    for (const auto& id : ids) {
        total += transact(c_, [&]() -> std::size_t {
            if (!lock(id, true)) return 0;  // busy: the engine is running there right now
            Run r;
            r.wf = *load_wf(id);
            r.now = now_ms;
            if (r.wf.state != "RUNNING") return 0;
            // Steps whose job ended without the engine noticing.
            auto stale = c_.exec("SELECT s.name, j.state, j.last_error FROM workflow_steps s JOIN jobs j ON j.job_id = s.job_id"
                                 " WHERE s.workflow_id = $1 AND s.state = 'ACTIVE'"
                                 " AND j.state IN ('COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED', 'VALIDATION_FAILED')"
                                 " ORDER BY s.seq",
                                 id);
            for (int i = 0; i < stale.rows(); ++i) mark_terminal(r, stale.str(i, 0), stale.str(i, 1), stale.str(i, 2));
            // WAITING steps whose dependencies are all settled.
            auto ready = c_.exec(
                "SELECT s.name FROM workflow_steps s WHERE s.workflow_id = $1 AND s.state = 'WAITING'"
                " AND NOT EXISTS (SELECT 1 FROM workflow_step_deps d"
                "   JOIN workflow_steps x ON x.workflow_id = d.workflow_id AND x.name = d.dep"
                "   WHERE d.workflow_id = s.workflow_id AND d.step = s.name"
                "   AND NOT (x.state IN ('COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED', 'VALIDATION_FAILED', 'SKIPPED')"
                "            AND x.open_desc = 0))"
                " ORDER BY s.seq",
                id);
            for (int i = 0; i < ready.rows(); ++i) r.to_eval.push_back(ready.str(i, 0));
            const auto before_state = r.wf.state;
            process(r);
            finish(r);
            if (r.changes > 0 || r.wf.state != before_state) save_wf(r);
            return r.changes;
        });
    }
    return total;
}

#undef SUBTREE
#undef TS
#undef MS

}  // namespace tdr::cpdb
