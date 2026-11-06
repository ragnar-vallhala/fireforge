// Workflow engine on PostgreSQL (docs/design/phase6.md §3-§5). Internal:
// PgRepository forwards the IRepository workflow methods here, and JobStore
// calls the engine hooks inside its job transitions.
//
// Lock order: a workflow row (workflows, FOR UPDATE) before its job rows.
// update_job / cancel_job read the (immutable) jobs.workflow_id first and lock
// the workflow before the job; expire_leases and sweep_workflows only use
// SKIP LOCKED, so they never wait while holding locks.
#pragma once

#include <deque>
#include <string>
#include <vector>

#include "pg.hpp"
#include "tdr/cpdb/db.hpp"

namespace tdr::cpdb {

class JobStore;

class WorkflowStore {
public:
    WorkflowStore(pg::Conn& c, JobStore& jobs) : c_(c), jobs_(jobs) {}

    void set_limits(const WorkflowLimits& l) { limits_ = l; }

    std::expected<cv1::Workflow, std::string> create_workflow(const cv1::CreateWorkflowRequest& req,
                                                              const std::string& submitted_by, std::int64_t now_ms);
    std::optional<cv1::Workflow> get_workflow(const std::string& workflow_id, bool with_steps);
    std::vector<cv1::Workflow> list_workflows(const std::string& state, const std::string& campaign_id,
                                              std::size_t limit);
    std::optional<cv1::Workflow> cancel_workflow(const std::string& workflow_id, const std::string& reason,
                                                 std::int64_t now_ms);
    std::size_t sweep_workflows(std::int64_t now_ms);

    // ---- engine hooks (inside the caller's transaction) ---------------------------
    struct NewStep {
        std::string name;    // full name
        std::string parent;  // "" = static
        int depth = 0;
        tv1::StepSpec spec;  // names resolved to full names
        std::vector<std::string> deps;
    };
    struct Expansion {
        std::vector<NewStep> steps;  // empty: nothing to add (or dropped: workflow not RUNNING)
    };

    // Locks the workflow row. skip_locked: also false when another transaction holds it.
    bool lock(const std::string& workflow_id, bool skip_locked);
    // Validates a fan-out of `step` (§5); lock held. "" = valid (out filled).
    std::string prepare_spawn(const std::string& workflow_id, const std::string& step,
                              const google::protobuf::RepeatedPtrField<tv1::StepSpec>& spawn, Expansion& out);
    // The step's job became terminal: record it, add the expansion, run the engine.
    void job_terminal(const std::string& workflow_id, const std::string& step, tv1::JobState state,
                      const std::string& error, const Expansion* expansion, std::int64_t now_ms);

private:
    struct Wf {
        std::string id, state, campaign_id, submitted_by, error;
        std::int64_t step_count = 0, open_steps = 0;
    };
    struct Run {
        Wf wf;
        std::int64_t now = 0;
        std::deque<std::string> finished;  // steps that just became terminal
        std::vector<std::string> to_eval;  // steps to evaluate first
        std::size_t changes = 0;
    };

    std::optional<Wf> load_wf(const std::string& id);
    void save_wf(const Run& r);
    void insert_steps(Run& r, const std::vector<NewStep>& steps, bool eval_all);
    bool mark_terminal(Run& r, const std::string& name, const std::string& state, const std::string& error);
    void process(Run& r);
    void evaluate(Run& r, const std::string& name);
    void finish(Run& r);
    std::optional<cv1::Workflow> load_workflow(const std::string& id, bool with_steps);

    pg::Conn& c_;
    JobStore& jobs_;
    WorkflowLimits limits_;
};

}  // namespace tdr::cpdb
