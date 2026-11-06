// In-memory IRepository for ControlApi tests. Mirrors the documented
// semantics of the PostgreSQL repository (control/db/include/tdr/cpdb/db.hpp).
#pragma once

#include <algorithm>
#include <format>
#include <functional>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <vector>

#include "tdr/cpdb/db.hpp"

namespace tdr::cpapi::testing {

namespace cv1 = ::fireforge::control::v1;
namespace tv1 = ::fireforge::tdr::v1;

class FakeRepository : public cpdb::IRepository {
public:
    // Hooks for fault injection.
    bool fail_all = false;                       // every call throws DbError
    std::function<void()> before_update_campaign; // runs inside update_campaign before the CAS

    struct AuditRow {
        std::string actor, action, target, details_json;
        std::int64_t at = 0;
    };
    std::vector<AuditRow> audit_rows;            // oldest first
    std::map<std::string, cv1::Campaign> campaigns;
    std::map<std::string, cv1::NodeInfo> nodes;  // by node_id

    cv1::NodeInfo register_node(const cv1::RegisterRequest& req, const std::string& fp, std::int64_t now) override {
        maybe_fail();
        cv1::NodeInfo* n = nullptr;
        for (auto& [id, info] : nodes)
            if (info.name() == req.name()) n = &info;
        if (!n) {
            std::string id = std::format("ff-node-{:016x}", ++counter_);
            n = &nodes[id];
            n->set_node_id(id);
            n->set_name(req.name());
            n->set_registered_unix_ms(now);
            n->mutable_policy()->set_version(1);
        }
        n->mutable_addresses()->CopyFrom(req.addresses());
        n->mutable_capabilities()->CopyFrom(req.capabilities());
        n->set_daemon_version(req.daemon_version());
        *n->mutable_physical() = req.physical();
        *n->mutable_configured() = req.configured();
        n->set_cert_fingerprint(fp);
        n->clear_offline_reason();
        set_health(*n, cv1::ONLINE, "registered", now);
        touch(*n, now);
        return *n;
    }

    std::optional<cv1::NodeInfo> get_node(const std::string& id) override {
        maybe_fail();
        auto it = nodes.find(id);
        if (it == nodes.end()) return std::nullopt;
        return it->second;
    }

    std::optional<cv1::NodeInfo> find_node_by_name(const std::string& name) override {
        maybe_fail();
        for (auto& [id, n] : nodes)
            if (n.name() == name) return n;
        return std::nullopt;
    }

    std::vector<cv1::NodeInfo> list_nodes(cv1::NodeHealth h) override {
        maybe_fail();
        std::vector<cv1::NodeInfo> out;
        for (auto& [id, n] : nodes)
            if (h == cv1::NODE_HEALTH_UNSPECIFIED || n.health() == h) out.push_back(n);
        std::ranges::sort(out, {}, [](const cv1::NodeInfo& n) { return n.name(); });
        return out;
    }

    bool record_heartbeat(const cv1::Heartbeat& hb, std::int64_t now) override {
        maybe_fail();
        auto it = nodes.find(hb.node_id());
        if (it == nodes.end()) return false;
        auto& n = it->second;
        *n.mutable_last_heartbeat() = hb;
        if (hb.addresses_size() > 0) n.mutable_addresses()->CopyFrom(hb.addresses());
        set_health(n, cv1::ONLINE, "heartbeat", now);
        touch(n, now);
        return true;
    }

    std::optional<cv1::ResourcePolicy> set_policy(const std::string& id, const cv1::ResourcePolicy& limits,
                                                  const std::string& actor, std::int64_t now) override {
        maybe_fail();
        auto it = nodes.find(id);
        if (it == nodes.end()) return std::nullopt;
        auto* p = it->second.mutable_policy();
        const auto v = p->version();
        *p = limits;
        p->set_version(v + 1);
        p->set_updated_by(actor);
        p->set_updated_unix_ms(now);
        return *p;
    }

    bool set_offline(const std::string& id, const std::string& reason, std::int64_t now) override {
        maybe_fail();
        auto it = nodes.find(id);
        if (it == nodes.end()) return false;
        it->second.set_offline_reason(reason);
        set_health(it->second, cv1::OFFLINE, reason, now);
        return true;
    }

    std::vector<cpdb::HealthChange> sweep_health(std::int64_t now, std::int64_t stale_after,
                                                 std::int64_t offline_after) override {
        maybe_fail();
        std::vector<cpdb::HealthChange> out;
        for (auto& [id, n] : nodes) {
            const auto seen = n.last_seen_unix_ms();
            const auto from = n.health();
            if ((from == cv1::ONLINE || from == cv1::STALE) && seen < now - offline_after) {
                n.set_offline_reason("heartbeat timeout");
                set_health(n, cv1::OFFLINE, "heartbeat timeout", now);
                out.push_back({id, from, cv1::OFFLINE, "heartbeat timeout"});
            } else if (from == cv1::ONLINE && seen < now - stale_after) {
                set_health(n, cv1::STALE, "no heartbeat", now);
                out.push_back({id, from, cv1::STALE, "no heartbeat"});
            }
        }
        return out;
    }

    std::vector<cv1::NodeEvent> node_events(const std::string& id, std::size_t limit) override {
        maybe_fail();
        std::vector<cv1::NodeEvent> out;
        auto& evs = events_[id];
        for (auto it = evs.rbegin(); it != evs.rend() && out.size() < limit; ++it) out.push_back(*it);
        return out;
    }

    std::expected<cv1::Campaign, std::string> create_campaign(const cv1::CreateCampaignRequest& req,
                                                              const std::string& actor, std::int64_t now) override {
        maybe_fail();
        for (auto& [id, c] : campaigns)
            if (c.name() == req.name()) return std::unexpected("campaign name already exists");
        cv1::Campaign c;
        c.set_campaign_id(std::format("camp-{:016x}", ++counter_));
        c.set_name(req.name());
        c.set_description(req.description());
        c.set_domain(req.domain());
        c.set_parameters_json(req.parameters_json());
        c.set_state(cv1::DRAFT);
        c.set_created_by(actor);
        c.set_created_unix_ms(now);
        c.set_updated_unix_ms(now);
        campaign_order_.push_back(c.campaign_id());
        campaigns[c.campaign_id()] = c;
        return c;
    }

    std::optional<cv1::Campaign> get_campaign(const std::string& id) override {
        maybe_fail();
        auto it = campaigns.find(id);
        if (it == campaigns.end()) return std::nullopt;
        return it->second;
    }

    std::vector<cv1::Campaign> list_campaigns(cv1::CampaignState s) override {
        maybe_fail();
        std::vector<cv1::Campaign> out;
        for (auto it = campaign_order_.rbegin(); it != campaign_order_.rend(); ++it) {
            auto& c = campaigns.at(*it);
            if (s == cv1::CAMPAIGN_STATE_UNSPECIFIED || c.state() == s) out.push_back(c);
        }
        return out;
    }

    bool update_campaign(const cv1::Campaign& u, cv1::CampaignState expected, std::int64_t now) override {
        maybe_fail();
        if (before_update_campaign) before_update_campaign();
        auto it = campaigns.find(u.campaign_id());
        if (it == campaigns.end() || it->second.state() != expected) return false;
        it->second.set_description(u.description());
        it->second.set_state(u.state());
        it->second.set_parameters_json(u.parameters_json());
        it->second.set_updated_unix_ms(now);
        return true;
    }


    // ---- jobs (simplified mirror of the Postgres semantics) -----------------------

    std::map<std::string, cv1::Job> jobs;                 // by job_id
    std::vector<std::string> job_order;                   // creation order
    // (artifact_id, node_id) -> ref
    std::map<std::pair<std::string, std::string>, cv1::ArtifactRef> locations;
    std::vector<cv1::ArtifactAnnouncement> announcements;

    std::expected<cv1::Job, std::string> create_job(const tv1::JobSpec& in, const std::string& by,
                                                    std::int64_t now) override {
        maybe_fail();
        tv1::JobSpec spec = in;
        if (spec.job_id().empty()) spec.set_job_id(std::format("job-{:016x}", ++counter_));
        if (jobs.contains(spec.job_id())) return std::unexpected("job id already exists");
        if (!spec.campaign_id().empty() && !campaigns.contains(spec.campaign_id()))
            return std::unexpected("unknown campaign");
        cv1::Job j;
        j.set_job_id(spec.job_id());
        j.set_state(tv1::QUEUED);
        *j.mutable_spec() = spec;
        j.set_submitted_by(by);
        j.set_created_unix_ms(now);
        j.set_updated_unix_ms(now);
        jobs[j.job_id()] = j;
        job_order.push_back(j.job_id());
        return j;
    }

    std::optional<cv1::Job> get_job(const std::string& id, bool with_attempts) override {
        maybe_fail();
        auto it = jobs.find(id);
        if (it == jobs.end()) return std::nullopt;
        cv1::Job j = it->second;
        if (!with_attempts) j.clear_attempts();
        return j;
    }

    std::vector<cv1::Job> list_jobs(tv1::JobState s, const std::string& campaign, std::size_t limit) override {
        maybe_fail();
        std::vector<cv1::Job> out;
        for (auto it = job_order.rbegin(); it != job_order.rend() && out.size() < limit; ++it) {
            auto j = jobs.at(*it);
            if (s != tv1::JOB_STATE_UNSPECIFIED && j.state() != s) continue;
            if (!campaign.empty() && j.spec().campaign_id() != campaign) continue;
            j.clear_attempts();
            out.push_back(j);
        }
        return out;
    }

    cpdb::CancelOutcome cancel_job(const std::string& id, const std::string& reason, std::int64_t now) override {
        maybe_fail();
        cpdb::CancelOutcome out;
        auto it = jobs.find(id);
        if (it == jobs.end()) return out;
        auto& j = it->second;
        const auto st = j.state();
        if (terminal(st)) out.result = cpdb::CancelResult::kTerminal;
        else if (st == tv1::LEASED || st == tv1::RUNNING) out.result = cpdb::CancelResult::kRequested;
        else {
            out.result = cpdb::CancelResult::kCancelled;
            j.set_state(tv1::CANCELLED);
        }
        if (out.result != cpdb::CancelResult::kTerminal) {
            j.set_cancel_requested(true);
            j.set_last_error(reason);
            j.set_updated_unix_ms(now);
        }
        out.job = j;
        out.job->clear_attempts();
        return out;
    }

    // Locality is not modelled here (the Postgres tests cover it); the wait is recorded.
    std::vector<cv1::Lease> lease_jobs(const cv1::LeaseRequest& req, std::int64_t ttl, std::int64_t locality_wait_ms,
                                       std::int64_t now) override {
        maybe_fail();
        last_lease_request = req;
        last_locality_wait_ms = locality_wait_ms;
        std::vector<cv1::Lease> out;
        auto nit = nodes.find(req.node_id());
        if (nit == nodes.end() || nit->second.health() != cv1::ONLINE) return out;
        std::uint64_t cpu = req.free().cpu_cores(), mem = req.free().memory_bytes(), gpu = req.free().gpu_count();
        for (const auto& id : job_order) {
            if (out.size() >= req.max_jobs()) break;
            auto& j = jobs.at(id);
            if (j.state() != tv1::QUEUED) continue;
            const auto& r = j.spec().resources();
            const std::uint64_t jc = std::max<std::uint32_t>(r.cpu_cores(), 1);
            if (jc > cpu || r.memory_bytes() > mem || r.gpu_count() > gpu) continue;
            cpu -= jc;
            mem -= r.memory_bytes();
            gpu -= r.gpu_count();
            const auto number = j.attempts_used() + 1;
            auto* a = j.add_attempts();
            a->set_attempt_id(std::format("att-{:016x}", ++counter_));
            a->set_number(number);
            a->set_fencing_token(number);
            a->set_node_id(req.node_id());
            a->set_state("LEASED");
            a->set_leased_unix_ms(now);
            a->set_expires_unix_ms(now + ttl);
            j.set_state(tv1::LEASED);
            j.set_attempts_used(number);
            j.set_current_attempt_id(a->attempt_id());
            j.set_node_id(req.node_id());
            cv1::Lease l;
            l.set_job_id(id);
            l.set_attempt_id(a->attempt_id());
            l.set_fencing_token(number);
            l.set_attempt_number(number);
            *l.mutable_spec() = j.spec();
            l.set_expires_unix_ms(now + ttl);
            out.push_back(l);
        }
        return out;
    }

    cpdb::UpdateOutcome update_job(const std::string& id, const std::string& node_id, const cv1::JobUpdate& u,
                                   std::int64_t now) override {
        maybe_fail();
        last_update = u;
        cpdb::UpdateOutcome out;
        auto it = jobs.find(id);
        if (it == jobs.end()) return out;
        auto& j = it->second;
        static const std::set<tv1::JobState> ok = {tv1::RUNNING,   tv1::COMPLETED, tv1::FAILED,
                                                   tv1::CANCELLED, tv1::EXPIRED,   tv1::VALIDATION_FAILED};
        if (!ok.contains(u.state())) {
            out.result = cpdb::UpdateResult::kInvalid;
            out.message = "bad state";
            return out;
        }
        cv1::JobAttempt* cur = current(j);
        if ((j.state() != tv1::LEASED && j.state() != tv1::RUNNING) || !cur || cur->attempt_id() != u.attempt_id() ||
            cur->fencing_token() != u.fencing_token() || cur->node_id() != node_id) {
            out.result = cpdb::UpdateResult::kStale;
            out.message = "not the current attempt";
            return out;
        }
        j.set_updated_unix_ms(now);
        switch (u.state()) {
            case tv1::RUNNING:
                j.set_state(tv1::RUNNING);
                cur->set_state("RUNNING");
                j.set_progress_fraction(u.progress_fraction());
                j.set_progress_message(u.progress_message());
                if (u.has_checkpoint()) *j.mutable_checkpoint() = u.checkpoint();
                break;
            case tv1::COMPLETED:
                j.set_state(tv1::COMPLETED);
                cur->set_state("SUCCEEDED");
                *j.mutable_outputs() = u.outputs();
                for (const auto& o : u.outputs()) locations[{o.artifact_id(), node_id}] = o;
                if (!u.spawn().empty() && j.workflow_id().empty()) {  // mirrors the Postgres rule
                    j.set_state(tv1::FAILED);
                    j.set_last_error("invalid spawn: only workflow steps may spawn");
                    out.result = cpdb::UpdateResult::kInvalid;
                    out.message = j.last_error();
                    out.job = j;
                    return out;
                }
                break;
            case tv1::FAILED: {
                cur->set_state("FAILED");
                const bool retry = u.retryable() && j.attempts_used() < j.spec().policy().max_attempts() &&
                                   !j.cancel_requested();
                j.set_state(retry ? tv1::QUEUED : tv1::FAILED);
                j.set_last_error(u.error());
                break;
            }
            case tv1::CANCELLED:
                cur->set_state("CANCELLED");
                j.set_state(tv1::CANCELLED);
                break;
            default:
                cur->set_state("FAILED");
                j.set_state(u.state());
                j.set_last_error(u.error());
                break;
        }
        out.result = cpdb::UpdateResult::kOk;
        out.job = j;
        out.job->clear_attempts();
        return out;
    }

    std::vector<cv1::LeaseRef> renew_leases(const std::string& node_id, const std::vector<cv1::LeaseRef>& leases,
                                            std::int64_t ttl, std::int64_t now) override {
        maybe_fail();
        std::vector<cv1::LeaseRef> revoked;
        for (const auto& l : leases) {
            auto it = jobs.find(l.job_id());
            cv1::JobAttempt* cur = it == jobs.end() ? nullptr : current(it->second);
            if (!cur || cur->attempt_id() != l.attempt_id() || cur->fencing_token() != l.fencing_token() ||
                cur->node_id() != node_id || it->second.cancel_requested() ||
                (it->second.state() != tv1::LEASED && it->second.state() != tv1::RUNNING)) {
                revoked.push_back(l);
                continue;
            }
            cur->set_expires_unix_ms(now + ttl);
        }
        return revoked;
    }

    std::vector<cpdb::ExpiredLease> expire_leases(std::int64_t now) override {
        maybe_fail();
        ++expire_calls;
        std::vector<cpdb::ExpiredLease> out;
        for (const auto& id : job_order) {
            auto& j = jobs.at(id);
            if (j.state() != tv1::LEASED && j.state() != tv1::RUNNING) continue;
            auto* cur = current(j);
            if (!cur || cur->expires_unix_ms() >= now) continue;
            cur->set_state("ABANDONED");
            cpdb::ExpiredLease e{id, cur->attempt_id(), cur->node_id(), tv1::FAILED};
            if (j.cancel_requested()) e.new_state = tv1::CANCELLED;
            else if (j.attempts_used() < j.spec().policy().max_attempts()) e.new_state = tv1::QUEUED;
            j.set_state(e.new_state);
            j.set_last_error("lease expired");
            out.push_back(e);
        }
        return out;
    }

    bool announce_artifacts(const cv1::ArtifactAnnouncement& a, std::int64_t) override {
        maybe_fail();
        if (!nodes.contains(a.node_id())) return false;
        announcements.push_back(a);
        for (const auto& x : a.artifacts()) locations[{x.artifact_id(), a.node_id()}] = x;
        return true;
    }

    std::optional<cv1::ArtifactInfo> get_artifact(const std::string& id) override {
        maybe_fail();
        std::optional<cv1::ArtifactInfo> out;
        for (const auto& [key, ref] : locations) {
            if (key.first != id) continue;
            if (!out) {
                out.emplace();
                *out->mutable_artifact() = ref;
            }
            out->add_node_ids(key.second);
        }
        if (!out && pins.contains(id)) {
            out.emplace();
            out->mutable_artifact()->set_artifact_id(id);
        }
        if (out) out->set_pinned(pins.contains(id));
        return out;
    }

    cv1::ReleaseReply release_artifact(const std::string& id, const std::string& node_id, bool force,
                                       std::int64_t) override {
        maybe_fail();
        cv1::ReleaseReply out;
        bool other = false;
        for (const auto& [key, ref] : locations)
            if (key.first == id && key.second != node_id) other = true;
        if (!force && pins.contains(id)) out.set_reason("pinned");
        else if (!force && !other) out.set_reason("last copy");
        else {
            locations.erase({id, node_id});
            out.set_released(true);
        }
        return out;
    }

    cv1::ArtifactInfo set_artifact_pinned(const std::string& id, bool pinned, const std::string& actor,
                                          std::int64_t) override {
        maybe_fail();
        if (pinned) pins.try_emplace(id, actor);
        else pins.erase(id);
        if (auto info = get_artifact(id)) return *info;
        cv1::ArtifactInfo info;
        info.mutable_artifact()->set_artifact_id(id);
        return info;
    }

    // ---- workflows (minimal: storage only, no engine) ---------------------------------
    std::map<std::string, cv1::Workflow> workflows;
    std::vector<std::string> workflow_order;
    cv1::CreateWorkflowRequest last_workflow_request;
    cpdb::WorkflowLimits workflow_limits;
    int limits_set = 0;
    std::size_t sweep_result = 0;
    int workflow_sweeps = 0;
    std::vector<cpdb::Reservation> reservations;  // returned by place_jobs
    cpdb::PlacementOptions last_placement;
    int place_calls = 0;
    cv1::JobUpdate last_update;

    std::expected<cv1::Workflow, std::string> create_workflow(const cv1::CreateWorkflowRequest& req,
                                                              const std::string& by, std::int64_t now) override {
        maybe_fail();
        last_workflow_request = req;
        if (req.steps().empty()) return std::unexpected("workflow has no steps");
        if (!req.campaign_id().empty() && !campaigns.contains(req.campaign_id()))
            return std::unexpected("unknown campaign " + req.campaign_id());
        cv1::Workflow w;
        w.set_workflow_id(std::format("wf-{:016x}", ++counter_));
        w.set_name(req.name());
        w.set_campaign_id(req.campaign_id());
        w.set_state("RUNNING");
        w.set_submitted_by(by);
        w.set_created_unix_ms(now);
        w.set_updated_unix_ms(now);
        for (const auto& s : req.steps()) {
            auto* x = w.add_steps();
            x->set_name(s.name());
            x->set_state("WAITING");
            *x->mutable_spec() = s;
        }
        (*w.mutable_step_counts())["WAITING"] = static_cast<std::uint32_t>(req.steps_size());
        workflows[w.workflow_id()] = w;
        workflow_order.push_back(w.workflow_id());
        return w;
    }

    std::optional<cv1::Workflow> get_workflow(const std::string& id, bool with_steps) override {
        maybe_fail();
        auto it = workflows.find(id);
        if (it == workflows.end()) return std::nullopt;
        cv1::Workflow w = it->second;
        if (!with_steps) w.clear_steps();
        return w;
    }

    std::vector<cv1::Workflow> list_workflows(const std::string& state, const std::string& campaign,
                                              std::size_t limit) override {
        maybe_fail();
        std::vector<cv1::Workflow> out;
        for (auto it = workflow_order.rbegin(); it != workflow_order.rend() && out.size() < limit; ++it) {
            auto w = workflows.at(*it);
            if (!state.empty() && w.state() != state) continue;
            if (!campaign.empty() && w.campaign_id() != campaign) continue;
            w.clear_steps();
            out.push_back(w);
        }
        return out;
    }

    std::optional<cv1::Workflow> cancel_workflow(const std::string& id, const std::string& reason,
                                                 std::int64_t now) override {
        maybe_fail();
        auto it = workflows.find(id);
        if (it == workflows.end()) return std::nullopt;
        auto& w = it->second;
        if (w.state() == "RUNNING") {
            w.set_state("CANCELLED");
            w.set_error(reason);
            w.set_finished_unix_ms(now);
            for (auto& s : *w.mutable_steps())
                if (s.state() == "WAITING") s.set_state("CANCELLED");
        }
        return w;
    }

    std::size_t sweep_workflows(std::int64_t) override {
        maybe_fail();
        ++workflow_sweeps;
        return sweep_result;
    }

    void set_workflow_limits(const cpdb::WorkflowLimits& l) override {
        workflow_limits = l;
        ++limits_set;
    }

    // Phase 7: stored only (gang placement is the repository's).
    cpdb::GangOptions gang_options;
    int gang_options_set = 0;
    void set_gang_options(const cpdb::GangOptions& g) override {
        gang_options = g;
        ++gang_options_set;
    }

    std::vector<cpdb::Reservation> place_jobs(const cpdb::PlacementOptions& o, std::int64_t) override {
        maybe_fail();
        last_placement = o;
        ++place_calls;
        return reservations;
    }

    // Phase 8: seeded by tests; reads return them, imports append (minimal checks only).
    std::map<std::string, cv1::Material> materials;
    std::map<std::string, cv1::StructureInfo> structures;
    std::vector<cv1::Calculation> calculations;  // oldest first
    std::string last_formula, last_element, last_material_campaign;
    std::size_t last_material_limit = 0;
    std::optional<CalcQuery> last_calc_query;
    std::vector<cv1::ImportCalculationsRequest> imports;

    std::vector<cv1::Material> list_materials(const std::string& formula, const std::string& element,
                                              const std::string& campaign_id, std::size_t limit) override {
        maybe_fail();
        last_formula = formula;
        last_element = element;
        last_material_campaign = campaign_id;
        last_material_limit = limit;
        std::vector<cv1::Material> out;
        for (const auto& [_, m] : materials) {
            if (!formula.empty() && m.reduced_formula() != formula) continue;
            if (!element.empty() && std::find(m.elements().begin(), m.elements().end(), element) == m.elements().end())
                continue;
            if (out.size() < limit) out.push_back(m);
        }
        return out;
    }
    std::optional<cv1::Material> get_material(const std::string& id) override {
        maybe_fail();
        auto it = materials.find(id);
        if (it == materials.end()) return std::nullopt;
        return it->second;
    }
    std::optional<cv1::StructureInfo> get_structure(const std::string& id) override {
        maybe_fail();
        auto it = structures.find(id);
        if (it == structures.end()) return std::nullopt;
        return it->second;
    }
    cv1::CalculationList list_calculations(const CalcQuery& q) override {
        maybe_fail();
        last_calc_query = q;
        cv1::CalculationList out;
        for (auto it = calculations.rbegin(); it != calculations.rend(); ++it) {
            if (!q.kind.empty() && it->kind() != q.kind) continue;
            if (q.evidence && it->evidence() != *q.evidence) continue;
            if (static_cast<std::size_t>(out.calculations_size()) < q.limit) *out.add_calculations() = *it;
        }
        return out;
    }
    std::optional<cv1::Calculation> get_calculation(const std::string& id) override {
        maybe_fail();
        for (const auto& c : calculations)
            if (c.calc_id() == id) return c;
        return std::nullopt;
    }
    std::expected<cv1::ImportCalculationsReply, std::string> import_calculations(
        const cv1::ImportCalculationsRequest& req, const std::string& actor, std::int64_t now) override {
        maybe_fail();
        if (req.records().empty()) return std::unexpected("records must not be empty");
        for (const auto& r : req.records())
            if (r.evidence() != tv1::LITERATURE && r.evidence() != tv1::MEASURED)
                return std::unexpected("imported records must have evidence LITERATURE or MEASURED");
        imports.push_back(req);
        cv1::ImportCalculationsReply out;
        for (const auto& r : req.records()) {
            cv1::Calculation c;
            c.set_calc_id(std::format("calc-{}", calculations.size() + 1));
            c.set_kind(r.kind());
            c.set_evidence(r.evidence());
            c.set_campaign_id(req.campaign_id());
            c.set_created_unix_ms(now);
            calculations.push_back(c);
            out.add_calc_ids(c.calc_id());
        }
        audit_rows.push_back({actor, "calculations.import", req.campaign_id(), "{}", now});
        return out;
    }

    std::map<std::string, std::string> pins;  // artifact_id -> pinned_by
    cv1::LeaseRequest last_lease_request;
    std::int64_t last_locality_wait_ms = -1;
    int expire_calls = 0;

    void audit(const std::string& actor, const std::string& action, const std::string& target,
               const std::string& details, std::int64_t now) override {
        maybe_fail();
        audit_rows.push_back({actor, action, target, details, now});
    }

    std::vector<cv1::AuditEntry> list_audit(std::size_t limit) override {
        maybe_fail();
        std::vector<cv1::AuditEntry> out;
        for (auto it = audit_rows.rbegin(); it != audit_rows.rend() && out.size() < limit; ++it) {
            cv1::AuditEntry e;
            e.set_at_unix_ms(it->at);
            e.set_actor(it->actor);
            e.set_action(it->action);
            e.set_target(it->target);
            e.set_details_json(it->details_json);
            out.push_back(e);
        }
        return out;
    }

    std::size_t event_count(const std::string& id) { return events_[id].size(); }

private:
    static bool terminal(tv1::JobState s) {
        return s == tv1::COMPLETED || s == tv1::FAILED || s == tv1::CANCELLED || s == tv1::EXPIRED ||
               s == tv1::VALIDATION_FAILED;
    }
    static cv1::JobAttempt* current(cv1::Job& j) {
        for (auto& a : *j.mutable_attempts())
            if (a.attempt_id() == j.current_attempt_id()) return &a;
        return nullptr;
    }
    void maybe_fail() {
        if (fail_all) throw cpdb::DbError("ERROR: relation \"nodes\" secret-sql-detail", "42P01");
    }
    static void touch(cv1::NodeInfo& n, std::int64_t now) { n.set_last_seen_unix_ms(now); }
    void set_health(cv1::NodeInfo& n, cv1::NodeHealth to, const std::string& reason, std::int64_t now) {
        if (n.health() == to) return;
        cv1::NodeEvent e;
        e.set_at_unix_ms(now);
        e.set_from(n.health());
        e.set_to(to);
        e.set_reason(reason);
        events_[n.node_id()].push_back(e);
        n.set_health(to);
    }

    std::uint64_t counter_ = 0;
    std::vector<std::string> campaign_order_;
    std::map<std::string, std::vector<cv1::NodeEvent>> events_;
};

}  // namespace tdr::cpapi::testing
