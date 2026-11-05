// In-memory IRepository for ControlApi tests. Mirrors the documented
// semantics of the PostgreSQL repository (control/db/include/tdr/cpdb/db.hpp).
#pragma once

#include <algorithm>
#include <format>
#include <functional>
#include <map>
#include <string>
#include <vector>

#include "tdr/cpdb/db.hpp"

namespace tdr::cpapi::testing {

namespace cv1 = ::fireforge::control::v1;

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
