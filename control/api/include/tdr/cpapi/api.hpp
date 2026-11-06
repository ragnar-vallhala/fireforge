// Control-plane HTTP API handlers (docs/design/phase2.md §3-§5).
//
// Pure request -> response logic over IRepository: routing, auth rules,
// proto3-JSON (de)serialization, profile presets, campaign transitions,
// audit records, health sweep. No sockets here; tdrcp wires it to HttpsServer.
#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>

#include "fireforge/control/v1/control.pb.h"
#include "tdr/cpdb/db.hpp"
#include "tdr/http/http.hpp"

namespace tdr::cpapi {

namespace cv1 = ::fireforge::control::v1;

struct ApiOptions {
    std::uint32_t heartbeat_interval_seconds = 10;
    std::uint32_t stale_after_seconds = 30;
    std::uint32_t offline_after_seconds = 90;
    std::uint32_t lease_ttl_seconds = 60;      // phase4.md §3
    std::uint32_t max_attempts_default = 3;    // when a submitted spec leaves it 0
    std::uint32_t locality_wait_seconds = 15;  // phase5.md §4 (0 = any node at once)
};

// §8.3 presets. nullopt for an unknown profile name.
std::optional<cv1::ResourcePolicy> profile_preset(const std::string& name);

// §5 transition table. Self-transitions are invalid.
bool is_valid_campaign_transition(cv1::CampaignState from, cv1::CampaignState to);

// proto3 JSON with original field names; parse ignores unknown fields.
std::string to_json(const google::protobuf::Message& m);
bool from_json(const std::string& json, google::protobuf::Message& m, std::string* error = nullptr);

class ControlApi {
public:
    // clock returns unix ms (injectable for tests).
    ControlApi(cpdb::IRepository& repo, ApiOptions options, std::function<std::int64_t()> clock);
    ~ControlApi();

    // Handles every /api/v1/... route and /healthz. Requests without a peer
    // identity get 401. DbError -> 500 (logged), never leaks SQL details.
    http::Response handle(http::Request& req);

    // Runs repo.sweep_health and logs changes. tdrcp calls this on a timer.
    std::vector<cpdb::HealthChange> sweep();
    // Runs repo.expire_leases unless still within lease_ttl of construction
    // (startup grace, phase4.md §3). Logs each expiry. tdrcp calls it with sweep().
    std::vector<cpdb::ExpiredLease> sweep_leases();

    const ApiOptions& options() const;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace tdr::cpapi
