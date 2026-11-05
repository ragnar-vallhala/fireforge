// Free helpers: profile presets, campaign transitions, proto3 JSON.
#include <google/protobuf/util/json_util.h>

#include "tdr/cpapi/api.hpp"

namespace tdr::cpapi {

namespace {
constexpr std::uint64_t kGiB = std::uint64_t{1} << 30;

cv1::ResourcePolicy preset(const char* name, std::uint32_t cores, std::uint64_t mem_gib, std::uint32_t jobs,
                           std::uint32_t gpus) {
    cv1::ResourcePolicy p;
    p.set_profile(name);
    p.set_max_cpu_cores(cores);
    p.set_max_memory_bytes(mem_gib * kGiB);
    p.set_max_jobs(jobs);
    p.set_max_gpus(gpus);
    return p;
}
}  // namespace

std::optional<cv1::ResourcePolicy> profile_preset(const std::string& name) {
    if (name == "small") return preset("small", 4, 8, 2, 0);
    if (name == "medium") return preset("medium", 12, 24, 4, 0);
    if (name == "compute") return preset("compute", 32, 96, 8, 0);
    if (name == "gpu") return preset("gpu", 16, 48, 4, 1);
    return std::nullopt;
}

bool is_valid_campaign_transition(cv1::CampaignState from, cv1::CampaignState to) {
    using enum cv1::CampaignState;
    switch (from) {
        case DRAFT: return to == ACTIVE || to == ARCHIVED;
        case ACTIVE: return to == PAUSED || to == COMPLETED;
        case PAUSED: return to == ACTIVE || to == COMPLETED;
        case COMPLETED: return to == ARCHIVED;
        default: return false;
    }
}

std::string to_json(const google::protobuf::Message& m) {
    google::protobuf::util::JsonPrintOptions opts;
    opts.preserve_proto_field_names = true;
    std::string out;
    auto st = google::protobuf::util::MessageToJsonString(m, &out, opts);
    if (!st.ok()) return "{}";  // only fails on invalid UTF-8 etc.; never for our messages
    return out;
}

bool from_json(const std::string& json, google::protobuf::Message& m, std::string* error) {
    google::protobuf::util::JsonParseOptions opts;
    opts.ignore_unknown_fields = true;
    m.Clear();
    auto st = google::protobuf::util::JsonStringToMessage(json, &m, opts);
    if (!st.ok()) {
        if (error) *error = std::string(st.message());
        return false;
    }
    return true;
}

}  // namespace tdr::cpapi
