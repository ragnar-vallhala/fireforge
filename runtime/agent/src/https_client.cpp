// IControlClient over http::AsyncClient with proto3 JSON bodies (phase2.md §3).
// Kept in its own translation unit so code that only uses NodeAgent with a
// fake client does not pull in the HTTPS stack at link time.
#include <google/protobuf/util/json_util.h>

#include <format>
#include <string_view>

#include "tdr/agent/agent.hpp"

namespace tdr::agent {

namespace {

namespace pbu = google::protobuf::util;

std::string to_json(const google::protobuf::Message& m) {
    std::string out;
    pbu::JsonPrintOptions opts;
    opts.preserve_proto_field_names = true;
    (void)pbu::MessageToJsonString(m, &out, opts);
    return out;
}

template <class Reply>
Result<Reply> decode(const std::expected<http::ClientResponse, std::string>& r) {
    if (!r) return std::unexpected(ClientError{0, r.error()});
    const int status = r->status;
    if (status < 200 || status >= 300) {
        cv1::ApiError err;
        pbu::JsonParseOptions po;
        po.ignore_unknown_fields = true;
        std::string message;
        if (!r->body.empty() && pbu::JsonStringToMessage(r->body, &err, po).ok() && !err.message().empty()) {
            message = err.message();
        } else if (!r->body.empty()) {
            message = r->body.size() > 512 ? r->body.substr(0, 512) + "..." : r->body;
        } else {
            message = std::format("HTTP {}", status);
        }
        return std::unexpected(ClientError{status, std::move(message)});
    }
    Reply reply;
    pbu::JsonParseOptions po;
    po.ignore_unknown_fields = true;
    const std::string& body = r->body.empty() ? std::string("{}") : r->body;
    auto st = pbu::JsonStringToMessage(body, &reply, po);
    if (!st.ok()) return std::unexpected(ClientError{status, "bad response: " + st.ToString()});
    return reply;
}

// Percent-encodes everything except RFC 3986 unreserved characters.
std::string url_encode(std::string_view in) {
    static constexpr char kHex[] = "0123456789ABCDEF";
    std::string out;
    out.reserve(in.size());
    for (unsigned char c : in) {
        if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' || c == '.' ||
            c == '_' || c == '~') {
            out += static_cast<char>(c);
        } else {
            out += '%';
            out += kHex[c >> 4];
            out += kHex[c & 0xF];
        }
    }
    return out;
}

// "artifact://sha256/<hex>" -> "<hex>" (anything else is passed through and
// rejected by the server).
std::string artifact_hex(const std::string& artifact_id) {
    constexpr std::string_view kPrefix = "artifact://sha256/";
    if (artifact_id.starts_with(kPrefix)) return artifact_id.substr(kPrefix.size());
    return artifact_id;
}

Result<void> decode_void(const std::expected<http::ClientResponse, std::string>& r) {
    // Reuse the error mapping; the 2xx body (normally "{}") is ignored.
    if (!r) return std::unexpected(ClientError{0, r.error()});
    if (r->status >= 200 && r->status < 300) return {};
    return std::unexpected(decode<cv1::ApiError>(r).error());  // non-2xx: always an error
}

class HttpsControlClient final : public IControlClient {
public:
    HttpsControlClient(boost::asio::io_context& io, std::shared_ptr<boost::asio::ssl::context> ctx,
                       http::ClientOptions options)
        : client_(io, std::move(ctx), std::move(options)) {}

    void register_node(const cv1::RegisterRequest& req, std::function<void(Result<cv1::RegisterReply>)> cb) override {
        call<cv1::RegisterReply>("POST", "/api/v1/nodes/register", to_json(req), std::move(cb));
    }
    void heartbeat(const cv1::Heartbeat& hb, std::function<void(Result<cv1::HeartbeatReply>)> cb) override {
        call<cv1::HeartbeatReply>("POST", "/api/v1/nodes/heartbeat", to_json(hb), std::move(cb));
    }
    void list_nodes(std::function<void(Result<cv1::NodeList>)> cb) override {
        call<cv1::NodeList>("GET", "/api/v1/nodes", {}, std::move(cb));
    }
    void offline(const std::string& node_id, const std::string& reason,
                 std::function<void(Result<cv1::NodeInfo>)> cb) override {
        cv1::OfflineRequest req;
        req.set_reason(reason);
        call<cv1::NodeInfo>("POST", "/api/v1/nodes/" + url_encode(node_id) + "/offline", to_json(req), std::move(cb));
    }

    void submit_job(const cv1::CreateJobRequest& req, std::function<void(Result<cv1::Job>)> cb) override {
        call<cv1::Job>("POST", "/api/v1/jobs", to_json(req), std::move(cb));
    }
    void get_job(const std::string& job_id, std::function<void(Result<cv1::Job>)> cb) override {
        call<cv1::Job>("GET", "/api/v1/jobs/" + url_encode(job_id), {}, std::move(cb));
    }
    void list_jobs(::fireforge::tdr::v1::JobState state, const std::string& campaign_id, std::uint32_t limit,
                   std::function<void(Result<cv1::JobList>)> cb) override {
        std::string target = "/api/v1/jobs";
        char sep = '?';
        auto add = [&](std::string_view key, const std::string& value) {
            target += sep;
            target += key;
            target += '=';
            target += url_encode(value);
            sep = '&';
        };
        if (state != ::fireforge::tdr::v1::JOB_STATE_UNSPECIFIED) {
            add("state", ::fireforge::tdr::v1::JobState_Name(state));
        }
        if (!campaign_id.empty()) add("campaign", campaign_id);
        if (limit > 0) add("limit", std::to_string(limit));
        call<cv1::JobList>("GET", target, {}, std::move(cb));
    }
    void cancel_job(const std::string& job_id, const std::string& reason,
                    std::function<void(Result<cv1::Job>)> cb) override {
        cv1::CancelRequest req;
        req.set_reason(reason);
        call<cv1::Job>("POST", "/api/v1/jobs/" + url_encode(job_id) + "/cancel", to_json(req), std::move(cb));
    }
    void lease(const cv1::LeaseRequest& req, std::function<void(Result<cv1::LeaseReply>)> cb) override {
        call<cv1::LeaseReply>("POST", "/api/v1/leases", to_json(req), std::move(cb));
    }
    void update_job(const std::string& job_id, const cv1::JobUpdate& update,
                    std::function<void(Result<cv1::Job>)> cb) override {
        call<cv1::Job>("POST", "/api/v1/jobs/" + url_encode(job_id) + "/status", to_json(update), std::move(cb));
    }
    void announce_artifacts(const cv1::ArtifactAnnouncement& a, std::function<void(Result<void>)> cb) override {
        client_.request("POST", "/api/v1/artifacts/announce", to_json(a),
                        [cb = std::move(cb)](std::expected<http::ClientResponse, std::string> r) {
                            cb(decode_void(r));
                        });
    }

    void get_artifact(const std::string& artifact_id, std::function<void(Result<cv1::ArtifactInfo>)> cb) override {
        call<cv1::ArtifactInfo>("GET", "/api/v1/artifacts/sha256/" + url_encode(artifact_hex(artifact_id)), {},
                                std::move(cb));
    }
    void release_artifact(const std::string& artifact_id, const cv1::ReleaseRequest& req,
                          std::function<void(Result<cv1::ReleaseReply>)> cb) override {
        call<cv1::ReleaseReply>("POST",
                                "/api/v1/artifacts/sha256/" + url_encode(artifact_hex(artifact_id)) + "/release",
                                to_json(req), std::move(cb));
    }

private:
    template <class Reply>
    void call(const char* method, const std::string& target, std::string body,
              std::function<void(Result<Reply>)> cb) {
        client_.request(method, target, std::move(body),
                        [cb = std::move(cb)](std::expected<http::ClientResponse, std::string> r) {
                            cb(decode<Reply>(r));
                        });
    }

    http::AsyncClient client_;
};

}  // namespace

std::unique_ptr<IControlClient> make_https_control_client(boost::asio::io_context& io,
                                                          std::shared_ptr<boost::asio::ssl::context> ctx,
                                                          http::ClientOptions options) {
    return std::make_unique<HttpsControlClient>(io, std::move(ctx), std::move(options));
}

}  // namespace tdr::agent
