// IControlClient over http::AsyncClient with proto3 JSON bodies (phase2.md §3).
// Kept in its own translation unit so code that only uses NodeAgent with a
// fake client does not pull in the HTTPS stack at link time.
#include <google/protobuf/util/json_util.h>

#include <format>

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
        call<cv1::NodeInfo>("POST", "/api/v1/nodes/" + node_id + "/offline", to_json(req), std::move(cb));
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
