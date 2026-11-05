#include <charconv>
#include <ranges>

#include <boost/json.hpp>

#include "tdr/http/http.hpp"

namespace tdr::http {

namespace {

std::vector<std::string> split_path(const std::string& path) {
    std::vector<std::string> out;
    for (auto part : std::views::split(path, '/')) {
        std::string s(part.begin(), part.end());
        if (!s.empty()) out.push_back(std::move(s));
    }
    return out;
}

bool is_param(const std::string& seg) { return seg.size() > 2 && seg.front() == '{' && seg.back() == '}'; }

}  // namespace

Response error_response(int status, const std::string& code, const std::string& message) {
    boost::json::object o;
    o["code"] = code;
    o["message"] = message;
    return Response{status, boost::json::serialize(o), "application/json"};
}

void Router::add(const std::string& method, const std::string& pattern, Handler handler) {
    routes_.push_back(Route{method, split_path(pattern), std::move(handler)});
}

Response Router::dispatch(Request& req) const {
    const auto segs = split_path(req.path);
    bool path_matched = false;
    for (const auto& r : routes_) {
        if (r.segments.size() != segs.size()) continue;
        std::map<std::string, std::string> params;
        bool ok = true;
        for (std::size_t i = 0; i < segs.size() && ok; ++i) {
            if (is_param(r.segments[i])) params[r.segments[i].substr(1, r.segments[i].size() - 2)] = segs[i];
            else ok = r.segments[i] == segs[i];
        }
        if (!ok) continue;
        path_matched = true;
        if (r.method != req.method) continue;
        req.params = std::move(params);
        return r.handler(req);
    }
    if (path_matched) return error_response(405, "method_not_allowed", req.method + " not allowed on " + req.path);
    return error_response(404, "not_found", "no route for " + req.path);
}

std::expected<ClientOptions, std::string> parse_url(const std::string& url) {
    constexpr std::string_view scheme = "https://";
    if (!url.starts_with(scheme)) return std::unexpected("URL must start with https://: " + url);
    std::string rest = url.substr(scheme.size());
    if (auto slash = rest.find('/'); slash != std::string::npos) {
        if (rest.substr(slash) != "/") return std::unexpected("URL must not contain a path: " + url);
        rest.resize(slash);
    }
    ClientOptions o;
    std::string port_s;
    if (rest.starts_with('[')) {
        auto close = rest.find(']');
        if (close == std::string::npos) return std::unexpected("bad IPv6 literal in " + url);
        o.host = rest.substr(1, close - 1);
        std::string after = rest.substr(close + 1);
        if (!after.empty()) {
            if (after.front() != ':') return std::unexpected("bad URL: " + url);
            port_s = after.substr(1);
        }
    } else {
        auto colon = rest.rfind(':');
        if (colon != std::string::npos) {
            o.host = rest.substr(0, colon);
            port_s = rest.substr(colon + 1);
        } else {
            o.host = rest;
        }
    }
    if (o.host.empty()) return std::unexpected("missing host in " + url);
    if (!port_s.empty()) {
        unsigned v = 0;
        auto [p, ec] = std::from_chars(port_s.data(), port_s.data() + port_s.size(), v);
        if (ec != std::errc{} || p != port_s.data() + port_s.size() || v == 0 || v > 65535)
            return std::unexpected("bad port in " + url);
        o.port = static_cast<std::uint16_t>(v);
    }
    o.verify_host = o.host;
    return o;
}

}  // namespace tdr::http
