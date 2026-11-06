// Ui: serves the compiled-in dashboard files (docs/design/phase9.md §2).
#include "tdr/cpui/ui.hpp"

#include <algorithm>
#include <array>
#include <cstdio>

#include <openssl/evp.h>

#include "embedded.hpp"

namespace tdr::cpui {

namespace {

constexpr std::string_view kIndex = "index.html";

std::string etag_of(std::string_view body) {
    std::array<unsigned char, EVP_MAX_MD_SIZE> md{};
    unsigned int n = 0;
    if (EVP_Digest(body.data(), body.size(), md.data(), &n, EVP_sha256(), nullptr) != 1) n = 0;
    std::string hex;
    for (unsigned int i = 0; i < n && hex.size() < 16; ++i) {
        char b[3];
        std::snprintf(b, sizeof b, "%02x", md[i]);
        hex += b;
    }
    return "\"" + hex + "\"";
}

std::vector<Asset> build_table() {
    std::vector<Asset> out;
    out.reserve(detail::kEmbeddedCount);
    for (std::size_t i = 0; i < detail::kEmbeddedCount; ++i) {
        const auto& f = detail::kEmbedded[i];
        Asset a;
        a.path = f.path;
        a.body = std::string_view(reinterpret_cast<const char*>(f.data), f.size);
        a.content_type = content_type_for(a.path);
        a.etag = etag_of(a.body);
        out.push_back(std::move(a));
    }
    std::sort(out.begin(), out.end(), [](const Asset& x, const Asset& y) { return x.path < y.path; });
    return out;
}

const std::vector<Asset>& table() {
    static const std::vector<Asset> t = build_table();
    return t;
}

void add_security_headers(http::Response& r) {
    r.headers.emplace_back("Content-Security-Policy", kContentSecurityPolicy);
    r.headers.emplace_back("X-Content-Type-Options", "nosniff");
    r.headers.emplace_back("Referrer-Policy", "no-referrer");
}

http::Response plain(int status, std::string body) {
    http::Response r{status, std::move(body), "text/plain; charset=utf-8"};
    r.headers.emplace_back("Cache-Control", "no-cache");
    return r;
}

// Only plain relative paths: no empty, "." or ".." segments, no escapes or backslashes.
bool safe_key(std::string_view key) {
    if (key.find_first_of("%\\") != std::string_view::npos) return false;
    if (key.find('\0') != std::string_view::npos) return false;
    std::size_t start = 0;
    while (start <= key.size()) {
        auto end = key.find('/', start);
        if (end == std::string_view::npos) end = key.size();
        const auto seg = key.substr(start, end - start);
        // A trailing "/" (empty last segment) is allowed: "/ui/jobs/" falls back to index.
        if ((seg.empty() && end != key.size()) || seg == "." || seg == "..") return false;
        start = end + 1;
    }
    return true;
}

bool has_extension(std::string_view key) {
    const auto slash = key.rfind('/');
    const auto last = slash == std::string_view::npos ? key : key.substr(slash + 1);
    return last.find('.') != std::string_view::npos;
}

std::string_view trim(std::string_view s) {
    while (!s.empty() && (s.front() == ' ' || s.front() == '\t')) s.remove_prefix(1);
    while (!s.empty() && (s.back() == ' ' || s.back() == '\t')) s.remove_suffix(1);
    return s;
}

// If-None-Match: "*" or a list of (possibly weak) entity tags.
bool etag_matches(std::string_view header, std::string_view etag) {
    std::size_t start = 0;
    while (start <= header.size()) {
        auto end = header.find(',', start);
        if (end == std::string_view::npos) end = header.size();
        auto t = trim(header.substr(start, end - start));
        if (t.starts_with("W/")) t.remove_prefix(2);
        if (t == "*" || t == etag) return true;
        start = end + 1;
    }
    return false;
}

}  // namespace

std::string content_type_for(std::string_view path) {
    const auto dot = path.rfind('.');
    const auto slash = path.rfind('/');
    if (dot == std::string_view::npos || (slash != std::string_view::npos && dot < slash))
        return "application/octet-stream";
    std::string ext(path.substr(dot + 1));
    for (auto& c : ext) c = static_cast<char>(c >= 'A' && c <= 'Z' ? c - 'A' + 'a' : c);
    if (ext == "html" || ext == "htm") return "text/html; charset=utf-8";
    if (ext == "js" || ext == "mjs") return "text/javascript; charset=utf-8";
    if (ext == "css") return "text/css; charset=utf-8";
    if (ext == "svg") return "image/svg+xml";
    if (ext == "json" || ext == "map") return "application/json";
    if (ext == "woff2") return "font/woff2";
    if (ext == "png") return "image/png";
    if (ext == "ico") return "image/x-icon";
    if (ext == "txt" || ext == "md") return "text/plain; charset=utf-8";
    return "application/octet-stream";
}

Ui::Ui(UiOptions options) : options_(options) { (void)table(); }

const std::vector<Asset>& Ui::assets() const { return table(); }

const Asset* Ui::find(std::string_view path) const {
    const auto& t = table();
    auto it = std::lower_bound(t.begin(), t.end(), path, [](const Asset& a, std::string_view p) { return a.path < p; });
    return it != t.end() && it->path == path ? &*it : nullptr;
}

std::optional<http::Response> Ui::serve(const http::Request& req) const {
    const std::string& path = req.path;
    if (path != "/" && path != "/ui" && !path.starts_with("/ui/")) return std::nullopt;
    if (!options_.enabled) return std::nullopt;

    auto respond = [&]() -> http::Response {
        if (!req.peer) return plain(401, "client certificate required\n");
        if (req.method != "GET" && req.method != "HEAD") {
            auto r = plain(405, "method not allowed\n");
            r.headers.emplace_back("Allow", "GET, HEAD");
            return r;
        }
        if (path == "/" || path == "/ui") {
            http::Response r{302, {}, ""};
            r.headers.emplace_back("Location", "/ui/");
            r.headers.emplace_back("Cache-Control", "no-cache");
            return r;
        }
        const std::string_view key = std::string_view(path).substr(4);  // after "/ui/"
        if (!safe_key(key)) return plain(404, "not found\n");
        const Asset* a = key.empty() ? nullptr : find(key);
        if (!a) {
            if (!key.empty() && has_extension(key)) return plain(404, "not found\n");
            a = find(kIndex);  // hash-routed app: extension-less paths get the shell
            if (!a) return plain(404, "dashboard not built (no index.html)\n");
        }
        const char* cache = a->path == kIndex ? "no-cache" : "max-age=3600";
        if (auto inm = req.header("if-none-match"); !inm.empty() && etag_matches(inm, a->etag)) {
            http::Response r{304, {}, ""};
            r.headers.emplace_back("ETag", a->etag);
            r.headers.emplace_back("Cache-Control", cache);
            return r;
        }
        http::Response r{200, std::string(a->body), a->content_type};
        r.headers.emplace_back("ETag", a->etag);
        r.headers.emplace_back("Cache-Control", cache);
        return r;
    };
    auto r = respond();
    add_security_headers(r);
    return r;
}

}  // namespace tdr::cpui
