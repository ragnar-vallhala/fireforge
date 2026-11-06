// Web dashboard static files served by tdrcp (docs/design/phase9.md §2).
//
// The files under control/ui/web/ (minus tests/) are compiled into the binary.
//   GET /                          302 -> /ui/
//   GET /ui, /ui/, /ui/<no ext>    index.html (the app uses hash routes)
//   GET /ui/<file>                 the asset; content type by extension
// ETag = quoted sha256 prefix, If-None-Match -> 304; Cache-Control no-cache for
// index.html, max-age=3600 otherwise. Every response carries the CSP,
// X-Content-Type-Options and Referrer-Policy headers. GET and HEAD only (else 405).
// Lookup is by exact key, so no request can name a file outside the table.
#pragma once

#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "tdr/http/http.hpp"

namespace tdr::cpui {

// tdrcp.conf [ui]
struct UiOptions {
    bool enabled = true;  // false: serve() returns nullopt for everything (the API then 404s / and /ui/*)
};

inline constexpr const char* kContentSecurityPolicy =
    "default-src 'self'; img-src 'self' data:; style-src 'self'; script-src 'self'; connect-src 'self'; "
    "frame-ancestors 'none'; base-uri 'none'; form-action 'none'";

struct Asset {
    std::string path;          // relative to web/, e.g. "index.html", "pages/jobs.js"
    std::string_view body;     // compiled-in bytes
    std::string content_type;
    std::string etag;          // "\"<16 hex of sha256>\""
};

// "text/javascript; charset=utf-8" etc.; application/octet-stream when unknown.
std::string content_type_for(std::string_view path);

class Ui {
public:
    explicit Ui(UiOptions options = {});

    // A response for "/" and "/ui..." paths, nullopt for anything else (or when disabled).
    std::optional<http::Response> serve(const http::Request& req) const;

    // The embedded files, sorted by path (tests, diagnostics).
    const std::vector<Asset>& assets() const;
    const Asset* find(std::string_view path) const;

private:
    UiOptions options_;
};

}  // namespace tdr::cpui
