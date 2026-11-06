// tdr_cpui (phase9.md §2, §5). web/ is in flux, so these iterate the embedded table
// and compare it with the source directory rather than naming files.
#include <algorithm>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <set>
#include <string>

#include <boost/test/unit_test.hpp>

#include "tdr/cpui/ui.hpp"

namespace fs = std::filesystem;
using namespace tdr;
using tdr::cpui::Ui;

#ifndef TDR_UI_WEB_DIR
#error "TDR_UI_WEB_DIR must point at control/ui/web"
#endif

namespace {

const fs::path kWeb = TDR_UI_WEB_DIR;

http::Request get(const std::string& path, std::string method = "GET") {
    http::Request r;
    r.method = std::move(method);
    r.path = path;
    tls::Identity id;
    id.role = tls::Role::kViewer;
    id.name = "vera";
    r.peer = id;
    return r;
}

std::string header(const http::Response& r, const std::string& name) {
    for (const auto& [k, v] : r.headers)
        if (k == name) return v;
    return {};
}

void check_security_headers(const http::Response& r) {
    BOOST_TEST(header(r, "Content-Security-Policy") == cpui::kContentSecurityPolicy);
    BOOST_TEST(header(r, "X-Content-Type-Options") == "nosniff");
    BOOST_TEST(header(r, "Referrer-Policy") == "no-referrer");
}

// What embed.cmake should have picked up: every file except tests/ and dot-files.
std::set<std::string> source_files() {
    std::set<std::string> out;
    if (!fs::is_directory(kWeb)) return out;
    for (const auto& e : fs::recursive_directory_iterator(kWeb)) {
        if (!e.is_regular_file()) continue;
        const auto rel = fs::relative(e.path(), kWeb).generic_string();
        if (rel.starts_with("tests/") || e.path().filename().string().starts_with(".")) continue;
        out.insert(rel);
    }
    return out;
}

std::string read_file(const fs::path& p) {
    std::ifstream in(p, std::ios::binary);
    return std::string(std::istreambuf_iterator<char>(in), {});
}

bool has_index() { return fs::exists(kWeb / "index.html"); }

}  // namespace

BOOST_AUTO_TEST_CASE(ui_table_matches_web_directory) {
    Ui ui;
    std::set<std::string> embedded;
    for (const auto& a : ui.assets()) {
        embedded.insert(a.path);
        BOOST_TEST_INFO(a.path);
        BOOST_TEST(!a.path.starts_with("tests/"));
        BOOST_TEST(a.etag.size() == 18u);  // quotes + 16 hex
        BOOST_TEST(a.etag.front() == '"');
        // Content as on disk (the build re-embeds edited files).
        if (fs::exists(kWeb / a.path)) BOOST_TEST((std::string(a.body) == read_file(kWeb / a.path)));
    }
    BOOST_TEST(embedded == source_files(), "embedded table differs from control/ui/web (stale build?)");
    BOOST_TEST(std::is_sorted(ui.assets().begin(), ui.assets().end(),
                              [](const auto& x, const auto& y) { return x.path < y.path; }));
    if (has_index()) BOOST_TEST(ui.find("index.html") != nullptr);
}

BOOST_AUTO_TEST_CASE(ui_every_asset_served_with_type_etag_and_cache) {
    Ui ui;
    for (const auto& a : ui.assets()) {
        BOOST_TEST_INFO(a.path);
        auto r = ui.serve(get("/ui/" + a.path));
        BOOST_REQUIRE(r);
        BOOST_TEST(r->status == 200);
        BOOST_TEST((r->body == a.body));
        BOOST_TEST(r->content_type == cpui::content_type_for(a.path));
        BOOST_TEST(header(*r, "ETag") == a.etag);
        BOOST_TEST(header(*r, "Cache-Control") == (a.path == "index.html" ? "no-cache" : "max-age=3600"));
        check_security_headers(*r);

        auto req = get("/ui/" + a.path);
        req.headers["if-none-match"] = a.etag;
        auto nm = ui.serve(req);
        BOOST_REQUIRE(nm);
        BOOST_TEST(nm->status == 304);
        BOOST_TEST(nm->body.empty());
        BOOST_TEST(header(*nm, "ETag") == a.etag);
        check_security_headers(*nm);

        req.headers["if-none-match"] = "\"0000000000000000\", W/" + a.etag;
        BOOST_TEST(ui.serve(req)->status == 304);
        req.headers["if-none-match"] = "\"0000000000000000\"";
        BOOST_TEST(ui.serve(req)->status == 200);

        auto h = ui.serve(get("/ui/" + a.path, "HEAD"));
        BOOST_TEST(h->status == 200);
        BOOST_TEST(header(*h, "ETag") == a.etag);
    }
}

BOOST_AUTO_TEST_CASE(ui_content_types) {
    BOOST_TEST(cpui::content_type_for("index.html") == "text/html; charset=utf-8");
    BOOST_TEST(cpui::content_type_for("pages/jobs.js") == "text/javascript; charset=utf-8");
    BOOST_TEST(cpui::content_type_for("style.css") == "text/css; charset=utf-8");
    BOOST_TEST(cpui::content_type_for("a/logo.svg") == "image/svg+xml");
    BOOST_TEST(cpui::content_type_for("x.json") == "application/json");
    BOOST_TEST(cpui::content_type_for("f.woff2") == "font/woff2");
    BOOST_TEST(cpui::content_type_for("i.PNG") == "image/png");
    BOOST_TEST(cpui::content_type_for("VENDORED.md") == "text/plain; charset=utf-8");
    BOOST_TEST(cpui::content_type_for("notes.txt") == "text/plain; charset=utf-8");
    BOOST_TEST(cpui::content_type_for("vendor/uplot/LICENSE") == "application/octet-stream");
    BOOST_TEST(cpui::content_type_for("a.b/c") == "application/octet-stream");
}

BOOST_AUTO_TEST_CASE(ui_root_redirects) {
    Ui ui;
    for (const char* p : {"/", "/ui"}) {
        auto r = ui.serve(get(p));
        BOOST_REQUIRE(r);
        BOOST_TEST(r->status == 302);
        BOOST_TEST(header(*r, "Location") == "/ui/");
        check_security_headers(*r);
        BOOST_TEST(ui.serve(get(p, "HEAD"))->status == 302);
    }
}

BOOST_AUTO_TEST_CASE(ui_index_fallback_for_extensionless_paths) {
    Ui ui;
    for (const char* p : {"/ui/", "/ui/materials", "/ui/jobs/job-1", "/ui/nodes/"}) {
        BOOST_TEST_INFO(p);
        auto r = ui.serve(get(p));
        BOOST_REQUIRE(r);
        check_security_headers(*r);
        if (has_index()) {
            BOOST_TEST(r->status == 200);
            BOOST_TEST(r->content_type == "text/html; charset=utf-8");
            BOOST_TEST(header(*r, "Cache-Control") == "no-cache");
            BOOST_TEST((r->body == ui.find("index.html")->body));
        } else {
            BOOST_TEST(r->status == 404);
        }
    }
}

BOOST_AUTO_TEST_CASE(ui_missing_files_and_traversal_are_404) {
    Ui ui;
    for (const char* p : {"/ui/nope.js", "/ui/pages/missing.css", "/ui/../x", "/ui/../CMakeLists.txt",
                          "/ui/%2e%2e/", "/ui/%2e%2e/embed.cmake", "/ui/./index.html", "/ui//index.html",
                          "/ui/lib/../index.html", "/ui/..", "/ui/a\\b", "/ui/tests/x.test.js"}) {
        BOOST_TEST_INFO(p);
        auto r = ui.serve(get(p));
        BOOST_REQUIRE(r);
        BOOST_TEST(r->status == 404);
        BOOST_TEST(r->content_type.starts_with("text/plain"));
        check_security_headers(*r);
    }
}

BOOST_AUTO_TEST_CASE(ui_other_methods_405) {
    Ui ui;
    for (const char* m : {"POST", "PUT", "DELETE", "PATCH", "OPTIONS"}) {
        for (const char* p : {"/", "/ui/", "/ui/index.html"}) {
            auto r = ui.serve(get(p, m));
            BOOST_REQUIRE(r);
            BOOST_TEST(r->status == 405);
            BOOST_TEST(header(*r, "Allow") == "GET, HEAD");
            check_security_headers(*r);
        }
    }
}

BOOST_AUTO_TEST_CASE(ui_non_ui_paths_and_disabled) {
    Ui ui;
    for (const char* p : {"/api/v1/nodes", "/healthz", "/uix", "/u", "/api/v1/whoami"})
        BOOST_TEST(!ui.serve(get(p)), p);
    Ui off(cpui::UiOptions{.enabled = false});
    for (const char* p : {"/", "/ui", "/ui/", "/ui/index.html", "/api/v1/nodes"}) BOOST_TEST(!off.serve(get(p)), p);
}

BOOST_AUTO_TEST_CASE(ui_requires_peer_identity) {
    Ui ui;
    http::Request r;
    r.method = "GET";
    r.path = "/ui/";
    auto out = ui.serve(r);
    BOOST_REQUIRE(out);
    BOOST_TEST(out->status == 401);
    check_security_headers(*out);
}
