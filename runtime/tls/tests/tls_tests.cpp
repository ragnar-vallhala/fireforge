#include <boost/test/unit_test.hpp>

#include <sys/stat.h>

#include <cstdio>
#include <ctime>
#include <filesystem>
#include <memory>
#include <ostream>
#include <string>

#include <openssl/pem.h>
#include <openssl/ssl.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>

#include "tdr/common/common.hpp"
#include "tdr/tls/tls.hpp"

namespace fs = std::filesystem;
using namespace tdr::tls;

namespace tdr::tls {
inline std::ostream& operator<<(std::ostream& os, Role r) { return os << to_string(r); }
}  // namespace tdr::tls

namespace {

struct TempDir {
    fs::path path = fs::path("/tmp/claude-1000") / tdr::new_id("tls-test");
    TempDir() { fs::create_directories(path); }
    ~TempDir() {
        std::error_code ec;
        fs::remove_all(path, ec);
    }
};

struct X509Free {
    void operator()(X509* x) const { X509_free(x); }
};
using X509Ptr = std::unique_ptr<X509, X509Free>;

X509Ptr load(const fs::path& p) {
    FILE* f = std::fopen(p.c_str(), "r");
    BOOST_REQUIRE(f);
    X509* x = PEM_read_X509(f, nullptr, nullptr, nullptr);
    std::fclose(f);
    BOOST_REQUIRE(x);
    return X509Ptr(x);
}

unsigned mode_of(const fs::path& p) {
    struct stat st{};
    BOOST_REQUIRE(::stat(p.c_str(), &st) == 0);
    return st.st_mode & 0777;
}

// X509_verify_cert against a store holding only ca; optional check time.
int verify(X509* ca, X509* leaf, int purpose, std::time_t at = 0) {
    X509_STORE* store = X509_STORE_new();
    X509_STORE_add_cert(store, ca);
    X509_STORE_CTX* ctx = X509_STORE_CTX_new();
    X509_STORE_CTX_init(ctx, store, leaf, nullptr);
    X509_STORE_CTX_set_purpose(ctx, purpose);
    if (at != 0) X509_VERIFY_PARAM_set_time(X509_STORE_CTX_get0_param(ctx), at);
    int ok = X509_verify_cert(ctx);
    int err = ok == 1 ? X509_V_OK : X509_STORE_CTX_get_error(ctx);
    X509_STORE_CTX_free(ctx);
    X509_STORE_free(store);
    return err;
}

std::string run(const std::string& cmd) {
    std::string out;
    FILE* p = ::popen(cmd.c_str(), "r");
    if (!p) return out;
    char buf[512];
    while (std::fgets(buf, sizeof buf, p)) out += buf;
    ::pclose(p);
    return out;
}

}  // namespace

BOOST_AUTO_TEST_CASE(tls_create_ca_files_and_modes) {
    TempDir t;
    auto r = create_ca(t.path, "fireforge-ca");
    BOOST_REQUIRE_MESSAGE(r, (r ? "" : r.error()));
    BOOST_TEST(mode_of(t.path / "ca.key") == 0600u);
    BOOST_TEST(mode_of(t.path / "ca.crt") == 0644u);
    auto ca = load(t.path / "ca.crt");
    BOOST_TEST(X509_check_ca(ca.get()) >= 1);
    BOOST_TEST(verify(ca.get(), ca.get(), X509_PURPOSE_ANY) == X509_V_OK);
    // Refuses to overwrite.
    auto again = create_ca(t.path, "fireforge-ca");
    BOOST_REQUIRE(!again);
    BOOST_TEST(again.error().find("overwrite") != std::string::npos);
}

BOOST_AUTO_TEST_CASE(tls_issue_all_roles_and_identity) {
    TempDir t;
    BOOST_REQUIRE(create_ca(t.path, "ca"));
    auto ca = load(t.path / "ca.crt");
    struct Case {
        Role role;
        const char* name;
        int purpose;
        bool both;  // node certs are valid for both purposes (phase3.md §2)
    } cases[] = {{Role::kNode, "node-1", X509_PURPOSE_SSL_CLIENT, true},
                 {Role::kAdmin, "alice", X509_PURPOSE_SSL_CLIENT, false},
                 {Role::kServer, "cp.example", X509_PURPOSE_SSL_SERVER, false}};
    for (const auto& c : cases) {
        IssueOptions o;
        o.role = c.role;
        o.name = c.name;
        if (c.role == Role::kServer) o.dns_sans = {"localhost"}, o.ip_sans = {"127.0.0.1"};
        fs::path key = t.path / (std::string(c.name) + ".key"), crt = t.path / (std::string(c.name) + ".crt");
        auto r = issue(t.path, o, key, crt);
        BOOST_REQUIRE_MESSAGE(r, (r ? "" : r.error()));
        BOOST_TEST(mode_of(key) == 0600u);
        auto cert = load(crt);
        BOOST_TEST(verify(ca.get(), cert.get(), c.purpose) == X509_V_OK);
        // Other purpose: fails for admin/server (EKU restricts), passes for node.
        int other = c.purpose == X509_PURPOSE_SSL_CLIENT ? X509_PURPOSE_SSL_SERVER : X509_PURPOSE_SSL_CLIENT;
        if (c.both)
            BOOST_TEST(verify(ca.get(), cert.get(), other) == X509_V_OK);
        else
            BOOST_TEST(verify(ca.get(), cert.get(), other) != X509_V_OK);
        BOOST_TEST(X509_check_ca(cert.get()) == 0);

        auto id = read_identity(crt);
        BOOST_REQUIRE_MESSAGE(id, (id ? "" : id.error()));
        BOOST_TEST(id->name == c.name);
        BOOST_TEST(id->role == c.role);
        BOOST_TEST(id->actor() == std::string(to_string(c.role)) + ":" + c.name);
        auto now = static_cast<std::int64_t>(std::time(nullptr));
        BOOST_TEST(id->not_after_unix > now + 364 * 86400);
        BOOST_TEST(id->not_after_unix < now + 366 * 86400);

        // Fingerprint matches `openssl x509 -fingerprint -sha256`, normalized.
        std::string out = run("openssl x509 -noout -fingerprint -sha256 -in " + crt.string() + " 2>/dev/null");
        if (!out.empty()) {
            auto eq = out.find('=');
            BOOST_REQUIRE(eq != std::string::npos);
            std::string fp;
            for (char ch : out.substr(eq + 1))
                if (std::isxdigit(static_cast<unsigned char>(ch))) fp.push_back(static_cast<char>(std::tolower(ch)));
            BOOST_TEST(fp == id->fingerprint_sha256);
        }
        BOOST_TEST(id->fingerprint_sha256.size() == 64u);
    }
}

BOOST_AUTO_TEST_CASE(tls_role_parsing) {
    BOOST_TEST(parse_role("node").value() == Role::kNode);
    BOOST_TEST(parse_role("admin").value() == Role::kAdmin);
    BOOST_TEST(parse_role("server").value() == Role::kServer);
    BOOST_TEST(!parse_role("ca"));
    BOOST_TEST(!parse_role(""));
}

BOOST_AUTO_TEST_CASE(tls_ca_cert_has_no_role_identity) {
    TempDir t;
    BOOST_REQUIRE(create_ca(t.path, "ca"));
    BOOST_TEST(!read_identity(t.path / "ca.crt"));
    BOOST_TEST(!read_identity(t.path / "missing.crt"));
}

BOOST_AUTO_TEST_CASE(tls_issue_refuses_overwrite_and_bad_names) {
    TempDir t;
    BOOST_REQUIRE(create_ca(t.path, "ca"));
    IssueOptions o;
    o.name = "n1";
    BOOST_REQUIRE(issue(t.path, o, t.path / "n1.key", t.path / "n1.crt"));
    auto again = issue(t.path, o, t.path / "n1.key", t.path / "other.crt");
    BOOST_REQUIRE(!again);
    BOOST_TEST(again.error().find("overwrite") != std::string::npos);
    BOOST_TEST(!fs::exists(t.path / "other.crt"));
    auto again2 = issue(t.path, o, t.path / "other.key", t.path / "n1.crt");
    BOOST_REQUIRE(!again2);
    BOOST_TEST(!fs::exists(t.path / "other.key"));

    for (std::string bad : {std::string(), std::string("a b"), std::string("x/y"), std::string("é"),
                            std::string(65, 'a'), std::string("a,OU=admin")}) {
        IssueOptions b;
        b.name = bad;
        BOOST_TEST(!issue(t.path, b, t.path / "bad.key", t.path / "bad.crt"), "accepted bad name '" << bad << "'");
    }
    IssueOptions ok64;
    ok64.name = std::string(64, 'a');
    BOOST_TEST(issue(t.path, ok64, t.path / "a64.key", t.path / "a64.crt").has_value());
    BOOST_TEST(!create_ca(t.path / "sub", "bad name"));

    IssueOptions badip;
    badip.role = Role::kServer;
    badip.name = "srv";
    badip.ip_sans = {"999.1.1.1"};
    BOOST_TEST(!issue(t.path, badip, t.path / "srv.key", t.path / "srv.crt"));
    BOOST_TEST(!fs::exists(t.path / "srv.key"));

    // Missing CA.
    BOOST_TEST(!issue(t.path / "nope", o, t.path / "x.key", t.path / "x.crt"));
}

BOOST_AUTO_TEST_CASE(tls_cert_from_other_ca_fails_verification) {
    TempDir a, b;
    BOOST_REQUIRE(create_ca(a.path, "ca-a"));
    BOOST_REQUIRE(create_ca(b.path, "ca-b"));
    IssueOptions o;
    o.name = "node-x";
    BOOST_REQUIRE(issue(b.path, o, b.path / "n.key", b.path / "n.crt"));
    auto ca_a = load(a.path / "ca.crt");
    auto ca_b = load(b.path / "ca.crt");
    auto leaf = load(b.path / "n.crt");
    BOOST_TEST(verify(ca_b.get(), leaf.get(), X509_PURPOSE_SSL_CLIENT) == X509_V_OK);
    BOOST_TEST(verify(ca_a.get(), leaf.get(), X509_PURPOSE_SSL_CLIENT) != X509_V_OK);
}

BOOST_AUTO_TEST_CASE(tls_expired_cert_fails_verification) {
    TempDir t;
    BOOST_REQUIRE(create_ca(t.path, "ca"));
    IssueOptions o;
    o.name = "short";
    o.validity = std::chrono::days{1};
    BOOST_REQUIRE(issue(t.path, o, t.path / "s.key", t.path / "s.crt"));
    auto ca = load(t.path / "ca.crt");
    auto leaf = load(t.path / "s.crt");
    BOOST_TEST(verify(ca.get(), leaf.get(), X509_PURPOSE_SSL_CLIENT) == X509_V_OK);
    std::time_t later = std::time(nullptr) + 2 * 86400;
    BOOST_TEST(verify(ca.get(), leaf.get(), X509_PURPOSE_SSL_CLIENT, later) == X509_V_ERR_CERT_HAS_EXPIRED);
}

BOOST_AUTO_TEST_CASE(tls_server_cert_sans) {
    TempDir t;
    BOOST_REQUIRE(create_ca(t.path, "ca"));
    IssueOptions o;
    o.role = Role::kServer;
    o.name = "cp";
    o.dns_sans = {"localhost", "cp.example.org"};
    o.ip_sans = {"127.0.0.1", "::1"};
    BOOST_REQUIRE(issue(t.path, o, t.path / "cp.key", t.path / "cp.crt"));
    auto cert = load(t.path / "cp.crt");
    BOOST_TEST(X509_check_host(cert.get(), "localhost", 0, 0, nullptr) == 1);
    BOOST_TEST(X509_check_host(cert.get(), "cp.example.org", 0, 0, nullptr) == 1);
    BOOST_TEST(X509_check_host(cert.get(), "wrong.example", 0, 0, nullptr) != 1);
    BOOST_TEST(X509_check_ip_asc(cert.get(), "127.0.0.1", 0) == 1);
    BOOST_TEST(X509_check_ip_asc(cert.get(), "::1", 0) == 1);
    BOOST_TEST(X509_check_ip_asc(cert.get(), "10.0.0.1", 0) != 1);

    // Server needs at least one SAN; client certs take none.
    IssueOptions nosan;
    nosan.role = Role::kServer;
    nosan.name = "cp2";
    BOOST_TEST(!issue(t.path, nosan, t.path / "cp2.key", t.path / "cp2.crt"));
}

BOOST_AUTO_TEST_CASE(tls_contexts_load_and_reject_bad_files) {
    TempDir t;
    BOOST_REQUIRE(create_ca(t.path, "ca"));
    IssueOptions s;
    s.role = Role::kServer;
    s.name = "srv";
    s.ip_sans = {"127.0.0.1"};
    BOOST_REQUIRE(issue(t.path, s, t.path / "srv.key", t.path / "srv.crt"));
    IssueOptions n;
    n.name = "n";
    BOOST_REQUIRE(issue(t.path, n, t.path / "n.key", t.path / "n.crt"));

    BOOST_CHECK_NO_THROW(make_server_context({t.path / "ca.crt", t.path / "srv.crt", t.path / "srv.key"}));
    BOOST_CHECK_NO_THROW(make_client_context({t.path / "ca.crt", t.path / "n.crt", t.path / "n.key"}));
    // Mismatched key.
    try {
        make_client_context({t.path / "ca.crt", t.path / "n.crt", t.path / "srv.key"});
        BOOST_FAIL("expected throw");
    } catch (const std::runtime_error& e) {
        BOOST_TEST(std::string(e.what()).find("srv.key") != std::string::npos);
    }
    try {
        make_server_context({t.path / "missing-ca.crt", t.path / "srv.crt", t.path / "srv.key"});
        BOOST_FAIL("expected throw");
    } catch (const std::runtime_error& e) {
        BOOST_TEST(std::string(e.what()).find("missing-ca.crt") != std::string::npos);
    }
    BOOST_CHECK_THROW(make_client_context({t.path / "ca.crt", t.path / "nope.crt", t.path / "n.key"}),
                      std::runtime_error);

    // Peer context: node cert both ways; bad files name the file.
    std::shared_ptr<boost::asio::ssl::context> peer;
    BOOST_CHECK_NO_THROW(peer = make_peer_context({t.path / "ca.crt", t.path / "n.crt", t.path / "n.key"}));
    if (peer) {
        BOOST_TEST(SSL_CTX_get_min_proto_version(peer->native_handle()) == TLS1_3_VERSION);
        BOOST_TEST((SSL_CTX_get_verify_mode(peer->native_handle()) & SSL_VERIFY_FAIL_IF_NO_PEER_CERT) != 0);
    }
    try {
        make_peer_context({t.path / "ca.crt", t.path / "n.crt", t.path / "srv.key"});
        BOOST_FAIL("expected throw");
    } catch (const std::runtime_error& e) {
        BOOST_TEST(std::string(e.what()).find("srv.key") != std::string::npos);
    }
    try {
        make_peer_context({t.path / "missing-ca.crt", t.path / "n.crt", t.path / "n.key"});
        BOOST_FAIL("expected throw");
    } catch (const std::runtime_error& e) {
        BOOST_TEST(std::string(e.what()).find("missing-ca.crt") != std::string::npos);
    }
}
