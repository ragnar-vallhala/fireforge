// FireForge PKI and TLS (docs/design/phase2.md §2).
//
// A private CA issues certificates whose subject is
//   CN=<name>, OU=<role>, O=FireForge
// Keys are ECDSA P-256, PEM files, private keys written 0600.
// TLS 1.3 only; both sides verify the peer chains to the same CA.
#pragma once

#include <chrono>
#include <cstdint>
#include <expected>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <boost/asio/ssl/context.hpp>

typedef struct x509_st X509;
typedef struct ssl_st SSL;

namespace tdr::tls {

namespace fs = std::filesystem;

enum class Role { kNode, kAdmin, kServer };
const char* to_string(Role r);                       // "node" | "admin" | "server"
std::optional<Role> parse_role(std::string_view s);

struct Identity {
    Role role = Role::kNode;
    std::string name;                 // CN
    std::string fingerprint_sha256;   // lowercase hex of SHA-256(cert DER)
    std::int64_t not_after_unix = 0;
    std::string actor() const;        // "<role>:<name>", used in audit records
};

// ---- PKI (used by `tdrctl pki`) -----------------------------------------------

// Writes <dir>/ca.key (0600) and <dir>/ca.crt. Refuses to overwrite an existing ca.key.
std::expected<void, std::string> create_ca(const fs::path& dir, const std::string& ca_name,
                                           std::chrono::days validity = std::chrono::days{3650});

struct IssueOptions {
    Role role = Role::kNode;
    std::string name;                         // CN; [A-Za-z0-9._-]{1,64}
    std::vector<std::string> dns_sans;        // server certs
    std::vector<std::string> ip_sans;         // server certs (IPv4/IPv6 literals)
    std::chrono::days validity{365};
};

// Generates a key pair and a certificate signed by <ca_dir>/ca.{key,crt}.
// EKU: serverAuth+clientAuth for node (peer links, phase3.md §2), clientAuth
// for admin, serverAuth for server. Random 128-bit serial.
// Writes out_key (0600) and out_cert; refuses to overwrite either.
std::expected<void, std::string> issue(const fs::path& ca_dir, const IssueOptions& opts,
                                       const fs::path& out_key, const fs::path& out_cert);

// Extract identity from a certificate. Fails if OU is not a known role or CN is empty.
std::expected<Identity, std::string> identity_of(X509* cert);
std::expected<Identity, std::string> read_identity(const fs::path& cert_pem);

// ---- TLS contexts -------------------------------------------------------------

struct Credentials {
    fs::path ca_file;
    fs::path cert_file;
    fs::path key_file;
};

// TLS 1.3 only. Loads cert + key (checks they match) and trusts only ca_file.
// Server: verify_peer | verify_fail_if_no_peer_cert (mTLS required).
// Client: verify_peer; the caller sets the expected host (see set_verify_host).
// Throws std::runtime_error with a readable message on bad files.
std::shared_ptr<boost::asio::ssl::context> make_server_context(const Credentials& creds);
std::shared_ptr<boost::asio::ssl::context> make_client_context(const Credentials& creds);

// Peer links (phase3.md §2): one context for both accepting and dialing.
// TLS 1.3 only, node cert + key, trusts only ca_file, verify_peer |
// verify_fail_if_no_peer_cert. No hostname check: callers compare the peer
// identity name themselves. Requires a node cert with serverAuth+clientAuth.
std::shared_ptr<boost::asio::ssl::context> make_peer_context(const Credentials& creds);

// Configure an SSL* (client side) for SNI and RFC 2818 host/IP verification
// against the server cert's SANs. Returns false on failure.
bool set_verify_host(SSL* ssl, const std::string& host);

// Identity of the verified peer of an established session, or nullopt.
std::optional<Identity> peer_identity(SSL* ssl);

}  // namespace tdr::tls
