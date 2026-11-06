// FireForge PKI and TLS contexts on OpenSSL 3.0 (docs/design/phase2.md §2).
#include "tdr/tls/tls.hpp"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstring>
#include <format>
#include <stdexcept>

#include <openssl/bio.h>
#include <openssl/bn.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/pkcs12.h>
#include <openssl/rand.h>
#include <openssl/ssl.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>

namespace tdr::tls {

namespace {

template <auto Fn>
struct Deleter {
    template <class T>
    void operator()(T* p) const { Fn(p); }
};

using PkeyPtr = std::unique_ptr<EVP_PKEY, Deleter<EVP_PKEY_free>>;
using X509Ptr = std::unique_ptr<X509, Deleter<X509_free>>;
using BioPtr = std::unique_ptr<BIO, Deleter<BIO_free_all>>;
using BnPtr = std::unique_ptr<BIGNUM, Deleter<BN_free>>;
using ExtPtr = std::unique_ptr<X509_EXTENSION, Deleter<X509_EXTENSION_free>>;
using NamePtr = std::unique_ptr<X509_NAME, Deleter<X509_NAME_free>>;

// Drains the OpenSSL error queue into "<what>: <err1>; <err2>".
std::string ossl_error(std::string_view what) {
    std::string msg(what);
    bool first = true;
    while (unsigned long e = ERR_get_error()) {
        std::array<char, 256> buf{};
        ERR_error_string_n(e, buf.data(), buf.size());
        msg += first ? ": " : "; ";
        msg += buf.data();
        first = false;
    }
    return msg;
}

bool valid_name(std::string_view s) {
    if (s.empty() || s.size() > 64) return false;
    for (char c : s) {
        bool ok = (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '.' ||
                  c == '_' || c == '-';
        if (!ok) return false;
    }
    return true;
}

std::expected<PkeyPtr, std::string> gen_key() {
    PkeyPtr key(EVP_EC_gen("P-256"));
    if (!key) return std::unexpected(ossl_error("EC P-256 key generation failed"));
    return key;
}

std::expected<std::string, std::string> key_pem(EVP_PKEY* key) {
    BioPtr bio(BIO_new(BIO_s_mem()));
    if (!bio || PEM_write_bio_PrivateKey(bio.get(), key, nullptr, nullptr, 0, nullptr, nullptr) != 1)
        return std::unexpected(ossl_error("cannot encode private key"));
    char* data = nullptr;
    long n = BIO_get_mem_data(bio.get(), &data);
    return std::string(data, static_cast<std::size_t>(n));
}

std::expected<std::string, std::string> cert_pem(X509* cert) {
    BioPtr bio(BIO_new(BIO_s_mem()));
    if (!bio || PEM_write_bio_X509(bio.get(), cert) != 1) return std::unexpected(ossl_error("cannot encode certificate"));
    char* data = nullptr;
    long n = BIO_get_mem_data(bio.get(), &data);
    return std::string(data, static_cast<std::size_t>(n));
}

// Creates the file exclusively (no overwrite, no race) with `mode` and writes data.
std::expected<void, std::string> write_new_file(const fs::path& path, const std::string& data, mode_t mode) {
    int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, mode);
    if (fd < 0) {
        if (errno == EEXIST) return std::unexpected(std::format("refusing to overwrite existing {}", path.string()));
        return std::unexpected(std::format("cannot create {}: {}", path.string(), std::strerror(errno)));
    }
    // Enforce the mode regardless of umask.
    (void)::fchmod(fd, mode);
    std::size_t off = 0;
    while (off < data.size()) {
        ssize_t w = ::write(fd, data.data() + off, data.size() - off);
        if (w < 0) {
            if (errno == EINTR) continue;
            int err = errno;
            ::close(fd);
            ::unlink(path.c_str());
            return std::unexpected(std::format("cannot write {}: {}", path.string(), std::strerror(err)));
        }
        off += static_cast<std::size_t>(w);
    }
    if (::fsync(fd) != 0 || ::close(fd) != 0) {
        int err = errno;
        ::unlink(path.c_str());
        return std::unexpected(std::format("cannot write {}: {}", path.string(), std::strerror(err)));
    }
    return {};
}

std::expected<X509Ptr, std::string> load_cert(const fs::path& path) {
    BioPtr bio(BIO_new_file(path.c_str(), "r"));
    if (!bio) return std::unexpected(ossl_error("cannot open certificate " + path.string()));
    X509Ptr cert(PEM_read_bio_X509(bio.get(), nullptr, nullptr, nullptr));
    if (!cert) return std::unexpected(ossl_error("cannot parse certificate " + path.string()));
    return cert;
}

std::expected<PkeyPtr, std::string> load_key(const fs::path& path) {
    BioPtr bio(BIO_new_file(path.c_str(), "r"));
    if (!bio) return std::unexpected(ossl_error("cannot open private key " + path.string()));
    PkeyPtr key(PEM_read_bio_PrivateKey(bio.get(), nullptr, nullptr, nullptr));
    if (!key) return std::unexpected(ossl_error("cannot parse private key " + path.string()));
    return key;
}

std::expected<void, std::string> set_random_serial(X509* cert) {
    std::array<unsigned char, 16> bytes{};
    if (RAND_bytes(bytes.data(), static_cast<int>(bytes.size())) != 1)
        return std::unexpected(ossl_error("RAND_bytes failed"));
    bytes[0] &= 0x7f;  // positive
    bytes[0] |= 0x40;  // full 128-bit-ish width, never zero
    BnPtr bn(BN_bin2bn(bytes.data(), static_cast<int>(bytes.size()), nullptr));
    if (!bn || !BN_to_ASN1_INTEGER(bn.get(), X509_get_serialNumber(cert)))
        return std::unexpected(ossl_error("cannot set serial number"));
    return {};
}

std::expected<NamePtr, std::string> make_name(const std::string& cn, const std::string& ou) {
    NamePtr name(X509_NAME_new());
    auto add = [&](const char* field, const std::string& v) {
        return X509_NAME_add_entry_by_txt(name.get(), field, MBSTRING_UTF8,
                                          reinterpret_cast<const unsigned char*>(v.c_str()), -1, -1, 0) == 1;
    };
    if (!name || !add("O", "FireForge") || !add("OU", ou) || !add("CN", cn))
        return std::unexpected(ossl_error("cannot build subject name"));
    return name;
}

std::expected<void, std::string> add_ext(X509* cert, X509* issuer, int nid, const std::string& value) {
    X509V3_CTX ctx;
    X509V3_set_ctx_nodb(&ctx);
    X509V3_set_ctx(&ctx, issuer, cert, nullptr, nullptr, 0);
    ExtPtr ext(X509V3_EXT_conf_nid(nullptr, &ctx, nid, value.c_str()));
    if (!ext || X509_add_ext(cert, ext.get(), -1) != 1)
        return std::unexpected(ossl_error(std::format("cannot add extension {}={}", OBJ_nid2sn(nid), value)));
    return {};
}

// Builds and signs a certificate. issuer == nullptr means self-signed.
std::expected<X509Ptr, std::string> build_cert(EVP_PKEY* subject_key, const X509_NAME* subject, X509* issuer,
                                               EVP_PKEY* issuer_key, std::chrono::days validity,
                                               const std::vector<std::pair<int, std::string>>& exts) {
    X509Ptr cert(X509_new());
    if (!cert) return std::unexpected(ossl_error("X509_new failed"));
    if (X509_set_version(cert.get(), 2) != 1) return std::unexpected(ossl_error("cannot set version"));
    if (auto r = set_random_serial(cert.get()); !r) return std::unexpected(r.error());
    if (!X509_gmtime_adj(X509_getm_notBefore(cert.get()), 0) ||
        !X509_time_adj_ex(X509_getm_notAfter(cert.get()), static_cast<int>(validity.count()), 0, nullptr))
        return std::unexpected(ossl_error("cannot set validity"));
    if (X509_set_subject_name(cert.get(), subject) != 1 ||
        X509_set_issuer_name(cert.get(), issuer ? X509_get_subject_name(issuer) : subject) != 1)
        return std::unexpected(ossl_error("cannot set names"));
    if (X509_set_pubkey(cert.get(), subject_key) != 1) return std::unexpected(ossl_error("cannot set public key"));
    X509* iss = issuer ? issuer : cert.get();
    for (const auto& [nid, value] : exts)
        if (auto r = add_ext(cert.get(), iss, nid, value); !r) return std::unexpected(r.error());
    if (X509_sign(cert.get(), issuer_key, EVP_sha256()) <= 0) return std::unexpected(ossl_error("signing failed"));
    return cert;
}

std::string name_entry(const X509_NAME* name, int nid) {
    int idx = X509_NAME_get_index_by_NID(name, nid, -1);
    if (idx < 0) return {};
    const ASN1_STRING* s = X509_NAME_ENTRY_get_data(X509_NAME_get_entry(name, idx));
    unsigned char* utf8 = nullptr;
    int n = ASN1_STRING_to_UTF8(&utf8, s);
    if (n < 0) return {};
    std::string out(reinterpret_cast<char*>(utf8), static_cast<std::size_t>(n));
    OPENSSL_free(utf8);
    return out;
}

bool is_ip_literal(const std::string& host) {
    ASN1_OCTET_STRING* ip = a2i_IPADDRESS(host.c_str());
    if (!ip) {
        ERR_clear_error();
        return false;
    }
    ASN1_OCTET_STRING_free(ip);
    return true;
}

[[noreturn]] void fail(const std::string& msg) { throw std::runtime_error(msg); }

// server: require a client certificate (verify_fail_if_no_peer_cert) and
// advertise the CA as acceptable issuer. Peer contexts set server = true and
// are used for both handshake directions (fail_if_no_peer_cert is ignored on
// the client side, where verify_peer already fails without a server cert).
std::shared_ptr<boost::asio::ssl::context> make_context(boost::asio::ssl::context::method method,
                                                        const Credentials& creds, bool server) {
    auto ctx = std::make_shared<boost::asio::ssl::context>(method);
    SSL_CTX* native = ctx->native_handle();
    if (SSL_CTX_set_min_proto_version(native, TLS1_3_VERSION) != 1 ||
        SSL_CTX_set_max_proto_version(native, TLS1_3_VERSION) != 1)
        fail(ossl_error("cannot restrict TLS to 1.3"));
    ERR_clear_error();
    if (SSL_CTX_use_certificate_chain_file(native, creds.cert_file.c_str()) != 1)
        fail(ossl_error("cannot load certificate " + creds.cert_file.string()));
    if (SSL_CTX_use_PrivateKey_file(native, creds.key_file.c_str(), SSL_FILETYPE_PEM) != 1)
        fail(ossl_error("cannot load private key " + creds.key_file.string()));
    if (SSL_CTX_check_private_key(native) != 1)
        fail(ossl_error(std::format("private key {} does not match certificate {}", creds.key_file.string(),
                                    creds.cert_file.string())));
    if (SSL_CTX_load_verify_locations(native, creds.ca_file.c_str(), nullptr) != 1)
        fail(ossl_error("cannot load CA file " + creds.ca_file.string()));
    if (server) {
        ctx->set_verify_mode(boost::asio::ssl::verify_peer | boost::asio::ssl::verify_fail_if_no_peer_cert);
        // Advertise the CA as acceptable client-cert issuer.
        if (STACK_OF(X509_NAME)* names = SSL_load_client_CA_file(creds.ca_file.c_str()))
            SSL_CTX_set_client_CA_list(native, names);
        ERR_clear_error();
    } else {
        ctx->set_verify_mode(boost::asio::ssl::verify_peer);
    }
    return ctx;
}

}  // namespace

const char* to_string(Role r) {
    switch (r) {
        case Role::kNode: return "node";
        case Role::kAdmin: return "admin";
        case Role::kServer: return "server";
        case Role::kViewer: return "viewer";
    }
    return "node";
}

std::optional<Role> parse_role(std::string_view s) {
    if (s == "node") return Role::kNode;
    if (s == "admin") return Role::kAdmin;
    if (s == "server") return Role::kServer;
    if (s == "viewer") return Role::kViewer;
    return std::nullopt;
}

std::string Identity::actor() const { return std::format("{}:{}", to_string(role), name); }

std::expected<void, std::string> create_ca(const fs::path& dir, const std::string& ca_name, std::chrono::days validity) {
    if (!valid_name(ca_name)) return std::unexpected("invalid CA name (want [A-Za-z0-9._-]{1,64}): " + ca_name);
    if (validity.count() < 0) return std::unexpected("negative validity");
    std::error_code ec;
    fs::create_directories(dir, ec);
    if (ec) return std::unexpected(std::format("cannot create {}: {}", dir.string(), ec.message()));
    const fs::path key_path = dir / "ca.key", cert_path = dir / "ca.crt";
    if (fs::exists(key_path, ec)) return std::unexpected("refusing to overwrite existing " + key_path.string());
    if (fs::exists(cert_path, ec)) return std::unexpected("refusing to overwrite existing " + cert_path.string());

    auto key = gen_key();
    if (!key) return std::unexpected(key.error());
    auto name = make_name(ca_name, "ca");
    if (!name) return std::unexpected(name.error());
    auto cert = build_cert(key->get(), name->get(), nullptr, key->get(), validity,
                           {{NID_basic_constraints, "critical,CA:TRUE"},
                            {NID_key_usage, "critical,keyCertSign,cRLSign"},
                            {NID_subject_key_identifier, "hash"},
                            {NID_authority_key_identifier, "keyid:always"}});
    if (!cert) return std::unexpected(cert.error());
    auto kp = key_pem(key->get());
    if (!kp) return std::unexpected(kp.error());
    auto cp = cert_pem(cert->get());
    if (!cp) return std::unexpected(cp.error());
    if (auto r = write_new_file(key_path, *kp, 0600); !r) return r;
    if (auto r = write_new_file(cert_path, *cp, 0644); !r) {
        ::unlink(key_path.c_str());
        return r;
    }
    return {};
}

std::expected<void, std::string> issue(const fs::path& ca_dir, const IssueOptions& opts, const fs::path& out_key,
                                       const fs::path& out_cert) {
    if (!valid_name(opts.name)) return std::unexpected("invalid name (want [A-Za-z0-9._-]{1,64}): " + opts.name);
    if (opts.validity.count() < 0) return std::unexpected("negative validity");
    if (opts.role != Role::kServer && (!opts.dns_sans.empty() || !opts.ip_sans.empty()))
        return std::unexpected("SANs are only allowed on server certificates");
    std::error_code ec;
    if (fs::exists(out_key, ec)) return std::unexpected("refusing to overwrite existing " + out_key.string());
    if (fs::exists(out_cert, ec)) return std::unexpected("refusing to overwrite existing " + out_cert.string());

    auto ca_cert = load_cert(ca_dir / "ca.crt");
    if (!ca_cert) return std::unexpected(ca_cert.error());
    auto ca_key = load_key(ca_dir / "ca.key");
    if (!ca_key) return std::unexpected(ca_key.error());
    if (X509_check_private_key(ca_cert->get(), ca_key->get()) != 1)
        return std::unexpected(ossl_error("CA key does not match CA certificate in " + ca_dir.string()));

    std::vector<std::pair<int, std::string>> exts{
        {NID_basic_constraints, "critical,CA:FALSE"},
        {NID_key_usage, "critical,digitalSignature"},
        {NID_ext_key_usage, opts.role == Role::kServer  ? "serverAuth"
                            : opts.role == Role::kNode  ? "serverAuth,clientAuth"
                                                        : "clientAuth"},
        {NID_subject_key_identifier, "hash"},
        {NID_authority_key_identifier, "keyid:always"},
    };
    if (opts.role == Role::kServer) {
        std::string san;
        for (const auto& d : opts.dns_sans) {
            if (d.empty() || d.find_first_of(", \t") != std::string::npos)
                return std::unexpected("invalid DNS SAN: " + d);
            san += (san.empty() ? "" : ",") + std::string("DNS:") + d;
        }
        for (const auto& ip : opts.ip_sans) {
            if (!is_ip_literal(ip)) return std::unexpected("invalid IP SAN: " + ip);
            san += (san.empty() ? "" : ",") + std::string("IP:") + ip;
        }
        if (san.empty()) return std::unexpected("server certificate needs at least one --dns or --ip SAN");
        exts.emplace_back(NID_subject_alt_name, san);
    }

    auto key = gen_key();
    if (!key) return std::unexpected(key.error());
    auto name = make_name(opts.name, to_string(opts.role));
    if (!name) return std::unexpected(name.error());
    auto cert = build_cert(key->get(), name->get(), ca_cert->get(), ca_key->get(), opts.validity, exts);
    if (!cert) return std::unexpected(cert.error());
    auto kp = key_pem(key->get());
    if (!kp) return std::unexpected(kp.error());
    auto cp = cert_pem(cert->get());
    if (!cp) return std::unexpected(cp.error());
    if (auto r = write_new_file(out_key, *kp, 0600); !r) return r;
    if (auto r = write_new_file(out_cert, *cp, 0644); !r) {
        ::unlink(out_key.c_str());
        return r;
    }
    return {};
}

std::expected<Identity, std::string> identity_of(X509* cert) {
    if (!cert) return std::unexpected("no certificate");
    const X509_NAME* subject = X509_get_subject_name(cert);
    Identity id;
    id.name = name_entry(subject, NID_commonName);
    if (id.name.empty()) return std::unexpected("certificate has no CN");
    std::string ou = name_entry(subject, NID_organizationalUnitName);
    auto role = parse_role(ou);
    if (!role) return std::unexpected("certificate OU is not a known role: '" + ou + "'");
    id.role = *role;

    std::array<unsigned char, EVP_MAX_MD_SIZE> md{};
    unsigned int md_len = 0;
    if (X509_digest(cert, EVP_sha256(), md.data(), &md_len) != 1)
        return std::unexpected(ossl_error("cannot compute certificate fingerprint"));
    static constexpr char kHex[] = "0123456789abcdef";
    id.fingerprint_sha256.reserve(md_len * 2);
    for (unsigned i = 0; i < md_len; ++i) {
        id.fingerprint_sha256.push_back(kHex[md[i] >> 4]);
        id.fingerprint_sha256.push_back(kHex[md[i] & 0xf]);
    }

    time_t t = 0;
    const ASN1_TIME* na = X509_get0_notAfter(cert);
    struct tm tm{};
    if (ASN1_TIME_to_tm(na, &tm) != 1) return std::unexpected("cannot parse notAfter");
    t = timegm(&tm);
    id.not_after_unix = static_cast<std::int64_t>(t);
    return id;
}

std::expected<Identity, std::string> read_identity(const fs::path& cert_pem_path) {
    auto cert = load_cert(cert_pem_path);
    if (!cert) return std::unexpected(cert.error());
    return identity_of(cert->get());
}

std::expected<void, std::string> export_pkcs12(const fs::path& cert_pem_path, const fs::path& key_pem_path,
                                               const fs::path& ca_pem_path, const std::string& password,
                                               const fs::path& out) {
    if (password.empty()) return std::unexpected("PKCS#12 password must not be empty");
    auto cert = load_cert(cert_pem_path);
    if (!cert) return std::unexpected(cert.error());
    auto key = load_key(key_pem_path);
    if (!key) return std::unexpected(key.error());
    auto ca = load_cert(ca_pem_path);
    if (!ca) return std::unexpected(ca.error());
    if (X509_check_private_key(cert->get(), key->get()) != 1)
        return std::unexpected(ossl_error(std::format("{} does not match {}", key_pem_path.string(), cert_pem_path.string())));
    auto id = identity_of(cert->get());
    if (!id) return std::unexpected(id.error());

    struct StackFree {
        void operator()(STACK_OF(X509) * sk) const { sk_X509_free(sk); }  // elements stay owned by `ca`
    };
    std::unique_ptr<STACK_OF(X509), StackFree> chain(sk_X509_new_null());
    if (!chain || sk_X509_push(chain.get(), ca->get()) <= 0) return std::unexpected(ossl_error("cannot build CA chain"));
    // mac_iter -1: no MAC here; PKCS12_set_mac adds an explicit HMAC-SHA256 below.
    using P12Ptr = std::unique_ptr<PKCS12, Deleter<PKCS12_free>>;
    P12Ptr p12(PKCS12_create(password.c_str(), id->name.c_str(), key->get(), cert->get(), chain.get(), NID_aes_256_cbc,
                             NID_aes_256_cbc, PKCS12_DEFAULT_ITER, -1, 0));
    if (!p12) return std::unexpected(ossl_error("PKCS12_create failed"));
    if (PKCS12_set_mac(p12.get(), password.c_str(), -1, nullptr, 0, PKCS12_DEFAULT_ITER, EVP_sha256()) != 1)
        return std::unexpected(ossl_error("PKCS12_set_mac failed"));
    BioPtr bio(BIO_new(BIO_s_mem()));
    if (!bio || i2d_PKCS12_bio(bio.get(), p12.get()) != 1) return std::unexpected(ossl_error("cannot encode PKCS#12"));
    char* data = nullptr;
    long n = BIO_get_mem_data(bio.get(), &data);
    return write_new_file(out, std::string(data, static_cast<std::size_t>(n)), 0600);
}

std::shared_ptr<boost::asio::ssl::context> make_server_context(const Credentials& creds) {
    return make_context(boost::asio::ssl::context::tls_server, creds, true);
}

std::shared_ptr<boost::asio::ssl::context> make_client_context(const Credentials& creds) {
    return make_context(boost::asio::ssl::context::tls_client, creds, false);
}

std::shared_ptr<boost::asio::ssl::context> make_peer_context(const Credentials& creds) {
    // Generic TLS method: the same context accepts and dials. OpenSSL picks
    // the verify purpose by role (sslserver when dialing, sslclient when
    // accepting), so node certs carry both EKUs.
    return make_context(boost::asio::ssl::context::tls, creds, true);
}

bool set_verify_host(SSL* ssl, const std::string& host) {
    if (!ssl || host.empty()) return false;
    if (is_ip_literal(host)) {
        X509_VERIFY_PARAM* param = SSL_get0_param(ssl);
        if (X509_VERIFY_PARAM_set1_ip_asc(param, host.c_str()) != 1) {
            ERR_clear_error();
            return false;
        }
        return true;
    }
    if (SSL_set_tlsext_host_name(ssl, host.c_str()) != 1 || SSL_set1_host(ssl, host.c_str()) != 1) {
        ERR_clear_error();
        return false;
    }
    return true;
}

std::optional<Identity> peer_identity(SSL* ssl) {
    if (!ssl) return std::nullopt;
    X509* cert = SSL_get0_peer_certificate(ssl);
    if (!cert || SSL_get_verify_result(ssl) != X509_V_OK) return std::nullopt;
    auto id = identity_of(cert);
    if (!id) return std::nullopt;
    return *id;
}

}  // namespace tdr::tls
