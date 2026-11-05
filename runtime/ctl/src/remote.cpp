// tdrctl pki ... (local CA / certificate issuing) and tdrctl cp ... (control-plane
// admin over HTTPS with an admin certificate). docs/design/phase2.md §2, §8.

#include "remote.hpp"

#include <algorithm>
#include <cstdlib>
#include <ctime>
#include <filesystem>
#include <format>
#include <iostream>
#include <map>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

#include <google/protobuf/util/json_util.h>

#include "fireforge/control/v1/control.pb.h"
#include "tdr/http/http.hpp"
#include "tdr/tls/tls.hpp"

namespace cv1 = ::fireforge::control::v1;
namespace fs = std::filesystem;

namespace tdrctl {

namespace {

constexpr std::uint64_t kMiB = 1024ull * 1024ull;

struct Args {
    std::vector<std::string> positional;
    std::multimap<std::string, std::string> opts;  // --key value
    std::vector<std::string> flags;                // --key (no value)

    std::optional<std::string> get(const std::string& k) const {
        auto it = opts.find(k);
        if (it == opts.end()) return std::nullopt;
        return it->second;
    }
    std::vector<std::string> all(const std::string& k) const {
        std::vector<std::string> out;
        for (auto [b, e] = opts.equal_range(k); b != e; ++b) out.push_back(b->second);
        return out;
    }
    bool flag(const std::string& k) const { return std::find(flags.begin(), flags.end(), k) != flags.end(); }
};

// `value_opts` lists the options that take a value; anything else starting with -- is a flag.
Args parse(const std::vector<std::string>& in, const std::vector<std::string>& value_opts) {
    Args a;
    for (std::size_t i = 0; i < in.size(); ++i) {
        const auto& s = in[i];
        if (s.starts_with("--")) {
            if (std::find(value_opts.begin(), value_opts.end(), s) != value_opts.end()) {
                if (i + 1 >= in.size()) throw UsageError(s + " needs a value");
                a.opts.emplace(s, in[++i]);
            } else {
                a.flags.push_back(s);
            }
        } else {
            a.positional.push_back(s);
        }
    }
    return a;
}

std::uint64_t to_u64(const std::string& flag, const std::string& v) {
    try {
        std::size_t n = 0;
        auto x = std::stoull(v, &n);
        if (n != v.size() || v.starts_with('-')) throw 0;
        return x;
    } catch (...) {
        throw UsageError(flag + " needs a non-negative integer, got '" + v + "'");
    }
}

std::string to_json(const google::protobuf::Message& m, bool pretty = true) {
    google::protobuf::util::JsonPrintOptions o;
    o.add_whitespace = pretty;
    o.preserve_proto_field_names = true;
    std::string out;
    (void)google::protobuf::util::MessageToJsonString(m, &out, o);
    return out;
}

void from_json(const std::string& body, google::protobuf::Message& m) {
    google::protobuf::util::JsonParseOptions o;
    o.ignore_unknown_fields = true;
    auto st = google::protobuf::util::JsonStringToMessage(body, &m, o);
    if (!st.ok()) throw std::runtime_error("bad response from control plane: " + std::string(st.message()));
}

std::string fmt_time(std::int64_t ms) {
    if (ms <= 0) return "-";
    std::time_t t = ms / 1000;
    std::tm tm{};
    gmtime_r(&t, &tm);
    char buf[32];
    std::strftime(buf, sizeof buf, "%Y-%m-%dT%H:%M:%SZ", &tm);
    return buf;
}

std::string health_name(cv1::NodeHealth h) {
    return h == cv1::NODE_HEALTH_UNSPECIFIED ? "-" : cv1::NodeHealth_Name(h);
}

// ---- pki ------------------------------------------------------------------------

constexpr const char* kPkiUsage = R"(usage:
  tdrctl pki init DIR [--name CA_NAME]
  tdrctl pki issue DIR --role node|admin|server NAME [--dns HOST]... [--ip ADDR]...
                   [--days N] [--out-key PATH] [--out-cert PATH]
  tdrctl pki show CERT
)";

int pki(const std::vector<std::string>& in) {
    if (in.empty()) throw UsageError(std::string("pki needs a subcommand\n") + kPkiUsage);
    std::vector<std::string> rest(in.begin() + 1, in.end());
    const auto& sub = in[0];
    if (sub == "init") {
        auto a = parse(rest, {"--name"});
        if (a.positional.size() != 1) throw UsageError(std::string("pki init needs DIR\n") + kPkiUsage);
        fs::path dir = a.positional[0];
        fs::create_directories(dir);
        auto r = tdr::tls::create_ca(dir, a.get("--name").value_or("FireForge-CA"));
        if (!r) throw std::runtime_error(r.error());
        std::cout << "created CA in " << dir.string() << " (ca.crt, ca.key — keep ca.key offline)\n";
        return 0;
    }
    if (sub == "issue") {
        auto a = parse(rest, {"--role", "--dns", "--ip", "--days", "--out-key", "--out-cert"});
        if (a.positional.size() != 2) throw UsageError(std::string("pki issue needs DIR and NAME\n") + kPkiUsage);
        auto role_s = a.get("--role");
        if (!role_s) throw UsageError("pki issue needs --role node|admin|server");
        auto role = tdr::tls::parse_role(*role_s);
        if (!role) throw UsageError("unknown role '" + *role_s + "'");
        tdr::tls::IssueOptions o;
        o.role = *role;
        o.name = a.positional[1];
        o.dns_sans = a.all("--dns");
        o.ip_sans = a.all("--ip");
        if (auto d = a.get("--days")) o.validity = std::chrono::days{to_u64("--days", *d)};
        fs::path dir = a.positional[0];
        fs::path key = a.get("--out-key").value_or((dir / (o.name + ".key")).string());
        fs::path crt = a.get("--out-cert").value_or((dir / (o.name + ".crt")).string());
        auto r = tdr::tls::issue(dir, o, key, crt);
        if (!r) throw std::runtime_error(r.error());
        std::cout << "issued " << *role_s << " certificate for " << o.name << ": " << crt.string() << ", "
                  << key.string() << "\n";
        return 0;
    }
    if (sub == "show") {
        if (rest.size() != 1) throw UsageError(std::string("pki show needs CERT\n") + kPkiUsage);
        auto id = tdr::tls::read_identity(rest[0]);
        if (!id) throw std::runtime_error(id.error());
        std::cout << "role:        " << tdr::tls::to_string(id->role) << "\n"
                  << "name:        " << id->name << "\n"
                  << "fingerprint: " << id->fingerprint_sha256 << "\n"
                  << "expires:     " << fmt_time(id->not_after_unix * 1000) << "\n";
        return 0;
    }
    throw UsageError("unknown pki subcommand: " + sub + "\n" + kPkiUsage);
}

// ---- cp ---------------------------------------------------------------------------

constexpr const char* kCpUsage = R"(usage: tdrctl cp [--url URL] [--ca F] [--cert F] [--key F] <command> [--json]
  nodes [--health ONLINE|STALE|OFFLINE]
  node ID
  limits ID [--profile small|medium|compute|gpu|none] [--cpus N] [--memory-mb N]
            [--jobs N] [--disk-mb N] [--gpus N]
  offline ID [--reason TEXT]
  events ID [--limit N]
  campaigns [--state DRAFT|ACTIVE|PAUSED|COMPLETED|ARCHIVED]
  campaign ID
  campaign-create NAME [--domain D] [--description TEXT] [--params JSON]
  campaign-set ID [--state S] [--description TEXT] [--params JSON]
  audit [--limit N]

connection: --url/--ca/--cert/--key, else $TDR_CP_URL, $TDR_CP_CA, $TDR_CP_CERT, $TDR_CP_KEY
            (an admin certificate: tdrctl pki issue DIR --role admin NAME)
)";

class Cp {
public:
    Cp(const std::string& url, tdr::tls::Credentials creds) {
        auto o = tdr::http::parse_url(url);
        if (!o) throw UsageError(o.error());
        opts_ = *o;
        ctx_ = tdr::tls::make_client_context(creds);
    }

    // Returns the 2xx body; throws with the API error otherwise.
    std::string call(const std::string& method, const std::string& target, const std::string& body = {}) {
        auto r = tdr::http::request_sync(*ctx_, opts_, method, target, body);
        if (!r) throw std::runtime_error("cannot reach control plane: " + r.error());
        if (r->status / 100 != 2) {
            cv1::ApiError e;
            google::protobuf::util::JsonParseOptions po;
            po.ignore_unknown_fields = true;
            if (google::protobuf::util::JsonStringToMessage(r->body, &e, po).ok() && !e.message().empty())
                throw std::runtime_error(std::format("{} ({}): {}", r->status, e.code(), e.message()));
            throw std::runtime_error(std::format("HTTP {}: {}", r->status, r->body));
        }
        return r->body;
    }

    template <class Msg>
    Msg get(const std::string& method, const std::string& target, const std::string& body = {}) {
        Msg m;
        from_json(call(method, target, body), m);
        return m;
    }

private:
    tdr::http::ClientOptions opts_;
    std::shared_ptr<boost::asio::ssl::context> ctx_;
};

std::string env_or(const char* name, const std::optional<std::string>& v) {
    if (v) return *v;
    const char* e = std::getenv(name);
    return e ? e : "";
}

void print_nodes(const cv1::NodeList& l) {
    std::cout << std::format("{:<26} {:<20} {:<8} {:>9} {:>11} {:>5} {:<20}\n", "NODE_ID", "NAME", "HEALTH", "CPUS",
                             "MEM_MIB", "JOBS", "LAST_SEEN");
    for (const auto& n : l.nodes()) {
        const auto& hb = n.last_heartbeat();
        std::cout << std::format("{:<26} {:<20} {:<8} {:>9} {:>11} {:>5} {:<20}\n", n.node_id(), n.name(),
                                 health_name(n.health()),
                                 std::format("{}/{}", hb.allocated().cpu_cores(), hb.capacity().cpu_cores()),
                                 std::format("{}/{}", hb.allocated().memory_bytes() / kMiB,
                                             hb.capacity().memory_bytes() / kMiB),
                                 hb.running_jobs(), fmt_time(n.last_seen_unix_ms()));
    }
}

void print_node(const cv1::NodeInfo& n) {
    const auto& p = n.policy();
    const auto& hb = n.last_heartbeat();
    auto lim = [](std::uint64_t v, std::uint64_t div = 1) { return v ? std::to_string(v / div) : std::string("-"); };
    std::cout << "node:       " << n.node_id() << " (" << n.name() << ")\n"
              << "health:     " << health_name(n.health())
              << (n.offline_reason().empty() ? "" : " — " + n.offline_reason()) << "\n"
              << "version:    " << n.daemon_version() << "\n"
              << "last seen:  " << fmt_time(n.last_seen_unix_ms()) << "\n"
              << "registered: " << fmt_time(n.registered_unix_ms()) << "\n";
    for (const auto& a : n.addresses()) std::cout << "address:    " << a.type() << " " << a.address() << ":" << a.port() << "\n";
    std::cout << "caps:       ";
    for (int i = 0; i < n.capabilities_size(); ++i) std::cout << (i ? "," : "") << n.capabilities(i);
    std::cout << "\n"
              << "hardware:   " << n.physical().cpu_cores() << " cpus, " << n.physical().memory_bytes() / kMiB
              << " MiB, " << n.physical().gpu_count() << " gpus\n"
              << "policy:     v" << p.version() << (p.profile().empty() ? "" : " profile=" + p.profile())
              << " cpus=" << lim(p.max_cpu_cores()) << " memory_mib=" << lim(p.max_memory_bytes(), kMiB)
              << " jobs=" << lim(p.max_jobs()) << " disk_mib=" << lim(p.max_disk_bytes(), kMiB)
              << " gpus=" << lim(p.max_gpus()) << "\n"
              << "load:       " << hb.running_jobs() << " running, " << hb.queued_jobs() << " queued, cpu "
              << static_cast<int>(hb.cpu_utilization() * 100) << "%, applied policy v" << hb.policy_version() << "\n";
}

void print_campaigns(const cv1::CampaignList& l) {
    std::cout << std::format("{:<22} {:<28} {:<10} {:<16} {:<20}\n", "CAMPAIGN_ID", "NAME", "STATE", "DOMAIN", "UPDATED");
    for (const auto& c : l.campaigns())
        std::cout << std::format("{:<22} {:<28} {:<10} {:<16} {:<20}\n", c.campaign_id(), c.name(),
                                 cv1::CampaignState_Name(c.state()), c.domain(), fmt_time(c.updated_unix_ms()));
}

void print_campaign(const cv1::Campaign& c) {
    std::cout << "campaign:    " << c.campaign_id() << " (" << c.name() << ")\n"
              << "state:       " << cv1::CampaignState_Name(c.state()) << "\n"
              << "domain:      " << c.domain() << "\n"
              << "description: " << c.description() << "\n"
              << "parameters:  " << c.parameters_json() << "\n"
              << "created:     " << fmt_time(c.created_unix_ms()) << " by " << c.created_by() << "\n"
              << "updated:     " << fmt_time(c.updated_unix_ms()) << "\n";
}

cv1::CampaignState parse_campaign_state(const std::string& s) {
    cv1::CampaignState st{};
    if (!cv1::CampaignState_Parse(s, &st) || st == cv1::CAMPAIGN_STATE_UNSPECIFIED)
        throw UsageError("unknown campaign state '" + s + "'");
    return st;
}

int cp(const std::vector<std::string>& in) {
    auto a = parse(in, {"--url", "--ca", "--cert", "--key", "--health", "--profile", "--cpus", "--memory-mb", "--jobs",
                        "--disk-mb", "--gpus", "--reason", "--limit", "--state", "--domain", "--description",
                        "--params"});
    if (a.positional.empty()) throw UsageError(std::string("cp needs a command\n") + kCpUsage);
    const std::string cmd = a.positional[0];
    auto need_arg = [&](const char* what) -> const std::string& {
        if (a.positional.size() < 2) throw UsageError(std::format("cp {} needs {}", cmd, what));
        return a.positional[1];
    };
    const bool json = a.flag("--json");

    std::string url = env_or("TDR_CP_URL", a.get("--url"));
    if (url.empty()) throw UsageError("control plane URL required: --url or $TDR_CP_URL");
    tdr::tls::Credentials creds{env_or("TDR_CP_CA", a.get("--ca")), env_or("TDR_CP_CERT", a.get("--cert")),
                                env_or("TDR_CP_KEY", a.get("--key"))};
    if (creds.ca_file.empty() || creds.cert_file.empty() || creds.key_file.empty())
        throw UsageError("--ca, --cert and --key (or $TDR_CP_CA/CERT/KEY) are required");
    Cp c(url, creds);

    auto out = [&](const google::protobuf::Message& m, auto&& human) {
        if (json) std::cout << to_json(m) << "\n";
        else human();
        return 0;
    };

    if (cmd == "nodes") {
        std::string t = "/api/v1/nodes";
        if (auto h = a.get("--health")) t += "?health=" + *h;
        auto l = c.get<cv1::NodeList>("GET", t);
        return out(l, [&] { print_nodes(l); });
    }
    if (cmd == "node") {
        auto n = c.get<cv1::NodeInfo>("GET", "/api/v1/nodes/" + need_arg("ID"));
        return out(n, [&] { print_node(n); });
    }
    if (cmd == "limits") {
        const auto& id = need_arg("ID");
        cv1::UpdateLimitsRequest req;
        if (auto v = a.get("--profile")) req.set_profile(*v);
        if (auto v = a.get("--cpus")) req.set_max_cpu_cores(static_cast<std::uint32_t>(to_u64("--cpus", *v)));
        if (auto v = a.get("--memory-mb")) req.set_max_memory_bytes(to_u64("--memory-mb", *v) * kMiB);
        if (auto v = a.get("--jobs")) req.set_max_jobs(static_cast<std::uint32_t>(to_u64("--jobs", *v)));
        if (auto v = a.get("--disk-mb")) req.set_max_disk_bytes(to_u64("--disk-mb", *v) * kMiB);
        if (auto v = a.get("--gpus")) req.set_max_gpus(static_cast<std::uint32_t>(to_u64("--gpus", *v)));
        auto p = c.get<cv1::ResourcePolicy>("PATCH", "/api/v1/nodes/" + id + "/limits", to_json(req, false));
        return out(p, [&] { std::cout << to_json(p) << "\n"; });
    }
    if (cmd == "offline") {
        cv1::OfflineRequest req;
        req.set_reason(a.get("--reason").value_or(""));
        auto n = c.get<cv1::NodeInfo>("POST", "/api/v1/nodes/" + need_arg("ID") + "/offline", to_json(req, false));
        return out(n, [&] { print_node(n); });
    }
    if (cmd == "events") {
        std::string t = "/api/v1/nodes/" + need_arg("ID") + "/events";
        if (auto l = a.get("--limit")) t += "?limit=" + std::to_string(to_u64("--limit", *l));
        auto l = c.get<cv1::NodeEventList>("GET", t);
        return out(l, [&] {
            for (const auto& e : l.events())
                std::cout << fmt_time(e.at_unix_ms()) << "  " << health_name(e.from()) << " -> " << health_name(e.to())
                          << "  " << e.reason() << "\n";
        });
    }
    if (cmd == "campaigns") {
        std::string t = "/api/v1/campaigns";
        if (auto s = a.get("--state")) t += "?state=" + *s;
        auto l = c.get<cv1::CampaignList>("GET", t);
        return out(l, [&] { print_campaigns(l); });
    }
    if (cmd == "campaign") {
        auto cam = c.get<cv1::Campaign>("GET", "/api/v1/campaigns/" + need_arg("ID"));
        return out(cam, [&] { print_campaign(cam); });
    }
    if (cmd == "campaign-create") {
        cv1::CreateCampaignRequest req;
        req.set_name(need_arg("NAME"));
        req.set_domain(a.get("--domain").value_or(""));
        req.set_description(a.get("--description").value_or(""));
        req.set_parameters_json(a.get("--params").value_or(""));
        auto cam = c.get<cv1::Campaign>("POST", "/api/v1/campaigns", to_json(req, false));
        return out(cam, [&] { print_campaign(cam); });
    }
    if (cmd == "campaign-set") {
        const auto& id = need_arg("ID");
        cv1::UpdateCampaignRequest req;
        if (auto s = a.get("--state")) req.set_state(parse_campaign_state(*s));
        if (auto d = a.get("--description")) req.set_description(*d);
        if (auto p = a.get("--params")) req.set_parameters_json(*p);
        auto cam = c.get<cv1::Campaign>("PATCH", "/api/v1/campaigns/" + id, to_json(req, false));
        return out(cam, [&] { print_campaign(cam); });
    }
    if (cmd == "audit") {
        std::string t = "/api/v1/audit";
        if (auto l = a.get("--limit")) t += "?limit=" + std::to_string(to_u64("--limit", *l));
        auto l = c.get<cv1::AuditList>("GET", t);
        return out(l, [&] {
            for (const auto& e : l.entries())
                std::cout << fmt_time(e.at_unix_ms()) << "  " << e.actor() << "  " << e.action() << "  " << e.target()
                          << "  " << e.details_json() << "\n";
        });
    }
    throw UsageError("unknown cp command: " + cmd + "\n" + kCpUsage);
}

}  // namespace

int run_pki(const std::vector<std::string>& args) { return pki(args); }
int run_cp(const std::vector<std::string>& args) { return cp(args); }

}  // namespace tdrctl
