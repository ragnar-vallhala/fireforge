// tdrctl pki ... (local CA / certificate issuing) and tdrctl cp ... (control-plane
// admin over HTTPS with an admin certificate). docs/design/phase2.md §2, §8.

#include "remote.hpp"

#include <algorithm>
#include <cctype>
#include <chrono>
#include <fstream>
#include <sstream>
#include <thread>
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
#include "fireforge/control/v1/materials.pb.h"
#include "fireforge/tdr/v1/job.pb.h"
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
  jobs [--state S] [--campaign ID] [--limit N]     central job queue (Phase 4)
  job ID                                           job with all attempts
  job-cancel ID [--reason TEXT]
  job-submit [--cpus N] [--memory-mb N] [--timeout S] [--attempts N] [--operation OP]
             [--params JSON] [--input ARTIFACT]... [--env KEY=VALUE]... [--campaign ID]
             [--material ID] [--capability C]...
             [--ranks N [--max-ranks-per-node K] [--mpirun-arg A]...] -- argv...   (multi-node MPI, Phase 7)
  workflow-submit FILE                             CreateWorkflowRequest JSON ("-" = stdin) (Phase 6)
  workflows [--state S] [--campaign ID] [--limit N]
  workflow ID                                      steps: state, job, node, dependencies
  workflow-cancel ID [--reason TEXT]
  workflow-wait ID [--timeout S]                   exit 0 iff the workflow SUCCEEDED
  artifact ID                                      which nodes hold an artifact (+ pinned)
  artifact-pin ID | artifact-unpin ID              pinned artifacts are never evicted (Phase 5)
  materials [--formula F] [--element E] [--campaign ID] [--limit N]     materials database (Phase 8)
  material ID                                      structures + latest calculation per kind
  calcs [--material ID] [--structure ID] [--kind K] [--campaign ID] [--workflow ID]
        [--evidence PREDICTED|LITERATURE|MEASURED] [--limit N] [--after CALC_ID]   newest first
  calc ID                                          one calculation in full
  calc-import FILE                                 ImportCalculationsRequest JSON ("-" = stdin; admin)

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
    for (const auto& peer : hb.peers()) {
        std::cout << "peer:       " << peer.name() << " " << peer.state();
        if (!peer.path_type().empty()) std::cout << " via " << peer.path_type() << " " << peer.address();
        if (peer.rtt_ms() > 0) std::cout << std::format(" rtt {:.2f} ms", peer.rtt_ms());
        if (!peer.last_error().empty() && peer.state() != "CONNECTED") std::cout << " (" << peer.last_error() << ")";
        std::cout << "\n";
    }
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

std::string short_spec(const ::fireforge::tdr::v1::JobSpec& s) {
    std::string what = s.operation();
    if (what.empty() && s.command().argv_size()) what = s.command().argv(0);
    return what;
}

void print_jobs(const cv1::JobList& l) {
    std::cout << std::format("{:<26} {:<18} {:>8} {:<26} {:<20} {}\n", "JOB_ID", "STATE", "ATTEMPTS", "NODE", "UPDATED",
                             "OPERATION/COMMAND");
    for (const auto& j : l.jobs())
        std::cout << std::format("{:<26} {:<18} {:>8} {:<26} {:<20} {}\n", j.job_id(),
                                 ::fireforge::tdr::v1::JobState_Name(j.state()),
                                 std::format("{}/{}", j.attempts_used(), j.spec().policy().max_attempts()),
                                 j.node_id().empty() ? "-" : j.node_id(), fmt_time(j.updated_unix_ms()),
                                 short_spec(j.spec()));
}

void print_workflow(const cv1::Workflow& w) {
    std::cout << "workflow:   " << w.workflow_id() << " (" << w.name() << ")\n"
              << "state:      " << w.state() << "\n"
              << "submitted:  " << fmt_time(w.created_unix_ms()) << " by " << w.submitted_by() << "\n";
    if (!w.campaign_id().empty()) std::cout << "campaign:   " << w.campaign_id() << "\n";
    if (w.finished_unix_ms() > 0) std::cout << "finished:   " << fmt_time(w.finished_unix_ms()) << "\n";
    if (!w.error().empty()) std::cout << "error:      " << w.error() << "\n";
    std::map<std::string, std::uint32_t> counts(w.step_counts().begin(), w.step_counts().end());
    std::cout << "steps:     ";
    for (const auto& [st, n] : counts) std::cout << " " << st << "=" << n;
    std::cout << "\n";
    if (w.steps_size() == 0) return;
    std::cout << std::format("\n{:<32} {:<18} {:<22} {:<26} {}\n", "STEP", "STATE", "JOB", "NODE", "DEPENDS ON / ERROR");
    for (const auto& st : w.steps()) {
        std::string deps;
        for (const auto& d : st.spec().depends_on()) deps += (deps.empty() ? "" : ",") + d;
        for (const auto& in : st.spec().inputs())
            if (deps.find(in.step()) == std::string::npos) deps += (deps.empty() ? "" : ",") + in.step();
        if (!st.parent_step().empty()) deps = "spawned by " + st.parent_step() + (deps.empty() ? "" : "; " + deps);
        if (!st.error().empty()) deps += (deps.empty() ? "" : " — ") + st.error();
        std::cout << std::format("{:<32} {:<18} {:<22} {:<26} {}\n", st.name(), st.state(),
                                 st.job_id().empty() ? "-" : st.job_id(), st.node_id().empty() ? "-" : st.node_id(), deps);
    }
}

void print_job(const cv1::Job& j) {
    std::cout << "job:        " << j.job_id() << "\n"
              << "state:      " << ::fireforge::tdr::v1::JobState_Name(j.state())
              << (j.cancel_requested() ? " (cancel requested)" : "") << "\n"
              << "attempts:   " << j.attempts_used() << "/" << j.spec().policy().max_attempts() << "\n"
              << "submitted:  " << fmt_time(j.created_unix_ms()) << " by " << j.submitted_by() << "\n";
    if (!j.spec().campaign_id().empty()) std::cout << "campaign:   " << j.spec().campaign_id() << "\n";
    std::cout << "command:    ";
    for (int i = 0; i < j.spec().command().argv_size(); ++i) std::cout << (i ? " " : "") << j.spec().command().argv(i);
    std::cout << "\n";
    if (!j.node_id().empty()) std::cout << "node:       " << j.node_id() << "\n";
    if (j.progress_fraction() > 0 || !j.progress_message().empty())
        std::cout << "progress:   " << static_cast<int>(j.progress_fraction() * 100) << "% " << j.progress_message() << "\n";
    if (!j.workflow_id().empty()) std::cout << "workflow:   " << j.workflow_id() << " step " << j.step() << "\n";
    if (j.spec().mpi().ranks() > 0) std::cout << "mpi:        " << j.spec().mpi().ranks() << " ranks\n";
    if (!j.pending_reason().empty()) std::cout << "pending:    " << j.pending_reason() << "\n";
    for (const auto& m : j.gang())
        std::cout << std::format("gang {}:     {} ({}) ranks {}-{} {}\n", m.member_index(),
                                 m.name().empty() ? m.node_id() : m.name(),
                                 m.role() == cv1::GANG_HEAD ? "head" : "member", m.rank_offset(),
                                 m.rank_offset() + m.ranks() - (m.ranks() ? 1 : 0), m.state());
    if (!j.reserved_node().empty())
        std::cout << "reserved:   for " << j.reserved_node() << " until " << fmt_time(j.reserved_until_unix_ms()) << "\n";
    if (!j.last_error().empty()) std::cout << "error:      " << j.last_error() << "\n";
    if (!j.checkpoint().artifact_id().empty())
        std::cout << "checkpoint: " << j.checkpoint().artifact_id() << " (" << j.checkpoint().name() << ", "
                  << j.checkpoint().size() << " bytes)\n";
    for (const auto& o : j.outputs())
        std::cout << "output:     " << o.artifact_id() << " (" << (o.name().empty() ? "" : o.name() + ", ") << o.size()
                  << " bytes)\n";
    for (const auto& at : j.attempts())
        std::cout << std::format("attempt {}:  {} on {} {}{}\n", at.number(), at.state(), at.node_id(),
                                 fmt_time(at.leased_unix_ms()), at.error().empty() ? "" : " — " + at.error());
}

// ---- materials database (Phase 8) ----------------------------------------------------

std::string url_encode(const std::string& s) {
    static constexpr char kHex[] = "0123456789ABCDEF";
    std::string out;
    for (unsigned char c : s) {
        if (std::isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~') out += static_cast<char>(c);
        else {
            out += '%';
            out += kHex[c >> 4];
            out += kHex[c & 0xf];
        }
    }
    return out;
}

std::string clip(std::string s, std::size_t n) {
    if (s.size() > n) s = s.substr(0, n - 3) + "...";
    return s;
}

std::string evidence_name(::fireforge::tdr::v1::Evidence e) { return ::fireforge::tdr::v1::Evidence_Name(e); }

void print_materials(const cv1::MaterialList& l) {
    std::cout << std::format("{:<22} {:<20} {:<20} {:>10} {:>6} {:<20}\n", "MATERIAL_ID", "FORMULA", "ELEMENTS",
                             "STRUCTURES", "CALCS", "CREATED");
    for (const auto& m : l.materials()) {
        std::string el;
        for (const auto& e : m.elements()) el += (el.empty() ? "" : ",") + e;
        std::cout << std::format("{:<22} {:<20} {:<20} {:>10} {:>6} {:<20}\n", m.material_id(), m.reduced_formula(), el,
                                 m.structure_count(), m.calculation_count(), fmt_time(m.created_unix_ms()));
    }
}

void print_calcs(const cv1::CalculationList& l) {
    std::cout << std::format("{:<22} {:<14} {:<18} {:<10} {:<22} {:<20} {}\n", "CALC_ID", "FORMULA", "KIND", "EVIDENCE",
                             "JOB", "CREATED", "PROPERTIES");
    for (const auto& c : l.calculations())
        std::cout << std::format("{:<22} {:<14} {:<18} {:<10} {:<22} {:<20} {}\n", c.calc_id(), c.reduced_formula(),
                                 c.kind(), evidence_name(c.evidence()), c.job_id().empty() ? "-" : c.job_id(),
                                 fmt_time(c.created_unix_ms()), clip(c.properties_json(), 60));
    if (!l.next().empty()) std::cout << "more: --after " << l.next() << "\n";
}

void print_calc(const cv1::Calculation& c) {
    const auto& m = c.method();
    std::cout << "calculation: " << c.calc_id() << "\n"
              << "kind:        " << c.kind() << "\n"
              << "evidence:    " << evidence_name(c.evidence()) << "\n"
              << "material:    " << c.material_id() << " (" << c.reduced_formula() << ")\n"
              << "structure:   " << c.structure_id() << "\n"
              << "created:     " << fmt_time(c.created_unix_ms()) << "\n";
    if (!c.job_id().empty())
        std::cout << "job:         " << c.job_id() << " attempt " << c.attempt_id() << " on " << c.node_id() << "\n";
    if (!c.workflow_id().empty()) std::cout << "workflow:    " << c.workflow_id() << " step " << c.step() << "\n";
    if (!c.campaign_id().empty()) std::cout << "campaign:    " << c.campaign_id() << "\n";
    std::cout << "properties:  " << c.properties_json() << "\n"
              << "conditions:  " << c.conditions_json() << "\n";
    if (!m.solver().empty() || !m.engine().empty()) {
        std::cout << "method:      " << m.solver() << (m.solver_version().empty() ? "" : " " + m.solver_version())
                  << (m.engine().empty() ? "" : " engine=" + m.engine())
                  << (m.functional().empty() ? "" : " functional=" + m.functional())
                  << (m.preset().empty() ? "" : " preset=" + m.preset()) << "\n";
        if (!m.settings_json().empty()) std::cout << "settings:    " << m.settings_json() << "\n";
        for (const auto& pp : m.pseudopotentials()) std::cout << "pseudo:      " << pp << "\n";
    }
    if (c.convergence_json() != "{}") std::cout << "convergence: " << c.convergence_json() << "\n";
    if (c.uncertainty_json() != "{}") std::cout << "uncertainty: " << c.uncertainty_json() << "\n";
    for (const auto& o : c.outputs()) std::cout << "output:      " << o.name() << " = " << o.artifact_id() << "\n";
    for (const auto& i : c.input_artifacts()) std::cout << "input:       " << i << "\n";
    if (!c.note().empty()) std::cout << "note:        " << c.note() << "\n";
}

void print_material(const cv1::Material& m) {
    std::string el;
    for (const auto& e : m.elements()) el += (el.empty() ? "" : ",") + e;
    std::cout << "material:   " << m.material_id() << " (" << m.reduced_formula() << ")\n"
              << "elements:   " << el << "\n"
              << "created:    " << fmt_time(m.created_unix_ms()) << "\n"
              << "counts:     " << m.structure_count() << " structures, " << m.calculation_count() << " calculations\n";
    if (m.structures_size() > 0) {
        std::cout << std::format("\n{:<22} {:>5} {:<14} {:<6} {:<26} {}\n", "STRUCTURE", "SG", "PROTOTYPE", "SITES",
                                 "SOURCE_JOB", "FINGERPRINT");
        for (const auto& s : m.structures())
            std::cout << std::format("{:<22} {:>5} {:<14} {:<6} {:<26} {}\n", s.structure_id(),
                                     s.structure().spacegroup_number(),
                                     s.structure().prototype().empty() ? "-" : s.structure().prototype(),
                                     s.structure().sites_size(), s.source_job_id().empty() ? "-" : s.source_job_id(),
                                     clip(s.structure().fingerprint(), 24));
    }
    if (m.latest_size() > 0) {
        std::cout << std::format("\n{:<18} {:<22} {:<10} {:<20} {}\n", "KIND", "LATEST_CALC", "EVIDENCE", "CREATED",
                                 "PROPERTIES");
        for (const auto& c : m.latest())
            std::cout << std::format("{:<18} {:<22} {:<10} {:<20} {}\n", c.kind(), c.calc_id(),
                                     evidence_name(c.evidence()), fmt_time(c.created_unix_ms()),
                                     clip(c.properties_json(), 80));
    }
}

cv1::CampaignState parse_campaign_state(const std::string& s) {
    cv1::CampaignState st{};
    if (!cv1::CampaignState_Parse(s, &st) || st == cv1::CAMPAIGN_STATE_UNSPECIFIED)
        throw UsageError("unknown campaign state '" + s + "'");
    return st;
}

int cp(const std::vector<std::string>& all_args) {
    // job-submit: everything after "--" is the job's argv.
    std::vector<std::string> in = all_args, job_argv;
    if (auto dd = std::find(in.begin(), in.end(), "--"); dd != in.end()) {
        job_argv.assign(dd + 1, in.end());
        in.erase(dd, in.end());
    }
    auto a = parse(in, {"--url", "--ca", "--cert", "--key", "--health", "--profile", "--cpus", "--memory-mb", "--jobs",
                        "--disk-mb", "--gpus", "--reason", "--limit", "--state", "--domain", "--description",
                        "--params", "--campaign", "--timeout", "--attempts", "--operation", "--input", "--env",
                        "--ranks", "--max-ranks-per-node", "--mpirun-arg",
                        "--material", "--capability", "--formula", "--element", "--structure", "--kind",
                        "--workflow", "--evidence", "--after"});
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
    if (cmd == "jobs") {
        std::string t = "/api/v1/jobs";
        std::string sep = "?";
        if (auto s = a.get("--state")) { t += sep + "state=" + *s; sep = "&"; }
        if (auto s = a.get("--campaign")) { t += sep + "campaign=" + *s; sep = "&"; }
        if (auto l = a.get("--limit")) t += sep + "limit=" + std::to_string(to_u64("--limit", *l));
        auto l = c.get<cv1::JobList>("GET", t);
        return out(l, [&] { print_jobs(l); });
    }
    if (cmd == "job") {
        auto j = c.get<cv1::Job>("GET", "/api/v1/jobs/" + need_arg("ID"));
        return out(j, [&] { print_job(j); });
    }
    if (cmd == "job-cancel") {
        cv1::CancelRequest req;
        req.set_reason(a.get("--reason").value_or(""));
        auto j = c.get<cv1::Job>("POST", "/api/v1/jobs/" + need_arg("ID") + "/cancel", to_json(req, false));
        return out(j, [&] { print_job(j); });
    }
    if (cmd == "job-submit") {
        cv1::CreateJobRequest req;
        auto* spec = req.mutable_spec();
        if (auto v = a.get("--cpus")) spec->mutable_resources()->set_cpu_cores(static_cast<std::uint32_t>(to_u64("--cpus", *v)));
        if (auto v = a.get("--memory-mb")) spec->mutable_resources()->set_memory_bytes(to_u64("--memory-mb", *v) * kMiB);
        if (auto v = a.get("--timeout")) spec->mutable_policy()->set_timeout_seconds(to_u64("--timeout", *v));
        if (auto v = a.get("--attempts")) spec->mutable_policy()->set_max_attempts(static_cast<std::uint32_t>(to_u64("--attempts", *v)));
        if (auto v = a.get("--operation")) spec->set_operation(*v);
        if (auto v = a.get("--params")) spec->set_parameters_json(*v);
        if (auto v = a.get("--campaign")) spec->set_campaign_id(*v);
        if (auto v = a.get("--material")) spec->set_material_id(*v);
        for (const auto& v : a.all("--input")) spec->add_input_artifacts(v);
        for (const auto& v : a.all("--capability")) spec->add_required_capabilities(v);
        if (auto v = a.get("--ranks")) spec->mutable_mpi()->set_ranks(static_cast<std::uint32_t>(to_u64("--ranks", *v)));
        if (auto v = a.get("--max-ranks-per-node"))
            spec->mutable_mpi()->set_max_ranks_per_node(static_cast<std::uint32_t>(to_u64("--max-ranks-per-node", *v)));
        for (const auto& v : a.all("--mpirun-arg")) spec->mutable_mpi()->add_mpirun_args(v);
        for (const auto& kv : a.all("--env")) {
            auto eq = kv.find('=');
            if (eq == std::string::npos || eq == 0) throw UsageError("--env needs KEY=VALUE");
            (*spec->mutable_command()->mutable_env())[kv.substr(0, eq)] = kv.substr(eq + 1);
        }
        for (const auto& v : job_argv) spec->mutable_command()->add_argv(v);
        if (job_argv.empty()) throw UsageError("cp job-submit needs a command after --");
        auto j = c.get<cv1::Job>("POST", "/api/v1/jobs", to_json(req, false));
        if (json) std::cout << to_json(j) << "\n";
        else std::cout << j.job_id() << "\n";
        return 0;
    }
    if (cmd == "workflow-submit") {
        auto file = need_arg("FILE");
        std::stringstream body;
        if (file == "-") body << std::cin.rdbuf();
        else {
            std::ifstream f(file);
            if (!f) throw std::runtime_error("cannot read " + file);
            body << f.rdbuf();
        }
        cv1::CreateWorkflowRequest req;
        google::protobuf::util::JsonParseOptions po;
        if (auto st = google::protobuf::util::JsonStringToMessage(body.str(), &req, po); !st.ok())
            throw UsageError(file + ": not a CreateWorkflowRequest: " + std::string(st.message()));
        auto w = c.get<cv1::Workflow>("POST", "/api/v1/workflows", to_json(req, false));
        if (json) std::cout << to_json(w) << "\n";
        else std::cout << w.workflow_id() << "\n";
        return 0;
    }
    if (cmd == "workflows") {
        std::string t = "/api/v1/workflows";
        std::string sep = "?";
        if (auto s = a.get("--state")) { t += sep + "state=" + *s; sep = "&"; }
        if (auto s = a.get("--campaign")) { t += sep + "campaign=" + *s; sep = "&"; }
        if (auto l = a.get("--limit")) t += sep + "limit=" + std::to_string(to_u64("--limit", *l));
        auto l = c.get<cv1::WorkflowList>("GET", t);
        return out(l, [&] {
            std::cout << std::format("{:<22} {:<24} {:<10} {:<8} {}\n", "WORKFLOW", "NAME", "STATE", "STEPS", "UPDATED");
            for (const auto& w : l.workflows()) {
                std::uint32_t n = 0;
                for (const auto& [_, k] : w.step_counts()) n += k;
                std::cout << std::format("{:<22} {:<24} {:<10} {:<8} {}\n", w.workflow_id(), w.name(), w.state(), n,
                                         fmt_time(w.updated_unix_ms()));
            }
        });
    }
    if (cmd == "workflow") {
        auto w = c.get<cv1::Workflow>("GET", "/api/v1/workflows/" + need_arg("ID"));
        return out(w, [&] { print_workflow(w); });
    }
    if (cmd == "workflow-cancel") {
        cv1::CancelRequest req;
        req.set_reason(a.get("--reason").value_or(""));
        auto w = c.get<cv1::Workflow>("POST", "/api/v1/workflows/" + need_arg("ID") + "/cancel", to_json(req, false));
        return out(w, [&] { print_workflow(w); });
    }
    if (cmd == "workflow-wait") {
        auto id = need_arg("ID");
        std::uint64_t timeout = 0;
        if (auto t = a.get("--timeout")) timeout = to_u64("--timeout", *t);
        auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(timeout);
        for (;;) {
            auto w = c.get<cv1::Workflow>("GET", "/api/v1/workflows/" + id);
            if (w.state() != "RUNNING") {
                if (json) std::cout << to_json(w) << "\n";
                else print_workflow(w);
                return w.state() == "SUCCEEDED" ? 0 : 1;
            }
            if (timeout && std::chrono::steady_clock::now() >= deadline) {
                std::cerr << "tdrctl: timed out waiting; workflow " << id << " still RUNNING\n";
                return 3;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        }
    }
    if (cmd == "artifact" || cmd == "artifact-pin" || cmd == "artifact-unpin") {
        auto id = need_arg("ID");
        constexpr std::string_view kPrefix = "artifact://sha256/";
        std::string hex = id.starts_with(kPrefix) ? id.substr(kPrefix.size()) : id;
        std::string path = "/api/v1/artifacts/sha256/" + hex;
        auto info = cmd == "artifact" ? c.get<cv1::ArtifactInfo>("GET", path)
                                      : c.get<cv1::ArtifactInfo>("POST", path + (cmd == "artifact-pin" ? "/pin" : "/unpin"), "{}");
        return out(info, [&] {
            std::cout << "artifact:   " << (info.artifact().artifact_id().empty() ? "artifact://sha256/" + hex
                                                                                  : info.artifact().artifact_id()) << "\n"
                      << "size:       " << info.artifact().size() << " bytes\n";
            if (!info.artifact().media_type().empty()) std::cout << "media type: " << info.artifact().media_type() << "\n";
            std::cout << "pinned:     " << (info.pinned() ? "yes" : "no") << "\n";
            for (const auto& n : info.node_ids()) std::cout << "on node:    " << n << "\n";
        });
    }
    if (cmd == "materials" || cmd == "calcs") {
        std::string t = cmd == "materials" ? "/api/v1/materials" : "/api/v1/calculations";
        std::string sep = "?";
        auto add = [&](const char* flag, const char* key) {
            if (auto v = a.get(flag)) {
                t += sep + key + "=" + url_encode(*v);
                sep = "&";
            }
        };
        if (cmd == "materials") {
            add("--formula", "formula");
            add("--element", "element");
            add("--campaign", "campaign_id");
        } else {
            add("--material", "material_id");
            add("--structure", "structure_id");
            add("--kind", "kind");
            add("--campaign", "campaign_id");
            add("--workflow", "workflow_id");
            add("--evidence", "evidence");
            add("--after", "after");
        }
        if (auto l = a.get("--limit")) t += sep + "limit=" + std::to_string(to_u64("--limit", *l));
        if (cmd == "materials") {
            auto l = c.get<cv1::MaterialList>("GET", t);
            return out(l, [&] { print_materials(l); });
        }
        auto l = c.get<cv1::CalculationList>("GET", t);
        return out(l, [&] { print_calcs(l); });
    }
    if (cmd == "material") {
        auto m = c.get<cv1::Material>("GET", "/api/v1/materials/" + url_encode(need_arg("ID")));
        return out(m, [&] { print_material(m); });
    }
    if (cmd == "calc") {
        auto calc = c.get<cv1::Calculation>("GET", "/api/v1/calculations/" + url_encode(need_arg("ID")));
        return out(calc, [&] { print_calc(calc); });
    }
    if (cmd == "calc-import") {
        auto file = need_arg("FILE");
        std::stringstream body;
        if (file == "-") body << std::cin.rdbuf();
        else {
            std::ifstream f(file);
            if (!f) throw std::runtime_error("cannot read " + file);
            body << f.rdbuf();
        }
        cv1::ImportCalculationsRequest req;
        google::protobuf::util::JsonParseOptions po;
        if (auto st = google::protobuf::util::JsonStringToMessage(body.str(), &req, po); !st.ok())
            throw UsageError(file + ": not an ImportCalculationsRequest: " + std::string(st.message()));
        auto rep = c.get<cv1::ImportCalculationsReply>("POST", "/api/v1/calculations:import", to_json(req, false));
        return out(rep, [&] {
            for (const auto& id : rep.calc_ids()) std::cout << id << "\n";
        });
    }
    throw UsageError("unknown cp command: " + cmd + "\n" + kCpUsage);
}

}  // namespace

int run_pki(const std::vector<std::string>& args) { return pki(args); }
int run_cp(const std::vector<std::string>& args) { return cp(args); }

}  // namespace tdrctl
