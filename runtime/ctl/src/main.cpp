// tdrctl — command-line client for tdrd's control socket.
// Usage: see docs/design/phase1.md §7 or `tdrctl --help`.

#include <chrono>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <filesystem>
#include <optional>
#include <format>
#include <iostream>
#include <map>
#include <string>
#include <thread>
#include <vector>

#include <google/protobuf/util/json_util.h>

#include "fireforge/tdr/v1/local.pb.h"
#include "tdr/ipc/ipc.hpp"
#include "tdr/protocol/envelope.hpp"

#include "remote.hpp"

namespace v1 = ::fireforge::tdr::v1;
using tdr::protocol::Frame;
using tdr::protocol::make_frame;
using tdr::protocol::next_message_id;

namespace {

bool g_remote = false;  // --remote: central job queue (phase4.md §7)

constexpr const char* kUsage = R"(usage: tdrctl [--socket PATH] <command> [args]

commands:
  submit [--cpus N] [--memory-mb N] [--timeout S] [--attempts N]
         [--operation OP] [--params JSON] [--input ARTIFACT]...
         [--env KEY=VALUE]... [--campaign ID] [--material ID]
         [--ranks N [--mpirun-arg A]...] -- argv...      (--ranks: MPI on this node; per-rank --cpus/--memory-mb)
  status JOB_ID [--json]
  list [--state STATE] [--limit N] [--json]
  cancel JOB_ID
  node [--json]                 includes control-plane state (Phase 2)
  peers [--json]                peer directory + live peer links (path, RTT)
  wait JOB_ID [--timeout S]     exit 0 iff the job COMPLETED

  artifact put FILE [--media-type T]   add a file to this node's store; prints its id
  artifact get ID [--out PATH]         fetch from a peer if missing; copy to PATH
  artifact ls [--json]                 this node's store (size, last used)
  artifact rm ID [--force]             drop the local copy (--force: even the last one)
  artifact gc                          evict now (down to gc_low % of max_disk_mb)

  --remote (with submit/status/list/cancel/wait): use the control-plane job
  queue through this node instead of local jobs (Phase 4). The job may run
  on any eligible node.

  pki ...                       local CA and certificates   (tdrctl pki --help)
  cp ...                        control-plane admin (HTTPS)  (tdrctl cp --help)

socket: --socket, else $TDR_SOCKET, else /var/lib/fireforge/run/ctl.sock
)";

using tdrctl::UsageError;

std::string to_json(const google::protobuf::Message& m) {
    google::protobuf::util::JsonPrintOptions o;
    o.add_whitespace = true;
    o.preserve_proto_field_names = true;
    std::string out;
    (void)google::protobuf::util::MessageToJsonString(m, &out, o);
    return out;
}

std::string state_name(v1::JobState s) { return v1::JobState_Name(s); }

class Client {
public:
    explicit Client(const std::string& path) : c_(path) {}

    // Sends a request; returns the reply frame, or throws with the daemon's error.
    template <class Req>
    Frame call(v1::MessageType type, const Req& req, v1::MessageType expect,
               std::chrono::milliseconds timeout = std::chrono::seconds(30)) {
        Frame reply = c_.request(make_frame(type, req, next_message_id()), timeout);
        if (reply.type() == v1::ERROR) {
            v1::Error e;
            reply.parse(e);
            throw std::runtime_error(v1::ErrorCode_Name(e.code()) + ": " + e.message());
        }
        if (reply.type() != expect) throw std::runtime_error("unexpected reply type " + std::to_string(reply.header.message_type));
        return reply;
    }

    v1::JobStatus get(const std::string& id) {
        v1::GetJobRequest req;
        req.set_job_id(id);
        req.set_remote(g_remote);
        v1::JobStatus st;
        if (!call(v1::CTL_GET_JOB, req, v1::CTL_JOB_INFO).parse(st)) throw std::runtime_error("bad JobStatus reply");
        return st;
    }

private:
    tdr::ipc::UnixClient c_;
};

std::uint64_t to_u64(const std::string& flag, const std::string& v) {
    try {
        std::size_t n = 0;
        auto x = std::stoull(v, &n);
        if (n != v.size()) throw 0;
        return x;
    } catch (...) {
        throw UsageError(flag + " needs a non-negative integer, got '" + v + "'");
    }
}

bool is_terminal(v1::JobState s) {
    return s == v1::COMPLETED || s == v1::FAILED || s == v1::CANCELLED || s == v1::EXPIRED || s == v1::VALIDATION_FAILED;
}

void print_status(const v1::JobStatus& s) {
    std::cout << "job:       " << s.job_id() << "\n"
              << "state:     " << state_name(s.state()) << "\n"
              << "attempts:  " << s.attempts_used() << "/" << std::max(1u, s.spec().policy().max_attempts()) << "\n";
    if (!s.spec().operation().empty()) std::cout << "operation: " << s.spec().operation() << "\n";
    if (s.progress_fraction() > 0 || !s.progress_message().empty())
        std::cout << "progress:  " << static_cast<int>(s.progress_fraction() * 100) << "% " << s.progress_message() << "\n";
    if (!s.node_id().empty()) std::cout << "node:      " << s.node_id() << "\n";
    if (!s.remote_job_id().empty()) std::cout << "remote:    leased from control-plane job " << s.remote_job_id() << "\n";
    if (!s.last_error().empty()) std::cout << "error:     " << s.last_error() << "\n";
    for (const auto& a : s.output_artifacts()) std::cout << "output:    " << a << "\n";
}

int cmd_submit(Client& cl, std::vector<std::string> a) {
    v1::SubmitJobRequest req;
    req.set_remote(g_remote);
    auto* spec = req.mutable_spec();
    std::size_t i = 0;
    auto val = [&](const std::string& flag) -> std::string {
        if (i + 1 >= a.size()) throw UsageError(flag + " needs a value");
        return a[++i];
    };
    for (; i < a.size(); ++i) {
        const auto& f = a[i];
        if (f == "--") { ++i; break; }
        if (f == "--cpus") spec->mutable_resources()->set_cpu_cores(static_cast<std::uint32_t>(to_u64(f, val(f))));
        else if (f == "--memory-mb") spec->mutable_resources()->set_memory_bytes(to_u64(f, val(f)) * 1024 * 1024);
        else if (f == "--timeout") spec->mutable_policy()->set_timeout_seconds(to_u64(f, val(f)));
        else if (f == "--attempts") spec->mutable_policy()->set_max_attempts(static_cast<std::uint32_t>(to_u64(f, val(f))));
        else if (f == "--operation") spec->set_operation(val(f));
        else if (f == "--params") spec->set_parameters_json(val(f));
        else if (f == "--ranks") spec->mutable_mpi()->set_ranks(static_cast<std::uint32_t>(to_u64(f, val(f))));
        else if (f == "--max-ranks-per-node")
            spec->mutable_mpi()->set_max_ranks_per_node(static_cast<std::uint32_t>(to_u64(f, val(f))));
        else if (f == "--mpirun-arg") spec->mutable_mpi()->add_mpirun_args(val(f));
        else if (f == "--input") spec->add_input_artifacts(val(f));
        else if (f == "--campaign") spec->set_campaign_id(val(f));
        else if (f == "--material") spec->set_material_id(val(f));
        else if (f == "--env") {
            auto kv = val(f);
            auto eq = kv.find('=');
            if (eq == std::string::npos || eq == 0) throw UsageError("--env needs KEY=VALUE");
            (*spec->mutable_command()->mutable_env())[kv.substr(0, eq)] = kv.substr(eq + 1);
        } else throw UsageError("unknown submit option: " + f + " (put the command after --)");
    }
    for (; i < a.size(); ++i) spec->mutable_command()->add_argv(a[i]);
    if (spec->command().argv_size() == 0) throw UsageError("submit needs a command after --");

    v1::SubmitJobReply rep;
    cl.call(v1::CTL_SUBMIT, req, v1::CTL_SUBMIT_REPLY).parse(rep);
    std::cout << rep.job_id() << "\n";
    return 0;
}

int cmd_status(Client& cl, const std::vector<std::string>& a) {
    if (a.empty()) throw UsageError("status needs JOB_ID");
    bool json = a.size() > 1 && a[1] == "--json";
    auto st = cl.get(a[0]);
    if (json) std::cout << to_json(st);
    else print_status(st);
    return 0;
}

int cmd_list(Client& cl, const std::vector<std::string>& a) {
    v1::ListJobsRequest req;
    req.set_remote(g_remote);
    bool json = false;
    for (std::size_t i = 0; i < a.size(); ++i) {
        if (a[i] == "--json") json = true;
        else if (a[i] == "--state" && i + 1 < a.size()) {
            v1::JobState s;
            if (!v1::JobState_Parse(a[++i], &s)) throw UsageError("unknown state: " + a[i]);
            req.set_state(s);
        } else if (a[i] == "--limit" && i + 1 < a.size()) req.set_limit(static_cast<std::uint32_t>(to_u64("--limit", a[++i])));
        else throw UsageError("unknown list option: " + a[i]);
    }
    v1::JobList list;
    cl.call(v1::CTL_LIST_JOBS, req, v1::CTL_JOB_LIST).parse(list);
    if (json) { std::cout << to_json(list); return 0; }
    std::printf("%-40s %-18s %-8s %s\n", "JOB", "STATE", "ATTEMPTS", "OPERATION/COMMAND");
    for (const auto& j : list.jobs()) {
        std::string what = j.spec().operation();
        if (what.empty() && j.spec().command().argv_size()) what = j.spec().command().argv(0);
        std::printf("%-40s %-18s %-8u %s\n", j.job_id().c_str(), state_name(j.state()).c_str(), j.attempts_used(), what.c_str());
    }
    return 0;
}

int cmd_cancel(Client& cl, const std::vector<std::string>& a) {
    if (a.empty()) throw UsageError("cancel needs JOB_ID");
    v1::CancelJobRequest req;
    req.set_job_id(a[0]);
    req.set_remote(g_remote);
    cl.call(v1::CTL_CANCEL, req, v1::CTL_ACK);
    std::cout << "cancel requested: " << a[0] << "\n";
    return 0;
}

int cmd_node(Client& cl, const std::vector<std::string>& a) {
    v1::NodeStatus st;
    cl.call(v1::CTL_NODE_STATUS, v1::NodeStatusRequest{}, v1::CTL_NODE_STATUS_REPLY).parse(st);
    if (!a.empty() && a[0] == "--json") { std::cout << to_json(st); return 0; }
    auto mib = [](std::uint64_t b) { return b / (1024 * 1024); };
    std::cout << "node:      " << st.node_name() << " (tdrd " << st.daemon_version() << ")\n"
              << "cpus:      " << st.allocated().cpu_cores() << "/" << st.capacity().cpu_cores() << " allocated\n"
              << "memory:    " << mib(st.allocated().memory_bytes()) << "/" << mib(st.capacity().memory_bytes()) << " MiB allocated\n"
              << "jobs:      " << st.running_jobs() << " running (max " << st.capacity().jobs() << "), "
              << st.queued_jobs() << " queued, " << st.failed_jobs() << " failed total\n"
              << "cgroups:   " << (st.cgroups_enabled() ? "on" : "off (rlimit fallback)") << "\n"
              << "control:   " << (st.control_state().empty() ? "standalone" : st.control_state());
    if (!st.control_url().empty()) std::cout << " (" << st.control_url() << ")";
    std::cout << "\n";
    if (!st.node_id().empty()) std::cout << "node_id:   " << st.node_id() << " (policy v" << st.policy_version() << ")\n";
    if (st.last_heartbeat_ok_unix_ms() > 0) {
        auto age = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch())
                       .count() - st.last_heartbeat_ok_unix_ms() / 1000;
        std::cout << "heartbeat: " << age << " s ago\n";
    }
    if (!st.last_control_error().empty() && st.control_state() != "connected")
        std::cout << "error:     " << st.last_control_error() << "\n";
    return 0;
}

int cmd_peers(Client& cl, const std::vector<std::string>& a) {
    namespace cv1 = ::fireforge::control::v1;
    v1::PeerList pl;
    cl.call(v1::CTL_PEERS, v1::PeersRequest{}, v1::CTL_PEERS_REPLY).parse(pl);
    if (!a.empty() && a[0] == "--json") { std::cout << to_json(pl); return 0; }
    if (pl.refreshed_unix_ms() == 0) {
        std::cout << "no peer directory yet (standalone, or not connected to the control plane)\n";
        return 0;
    }
    std::map<std::string, const cv1::PeerReachability*> links;
    for (const auto& c : pl.connections()) links[c.node_id()] = &c;
    if (pl.listen_port()) std::cout << "peer listener on port " << pl.listen_port() << "\n";
    std::cout << std::format("{:<20} {:<26} {:<8} {:<12} {:<10} {:<22} {:>8}  {}\n", "NAME", "NODE_ID", "HEALTH",
                             "LINK", "PATH", "ADDRESS", "RTT_MS", "LAST_ERROR");
    for (const auto& n : pl.peers()) {
        auto it = links.find(n.node_id());
        const cv1::PeerReachability* l = it == links.end() ? nullptr : it->second;
        std::string rtt = l && l->rtt_ms() > 0 ? std::format("{:.2f}", l->rtt_ms()) : "-";
        std::cout << std::format("{:<20} {:<26} {:<8} {:<12} {:<10} {:<22} {:>8}  {}\n", n.name(), n.node_id(),
                                 cv1::NodeHealth_Name(n.health()), l ? l->state() : "self/-",
                                 l && !l->path_type().empty() ? l->path_type() : "-",
                                 l && !l->address().empty() ? l->address() : "-", rtt, l ? l->last_error() : "");
    }
    return 0;
}

// ---- artifacts (phase5.md §7) ----
std::string fmt_bytes(std::uint64_t b) {
    const double d = static_cast<double>(b);
    if (b >= 1024ull * 1024 * 1024) return std::format("{:.1f} GiB", d / (1024.0 * 1024 * 1024));
    if (b >= 1024ull * 1024) return std::format("{:.1f} MiB", d / (1024.0 * 1024));
    if (b >= 1024) return std::format("{:.1f} KiB", d / 1024.0);
    return std::format("{} B", b);
}

int cmd_artifact(Client& cl, std::vector<std::string> a) {
    if (a.empty()) throw UsageError("artifact needs put|get|ls|rm|gc");
    auto sub = a[0];
    a.erase(a.begin());
    auto flag = [&](const std::string& name) -> std::optional<std::string> {
        for (std::size_t i = 0; i < a.size(); ++i)
            if (a[i] == name) {
                if (i + 1 >= a.size()) throw UsageError(name + " needs a value");
                auto v = a[i + 1];
                a.erase(a.begin() + i, a.begin() + i + 2);
                return v;
            }
        return std::nullopt;
    };
    auto has = [&](const std::string& name) {
        for (auto it = a.begin(); it != a.end(); ++it)
            if (*it == name) { a.erase(it); return true; }
        return false;
    };
    if (sub == "put") {
        v1::ArtifactPutRequest req;
        if (auto t = flag("--media-type")) req.set_media_type(*t);
        if (a.size() != 1) throw UsageError("artifact put needs FILE");
        req.set_path(std::filesystem::absolute(a[0]).string());
        v1::LocalArtifact art;
        cl.call(v1::CTL_ARTIFACT_PUT, req, v1::CTL_ARTIFACT_REPLY).parse(art);
        std::cout << art.ref().artifact_id() << "\n";
        return 0;
    }
    if (sub == "get") {
        auto out = flag("--out");
        if (a.size() != 1) throw UsageError("artifact get needs ID");
        v1::ArtifactGetRequest req;
        req.set_artifact_id(a[0]);
        v1::LocalArtifact art;
        cl.call(v1::CTL_ARTIFACT_GET, req, v1::CTL_ARTIFACT_REPLY, std::chrono::hours(24)).parse(art);
        if (out) {
            std::error_code ec;
            std::filesystem::copy_file(art.local_path(), *out, std::filesystem::copy_options::overwrite_existing, ec);
            if (ec) throw std::runtime_error("copy " + art.local_path() + " -> " + *out + ": " + ec.message() +
                                             " (the file is in the store at " + art.local_path() + ")");
            std::filesystem::permissions(*out, std::filesystem::perms::owner_write, std::filesystem::perm_options::add, ec);
        }
        std::cout << (art.fetched() ? "fetched " : "present ") << art.ref().artifact_id() << " ("
                  << fmt_bytes(art.ref().size()) << ")\n";
        std::cout << (out ? *out : art.local_path()) << "\n";
        return 0;
    }
    if (sub == "ls") {
        bool json = has("--json");
        v1::LocalArtifactList list;
        cl.call(v1::CTL_ARTIFACT_LIST, v1::ArtifactListRequest{}, v1::CTL_ARTIFACT_LIST_REPLY).parse(list);
        if (json) { std::cout << to_json(list); return 0; }
        std::cout << "store: " << fmt_bytes(list.store_bytes()) << " in " << list.artifacts_size() << " artifacts";
        if (list.max_bytes()) std::cout << " (cap " << fmt_bytes(list.max_bytes()) << ")";
        std::cout << "\n";
        for (const auto& x : list.artifacts()) {
            std::time_t t = x.last_used_unix_ms() / 1000;
            char buf[32] = {};
            std::strftime(buf, sizeof buf, "%Y-%m-%d %H:%M:%S", std::localtime(&t));
            std::cout << std::format("{}  {:>10}  {}  {}\n", x.ref().artifact_id(), fmt_bytes(x.ref().size()), buf,
                                     x.ref().media_type());
        }
        return 0;
    }
    if (sub == "rm") {
        v1::ArtifactRmRequest req;
        req.set_force(has("--force"));
        if (a.size() != 1) throw UsageError("artifact rm needs ID");
        req.set_artifact_id(a[0]);
        cl.call(v1::CTL_ARTIFACT_RM, req, v1::CTL_ACK);
        std::cout << "removed " << a[0] << "\n";
        return 0;
    }
    if (sub == "gc") {
        bool json = has("--json");
        v1::GcReport rep;
        cl.call(v1::CTL_ARTIFACT_GC, v1::ArtifactGcRequest{}, v1::CTL_ARTIFACT_GC_REPLY, std::chrono::minutes(10)).parse(rep);
        if (json) { std::cout << to_json(rep); return 0; }
        std::cout << "evicted " << rep.evicted_size() << " (" << fmt_bytes(rep.freed_bytes()) << "), store now "
                  << fmt_bytes(rep.store_bytes());
        if (rep.max_bytes()) std::cout << " of " << fmt_bytes(rep.max_bytes());
        std::cout << "; kept " << rep.kept_last_copy() << " last copies, " << rep.kept_pinned() << " pinned, "
                  << rep.kept_protected() << " in use/recent\n";
        for (const auto& e : rep.evicted()) std::cout << "  " << e << "\n";
        return 0;
    }
    throw UsageError("unknown artifact command: " + sub);
}

int cmd_wait(Client& cl, const std::vector<std::string>& a) {
    if (a.empty()) throw UsageError("wait needs JOB_ID");
    std::uint64_t timeout = 0;
    if (a.size() >= 3 && a[1] == "--timeout") timeout = to_u64("--timeout", a[2]);
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(timeout);
    auto delay = std::chrono::milliseconds(20);
    for (;;) {
        auto st = cl.get(a[0]);
        if (is_terminal(st.state())) {
            print_status(st);
            return st.state() == v1::COMPLETED ? 0 : 1;
        }
        if (timeout && std::chrono::steady_clock::now() >= deadline) {
            std::cerr << "tdrctl: timed out waiting; state " << state_name(st.state()) << "\n";
            return 3;
        }
        std::this_thread::sleep_for(delay);
        delay = std::min(delay * 2, std::chrono::milliseconds(500));
    }
}

}  // namespace

int main(int argc, char** argv) {
    std::vector<std::string> args(argv + 1, argv + argc);
    std::string socket = std::getenv("TDR_SOCKET") ? std::getenv("TDR_SOCKET") : "/var/lib/fireforge/run/ctl.sock";
    if (args.size() >= 2 && args[0] == "--socket") {
        socket = args[1];
        args.erase(args.begin(), args.begin() + 2);
    }
    if (args.empty() || args[0] == "-h" || args[0] == "--help" || args[0] == "help") {
        std::cout << kUsage;
        return args.empty() ? 2 : 0;
    }
    std::string cmd = args[0];
    args.erase(args.begin());
    if (cmd == "submit" || cmd == "status" || cmd == "list" || cmd == "cancel" || cmd == "wait") {
        // --remote anywhere before "--" (after it, it belongs to the job's argv).
        for (auto it = args.begin(); it != args.end() && *it != "--"; ++it) {
            if (*it == "--remote") { g_remote = true; args.erase(it); break; }
        }
    }
    try {
        if (cmd == "pki") return tdrctl::run_pki(args);
        if (cmd == "cp") return tdrctl::run_cp(args);
        Client cl(socket);
        if (cmd == "submit") return cmd_submit(cl, args);
        if (cmd == "status") return cmd_status(cl, args);
        if (cmd == "list") return cmd_list(cl, args);
        if (cmd == "cancel") return cmd_cancel(cl, args);
        if (cmd == "node") return cmd_node(cl, args);
        if (cmd == "wait") return cmd_wait(cl, args);
        if (cmd == "peers") return cmd_peers(cl, args);
        if (cmd == "artifact") return cmd_artifact(cl, args);
        throw UsageError("unknown command: " + cmd);
    } catch (const UsageError& e) {
        std::cerr << "tdrctl: " << e.what() << "\n\n" << kUsage;
        return 2;
    } catch (const std::system_error& e) {
        std::cerr << "tdrctl: cannot reach tdrd at " << socket << ": " << e.what() << "\n";
        return 1;
    } catch (const std::exception& e) {
        std::cerr << "tdrctl: " << e.what() << "\n";
        return 1;
    }
}
