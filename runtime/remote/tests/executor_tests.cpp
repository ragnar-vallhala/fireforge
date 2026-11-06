// RemoteExecutor against a fake control client and a REAL JobManager (sqlite
// journal, local artifact store, Linux supervisor without cgroups) running
// tiny commands.
#include <boost/asio/post.hpp>
#include <boost/json.hpp>
#include <boost/test/unit_test.hpp>
#include <chrono>
#include <deque>
#include <fstream>
#include <set>
#include <sstream>

#include "tdr/artifacts/store.hpp"
#include "tdr/common/common.hpp"
#include "tdr/journal/journal.hpp"
#include "tdr/remote/executor.hpp"
#include "tdr/supervisor/supervisor.hpp"

namespace asio = boost::asio;
using namespace std::chrono_literals;
using namespace tdr::remote;
using tdr::agent::ClientError;
using tdr::agent::Result;

namespace {

constexpr std::uint64_t kMiB = 1ull << 20;

struct TempDir {
    fs::path path;
    TempDir() {
        path = fs::temp_directory_path() / tdr::new_id("remote-test");
        fs::create_directories(path);
    }
    ~TempDir() {
        std::error_code ec;
        fs::remove_all(path, ec);
    }
};

template <class T>
Result<T> fail(int status, std::string msg = "boom") {
    return std::unexpected(ClientError{status, std::move(msg)});
}

// Records every call. Each kind answers through `auto_reply` (when it returns a
// value) or leaves the callback pending for the test. Replies always arrive
// via asio::post, like the real client.
class FakeClient final : public tdr::agent::IControlClient {
public:
    explicit FakeClient(asio::io_context& io) : io_(io) {}

    template <class Req, class T>
    struct Kind {
        std::vector<Req> requests;
        std::deque<std::function<void(Result<T>)>> pending;
        std::function<std::optional<Result<T>>(const Req&)> auto_reply;
    };
    struct Update {
        std::string job_id;
        cv1::JobUpdate update;
    };
    Kind<cv1::LeaseRequest, cv1::LeaseReply> leases;
    Kind<Update, cv1::Job> updates;
    Kind<cv1::ArtifactAnnouncement, void> announces;

    template <class Req, class T>
    void answer(Kind<Req, T>& k, Result<T> r) {
        BOOST_REQUIRE(!k.pending.empty());
        auto cb = std::move(k.pending.front());
        k.pending.pop_front();
        asio::post(io_, [cb = std::move(cb), r = std::move(r)]() mutable { cb(std::move(r)); });
    }

    void lease(const cv1::LeaseRequest& req, std::function<void(Result<cv1::LeaseReply>)> cb) override {
        enqueue(leases, req, std::move(cb));
    }
    void update_job(const std::string& job_id, const cv1::JobUpdate& u,
                    std::function<void(Result<cv1::Job>)> cb) override {
        enqueue(updates, Update{job_id, u}, std::move(cb));
    }
    void announce_artifacts(const cv1::ArtifactAnnouncement& a, std::function<void(Result<void>)> cb) override {
        enqueue(announces, a, std::move(cb));
    }

    // Not used by the executor.
    void register_node(const cv1::RegisterRequest&, std::function<void(Result<cv1::RegisterReply>)>) override {
        BOOST_FAIL("unexpected register_node");
    }
    void heartbeat(const cv1::Heartbeat&, std::function<void(Result<cv1::HeartbeatReply>)>) override {
        BOOST_FAIL("unexpected heartbeat");
    }
    void list_nodes(std::function<void(Result<cv1::NodeList>)>) override { BOOST_FAIL("unexpected list_nodes"); }
    void offline(const std::string&, const std::string&, std::function<void(Result<cv1::NodeInfo>)>) override {
        BOOST_FAIL("unexpected offline");
    }
    void submit_job(const cv1::CreateJobRequest&, std::function<void(Result<cv1::Job>)>) override {
        BOOST_FAIL("unexpected submit_job");
    }
    void get_job(const std::string&, std::function<void(Result<cv1::Job>)>) override {
        BOOST_FAIL("unexpected get_job");
    }
    void list_jobs(tv1::JobState, const std::string&, std::uint32_t,
                   std::function<void(Result<cv1::JobList>)>) override {
        BOOST_FAIL("unexpected list_jobs");
    }
    void cancel_job(const std::string&, const std::string&, std::function<void(Result<cv1::Job>)>) override {
        BOOST_FAIL("unexpected cancel_job");
    }

private:
    template <class Req, class T>
    void enqueue(Kind<Req, T>& k, Req req, std::function<void(Result<T>)> cb) {
        k.requests.push_back(req);
        k.pending.push_back(std::move(cb));
        if (k.auto_reply) {
            if (auto r = k.auto_reply(k.requests.back())) answer(k, std::move(*r));
        }
    }
    asio::io_context& io_;
};

cv1::Lease make_lease(const std::string& job_id, std::vector<std::string> argv, std::uint32_t attempt = 1) {
    cv1::Lease l;
    l.set_job_id(job_id);
    l.set_attempt_id("att-" + job_id + "-" + std::to_string(attempt));
    l.set_fencing_token(attempt);
    l.set_attempt_number(attempt);
    l.mutable_spec()->set_job_id(job_id);
    for (auto& a : argv) l.mutable_spec()->mutable_command()->add_argv(a);
    l.mutable_spec()->mutable_resources()->set_cpu_cores(1);
    l.mutable_spec()->mutable_resources()->set_memory_bytes(64 * kMiB);
    l.mutable_spec()->mutable_policy()->set_max_attempts(3);
    return l;
}

cv1::LeaseRef ref_of(const cv1::Lease& l) {
    cv1::LeaseRef r;
    r.set_job_id(l.job_id());
    r.set_attempt_id(l.attempt_id());
    r.set_fencing_token(l.fencing_token());
    return r;
}

const std::vector<std::string> kWriteOutput = {"/bin/sh", "-c", "echo hello > \"$FIREFORGE_OUTPUT_DIR/out.txt\""};

struct Fixture {
    TempDir tmp;
    asio::io_context io;
    FakeClient client{io};
    bool connected = true;
    tdr::jobs::Capacity capacity{4, 1024 * kMiB, 2};

    std::unique_ptr<tdr::supervisor::IProcessSupervisor> sup;
    std::unique_ptr<tdr::journal::IJournal> journal;
    std::unique_ptr<tdr::artifacts::IArtifactStore> store;
    std::unique_ptr<tdr::jobs::JobManager> mgr;
    std::unique_ptr<RemoteExecutor> exec;
    RemoteOptions ropts;
    std::vector<cv1::ArtifactRef> local_artifacts;  // list_local_artifacts hook
    std::vector<std::pair<std::string, tv1::JobState>> events;

    Fixture() {
        ropts.state_file = tmp.path / "state" / "remote.json";
        ropts.poll_interval = 30ms;
        ropts.poll_idle = 60ms;
        ropts.retry_min = 20ms;
        ropts.retry_max = 80ms;
        ropts.default_job_memory_bytes = 64 * kMiB;
        // Default replies: no leases, updates and announcements succeed.
        client.leases.auto_reply = [](const cv1::LeaseRequest&) {
            return std::optional<Result<cv1::LeaseReply>>(cv1::LeaseReply{});
        };
        client.updates.auto_reply = [](const FakeClient::Update& u) {
            cv1::Job j;
            j.set_job_id(u.job_id);
            return std::optional<Result<cv1::Job>>(j);
        };
        client.announces.auto_reply = [](const cv1::ArtifactAnnouncement&) {
            return std::optional<Result<void>>(Result<void>{});
        };
    }

    ~Fixture() {
        exec.reset();
        // Do not leave children behind: cancel what is still running.
        if (mgr) {
            for (const auto& st : mgr->list(tv1::JOB_STATE_UNSPECIFIED, 1000)) {
                if (!tdr::jobs::is_terminal(st.state())) (void)mgr->cancel(st.job_id());
            }
            run_until([&] {
                for (const auto& st : mgr->list(tv1::JOB_STATE_UNSPECIFIED, 1000)) {
                    if (!tdr::jobs::is_terminal(st.state())) return false;
                }
                return true;
            }, 5000ms);
        }
        mgr.reset();
    }

    void make_manager() {
        tdr::supervisor::SupervisorOptions so;
        so.cgroups = tdr::supervisor::SupervisorOptions::Cgroups::kOff;
        so.kill_grace = 1s;
        sup = tdr::supervisor::make_linux_supervisor(io, so);
        journal = tdr::journal::open_sqlite_journal(tmp.path / "journal.db");
        store = tdr::artifacts::open_local_store(tmp.path / "artifacts");
        tdr::jobs::ManagerOptions mo;
        mo.node_name = "node-a";
        mo.capacity = capacity;
        mo.default_job_memory_bytes = 64 * kMiB;
        mo.work_root = tmp.path / "work";
        mo.worker_socket = tmp.path / "worker.sock";
        mo.cancel_grace = 1s;
        mgr = std::make_unique<tdr::jobs::JobManager>(io, *journal, *store, *sup, mo);
        mgr->set_state_listener([this](const std::string& id, tv1::JobState s) {
            events.emplace_back(id, s);
            if (exec) exec->on_job_state(id, s);  // what tdrd does
        });
        mgr->recover();
    }

    void make_executor() {
        RemoteHooks h;
        h.connected = [this] { return connected; };
        h.list_local_artifacts = [this] { return local_artifacts; };
        h.artifact_meta = [this](const std::string& id) -> std::optional<cv1::ArtifactRef> {
            auto p = store->path_of(id);
            if (!p) return std::nullopt;
            cv1::ArtifactRef r;
            r.set_artifact_id(id);
            r.set_size(fs::file_size(*p));
            r.set_media_type("test/meta");
            return r;
        };
        exec = std::make_unique<RemoteExecutor>(io, client, *mgr, ropts, std::move(h));
        exec->start();
    }

    void make() {
        make_manager();
        make_executor();
    }

    // Hand out `l` on the next lease request, then nothing.
    void lease_once(std::vector<cv1::Lease> ls) {
        auto shared = std::make_shared<std::vector<cv1::Lease>>(std::move(ls));
        client.leases.auto_reply = [shared](const cv1::LeaseRequest&) {
            cv1::LeaseReply r;
            for (auto& l : *shared) *r.add_leases() = l;
            shared->clear();
            r.set_lease_ttl_seconds(60);
            return std::optional<Result<cv1::LeaseReply>>(r);
        };
    }

    template <class Pred>
    bool run_until(Pred pred, std::chrono::milliseconds timeout = 5000ms) {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (!pred()) {
            if (std::chrono::steady_clock::now() >= deadline) return false;
            if (io.stopped()) io.restart();
            io.run_one_for(10ms);
        }
        return true;
    }
    void run_for(std::chrono::milliseconds d) {
        const auto deadline = std::chrono::steady_clock::now() + d;
        while (std::chrono::steady_clock::now() < deadline) {
            if (io.stopped()) io.restart();
            io.run_one_for(10ms);
        }
    }

    std::vector<tv1::JobState> update_states(const std::string& job_id) const {
        std::vector<tv1::JobState> out;
        for (const auto& u : client.updates.requests) {
            if (u.job_id == job_id) out.push_back(u.update.state());
        }
        return out;
    }
    const cv1::JobUpdate* last_update(const std::string& job_id) const {
        const cv1::JobUpdate* r = nullptr;
        for (const auto& u : client.updates.requests) {
            if (u.job_id == job_id) r = &u.update;
        }
        return r;
    }
    bool has_terminal_update(const std::string& job_id) const {
        auto u = last_update(job_id);
        return u && tdr::jobs::is_terminal(u->state());
    }
    std::string state_file_text() const {
        std::ifstream in(ropts.state_file);
        std::stringstream ss;
        ss << in.rdbuf();
        return ss.str();
    }
};

}  // namespace

BOOST_AUTO_TEST_SUITE(remote_executor)

BOOST_AUTO_TEST_CASE(local_job_id_format) {
    BOOST_TEST(local_job_id("job-abc", 2) == "job-abc.a2");
}

BOOST_FIXTURE_TEST_CASE(no_lease_when_not_registered_disconnected_or_disabled, Fixture) {
    make();
    run_for(150ms);
    BOOST_TEST(client.leases.requests.empty());  // not registered
    BOOST_TEST(client.announces.requests.empty());

    connected = false;
    exec->on_registered("ff-node-1");
    run_for(200ms);
    BOOST_TEST(client.leases.requests.empty());  // disconnected

    connected = true;  // reconnect without re-registration: the loop picks it up
    BOOST_REQUIRE(run_until([&] { return !client.leases.requests.empty(); }));
    const auto& req = client.leases.requests[0];
    BOOST_TEST(req.node_id() == "ff-node-1");
    BOOST_TEST(req.max_jobs() == 2u);
    BOOST_TEST(req.free().cpu_cores() == 4u);
    BOOST_TEST(req.free().memory_bytes() == 1024 * kMiB);
    BOOST_TEST(req.free().gpu_count() == 0u);
    // Empty replies -> idle poll interval, never two in flight.
    BOOST_REQUIRE(run_until([&] { return client.leases.requests.size() >= 3; }));
    BOOST_TEST(client.leases.pending.empty());

    exec.reset();
    ropts.enabled = false;
    client.leases.requests.clear();
    make_executor();
    exec->on_registered("ff-node-1");
    run_for(200ms);
    BOOST_TEST(client.leases.requests.empty());
    BOOST_TEST(client.announces.requests.size() >= 1u);  // artifact sync still runs
}

BOOST_FIXTURE_TEST_CASE(no_lease_while_local_queue_non_empty_or_no_slot, Fixture) {
    capacity = {4, 512 * kMiB, 3};
    make();
    // A takes most of the memory; B (256 MiB) has to wait in the local queue.
    // Free cores, memory and a job slot remain, so only the queue blocks leasing.
    tv1::JobSpec a;
    a.mutable_command()->add_argv("/bin/sleep");
    a.mutable_command()->add_argv("30");
    a.mutable_resources()->set_memory_bytes(384 * kMiB);
    auto ida = mgr->submit(a);
    BOOST_REQUIRE(ida.has_value());
    BOOST_REQUIRE(run_until([&] { return mgr->usage().running == 1u; }));
    tv1::JobSpec b;
    b.mutable_command()->add_argv("/bin/true");
    b.mutable_resources()->set_memory_bytes(256 * kMiB);
    auto idb = mgr->submit(b);
    BOOST_REQUIRE(idb.has_value());
    BOOST_REQUIRE(mgr->usage().queued == 1u);

    exec->on_registered("ff-node-1");
    run_for(200ms);
    BOOST_TEST(client.leases.requests.empty());

    // A ends -> B runs and ends -> capacity freed -> prompt poll.
    BOOST_REQUIRE(mgr->cancel(*ida).has_value());
    BOOST_REQUIRE(run_until([&] { return !client.leases.requests.empty(); }));
    BOOST_TEST(client.leases.requests[0].max_jobs() >= 2u);
    BOOST_REQUIRE(run_until([&] { return mgr->status(*idb)->state() == tv1::COMPLETED; }));
    BOOST_TEST(client.updates.requests.empty());  // local jobs are never reported

    // All slots busy -> no lease.
    exec.reset();
    capacity = {4, 512 * kMiB, 1};
    mgr->set_capacity(capacity);
    tv1::JobSpec c = a;
    c.mutable_resources()->set_memory_bytes(64 * kMiB);
    BOOST_REQUIRE(mgr->submit(c).has_value());
    BOOST_REQUIRE(run_until([&] { return mgr->usage().running == 1u; }));
    client.leases.requests.clear();
    make_executor();
    exec->on_registered("ff-node-1");
    run_for(200ms);
    BOOST_TEST(client.leases.requests.empty());
}

BOOST_FIXTURE_TEST_CASE(lease_runs_and_reports_in_order_with_outputs, Fixture) {
    make();
    lease_once({make_lease("job-1", kWriteOutput)});
    exec->on_registered("ff-node-1");
    BOOST_REQUIRE(run_until([&] { return has_terminal_update("job-1"); }));

    const auto st = mgr->status("job-1.a1");
    BOOST_REQUIRE(st.has_value());
    BOOST_TEST(st->state() == tv1::COMPLETED);
    BOOST_TEST(st->spec().policy().max_attempts() == 1u);
    BOOST_REQUIRE(st->output_artifacts_size() == 1);

    BOOST_TEST(update_states("job-1") == (std::vector<tv1::JobState>{tv1::RUNNING, tv1::COMPLETED}),
               boost::test_tools::per_element());
    const auto* done = last_update("job-1");
    BOOST_TEST(done->attempt_id() == "att-job-1-1");
    BOOST_TEST(done->fencing_token() == 1u);
    BOOST_REQUIRE(done->outputs_size() == 1);
    BOOST_TEST(done->outputs(0).artifact_id() == st->output_artifacts(0));
    BOOST_TEST(done->outputs(0).media_type() == "test/meta");
    BOOST_TEST(done->outputs(0).size() == 6u);  // "hello\n"

    // Acknowledged -> mapping gone, nothing pending, persisted.
    BOOST_REQUIRE(run_until([&] { return exec->stats().pending_updates == 0; }));
    BOOST_TEST(exec->active_leases().empty());
    BOOST_TEST(!exec->central_job_id("job-1.a1").has_value());
    BOOST_TEST(exec->stats().leases_received == 1u);
    BOOST_TEST(state_file_text().find("job-1") == std::string::npos);

    // Incremental announcement of the output.
    BOOST_REQUIRE(run_until([&] {
        for (const auto& a : client.announces.requests) {
            if (a.sync_id().empty() && a.artifacts_size() == 1 && a.artifacts(0).artifact_id() == st->output_artifacts(0))
                return true;
        }
        return false;
    }));
}

BOOST_FIXTURE_TEST_CASE(failure_is_reported_retryable, Fixture) {
    make();
    lease_once({make_lease("job-f", {"/bin/sh", "-c", "exit 3"}, 2)});
    exec->on_registered("ff-node-1");
    BOOST_REQUIRE(run_until([&] { return has_terminal_update("job-f"); }));
    BOOST_TEST(mgr->status("job-f.a2")->state() == tv1::FAILED);
    const auto* u = last_update("job-f");
    BOOST_TEST(u->state() == tv1::FAILED);
    BOOST_TEST(u->retryable());
    BOOST_TEST(!u->error().empty());
    BOOST_TEST(u->fencing_token() == 2u);
}

BOOST_FIXTURE_TEST_CASE(submit_rejection_is_reported, Fixture) {
    make();
    auto missing = make_lease("job-v", {"/bin/true"});
    missing.mutable_spec()->add_input_artifacts("artifact://sha256/" + std::string(64, '0'));
    auto too_big = make_lease("job-c", {"/bin/true"});
    too_big.mutable_spec()->mutable_resources()->set_cpu_cores(100);
    lease_once({missing, too_big});
    exec->on_registered("ff-node-1");
    BOOST_REQUIRE(run_until([&] { return has_terminal_update("job-v") && has_terminal_update("job-c"); }));

    BOOST_TEST(update_states("job-v") == (std::vector<tv1::JobState>{tv1::FAILED}), boost::test_tools::per_element());
    BOOST_TEST(!last_update("job-v")->retryable());
    BOOST_TEST(last_update("job-v")->error().find("not found locally") != std::string::npos);
    BOOST_TEST(last_update("job-c")->state() == tv1::FAILED);
    BOOST_TEST(last_update("job-c")->retryable());
    BOOST_TEST(!mgr->status("job-v.a1").has_value());
    BOOST_REQUIRE(run_until([&] { return exec->stats().pending_updates == 0; }));
    BOOST_TEST(exec->active_leases().empty());
}

BOOST_FIXTURE_TEST_CASE(outbox_retries_transport_errors_in_order, Fixture) {
    make();
    int failures = 0;
    client.updates.auto_reply = [&](const FakeClient::Update& u) -> std::optional<Result<cv1::Job>> {
        if (failures < 3) {
            ++failures;
            return failures == 2 ? fail<cv1::Job>(503, "unavailable") : fail<cv1::Job>(0, "connection refused");
        }
        cv1::Job j;
        j.set_job_id(u.job_id);
        return j;
    };
    lease_once({make_lease("job-r", {"/bin/true"})});
    exec->on_registered("ff-node-1");
    BOOST_REQUIRE(run_until([&] { return mgr->status("job-r.a1") && mgr->status("job-r.a1")->state() == tv1::COMPLETED; }));
    BOOST_REQUIRE(run_until([&] { return exec->stats().pending_updates == 0; }));
    // RUNNING sent 4x (3 failures), then COMPLETED once; never COMPLETED before RUNNING was acked.
    BOOST_TEST(update_states("job-r") ==
                   (std::vector<tv1::JobState>{tv1::RUNNING, tv1::RUNNING, tv1::RUNNING, tv1::RUNNING, tv1::COMPLETED}),
               boost::test_tools::per_element());
    BOOST_TEST(exec->active_leases().empty());
    BOOST_TEST(exec->stats().stale_updates == 0u);
}

BOOST_FIXTURE_TEST_CASE(outbox_waits_while_disconnected, Fixture) {
    make();
    lease_once({make_lease("job-d", {"/bin/true"})});
    exec->on_registered("ff-node-1");
    BOOST_REQUIRE(run_until([&] { return !client.updates.requests.empty(); }));
    connected = false;
    BOOST_REQUIRE(run_until([&] { return mgr->status("job-d.a1")->state() == tv1::COMPLETED; }));
    run_for(100ms);
    BOOST_TEST(exec->stats().pending_updates == 1u);
    BOOST_TEST(!has_terminal_update("job-d"));
    connected = true;
    BOOST_REQUIRE(run_until([&] { return has_terminal_update("job-d"); }));
}

BOOST_FIXTURE_TEST_CASE(stale_409_drops_update_and_mapping, Fixture) {
    make();
    client.updates.auto_reply = [](const FakeClient::Update&) {
        return std::optional<Result<cv1::Job>>(fail<cv1::Job>(409, "stale attempt"));
    };
    lease_once({make_lease("job-s", {"/bin/true"})});
    exec->on_registered("ff-node-1");
    BOOST_REQUIRE(run_until([&] { return has_terminal_update("job-s"); }));
    BOOST_REQUIRE(run_until([&] { return exec->stats().pending_updates == 0; }));
    BOOST_TEST(exec->stats().stale_updates == 2u);  // RUNNING and COMPLETED
    BOOST_TEST(exec->active_leases().empty());
    BOOST_TEST(!exec->central_job_id("job-s.a1").has_value());
    BOOST_TEST(state_file_text().find("job-s") == std::string::npos);
    BOOST_TEST(mgr->status("job-s.a1")->state() == tv1::COMPLETED);  // local result stays
}

BOOST_FIXTURE_TEST_CASE(revoked_lease_is_cancelled_and_reported, Fixture) {
    make();
    auto l = make_lease("job-x", {"/bin/sleep", "30"}, 3);
    lease_once({l});
    exec->on_registered("ff-node-1");
    BOOST_REQUIRE(run_until([&] { return !update_states("job-x").empty(); }));
    BOOST_TEST(last_update("job-x")->state() == tv1::RUNNING);

    // Heartbeat view.
    auto active = exec->active_leases();
    BOOST_REQUIRE(active.size() == 1u);
    BOOST_TEST(active[0].job_id() == "job-x");
    BOOST_TEST(active[0].attempt_id() == "att-job-x-3");
    BOOST_TEST(active[0].fencing_token() == 3u);
    BOOST_TEST(exec->central_job_id("job-x.a3").value_or("") == "job-x");
    BOOST_TEST(exec->stats().active_leases == 1u);

    // A ref with another token does not match.
    auto wrong = ref_of(l);
    wrong.set_fencing_token(2);
    exec->on_revoked({wrong});
    run_for(100ms);
    BOOST_TEST(mgr->status("job-x.a3")->state() == tv1::RUNNING);

    exec->on_revoked({ref_of(l)});
    BOOST_REQUIRE(run_until([&] { return has_terminal_update("job-x"); }));
    BOOST_TEST(last_update("job-x")->state() == tv1::CANCELLED);
    BOOST_TEST(mgr->status("job-x.a3")->state() == tv1::CANCELLED);
    BOOST_REQUIRE(run_until([&] { return exec->active_leases().empty(); }));
    exec->on_revoked({ref_of(l)});  // again: no mapping, ignored
}

BOOST_FIXTURE_TEST_CASE(restart_reconciles_state_file, Fixture) {
    capacity = {4, 1024 * kMiB, 3};
    make();
    connected = false;  // nothing is delivered in the first life
    lease_once({make_lease("job-a", {"/bin/true"}), make_lease("job-b", {"/bin/sleep", "30"}),
                make_lease("job-c", {"/bin/sleep", "30"})});
    exec->on_registered("ff-node-1");
    connected = true;
    BOOST_REQUIRE(run_until([&] { return exec->stats().leases_received == 3; }));
    connected = false;
    BOOST_REQUIRE(run_until([&] { return mgr->status("job-a.a1")->state() == tv1::COMPLETED; }));
    BOOST_REQUIRE(mgr->status("job-b.a1")->state() == tv1::RUNNING);
    // Updates sent so far (RUNNING of some jobs) may have been acknowledged;
    // whatever else is in the outbox is persisted.
    const auto sent_before = client.updates.requests.size();

    // Crash: executor and manager go away; job-b/job-c keep running as orphans.
    exec.reset();
    mgr.reset();
    journal.reset();
    sup.reset();

    // A ghost lease in the state file whose local job never existed (e.g. a
    // crash between persisting the mapping and the local submit).
    {
        std::ifstream in(ropts.state_file);
        std::stringstream ss;
        ss << in.rdbuf();
        auto root = boost::json::parse(ss.str()).as_object();
        root["leases"].as_array().push_back(boost::json::object{{"local_job_id", "ghost.a1"},
                                                                {"job_id", "ghost"},
                                                                {"attempt_id", "att-ghost"},
                                                                {"fencing_token", 1},
                                                                {"attempt_number", 1},
                                                                {"terminal", false}});
        std::ofstream(ropts.state_file) << boost::json::serialize(root);
    }

    // Second life: recover() fails job-b and job-c (max_attempts 1, process gone).
    make_manager();
    BOOST_TEST(mgr->status("job-b.a1")->state() == tv1::FAILED);
    make_executor();
    BOOST_TEST(exec->active_leases().empty());
    BOOST_TEST(exec->stats().pending_updates >= 4u);  // a COMPLETED, b FAILED, c FAILED, ghost FAILED
    BOOST_TEST(client.updates.requests.size() == sent_before);  // nothing before on_registered

    connected = true;
    exec->on_registered("ff-node-1");
    BOOST_REQUIRE(run_until([&] { return exec->stats().pending_updates == 0; }));
    BOOST_TEST(last_update("job-a")->state() == tv1::COMPLETED);
    BOOST_TEST(last_update("job-a")->outputs_size() == 0);
    BOOST_TEST(last_update("job-b")->state() == tv1::FAILED);
    BOOST_TEST(last_update("job-b")->retryable());
    BOOST_TEST(last_update("job-c")->state() == tv1::FAILED);
    BOOST_REQUIRE(last_update("ghost") != nullptr);
    BOOST_TEST(last_update("ghost")->state() == tv1::FAILED);
    BOOST_TEST(last_update("ghost")->retryable());
    BOOST_TEST(last_update("ghost")->error() == "local job lost");
    BOOST_TEST(last_update("ghost")->attempt_id() == "att-ghost");
    // Each job reported exactly one terminal update.
    for (const auto* id : {"job-a", "job-b", "job-c", "ghost"}) {
        int n = 0;
        for (auto s : update_states(id)) n += tdr::jobs::is_terminal(s) ? 1 : 0;
        BOOST_TEST(n == 1, id);
    }
}

BOOST_FIXTURE_TEST_CASE(restart_keeps_running_lease_active, Fixture) {
    make();
    auto l = make_lease("job-k", {"/bin/sleep", "30"});
    lease_once({l});
    exec->on_registered("ff-node-1");
    BOOST_REQUIRE(run_until([&] { return !update_states("job-k").empty() && exec->stats().pending_updates == 0; }));
    // Executor restart only (tdrd keeps the job): the lease is still active.
    exec.reset();
    make_executor();
    auto active = exec->active_leases();
    BOOST_REQUIRE(active.size() == 1u);
    BOOST_TEST(active[0].job_id() == "job-k");
    BOOST_TEST(exec->stats().pending_updates == 0u);
    exec->on_registered("ff-node-1");
    BOOST_REQUIRE(mgr->cancel("job-k.a1").has_value());
    BOOST_REQUIRE(run_until([&] { return has_terminal_update("job-k"); }));
    BOOST_TEST(last_update("job-k")->state() == tv1::CANCELLED);
}

BOOST_FIXTURE_TEST_CASE(corrupt_state_file_is_moved_aside, Fixture) {
    fs::create_directories(ropts.state_file.parent_path());
    std::ofstream(ropts.state_file) << "{not json";
    make();
    BOOST_TEST(exec->active_leases().empty());
    BOOST_TEST(fs::exists(ropts.state_file.string() + ".corrupt"));
    BOOST_TEST(state_file_text().find("\"version\":1") != std::string::npos);  // rewritten empty
}

BOOST_FIXTURE_TEST_CASE(full_artifact_sync_pages, Fixture) {
    ropts.announce_page = 2;
    for (int i = 0; i < 5; ++i) {
        cv1::ArtifactRef r;
        r.set_artifact_id("artifact://sha256/" + std::string(63, '0') + std::to_string(i));
        local_artifacts.push_back(r);
    }
    ropts.enabled = false;
    make();
    // First sync attempt fails on its second page -> whole sync restarts with a new id.
    int calls = 0;
    client.announces.auto_reply = [&](const cv1::ArtifactAnnouncement&) -> std::optional<Result<void>> {
        if (++calls == 2) return fail<void>(503, "busy");
        return Result<void>{};
    };
    exec->on_registered("ff-node-1");
    BOOST_REQUIRE(run_until([&] { return client.announces.requests.size() == 5u; }));
    run_for(100ms);
    const auto& a = client.announces.requests;
    BOOST_REQUIRE(a.size() == 5u);
    BOOST_TEST(!a[0].sync_id().empty());
    BOOST_TEST(a[1].sync_id() == a[0].sync_id());
    const std::string second = a[2].sync_id();
    BOOST_TEST(second != a[0].sync_id());
    std::set<std::string> ids;
    for (int i = 2; i < 5; ++i) {
        BOOST_TEST(a[i].sync_id() == second);
        BOOST_TEST(a[i].node_id() == "ff-node-1");
        BOOST_TEST(a[i].full() == (i == 4));
        for (const auto& r : a[i].artifacts()) ids.insert(r.artifact_id());
    }
    BOOST_TEST(a[2].artifacts_size() == 2);
    BOOST_TEST(a[4].artifacts_size() == 1);
    BOOST_TEST(ids.size() == 5u);

    // Empty store: one page, full.
    local_artifacts.clear();
    client.announces.requests.clear();
    exec->on_registered("ff-node-1");
    BOOST_REQUIRE(run_until([&] { return !client.announces.requests.empty(); }));
    run_for(50ms);
    BOOST_REQUIRE(client.announces.requests.size() == 1u);
    BOOST_TEST(client.announces.requests[0].full());
    BOOST_TEST(client.announces.requests[0].artifacts_size() == 0);
    BOOST_TEST(!client.announces.requests[0].sync_id().empty());
}

BOOST_FIXTURE_TEST_CASE(local_job_outputs_announced_incrementally, Fixture) {
    ropts.enabled = false;
    make();
    exec->on_registered("ff-node-1");
    BOOST_REQUIRE(run_until([&] { return !client.announces.requests.empty(); }));  // full sync
    tv1::JobSpec s;
    for (const auto& a : kWriteOutput) s.mutable_command()->add_argv(a);
    auto id = mgr->submit(s);
    BOOST_REQUIRE(id.has_value());
    BOOST_REQUIRE(run_until([&] { return client.announces.requests.size() == 2u; }));
    const auto& a = client.announces.requests[1];
    BOOST_TEST(a.sync_id().empty());
    BOOST_TEST(!a.full());
    BOOST_REQUIRE(a.artifacts_size() == 1);
    BOOST_TEST(a.artifacts(0).artifact_id() == mgr->status(*id)->output_artifacts(0));
    BOOST_TEST(a.artifacts(0).media_type() == "test/meta");
    BOOST_TEST(client.updates.requests.empty());
}

BOOST_FIXTURE_TEST_CASE(destroy_with_requests_in_flight_is_safe, Fixture) {
    make();
    client.leases.auto_reply = nullptr;
    client.updates.auto_reply = nullptr;
    client.announces.auto_reply = nullptr;
    exec->on_registered("ff-node-1");
    BOOST_REQUIRE(run_until([&] { return !client.leases.pending.empty() && !client.announces.pending.empty(); }));
    cv1::LeaseReply r;
    *r.add_leases() = make_lease("job-z", {"/bin/sleep", "30"});
    client.answer(client.leases, Result<cv1::LeaseReply>(r));
    BOOST_REQUIRE(run_until([&] { return !client.updates.pending.empty(); }));

    exec.reset();  // lease poll, status update and sync all pending / armed
    client.answer(client.updates, Result<cv1::Job>(cv1::Job{}));
    client.answer(client.announces, Result<void>{});
    run_for(100ms);

    // stop(): later callbacks and job events are no-ops too.
    make_executor();
    exec->on_registered("ff-node-1");
    BOOST_REQUIRE(run_until([&] { return !client.updates.pending.empty(); }));
    exec->stop();
    const auto n = client.updates.requests.size();
    while (!client.updates.pending.empty()) client.answer(client.updates, Result<cv1::Job>(cv1::Job{}));
    while (!client.leases.pending.empty()) client.answer(client.leases, Result<cv1::LeaseReply>(cv1::LeaseReply{}));
    while (!client.announces.pending.empty()) client.answer(client.announces, Result<void>{});
    BOOST_REQUIRE(mgr->cancel("job-z.a1").has_value());
    run_for(200ms);
    BOOST_TEST(client.updates.requests.size() == n);
}

BOOST_AUTO_TEST_SUITE_END()
