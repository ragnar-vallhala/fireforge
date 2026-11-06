// RemoteExecutor against a fake control client and a REAL JobManager (sqlite
// journal, local artifact store, Linux supervisor without cgroups) running
// tiny commands.
#include <boost/asio/post.hpp>
#include <boost/json.hpp>
#include <boost/test/unit_test.hpp>
#include <chrono>
#include <deque>
#include <fstream>
#include <map>
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
    void get_artifact(const std::string&, std::function<void(Result<cv1::ArtifactInfo>)>) override {
        BOOST_FAIL("unexpected get_artifact");
    }
    void release_artifact(const std::string&, const cv1::ReleaseRequest&,
                          std::function<void(Result<cv1::ReleaseReply>)>) override {
        BOOST_FAIL("unexpected release_artifact");
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

// IArtifactFetcher fake over the fixture's real store. Per id: ingest the
// registered content (success), fail with a reason, or hang (callback kept).
// Present artifacts are answered at once (still via post).
class FakeFetcher final : public tdr::transfer::IArtifactFetcher {
public:
    enum class Mode { kIngest, kFail, kHang };
    FakeFetcher(asio::io_context& io, tdr::artifacts::IArtifactStore& store, fs::path tmp)
        : io_(io), store_(store), tmp_(std::move(tmp)) {}

    Mode mode = Mode::kIngest;
    std::string fail_reason = "no holder";
    std::map<std::string, std::string> content;  // id -> bytes
    std::vector<std::string> calls;
    std::vector<std::pair<std::string, std::function<void(std::expected<tdr::artifacts::ArtifactMeta, std::string>)>>>
        hung;

    // Registers `data` and returns its artifact id (not stored yet).
    std::string add(const std::string& data) {
        const fs::path f = tmp_ / tdr::new_id("content");
        std::ofstream(f, std::ios::binary) << data;
        auto hex = tdr::artifacts::sha256_file(f);
        fs::remove(f);
        BOOST_REQUIRE(hex.has_value());
        const std::string id = tdr::artifacts::make_artifact_id(*hex);
        content[id] = data;
        return id;
    }

    void fetch(const std::string& id,
               std::function<void(std::expected<tdr::artifacts::ArtifactMeta, std::string>)> cb) override {
        calls.push_back(id);
        using R = std::expected<tdr::artifacts::ArtifactMeta, std::string>;
        if (store_.contains(id)) {
            tdr::artifacts::ArtifactMeta m;
            m.id = id;
            m.local_path = *store_.path_of(id);
            return post(std::move(cb), R(m));
        }
        switch (mode) {
            case Mode::kHang: hung.emplace_back(id, std::move(cb)); return;
            case Mode::kFail: return post(std::move(cb), R(std::unexpected(fail_reason)));
            case Mode::kIngest: {
                auto it = content.find(id);
                if (it == content.end()) return post(std::move(cb), R(std::unexpected(std::string("unknown"))));
                const fs::path f = tmp_ / tdr::new_id("fetched");
                std::ofstream(f, std::ios::binary) << it->second;
                auto m = store_.ingest(f, "application/octet-stream", "", true);
                if (!m) return post(std::move(cb), R(std::unexpected(m.error())));
                return post(std::move(cb), R(*m));
            }
        }
    }

private:
    template <class Cb, class R>
    void post(Cb cb, R r) {
        asio::post(io_, [cb = std::move(cb), r = std::move(r)]() mutable { cb(std::move(r)); });
    }
    asio::io_context& io_;
    tdr::artifacts::IArtifactStore& store_;
    fs::path tmp_;
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
    std::unique_ptr<FakeFetcher> fetcher;  // set_fetcher() in make_executor() when present
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
        mgr->set_checkpoint_listener([this](const std::string& id, const tv1::NamedArtifact& a) {
            if (exec) exec->on_checkpoint(id, a);  // what tdrd does
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
        if (fetcher) exec->set_fetcher(fetcher.get());
    }

    void make() {
        make_manager();
        make_executor();
    }
    void make_with_fetcher() {
        make_manager();
        fetcher = std::make_unique<FakeFetcher>(io, *store, tmp.path);
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
    // Phase 5: a missing input without a fetcher is retryable elsewhere.
    BOOST_TEST(last_update("job-v")->retryable());
    BOOST_TEST(last_update("job-v")->error() == "artifact transfer unavailable");
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
    BOOST_TEST(state_file_text().find("\"version\":3") != std::string::npos);  // rewritten empty
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

// ---- Phase 5: inputs fetched before run (phase5.md §5) ----------------------

namespace {
std::string hex_of(const std::string& id) { return id.substr(std::string("artifact://sha256/").size()); }
}  // namespace

BOOST_FIXTURE_TEST_CASE(missing_input_is_fetched_then_job_runs, Fixture) {
    make_with_fetcher();
    // One input already present, one fetched.
    const std::string present = fetcher->add("local-bytes\n");
    {
        const fs::path f = tmp.path / "present";
        std::ofstream(f) << "local-bytes\n";
        BOOST_REQUIRE(store->ingest(f, "text/plain", "", true).has_value());
    }
    const std::string remote = fetcher->add("remote-input-data\n");
    BOOST_REQUIRE(!store->contains(remote));
    auto l = make_lease("job-in", {"/bin/sh", "-c",
                                   "cat \"$FIREFORGE_INPUT_DIR/" + hex_of(remote) + "\" \"$FIREFORGE_INPUT_DIR/" +
                                       hex_of(present) + "\" > \"$FIREFORGE_OUTPUT_DIR/out.txt\""});
    l.mutable_spec()->add_input_artifacts(remote);
    l.mutable_spec()->add_input_artifacts(present);
    l.mutable_spec()->add_input_artifacts(remote);  // duplicates fetched once
    lease_once({l});
    exec->on_registered("ff-node-1");
    BOOST_REQUIRE(run_until([&] { return has_terminal_update("job-in"); }));
    BOOST_TEST(last_update("job-in")->state() == tv1::COMPLETED);
    BOOST_TEST(update_states("job-in") == (std::vector<tv1::JobState>{tv1::RUNNING, tv1::COMPLETED}),
               boost::test_tools::per_element());
    BOOST_TEST(fetcher->calls.size() == 2u);
    BOOST_TEST(store->contains(remote));
    const auto st = mgr->status("job-in.a1");
    BOOST_REQUIRE(st.has_value());
    BOOST_REQUIRE(st->output_artifacts_size() == 1);
    BOOST_TEST(fs::file_size(*store->path_of(st->output_artifacts(0))) ==
               std::string("remote-input-data\nlocal-bytes\n").size());
    BOOST_TEST(exec->protected_inputs().empty());
}

BOOST_FIXTURE_TEST_CASE(fetch_failure_is_reported_retryable, Fixture) {
    make_with_fetcher();
    fetcher->mode = FakeFetcher::Mode::kFail;
    fetcher->fail_reason = "no holder";
    const std::string id = fetcher->add("x");
    auto l = make_lease("job-ff", {"/bin/true"});
    l.mutable_spec()->add_input_artifacts(id);
    lease_once({l});
    exec->on_registered("ff-node-1");
    BOOST_REQUIRE(run_until([&] { return has_terminal_update("job-ff"); }));
    BOOST_TEST(update_states("job-ff") == (std::vector<tv1::JobState>{tv1::FAILED}), boost::test_tools::per_element());
    BOOST_TEST(last_update("job-ff")->retryable());
    BOOST_TEST(last_update("job-ff")->error() == "fetch " + id + ": no holder");
    BOOST_TEST(!mgr->status("job-ff.a1").has_value());
    BOOST_REQUIRE(run_until([&] { return exec->active_leases().empty() && exec->stats().pending_updates == 0; }));
    BOOST_TEST(exec->protected_inputs().empty());
}

BOOST_FIXTURE_TEST_CASE(no_fetcher_fails_retryable, Fixture) {
    make();
    auto l = make_lease("job-nf", {"/bin/true"});
    l.mutable_spec()->add_input_artifacts("artifact://sha256/" + std::string(64, 'a'));
    lease_once({l});
    exec->on_registered("ff-node-1");
    BOOST_REQUIRE(run_until([&] { return has_terminal_update("job-nf"); }));
    BOOST_TEST(last_update("job-nf")->state() == tv1::FAILED);
    BOOST_TEST(last_update("job-nf")->retryable());
    BOOST_TEST(last_update("job-nf")->error() == "artifact transfer unavailable");
}

BOOST_FIXTURE_TEST_CASE(capacity_is_held_while_fetching, Fixture) {
    capacity = {4, 1024 * kMiB, 2};
    make_with_fetcher();
    fetcher->mode = FakeFetcher::Mode::kHang;
    const std::string id = fetcher->add("slow-input");
    auto l = make_lease("job-h", {"/bin/true"});
    l.mutable_spec()->mutable_resources()->set_cpu_cores(2);
    l.mutable_spec()->mutable_resources()->set_memory_bytes(256 * kMiB);
    l.mutable_spec()->add_input_artifacts(id);
    lease_once({l});
    exec->on_registered("ff-node-1");
    BOOST_REQUIRE(run_until([&] { return fetcher->hung.size() == 1u; }));
    // Next lease request: one slot, 2 cores and 768 MiB left.
    const auto before = client.leases.requests.size();
    BOOST_REQUIRE(run_until([&] { return client.leases.requests.size() > before; }));
    const auto& req = client.leases.requests.back();
    BOOST_TEST(req.max_jobs() == 1u);
    BOOST_TEST(req.free().cpu_cores() == 2u);
    BOOST_TEST(req.free().memory_bytes() == 768 * kMiB);
    // Renewed by heartbeats, protected from GC, no local job yet.
    BOOST_REQUIRE(exec->active_leases().size() == 1u);
    BOOST_TEST(exec->stats().active_leases == 1u);
    BOOST_TEST(exec->protected_inputs() == std::vector<std::string>{id}, boost::test_tools::per_element());
    BOOST_TEST(!mgr->status("job-h.a1").has_value());
    BOOST_TEST(update_states("job-h").empty());

    // A second fetching lease takes the last slot: no more lease requests.
    auto l2 = make_lease("job-h2", {"/bin/true"});
    l2.mutable_spec()->add_input_artifacts(id);
    lease_once({l2});
    BOOST_REQUIRE(run_until([&] { return fetcher->hung.size() == 2u; }));
    const auto n = client.leases.requests.size();
    run_for(200ms);
    BOOST_TEST(client.leases.requests.size() == n);

    // Fetches complete: the jobs run and capacity is released.
    auto hung = std::move(fetcher->hung);
    fetcher->hung.clear();
    for (auto& [hid, cb] : hung) {
        const fs::path f = tmp.path / tdr::new_id("late");
        std::ofstream(f) << fetcher->content.at(hid);
        auto m = store->ingest(f, "application/octet-stream", "", true);
        BOOST_REQUIRE(m.has_value());
        cb(*m);
    }
    BOOST_REQUIRE(run_until([&] { return has_terminal_update("job-h") && has_terminal_update("job-h2"); }));
    BOOST_TEST(last_update("job-h")->state() == tv1::COMPLETED);
    BOOST_TEST(last_update("job-h2")->state() == tv1::COMPLETED);
    BOOST_TEST(exec->protected_inputs().empty());
    const auto m = client.leases.requests.size();
    BOOST_REQUIRE(run_until([&] { return client.leases.requests.size() > m; }));
    BOOST_TEST(client.leases.requests.back().max_jobs() == 2u);
}

BOOST_FIXTURE_TEST_CASE(revoked_while_fetching_is_cancelled, Fixture) {
    make_with_fetcher();
    fetcher->mode = FakeFetcher::Mode::kHang;
    const std::string id = fetcher->add("never");
    auto l = make_lease("job-r", {"/bin/true"}, 2);
    l.mutable_spec()->add_input_artifacts(id);
    lease_once({l});
    exec->on_registered("ff-node-1");
    BOOST_REQUIRE(run_until([&] { return fetcher->hung.size() == 1u; }));
    exec->on_revoked({ref_of(l)});
    BOOST_REQUIRE(run_until([&] { return has_terminal_update("job-r"); }));
    BOOST_TEST(update_states("job-r") == (std::vector<tv1::JobState>{tv1::CANCELLED}),
               boost::test_tools::per_element());
    BOOST_TEST(last_update("job-r")->fencing_token() == 2u);
    BOOST_REQUIRE(run_until([&] { return exec->active_leases().empty() && exec->stats().pending_updates == 0; }));
    BOOST_TEST(exec->protected_inputs().empty());
    // The fetch finishing later starts nothing.
    const fs::path f = tmp.path / "late";
    std::ofstream(f) << "never";
    auto m = store->ingest(f, "application/octet-stream", "", true);
    BOOST_REQUIRE(m.has_value());
    fetcher->hung[0].second(*m);
    run_for(100ms);
    BOOST_TEST(!mgr->status("job-r.a2").has_value());
    BOOST_TEST(update_states("job-r").size() == 1u);
}

BOOST_FIXTURE_TEST_CASE(restart_while_fetching_refetches_and_runs, Fixture) {
    make_with_fetcher();
    fetcher->mode = FakeFetcher::Mode::kHang;
    const std::string id = fetcher->add("resumed-input\n");
    auto l = make_lease("job-rs", {"/bin/sh", "-c",
                                   "cat \"$FIREFORGE_INPUT_DIR/" + hex_of(id) + "\" > \"$FIREFORGE_OUTPUT_DIR/o\""});
    l.mutable_spec()->add_input_artifacts(id);
    lease_once({l});
    exec->on_registered("ff-node-1");
    BOOST_REQUIRE(run_until([&] { return fetcher->hung.size() == 1u; }));
    BOOST_TEST(state_file_text().find("\"submitted\":false") != std::string::npos);

    // tdrd restart: the executor goes away mid-fetch; its callback is a no-op.
    exec->stop();
    fetcher->hung[0].second(std::unexpected(std::string("transfer stopped")));
    run_for(50ms);
    exec.reset();
    fetcher->hung.clear();
    const auto sent = client.updates.requests.size();
    BOOST_TEST(sent == 0u);

    // Second life without a fetcher yet: waits (no "local job lost", nothing sent).
    auto saved = std::move(fetcher);
    make_executor();
    exec->on_registered("ff-node-1");
    run_for(150ms);
    BOOST_TEST(client.updates.requests.empty());
    BOOST_REQUIRE(exec->active_leases().size() == 1u);
    BOOST_TEST(exec->protected_inputs() == std::vector<std::string>{id}, boost::test_tools::per_element());

    // The peer layer comes up: fetch again and run.
    fetcher = std::move(saved);
    fetcher->mode = FakeFetcher::Mode::kIngest;
    exec->set_fetcher(fetcher.get());
    BOOST_REQUIRE(run_until([&] { return has_terminal_update("job-rs"); }));
    BOOST_TEST(update_states("job-rs") == (std::vector<tv1::JobState>{tv1::RUNNING, tv1::COMPLETED}),
               boost::test_tools::per_element());
    BOOST_TEST(fetcher->calls.size() == 2u);
    BOOST_TEST(mgr->status("job-rs.a1")->state() == tv1::COMPLETED);
    BOOST_REQUIRE(run_until([&] { return exec->stats().pending_updates == 0; }));
    BOOST_TEST(state_file_text().find("job-rs") == std::string::npos);
}

BOOST_FIXTURE_TEST_CASE(version1_state_file_is_read, Fixture) {
    fs::create_directories(ropts.state_file.parent_path());
    std::ofstream(ropts.state_file) << R"({"version":1,"leases":[{"local_job_id":"old.a1","job_id":"old",
        "attempt_id":"att-old","fencing_token":1,"attempt_number":1,"terminal":false}],"outbox":[]})";
    make();
    BOOST_TEST(exec->stats().pending_updates == 1u);
    exec->on_registered("ff-node-1");
    BOOST_REQUIRE(run_until([&] { return has_terminal_update("old"); }));
    BOOST_TEST(last_update("old")->error() == "local job lost");
    BOOST_TEST(state_file_text().find("\"version\":3") != std::string::npos);
}

BOOST_FIXTURE_TEST_CASE(announce_reaches_client, Fixture) {
    ropts.enabled = false;
    make();
    cv1::ArtifactRef r;
    r.set_artifact_id("artifact://sha256/" + std::string(64, 'b'));
    r.set_size(7);
    exec->announce({r});  // queued until registered
    run_for(50ms);
    BOOST_TEST(client.announces.requests.empty());
    exec->on_registered("ff-node-1");
    BOOST_REQUIRE(run_until([&] {
        for (const auto& a : client.announces.requests) {
            if (a.sync_id().empty() && a.artifacts_size() == 1 && a.artifacts(0).artifact_id() == r.artifact_id())
                return true;
        }
        return false;
    }));
}

// ---- Phase 6: named outputs, spawn, checkpoints, resume ---------------------------

namespace {
// Runs until $FIREFORGE_WORKDIR/go exists, writing out.txt first.
const std::vector<std::string> kWaitForGo = {
    "/bin/sh", "-c",
    "echo hello > \"$FIREFORGE_OUTPUT_DIR/out.txt\"; while [ ! -f \"$FIREFORGE_WORKDIR/go\" ]; do sleep 0.02; done"};

fs::path workdir_of(const tdr::jobs::JobManager& m, const fs::path& root, const std::string& local) {
    auto st = m.status(local);
    return root / local / st->current_attempt_id();
}

tv1::WorkerResult named_result(const std::string& att) {
    tv1::WorkerResult r;
    r.set_attempt_id(att);
    r.set_success(true);
    auto* o = r.add_outputs();
    o->set_path("out.txt");
    o->set_name("greeting");
    o->set_media_type("text/plain");
    auto* sp = r.add_spawn();
    sp->set_name("cand-1");
    sp->mutable_job()->set_operation("relax");
    r.add_spawn()->set_name("cand-2");
    return r;
}
}  // namespace

BOOST_FIXTURE_TEST_CASE(completion_carries_named_outputs_and_spawn, Fixture) {
    make();
    lease_once({make_lease("job-s", kWaitForGo)});
    exec->on_registered("ff-node-1");
    BOOST_REQUIRE(run_until([&] { return update_states("job-s").size() == 1u && exec->stats().pending_updates == 0; }));
    const fs::path wd = workdir_of(*mgr, tmp.path / "work", "job-s.a1");
    BOOST_REQUIRE(mgr->on_worker_result(named_result(mgr->status("job-s.a1")->current_attempt_id())).has_value());
    std::ofstream(wd / "go") << "";
    BOOST_REQUIRE(run_until([&] { return has_terminal_update("job-s"); }));
    const auto* u = last_update("job-s");
    BOOST_TEST(u->state() == tv1::COMPLETED);
    BOOST_REQUIRE(u->outputs_size() == 1);
    BOOST_TEST(u->outputs(0).name() == "greeting");
    BOOST_TEST(u->outputs(0).size() == 6u);
    BOOST_TEST(u->outputs(0).artifact_id() == mgr->status("job-s.a1")->output_artifacts(0));
    BOOST_REQUIRE(u->spawn_size() == 2);
    BOOST_TEST(u->spawn(0).name() == "cand-1");
    BOOST_TEST(u->spawn(0).job().operation() == "relax");
    BOOST_TEST(u->spawn(1).name() == "cand-2");
    // The incremental announcement of the output has no name.
    BOOST_REQUIRE(run_until([&] {
        for (const auto& a : client.announces.requests)
            if (a.sync_id().empty() && a.artifacts_size() == 1 && a.artifacts(0).artifact_id() == u->outputs(0).artifact_id())
                return a.artifacts(0).name().empty();
        return false;
    }));
}

BOOST_FIXTURE_TEST_CASE(named_outputs_and_spawn_survive_executor_restart, Fixture) {
    capacity = {4, 1024 * kMiB, 3};
    make();
    lease_once({make_lease("job-p", kWaitForGo), make_lease("job-q", kWaitForGo)});
    exec->on_registered("ff-node-1");
    BOOST_REQUIRE(run_until([&] {
        return update_states("job-p").size() == 1u && update_states("job-q").size() == 1u &&
               exec->stats().pending_updates == 0;
    }));
    // job-p completes while disconnected: its COMPLETED update waits in the
    // persisted outbox.
    connected = false;
    BOOST_REQUIRE(mgr->on_worker_result(named_result(mgr->status("job-p.a1")->current_attempt_id())).has_value());
    std::ofstream(workdir_of(*mgr, tmp.path / "work", "job-p.a1") / "go") << "";
    BOOST_REQUIRE(run_until([&] { return mgr->status("job-p.a1")->state() == tv1::COMPLETED; }));
    BOOST_REQUIRE(run_until([&] { return exec->stats().pending_updates == 1u; }));
    BOOST_TEST(state_file_text().find("cand-2") != std::string::npos);
    BOOST_TEST(state_file_text().find("\"version\":3") != std::string::npos);
    // job-q completes while no executor runs: start() queues it from the
    // JobManager's status (journal).
    exec.reset();
    BOOST_REQUIRE(mgr->on_worker_result(named_result(mgr->status("job-q.a1")->current_attempt_id())).has_value());
    std::ofstream(workdir_of(*mgr, tmp.path / "work", "job-q.a1") / "go") << "";
    BOOST_REQUIRE(run_until([&] { return mgr->status("job-q.a1")->state() == tv1::COMPLETED; }));

    const auto sent = client.updates.requests.size();
    connected = true;
    make_executor();
    exec->on_registered("ff-node-1");
    BOOST_REQUIRE(run_until([&] { return has_terminal_update("job-p") && has_terminal_update("job-q"); }));
    BOOST_TEST(client.updates.requests.size() == sent + 2);
    for (const auto* id : {"job-p", "job-q"}) {
        const auto* u = last_update(id);
        BOOST_TEST(u->state() == tv1::COMPLETED, id);
        BOOST_REQUIRE(u->outputs_size() == 1);
        BOOST_TEST(u->outputs(0).name() == "greeting", id);
        BOOST_REQUIRE(u->spawn_size() == 2);
        BOOST_TEST(u->spawn(1).name() == "cand-2", id);
    }
}

BOOST_FIXTURE_TEST_CASE(checkpoint_update_queued_coalesced_fenced_and_announced, Fixture) {
    make();
    auto l = make_lease("job-ck", kWaitForGo, 2);
    l.mutable_spec()->mutable_policy()->set_checkpoint(true);
    lease_once({l});
    exec->on_registered("ff-node-1");
    BOOST_REQUIRE(run_until([&] { return update_states("job-ck").size() == 1u && exec->stats().pending_updates == 0; }));
    const auto att = mgr->status("job-ck.a2")->current_attempt_id();
    const fs::path ckd = workdir_of(*mgr, tmp.path / "work", "job-ck.a2") / "checkpoints";
    auto checkpoint = [&](const std::string& data) {
        std::ofstream(ckd / "state.bin", std::ios::trunc) << data;
        tv1::WorkerCheckpoint c;
        c.set_attempt_id(att);
        c.set_path("state.bin");
        c.set_name("state");
        c.set_media_type("application/x-state");
        auto r = mgr->on_worker_checkpoint(c);
        BOOST_REQUIRE_MESSAGE(r.has_value(), (r ? "" : r.error()));
        return *r;
    };

    // Disconnected: two checkpoints coalesce into one queued update.
    connected = false;
    const auto first = checkpoint("step-1");
    BOOST_TEST(exec->stats().pending_updates == 1u);
    const auto second = checkpoint("step-2");
    BOOST_TEST(exec->stats().pending_updates == 1u);
    BOOST_TEST(first.artifact_id() != second.artifact_id());
    BOOST_TEST(state_file_text().find(second.artifact_id()) != std::string::npos);
    connected = true;
    BOOST_REQUIRE(run_until([&] { return update_states("job-ck").size() == 2u && exec->stats().pending_updates == 0; }));
    const auto* u = last_update("job-ck");
    BOOST_TEST(u->state() == tv1::RUNNING);
    BOOST_TEST(u->attempt_id() == "att-job-ck-2");
    BOOST_TEST(u->fencing_token() == 2u);
    BOOST_REQUIRE(u->has_checkpoint());
    BOOST_TEST(u->checkpoint().artifact_id() == second.artifact_id());
    BOOST_TEST(u->checkpoint().name() == "state");
    BOOST_TEST(u->checkpoint().size() == 6u);
    BOOST_TEST(u->checkpoint().media_type() == "application/x-state");

    // In flight: a new checkpoint does not touch the update being sent.
    client.updates.auto_reply = nullptr;
    const auto third = checkpoint("step-3");
    BOOST_REQUIRE(run_until([&] { return !client.updates.pending.empty(); }));
    const auto fourth = checkpoint("step-4");
    const auto fifth = checkpoint("step-5");  // coalesces with fourth
    BOOST_TEST(exec->stats().pending_updates == 2u);
    client.answer(client.updates, Result<cv1::Job>(cv1::Job{}));
    BOOST_REQUIRE(run_until([&] { return client.updates.pending.size() == 1u; }));
    client.answer(client.updates, Result<cv1::Job>(cv1::Job{}));
    BOOST_REQUIRE(run_until([&] { return exec->stats().pending_updates == 0; }));
    const auto& reqs = client.updates.requests;
    BOOST_REQUIRE(reqs.size() >= 2u);
    BOOST_TEST(reqs[reqs.size() - 2].update.checkpoint().artifact_id() == third.artifact_id());
    BOOST_TEST(reqs.back().update.checkpoint().artifact_id() == fifth.artifact_id());
    (void)fourth;

    // Each checkpoint is announced (without a name).
    BOOST_REQUIRE(run_until([&] {
        std::set<std::string> ids;
        for (const auto& a : client.announces.requests)
            for (const auto& r : a.artifacts())
                if (a.sync_id().empty() && r.name().empty()) ids.insert(r.artifact_id());
        return ids.contains(first.artifact_id()) && ids.contains(fifth.artifact_id());
    }));

    // After the terminal update is queued, checkpoints no longer produce updates.
    client.updates.auto_reply = [](const FakeClient::Update& up) {
        cv1::Job j;
        j.set_job_id(up.job_id);
        return std::optional<Result<cv1::Job>>(j);
    };
    std::ofstream(workdir_of(*mgr, tmp.path / "work", "job-ck.a2") / "go") << "";
    BOOST_REQUIRE(run_until([&] { return has_terminal_update("job-ck"); }));
    const auto n = client.updates.requests.size();
    exec->on_checkpoint("job-ck.a2", fifth);
    run_for(100ms);
    BOOST_TEST(client.updates.requests.size() == n);
    // Local (non-lease) jobs: announced only, never reported.
    exec->on_checkpoint("local-job", first);
    run_for(50ms);
    BOOST_TEST(client.updates.requests.size() == n);
}

BOOST_FIXTURE_TEST_CASE(resume_checkpoint_fetched_before_submit, Fixture) {
    make_with_fetcher();
    fetcher->mode = FakeFetcher::Mode::kHang;
    const std::string ck = fetcher->add("checkpoint-bytes\n");
    auto l = make_lease("job-rc", {"/bin/sh", "-c",
                                   "cat \"$FIREFORGE_CHECKPOINT_DIR/resume/checkpoint\" > \"$FIREFORGE_OUTPUT_DIR/out.txt\""});
    l.mutable_spec()->set_resume_checkpoint(ck);
    l.mutable_spec()->mutable_policy()->set_checkpoint(true);
    lease_once({l});
    exec->on_registered("ff-node-1");
    BOOST_REQUIRE(run_until([&] { return fetcher->hung.size() == 1u; }));
    BOOST_TEST(fetcher->hung[0].first == ck);
    BOOST_TEST(!mgr->status("job-rc.a1").has_value());  // not submitted while fetching
    BOOST_TEST(exec->protected_inputs() == std::vector<std::string>{ck}, boost::test_tools::per_element());

    // The fetch completes: into the store, then the job is submitted and runs.
    {
        const fs::path f = tmp.path / "fetched-ck";
        std::ofstream(f) << "checkpoint-bytes\n";
        auto m = store->ingest(f, "application/octet-stream", "", true);
        BOOST_REQUIRE(m.has_value());
        auto cb = std::move(fetcher->hung[0].second);
        fetcher->hung.clear();
        cb(*m);
    }
    BOOST_REQUIRE(run_until([&] { return has_terminal_update("job-rc"); }));
    BOOST_TEST(last_update("job-rc")->state() == tv1::COMPLETED);
    const auto st = mgr->status("job-rc.a1");
    BOOST_REQUIRE(st->output_artifacts_size() == 1);
    std::ifstream in(*store->path_of(st->output_artifacts(0)));
    std::stringstream ss;
    ss << in.rdbuf();
    BOOST_TEST(ss.str() == "checkpoint-bytes\n");
    BOOST_TEST(exec->protected_inputs().empty());
}

BOOST_FIXTURE_TEST_CASE(resume_checkpoint_without_fetcher_fails_retryable, Fixture) {
    make();
    auto l = make_lease("job-nf", {"/bin/true"});
    l.mutable_spec()->set_resume_checkpoint("artifact://sha256/" + std::string(64, 'c'));
    lease_once({l});
    exec->on_registered("ff-node-1");
    BOOST_REQUIRE(run_until([&] { return has_terminal_update("job-nf"); }));
    const auto* u = last_update("job-nf");
    BOOST_TEST(u->state() == tv1::FAILED);
    BOOST_TEST(u->retryable());
    BOOST_TEST(u->error() == "artifact transfer unavailable");
}

BOOST_FIXTURE_TEST_CASE(version2_state_file_is_read, Fixture) {
    fs::create_directories(ropts.state_file.parent_path());
    std::ofstream(ropts.state_file) << R"({"version":2,"leases":[{"local_job_id":"v2.a1","job_id":"v2",
        "attempt_id":"att-v2","fencing_token":1,"attempt_number":1,"terminal":true,"submitted":true}],
        "outbox":[{"job_id":"v2","local_job_id":"v2.a1","terminal":true,
        "update":{"attempt_id":"att-v2","fencing_token":"1","state":"COMPLETED"}}]})";
    make();
    BOOST_TEST(exec->stats().pending_updates == 1u);
    exec->on_registered("ff-node-1");
    BOOST_REQUIRE(run_until([&] { return has_terminal_update("v2"); }));
    BOOST_TEST(last_update("v2")->state() == tv1::COMPLETED);
    BOOST_TEST(last_update("v2")->spawn_size() == 0);
}

BOOST_AUTO_TEST_SUITE_END()
