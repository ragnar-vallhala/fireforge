#include <sys/stat.h>
#include <unistd.h>

#include <boost/test/unit_test.hpp>

#include "fakes.hpp"
#include "tdr/jobs/jobs.hpp"

using namespace tdr::jobs;
using namespace tdr::jobs::testing;
using tdr::supervisor::ExitKind;

namespace {

constexpr std::uint64_t kGiB = 1ull << 30;

struct Fixture {
    TempDir tmp;
    boost::asio::io_context io;
    FakeJournal journal;
    FakeStore store{tmp.path / "store"};
    FakeSupervisor sup{io};
    std::unique_ptr<JobManager> mgr;
    std::vector<std::pair<std::string, v1::JobState>> events;
    std::vector<std::pair<std::string, std::string>> cancel_notes;
    std::optional<tdr::supervisor::RunAs> run_as;  // applied by make()

    explicit Fixture(Capacity cap = {4, 4 * kGiB, 2}, bool keep = false) { make(cap, keep); }

    void make(Capacity cap, bool keep = false) {
        ManagerOptions o;
        o.node_name = "test";
        o.capacity = cap;
        o.default_job_memory_bytes = kGiB;
        o.work_root = tmp.path / "work";
        o.worker_socket = tmp.path / "worker.sock";
        o.cancel_grace = std::chrono::seconds(3);
        o.keep_work_dirs = keep;
        o.run_as = run_as;
        mgr = std::make_unique<JobManager>(io, journal, store, sup, o);
        mgr->set_state_listener([this](const std::string& id, v1::JobState s) { events.emplace_back(id, s); });
        mgr->set_cancel_notifier(
            [this](const std::string& att, const std::string& why) { cancel_notes.emplace_back(att, why); });
    }

    void pump() {
        io.restart();
        io.poll();
    }

    static v1::JobSpec spec(std::uint32_t attempts = 1, std::uint32_t cpus = 1, std::uint64_t mem = 0) {
        v1::JobSpec s;
        s.mutable_command()->add_argv("/bin/true");
        s.mutable_policy()->set_max_attempts(attempts);
        s.mutable_resources()->set_cpu_cores(cpus);
        s.mutable_resources()->set_memory_bytes(mem);
        return s;
    }

    std::string submit(v1::JobSpec s) {
        auto r = mgr->submit(std::move(s));
        BOOST_REQUIRE_MESSAGE(r.has_value(), (r ? "" : r.error()));
        pump();
        return *r;
    }

    v1::JobState state(const std::string& id) { return journal.jobs.at(id).state; }
    const tdr::supervisor::LaunchSpec& launch(std::size_t i) { return sup.launches.at(i).spec; }
    fs::path outdir(std::size_t i) { return launch(i).working_dir / "outputs"; }

    std::vector<v1::JobState> states_of(const std::string& id) {
        std::vector<v1::JobState> out;
        for (auto& [j, s] : events) {
            if (j == id) out.push_back(s);
        }
        return out;
    }
};

v1::WorkerResult ok_result(const std::string& att) {
    v1::WorkerResult r;
    r.set_attempt_id(att);
    r.set_success(true);
    return r;
}

}  // namespace

BOOST_AUTO_TEST_SUITE(jobs_manager)

// ---- submit -------------------------------------------------------------------

BOOST_FIXTURE_TEST_CASE(submit_validation, Fixture) {
    v1::JobSpec empty;
    BOOST_TEST(!mgr->submit(empty).has_value());

    auto big = spec(1, 5);  // node has 4 cores
    BOOST_TEST(!mgr->submit(big).has_value());
    auto huge = spec(1, 1, 5 * kGiB);
    BOOST_TEST(!mgr->submit(huge).has_value());

    auto missing = spec();
    missing.add_input_artifacts("artifact://sha256/" + std::string(64, 'a'));
    BOOST_TEST(!mgr->submit(missing).has_value());

    auto bad_id = spec();
    bad_id.add_input_artifacts("artifact://sha256/XYZ");
    BOOST_TEST(!mgr->submit(bad_id).has_value());

    auto traversal = spec();
    traversal.set_job_id("../evil");
    BOOST_TEST(!mgr->submit(traversal).has_value());

    BOOST_TEST(journal.jobs.empty());
    BOOST_TEST(sup.launches.empty());

    auto named = spec();
    named.set_job_id("my-job");
    BOOST_TEST(mgr->submit(named).value() == "my-job");
    BOOST_TEST(!mgr->submit(named).has_value());  // duplicate id
}

BOOST_FIXTURE_TEST_CASE(submit_assigns_id_and_queues, Fixture) {
    auto r = mgr->submit(spec());
    BOOST_REQUIRE(r.has_value());
    BOOST_TEST(r->starts_with("job-"));
    BOOST_TEST(state(*r) == v1::QUEUED);  // start happens on the posted pass
    BOOST_TEST(mgr->usage().queued == 1u);
    pump();
    BOOST_TEST(state(*r) == v1::RUNNING);
    BOOST_TEST(journal.jobs.at(*r).spec.job_id() == *r);
}

// ---- admission ------------------------------------------------------------------

BOOST_FIXTURE_TEST_CASE(fifo_job_cap, Fixture) {
    auto a = submit(spec());
    auto b = submit(spec());
    auto c = submit(spec());
    BOOST_TEST(state(a) == v1::RUNNING);
    BOOST_TEST(state(b) == v1::RUNNING);
    BOOST_TEST(state(c) == v1::QUEUED);
    auto u = mgr->usage();
    BOOST_TEST(u.running == 2u);
    BOOST_TEST(u.queued == 1u);
    BOOST_TEST(u.allocated.cpu_cores == 2u);
    BOOST_TEST(u.allocated.memory_bytes == 2 * kGiB);

    sup.exit_code(0, 0);
    pump();
    BOOST_TEST(state(a) == v1::COMPLETED);
    BOOST_TEST(state(c) == v1::RUNNING);
    BOOST_TEST(sup.launches.size() == 3u);
}

BOOST_FIXTURE_TEST_CASE(memory_cap_and_head_of_line, Fixture) {
    make({4, 4 * kGiB, 4});
    auto a = submit(spec(1, 1, 3 * kGiB));
    auto b = submit(spec(1, 1, 2 * kGiB));  // does not fit next to a
    auto c = submit(spec(1, 1, 512 << 20)); // would fit, but FIFO head (b) blocks it
    BOOST_TEST(state(a) == v1::RUNNING);
    BOOST_TEST(state(b) == v1::QUEUED);
    BOOST_TEST(state(c) == v1::QUEUED);
    sup.exit_code(0, 0);
    pump();
    BOOST_TEST(state(b) == v1::RUNNING);
    BOOST_TEST(state(c) == v1::RUNNING);
    BOOST_TEST(launch(1).env.at("FIREFORGE_JOB_ID") == b);
    BOOST_TEST(launch(2).env.at("FIREFORGE_JOB_ID") == c);
}

BOOST_FIXTURE_TEST_CASE(cpu_cap, Fixture) {
    make({4, 16 * kGiB, 8});
    auto a = submit(spec(1, 3));
    auto b = submit(spec(1, 2));
    BOOST_TEST(state(a) == v1::RUNNING);
    BOOST_TEST(state(b) == v1::QUEUED);
}

// ---- launch spec ------------------------------------------------------------------

BOOST_FIXTURE_TEST_CASE(launch_env_and_layout, Fixture) {
    auto in = store.add("input-data");
    auto s = spec(1, 2, 256 << 20);
    s.mutable_command()->mutable_env()->insert({"MY_VAR", "x"});
    s.mutable_command()->mutable_env()->insert({"FIREFORGE_JOB_ID", "spoofed"});
    s.mutable_policy()->set_timeout_seconds(30);
    s.add_input_artifacts(in);
    s.set_operation("demo.op");
    s.set_parameters_json("{\"k\":1}");
    s.set_campaign_id("camp");
    s.set_material_id("mat");
    auto id = submit(s);
    BOOST_REQUIRE_EQUAL(sup.launches.size(), 1u);
    const auto& l = launch(0);
    const auto& rec = journal.jobs.at(id);
    const fs::path dir = tmp.path / "work" / id / rec.current_attempt_id;

    BOOST_TEST(l.attempt_id == rec.current_attempt_id);
    BOOST_TEST(l.attempt_id.starts_with("att-"));
    BOOST_TEST(l.argv == std::vector<std::string>{"/bin/true"});
    BOOST_TEST(l.working_dir == dir);
    BOOST_TEST(l.stdout_path == dir / "stdout.log");
    BOOST_TEST(l.stderr_path == dir / "stderr.log");
    BOOST_TEST(l.limits.cpu_cores == 2u);
    BOOST_TEST(l.limits.memory_bytes == (256u << 20));
    BOOST_TEST(l.timeout.count() == 30);
    BOOST_TEST(fs::is_directory(dir / "inputs"));
    BOOST_TEST(fs::is_directory(dir / "outputs"));
    BOOST_TEST(fs::is_directory(dir / "checkpoints"));
    const std::string hex = in.substr(std::string("artifact://sha256/").size());
    BOOST_TEST(read_file(dir / "inputs" / hex) == "input-data");

    const auto& att = journal.attempts.at(l.attempt_id);
    BOOST_TEST(l.env.at("MY_VAR") == "x");
    BOOST_TEST(l.env.at("FIREFORGE_IPC_SOCKET") == (tmp.path / "worker.sock").string());
    BOOST_TEST(l.env.at("FIREFORGE_JOB_ID") == id);
    BOOST_TEST(l.env.at("FIREFORGE_ATTEMPT_ID") == l.attempt_id);
    BOOST_TEST(l.env.at("FIREFORGE_ATTEMPT_TOKEN") == att.token);
    BOOST_TEST(att.token.size() == 64u);
    BOOST_TEST(l.env.at("FIREFORGE_WORKDIR") == dir.string());
    BOOST_TEST(l.env.at("FIREFORGE_INPUT_DIR") == (dir / "inputs").string());
    BOOST_TEST(l.env.at("FIREFORGE_OUTPUT_DIR") == (dir / "outputs").string());
    BOOST_TEST(l.env.at("FIREFORGE_CHECKPOINT_DIR") == (dir / "checkpoints").string());

    BOOST_TEST(att.number == 1u);
    BOOST_TEST(att.pid == 1000);
    BOOST_TEST(att.work_dir == dir.string());
    BOOST_TEST(rec.attempts_used == 1u);

    // Worker handshake returns the task.
    v1::WorkerHello h;
    h.set_attempt_id(l.attempt_id);
    h.set_token(att.token);
    auto task = mgr->on_worker_hello(h);
    BOOST_REQUIRE(task.has_value());
    BOOST_TEST(task->job_id() == id);
    BOOST_TEST(task->attempt_id() == l.attempt_id);
    BOOST_TEST(task->attempt_number() == 1u);
    BOOST_TEST(task->operation() == "demo.op");
    BOOST_TEST(task->parameters_json() == "{\"k\":1}");
    BOOST_TEST(task->campaign_id() == "camp");
    BOOST_TEST(task->material_id() == "mat");
    BOOST_TEST(task->output_dir() == (dir / "outputs").string());
    BOOST_TEST(task->checkpoint_dir() == (dir / "checkpoints").string());
    BOOST_REQUIRE_EQUAL(task->inputs_size(), 1);
    BOOST_TEST(task->inputs(0).artifact_id() == in);
    BOOST_TEST(task->inputs(0).path() == (dir / "inputs" / hex).string());
}

// ---- success / commit -------------------------------------------------------------

BOOST_FIXTURE_TEST_CASE(happy_path_outputs_from_dir, Fixture) {
    auto id = submit(spec());
    const fs::path dir = launch(0).working_dir;
    write_file(outdir(0) / "a.txt", "alpha");
    write_file(outdir(0) / "sub" / "b.txt", "beta");
    fs::create_symlink("/etc/passwd", outdir(0) / "link");  // never published
    sup.exit_code(0, 0);
    pump();

    const auto& rec = journal.jobs.at(id);
    BOOST_TEST(rec.state == v1::COMPLETED);
    BOOST_REQUIRE_EQUAL(rec.output_artifacts.size(), 2u);
    BOOST_TEST(rec.output_artifacts[0] == "artifact://sha256/" + fake_digest("alpha"));
    BOOST_TEST(rec.output_artifacts[1] == "artifact://sha256/" + fake_digest("beta"));
    BOOST_TEST(journal.artifacts.size() == 2u);
    BOOST_TEST(journal.artifacts.at(rec.output_artifacts[0]).producer_job == id);
    for (const auto& ing : store.ingests) BOOST_TEST(ing.move_source);
    BOOST_TEST(store.ingests.size() == 2u);
    BOOST_TEST(journal.attempts.at(rec.current_attempt_id).state == AttemptState::kSucceeded);
    BOOST_TEST(!fs::exists(dir));  // removed after success
    BOOST_TEST(mgr->usage().running == 0u);
    BOOST_TEST(mgr->usage().allocated.cpu_cores == 0u);

    auto st = mgr->status(id);
    BOOST_REQUIRE(st.has_value());
    BOOST_TEST(st->state() == v1::COMPLETED);
    BOOST_TEST(st->output_artifacts_size() == 2);
    BOOST_TEST(mgr->list(v1::COMPLETED, 10).size() == 1u);
    BOOST_TEST(mgr->list(v1::FAILED, 10).empty());
    BOOST_TEST(!mgr->status("nope").has_value());
}

// CREATED is emitted: it is the job's first durable state, and the listener
// is documented to fire after every state change.
BOOST_FIXTURE_TEST_CASE(state_listener_sequence, Fixture) {
    auto id = submit(spec());
    sup.exit_code(0, 0);
    pump();
    std::vector<v1::JobState> want{v1::CREATED, v1::QUEUED, v1::ASSIGNED, v1::RUNNING, v1::COMMITTING,
                                   v1::COMPLETED};
    BOOST_TEST(states_of(id) == want, boost::test_tools::per_element());
}

BOOST_FIXTURE_TEST_CASE(keep_work_dirs, Fixture) {
    make({4, 4 * kGiB, 2}, /*keep=*/true);
    submit(spec());
    const fs::path dir = launch(0).working_dir;
    sup.exit_code(0, 0);
    pump();
    BOOST_TEST(fs::exists(dir));
}

BOOST_FIXTURE_TEST_CASE(declared_outputs, Fixture) {
    auto id = submit(spec());
    const auto att = launch(0).attempt_id;
    write_file(outdir(0) / "result.json", "{}");
    write_file(outdir(0) / "scratch.tmp", "ignored");
    v1::WorkerResult r = ok_result(att);
    auto* o = r.add_outputs();
    o->set_path("result.json");
    o->set_media_type("application/json");
    BOOST_TEST(mgr->on_worker_result(r).has_value());
    BOOST_TEST(!mgr->on_worker_result(r).has_value());  // only one result
    sup.exit_code(0, 0);
    pump();
    const auto& rec = journal.jobs.at(id);
    BOOST_TEST(rec.state == v1::COMPLETED);
    BOOST_REQUIRE_EQUAL(rec.output_artifacts.size(), 1u);
    BOOST_TEST(store.ingests.at(0).media_type == "application/json");
    BOOST_TEST(journal.artifacts.at(rec.output_artifacts[0]).media_type == "application/json");
}

BOOST_FIXTURE_TEST_CASE(declared_output_validation_failures, Fixture) {
    make({4, 4 * kGiB, 8});
    write_file(tmp.path / "secret", "outside");
    struct Case {
        std::string path;
        std::function<void(const fs::path&)> setup;
    };
    std::vector<Case> cases = {
        {"../../escape.txt", [](const fs::path&) {}},
        {"/etc/passwd", [](const fs::path&) {}},
        {"missing.txt", [](const fs::path&) {}},
        {"link", [this](const fs::path& out) { fs::create_symlink(tmp.path / "secret", out / "link"); }},
        {"subdir", [](const fs::path& out) { fs::create_directories(out / "subdir"); }},
        {"", [](const fs::path&) {}},
    };
    for (std::size_t i = 0; i < cases.size(); ++i) {
        auto id = submit(spec());
        cases[i].setup(outdir(i));
        v1::WorkerResult r = ok_result(launch(i).attempt_id);
        r.add_outputs()->set_path(cases[i].path);
        BOOST_REQUIRE(mgr->on_worker_result(r).has_value());
        sup.exit_code(i, 0);
        pump();
        BOOST_TEST(state(id) == v1::VALIDATION_FAILED, "path '" << cases[i].path << "'");
        BOOST_TEST(!journal.jobs.at(id).last_error.empty());
        BOOST_TEST(fs::exists(launch(i).working_dir));  // kept for inspection
        BOOST_TEST(journal.attempts.at(launch(i).attempt_id).state == AttemptState::kFailed);
    }
    BOOST_TEST(store.ingests.empty());
    BOOST_TEST(read_file(tmp.path / "secret") == "outside");
}

BOOST_FIXTURE_TEST_CASE(ingest_failure_retries, Fixture) {
    auto id = submit(spec(2));
    write_file(outdir(0) / "a", "x");
    store.fail_ingest = "disk full";
    sup.exit_code(0, 0);
    pump();
    BOOST_TEST(state(id) == v1::RUNNING);
    BOOST_TEST(sup.launches.size() == 2u);
    BOOST_TEST(journal.jobs.at(id).last_error.find("disk full") != std::string::npos);
}

// ---- failures / retries ------------------------------------------------------------

BOOST_FIXTURE_TEST_CASE(exit_code_retried_then_failed, Fixture) {
    auto id = submit(spec(3));
    for (std::size_t i = 0; i < 3; ++i) {
        BOOST_REQUIRE_EQUAL(sup.launches.size(), i + 1);
        BOOST_TEST(state(id) == v1::RUNNING);
        sup.exit_code(i, 1);
        pump();
    }
    BOOST_TEST(state(id) == v1::FAILED);
    BOOST_TEST(sup.launches.size() == 3u);
    const auto& rec = journal.jobs.at(id);
    BOOST_TEST(rec.attempts_used == 3u);
    BOOST_TEST(rec.last_error.find("code 1") != std::string::npos);
    BOOST_TEST(journal.attempts.at(launch(0).attempt_id).state == AttemptState::kFailed);
    BOOST_TEST(journal.attempts.at(launch(0).attempt_id).exit_code == 1);
    BOOST_TEST(fs::exists(launch(0).working_dir));  // failed attempts keep logs
    BOOST_TEST(mgr->usage().allocated.jobs == 0u);

    auto seq = states_of(id);
    std::vector<v1::JobState> want{v1::CREATED,  v1::QUEUED,   v1::ASSIGNED,      v1::RUNNING,
                                   v1::RETRY_PENDING, v1::QUEUED, v1::ASSIGNED, v1::RUNNING,
                                   v1::RETRY_PENDING, v1::QUEUED, v1::ASSIGNED, v1::RUNNING,
                                   v1::FAILED};
    BOOST_TEST(seq == want, boost::test_tools::per_element());
}

BOOST_FIXTURE_TEST_CASE(signal_and_oom_are_retryable, Fixture) {
    auto id = submit(spec(3));
    sup.exit_kind(0, ExitKind::kSignaled);
    pump();
    BOOST_TEST(state(id) == v1::RUNNING);
    sup.exit_kind(1, ExitKind::kOomKilled);
    pump();
    BOOST_TEST(state(id) == v1::RUNNING);
    BOOST_TEST(journal.jobs.at(id).last_error.find("memory") != std::string::npos);
}

BOOST_FIXTURE_TEST_CASE(worker_failure_not_retryable, Fixture) {
    auto id = submit(spec(3));
    v1::WorkerResult r;
    r.set_attempt_id(launch(0).attempt_id);
    r.set_success(false);
    r.set_retryable(false);
    r.set_error("bad input");
    BOOST_REQUIRE(mgr->on_worker_result(r).has_value());
    sup.exit_code(0, 0);
    pump();
    BOOST_TEST(state(id) == v1::FAILED);
    BOOST_TEST(journal.jobs.at(id).last_error == "bad input");
    BOOST_TEST(sup.launches.size() == 1u);
}

BOOST_FIXTURE_TEST_CASE(worker_failure_retryable, Fixture) {
    auto id = submit(spec(2));
    v1::WorkerResult r;
    r.set_attempt_id(launch(0).attempt_id);
    r.set_success(false);
    r.set_retryable(true);
    BOOST_REQUIRE(mgr->on_worker_result(r).has_value());
    sup.exit_code(0, 0);
    pump();
    BOOST_TEST(state(id) == v1::RUNNING);
    BOOST_TEST(sup.launches.size() == 2u);
    // The second attempt's task carries attempt_number 2.
    v1::WorkerHello h;
    h.set_attempt_id(launch(1).attempt_id);
    h.set_token(launch(1).env.at("FIREFORGE_ATTEMPT_TOKEN"));
    BOOST_TEST(mgr->on_worker_hello(h).value().attempt_number() == 2u);
}

BOOST_FIXTURE_TEST_CASE(timeout_retry_then_expired, Fixture) {
    auto s = spec(2);
    s.mutable_policy()->set_timeout_seconds(5);
    auto id = submit(s);
    sup.exit_kind(0, ExitKind::kTimedOut);
    pump();
    BOOST_TEST(state(id) == v1::RUNNING);
    sup.exit_kind(1, ExitKind::kTimedOut);
    pump();
    BOOST_TEST(state(id) == v1::EXPIRED);
    BOOST_TEST(journal.jobs.at(id).last_error.find("timed out") != std::string::npos);
}

BOOST_FIXTURE_TEST_CASE(launch_failure, Fixture) {
    sup.fail_launch = "execvp: No such file or directory";
    auto id = submit(spec(2));
    BOOST_TEST(state(id) == v1::FAILED);
    const auto& rec = journal.jobs.at(id);
    BOOST_TEST(rec.attempts_used == 2u);
    BOOST_TEST(rec.last_error.find("execvp") != std::string::npos);
    BOOST_TEST(journal.attempts.size() == 2u);
    for (auto& [aid, a] : journal.attempts) BOOST_TEST(a.state == AttemptState::kFailed);
    BOOST_TEST(mgr->usage().allocated.jobs == 0u);
    BOOST_TEST(mgr->usage().queued == 0u);

    // Recovers once launches work again.
    sup.fail_launch.reset();
    auto id2 = submit(spec());
    BOOST_TEST(state(id2) == v1::RUNNING);
}

BOOST_FIXTURE_TEST_CASE(launch_failure_then_success, Fixture) {
    sup.fail_launch = "transient";
    auto r = mgr->submit(spec(3));
    BOOST_REQUIRE(r.has_value());
    // Fail exactly once: clear the failure from inside the listener after the
    // first RETRY_PENDING.
    mgr->set_state_listener([&](const std::string&, v1::JobState s) {
        if (s == v1::RETRY_PENDING) sup.fail_launch.reset();
    });
    pump();
    BOOST_TEST(state(*r) == v1::RUNNING);
    BOOST_TEST(journal.jobs.at(*r).attempts_used == 2u);
}

// ---- cancel ----------------------------------------------------------------------

BOOST_FIXTURE_TEST_CASE(cancel_queued, Fixture) {
    make({4, 4 * kGiB, 1});
    auto a = submit(spec());
    auto b = submit(spec());
    BOOST_TEST(state(b) == v1::QUEUED);
    BOOST_TEST(mgr->cancel(b).has_value());
    BOOST_TEST(state(b) == v1::CANCELLED);
    BOOST_TEST(mgr->usage().queued == 0u);
    sup.exit_code(0, 0);
    pump();
    BOOST_TEST(state(a) == v1::COMPLETED);
    BOOST_TEST(sup.launches.size() == 1u);  // b never started
    BOOST_TEST(!mgr->cancel(b).has_value());  // terminal
    BOOST_TEST(!mgr->cancel(a).has_value());
    BOOST_TEST(!mgr->cancel("unknown").has_value());
}

BOOST_FIXTURE_TEST_CASE(cancel_running, Fixture) {
    auto id = submit(spec(3));
    const auto att = launch(0).attempt_id;
    BOOST_TEST(mgr->cancel(id).has_value());
    BOOST_TEST(state(id) == v1::RUNNING);  // until the process exits
    BOOST_REQUIRE_EQUAL(sup.cancels.size(), 1u);
    BOOST_TEST(sup.cancels[0].first == att);
    BOOST_TEST(sup.cancels[0].second.count() == 3);
    BOOST_REQUIRE_EQUAL(cancel_notes.size(), 1u);
    BOOST_TEST(cancel_notes[0].first == att);
    BOOST_TEST(mgr->cancel(id).has_value());  // idempotent while pending
    BOOST_TEST(sup.cancels.size() == 1u);

    // A worker connecting now is turned away.
    v1::WorkerHello h;
    h.set_attempt_id(att);
    h.set_token(launch(0).env.at("FIREFORGE_ATTEMPT_TOKEN"));
    BOOST_TEST(!mgr->on_worker_hello(h).has_value());

    sup.exit_kind(0, ExitKind::kCancelled);
    pump();
    BOOST_TEST(state(id) == v1::CANCELLED);
    BOOST_TEST(journal.attempts.at(att).state == AttemptState::kCancelled);
    BOOST_TEST(sup.launches.size() == 1u);  // no retry
    BOOST_TEST(!mgr->cancel(id).has_value());
}

BOOST_FIXTURE_TEST_CASE(cancel_running_process_exits_zero, Fixture) {
    auto id = submit(spec());
    BOOST_TEST(mgr->cancel(id).has_value());
    write_file(outdir(0) / "x", "y");
    sup.exit_code(0, 0);  // raced: exited cleanly before the signal
    pump();
    BOOST_TEST(state(id) == v1::CANCELLED);
    BOOST_TEST(store.ingests.empty());
}

BOOST_FIXTURE_TEST_CASE(cancel_during_assigned, Fixture) {
    auto r = mgr->submit(spec());
    BOOST_REQUIRE(r.has_value());
    mgr->set_state_listener([&](const std::string& id, v1::JobState s) {
        events.emplace_back(id, s);
        if (s == v1::ASSIGNED) BOOST_TEST(mgr->cancel(id).has_value());
    });
    pump();
    BOOST_TEST(state(*r) == v1::CANCELLED);
    BOOST_TEST(sup.launches.empty());
    BOOST_TEST(journal.attempts.at(journal.jobs.at(*r).current_attempt_id).state == AttemptState::kCancelled);
    BOOST_TEST(mgr->usage().allocated.jobs == 0u);
}

BOOST_FIXTURE_TEST_CASE(cancel_immediately_after_launch, Fixture) {
    auto r = mgr->submit(spec());
    BOOST_REQUIRE(r.has_value());
    mgr->set_state_listener([&](const std::string& id, v1::JobState s) {
        if (s == v1::RUNNING) BOOST_TEST(mgr->cancel(id).has_value());
    });
    pump();
    BOOST_REQUIRE_EQUAL(sup.launches.size(), 1u);
    BOOST_REQUIRE_EQUAL(sup.cancels.size(), 1u);
    BOOST_TEST(sup.cancels[0].first == launch(0).attempt_id);
    sup.exit_kind(0, ExitKind::kCancelled);
    pump();
    BOOST_TEST(state(*r) == v1::CANCELLED);
}

// ---- fencing ------------------------------------------------------------------------

BOOST_FIXTURE_TEST_CASE(stale_attempt_cannot_commit, Fixture) {
    auto id = submit(spec());
    const auto att = launch(0).attempt_id;
    write_file(outdir(0) / "out", "data");
    // Another owner takes over the job between RUNNING -> COMMITTING and
    // commit_outputs (simulated from the COMMITTING listener).
    mgr->set_state_listener([&](const std::string& jid, v1::JobState s) {
        if (s == v1::COMMITTING) journal.jobs.at(jid).current_attempt_id = "att-someone-else";
    });
    sup.exit_code(0, 0);
    pump();
    const auto& rec = journal.jobs.at(id);
    BOOST_TEST(rec.state == v1::COMMITTING);
    BOOST_TEST(rec.output_artifacts.empty());
    BOOST_TEST(store.ingests.size() == 1u);  // stays in the store, unreferenced
    BOOST_TEST(journal.attempts.at(att).state == AttemptState::kAbandoned);
}

BOOST_FIXTURE_TEST_CASE(exit_of_non_current_attempt_ignored, Fixture) {
    auto id = submit(spec());
    journal.jobs.at(id).current_attempt_id = "att-other";
    sup.exit_code(0, 0);
    pump();
    BOOST_TEST(state(id) == v1::RUNNING);
    BOOST_TEST(journal.attempts.at(launch(0).attempt_id).state == AttemptState::kAbandoned);
}

// ---- worker hooks -------------------------------------------------------------------

BOOST_FIXTURE_TEST_CASE(worker_hello_rejections, Fixture) {
    submit(spec());
    const auto att = launch(0).attempt_id;
    const auto token = launch(0).env.at("FIREFORGE_ATTEMPT_TOKEN");
    v1::WorkerHello h;
    h.set_attempt_id(att);
    h.set_token(std::string(64, '0'));
    BOOST_TEST(!mgr->on_worker_hello(h).has_value());
    h.set_token("");
    BOOST_TEST(!mgr->on_worker_hello(h).has_value());
    h.set_attempt_id("att-unknown");
    h.set_token(token);
    BOOST_TEST(!mgr->on_worker_hello(h).has_value());
    h.set_attempt_id(att);
    BOOST_TEST(mgr->on_worker_hello(h).has_value());

    sup.exit_code(0, 0);
    pump();
    BOOST_TEST(!mgr->on_worker_hello(h).has_value());  // finished attempt
    BOOST_TEST(!mgr->on_worker_result(ok_result(att)).has_value());
    BOOST_TEST(!mgr->on_worker_result(ok_result("att-unknown")).has_value());
}

BOOST_FIXTURE_TEST_CASE(progress_stored, Fixture) {
    auto id = submit(spec());
    v1::WorkerProgress p;
    p.set_attempt_id(launch(0).attempt_id);
    p.set_fraction(0.25);
    p.set_message("scf step 3");
    mgr->on_worker_progress(p);
    auto st = mgr->status(id);
    BOOST_TEST(st->progress_fraction() == 0.25);
    BOOST_TEST(st->progress_message() == "scf step 3");
    p.set_fraction(7.0);
    mgr->on_worker_progress(p);
    BOOST_TEST(journal.jobs.at(id).progress_fraction == 1.0);
    p.set_attempt_id("att-unknown");
    p.set_fraction(0.1);
    mgr->on_worker_progress(p);  // ignored
    BOOST_TEST(journal.jobs.at(id).progress_fraction == 1.0);
}

// ---- recovery ------------------------------------------------------------------------

namespace {
JobRecord job_rec(const std::string& id, v1::JobState s, std::uint32_t used, std::uint32_t max,
                  std::int64_t created) {
    JobRecord r;
    r.job_id = id;
    r.state = s;
    r.attempts_used = used;
    r.created_unix_ms = created;
    r.spec.set_job_id(id);
    r.spec.mutable_command()->add_argv("/bin/true");
    r.spec.mutable_policy()->set_max_attempts(max);
    return r;
}
AttemptRecord att_rec(const std::string& id, const std::string& job, AttemptState s, int pid) {
    AttemptRecord a;
    a.attempt_id = id;
    a.job_id = job;
    a.state = s;
    a.pid = pid;
    a.pid_start_ticks = 777;
    a.number = 1;
    return a;
}
}  // namespace

BOOST_FIXTURE_TEST_CASE(recover_after_crash, Fixture) {
    make({8, 16 * kGiB, 8});
    // Running job with attempts left -> requeued and restarted.
    auto r1 = job_rec("j-run-retry", v1::RUNNING, 1, 3, 100);
    r1.current_attempt_id = "att-1";
    journal.put(r1);
    journal.attempts["att-1"] = att_rec("att-1", r1.job_id, AttemptState::kRunning, 4242);
    // Running job with no attempts left -> FAILED.
    auto r2 = job_rec("j-run-done", v1::RUNNING, 1, 1, 200);
    r2.current_attempt_id = "att-2";
    journal.put(r2);
    journal.attempts["att-2"] = att_rec("att-2", r2.job_id, AttemptState::kRunning, 4343);
    // ASSIGNED with a kStarting attempt that never got a pid.
    auto r3 = job_rec("j-assigned", v1::ASSIGNED, 1, 2, 300);
    r3.current_attempt_id = "att-3";
    journal.put(r3);
    journal.attempts["att-3"] = att_rec("att-3", r3.job_id, AttemptState::kStarting, 0);
    // COMMITTING without an open attempt (crash after attempt finished).
    auto r4 = job_rec("j-commit", v1::COMMITTING, 1, 2, 400);
    journal.put(r4);
    // Plain queued / retry-pending / created jobs.
    journal.put(job_rec("j-queued", v1::QUEUED, 0, 1, 50));  // oldest
    journal.put(job_rec("j-retry", v1::RETRY_PENDING, 1, 2, 500));
    journal.put(job_rec("j-created", v1::CREATED, 0, 1, 600));
    journal.put(job_rec("j-done", v1::COMPLETED, 1, 1, 10));

    mgr->recover();
    BOOST_REQUIRE_EQUAL(sup.orphans_killed.size(), 2u);  // pid 0 is skipped
    std::set<int> pids;
    for (auto& h : sup.orphans_killed) {
        pids.insert(h.pid);
        BOOST_TEST(h.start_ticks == 777u);
    }
    BOOST_TEST((pids == std::set<int>{4242, 4343}));
    for (auto id : {"att-1", "att-2", "att-3"}) {
        BOOST_TEST(journal.attempts.at(id).state == AttemptState::kAbandoned, id);
    }
    BOOST_TEST(state("j-run-done") == v1::FAILED);

    pump();
    for (auto id : {"j-run-retry", "j-assigned", "j-commit", "j-queued", "j-retry", "j-created"}) {
        BOOST_TEST(state(id) == v1::RUNNING, id);
    }
    BOOST_TEST(state("j-done") == v1::COMPLETED);
    // Started in creation order.
    BOOST_REQUIRE_EQUAL(sup.launches.size(), 6u);
    std::vector<std::string> order;
    for (auto& l : sup.launches) order.push_back(l.spec.env.at("FIREFORGE_JOB_ID"));
    std::vector<std::string> want{"j-queued", "j-run-retry", "j-assigned", "j-commit", "j-retry", "j-created"};
    BOOST_TEST(order == want, boost::test_tools::per_element());
    BOOST_TEST(journal.jobs.at("j-run-retry").attempts_used == 2u);
    BOOST_TEST(journal.jobs.at("j-run-retry").last_error.find("abandoned") != std::string::npos);
}

BOOST_FIXTURE_TEST_CASE(recover_empty, Fixture) {
    mgr->recover();
    pump();
    BOOST_TEST(sup.launches.empty());
    BOOST_TEST(mgr->usage().queued == 0u);
}

// ---- lifetime -------------------------------------------------------------------------

BOOST_FIXTURE_TEST_CASE(destroyed_manager_ignores_late_callbacks, Fixture) {
    submit(spec());
    auto r = mgr->submit(spec());  // schedule pass pending
    BOOST_REQUIRE(r.has_value());
    sup.exit_code(0, 0);
    mgr.reset();
    pump();  // must not touch the destroyed manager
    BOOST_TEST(sup.launches.size() == 1u);
}

// ---- restricted job user (section 11) ----------------------------------------------

namespace {

struct DirInfo {
    unsigned mode;
    gid_t gid;
};

DirInfo dir_info(const fs::path& p) {
    struct stat st{};
    BOOST_REQUIRE_MESSAGE(::stat(p.c_str(), &st) == 0, "stat " + p.string());
    return {static_cast<unsigned>(st.st_mode & 07777), st.st_gid};
}

// Our own primary group: chown to it needs no privileges.
tdr::supervisor::RunAs self_run_as() { return {::getuid(), ::getgid(), {::getgid()}}; }

}  // namespace

BOOST_FIXTURE_TEST_CASE(run_as_dirs_env_and_copy_ingest, Fixture) {
    run_as = self_run_as();
    make({4, 4 * kGiB, 2});
    const std::string in = store.add("input-data");
    auto s = spec();
    s.add_input_artifacts(in);
    (*s.mutable_command()->mutable_env())["FIREFORGE_JOB_ID"] = "spoofed";
    auto id = submit(s);
    BOOST_REQUIRE_EQUAL(sup.launches.size(), 1u);
    const auto& ls = launch(0);
    const fs::path dir = ls.working_dir;

    BOOST_REQUIRE(ls.run_as.has_value());
    BOOST_TEST(ls.run_as->uid == ::getuid());
    BOOST_TEST(ls.run_as->gid == ::getgid());
    BOOST_TEST((ls.run_as->groups == std::vector<gid_t>{::getgid()}));
    BOOST_TEST(ls.env.at("HOME") == dir.string());
    BOOST_TEST(ls.env.at("FIREFORGE_JOB_ID") == id);  // daemon-set FIREFORGE_* win

    const std::pair<fs::path, unsigned> expected[] = {
        {dir.parent_path(), 02750u}, {dir, 02770u}, {dir / "outputs", 02770u},
        {dir / "checkpoints", 02770u}, {dir / "inputs", 02750u},
    };
    for (const auto& [p, mode] : expected) {
        auto info = dir_info(p);
        BOOST_TEST(info.mode == mode, p.string() << std::oct << " mode " << info.mode << " != " << mode);
        BOOST_TEST(info.gid == ::getgid());
    }
    // Inputs were materialized before inputs/ was made read-only for the group.
    BOOST_TEST(read_file(dir / "inputs" / in.substr(18)) == "input-data");

    write_file(outdir(0) / "a.txt", "alpha");
    sup.exit_code(0, 0);
    pump();
    BOOST_TEST(state(id) == v1::COMPLETED);
    BOOST_REQUIRE_EQUAL(store.ingests.size(), 1u);
    BOOST_TEST(!store.ingests[0].move_source);
    BOOST_TEST(!fs::exists(dir));  // cleanup still works
}

BOOST_FIXTURE_TEST_CASE(run_as_job_env_may_override_home, Fixture) {
    run_as = self_run_as();
    make({4, 4 * kGiB, 2});
    auto s = spec();
    (*s.mutable_command()->mutable_env())["HOME"] = "/custom/home";
    submit(s);
    BOOST_TEST(launch(0).env.at("HOME") == "/custom/home");
}

BOOST_FIXTURE_TEST_CASE(no_run_as_unchanged, Fixture) {
    auto id = submit(spec());
    const auto& ls = launch(0);
    BOOST_TEST(!ls.run_as.has_value());
    BOOST_TEST(!ls.env.contains("HOME"));
    for (const auto& p : {ls.working_dir.parent_path(), ls.working_dir, ls.working_dir / "inputs",
                          ls.working_dir / "outputs", ls.working_dir / "checkpoints"}) {
        BOOST_TEST((dir_info(p).mode & 02000u) == 0u, p.string() << " has setgid");
    }
    write_file(outdir(0) / "a.txt", "alpha");
    sup.exit_code(0, 0);
    pump();
    BOOST_TEST(state(id) == v1::COMPLETED);
    BOOST_REQUIRE_EQUAL(store.ingests.size(), 1u);
    BOOST_TEST(store.ingests[0].move_source);
}

BOOST_FIXTURE_TEST_CASE(run_as_foreign_group_is_setup_error, Fixture) {
    if (::geteuid() == 0) {
        BOOST_TEST_MESSAGE("skipped: root may chown to any group");
        return;
    }
    // A gid we are not a member of.
    std::vector<gid_t> mine(static_cast<std::size_t>(::getgroups(0, nullptr)));
    mine.resize(static_cast<std::size_t>(::getgroups(static_cast<int>(mine.size()), mine.data())));
    mine.push_back(::getegid());
    gid_t foreign = 0;
    while (std::ranges::find(mine, foreign) != mine.end()) ++foreign;

    run_as = tdr::supervisor::RunAs{::getuid(), foreign, {foreign}};
    make({4, 4 * kGiB, 2});
    auto r = mgr->submit(spec(2));
    BOOST_REQUIRE(r.has_value());
    std::vector<v1::JobState> seen;
    mgr->set_state_listener([&](const std::string&, v1::JobState st) { seen.push_back(st); });
    pump();

    BOOST_TEST(sup.launches.empty());
    BOOST_TEST(state(*r) == v1::FAILED);
    const auto& rec = journal.jobs.at(*r);
    BOOST_TEST(rec.attempts_used == 2u);
    BOOST_TEST(rec.last_error.find("chown") != std::string::npos, rec.last_error);
    BOOST_TEST(rec.last_error.find("group " + std::to_string(foreign)) != std::string::npos, rec.last_error);
    BOOST_TEST(std::ranges::count(seen, v1::RETRY_PENDING) == 1);
    for (auto& [aid, a] : journal.attempts) BOOST_TEST(a.state == AttemptState::kFailed);
    BOOST_TEST(mgr->usage().allocated.jobs == 0u);
}

// ---- Phase 6: named outputs/inputs, checkpoints, resume, spawn ----------------

namespace {
v1::WorkerHello hello_for(const tdr::supervisor::LaunchSpec& l) {
    v1::WorkerHello h;
    h.set_attempt_id(l.attempt_id);
    h.set_token(l.env.at("FIREFORGE_ATTEMPT_TOKEN"));
    return h;
}
v1::WorkerCheckpoint ckpt(const std::string& att, const std::string& path, const std::string& name = "",
                          const std::string& media = "") {
    v1::WorkerCheckpoint c;
    c.set_attempt_id(att);
    c.set_path(path);
    c.set_name(name);
    c.set_media_type(media);
    return c;
}
v1::JobSpec ckpt_spec(std::uint32_t attempts = 1) {
    auto s = Fixture::spec(attempts);
    s.mutable_policy()->set_checkpoint(true);
    return s;
}
fs::path ckdir(const tdr::supervisor::LaunchSpec& l) { return l.working_dir / "checkpoints"; }
}  // namespace

BOOST_FIXTURE_TEST_CASE(named_outputs_declared, Fixture) {
    auto id = submit(spec());
    const auto att = launch(0).attempt_id;
    write_file(outdir(0) / "result.json", "{}");
    write_file(outdir(0) / "sub" / "b.txt", "beta");
    v1::WorkerResult r = ok_result(att);
    auto* o = r.add_outputs();
    o->set_path("result.json");
    o->set_name("result");
    o->set_media_type("application/json");
    r.add_outputs()->set_path("./sub/b.txt");  // unnamed: normalized relative path
    o = r.add_outputs();                       // same file under a second name
    o->set_path("result.json");
    o->set_name("copy");
    o = r.add_outputs();                       // exact repeat: ignored
    o->set_path("result.json");
    o->set_name("result");
    BOOST_REQUIRE(mgr->on_worker_result(r).has_value());
    sup.exit_code(0, 0);
    pump();
    auto st = mgr->status(id);
    BOOST_REQUIRE(st.has_value());
    BOOST_TEST(st->state() == v1::COMPLETED);
    BOOST_REQUIRE_EQUAL(st->outputs_size(), 3);
    BOOST_TEST(st->outputs(0).name() == "result");
    BOOST_TEST(st->outputs(0).artifact_id() == "artifact://sha256/" + fake_digest("{}"));
    BOOST_TEST(st->outputs(0).size() == 2u);
    BOOST_TEST(st->outputs(0).media_type() == "application/json");
    BOOST_TEST(st->outputs(1).name() == "sub/b.txt");
    BOOST_TEST(st->outputs(1).size() == 4u);
    BOOST_TEST(st->outputs(1).media_type() == "application/octet-stream");
    BOOST_TEST(st->outputs(2).name() == "copy");
    BOOST_TEST(st->outputs(2).artifact_id() == st->outputs(0).artifact_id());
    // output_artifacts: distinct ids, same order.
    BOOST_REQUIRE_EQUAL(st->output_artifacts_size(), 2);
    BOOST_TEST(st->output_artifacts(0) == st->outputs(0).artifact_id());
    BOOST_TEST(st->output_artifacts(1) == st->outputs(1).artifact_id());
    BOOST_TEST(store.ingests.size() == 2u);  // the shared file is ingested once
}

BOOST_FIXTURE_TEST_CASE(named_outputs_scanned, Fixture) {
    auto id = submit(spec());
    write_file(outdir(0) / "z.txt", "zeta");
    write_file(outdir(0) / "sub" / "b.txt", "beta");
    sup.exit_code(0, 0);
    pump();
    auto st = mgr->status(id);
    BOOST_TEST(st->state() == v1::COMPLETED);
    BOOST_REQUIRE_EQUAL(st->outputs_size(), 2);
    BOOST_TEST(st->outputs(0).name() == "sub/b.txt");
    BOOST_TEST(st->outputs(1).name() == "z.txt");
    BOOST_TEST(st->output_artifacts(0) == st->outputs(0).artifact_id());
    BOOST_TEST(st->output_artifacts(1) == st->outputs(1).artifact_id());
}

BOOST_FIXTURE_TEST_CASE(duplicate_output_name_fails_not_retryable, Fixture) {
    auto id = submit(spec(3));
    write_file(outdir(0) / "a.txt", "alpha");
    write_file(outdir(0) / "b.txt", "beta");
    v1::WorkerResult r = ok_result(launch(0).attempt_id);
    auto* o = r.add_outputs();
    o->set_path("a.txt");
    o->set_name("x");
    o = r.add_outputs();
    o->set_path("b.txt");
    o->set_name("x");
    BOOST_REQUIRE(mgr->on_worker_result(r).has_value());
    sup.exit_code(0, 0);
    pump();
    BOOST_TEST(state(id) == v1::FAILED);
    BOOST_TEST(journal.jobs.at(id).last_error == "duplicate output name: x");
    BOOST_TEST(sup.launches.size() == 1u);  // not retried
    BOOST_TEST(store.ingests.empty());
    BOOST_TEST(journal.attempts.at(launch(0).attempt_id).state == AttemptState::kFailed);

    // A declared name equal to another output's path also collides.
    auto id2 = submit(spec());
    write_file(outdir(1) / "a.txt", "alpha");
    write_file(outdir(1) / "b.txt", "beta");
    v1::WorkerResult r2 = ok_result(launch(1).attempt_id);
    r2.add_outputs()->set_path("a.txt");
    o = r2.add_outputs();
    o->set_path("b.txt");
    o->set_name("a.txt");
    BOOST_REQUIRE(mgr->on_worker_result(r2).has_value());
    sup.exit_code(1, 0);
    pump();
    BOOST_TEST(state(id2) == v1::FAILED);
    BOOST_TEST(journal.jobs.at(id2).last_error == "duplicate output name: a.txt");
}

BOOST_FIXTURE_TEST_CASE(input_names_and_workflow_fields_in_task, Fixture) {
    const auto in1 = store.add("structure-data");
    const auto in2 = store.add("other");
    auto s = spec();
    s.add_input_artifacts(in1);
    s.add_input_artifacts(in2);
    s.add_input_names("structure");  // shorter than input_artifacts: the rest is unnamed
    s.set_workflow_id("wf-0123456789abcdef");
    s.set_step("relax/child-1");

    auto too_many = s;
    too_many.add_input_names("b");
    too_many.add_input_names("c");
    BOOST_TEST(!mgr->submit(too_many).has_value());
    BOOST_TEST(journal.jobs.empty());

    submit(s);
    auto task = mgr->on_worker_hello(hello_for(launch(0)));
    BOOST_REQUIRE(task.has_value());
    BOOST_REQUIRE_EQUAL(task->inputs_size(), 2);
    BOOST_TEST(task->inputs(0).name() == "structure");
    BOOST_TEST(task->inputs(0).artifact_id() == in1);
    BOOST_TEST(read_file(task->inputs(0).path()) == "structure-data");
    BOOST_TEST(task->inputs(1).name().empty());
    BOOST_TEST(task->workflow_id() == "wf-0123456789abcdef");
    BOOST_TEST(task->step() == "relax/child-1");
    BOOST_TEST(!task->checkpoint_enabled());
    BOOST_TEST(task->resume_checkpoint().empty());

    submit(ckpt_spec());
    auto t2 = mgr->on_worker_hello(hello_for(launch(1)));
    BOOST_REQUIRE(t2.has_value());
    BOOST_TEST(t2->checkpoint_enabled());
}

BOOST_FIXTURE_TEST_CASE(checkpoint_accepted_and_listener_fired, Fixture) {
    std::vector<std::pair<std::string, v1::NamedArtifact>> seen;
    mgr->set_checkpoint_listener(
        [&](const std::string& job, const v1::NamedArtifact& a) { seen.emplace_back(job, a); });
    auto id = submit(ckpt_spec());
    const auto att = launch(0).attempt_id;
    write_file(ckdir(launch(0)) / "step-10.bin", "state-10");

    auto r = mgr->on_worker_checkpoint(ckpt(att, "step-10.bin", "", "application/x-state"));
    BOOST_REQUIRE_MESSAGE(r.has_value(), (r ? "" : r.error()));
    BOOST_TEST(r->name() == "checkpoint");
    BOOST_TEST(r->artifact_id() == "artifact://sha256/" + fake_digest("state-10"));
    BOOST_TEST(r->size() == 8u);
    BOOST_TEST(r->media_type() == "application/x-state");
    BOOST_REQUIRE_EQUAL(store.ingests.size(), 1u);
    BOOST_TEST(!store.ingests[0].move_source);  // copied: the worker may keep writing
    BOOST_TEST(store.ingests[0].producer_job == id);
    BOOST_TEST(fs::exists(ckdir(launch(0)) / "step-10.bin"));
    BOOST_TEST(journal.artifacts.at(r->artifact_id()).producer_job == id);
    BOOST_REQUIRE_EQUAL(seen.size(), 1u);
    BOOST_TEST(seen[0].first == id);
    BOOST_TEST(seen[0].second.artifact_id() == r->artifact_id());
    BOOST_TEST(mgr->status(id)->checkpoint().artifact_id() == r->artifact_id());

    // Absolute path inside the dir, custom name; the last one wins.
    write_file(ckdir(launch(0)) / "sub" / "step-20.bin", "state-20");
    auto r2 = mgr->on_worker_checkpoint(ckpt(att, (ckdir(launch(0)) / "sub" / "step-20.bin").string(), "state"));
    BOOST_REQUIRE_MESSAGE(r2.has_value(), (r2 ? "" : r2.error()));
    BOOST_TEST(r2->media_type() == "application/octet-stream");
    auto st = mgr->status(id);
    BOOST_TEST(st->checkpoint().name() == "state");
    BOOST_TEST(st->checkpoint().artifact_id() == "artifact://sha256/" + fake_digest("state-20"));
    BOOST_TEST(seen.size() == 2u);
    // A job without a checkpoint reports none.
    auto other = submit(spec());
    BOOST_TEST(!mgr->status(other)->has_checkpoint());
}

BOOST_FIXTURE_TEST_CASE(checkpoint_rejections, Fixture) {
    make({4, 4 * kGiB, 4});
    std::size_t fired = 0;
    mgr->set_checkpoint_listener([&](const std::string&, const v1::NamedArtifact&) { ++fired; });
    // Not enabled.
    submit(spec());
    write_file(ckdir(launch(0)) / "c.bin", "x");
    auto r = mgr->on_worker_checkpoint(ckpt(launch(0).attempt_id, "c.bin"));
    BOOST_REQUIRE(!r.has_value());
    BOOST_TEST(r.error() == "checkpointing not enabled for this job");

    auto id = submit(ckpt_spec());
    const auto& l = launch(1);
    const auto att = l.attempt_id;
    write_file(tmp.path / "secret", "outside");
    write_file(l.working_dir / "outputs" / "o.bin", "out");
    write_file(ckdir(l) / "ok.bin", "fine");
    fs::create_symlink(tmp.path / "secret", ckdir(l) / "link");
    fs::create_directories(ckdir(l) / "dir");
    for (const std::string& path : {(tmp.path / "secret").string(), std::string("../outputs/o.bin"),
                                    (ckdir(l) / ".." / "outputs" / "o.bin").string(), std::string("link"),
                                    std::string("dir"), std::string("missing.bin"), std::string(""),
                                    std::string("."), ckdir(l).string()}) {
        BOOST_TEST(!mgr->on_worker_checkpoint(ckpt(att, path)).has_value(), "path '" << path << "' accepted");
    }
    // Bad names (they become file names on resume).
    for (const std::string& name : {std::string("a/b"), std::string(".."), std::string(200, 'n')}) {
        BOOST_TEST(!mgr->on_worker_checkpoint(ckpt(att, "ok.bin", name)).has_value(), "name '" << name << "'");
    }
    // Unknown attempt (the daemon authenticates the connection by hello token;
    // here only the attempt id is checked).
    BOOST_TEST(!mgr->on_worker_checkpoint(ckpt("att-unknown", "ok.bin")).has_value());
    BOOST_TEST(fired == 0u);
    BOOST_TEST(store.ingests.empty());
    BOOST_TEST(mgr->on_worker_checkpoint(ckpt(att, "ok.bin")).has_value());
    BOOST_TEST(fired == 1u);

    // Not running any more.
    sup.exit_code(1, 0);
    pump();
    BOOST_TEST(state(id) == v1::COMPLETED);
    BOOST_TEST(!mgr->on_worker_checkpoint(ckpt(att, "ok.bin")).has_value());
    // Being cancelled.
    submit(ckpt_spec());
    const auto& l3 = launch(2);
    write_file(ckdir(l3) / "c.bin", "x");
    BOOST_REQUIRE(mgr->cancel(journal.jobs.at(l3.env.at("FIREFORGE_JOB_ID")).job_id).has_value());
    BOOST_TEST(!mgr->on_worker_checkpoint(ckpt(l3.attempt_id, "c.bin")).has_value());
    BOOST_TEST(fired == 1u);
}

BOOST_FIXTURE_TEST_CASE(local_retry_resumes_from_latest_checkpoint, Fixture) {
    const auto spec_ck = store.add("control-plane-ck");
    auto s = ckpt_spec(3);
    s.set_resume_checkpoint(spec_ck);
    auto id = submit(s);
    // Attempt 1 resumes from the spec's checkpoint.
    auto t1 = mgr->on_worker_hello(hello_for(launch(0)));
    BOOST_REQUIRE(t1.has_value());
    BOOST_TEST(t1->resume_checkpoint() == (ckdir(launch(0)) / "resume" / "checkpoint").string());
    BOOST_TEST(read_file(t1->resume_checkpoint()) == "control-plane-ck");

    write_file(ckdir(launch(0)) / "a.bin", "first");
    BOOST_REQUIRE(mgr->on_worker_checkpoint(ckpt(launch(0).attempt_id, "a.bin", "state")).has_value());
    write_file(ckdir(launch(0)) / "a.bin", "second");  // the worker keeps writing
    BOOST_REQUIRE(mgr->on_worker_checkpoint(ckpt(launch(0).attempt_id, "a.bin", "state")).has_value());
    sup.exit_kind(0, ExitKind::kSignaled);
    pump();
    BOOST_TEST(state(id) == v1::RUNNING);
    BOOST_REQUIRE_EQUAL(sup.launches.size(), 2u);

    // Attempt 2: the latest local checkpoint wins over spec.resume_checkpoint.
    auto t2 = mgr->on_worker_hello(hello_for(launch(1)));
    BOOST_REQUIRE(t2.has_value());
    BOOST_TEST(t2->attempt_number() == 2u);
    const fs::path resume = ckdir(launch(1)) / "resume" / "state";
    BOOST_TEST(t2->resume_checkpoint() == resume.string());
    BOOST_TEST(read_file(resume) == "second");
    BOOST_TEST(mgr->status(id)->checkpoint().name() == "state");

    // The resume file itself may be re-submitted as a checkpoint.
    auto again = mgr->on_worker_checkpoint(ckpt(launch(1).attempt_id, "resume/state", "state"));
    BOOST_TEST(again.has_value());
    sup.exit_code(1, 0);
    pump();
    BOOST_TEST(state(id) == v1::COMPLETED);
    BOOST_TEST(mgr->status(id)->checkpoint().name() == "state");  // kept after completion
}

BOOST_FIXTURE_TEST_CASE(spec_resume_checkpoint_validated_and_used, Fixture) {
    auto missing = ckpt_spec();
    missing.set_resume_checkpoint("artifact://sha256/" + std::string(64, 'e'));
    auto r = mgr->submit(missing);
    BOOST_REQUIRE(!r.has_value());
    BOOST_TEST(r.error().find("not found locally") != std::string::npos);
    auto bad = ckpt_spec();
    bad.set_resume_checkpoint("nope");
    BOOST_TEST(!mgr->submit(bad).has_value());

    // Works without policy.checkpoint too (resume only needs the file).
    const auto ck = store.add("resume-me");
    auto s = spec();
    s.set_resume_checkpoint(ck);
    submit(s);
    auto t = mgr->on_worker_hello(hello_for(launch(0)));
    BOOST_REQUIRE(t.has_value());
    BOOST_TEST(read_file(t->resume_checkpoint()) == "resume-me");
    BOOST_TEST(!t->checkpoint_enabled());
}

BOOST_FIXTURE_TEST_CASE(run_as_resume_dir_is_read_only_for_job, Fixture) {
    run_as = self_run_as();
    make({4, 4 * kGiB, 2});
    const auto ck = store.add("resume-me");
    auto s = ckpt_spec();
    s.set_resume_checkpoint(ck);
    submit(s);
    const fs::path resume_dir = ckdir(launch(0)) / "resume";
    BOOST_TEST(dir_info(resume_dir).mode == 02750u);
    BOOST_TEST(dir_info(ckdir(launch(0))).mode == 02770u);
    BOOST_TEST(read_file(resume_dir / "checkpoint") == "resume-me");
    write_file(ckdir(launch(0)) / "c.bin", "x");
    BOOST_TEST(mgr->on_worker_checkpoint(ckpt(launch(0).attempt_id, "c.bin")).has_value());
    BOOST_TEST(!store.ingests.back().move_source);
}

BOOST_FIXTURE_TEST_CASE(spawn_persisted_and_survives_restart, Fixture) {
    auto id = submit(spec());
    v1::WorkerResult r = ok_result(launch(0).attempt_id);
    auto* sp = r.add_spawn();
    sp->set_name("cand-1");
    sp->mutable_job()->set_operation("relax");
    sp->add_depends_on("prep");
    r.add_spawn()->set_name("cand-2");
    BOOST_REQUIRE(mgr->on_worker_result(r).has_value());
    sup.exit_code(0, 0);
    pump();
    auto st = mgr->status(id);
    BOOST_TEST(st->state() == v1::COMPLETED);
    BOOST_REQUIRE_EQUAL(st->spawn_size(), 2);
    BOOST_TEST(st->spawn(0).name() == "cand-1");
    BOOST_TEST(st->spawn(0).job().operation() == "relax");
    BOOST_TEST(st->spawn(1).name() == "cand-2");

    // Failed attempt: spawn ignored.
    auto failed = submit(spec());
    v1::WorkerResult fr;
    fr.set_attempt_id(launch(1).attempt_id);
    fr.set_success(false);
    fr.add_spawn()->set_name("ignored");
    BOOST_REQUIRE(mgr->on_worker_result(fr).has_value());
    sup.exit_code(1, 0);
    pump();
    BOOST_TEST(state(failed) == v1::FAILED);
    BOOST_TEST(mgr->status(failed)->spawn_size() == 0);

    // A new manager over the same journal still reports it.
    make({4, 4 * kGiB, 2});
    mgr->recover();
    pump();
    st = mgr->status(id);
    BOOST_REQUIRE_EQUAL(st->spawn_size(), 2);
    BOOST_TEST(st->spawn(1).name() == "cand-2");
    BOOST_TEST(mgr->list(v1::COMPLETED, 10).at(0).spawn_size() == 2);
}

BOOST_AUTO_TEST_SUITE_END()
