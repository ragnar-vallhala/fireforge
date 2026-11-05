#include <boost/test/unit_test.hpp>

#include <filesystem>

#include "tdr/common/common.hpp"
#include "tdr/journal/journal.hpp"

namespace fs = std::filesystem;
using namespace tdr::journal;

namespace {

struct Fixture {
    fs::path dir = fs::path("/tmp/claude-1000") / tdr::new_id("journal-test");
    fs::path db_path = dir / "state" / "journal.db";  // parent dir created by open
    std::unique_ptr<IJournal> j = open_sqlite_journal(db_path);
    ~Fixture() {
        j.reset();
        std::error_code ec;
        fs::remove_all(dir, ec);
    }
};

JobRecord make_job(const std::string& id, std::int64_t created, v1::JobState st = v1::QUEUED) {
    JobRecord r;
    r.job_id = id;
    r.spec.set_job_id(id);
    r.spec.set_operation("transport.calculate");
    r.spec.set_solver("qe");
    r.spec.mutable_resources()->set_cpu_cores(4);
    r.spec.mutable_resources()->set_memory_bytes(8ull << 30);
    r.spec.mutable_policy()->set_max_attempts(3);
    r.spec.add_input_artifacts("artifact://sha256/" + std::string(64, 'a'));
    r.spec.mutable_command()->add_argv("python3");
    r.spec.mutable_command()->add_argv("-m");
    (*r.spec.mutable_command()->mutable_env())["OMP_NUM_THREADS"] = "4";
    r.spec.set_parameters_json(R"({"k": [1, 2]})");
    r.state = st;
    r.created_unix_ms = created;
    r.updated_unix_ms = created;
    return r;
}

AttemptRecord make_attempt(const std::string& id, const std::string& job, std::uint32_t n) {
    AttemptRecord a;
    a.attempt_id = id;
    a.job_id = job;
    a.number = n;
    a.token = tdr::new_secret();
    a.work_dir = "/var/lib/fireforge/work/" + job + "/" + id;
    a.started_unix_ms = 1000 + n;
    return a;
}

std::vector<std::string> ids(const std::vector<JobRecord>& v) {
    std::vector<std::string> out;
    for (auto& r : v) out.push_back(r.job_id);
    return out;
}

}  // namespace

BOOST_FIXTURE_TEST_SUITE(journal, Fixture)

BOOST_AUTO_TEST_CASE(insert_get_roundtrip) {
    auto in = make_job("job-1", 111);
    in.attempts_used = 2;
    in.current_attempt_id = "att-x";
    in.last_error = "boom";
    in.output_artifacts = {"artifact://sha256/" + std::string(64, 'b'), "artifact://sha256/" + std::string(64, 'c')};
    in.progress_fraction = 0.25;
    in.progress_message = "scf";
    in.updated_unix_ms = 222;
    j->insert_job(in);

    auto out = j->get_job("job-1");
    BOOST_TEST_REQUIRE(out.has_value());
    BOOST_TEST(out->job_id == "job-1");
    BOOST_TEST(out->spec.SerializeAsString() == in.spec.SerializeAsString());
    BOOST_TEST(out->spec.command().env().at("OMP_NUM_THREADS") == "4");
    BOOST_TEST(out->spec.resources().memory_bytes() == (8ull << 30));
    BOOST_TEST(out->state == v1::QUEUED);
    BOOST_TEST(out->attempts_used == 2u);
    BOOST_TEST(out->current_attempt_id == "att-x");
    BOOST_TEST(out->last_error == "boom");
    BOOST_TEST(out->output_artifacts == in.output_artifacts);
    BOOST_TEST(out->created_unix_ms == 111);
    BOOST_TEST(out->updated_unix_ms == 222);
    BOOST_TEST(out->progress_fraction == 0.25);
    BOOST_TEST(out->progress_message == "scf");

    BOOST_TEST(!j->get_job("job-none").has_value());
}

BOOST_AUTO_TEST_CASE(duplicate_insert_throws) {
    j->insert_job(make_job("job-1", 1));
    BOOST_CHECK_THROW(j->insert_job(make_job("job-1", 2, v1::FAILED)), JournalError);
    BOOST_TEST(j->get_job("job-1")->created_unix_ms == 1);
    BOOST_TEST(j->count_in_state(v1::FAILED) == 0u);
    // Journal still usable after the failed transaction.
    j->insert_job(make_job("job-2", 2));
    BOOST_TEST(j->count_in_state(v1::QUEUED) == 2u);
}

BOOST_AUTO_TEST_CASE(transition_cas) {
    j->insert_job(make_job("job-1", 1));
    BOOST_TEST(j->transition("job-1", v1::QUEUED, v1::ASSIGNED));
    auto r = j->get_job("job-1");
    BOOST_TEST(r->state == v1::ASSIGNED);
    BOOST_TEST(r->updated_unix_ms > 1);
    BOOST_TEST(r->last_error == "");

    // Wrong `from`: no change.
    BOOST_TEST(!j->transition("job-1", v1::QUEUED, v1::RUNNING, "x"));
    r = j->get_job("job-1");
    BOOST_TEST(r->state == v1::ASSIGNED);
    BOOST_TEST(r->last_error == "");

    BOOST_TEST(j->transition("job-1", v1::ASSIGNED, v1::RETRY_PENDING, "exit 3"));
    BOOST_TEST(j->get_job("job-1")->last_error == "exit 3");
    // Empty error keeps the previous one.
    BOOST_TEST(j->transition("job-1", v1::RETRY_PENDING, v1::QUEUED));
    BOOST_TEST(j->get_job("job-1")->last_error == "exit 3");

    BOOST_TEST(!j->transition("job-missing", v1::QUEUED, v1::ASSIGNED));
}

BOOST_AUTO_TEST_CASE(progress) {
    j->insert_job(make_job("job-1", 1));
    j->set_progress("job-1", 0.5, "half");
    auto r = j->get_job("job-1");
    BOOST_TEST(r->progress_fraction == 0.5);
    BOOST_TEST(r->progress_message == "half");
    BOOST_CHECK_NO_THROW(j->set_progress("job-missing", 1.0, ""));
}

BOOST_AUTO_TEST_CASE(attempts_lifecycle) {
    j->insert_job(make_job("job-1", 1));
    auto a1 = make_attempt("att-1", "job-1", 1);
    j->begin_attempt(a1);
    auto r = j->get_job("job-1");
    BOOST_TEST(r->attempts_used == 1u);
    BOOST_TEST(r->current_attempt_id == "att-1");

    auto g = j->get_attempt("att-1");
    BOOST_TEST_REQUIRE(g.has_value());
    BOOST_TEST(g->job_id == "job-1");
    BOOST_TEST(g->number == 1u);
    BOOST_TEST(g->token == a1.token);
    BOOST_TEST(g->work_dir == a1.work_dir);
    BOOST_TEST(g->started_unix_ms == a1.started_unix_ms);
    BOOST_TEST((g->state == AttemptState::kStarting));

    const std::uint64_t big_ticks = 0xFFFF'FFFF'FFFF'FFF0ull;  // survives int64 storage
    j->update_attempt_process("att-1", 4242, big_ticks, "/sys/fs/cgroup/x/jobs/att-1");
    g = j->get_attempt("att-1");
    BOOST_TEST(g->pid == 4242);
    BOOST_TEST(g->pid_start_ticks == big_ticks);
    BOOST_TEST(g->cgroup_path == "/sys/fs/cgroup/x/jobs/att-1");
    BOOST_TEST((g->state == AttemptState::kRunning));

    j->finish_attempt("att-1", AttemptState::kFailed, 3, 0, "exit code 3");
    g = j->get_attempt("att-1");
    BOOST_TEST((g->state == AttemptState::kFailed));
    BOOST_TEST(g->exit_code == 3);
    BOOST_TEST(g->exit_signal == 0);
    BOOST_TEST(g->error == "exit code 3");
    BOOST_TEST(g->ended_unix_ms > 0);

    j->begin_attempt(make_attempt("att-2", "job-1", 2));
    r = j->get_job("job-1");
    BOOST_TEST(r->attempts_used == 2u);
    BOOST_TEST(r->current_attempt_id == "att-2");

    BOOST_TEST(!j->get_attempt("att-none").has_value());
    BOOST_CHECK_THROW(j->begin_attempt(make_attempt("att-3", "job-missing", 1)), JournalError);
    BOOST_CHECK_THROW(j->begin_attempt(make_attempt("att-2", "job-1", 3)), JournalError);  // dup id
    BOOST_TEST(j->get_job("job-1")->attempts_used == 2u);  // failed begin changed nothing
    BOOST_CHECK_THROW(j->finish_attempt("att-none", AttemptState::kFailed, 0, 0, ""), JournalError);
    BOOST_CHECK_THROW(j->update_attempt_process("att-none", 1, 1, ""), JournalError);
}

BOOST_AUTO_TEST_CASE(open_attempts_filter) {
    j->insert_job(make_job("job-1", 1));
    j->insert_job(make_job("job-2", 2));
    j->begin_attempt(make_attempt("att-1", "job-1", 1));  // starting
    j->begin_attempt(make_attempt("att-2", "job-2", 1));
    j->update_attempt_process("att-2", 10, 20, "");       // running
    j->begin_attempt(make_attempt("att-3", "job-1", 2));
    j->finish_attempt("att-3", AttemptState::kCancelled, 0, 15, "");
    auto open = j->open_attempts();
    BOOST_TEST_REQUIRE(open.size() == 2u);
    BOOST_TEST(open[0].attempt_id == "att-1");
    BOOST_TEST(open[1].attempt_id == "att-2");
    j->finish_attempt("att-1", AttemptState::kAbandoned, 0, 0, "daemon restart");
    j->finish_attempt("att-2", AttemptState::kSucceeded, 0, 0, "");
    BOOST_TEST(j->open_attempts().empty());
}

BOOST_AUTO_TEST_CASE(commit_outputs_fencing) {
    j->insert_job(make_job("job-1", 1));
    j->begin_attempt(make_attempt("att-1", "job-1", 1));
    j->finish_attempt("att-1", AttemptState::kFailed, 1, 0, "");
    j->begin_attempt(make_attempt("att-2", "job-1", 2));
    const std::vector<std::string> outs = {"artifact://sha256/" + std::string(64, 'd')};

    // Not COMMITTING yet: refused.
    BOOST_TEST(!j->commit_outputs("job-1", "att-2", outs));
    BOOST_TEST(j->get_job("job-1")->state == v1::QUEUED);

    BOOST_TEST(j->transition("job-1", v1::QUEUED, v1::COMMITTING));
    const auto before = j->get_job("job-1");

    // Stale attempt: refused, nothing changed.
    BOOST_TEST(!j->commit_outputs("job-1", "att-1", outs));
    auto r = j->get_job("job-1");
    BOOST_TEST(r->state == v1::COMMITTING);
    BOOST_TEST(r->output_artifacts.empty());
    BOOST_TEST(r->updated_unix_ms == before->updated_unix_ms);
    BOOST_TEST((j->get_attempt("att-1")->state == AttemptState::kFailed));
    BOOST_TEST((j->get_attempt("att-2")->state == AttemptState::kStarting));

    BOOST_TEST(!j->commit_outputs("job-1", "", outs));
    BOOST_TEST(!j->commit_outputs("job-missing", "att-2", outs));

    // Current attempt: accepted.
    BOOST_TEST(j->commit_outputs("job-1", "att-2", outs));
    r = j->get_job("job-1");
    BOOST_TEST(r->state == v1::COMPLETED);
    BOOST_TEST(r->output_artifacts == outs);
    auto a = j->get_attempt("att-2");
    BOOST_TEST((a->state == AttemptState::kSucceeded));
    BOOST_TEST(a->ended_unix_ms > 0);

    // Second commit: job no longer COMMITTING.
    BOOST_TEST(!j->commit_outputs("job-1", "att-2", {}));
    BOOST_TEST(j->get_job("job-1")->output_artifacts == outs);
}

BOOST_AUTO_TEST_CASE(list_ordering_and_filters) {
    j->insert_job(make_job("job-b", 200));
    j->insert_job(make_job("job-a", 100));
    j->insert_job(make_job("job-c", 300, v1::RUNNING));
    j->insert_job(make_job("job-d", 200, v1::FAILED));  // ties job-b on created; inserted later
    j->insert_job(make_job("job-e", 50, v1::RETRY_PENDING));

    using V = std::vector<std::string>;
    BOOST_TEST(ids(j->list_jobs(v1::JOB_STATE_UNSPECIFIED, 10)) == (V{"job-c", "job-d", "job-b", "job-a", "job-e"}));
    BOOST_TEST(ids(j->list_jobs(v1::JOB_STATE_UNSPECIFIED, 2)) == (V{"job-c", "job-d"}));
    BOOST_TEST(ids(j->list_jobs(v1::QUEUED, 10)) == (V{"job-b", "job-a"}));
    BOOST_TEST(ids(j->list_jobs(v1::QUEUED, 1)) == (V{"job-b"}));
    BOOST_TEST(j->list_jobs(v1::COMPLETED, 10).empty());
    BOOST_TEST(j->list_jobs(v1::JOB_STATE_UNSPECIFIED, 0).size() == 5u);  // 0 = no limit

    BOOST_TEST(ids(j->jobs_in_states({v1::QUEUED, v1::RETRY_PENDING})) == (V{"job-e", "job-a", "job-b"}));
    BOOST_TEST(ids(j->jobs_in_states({v1::QUEUED, v1::FAILED})) == (V{"job-a", "job-b", "job-d"}));
    BOOST_TEST(j->jobs_in_states({}).empty());

    BOOST_TEST(j->count_in_state(v1::QUEUED) == 2u);
    BOOST_TEST(j->count_in_state(v1::RUNNING) == 1u);
    BOOST_TEST(j->count_in_state(v1::COMPLETED) == 0u);
    BOOST_TEST(j->list_jobs(v1::QUEUED, 10)[0].spec.operation() == "transport.calculate");
}

BOOST_AUTO_TEST_CASE(failed_job_count) {
    j->insert_job(make_job("j1", 1, v1::FAILED));
    j->insert_job(make_job("j2", 2, v1::EXPIRED));
    j->insert_job(make_job("j3", 3, v1::VALIDATION_FAILED));
    j->insert_job(make_job("j4", 4, v1::CANCELLED));
    j->insert_job(make_job("j5", 5, v1::COMPLETED));
    BOOST_TEST(j->failed_job_count() == 3u);
}

BOOST_AUTO_TEST_CASE(artifacts_upsert) {
    ArtifactRow r;
    r.id = "artifact://sha256/" + std::string(64, 'e');
    r.sha256_hex = std::string(64, 'e');
    r.size = 1ull << 40;
    r.media_type = "text/plain";
    r.producer_job = "job-1";
    r.local_path = "/var/lib/fireforge/artifacts/sha256/ee/" + r.sha256_hex;
    r.created_unix_ms = 77;
    j->record_artifact(r);
    auto g = j->get_artifact(r.id);
    BOOST_TEST_REQUIRE(g.has_value());
    BOOST_TEST(g->sha256_hex == r.sha256_hex);
    BOOST_TEST(g->size == r.size);
    BOOST_TEST(g->media_type == "text/plain");
    BOOST_TEST(g->producer_job == "job-1");
    BOOST_TEST(g->local_path == r.local_path);
    BOOST_TEST(g->created_unix_ms == 77);

    r.media_type = "application/json";
    r.producer_job = "job-2";
    BOOST_CHECK_NO_THROW(j->record_artifact(r));
    g = j->get_artifact(r.id);
    BOOST_TEST(g->media_type == "application/json");
    BOOST_TEST(g->producer_job == "job-2");
    BOOST_TEST(!j->get_artifact("artifact://sha256/none").has_value());
}

BOOST_AUTO_TEST_CASE(reopen_persists) {
    j->insert_job(make_job("job-1", 1));
    j->begin_attempt(make_attempt("att-1", "job-1", 1));
    j->update_attempt_process("att-1", 99, 12345, "cg");
    BOOST_TEST(j->transition("job-1", v1::QUEUED, v1::RUNNING));
    ArtifactRow art;
    art.id = "artifact://sha256/" + std::string(64, 'f');
    art.sha256_hex = std::string(64, 'f');
    j->record_artifact(art);

    j.reset();
    BOOST_TEST(fs::exists(db_path));
    j = open_sqlite_journal(db_path);  // existing schema, user_version already 1

    auto r = j->get_job("job-1");
    BOOST_TEST_REQUIRE(r.has_value());
    BOOST_TEST(r->state == v1::RUNNING);
    BOOST_TEST(r->current_attempt_id == "att-1");
    BOOST_TEST(r->attempts_used == 1u);
    BOOST_TEST(r->spec.command().argv_size() == 2);
    auto open = j->open_attempts();
    BOOST_TEST_REQUIRE(open.size() == 1u);
    BOOST_TEST(open[0].pid == 99);
    BOOST_TEST(open[0].pid_start_ticks == 12345u);
    BOOST_TEST(j->get_artifact(art.id).has_value());

    // A second handle on the same file sees the committed data too.
    auto other = open_sqlite_journal(db_path);
    BOOST_TEST(other->count_in_state(v1::RUNNING) == 1u);
}

BOOST_AUTO_TEST_CASE(attempt_state_names) {
    BOOST_TEST(std::string(to_string(AttemptState::kStarting)) == "STARTING");
    BOOST_TEST(std::string(to_string(AttemptState::kRunning)) == "RUNNING");
    BOOST_TEST(std::string(to_string(AttemptState::kSucceeded)) == "SUCCEEDED");
    BOOST_TEST(std::string(to_string(AttemptState::kFailed)) == "FAILED");
    BOOST_TEST(std::string(to_string(AttemptState::kCancelled)) == "CANCELLED");
    BOOST_TEST(std::string(to_string(AttemptState::kAbandoned)) == "ABANDONED");
}

BOOST_AUTO_TEST_CASE(open_bad_path_throws) {
    BOOST_CHECK_THROW(open_sqlite_journal("/proc/no-such/journal.db"), JournalError);
}

BOOST_AUTO_TEST_SUITE_END()
