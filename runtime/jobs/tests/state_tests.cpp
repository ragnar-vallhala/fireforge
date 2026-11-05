#include <boost/test/unit_test.hpp>

#include "tdr/jobs/jobs.hpp"

namespace v1 = ::fireforge::tdr::v1;
using tdr::jobs::is_terminal;
using tdr::jobs::is_valid_transition;

BOOST_AUTO_TEST_SUITE(jobs_state_machine)

BOOST_AUTO_TEST_CASE(terminal_states) {
    for (auto s : {v1::COMPLETED, v1::FAILED, v1::CANCELLED, v1::EXPIRED, v1::VALIDATION_FAILED}) {
        BOOST_TEST(is_terminal(s), tdr::jobs::to_string(s));
    }
    for (auto s : {v1::CREATED, v1::QUEUED, v1::LEASED, v1::ASSIGNED, v1::RUNNING, v1::COMMITTING,
                   v1::RETRY_PENDING}) {
        BOOST_TEST(!is_terminal(s), tdr::jobs::to_string(s));
    }
}

BOOST_AUTO_TEST_CASE(transition_table) {
    struct T {
        v1::JobState from, to;
        bool ok;
    };
    const T table[] = {
        {v1::CREATED, v1::QUEUED, true},
        {v1::CREATED, v1::RUNNING, false},
        {v1::QUEUED, v1::ASSIGNED, true},
        {v1::QUEUED, v1::LEASED, true},
        {v1::QUEUED, v1::CANCELLED, true},
        {v1::QUEUED, v1::RUNNING, false},
        {v1::QUEUED, v1::COMPLETED, false},
        {v1::LEASED, v1::ASSIGNED, true},
        {v1::LEASED, v1::QUEUED, true},
        {v1::LEASED, v1::EXPIRED, true},
        {v1::LEASED, v1::CANCELLED, true},
        {v1::LEASED, v1::RUNNING, false},
        {v1::ASSIGNED, v1::RUNNING, true},
        {v1::ASSIGNED, v1::CANCELLED, true},
        {v1::ASSIGNED, v1::RETRY_PENDING, true},
        {v1::ASSIGNED, v1::FAILED, true},
        {v1::ASSIGNED, v1::COMMITTING, false},
        {v1::RUNNING, v1::COMMITTING, true},
        {v1::RUNNING, v1::RETRY_PENDING, true},
        {v1::RUNNING, v1::FAILED, true},
        {v1::RUNNING, v1::EXPIRED, true},
        {v1::RUNNING, v1::CANCELLED, true},
        {v1::RUNNING, v1::COMPLETED, false},
        {v1::RUNNING, v1::QUEUED, false},
        {v1::COMMITTING, v1::COMPLETED, true},
        {v1::COMMITTING, v1::VALIDATION_FAILED, true},
        {v1::COMMITTING, v1::RUNNING, false},
        {v1::RETRY_PENDING, v1::QUEUED, true},
        {v1::RETRY_PENDING, v1::CANCELLED, true},
        {v1::RETRY_PENDING, v1::RUNNING, false},
        {v1::COMPLETED, v1::QUEUED, false},
        {v1::FAILED, v1::QUEUED, false},
        {v1::CANCELLED, v1::QUEUED, false},
        {v1::EXPIRED, v1::QUEUED, false},
        {v1::VALIDATION_FAILED, v1::COMPLETED, false},
        {v1::JOB_STATE_UNSPECIFIED, v1::CREATED, false},
    };
    for (const auto& t : table) {
        BOOST_TEST(is_valid_transition(t.from, t.to) == t.ok,
                   tdr::jobs::to_string(t.from) << " -> " << tdr::jobs::to_string(t.to));
    }
    for (int s = v1::JobState_MIN; s <= v1::JobState_MAX; ++s) {
        auto st = static_cast<v1::JobState>(s);
        BOOST_TEST(!is_valid_transition(st, st), "self " << tdr::jobs::to_string(st));
        if (is_terminal(st)) {
            for (int t = v1::JobState_MIN; t <= v1::JobState_MAX; ++t) {
                BOOST_TEST(!is_valid_transition(st, static_cast<v1::JobState>(t)));
            }
        }
    }
}

BOOST_AUTO_TEST_CASE(names) {
    BOOST_TEST(std::string(tdr::jobs::to_string(v1::RETRY_PENDING)) == "RETRY_PENDING");
    BOOST_TEST(std::string(tdr::jobs::to_string(v1::VALIDATION_FAILED)) == "VALIDATION_FAILED");
}

BOOST_AUTO_TEST_CASE(effective_request_defaults) {
    v1::JobSpec spec;
    auto c = tdr::jobs::effective_request(spec, 512);
    BOOST_TEST(c.cpu_cores == 1u);
    BOOST_TEST(c.memory_bytes == 512u);
    BOOST_TEST(c.jobs == 1u);
    spec.mutable_resources()->set_cpu_cores(3);
    spec.mutable_resources()->set_memory_bytes(7);
    c = tdr::jobs::effective_request(spec, 512);
    BOOST_TEST(c.cpu_cores == 3u);
    BOOST_TEST(c.memory_bytes == 7u);
}

BOOST_AUTO_TEST_CASE(to_status_copies_fields) {
    tdr::journal::JobRecord rec;
    rec.job_id = "j1";
    rec.state = v1::RUNNING;
    rec.attempts_used = 2;
    rec.current_attempt_id = "a2";
    rec.last_error = "boom";
    rec.output_artifacts = {"x", "y"};
    rec.created_unix_ms = 10;
    rec.updated_unix_ms = 20;
    rec.progress_fraction = 0.5;
    rec.progress_message = "half";
    rec.spec.set_operation("op");
    auto st = tdr::jobs::to_status(rec);
    BOOST_TEST(st.job_id() == "j1");
    BOOST_TEST(st.state() == v1::RUNNING);
    BOOST_TEST(st.attempts_used() == 2u);
    BOOST_TEST(st.current_attempt_id() == "a2");
    BOOST_TEST(st.last_error() == "boom");
    BOOST_TEST(st.output_artifacts_size() == 2);
    BOOST_TEST(st.created_unix_ms() == 10);
    BOOST_TEST(st.updated_unix_ms() == 20);
    BOOST_TEST(st.progress_fraction() == 0.5);
    BOOST_TEST(st.progress_message() == "half");
    BOOST_TEST(st.spec().operation() == "op");
}

BOOST_AUTO_TEST_SUITE_END()
