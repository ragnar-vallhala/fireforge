// In-memory fakes of the journal, artifact store and process supervisor used
// by the JobManager tests. They follow the documented interface semantics
// (journal.hpp / store.hpp / supervisor.hpp) without depending on the real
// implementations.
#pragma once

#include <algorithm>
#include <cstdio>
#include <format>
#include <fstream>
#include <functional>
#include <map>
#include <optional>
#include <ostream>
#include <random>
#include <sstream>
#include <string>
#include <vector>

#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>

#include "tdr/artifacts/store.hpp"
#include "tdr/common/common.hpp"
#include "tdr/journal/journal.hpp"
#include "tdr/supervisor/supervisor.hpp"

// Boost.Test printing for the scoped enum (found via ADL).
namespace tdr::journal {
inline std::ostream& operator<<(std::ostream& os, AttemptState s) { return os << static_cast<int>(s); }
}  // namespace tdr::journal

namespace tdr::jobs::testing {

namespace fs = std::filesystem;
namespace v1 = ::fireforge::tdr::v1;
using journal::AttemptRecord;
using journal::AttemptState;
using journal::JobRecord;

// ---- journal -------------------------------------------------------------------

class FakeJournal : public journal::IJournal {
public:
    std::map<std::string, JobRecord> jobs;
    std::vector<std::string> order;  // insertion (creation) order
    std::map<std::string, AttemptRecord> attempts;
    std::map<std::string, journal::ArtifactRow> artifacts;
    std::vector<std::tuple<std::string, v1::JobState, v1::JobState>> transitions;

    void insert_job(const JobRecord& job) override {
        if (jobs.contains(job.job_id)) throw journal::JournalError("duplicate job " + job.job_id);
        jobs[job.job_id] = job;
        order.push_back(job.job_id);
    }
    std::optional<JobRecord> get_job(const std::string& id) const override {
        auto it = jobs.find(id);
        if (it == jobs.end()) return std::nullopt;
        return it->second;
    }
    std::vector<JobRecord> list_jobs(v1::JobState state, std::size_t limit) const override {
        std::vector<JobRecord> out;
        for (auto it = order.rbegin(); it != order.rend() && out.size() < limit; ++it) {
            const auto& j = jobs.at(*it);
            if (state == v1::JOB_STATE_UNSPECIFIED || j.state == state) out.push_back(j);
        }
        return out;
    }
    std::vector<JobRecord> jobs_in_states(const std::vector<v1::JobState>& states) const override {
        std::vector<JobRecord> out;
        for (const auto& id : order) {
            const auto& j = jobs.at(id);
            if (std::ranges::find(states, j.state) != states.end()) out.push_back(j);
        }
        return out;
    }
    std::size_t count_in_state(v1::JobState state) const override {
        return static_cast<std::size_t>(
            std::ranges::count_if(jobs, [&](const auto& kv) { return kv.second.state == state; }));
    }
    bool transition(const std::string& id, v1::JobState from, v1::JobState to, const std::string& error) override {
        auto it = jobs.find(id);
        if (it == jobs.end() || it->second.state != from) return false;
        it->second.state = to;
        if (!error.empty()) it->second.last_error = error;
        it->second.updated_unix_ms = unix_ms();
        transitions.emplace_back(id, from, to);
        return true;
    }
    void set_progress(const std::string& id, double fraction, const std::string& message) override {
        auto& j = jobs.at(id);
        j.progress_fraction = fraction;
        j.progress_message = message;
    }
    void begin_attempt(const AttemptRecord& a) override {
        if (attempts.contains(a.attempt_id)) throw journal::JournalError("duplicate attempt");
        auto& j = jobs.at(a.job_id);
        attempts[a.attempt_id] = a;
        j.current_attempt_id = a.attempt_id;
        j.attempts_used += 1;
    }
    std::optional<AttemptRecord> get_attempt(const std::string& id) const override {
        auto it = attempts.find(id);
        if (it == attempts.end()) return std::nullopt;
        return it->second;
    }
    // Assumption: recording the process also moves the attempt to kRunning.
    void update_attempt_process(const std::string& id, std::int32_t pid, std::uint64_t ticks,
                                const std::string& cgroup) override {
        auto& a = attempts.at(id);
        a.pid = pid;
        a.pid_start_ticks = ticks;
        a.cgroup_path = cgroup;
        a.state = AttemptState::kRunning;
    }
    void finish_attempt(const std::string& id, AttemptState state, std::int32_t code, std::int32_t sig,
                        const std::string& error) override {
        auto& a = attempts.at(id);
        a.state = state;
        a.exit_code = code;
        a.exit_signal = sig;
        a.error = error;
        a.ended_unix_ms = unix_ms();
    }
    std::vector<AttemptRecord> open_attempts() const override {
        std::vector<AttemptRecord> out;
        for (const auto& [id, a] : attempts) {
            if (a.state == AttemptState::kStarting || a.state == AttemptState::kRunning) out.push_back(a);
        }
        return out;
    }
    bool commit_outputs(const std::string& job_id, const std::string& attempt_id,
                        const std::vector<v1::NamedArtifact>& outs, const std::vector<v1::StepSpec>& spawn,
                        const std::vector<v1::CalcRecord>& records) override {
        auto it = jobs.find(job_id);
        if (it == jobs.end() || it->second.state != v1::COMMITTING || it->second.current_attempt_id != attempt_id) {
            return false;
        }
        it->second.outputs = outs;
        it->second.output_artifacts.clear();
        for (const auto& o : outs) {
            if (std::ranges::find(it->second.output_artifacts, o.artifact_id()) == it->second.output_artifacts.end())
                it->second.output_artifacts.push_back(o.artifact_id());
        }
        it->second.spawn = spawn;
        it->second.records = records;
        it->second.state = v1::COMPLETED;
        it->second.updated_unix_ms = unix_ms();
        attempts.at(attempt_id).state = AttemptState::kSucceeded;
        transitions.emplace_back(job_id, v1::COMMITTING, v1::COMPLETED);
        return true;
    }
    void set_checkpoint(const std::string& job_id, const v1::NamedArtifact& c) override {
        if (auto it = jobs.find(job_id); it != jobs.end()) it->second.checkpoint = c;
    }
    void record_artifact(const journal::ArtifactRow& row) override { artifacts[row.id] = row; }
    std::optional<journal::ArtifactRow> get_artifact(const std::string& id) const override {
        auto it = artifacts.find(id);
        if (it == artifacts.end()) return std::nullopt;
        return it->second;
    }
    std::uint64_t failed_job_count() const override { return count_in_state(v1::FAILED); }

    // Test helper: put a record straight in (for recovery scenarios).
    void put(JobRecord rec) {
        if (!jobs.contains(rec.job_id)) order.push_back(rec.job_id);
        jobs[rec.job_id] = std::move(rec);
    }
};

// ---- store ---------------------------------------------------------------------

inline std::string read_file(const fs::path& p) {
    std::ifstream in(p, std::ios::binary);
    std::stringstream ss;
    ss << in.rdbuf();
    return ss.str();
}

inline void write_file(const fs::path& p, const std::string& content) {
    fs::create_directories(p.parent_path());
    std::ofstream out(p, std::ios::binary | std::ios::trunc);
    out << content;
}

// Deterministic 64-hex pseudo digest (not SHA-256; fine for a fake).
inline std::string fake_digest(const std::string& content) {
    std::string out;
    for (std::size_t salt = 0; salt < 4; ++salt) {
        out += std::format("{:016x}", std::hash<std::string>{}(std::to_string(salt) + ":" + content));
    }
    return out;
}

class FakeStore : public artifacts::IArtifactStore {
public:
    struct Ingest {
        fs::path src;
        std::string media_type;
        std::string producer_job;
        bool move_source;
    };

    explicit FakeStore(fs::path root) : root_(std::move(root)) { fs::create_directories(root_); }

    std::vector<Ingest> ingests;
    std::optional<std::string> fail_ingest;  // next ingest fails with this

    // Adds content directly; returns its id.
    std::string add(const std::string& content) {
        std::string hex = fake_digest(content);
        write_file(root_ / hex, content);
        return "artifact://sha256/" + hex;
    }

    std::expected<artifacts::ArtifactMeta, std::string> ingest(const fs::path& src, std::string media_type,
                                                               std::string producer_job, bool move_source) override {
        ingests.push_back({src, media_type, producer_job, move_source});
        if (fail_ingest) {
            auto e = *fail_ingest;
            fail_ingest.reset();
            return std::unexpected(e);
        }
        if (!fs::is_regular_file(src)) return std::unexpected("no such file: " + src.string());
        std::string content = read_file(src);
        std::string hex = fake_digest(content);
        fs::path dst = root_ / hex;
        if (move_source) {
            fs::rename(src, dst);
        } else {
            write_file(dst, content);
        }
        artifacts::ArtifactMeta m;
        m.id = "artifact://sha256/" + hex;
        m.sha256_hex = hex;
        m.size = content.size();
        m.media_type = media_type;
        m.producer_job = producer_job;
        m.created_unix_ms = unix_ms();
        m.local_path = dst;
        return m;
    }
    bool contains(std::string_view id) const override { return path_of(id).has_value(); }
    std::optional<fs::path> path_of(std::string_view id) const override {
        constexpr std::string_view prefix = "artifact://sha256/";
        if (!id.starts_with(prefix)) return std::nullopt;
        fs::path p = root_ / std::string(id.substr(prefix.size()));
        if (!fs::exists(p)) return std::nullopt;
        return p;
    }
    std::expected<void, std::string> materialize(std::string_view id, const fs::path& dest) const override {
        auto p = path_of(id);
        if (!p) return std::unexpected("missing artifact");
        fs::create_directories(dest.parent_path());
        fs::copy_file(*p, dest, fs::copy_options::overwrite_existing);
        return {};
    }
    bool verify(std::string_view id) const override { return contains(id); }
    const fs::path& root() const override { return root_; }

private:
    fs::path root_;
};

// ---- supervisor ----------------------------------------------------------------

class FakeSupervisor : public supervisor::IProcessSupervisor {
public:
    explicit FakeSupervisor(boost::asio::io_context& io) : io_(io) {}

    struct Launch {
        supervisor::LaunchSpec spec;
        supervisor::ExitCallback on_exit;
        bool exited = false;
    };

    std::vector<Launch> launches;                              // in launch order
    std::vector<std::pair<std::string, std::chrono::seconds>> cancels;
    std::vector<supervisor::ProcessHandle> orphans_killed;
    std::optional<std::string> fail_launch;                   // sticky while set
    int next_pid = 1000;

    std::expected<supervisor::ProcessHandle, std::string> launch(const supervisor::LaunchSpec& spec,
                                                                 supervisor::ExitCallback on_exit) override {
        if (fail_launch) return std::unexpected(*fail_launch);
        launches.push_back({spec, std::move(on_exit), false});
        supervisor::ProcessHandle h;
        h.pid = next_pid++;
        h.start_ticks = 12345;
        return h;
    }
    void cancel(const std::string& attempt_id, std::chrono::seconds grace) override {
        cancels.emplace_back(attempt_id, grace);
    }
    bool kill_orphan(const supervisor::ProcessHandle& h) override {
        orphans_killed.push_back(h);
        return true;
    }
    std::size_t running_count() const override {
        return static_cast<std::size_t>(std::ranges::count_if(launches, [](const Launch& l) { return !l.exited; }));
    }
    bool cgroups_enabled() const override { return false; }

    Launch& find(const std::string& attempt_id) {
        for (auto& l : launches) {
            if (l.spec.attempt_id == attempt_id) return l;
        }
        throw std::runtime_error("no launch for " + attempt_id);
    }

    // Simulate the child exiting: the callback runs later on the io thread.
    void exit(std::size_t index, supervisor::ExitInfo info) {
        Launch& l = launches.at(index);
        if (l.exited) throw std::runtime_error("double exit");
        l.exited = true;
        boost::asio::post(io_, [cb = l.on_exit, info] { cb(info); });
    }
    void exit_code(std::size_t index, int code) {
        supervisor::ExitInfo info;
        info.kind = supervisor::ExitKind::kExited;
        info.exit_code = code;
        exit(index, info);
    }
    void exit_kind(std::size_t index, supervisor::ExitKind kind) {
        supervisor::ExitInfo info;
        info.kind = kind;
        info.signal = kind == supervisor::ExitKind::kExited ? 0 : 9;
        exit(index, info);
    }

private:
    boost::asio::io_context& io_;
};

// ---- temp dir -------------------------------------------------------------------

struct TempDir {
    fs::path path;
    TempDir() {
        path = fs::path("/tmp/claude-1000") / new_id("jobs-test");
        fs::create_directories(path);
    }
    ~TempDir() {
        std::error_code ec;
        fs::remove_all(path, ec);
    }
};

}  // namespace tdr::jobs::testing
