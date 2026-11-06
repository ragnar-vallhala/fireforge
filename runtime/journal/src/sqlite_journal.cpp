// SQLite implementation of IJournal. See journal.hpp.
#include <google/protobuf/util/json_util.h>
#include <sqlite3.h>

#include <format>
#include <map>
#include <set>
#include <string_view>
#include <utility>

#include "tdr/common/common.hpp"
#include "tdr/journal/journal.hpp"

namespace tdr::journal {

namespace {

constexpr int kSchemaVersion = 2;
constexpr int kBusyTimeoutMs = 5000;

constexpr const char* kSchemaV1 = R"sql(
CREATE TABLE jobs (
    job_id             TEXT PRIMARY KEY NOT NULL,
    spec               BLOB NOT NULL,
    state              INTEGER NOT NULL,
    attempts_used      INTEGER NOT NULL DEFAULT 0,
    current_attempt_id TEXT NOT NULL DEFAULT '',
    last_error         TEXT NOT NULL DEFAULT '',
    created_ms         INTEGER NOT NULL,
    updated_ms         INTEGER NOT NULL,
    progress_fraction  REAL NOT NULL DEFAULT 0,
    progress_message   TEXT NOT NULL DEFAULT ''
);
CREATE INDEX jobs_state_created ON jobs(state, created_ms);
CREATE INDEX jobs_created ON jobs(created_ms);

CREATE TABLE job_outputs (
    job_id      TEXT NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
    idx         INTEGER NOT NULL,
    artifact_id TEXT NOT NULL,
    PRIMARY KEY (job_id, idx)
) WITHOUT ROWID;

CREATE TABLE attempts (
    attempt_id      TEXT PRIMARY KEY NOT NULL,
    job_id          TEXT NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
    number          INTEGER NOT NULL,
    token           TEXT NOT NULL,
    state           INTEGER NOT NULL,
    pid             INTEGER NOT NULL DEFAULT 0,
    pid_start_ticks INTEGER NOT NULL DEFAULT 0,
    cgroup_path     TEXT NOT NULL DEFAULT '',
    work_dir        TEXT NOT NULL DEFAULT '',
    exit_code       INTEGER NOT NULL DEFAULT 0,
    exit_signal     INTEGER NOT NULL DEFAULT 0,
    error           TEXT NOT NULL DEFAULT '',
    started_ms      INTEGER NOT NULL DEFAULT 0,
    ended_ms        INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX attempts_state ON attempts(state);
CREATE INDEX attempts_job ON attempts(job_id);

CREATE TABLE artifacts (
    id           TEXT PRIMARY KEY NOT NULL,
    sha256_hex   TEXT NOT NULL,
    size         INTEGER NOT NULL,
    media_type   TEXT NOT NULL DEFAULT '',
    producer_job TEXT NOT NULL DEFAULT '',
    local_path   TEXT NOT NULL DEFAULT '',
    created_ms   INTEGER NOT NULL DEFAULT 0
);
)sql";

// Phase 6: output names, spawn list, latest checkpoint.
constexpr const char* kSchemaV2 = R"sql(
ALTER TABLE job_outputs ADD COLUMN name TEXT NOT NULL DEFAULT '';
ALTER TABLE job_outputs ADD COLUMN size INTEGER NOT NULL DEFAULT 0;
ALTER TABLE job_outputs ADD COLUMN media_type TEXT NOT NULL DEFAULT '';
ALTER TABLE jobs ADD COLUMN spawn_json TEXT NOT NULL DEFAULT '';
ALTER TABLE jobs ADD COLUMN checkpoint_id TEXT NOT NULL DEFAULT '';
ALTER TABLE jobs ADD COLUMN checkpoint_name TEXT NOT NULL DEFAULT '';
ALTER TABLE jobs ADD COLUMN checkpoint_size INTEGER NOT NULL DEFAULT 0;
ALTER TABLE jobs ADD COLUMN checkpoint_media_type TEXT NOT NULL DEFAULT '';
)sql";

constexpr const char* kJobColumns =
    "job_id, spec, state, attempts_used, current_attempt_id, last_error, created_ms, updated_ms, "
    "progress_fraction, progress_message, spawn_json, checkpoint_id, checkpoint_name, checkpoint_size, "
    "checkpoint_media_type";

// spawn_json holds the JSON array of StepSpec (proto3 JSON, proto field names);
// "" = none. Encoded through the JobStatus.spawn field, the only repeated
// StepSpec field around.
constexpr std::string_view kSpawnPrefix = "{\"spawn\":";

std::string spawn_to_json(const std::vector<v1::StepSpec>& spawn) {
    if (spawn.empty()) return {};
    v1::JobStatus holder;
    for (const auto& s : spawn) *holder.add_spawn() = s;
    google::protobuf::util::JsonPrintOptions po;
    po.preserve_proto_field_names = true;
    std::string out;
    if (!google::protobuf::util::MessageToJsonString(holder, &out, po).ok() || !out.starts_with(kSpawnPrefix) ||
        !out.ends_with('}')) {
        throw JournalError("journal: cannot encode spawn list");
    }
    return out.substr(kSpawnPrefix.size(), out.size() - kSpawnPrefix.size() - 1);
}

std::vector<v1::StepSpec> spawn_from_json(const std::string& job_id, const std::string& json) {
    if (json.empty()) return {};
    v1::JobStatus holder;
    google::protobuf::util::JsonParseOptions po;
    po.ignore_unknown_fields = true;
    const std::string doc = std::string(kSpawnPrefix) + json + "}";
    if (!google::protobuf::util::JsonStringToMessage(doc, &holder, po).ok()) {
        throw JournalError("journal: corrupt spawn list for job " + job_id);
    }
    return {holder.spawn().begin(), holder.spawn().end()};
}

std::vector<v1::NamedArtifact> unnamed(const std::vector<std::string>& ids) {
    std::vector<v1::NamedArtifact> out;
    for (const auto& id : ids) {
        v1::NamedArtifact a;
        a.set_artifact_id(id);
        out.push_back(std::move(a));
    }
    return out;
}
constexpr const char* kAttemptColumns =
    "attempt_id, job_id, number, token, state, pid, pid_start_ticks, cgroup_path, work_dir, exit_code, "
    "exit_signal, error, started_ms, ended_ms";

[[noreturn]] void fail(sqlite3* db, std::string_view what) {
    throw JournalError(std::format("journal: {}: {}", what, db ? sqlite3_errmsg(db) : "out of memory"));
}

// RAII prepared statement. Owned by the statement cache; reset after each use.
class Stmt {
public:
    Stmt(sqlite3* db, const std::string& sql) : db_(db) {
        if (sqlite3_prepare_v3(db, sql.c_str(), static_cast<int>(sql.size()), SQLITE_PREPARE_PERSISTENT, &s_,
                               nullptr) != SQLITE_OK) {
            fail(db, "prepare '" + sql + "'");
        }
    }
    Stmt(const Stmt&) = delete;
    Stmt& operator=(const Stmt&) = delete;
    ~Stmt() { sqlite3_finalize(s_); }

    void reset() {
        sqlite3_reset(s_);
        sqlite3_clear_bindings(s_);
    }

    Stmt& bind(int i, std::string_view v) {
        check(sqlite3_bind_text(s_, i, v.data(), static_cast<int>(v.size()), SQLITE_TRANSIENT));
        return *this;
    }
    Stmt& bind(int i, const std::string& v) { return bind(i, std::string_view(v)); }
    Stmt& bind(int i, const char* v) { return bind(i, std::string_view(v)); }
    Stmt& bind_blob(int i, const std::string& v) {
        check(sqlite3_bind_blob(s_, i, v.data(), static_cast<int>(v.size()), SQLITE_TRANSIENT));
        return *this;
    }
    Stmt& bind(int i, std::int64_t v) {
        check(sqlite3_bind_int64(s_, i, v));
        return *this;
    }
    Stmt& bind(int i, int v) { return bind(i, static_cast<std::int64_t>(v)); }
    Stmt& bind(int i, std::uint32_t v) { return bind(i, static_cast<std::int64_t>(v)); }
    // uint64 values are stored bit-for-bit as int64.
    Stmt& bind(int i, std::uint64_t v) { return bind(i, static_cast<std::int64_t>(v)); }
    Stmt& bind(int i, double v) {
        check(sqlite3_bind_double(s_, i, v));
        return *this;
    }

    // true = a row is available, false = done.
    bool step() {
        const int rc = sqlite3_step(s_);
        if (rc == SQLITE_ROW) return true;
        if (rc == SQLITE_DONE) return false;
        fail(db_, std::string("step '") + sqlite3_sql(s_) + "'");
    }
    void exec() {
        while (step()) {
        }
    }

    std::int64_t i64(int c) const { return sqlite3_column_int64(s_, c); }
    double dbl(int c) const { return sqlite3_column_double(s_, c); }
    std::string text(int c) const {
        const auto* p = reinterpret_cast<const char*>(sqlite3_column_text(s_, c));
        return p ? std::string(p, static_cast<std::size_t>(sqlite3_column_bytes(s_, c))) : std::string();
    }
    std::string blob(int c) const {
        const auto* p = static_cast<const char*>(sqlite3_column_blob(s_, c));
        return p ? std::string(p, static_cast<std::size_t>(sqlite3_column_bytes(s_, c))) : std::string();
    }

private:
    void check(int rc) {
        if (rc != SQLITE_OK) fail(db_, "bind");
    }
    sqlite3* db_;
    sqlite3_stmt* s_ = nullptr;
};

// Borrowed cached statement; resets it when the use ends so no read snapshot
// or lock outlives the call.
class StmtUse {
public:
    explicit StmtUse(Stmt& s) : s_(&s) {}
    StmtUse(const StmtUse&) = delete;
    StmtUse& operator=(const StmtUse&) = delete;
    ~StmtUse() { s_->reset(); }
    Stmt* operator->() const { return s_; }
    Stmt& operator*() const { return *s_; }

private:
    Stmt* s_;
};

void exec_sql(sqlite3* db, const char* sql) {
    char* err = nullptr;
    if (sqlite3_exec(db, sql, nullptr, nullptr, &err) != SQLITE_OK) {
        std::string msg = err ? err : sqlite3_errmsg(db);
        sqlite3_free(err);
        throw JournalError(std::format("journal: exec '{}': {}", sql, msg));
    }
}

// BEGIN IMMEDIATE ... COMMIT; rolls back if not committed.
class Tx {
public:
    explicit Tx(sqlite3* db) : db_(db) { exec_sql(db_, "BEGIN IMMEDIATE"); }
    Tx(const Tx&) = delete;
    Tx& operator=(const Tx&) = delete;
    ~Tx() {
        if (!done_) sqlite3_exec(db_, "ROLLBACK", nullptr, nullptr, nullptr);
    }
    void commit() {
        exec_sql(db_, "COMMIT");
        done_ = true;
    }

private:
    sqlite3* db_;
    bool done_ = false;
};

int attempt_state_int(AttemptState s) { return static_cast<int>(s); }
AttemptState attempt_state_from(std::int64_t v) {
    if (v < 0 || v > static_cast<int>(AttemptState::kAbandoned)) return AttemptState::kAbandoned;
    return static_cast<AttemptState>(v);
}

class SqliteJournal final : public IJournal {
public:
    explicit SqliteJournal(const std::filesystem::path& path) {
        std::error_code ec;
        if (path.has_parent_path()) {
            std::filesystem::create_directories(path.parent_path(), ec);
            if (ec) {
                throw JournalError(
                    std::format("journal: mkdir {}: {}", path.parent_path().string(), ec.message()));
            }
        }
        const int rc = sqlite3_open_v2(path.c_str(), &db_,
                                       SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_NOMUTEX, nullptr);
        if (rc != SQLITE_OK) {
            std::string msg = db_ ? sqlite3_errmsg(db_) : "out of memory";
            sqlite3_close(db_);
            db_ = nullptr;
            throw JournalError(std::format("journal: open {}: {}", path.string(), msg));
        }
        try {
            sqlite3_extended_result_codes(db_, 1);
            sqlite3_busy_timeout(db_, kBusyTimeoutMs);
            exec_sql(db_, "PRAGMA journal_mode=WAL");
            exec_sql(db_, "PRAGMA synchronous=FULL");
            exec_sql(db_, "PRAGMA foreign_keys=ON");
            migrate();
        } catch (...) {
            cache_.clear();
            sqlite3_close(db_);
            db_ = nullptr;
            throw;
        }
    }

    ~SqliteJournal() override {
        cache_.clear();  // finalize statements before closing
        if (db_) sqlite3_close(db_);
    }

    SqliteJournal(const SqliteJournal&) = delete;
    SqliteJournal& operator=(const SqliteJournal&) = delete;

    // ---- jobs ----------------------------------------------------------------

    void insert_job(const JobRecord& job) override {
        std::string spec;
        if (!job.spec.SerializeToString(&spec)) throw JournalError("journal: cannot serialize JobSpec");
        Tx tx(db_);
        {
            auto s = stmt(std::format("INSERT INTO jobs ({}) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, "
                                      "?11, ?12, ?13, ?14, ?15)",
                                      kJobColumns));
            s->bind(1, job.job_id).bind_blob(2, spec).bind(3, static_cast<int>(job.state));
            s->bind(4, job.attempts_used).bind(5, job.current_attempt_id).bind(6, job.last_error);
            s->bind(7, job.created_unix_ms).bind(8, job.updated_unix_ms);
            s->bind(9, job.progress_fraction).bind(10, job.progress_message);
            s->bind(11, spawn_to_json(job.spawn));
            s->bind(12, job.checkpoint.artifact_id()).bind(13, job.checkpoint.name());
            s->bind(14, job.checkpoint.size()).bind(15, job.checkpoint.media_type());
            s->exec();
        }
        write_outputs(job.job_id, job.outputs.empty() ? unnamed(job.output_artifacts) : job.outputs);
        tx.commit();
    }

    std::optional<JobRecord> get_job(const std::string& job_id) const override {
        std::optional<JobRecord> r;
        {
            auto s = stmt(std::format("SELECT {} FROM jobs WHERE job_id = ?1", kJobColumns));
            s->bind(1, job_id);
            if (s->step()) r = read_job(*s);
        }
        if (r) load_outputs(*r);
        return r;
    }

    std::vector<JobRecord> list_jobs(v1::JobState state, std::size_t limit) const override {
        std::vector<JobRecord> out;
        {
            const bool all = state == v1::JOB_STATE_UNSPECIFIED;
            auto s = stmt(all ? std::format("SELECT {} FROM jobs ORDER BY created_ms DESC, rowid DESC LIMIT ?1",
                                            kJobColumns)
                              : std::format("SELECT {} FROM jobs WHERE state = ?2 "
                                            "ORDER BY created_ms DESC, rowid DESC LIMIT ?1",
                                            kJobColumns));
            // limit 0 = no limit (SQLite: negative LIMIT means unbounded).
            s->bind(1, limit == 0 ? std::int64_t{-1} : static_cast<std::int64_t>(limit));
            if (!all) s->bind(2, static_cast<int>(state));
            while (s->step()) out.push_back(read_job(*s));
        }
        for (auto& j : out) load_outputs(j);
        return out;
    }

    std::vector<JobRecord> jobs_in_states(const std::vector<v1::JobState>& states) const override {
        std::vector<JobRecord> out;
        if (states.empty()) return out;
        std::string placeholders;
        for (std::size_t i = 0; i < states.size(); ++i) placeholders += i ? ",?" : "?";
        {
            auto s = stmt(std::format("SELECT {} FROM jobs WHERE state IN ({}) ORDER BY created_ms ASC, rowid ASC",
                                      kJobColumns, placeholders));
            for (std::size_t i = 0; i < states.size(); ++i) s->bind(static_cast<int>(i + 1), static_cast<int>(states[i]));
            while (s->step()) out.push_back(read_job(*s));
        }
        for (auto& j : out) load_outputs(j);
        return out;
    }

    std::size_t count_in_state(v1::JobState state) const override {
        auto s = stmt("SELECT COUNT(*) FROM jobs WHERE state = ?1");
        s->bind(1, static_cast<int>(state));
        s->step();
        return static_cast<std::size_t>(s->i64(0));
    }

    bool transition(const std::string& job_id, v1::JobState from, v1::JobState to,
                    const std::string& error) override {
        Tx tx(db_);
        {
            auto s = stmt(
                "UPDATE jobs SET state = ?3, updated_ms = ?4, "
                "last_error = CASE WHEN ?5 <> '' THEN ?5 ELSE last_error END "
                "WHERE job_id = ?1 AND state = ?2");
            s->bind(1, job_id).bind(2, static_cast<int>(from)).bind(3, static_cast<int>(to));
            s->bind(4, unix_ms()).bind(5, error);
            s->exec();
        }
        const bool changed = sqlite3_changes(db_) == 1;
        tx.commit();
        return changed;
    }

    void set_progress(const std::string& job_id, double fraction, const std::string& message) override {
        Tx tx(db_);
        {
            auto s = stmt(
                "UPDATE jobs SET progress_fraction = ?2, progress_message = ?3, updated_ms = ?4 WHERE job_id = ?1");
            s->bind(1, job_id).bind(2, fraction).bind(3, message).bind(4, unix_ms());
            s->exec();
        }
        tx.commit();  // unknown job: no-op
    }

    // ---- attempts ------------------------------------------------------------

    void begin_attempt(const AttemptRecord& a) override {
        Tx tx(db_);
        {
            auto s = stmt(std::format("INSERT INTO attempts ({}) VALUES "
                                      "(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
                                      kAttemptColumns));
            s->bind(1, a.attempt_id).bind(2, a.job_id).bind(3, a.number).bind(4, a.token);
            s->bind(5, attempt_state_int(a.state)).bind(6, a.pid).bind(7, a.pid_start_ticks);
            s->bind(8, a.cgroup_path).bind(9, a.work_dir).bind(10, a.exit_code).bind(11, a.exit_signal);
            s->bind(12, a.error).bind(13, a.started_unix_ms).bind(14, a.ended_unix_ms);
            s->exec();
        }
        {
            auto s = stmt(
                "UPDATE jobs SET current_attempt_id = ?2, attempts_used = attempts_used + 1, updated_ms = ?3 "
                "WHERE job_id = ?1");
            s->bind(1, a.job_id).bind(2, a.attempt_id).bind(3, unix_ms());
            s->exec();
        }
        if (sqlite3_changes(db_) != 1) throw JournalError("journal: begin_attempt: no such job " + a.job_id);
        tx.commit();
    }

    std::optional<AttemptRecord> get_attempt(const std::string& attempt_id) const override {
        auto s = stmt(std::format("SELECT {} FROM attempts WHERE attempt_id = ?1", kAttemptColumns));
        s->bind(1, attempt_id);
        if (!s->step()) return std::nullopt;
        return read_attempt(*s);
    }

    void update_attempt_process(const std::string& attempt_id, std::int32_t pid, std::uint64_t pid_start_ticks,
                                const std::string& cgroup_path) override {
        Tx tx(db_);
        {
            // A recorded process means the attempt is running.
            auto s = stmt(
                "UPDATE attempts SET pid = ?2, pid_start_ticks = ?3, cgroup_path = ?4, "
                "state = CASE WHEN state = ?5 THEN ?6 ELSE state END WHERE attempt_id = ?1");
            s->bind(1, attempt_id).bind(2, pid).bind(3, pid_start_ticks).bind(4, cgroup_path);
            s->bind(5, attempt_state_int(AttemptState::kStarting)).bind(6, attempt_state_int(AttemptState::kRunning));
            s->exec();
        }
        if (sqlite3_changes(db_) != 1) throw JournalError("journal: update_attempt_process: no such attempt " + attempt_id);
        tx.commit();
    }

    void finish_attempt(const std::string& attempt_id, AttemptState state, std::int32_t exit_code,
                        std::int32_t exit_signal, const std::string& error) override {
        Tx tx(db_);
        {
            auto s = stmt(
                "UPDATE attempts SET state = ?2, exit_code = ?3, exit_signal = ?4, error = ?5, ended_ms = ?6 "
                "WHERE attempt_id = ?1");
            s->bind(1, attempt_id).bind(2, attempt_state_int(state)).bind(3, exit_code).bind(4, exit_signal);
            s->bind(5, error).bind(6, unix_ms());
            s->exec();
        }
        if (sqlite3_changes(db_) != 1) throw JournalError("journal: finish_attempt: no such attempt " + attempt_id);
        tx.commit();
    }

    std::vector<AttemptRecord> open_attempts() const override {
        std::vector<AttemptRecord> out;
        auto s = stmt(std::format("SELECT {} FROM attempts WHERE state IN (?1, ?2) ORDER BY started_ms, rowid",
                                  kAttemptColumns));
        s->bind(1, attempt_state_int(AttemptState::kStarting)).bind(2, attempt_state_int(AttemptState::kRunning));
        while (s->step()) out.push_back(read_attempt(*s));
        return out;
    }

    // ---- commit (fencing) ----------------------------------------------------

    bool commit_outputs(const std::string& job_id, const std::string& attempt_id,
                        const std::vector<v1::NamedArtifact>& outputs,
                        const std::vector<v1::StepSpec>& spawn) override {
        const std::string spawn_json = spawn_to_json(spawn);
        Tx tx(db_);
        const std::int64_t now = unix_ms();
        {
            auto s = stmt(
                "UPDATE jobs SET state = ?4, updated_ms = ?5, spawn_json = ?6 "
                "WHERE job_id = ?1 AND state = ?2 AND current_attempt_id = ?3 AND ?3 <> ''");
            s->bind(1, job_id).bind(2, static_cast<int>(v1::COMMITTING)).bind(3, attempt_id);
            s->bind(4, static_cast<int>(v1::COMPLETED)).bind(5, now).bind(6, spawn_json);
            s->exec();
        }
        if (sqlite3_changes(db_) != 1) return false;  // Tx dtor rolls back (nothing changed anyway)
        write_outputs(job_id, outputs);
        {
            auto s = stmt(
                "UPDATE attempts SET state = ?2, ended_ms = CASE WHEN ended_ms = 0 THEN ?3 ELSE ended_ms END "
                "WHERE attempt_id = ?1");
            s->bind(1, attempt_id).bind(2, attempt_state_int(AttemptState::kSucceeded)).bind(3, now);
            s->exec();
        }
        tx.commit();
        return true;
    }

    // ---- checkpoints -----------------------------------------------------------

    void set_checkpoint(const std::string& job_id, const v1::NamedArtifact& c) override {
        Tx tx(db_);
        {
            auto s = stmt(
                "UPDATE jobs SET checkpoint_id = ?2, checkpoint_name = ?3, checkpoint_size = ?4, "
                "checkpoint_media_type = ?5, updated_ms = ?6 WHERE job_id = ?1");
            s->bind(1, job_id).bind(2, c.artifact_id()).bind(3, c.name()).bind(4, c.size());
            s->bind(5, c.media_type()).bind(6, unix_ms());
            s->exec();
        }
        tx.commit();  // unknown job: no-op
    }

    // ---- artifacts -----------------------------------------------------------

    void record_artifact(const ArtifactRow& r) override {
        Tx tx(db_);
        {
            auto s = stmt(
                "INSERT INTO artifacts (id, sha256_hex, size, media_type, producer_job, local_path, created_ms) "
                "VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7) "
                "ON CONFLICT(id) DO UPDATE SET sha256_hex = excluded.sha256_hex, size = excluded.size, "
                "media_type = excluded.media_type, producer_job = excluded.producer_job, "
                "local_path = excluded.local_path, created_ms = excluded.created_ms");
            s->bind(1, r.id).bind(2, r.sha256_hex).bind(3, r.size).bind(4, r.media_type);
            s->bind(5, r.producer_job).bind(6, r.local_path).bind(7, r.created_unix_ms);
            s->exec();
        }
        tx.commit();
    }

    std::optional<ArtifactRow> get_artifact(const std::string& id) const override {
        auto s = stmt(
            "SELECT id, sha256_hex, size, media_type, producer_job, local_path, created_ms FROM artifacts "
            "WHERE id = ?1");
        s->bind(1, id);
        if (!s->step()) return std::nullopt;
        ArtifactRow r;
        r.id = s->text(0);
        r.sha256_hex = s->text(1);
        r.size = static_cast<std::uint64_t>(s->i64(2));
        r.media_type = s->text(3);
        r.producer_job = s->text(4);
        r.local_path = s->text(5);
        r.created_unix_ms = s->i64(6);
        return r;
    }

    // ---- node counters -------------------------------------------------------

    std::uint64_t failed_job_count() const override {
        auto s = stmt("SELECT COUNT(*) FROM jobs WHERE state IN (?1, ?2, ?3)");
        s->bind(1, static_cast<int>(v1::FAILED)).bind(2, static_cast<int>(v1::EXPIRED));
        s->bind(3, static_cast<int>(v1::VALIDATION_FAILED));
        s->step();
        return static_cast<std::uint64_t>(s->i64(0));
    }

private:
    StmtUse stmt(const std::string& sql) const {
        auto it = cache_.find(sql);
        if (it == cache_.end()) it = cache_.try_emplace(sql, db_, sql).first;
        return StmtUse(it->second);
    }

    void migrate() {
        int version = 0;
        {
            auto s = stmt("PRAGMA user_version");
            if (s->step()) version = static_cast<int>(s->i64(0));
        }
        if (version == kSchemaVersion) return;
        if (version > kSchemaVersion) {
            throw JournalError(std::format("journal: schema version {} is newer than supported {}", version,
                                           kSchemaVersion));
        }
        Tx tx(db_);
        if (version < 1) exec_sql(db_, kSchemaV1);
        if (version < 2) exec_sql(db_, kSchemaV2);
        exec_sql(db_, std::format("PRAGMA user_version = {}", kSchemaVersion).c_str());
        tx.commit();
    }

    void write_outputs(const std::string& job_id, const std::vector<v1::NamedArtifact>& outs) {
        {
            auto d = stmt("DELETE FROM job_outputs WHERE job_id = ?1");
            d->bind(1, job_id);
            d->exec();
        }
        for (std::size_t i = 0; i < outs.size(); ++i) {
            auto s = stmt(
                "INSERT INTO job_outputs (job_id, idx, artifact_id, name, size, media_type) "
                "VALUES (?1, ?2, ?3, ?4, ?5, ?6)");
            s->bind(1, job_id).bind(2, static_cast<std::int64_t>(i)).bind(3, outs[i].artifact_id());
            s->bind(4, outs[i].name()).bind(5, outs[i].size()).bind(6, outs[i].media_type());
            s->exec();
        }
    }

    // outputs = every row in order; output_artifacts = their distinct ids.
    void load_outputs(JobRecord& j) const {
        j.output_artifacts.clear();
        j.outputs.clear();
        std::set<std::string> seen;
        auto s = stmt("SELECT artifact_id, name, size, media_type FROM job_outputs WHERE job_id = ?1 ORDER BY idx");
        s->bind(1, j.job_id);
        while (s->step()) {
            v1::NamedArtifact a;
            a.set_artifact_id(s->text(0));
            a.set_name(s->text(1));
            a.set_size(static_cast<std::uint64_t>(s->i64(2)));
            a.set_media_type(s->text(3));
            if (seen.insert(a.artifact_id()).second) j.output_artifacts.push_back(a.artifact_id());
            j.outputs.push_back(std::move(a));
        }
    }

    static JobRecord read_job(const Stmt& s) {
        JobRecord j;
        j.job_id = s.text(0);
        const std::string spec = s.blob(1);
        if (!j.spec.ParseFromString(spec)) throw JournalError("journal: corrupt JobSpec for job " + j.job_id);
        j.state = static_cast<v1::JobState>(s.i64(2));
        j.attempts_used = static_cast<std::uint32_t>(s.i64(3));
        j.current_attempt_id = s.text(4);
        j.last_error = s.text(5);
        j.created_unix_ms = s.i64(6);
        j.updated_unix_ms = s.i64(7);
        j.progress_fraction = s.dbl(8);
        j.progress_message = s.text(9);
        j.spawn = spawn_from_json(j.job_id, s.text(10));
        j.checkpoint.set_artifact_id(s.text(11));
        j.checkpoint.set_name(s.text(12));
        j.checkpoint.set_size(static_cast<std::uint64_t>(s.i64(13)));
        j.checkpoint.set_media_type(s.text(14));
        return j;
    }

    static AttemptRecord read_attempt(const Stmt& s) {
        AttemptRecord a;
        a.attempt_id = s.text(0);
        a.job_id = s.text(1);
        a.number = static_cast<std::uint32_t>(s.i64(2));
        a.token = s.text(3);
        a.state = attempt_state_from(s.i64(4));
        a.pid = static_cast<std::int32_t>(s.i64(5));
        a.pid_start_ticks = static_cast<std::uint64_t>(s.i64(6));
        a.cgroup_path = s.text(7);
        a.work_dir = s.text(8);
        a.exit_code = static_cast<std::int32_t>(s.i64(9));
        a.exit_signal = static_cast<std::int32_t>(s.i64(10));
        a.error = s.text(11);
        a.started_unix_ms = s.i64(12);
        a.ended_unix_ms = s.i64(13);
        return a;
    }

    sqlite3* db_ = nullptr;
    mutable std::map<std::string, Stmt> cache_;
};

}  // namespace

std::unique_ptr<IJournal> open_sqlite_journal(const std::filesystem::path& path) {
    return std::make_unique<SqliteJournal>(path);
}

const char* to_string(AttemptState s) {
    switch (s) {
        case AttemptState::kStarting: return "STARTING";
        case AttemptState::kRunning: return "RUNNING";
        case AttemptState::kSucceeded: return "SUCCEEDED";
        case AttemptState::kFailed: return "FAILED";
        case AttemptState::kCancelled: return "CANCELLED";
        case AttemptState::kAbandoned: return "ABANDONED";
    }
    return "UNKNOWN";
}

}  // namespace tdr::journal
