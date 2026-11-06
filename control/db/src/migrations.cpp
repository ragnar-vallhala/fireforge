// Schema migrations (docs/design/phase2.md §6). Append only: never edit a
// migration that has shipped; add a new one.
#include <array>
#include <string_view>

#include "pg.hpp"

namespace tdr::cpdb::pg {

namespace {

// Arbitrary constant: pg_advisory_xact_lock key for "fireforge schema".
constexpr std::int64_t kMigrationLock = 0x4646'5343'4845'4d41;  // "FFSCHEMA"

constexpr std::array<std::string_view, 2> kMigrations = {
    // 1: initial control-plane schema
    R"sql(
CREATE TABLE nodes (
    node_id          text PRIMARY KEY,
    name             text NOT NULL UNIQUE,
    health           text NOT NULL CHECK (health IN ('ONLINE', 'STALE', 'OFFLINE')),
    addresses        jsonb NOT NULL DEFAULT '[]',
    capabilities     jsonb NOT NULL DEFAULT '[]',
    daemon_version   text NOT NULL DEFAULT '',
    physical         jsonb NOT NULL DEFAULT '{}',
    configured       jsonb NOT NULL DEFAULT '{}',
    cert_fingerprint text NOT NULL DEFAULT '',
    registered_at    timestamptz NOT NULL,
    last_seen        timestamptz NOT NULL,
    offline_reason   text NOT NULL DEFAULT '',
    last_heartbeat   jsonb
);
CREATE INDEX nodes_health_idx ON nodes (health, last_seen);

CREATE TABLE node_policies (
    node_id          text PRIMARY KEY REFERENCES nodes (node_id) ON DELETE CASCADE,
    version          bigint NOT NULL DEFAULT 1 CHECK (version >= 1),
    profile          text NOT NULL DEFAULT '',
    max_cpu_cores    bigint NOT NULL DEFAULT 0 CHECK (max_cpu_cores >= 0),
    max_memory_bytes numeric(20, 0) NOT NULL DEFAULT 0 CHECK (max_memory_bytes >= 0),
    max_jobs         bigint NOT NULL DEFAULT 0 CHECK (max_jobs >= 0),
    max_disk_bytes   numeric(20, 0) NOT NULL DEFAULT 0 CHECK (max_disk_bytes >= 0),
    max_gpus         bigint NOT NULL DEFAULT 0 CHECK (max_gpus >= 0),
    updated_at       timestamptz NOT NULL,
    updated_by       text NOT NULL DEFAULT ''
);

CREATE TABLE node_events (
    id          bigserial PRIMARY KEY,
    node_id     text NOT NULL REFERENCES nodes (node_id) ON DELETE CASCADE,
    at          timestamptz NOT NULL,
    from_health text CHECK (from_health IN ('ONLINE', 'STALE', 'OFFLINE')),
    to_health   text NOT NULL CHECK (to_health IN ('ONLINE', 'STALE', 'OFFLINE')),
    reason      text NOT NULL DEFAULT ''
);
CREATE INDEX node_events_node_at_idx ON node_events (node_id, at DESC);

CREATE TABLE campaigns (
    campaign_id text PRIMARY KEY,
    name        text NOT NULL UNIQUE,
    description text NOT NULL DEFAULT '',
    domain      text NOT NULL DEFAULT '',
    state       text NOT NULL CHECK (state IN ('DRAFT', 'ACTIVE', 'PAUSED', 'COMPLETED', 'ARCHIVED')),
    parameters  jsonb NOT NULL DEFAULT '{}',
    created_by  text NOT NULL DEFAULT '',
    created_at  timestamptz NOT NULL,
    updated_at  timestamptz NOT NULL
);
CREATE INDEX campaigns_state_idx ON campaigns (state);
CREATE INDEX campaigns_created_idx ON campaigns (created_at DESC);

CREATE TABLE audit_log (
    id      bigserial PRIMARY KEY,
    at      timestamptz NOT NULL,
    actor   text NOT NULL,
    action  text NOT NULL,
    target  text NOT NULL DEFAULT '',
    details jsonb NOT NULL DEFAULT '{}'
);
CREATE INDEX audit_log_at_idx ON audit_log (at DESC);
)sql",
    // 2: central job queue, leases, artifact locations (docs/design/phase4.md §2-§4)
    R"sql(
CREATE TABLE jobs (
    seq                   bigserial NOT NULL UNIQUE,
    job_id                text PRIMARY KEY,
    state                 text NOT NULL CHECK (state IN ('QUEUED', 'LEASED', 'RUNNING', 'RETRY_PENDING', 'COMPLETED',
                                                         'FAILED', 'CANCELLED', 'EXPIRED', 'VALIDATION_FAILED')),
    spec                  bytea NOT NULL,                -- serialized fireforge.tdr.v1.JobSpec
    campaign_id           text REFERENCES campaigns (campaign_id),
    cpu_cores             bigint NOT NULL DEFAULT 1 CHECK (cpu_cores >= 1),  -- max(1, spec)
    memory_bytes          numeric(20, 0) NOT NULL DEFAULT 0 CHECK (memory_bytes >= 0),
    gpu_count             bigint NOT NULL DEFAULT 0 CHECK (gpu_count >= 0),
    required_capabilities jsonb NOT NULL DEFAULT '[]',
    input_artifacts       jsonb NOT NULL DEFAULT '[]',
    max_attempts          int NOT NULL CHECK (max_attempts >= 1),
    attempts_used         int NOT NULL DEFAULT 0 CHECK (attempts_used >= 0),
    current_attempt_id    text,
    node_id               text,
    cancel_requested      boolean NOT NULL DEFAULT false,
    submitted_by          text NOT NULL DEFAULT '',
    last_error            text NOT NULL DEFAULT '',
    progress_fraction     double precision NOT NULL DEFAULT 0,
    progress_message      text NOT NULL DEFAULT '',
    outputs               jsonb NOT NULL DEFAULT '[]',   -- [ArtifactRef] (proto3 JSON)
    created_at            timestamptz NOT NULL,
    updated_at            timestamptz NOT NULL
);
CREATE INDEX jobs_queue_idx ON jobs (created_at, seq) WHERE state = 'QUEUED';
CREATE INDEX jobs_state_idx ON jobs (state, created_at DESC);
CREATE INDEX jobs_campaign_idx ON jobs (campaign_id, created_at DESC);
CREATE INDEX jobs_created_idx ON jobs (created_at DESC, seq DESC);
CREATE INDEX jobs_current_attempt_idx ON jobs (current_attempt_id);
CREATE INDEX jobs_node_idx ON jobs (node_id) WHERE state IN ('LEASED', 'RUNNING');

CREATE TABLE job_attempts (
    attempt_id    text PRIMARY KEY,
    job_id        text NOT NULL REFERENCES jobs (job_id) ON DELETE CASCADE,
    number        int NOT NULL CHECK (number >= 1),
    fencing_token bigint NOT NULL,
    node_id       text NOT NULL,
    state         text NOT NULL CHECK (state IN ('LEASED', 'RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELLED', 'ABANDONED')),
    error         text NOT NULL DEFAULT '',
    leased_at     timestamptz NOT NULL,
    expires_at    timestamptz NOT NULL,
    ended_at      timestamptz,
    UNIQUE (job_id, number)
);
CREATE INDEX job_attempts_live_idx ON job_attempts (expires_at) WHERE state IN ('LEASED', 'RUNNING');

CREATE TABLE artifact_locations (
    artifact_id  text NOT NULL,
    node_id      text NOT NULL REFERENCES nodes (node_id) ON DELETE CASCADE,
    size         numeric(20, 0) NOT NULL DEFAULT 0,
    media_type   text NOT NULL DEFAULT '',
    producer_job text NOT NULL DEFAULT '',
    created_at   timestamptz NOT NULL,
    seen_at      timestamptz NOT NULL,       -- last announcement / commit
    sync_id      text NOT NULL DEFAULT '',   -- full sync that last listed it ('' = incremental)
    PRIMARY KEY (artifact_id, node_id)
);
CREATE INDEX artifact_locations_node_idx ON artifact_locations (node_id);

-- Full artifact sync in progress per node (ArtifactAnnouncement.sync_id).
CREATE TABLE artifact_syncs (
    node_id    text PRIMARY KEY REFERENCES nodes (node_id) ON DELETE CASCADE,
    sync_id    text NOT NULL,
    started_at timestamptz NOT NULL
);
)sql",
};

int read_version(Conn& c) {
    auto r = c.exec("SELECT coalesce(max(version), 0) FROM schema_migrations");
    return static_cast<int>(r.i64(0, 0));
}

}  // namespace

int latest_version() { return static_cast<int>(kMigrations.size()); }

void migrate(Conn& c) {
    for (;;) {
        bool done = transact(c, [&] {
            // Serializes concurrent tdrcp starts; released at COMMIT/ROLLBACK.
            c.exec("SELECT pg_advisory_xact_lock($1::bigint)", kMigrationLock);
            c.exec_script(
                "CREATE TABLE IF NOT EXISTS schema_migrations ("
                " version int PRIMARY KEY,"
                " applied_at timestamptz NOT NULL DEFAULT now())");
            int v = read_version(c);
            if (v > latest_version())
                throw DbError("database schema version " + std::to_string(v) + " is newer than this tdrcp (" +
                              std::to_string(latest_version()) + ")");
            if (v == latest_version()) return true;
            c.exec_script(kMigrations[static_cast<std::size_t>(v)]);
            c.exec("INSERT INTO schema_migrations (version, applied_at) VALUES ($1::int, now())", v + 1);
            return false;
        });
        if (done) return;
    }
}

int current_version(Conn& c) {
    auto r = c.exec("SELECT to_regclass('schema_migrations') IS NOT NULL");
    if (r.str(0, 0) != "t") return 0;
    return read_version(c);
}

}  // namespace tdr::cpdb::pg
