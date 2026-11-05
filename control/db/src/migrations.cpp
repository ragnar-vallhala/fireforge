// Schema migrations (docs/design/phase2.md §6). Append only: never edit a
// migration that has shipped; add a new one.
#include <array>
#include <string_view>

#include "pg.hpp"

namespace tdr::cpdb::pg {

namespace {

// Arbitrary constant: pg_advisory_xact_lock key for "fireforge schema".
constexpr std::int64_t kMigrationLock = 0x4646'5343'4845'4d41;  // "FFSCHEMA"

constexpr std::array<std::string_view, 1> kMigrations = {
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
