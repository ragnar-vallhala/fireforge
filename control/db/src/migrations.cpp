// Schema migrations (docs/design/phase2.md §6). Append only: never edit a
// migration that has shipped; add a new one.
#include <array>
#include <string_view>

#include "pg.hpp"

namespace tdr::cpdb::pg {

namespace {

// Arbitrary constant: pg_advisory_xact_lock key for "fireforge schema".
constexpr std::int64_t kMigrationLock = 0x4646'5343'4845'4d41;  // "FFSCHEMA"

constexpr std::array<std::string_view, 6> kMigrations = {
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
    // 3: Phase 5 placement and retention (docs/design/phase5.md §4, §6)
    R"sql(
-- Set whenever a job enters QUEUED (submit or requeue); locality_wait counts from it.
ALTER TABLE jobs ADD COLUMN queued_at timestamptz;
UPDATE jobs SET queued_at = created_at WHERE state = 'QUEUED';

-- Pinned artifacts are never released (without force). No FK: an artifact may
-- be pinned before (or after) any node holds it.
CREATE TABLE artifact_pins (
    artifact_id text PRIMARY KEY,
    pinned_by   text NOT NULL DEFAULT '',
    pinned_at   timestamptz NOT NULL
);
)sql",
    // 4: Phase 6 workflows, checkpoints, ranked placement (docs/design/phase6.md §3-§7)
    R"sql(
CREATE TABLE workflows (
    seq          bigserial NOT NULL UNIQUE,
    workflow_id  text PRIMARY KEY,
    name         text NOT NULL DEFAULT '',
    campaign_id  text REFERENCES campaigns (campaign_id),
    state        text NOT NULL CHECK (state IN ('RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELLED')),
    submitted_by text NOT NULL DEFAULT '',
    error        text NOT NULL DEFAULT '',          -- first failing step (FAILED)
    step_count   int NOT NULL DEFAULT 0,            -- static + spawned
    open_steps   int NOT NULL DEFAULT 0,            -- steps not yet terminal
    created_at   timestamptz NOT NULL,
    updated_at   timestamptz NOT NULL,
    finished_at  timestamptz
);
CREATE INDEX workflows_created_idx ON workflows (created_at DESC, seq DESC);
CREATE INDEX workflows_running_idx ON workflows (created_at, seq) WHERE state = 'RUNNING';

-- Names are byte-ordered (COLLATE "C") so the spawned subtree of X is the
-- range ['X/', 'X0') of the primary key.
CREATE TABLE workflow_steps (
    workflow_id text NOT NULL REFERENCES workflows (workflow_id) ON DELETE CASCADE,
    name        text COLLATE "C" NOT NULL,
    seq         int NOT NULL,                       -- creation order within the workflow
    parent_step text COLLATE "C",                   -- spawning step (NULL = static)
    depth       int NOT NULL DEFAULT 0,
    spec        jsonb NOT NULL,                     -- fireforge.tdr.v1.StepSpec (proto3 JSON, full names)
    state       text NOT NULL CHECK (state IN ('WAITING', 'ACTIVE', 'SKIPPED', 'CANCELLED', 'COMPLETED', 'FAILED',
                                               'EXPIRED', 'VALIDATION_FAILED')),
    job_id      text,
    error       text NOT NULL DEFAULT '',
    open_desc   int NOT NULL DEFAULT 0,             -- spawned descendants not yet terminal
    updated_at  timestamptz NOT NULL,
    PRIMARY KEY (workflow_id, name),
    UNIQUE (workflow_id, seq)
);
CREATE INDEX workflow_steps_state_idx ON workflow_steps (workflow_id, state);

CREATE TABLE workflow_step_deps (
    workflow_id text NOT NULL,
    step        text COLLATE "C" NOT NULL,
    dep         text COLLATE "C" NOT NULL,
    PRIMARY KEY (workflow_id, step, dep),
    FOREIGN KEY (workflow_id, step) REFERENCES workflow_steps (workflow_id, name) ON DELETE CASCADE
);
CREATE INDEX workflow_step_deps_dep_idx ON workflow_step_deps (workflow_id, dep);

ALTER TABLE jobs
    ADD COLUMN workflow_id    text REFERENCES workflows (workflow_id),
    ADD COLUMN step           text,
    ADD COLUMN checkpoint     jsonb,                           -- latest checkpoint (ArtifactRef)
    ADD COLUMN strict         boolean NOT NULL DEFAULT false,  -- needs ranked placement (§7)
    ADD COLUMN reserved_node  text,
    ADD COLUMN reserved_until timestamptz,
    ADD COLUMN reserve_tried  text[] NOT NULL DEFAULT '{}',
    ADD COLUMN reserve_done   boolean NOT NULL DEFAULT false;  -- released to every node
CREATE INDEX jobs_workflow_idx ON jobs (workflow_id) WHERE workflow_id IS NOT NULL;
CREATE INDEX jobs_placement_idx ON jobs (created_at, seq) WHERE state = 'QUEUED' AND strict AND NOT reserve_done;
)sql",
    // 5: Phase 7 gangs: multi-node MPI jobs (docs/design/phase7.md §3, §4)
    R"sql(
ALTER TABLE jobs
    ADD COLUMN is_gang        boolean NOT NULL DEFAULT false,  -- spec.mpi.ranks > 0
    ADD COLUMN pending_reason text NOT NULL DEFAULT '',        -- why a QUEUED gang isn't running
    ADD COLUMN gang_tried     text[] NOT NULL DEFAULT '{}';    -- members of holds that timed out
CREATE INDEX jobs_gang_queue_idx ON jobs (created_at, seq) WHERE state = 'QUEUED' AND is_gang;

-- Last lease_jobs call (gang eligibility: the node takes remote work).
ALTER TABLE nodes ADD COLUMN last_lease_poll timestamptz;

-- Capacity held (and drained) for a QUEUED gang job, one row per member node.
CREATE TABLE gang_holds (
    job_id       text NOT NULL REFERENCES jobs (job_id) ON DELETE CASCADE,
    node_id      text NOT NULL REFERENCES nodes (node_id) ON DELETE CASCADE,
    member_index int NOT NULL CHECK (member_index >= 0),       -- 0 = head
    rank_offset  int NOT NULL CHECK (rank_offset >= 0),
    ranks        int NOT NULL CHECK (ranks >= 1),
    cpu          bigint NOT NULL CHECK (cpu >= 0),             -- ranks x per-rank request
    memory       numeric(20, 0) NOT NULL CHECK (memory >= 0),
    gpus         bigint NOT NULL CHECK (gpus >= 0),
    ready_at     timestamptz,                                  -- last poll with free >= hold
    created_at   timestamptz NOT NULL,
    PRIMARY KEY (job_id, member_index),
    UNIQUE (job_id, node_id)
);
CREATE INDEX gang_holds_node_idx ON gang_holds (node_id);

-- Members of a gang attempt (one job_attempts row; node_id there is the head).
CREATE TABLE job_attempt_members (
    attempt_id   text NOT NULL REFERENCES job_attempts (attempt_id) ON DELETE CASCADE,
    node_id      text NOT NULL,
    member_index int NOT NULL CHECK (member_index >= 0),
    rank_offset  int NOT NULL CHECK (rank_offset >= 0),
    ranks        int NOT NULL CHECK (ranks >= 1),
    role         text NOT NULL CHECK (role IN ('HEAD', 'MEMBER')),
    state        text NOT NULL CHECK (state IN ('LEASED', 'RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELLED', 'ABANDONED')),
    delivered    boolean NOT NULL DEFAULT false,               -- lease handed to the member
    expires_at   timestamptz NOT NULL,
    error        text NOT NULL DEFAULT '',
    PRIMARY KEY (attempt_id, member_index),
    UNIQUE (attempt_id, node_id)
);
CREATE INDEX job_attempt_members_live_idx ON job_attempt_members (expires_at) WHERE state IN ('LEASED', 'RUNNING');
CREATE INDEX job_attempt_members_undelivered_idx ON job_attempt_members (node_id) WHERE NOT delivered;
)sql",
    // 6: materials database (docs/design/phase8.md §2-§3)
    R"sql(
CREATE TABLE materials (
    material_id     text PRIMARY KEY,
    reduced_formula text NOT NULL UNIQUE,
    elements        text[] NOT NULL DEFAULT '{}',              -- sorted, unique
    nsites_min      int NOT NULL DEFAULT 0 CHECK (nsites_min >= 0),
    created_at      timestamptz NOT NULL
);
CREATE INDEX materials_elements_idx ON materials USING gin (elements);

CREATE TABLE structures (
    seq                 bigserial NOT NULL UNIQUE,
    structure_id        text PRIMARY KEY,
    material_id         text NOT NULL REFERENCES materials (material_id),
    fingerprint         text NOT NULL UNIQUE,                  -- "fp1:..." (phase8.md §3)
    structure           jsonb NOT NULL,                        -- tdr.v1.Structure (proto3 JSON)
    spacegroup_number   int NOT NULL DEFAULT 0,
    prototype           text NOT NULL DEFAULT '',
    source_job_id       text,
    parent_structure_id text,
    created_at          timestamptz NOT NULL
);
CREATE INDEX structures_material_idx ON structures (material_id, seq);

CREATE TABLE calculations (
    seq              bigserial NOT NULL UNIQUE,                -- newest-first order and paging
    calc_id          text PRIMARY KEY,
    structure_id     text NOT NULL REFERENCES structures (structure_id),
    material_id      text NOT NULL REFERENCES materials (material_id),
    kind             text NOT NULL,
    properties       jsonb NOT NULL DEFAULT '{}',
    conditions       jsonb NOT NULL DEFAULT '{}',
    method           jsonb NOT NULL DEFAULT '{}',              -- tdr.v1.Method (proto3 JSON)
    convergence      jsonb NOT NULL DEFAULT '{}',
    uncertainty      jsonb NOT NULL DEFAULT '{}',
    evidence         text NOT NULL CHECK (evidence IN ('PREDICTED', 'LITERATURE', 'MEASURED')),
    output_artifacts jsonb NOT NULL DEFAULT '{}',              -- output name -> artifact id
    input_artifacts  text[] NOT NULL DEFAULT '{}',
    job_id           text REFERENCES jobs (job_id),            -- NULL for imports
    attempt_id       text,
    workflow_id      text,
    step             text COLLATE "C",
    campaign_id      text REFERENCES campaigns (campaign_id),
    node_id          text,
    note             text NOT NULL DEFAULT '',
    created_at       timestamptz NOT NULL
);
CREATE INDEX calculations_material_kind_idx ON calculations (material_id, kind, seq DESC);
CREATE INDEX calculations_campaign_kind_idx ON calculations (campaign_id, kind, seq DESC);
CREATE INDEX calculations_workflow_idx ON calculations (workflow_id, seq DESC);
CREATE INDEX calculations_structure_idx ON calculations (structure_id, seq DESC);
CREATE INDEX calculations_job_idx ON calculations (job_id);
CREATE INDEX calculations_properties_idx ON calculations USING gin (properties);
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
