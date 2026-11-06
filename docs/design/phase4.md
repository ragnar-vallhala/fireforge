# Phase 4 — Distributed Execution: Central Queue, Leases, Remote Jobs

Status: design, v0.1 (2026-11-05). Builds on Phases 1–3.

Goal: jobs submitted to the control plane run on any eligible node. Nodes
**pull** leases over HTTPS when they have spare capacity. A lease is
renewed by heartbeats and guarded by a fencing token. When a node dies or goes
silent, its lease expires and the job is retried elsewhere. A stale node can
never commit a result.

Decisions (2026-11-05):
- central Postgres queue in tdrcp, HTTPS pull (§21 #3, pull first)
- jobs with input artifacts are only leased to nodes that already hold all of them (Phase 5 lifts this)
- `tdrctl submit` stays local by default; `--remote` submits to the central queue through the local daemon

---

## 1. Components

```
 tdrctl submit --remote ─► tdrd (ctl socket) ─► NodeAgent client ─► POST /api/v1/jobs
 tdrctl cp job-* (admin cert) ─────────────────────────────────────► /api/v1/jobs...

 tdrcp: jobs, job_attempts, artifact_locations (Postgres)
   POST /api/v1/leases ◄── RemoteExecutor (tdrd) pulls when spare capacity
   heartbeat: leases renewed / revoked
   POST /api/v1/jobs/{id}/status ◄── RemoteExecutor reports RUNNING / outcome (fenced)
   lease sweeper: expired -> retry or fail

 tdrd: RemoteExecutor (runtime/remote)
   lease -> JobManager::submit (local journal, max_attempts = 1)
   JobManager state listener -> status updates (persisted outbox, retried)
   heartbeat reply "revoked" -> JobManager::cancel
```

| Module | Path | Target | Owns |
|---|---|---|---|
| cpdb (extend) | `control/db` | `tdr_cpdb` | migration 2, job/lease/artifact repository methods |
| cpapi (extend) | `control/api` | `tdr_cpapi` | job, lease, status, artifact endpoints; lease sweep |
| agent (extend) | `runtime/agent` | `tdr_agent` | client calls for jobs/leases/status/artifacts; heartbeat lease hook |
| remote | `runtime/remote` | `tdr_remote` | RemoteExecutor: pull loop, lease ↔ local job, outbox, recovery |

## 2. Job model on the control plane

Central job states reuse `fireforge.tdr.v1.JobState`:

```
QUEUED ──lease──► LEASED ──node RUNNING──► RUNNING ──► COMPLETED
  ▲                 │                        │      ├─► FAILED (non-retryable / attempts exhausted)
  │                 │                        │      ├─► EXPIRED (job timeout, terminal)
  └── RETRY_PENDING ◄┴──── lease expired / retryable failure ─┘ └─► VALIDATION_FAILED
 any non-terminal ──cancel──► CANCELLED (LEASED/RUNNING: revoked via heartbeat first)
```

- Submission validates the spec: non-empty `command.argv`, `campaign_id` exists
  if set, and `policy.max_attempts` ≥ 1 (default 3). `job_id` is assigned when
  empty and must be unique. The state goes straight to QUEUED.
- Each lease creates an **attempt** (`attempt_id`, `fencing_token` = a
  per-job counter that increases by one with every attempt, `node_id`,
  `expires_at = now + lease_ttl`) and sets `jobs.current_attempt_id`.
- **Fencing.** Every status update carries `(attempt_id, fencing_token)`, and the
  control plane accepts it only when both match the job's current attempt and the job is
  LEASED/RUNNING. Anything else gets **409 stale** and the node drops that work. COMPLETED is
  one transaction: attempt `SUCCEEDED`, job COMPLETED, outputs and
  artifact locations recorded.
- **Retry.** If a FAILED with `retryable` or an expired lease arrives while `attempts_used < max_attempts`,
  the job goes RETRY_PENDING → QUEUED (immediately; backoff later). Otherwise it
  becomes FAILED with the last error. EXPIRED and VALIDATION_FAILED from the node are terminal.
- **Cancel** (`POST /jobs/{id}/cancel`): QUEUED/RETRY_PENDING → CANCELLED at once.
  LEASED/RUNNING sets `cancel_requested`. The attempt is listed in the next
  heartbeat reply's `revoked`, the node cancels it, and its CANCELLED report
  finishes the job. If the lease expires first, the job becomes CANCELLED (not retried).

## 3. Leasing (pull)

`POST /api/v1/leases` (node) with `LeaseRequest{node_id, free, max_jobs}`, where
`free` is the node's spare capacity: effective caps − allocated (including queued local work).

Eligibility, checked by the control plane in one transaction (`FOR UPDATE SKIP LOCKED`,
oldest first, up to 200 candidates scanned):
1. Job QUEUED and its campaign (if any) is ACTIVE. DRAFT and PAUSED campaigns hold their jobs.
2. `resources` fit the remaining free cpu and memory, and gpus ≤ free gpus (0 = none needed).
3. `required_capabilities` ⊆ the node's registered capabilities.
4. **Locality:** every `input_artifacts` id has a location on this node.
5. The node is ONLINE and not draining (OFFLINE nodes get none).

Jobs are granted greedily until `free` or `max_jobs` runs out. The reply is a list of `Lease`
entries `{job_id, attempt_id, fencing_token, attempt_number, spec, expires_unix_ms}`
plus `lease_ttl_seconds`. An empty list is normal.

**Renewal.** `Heartbeat.leases` lists the node's active `(job_id, attempt_id,
fencing_token)`. The control plane extends each one that is still current and returns
`HeartbeatReply.revoked` for the rest: cancelled, fenced or unknown. The node
cancels those locally and discards their results.

**Expiry sweep** (tdrcp timer, every heartbeat interval): for attempts LEASED or
RUNNING with `expires_at < now`, mark the attempt ABANDONED (reason `lease expired`)
and apply the retry rule. **Startup grace:** for `lease_ttl` after tdrcp starts,
nothing expires, so nodes have time to renew after a control-plane outage.

Defaults: `lease_ttl` 60 s (≥ 3 heartbeats), node poll 2 s while it has spare
capacity, backing off to 10 s while it gets empty replies.

## 4. Artifact locations (metadata only, §13)

- `artifact_locations(artifact_id, node_id, size, media_type, producer_job,
  created_at)`, with primary key (artifact_id, node_id).
- Nodes announce artifacts with `POST /api/v1/artifacts/announce`
  (`ArtifactAnnouncement{node_id, artifacts, full}`):
  - on registration: everything in the local store, in pages of 1000, with `full` on the last page so stale rows for that node are dropped
  - after every local commit: the new outputs
- Remote COMPLETED updates also record their outputs.
- `GET /api/v1/artifacts/sha256/{hex}` returns `ArtifactInfo{artifact, node_ids}`.
  No payloads pass through the control plane.

## 5. Node side: RemoteExecutor (runtime/remote)

- **Pull loop.** It runs while the agent is `connected` and `[control] remote_jobs = on`
  (the default). When no local job is queued and a job slot is free, it sends POST /leases
  with `free` = effective caps − allocated (GPUs: 0 until GPU accounting exists, so GPU
  jobs are not leased in Phase 4). Every lease
  becomes a local job (`JobManager::submit`) with:
  - local `job_id` = `<central id>.a<attempt number>` (a node may get a later
    attempt of the same job; `tdrctl status` shows the central id too)
  - `policy.max_attempts` = 1 (the control plane owns retries)
  - the spec otherwise unchanged

  A local submit that fails (inputs missing, too big) is reported at once as
  FAILED, retryable when the cause is capacity and non-retryable when it is validation.
- **Mapping and outbox.** `job_id → {attempt_id, fencing_token}` and the pending status updates
  live in `state/remote.json`, written atomically. They survive a tdrd restart.
- **Status reports.** The JobManager state listener turns RUNNING into a RUNNING update.
  Every terminal state becomes an outcome update:
  - COMPLETED carries the output artifact refs from the store and journal
  - FAILED is retryable
  - EXPIRED, CANCELLED and VALIDATION_FAILED are terminal

  Updates go out in order per job and are retried with backoff while the control plane is unreachable.
  A 409 response means stale: the update is dropped and logged, and the local outputs stay in the store.
- **Heartbeat.** Each heartbeat carries the active leases. For every lease in `revoked`, the executor
  calls `JobManager::cancel`. The resulting CANCELLED is still reported: it finishes a
  cancel-requested job, and for a fenced lease it just gets 409 and is dropped.
- **Restart.** `JobManager::recover()` fails leased jobs whose process died,
  because `max_attempts` = 1. The executor then reports FAILED (retryable), and the control plane retries.
- **Outage.** While the control plane is down, running leased jobs continue and their outcomes wait
  in the outbox. If the lease expired in the meantime, the update gets 409 and that work is lost,
  but the job has already been retried elsewhere. That is the correct fencing behaviour.

## 6. API additions

| Endpoint | Method | Role | Body → Reply |
|---|---|---|---|
| `/api/v1/jobs` | POST | node, admin | CreateJobRequest → Job (201) |
| `/api/v1/jobs` | GET | node, admin | → JobList (`?state=&campaign=&limit=`) |
| `/api/v1/jobs/{id}` | GET | node, admin | → Job |
| `/api/v1/jobs/{id}/cancel` | POST | node, admin | CancelRequest → Job |
| `/api/v1/jobs/{id}/status` | POST | node (lease holder) | JobUpdate → Job, or 409 `stale` |
| `/api/v1/leases` | POST | node (self) | LeaseRequest → LeaseReply |
| `/api/v1/artifacts/announce` | POST | node (self) | ArtifactAnnouncement → `{}` |
| `/api/v1/artifacts/sha256/{hex}` | GET | node, admin | → ArtifactInfo |

Audit entries: `job.submit`, `job.cancel`. Status updates and leases are not
audited; they're recorded in `job_attempts`.

## 7. CLI

- `tdrctl submit --remote ...` goes to the local tdrd over `CTL_SUBMIT` with `remote` = true,
  and tdrd forwards it as POST /jobs using the node identity. It prints the central job id.
  `tdrctl status/wait --remote ID` query the central job through the daemon.
- `tdrctl cp jobs [--state S] [--campaign C]`, `cp job ID`, `cp job-cancel ID`,
  `cp job-submit ...` (same flags as submit), `cp artifact ID`.

## 8. Configuration

- tdrd `[control]`: `remote_jobs = on|off` (default on), `lease_poll = 2` and
  `lease_poll_idle = 10` (seconds).
- tdrcp `[jobs]`: `lease_ttl = 60` (seconds), `max_attempts_default = 3`.

## 9. Testing

- cpdb: migration 2, the lease transaction (eligibility rules, SKIP LOCKED with two
  connections), fencing, expiry sweep with grace, retry and cancel paths, and artifact locations.
- cpapi: the endpoints, auth (only the lease holder may update; a node may lease only for itself), and 409 stale.
- remote: RemoteExecutor with a fake control client and a real JobManager: pull when free,
  outbox order and retries, 409 drop, revoked → cancel, restart recovery.
- E2E (`tests/e2e/test_remote_jobs.py`), with tdrcp and 2–3 nodes:
  - remote submit runs on another node when the submitter is full
  - jobs spread across nodes
  - kill -9 the executing node → lease expires → the job completes elsewhere
  - SIGSTOP a node past its lease, then SIGCONT → its late result is fenced (409) and the job keeps the other result
  - cancel of a running remote job
  - campaign PAUSED holds jobs, ACTIVE releases them
  - locality: a job whose input exists only on node B runs on node B
  - control-plane outage during a remote job → completes, and the outcome is delivered after reconnect
  - 200 small jobs across 3 nodes all complete exactly once

Exit criteria: First Demonstration (Description.md §20), "execute remote
jobs" and "recovery after worker failure", on localhost with three nodes.

## 10. Status (2026-11-06)

Implemented as designed.

| Check | Result |
|---|---|
| C++ suites (14, `-Werror`) | all pass; new: cpdb 18 + cpapi 10 job tests, agent client/hooks, remote 16 |
| ASan/UBSan (remote, agent, cpdb, cpapi, jobs, peer) | clean |
| e2e | 42 pass (9 remote-jobs + 4 peer + 10 control-plane + 19 Phase 1), stable over 3 runs |
| 200 `true` jobs, 3 nodes × 4 slots | all COMPLETED exactly once, spread over all 3 nodes |

Behaviour notes:
- Job specs are stored as serialized protobuf (bytea). The lease query filters
  eligibility in SQL, then packs greedily in C++, scanning up to 200 eligible rows.
- Only the expiry sweep ends a lease. Status updates and renewals don't check
  `expires_at`, so the startup grace protects leases across a control-plane outage.
- A FAILED report on a cancel-requested job ends FAILED, not retried. The normal
  path, revoke then local cancel, ends CANCELLED.
- Full artifact syncs are tracked in `artifact_syncs`. Rows announced
  incrementally during a sync are not dropped by it.
- `tdrctl status/list` show the central id of a local job that ran a lease
  (`remote:` line, JSON `remote_job_id`). For finished leases it comes from the `.a<N>` suffix.

Known limitations:
- GPU jobs are not leased: the node reports 0 free GPUs until GPU accounting exists.
- No node "draining" state yet. OFFLINE and STALE nodes get no leases.
- A lease call holds up to 200 candidate rows locked until it commits.
  Concurrent leasers skip them, so they briefly see fewer jobs.
- Retry happens immediately (no backoff), and there's no job history pruning.
- Cancellation reaches a running job on the next heartbeat (≤ `heartbeat_interval`).
