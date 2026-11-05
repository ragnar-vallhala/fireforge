# Phase 1 — TDR Foundation: Reliable Local Job Execution

Status: design, v0.1 (2026-11-05). Scope: one machine, no control plane.

Goal: `tdrd` accepts jobs from `tdrctl`, queues them within the node's resource
caps, runs each attempt as an isolated child process, publishes outputs into a
content-addressed store, records every state change in a durable journal, and
recovers cleanly after a crash.

Decisions this builds on (Description.md §21, decided 2026-11-05): custom
48-byte header + Protobuf payload; Postgres control plane (Phase 2); hybrid
push/pull scheduling (Phase 4+); control-plane-owned campaigns; control-plane
peer discovery; peer-local content-addressed artifacts; QE + Phonopy +
BoltzTraP2; multi-node MPI in TDR after Phase 6; in-house GPU kernels for
hot paths; first campaign half-Heuslers.

---

## 1. Components

```
                 ctl.sock (0660)                     worker.sock (0660)
 tdrctl  ───────────────────────►  tdrd  ◄─────────────────────────  child (Python SDK / any cmd)
                                    │
           ┌───────────────┬────────┼──────────────┬───────────────┐
        JobManager      Journal   ArtifactStore  Supervisor      UnixServer x2
        (runtime/jobs)  (SQLite)  (CAS, sha256)  (spawn,pidfd,   (runtime/ipc)
                                                  cgroups v2)
```

| Module | Path | Owns |
|---|---|---|
| common | `runtime/common` | ids, time, logging, `tdr.conf` parsing |
| protocol | `runtime/protocol` | envelope header codec, framing |
| ipc | `runtime/ipc` | async Unix-socket server, blocking client |
| journal | `runtime/journal` | SQLite journal (jobs, attempts, artifacts) |
| artifacts | `runtime/artifacts` | local CAS store |
| supervisor | `runtime/supervisor` | child process launch/limits/exit |
| jobs | `runtime/jobs` | state machine, admission, attempts, commit, recovery |
| daemon | `runtime/daemon` | `tdrd` main: wiring, signal handling, socket handlers |
| ctl | `runtime/ctl` | `tdrctl` CLI |
| SDK | `sdk/python/fireforge` | Python worker SDK |

Public interfaces are the headers under `runtime/*/include/tdr/*/`. They are
the contract between modules; change them only with a note in this doc.

**Threading.** `tdrd` runs one `boost::asio::io_context` on one thread. All
module calls and callbacks happen on that thread. SQLite calls are synchronous
(WAL + `synchronous=FULL`; a transition is ~1 ms). No locks in module code.

## 2. Wire protocol

Same framing on every transport (local Unix sockets now, peer TCP later):

| Offset | Size | Field | Notes |
|---:|---:|---|---|
| 0 | 4 | magic | `0x46465452` ("FFTR") |
| 4 | 2 | version | `1` |
| 6 | 2 | message_type | `fireforge.tdr.v1.MessageType` |
| 8 | 4 | flags | v1: must be 0 |
| 12 | 4 | payload_length | ≤ 64 MiB |
| 16 | 8 | message_id | sender-unique, starts at 1 |
| 24 | 8 | correlation_id | request's message_id on replies, else 0 |
| 32 | 16 | sender_id | node id; zero on local IPC |
| 48 | n | payload | serialized protobuf of the type implied by message_type |

All integers are **big-endian**. Decoders reject a bad magic, an unknown
version, non-zero flags, or an oversize payload, and then close the connection.

Message payloads: `proto/fireforge/tdr/v1/{envelope,job,local}.proto`.
Any request may get an `ERROR` reply (`Error{code,message}`).

## 3. Job lifecycle (local)

```
submit ─► CREATED ─► QUEUED ─► ASSIGNED ─► RUNNING ─► COMMITTING ─► COMPLETED
             │          ▲          │           │            │
             │          │          │           ├─► RETRY_PENDING ─► QUEUED   (attempts left)
             │          │          │           ├─► FAILED                    (none left / not retryable)
             │          │          │           ├─► EXPIRED                   (timed out, none left)
             │          │          │           └─► CANCELLED
             │          │          └─► CANCELLED / RETRY_PENDING (launch failed)
             │          └──── RETRY_PENDING
             └─► (validation errors never create a job: submit returns an error)
                                    COMMITTING ─► VALIDATION_FAILED (declared output missing / outside output_dir)
```

`LEASED` is defined but unused until the control plane exists.

**Admission** (FIFO, no backfill skipping in Phase 1 — the head of the queue
blocks if it does not fit). A job is started when, after reserving
`effective_request(spec)`:
`allocated.cpu ≤ cap.cpu`, `allocated.memory ≤ cap.memory`, `running < cap.jobs`.
A job whose request exceeds the node caps *at all* is rejected at submit.

## 4. Running an attempt

When admitting a job, JobManager:

1. Creates `attempt_id = new_id("att")`, `token = new_secret()`.
2. Creates `<work_root>/<job_id>/<attempt_id>/` with `inputs/ outputs/ checkpoints/`.
3. Materializes each input artifact into `inputs/<sha256 hex>`.
4. Journal: `begin_attempt(...)`, `QUEUED → ASSIGNED`.
5. `supervisor.launch()` with:
   - argv/env from `spec.command`, plus `FIREFORGE_*` env (see `local.proto`)
   - cwd = attempt dir, `stdout.log` / `stderr.log` in the attempt dir
   - limits = effective request; timeout = `policy.timeout_seconds`
6. Journal: `update_attempt_process(pid, start_ticks, cgroup)`, `ASSIGNED → RUNNING`.
   Launch failure → attempt `kFailed`, job `RETRY_PENDING` (or `FAILED`).

**Worker protocol** (optional for the child): `HELLO(attempt_id, token)` →
`TASK`; `PROGRESS*`; `RESULT` → `ACK`. The daemon may push `CANCEL`.

**On exit** (supervisor callback):

| Exit | WORKER_RESULT | Outcome |
|---|---|---|
| code 0 | none, or success=true | → COMMITTING |
| code 0 | success=false | failure; retry iff `retryable` |
| code ≠ 0 / signal / OOM | any | failure; retryable |
| timed out | any | failure; retry if attempts left, else EXPIRED |
| cancelled | any | CANCELLED |

Failure → `RETRY_PENDING` then straight back to `QUEUED` if
`attempts_used < max_attempts`, else `FAILED`. Error text lands in `last_error`.

**Commit** (COMMITTING):
1. Output set = the files declared in WORKER_RESULT (paths relative to
   `outputs/`, must stay inside it, must exist), or, with no result message,
   every regular file under `outputs/`.
2. Each file is ingested into the store (`move_source=true`) and recorded via
   `journal.record_artifact`.
3. `journal.commit_outputs(job, attempt, ids)` — the fencing check. If it
   returns false (the attempt is no longer current) the outputs stay in the
   store, unreferenced, and are not attached to the job.
4. The attempt dir is removed unless `keep_work_dirs`. Failed attempts keep
   their dir (logs) for inspection.

Reservations are released when the process exits, before commit.

## 5. Crash recovery

On start, `JobManager::recover()`:
- For each open attempt (`kStarting`/`kRunning`): `supervisor.kill_orphan()`
  (verifies `/proc/<pid>/stat` start time first, so a reused PID is never
  killed), attempt → `kAbandoned`, job → `RETRY_PENDING` → `QUEUED` or `FAILED`.
- Jobs in `ASSIGNED`/`RUNNING`/`COMMITTING` with no open attempt are treated the
  same way; `QUEUED`/`RETRY_PENDING` are re-queued in creation order.

Phase 1 does not re-attach to surviving children (we cannot reap a non-child
or recover its exit status). That arrives with the control plane's
"keep running during outage" work, which concerns control-plane loss, not daemon loss.

## 6. Resource enforcement

- **cgroups v2** when the daemon's cgroup is delegated (`Delegate=yes` in the
  systemd unit): the daemon moves itself into `<root>/daemon` and creates
  `<root>/jobs/<attempt_id>` with `memory.max`, `memory.swap.max=0`,
  `cpu.max`, `pids.max`. Kill = `cgroup.kill`. OOM = `memory.events:oom_kill`.
- **Fallback** (`cgroups = auto` and not delegated): `RLIMIT_AS` = memory
  request, own process group, kill via `killpg`. Logged once as a warning.
- `cgroups = required` refuses to start without delegation.

## 7. tdrctl

```
tdrctl [--socket PATH] submit [--cpus N] [--memory-mb N] [--timeout S] [--attempts N]
                              [--operation OP] [--params JSON] [--input ARTIFACT]... -- argv...
tdrctl status JOB_ID [--json]
tdrctl list [--state STATE] [--limit N] [--json]
tdrctl cancel JOB_ID
tdrctl node [--json]
tdrctl wait JOB_ID [--timeout S]        # polls until terminal; exit 0 iff COMPLETED
```
Default socket: `/var/lib/fireforge/run/ctl.sock`, overridable by `TDR_SOCKET`.

## 8. Python SDK

`sdk/python/fireforge`: `Worker`, `@worker.task(name)`, `Context`, `JobResult`
(Description.md §12). Pure Python, no C++ extension. Envelope codec in
`fireforge/protocol.py`; protobuf classes generated into `fireforge/tdr/v1/` by
`tools/gen_proto.sh`. `Worker.run()` connects using the `FIREFORGE_*` env, sends
HELLO, dispatches TASK.operation to the registered function, reports
PROGRESS, sends RESULT, waits for ACK, and exits 0 (or 1 on failure). A
background reader thread sets `context.cancelled` on CANCEL.

## 9. Testing and builds

- Every build/test command runs through `tools/capped` (shared 8 GiB cap).
- Unit tests per module (Boost.Test, `tests/*.cpp`); SDK tests with pytest.
- End-to-end tests (`tests/e2e/`) start a real `tdrd` on a temp `state_dir`.
- Phase 1 exit criteria: 10,000 synthetic local jobs complete; kill -9 of
  `tdrd` mid-run loses no job; child crash/timeout/OOM/cancel each produce
  the right state; idle RSS < 30 MB; local setup overhead < 100 ms per job.

---

## 10. Status (2026-11-05)

Implemented and passing: all modules, `tdrd`, `tdrctl`, Python SDK.

| Suite | Result |
|---|---|
| C++ unit tests (7 suites, ~160 cases) | pass, also under ASan/UBSan |
| Python SDK (pytest) | 33 pass |
| End-to-end (`tools/e2e.sh`) | 11 pass |

Measured on the dev machine (12 cores, cgroups delegated):

| Metric | Target | Measured |
|---|---:|---:|
| Idle daemon RSS | < 30 MB | ~10 MB (12 MB after 10k jobs) |
| 10,000 trivial jobs | complete | 10,000/10,000, 0 errors, ~97 s |
| Submit latency (local) | < 10 ms | ~9.6 ms |
| Kill -9 of tdrd mid-job | no job lost | orphan killed, job retried, COMPLETED |
| Cgroup OOM | detected | FAILED "out of memory" |

Throughput (~100 jobs/s) is bound by SQLite `synchronous=FULL` fsyncs (≈5
transactions per job). **Accepted (2026-11-05):** the benchmark jobs do no
work, so they measure journal I/O rather than scheduling. Real scientific jobs
run for minutes to days, so durability wins and `synchronous=FULL` stays.

### Known limitations / follow-ups

- **Inputs are hard links into the store.** Store files are 0444, but a child
  running as the same user could chmod and modify one, corrupting the CAS.
  **Decided (2026-11-05):** fix with §19's restricted users. Job children run
  under a separate unprivileged uid (e.g. `fireforge-job`) that cannot chmod
  store files owned by the daemon's uid. Implemented (§11); enable with
  `[resources] job_user`.
- **Fallback mode (no cgroup delegation):** only memory (RLIMIT_AS) is
  enforced; CPU and pids are not, and a child calling `setsid()` escapes the
  group kill. **Decided (2026-11-05):** production nodes run `tdrd` under the
  systemd unit with `Delegate=yes` (`deploy/systemd/tdrd.service`); the
  fallback is for development only.
- Admission is strict FIFO (head-of-line blocking), by design for Phase 1.
- Daemon restart kills and retries running attempts (no re-attach).
- `JournalError` on the scheduling path terminates the daemon (crash-and-recover).

### Contract notes from implementation

- `journal::update_attempt_process` also moves the attempt to `kRunning`.
- `journal::commit_outputs` sets COMPLETED itself; the job manager emits the listener event.
- `ipc::Connection::close()` drops queued writes; send a final ERROR and let the peer hang up.
- The supervisor moves every process in its cgroup into `<root>/daemon` at
  construction, so `tdrd` creates it before anything else. Don't install a
  `waitpid(-1)` SIGCHLD handler or set `PR_SET_CHILD_SUBREAPER` in tdrd.
- `tdrd` binds an attempt to the worker connection that completed HELLO;
  PROGRESS/RESULT from any other connection are rejected.

---

## 11. Restricted job user

Decided 2026-11-05: job children run as a separate unprivileged user so they
cannot modify stored artifacts (which are hard-linked into `inputs/`), talk to
the control socket, or touch other jobs' files.

**Identities** (production names; configurable):

| Who | User | Groups |
|---|---|---|
| tdrd | `fireforge` | `fireforge`, plus `fireforge-job` (added by tdrd itself at startup via setgroups) |
| job children | `fireforge-job` | `fireforge-job` |

**Config:** `[resources] job_user = fireforge-job`. Empty (default) = jobs run
as the daemon's user (development). The job group is the user's primary group.
tdrd refuses to start if `job_user` is root, equals the daemon's user, or
the daemon lacks `CAP_SETUID`, `CAP_SETGID` and `CAP_KILL` (unless euid 0).

**Permissions:**

| Path | Owner:group | Mode | Job can |
|---|---|---|---|
| `artifacts/sha256/**` | fireforge:fireforge | 0444 | read (not chmod: not owner) |
| `work/` , `run/` | fireforge | 0711 | traverse only |
| `run/ctl.sock` | fireforge:fireforge | 0660 | nothing |
| `run/worker.sock` | fireforge:fireforge-job | 0660 | connect |
| `work/<job>/` | fireforge:fireforge-job | 02750 | traverse |
| `work/<job>/<attempt>/`, `outputs/`, `checkpoints/` | fireforge:fireforge-job | 02770 | read/write |
| `work/<job>/<attempt>/inputs/` | fireforge:fireforge-job | 02750 | read |
| `stdout.log`, `stderr.log` | fireforge | 0640 | (written via inherited fds) |

The setgid bit makes job-created files inherit group `fireforge-job`; the
job's umask is 0007, so the daemon (a group member) can read outputs and
clean up. Outputs are **copied** into the store (never moved), so every store
file is a fresh daemon-owned 0444 file. A job that makes its own files
unreadable (e.g. 0600) fails at commit with a permission error.

tdrd checks at startup that the job user can traverse every ancestor of
`state_dir` (o+x, or group/owner match) and refuses to start otherwise.

**Child hardening** (in the child, after setpgid + cgroup join, before exec):
setgroups → setgid → setuid → verify ids → clear ambient caps → capset(empty)
→ `PR_SET_NO_NEW_PRIVS` → umask 0007. Clearing ambient caps matters: under
systemd `AmbientCapabilities=` would otherwise pass CAP_SETUID to the job.

**Killing:** the daemon signals processes of another uid, which needs
CAP_KILL (cgroup.kill works without it, the fallback killpg does not).

**systemd:** `User=fireforge`, `AmbientCapabilities=CAP_SETUID CAP_SETGID CAP_KILL`,
`CapabilityBoundingSet=` the same. **Dev:** `sudo tools/setup-job-user.sh`
creates the user and installs a capability-enabled copy of tdrd at
`build/caps/tdrd` (re-run with `--caps-only` after rebuilding).
