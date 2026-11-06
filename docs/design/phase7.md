# Phase 7: Multi-node execution (MPI)

Status: design. Builds on Phase 4 (leases, fencing), Phase 5 (inputs) and Phase 6 (ranking,
checkpoints).

One job runs as R MPI ranks across several nodes. The control plane allocates the nodes
together (a gang), holding and draining capacity until they are all free. The head node runs
`mpirun`, and mpirun's remote launcher is a TDR program, so each node's MPI daemon runs inside
that node's job sandbox (limits, cgroup, run_as user, cancellation). There is no ssh and no
Slurm.

Decisions (user, 2026-11-06):

- Launch: TDR launch agent for `mpirun` (Open MPI `plm_rsh_agent`); MPI wires itself up.
- Allocation: reserve and drain; one gang lease when every member is free.
- Shape: total ranks with per-rank resources; TDR packs ranks onto as few nodes as fit.

Lead decisions: losing any member fails the whole gang attempt (retried as a unit, resuming
from the latest checkpoint); MPI traffic goes directly between nodes over TCP.

## 1. Components

```
tdrcp  place_gangs (in the placement pass): pick nodes, hold capacity, gang lease when all ready
       lease_jobs: subtract others' holds from free capacity; deliver gang member leases
tdrd   RemoteExecutor: HEAD lease -> local job `tdr-launch head`; MEMBER lease -> `tdr-launch member`
       GangService: peer frames GANG_LAUNCH / GANG_STARTED / GANG_EXIT / GANG_KILL
tdr-launch (new executable, runtime/launch)
  head    build the hostfile and exec mpirun --mca plm_rsh_agent "tdr-launch agent" ...
  agent   called by mpirun as `<agent> <host> <shell words...>`; asks tdrd to start them on <host>
  member  waits on the worker socket for WORKER_EXEC, then execs /bin/sh -c <command>
  rank    chdir $FIREFORGE_WORKDIR, exec the user's program (ranks start in the session dir)
```

Verified with Open MPI 4.1.6 here: rsh agent with `plm_rsh_no_tree_spawn 1`, `orted` started
without `--daemonize` (the agent removes that word), and `--set-cwd-to-session-dir` plus the
rank wrapper puts each rank in its own node's job directory.

## 2. Job spec

```
JobSpec.mpi = MpiSpec {
  uint32 ranks;                 // > 0 makes this a gang job (even if it fits one node)
  uint32 max_ranks_per_node;    // 0 = no cap
  repeated string mpirun_args;  // extra args placed before the program (e.g. "--bind-to", "core")
}
```

- `resources` is per rank: `cpu_cores` (0 → 1), `memory_bytes` (0 → node default),
  `gpu_count`, `disk_bytes` (per node, not per rank).
- `command.argv` is the program each rank runs. TDR adds mpirun; the argv must not start with
  `mpirun`/`mpiexec` (rejected).
- Validation: ranks ≤ 4096; ≤ `[gangs] max_nodes` (16) nodes can be used.
- Outputs come from the head node only (rank 0's node: its `outputs/`). Ranks on other nodes may
  write their own `outputs/`, but those files are discarded. Inputs are materialised on every
  member. Checkpoints come from the head (the SDK checkpoints from rank 0 only, §8).

## 3. Placement (control plane)

Runs in the `place_jobs` pass (Phase 6 §7, every `[placement] interval`). Gang jobs are never
"strict" single-node jobs.

**Slots.** For a node, `slots = min(cap_cpu / cpu_per_rank, cap_mem / mem_per_rank,
cap_gpu / gpu_per_rank (if gpus requested), max_ranks_per_node)`. `cap_*` are the node's
policy capacity from its last heartbeat (`capacity`, not free).

**Eligible node:** ONLINE, has every required capability, has polled for leases within the last
30 s (remote jobs enabled), and slots ≥ 1.

**Choosing members.** Order eligible nodes by slots descending, then Phase 6 cost ascending
(input transfer etc.). Greedily add a node if its last heartbeat reports a CONNECTED peer link to
every node already chosen (and vice versa), until the chosen slots cover R ranks. The first
chosen node is the **head**. Ranks are assigned in member order: each member gets
`min(slots, remaining)`. If R can't be covered: no hold, the job stays QUEUED with
`pending_reason` = "not enough capacity: N of R ranks on M eligible nodes".

**Hold.** Insert `gang_holds(job_id, node_id, member_index, ranks, cpu, memory, gpus, created_at)`.
While a node has holds of other jobs, `lease_jobs` subtracts them from the caller's reported
free capacity before handing out ordinary jobs (drain). A held node keeps running its current
work.

**Ready.** When a held node calls `lease_jobs` with free capacity ≥ its hold, mark the member
`ready_at = now`. When every member's `ready_at` is within the last `[gangs] ready_window`
(5 s), create the attempt (§4) in the same transaction, and return the caller's member lease in
that reply. The other members get theirs on their next poll (`delivered = false` until then).

**Timeout.** A hold older than `[gangs] drain_timeout` (600 s) is dropped and re-picked on the
next pass; the previous members go into `gang_tried` so another set is preferred when one
exists. `pending_reason` shows "draining a, b (12 s of 600 s)".

Holds are released when the job is cancelled, leased or becomes terminal.

## 4. Gang attempt

One `job_attempts` row (one attempt id, one fencing token) plus
`job_attempt_members(attempt_id, node_id, member_index, rank_offset, ranks, role HEAD|MEMBER,
state LEASED|RUNNING|SUCCEEDED|FAILED|CANCELLED|ABANDONED, delivered, expires_at, error)`.

- `Job.node_id` is the head. `Job.gang` lists the current attempt's members (or the hold, while
  draining).
- Leases: each member's `Lease` carries the same attempt id and token plus
  `gang = GangInfo{role, member_index, members: [GangMember{node_id, name, ranks, rank_offset}],
  ranks, head_node_id}`. Each member's lease is renewed by that member's heartbeats
  (`active_leases`) and expires on its own.
- Updates: `POST /jobs/{id}/status` from any member, fenced by attempt id, token and membership.
  - RUNNING from a member: marks the member RUNNING.
  - Terminal from the **head**: decides the job (COMPLETED with outputs / FAILED / CANCELLED,
    with the normal retry rules). The other members are revoked (next heartbeat reply).
  - FAILED, CANCELLED or lease expiry of a **non-head member** while the head is not terminal:
    the gang attempt fails ("member <name> failed: …"), retryable unless the member said
    otherwise. All members are revoked and the job requeues or fails per `max_attempts`.
  - COMPLETED from a non-head member: the member is done; it doesn't end the job.
- Expiry sweep: any member's lease expired → the whole attempt is EXPIRED/requeued as above.
- Cancel: revokes every member.
- On requeue: holds and readiness are cleared; the next placement pass picks again (a dead
  node is no longer ONLINE, so it is not picked).

## 5. Node runtime

**RemoteExecutor.** A lease with `gang` set:

- Resources for the local job: `ranks_here × per-rank` (cpu, memory, gpus) plus `disk_bytes`.
- HEAD: writes `<remote state>/gangs/<attempt>.json` (members, ranks, the node's `[mpi]` settings)
  and submits a local job with argv
  `[launcher, "head", "--gang", <file>, "--", <spec argv...>]`.
- MEMBER: submits a local job with argv `[launcher, "member"]` and env
  `FIREFORGE_GANG_ID=<attempt>`, `FIREFORGE_GANG_HEAD=<node id>`.
- Inputs are fetched on every member as in Phase 5.
- Reports RUNNING and the terminal state of its own local job, as for any lease.
- Persists the gang info in the state file (format v4, reads v1–v3).

**GangService** (`runtime/gang`, in tdrd, single-threaded):

- Agent request (worker socket, `WORKER_GANG_LAUNCH`): `{attempt_id, token, host, command}`.
  The attempt must be a running local job that is a gang HEAD; `host` must be a member node id
  of that gang (never the head itself: its ranks are local to mpirun). It sends `GANG_LAUNCH{gang_id, launch_id, command}` on the
  peer link to `host`, replies `WORKER_ACK` once the member answers `GANG_STARTED`, or `ERROR`
  (member not running / not connected / timeout `[mpi] launch_timeout`, 120 s). When the member
  later sends `GANG_EXIT{launch_id, exit_code}`, tdrd sends `WORKER_GANG_EXIT` to the agent's
  connection. If the agent disconnects first, tdrd sends `GANG_KILL{gang_id}`.
- Member side: `GANG_LAUNCH` is accepted only from the gang's head node (peer sender id) for a
  gang whose member job is RUNNING and whose member process has said hello and not yet received
  a command. tdrd sends `WORKER_EXEC{command}` to that process, replies `GANG_STARTED`, and
  sends `GANG_EXIT` when the local job ends. One launch per member job; a second is refused.
- `GANG_KILL` (from the head) cancels the member's local job.
- When a HEAD local job ends, tdrd sends `GANG_KILL` to every member still launched.

**tdr-launch** (`runtime/launch`, executable installed next to tdrd):

- `head --gang FILE -- argv...`: writes `$FIREFORGE_WORKDIR/mpi/hostfile`
  (`<node id> slots=<ranks>`; the head itself as `localhost`), then execs
  `mpirun --hostfile H -np R --set-cwd-to-session-dir --mca plm_rsh_agent "<self> agent"
  --mca plm_rsh_no_tree_spawn 1 [--mca oob_tcp_if_include X --mca btl_tcp_if_include X]
  [--mca k v from [mpi] mca] <mpirun_args...> <self> rank -- argv...`. `[mpi] mpirun`
  is the mpirun path.
- `agent <host> <words...>`: removes the word `--daemonize`, joins the rest with spaces, connects
  to `$FIREFORGE_IPC_SOCKET`, sends `WORKER_GANG_LAUNCH` with `FIREFORGE_ATTEMPT_ID/TOKEN`,
  waits for `WORKER_GANG_EXIT`, and exits with that code (128+signal for signals). SIGTERM or
  SIGINT: closes the connection (tdrd kills the member) and exits 143.
- `member`: `WORKER_HELLO`, waits up to `launch_timeout` for `WORKER_EXEC`, then execs
  `/bin/sh -c <command>` (the socket fd is close-on-exec). Timeout → exit 75 with an error.
- `rank -- argv...`: `chdir($FIREFORGE_WORKDIR)` and exec argv.

The worker socket accepts `WORKER_GANG_LAUNCH` on a fresh connection (it is not a worker hello);
JobManager gains `verify_attempt(attempt_id, token)` for that.

**Local single-node MPI.** `tdrctl submit --ranks N ...` on a node: the daemon rejects a local
spec with `mpi.ranks` unless the ranks fit this node, and runs it as a gang of one (HEAD only,
hostfile `localhost slots=N`, no agent use).

## 6. Wire

- `job.proto`: `MpiSpec`; `JobSpec.mpi = 18`.
- `control.proto`: `GangMember{node_id, name, member_index, rank_offset, ranks, role, state}`,
  `GangInfo{role, member_index, members, ranks, head_node_id}`, `Lease.gang = 7`,
  `Job.gang = 21`, `Job.pending_reason = 22`, `JobAttempt.members = 10`.
- `envelope.proto`: peer `GANG_LAUNCH 0x0501`, `GANG_STARTED 0x0502`, `GANG_EXIT 0x0503`,
  `GANG_KILL 0x0504`; local `WORKER_EXEC 0x1008`, `WORKER_GANG_LAUNCH 0x1009`,
  `WORKER_GANG_EXIT 0x100A`. Payloads in `gang.proto` (peer) and `local.proto` (local).

## 7. Configuration

- tdrcp `[gangs]`: `drain_timeout = 600`, `ready_window = 5`, `max_nodes = 16`, `max_ranks = 4096`.
- tdrd `[mpi]`: `mpirun = mpirun`, `launcher = ""` (default: `tdr-launch` next to the tdrd
  binary), `if_include = ""` (interface or CIDR for MPI TCP traffic; empty = Open MPI default),
  `mca = ""` (`k=v,k=v` extra MCA params), `launch_timeout = 120`.

## 8. SDK and CLI

- SDK: `context.mpi_rank`, `context.mpi_size`, `context.is_root` (from `OMPI_COMM_WORLD_RANK`/
  `PMI_RANK`). Under MPI only rank 0 on the head talks to tdrd; other ranks run the task with a
  detached context (outputs, checkpoints and progress are no-ops, results ignored). `spawn` and
  the Workflow builder accept `ranks=`/`max_ranks_per_node=`.
- tdrctl: `cp job-submit --ranks N [--max-ranks-per-node K] [--mpirun-arg A]...`,
  `submit --ranks N` (local); `cp job` shows the gang (member, ranks, state) and
  `pending_reason`.

## 9. Failure handling

| Event | Result |
|---|---|
| member node dies or is SIGKILLed | its lease expires → attempt fails, requeued (retry), resumes from the checkpoint |
| member orted/ranks crash | member job FAILED → attempt fails; mpirun on the head also exits non-zero |
| head node dies | head lease expires → attempt fails; members revoked |
| agent killed by mpirun teardown | tdrd sends GANG_KILL; member job ends |
| member never gets a launch | `tdr-launch member` exits 75 after launch_timeout → attempt fails |
| cancel | every member revoked → local cancel |
| control plane outage | leases keep running (Phase 4 grace) |

## 10. Testing

- cpdb: slots and packing (unequal nodes, max_ranks_per_node, GPUs), peer-connectivity filter,
  holds subtract from lease capacity, readiness window, drain timeout and re-pick with
  `gang_tried`, member leases delivered once, fenced member updates, head terminal decides,
  member failure/expiry fails the attempt and revokes the rest, cancel, pending_reason.
- remote/gang: HEAD/MEMBER local job construction, gang file, GANG_LAUNCH validation (only from
  head, only once, member must be waiting), GANG_EXIT relay, GANG_KILL, head end kills members,
  state file v4.
- launch: hostfile and mpirun argv construction, `--daemonize` removal, rank wrapper, member
  timeout (unit tests on the argv builders; the real mpirun runs in e2e).
- SDK: rank detection, detached non-root context.
- E2E (`tests/e2e/test_mpi_gangs.py`, real Open MPI over `lo`, program built with `mpicc` in the test):
  - 6 ranks across 2 nodes (4 + 2): rank 0 writes the allreduce sum and each rank's node
    workdir; ranks really ran on both nodes, each in its own job sandbox.
  - drain: a node busy with a long job; the gang waits (pending_reason), small jobs submitted later
    don't use the held capacity; the gang runs when the long job ends.
  - member node SIGKILLed mid-run: the attempt fails and is retried on the remaining nodes.
  - cancel: every member's processes are gone.
  - not enough capacity: QUEUED with pending_reason.
  - local `tdrctl submit --ranks 2`.

Exit criteria: an MPI program runs across 2–3 localhost nodes under TDR's control, survives a
member node kill by retrying, and nothing is left running after cancel.

## 11. Status

Implemented as designed above, with these additions found during integration:

- **Busy nodes keep polling.** A node with no free slot, CPU or memory still sends
  lease requests, offering nothing (`max_jobs = 0`, zero free). Gang eligibility needs a
  poll within 30 s, and a held node must be seen draining; undelivered member leases
  (already accounted) still come back on such a poll.
- **Members wait for their job before accepting a launch.** The head can send
  `GANG_LAUNCH` before the member has received its lease, fetched inputs, or connected
  `tdr-launch member`. A member treats "unknown gang", "job not running yet" and "process
  not connected" as temporary and retries every 200 ms for ¾ of `launch_timeout` (the head
  gives up at the full timeout). `GANG_KILL` drops a waiting launch. Wrong head, wrong
  role, an ended job and a second launch are refused at once.
- **`HWLOC_COMPONENTS=-gl`** is set in gang job environments (unless the spec sets it) to
  silence hwloc's X11 probing in `mpirun` and `orted`.
- **Launcher lookup:** `[mpi] launcher`, else `tdr-launch` next to `tdrd`, else the build
  tree's `runtime/launch/tdr-launch`, else `PATH`.
- **The head node is never a launch `host`**: its own ranks are local to
  `mpirun` and never go through the agent.

Verification: all 17 unit suites pass plain and under ASan with -Werror (new: tdr_gang 20
cases, tdr_launch 18, cpdb gang 16, cpapi gang 4); SDK 111 tests; e2e
`tests/e2e/test_mpi_gangs.py` (6 tests, real Open MPI 4.1.6 over loopback) passed 3/3 runs.

Limitations:

- `[mpi] mca` is `k=v,k=v`, so a value containing a comma (`btl=self,tcp`) cannot be
  given there; use `mpirun_args` or a `^` exclusion (`btl=^vader`).
- Several "nodes" on one host (the e2e setup) must exclude Open MPI shared memory
  (`btl=^vader`), or ranks of different members crash in `vader`. Real multi-host gangs
  are unaffected.
- Node-side GPU accounting does not exist: tdrd always reports `free.gpu_count = 0`, so
  a gang needing GPUs never becomes ready. Needs node GPU inventory (later phase).
- Non-root ranks in the Python SDK do not know the task's operation or parameters unless
  `FIREFORGE_MPI_OPERATION` / `FIREFORGE_MPI_PARAMS` are passed (single-task workers
  work without them); tdrd does not set them yet.
- No preemption: a hold waits up to `drain_timeout` for running jobs to finish.
- The member's one-launch-per-job record is in memory; it resets on tdrd restart (the
  member job fails with the restart anyway).
- A brief peer-link flap cancels member jobs launched from that head at once.
- `place_jobs` considers at most 200 queued gang jobs per pass; `poll_recent` (30 s) is
  not configurable in `tdrcp.conf`.
- `test_remote_jobs.py::test_many_small_jobs_complete_exactly_once` (Phase 4, lease_ttl
  4 s) still fails when the host is heavily loaded (load ~25 on 12 cores); it fails the
  same way with the pre-Phase-7 polling code.
