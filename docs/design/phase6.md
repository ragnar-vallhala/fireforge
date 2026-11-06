# Phase 6 — Workflow Engine: Distributed DAGs, Fan-out, Checkpoints, Ranked Placement

Status: design, v0.1 (2026-11-06). Builds on Phases 1–5.

Goal: a scientist submits a **workflow**, a DAG of steps. The control plane runs each
step as a central job once its dependencies are done, and wires parents' outputs
into children's inputs. A running step may **add steps** to its own workflow (fan-out,
e.g. one job per candidate structure). Steps can **checkpoint**, and a retry on any node
resumes from the latest checkpoint. Jobs with strict placement needs are **reserved**
for the best-ranked node before others may take them. Description.md §10.3, §14.

Decisions (2026-11-06):
- DAG engine lives in the **control plane** (Postgres, next to the job queue)
- model: **static DAG + dynamic fan-out**, with outcome rules (`run_if`)
- push: **reserve + rank via pull**. There is no second delivery path. Strict jobs are held for
  their best-ranked node for a short window, then the next one.
- checkpoints: **artifact-based**, so a retry anywhere gets the latest checkpoint

---

## 1. Components

```
 tdrctl cp workflow-submit wf.json ─► tdrcp  POST /workflows ─► cpdb: workflows, workflow_steps
                                            │  engine (in the job-update transaction):
                                            │   step ready  → create central job (inputs wired)
                                            │   rule unmet  → SKIPPED;  all terminal → workflow done
                                            │  placement sweep: rank nodes, reserve strict jobs
 tdrd ── lease ─► RemoteExecutor ─► JobManager ─► worker (SDK)
                     ▲   │ fetch inputs + resume checkpoint      │ context.checkpoint(), context.spawn()
                     │   └ JobUpdate{RUNNING, checkpoint} ◄──────┤ WORKER_CHECKPOINT
                     └──── JobUpdate{COMPLETED, outputs (named), spawn[]} ◄ WorkerResult.spawn
```

| Module | Path | Owns |
|---|---|---|
| cpdb/cpapi (extend) | `control/*` | workflows, engine, expansion, checkpoints, ranking + reservations |
| jobs/journal (extend) | `runtime/jobs`, `runtime/journal` | named outputs, named inputs, checkpoints, resume, spawn persistence |
| remote (extend) | `runtime/remote` | report checkpoints + spawn, fetch resume checkpoint |
| SDK (extend) | `sdk/python` | `checkpoint`, `load_checkpoint`, `spawn`, `input(name)` |
| tdrd/tdrctl | `runtime/daemon`, `runtime/ctl` | WORKER_CHECKPOINT, `cp workflow*` |

## 2. Named outputs and inputs (prerequisite)

- Outputs get **names**. `OutputFile.name` if set, else its relative path. Undeclared
  outputs (directory scan) are named by their path relative to the output dir.
  Names are unique per job: a duplicate name fails the job (non-retryable).
- `ArtifactRef.name` (control.proto) and `JobStatus.outputs` (job.proto, `NamedArtifact`)
  carry the names. They are stored in the journal (`job_outputs.name`) and in
  `jobs.outputs` (JSONB, already ArtifactRef) on the control plane.
- Inputs get names too. `JobSpec.input_names` lines up with `input_artifacts` ("" = unnamed).
  `InputFile.name` reaches the worker, and SDK `context.input("structure")` returns its path.

## 3. Workflow model

`proto/fireforge/tdr/v1/job.proto` (shared by the control plane and the worker protocol):

```
StepSpec { name, job: JobSpec (template), depends_on[]: step names,
           inputs[]: StepInput, run_if: RunIf, allow_failure: bool }
StepInput { step, output ("" or "*" = all, else a name), spawned: bool, input_name }
RunIf { ALL_SUCCEEDED (default) | ANY_FAILED | ALWAYS }
```

- A workflow = `{name, campaign_id, steps[]}`. Step names match `[A-Za-z0-9_.-]{1,64}`
  and are unique. `depends_on` must name existing steps, and the graph must be acyclic. Every
  `StepInput.step` is implicitly a dependency. Max 10 000 steps per workflow.
- The `job` template is a normal JobSpec without job_id, inputs from steps, or campaign_id
  (it inherits the workflow's). Static `input_artifacts` in the template are kept.
- **Readiness.** A step is ready when every dependency is *settled*. Dependency X is settled
  when X is terminal **and every step X spawned (recursively) is terminal**. So a join
  step that depends on a fan-out step waits for the whole fan-out.
- **Rules**, evaluated on the settled dependencies and their spawned subtrees:
  - ALL_SUCCEEDED (default): every one COMPLETED
  - ANY_FAILED: at least one FAILED, EXPIRED, VALIDATION_FAILED or CANCELLED
  - ALWAYS: no condition

  A ready step whose rule fails becomes **SKIPPED** and gets no job. SKIPPED is neither success
  nor failure: ALL_SUCCEEDED fails on a SKIPPED dependency (so a chain after a failure is skipped as a whole),
  ANY_FAILED ignores it, and ALWAYS runs anyway.
- **Inputs.** When a step becomes ready, each StepInput resolves to artifacts: the named output
  (missing → the step FAILS, non-retryable, "input <step>.<output> missing"), or all outputs
  (`"*"`), plus, with `spawned`, the same selection from every COMPLETED step in X's spawned
  subtree. Each artifact is appended to `input_artifacts`, with `input_names` =
  `input_name` (or the output name). If names collide, a `#<n>` suffix is added in step order.
- **Step states:** WAITING, then (job) QUEUED…COMPLETED|FAILED|CANCELLED|EXPIRED|VALIDATION_FAILED,
  or SKIPPED. Step job id = `job-<16 hex>`. The job carries `workflow_id` and `step`.
- **Workflow state:** RUNNING until every step is terminal or SKIPPED. Then it is FAILED if any step
  ended FAILED/EXPIRED/VALIDATION_FAILED/CANCELLED without `allow_failure`, else SUCCEEDED.
  A cancelled workflow is CANCELLED.
- **Cancel:** `POST /workflows/{id}/cancel` marks WAITING steps CANCELLED and cancels their jobs
  (Phase 4 semantics). No new steps start, and expansions from finishing jobs are dropped.

## 4. Engine

- Runs **inside the transaction that makes a step's job terminal**: `update_job` (COMPLETED /
  FAILED-final / CANCELLED / VALIDATION_FAILED), `expire_leases` (final EXPIRED), and `cancel_job`
  for a QUEUED job. It locks the workflow row (`FOR UPDATE`), applies any expansion, re-evaluates
  WAITING steps of that workflow, creates jobs for ready ones (queued_at = now), marks SKIPPED,
  and finishes the workflow if everything is terminal. A retryable failure is not terminal and
  does nothing.
- One workflow is processed at a time (row lock), so concurrent completions in the same workflow serialise.
  The evaluation is incremental: only steps depending on the finished step (or its ancestors in
  the spawn tree) are re-checked.
- `sweep_workflows()` (tdrcp timer, 5 s) re-evaluates RUNNING workflows changed in the last
  sweep window plus any with WAITING steps whose deps are settled, as a safety net. It is idempotent.

## 5. Dynamic fan-out

- A worker calls `context.spawn(name, operation=..., params=..., inputs=[...], depends_on=[...],
  resources=..., run_if=..., allow_failure=...)`. The spawned steps return in
  `WorkerResult.spawn` (repeated StepSpec), which only counts if the job succeeds.
- tdrd journals the spawn list with the job (`jobs.spawn_json`, local). A lease's COMPLETED update
  carries it: `JobUpdate.spawn`. It is fenced like the outputs.
- The control plane validates the expansion in the completion transaction:
  - only workflow steps may spawn (a standalone job that spawns → FAILED non-retryable)
  - names become `<parent>/<name>` and must be unique
  - deps may name existing steps (by full name) or siblings in the same expansion (by short
    name), and the expansion must be acyclic
  - at most 1 000 per expansion and 10 000 per workflow
  - inputs may also use `step: "."`, meaning the spawning step's own outputs

  An invalid expansion makes the update **kInvalid**: the job becomes FAILED (non-retryable)
  with `invalid spawn: <reason>`, and its outputs are still recorded.
- A spawned step's parent is the spawning step, and readiness uses the spawn tree (§3). Spawned
  steps may spawn too (depth ≤ 8).
- The spawn tree: `workflow_steps.parent_step` (NULL for static steps).

## 6. Checkpoints

- SDK `context.checkpoint(data | path, name="checkpoint")` writes the file into checkpoint_dir
  and sends `WORKER_CHECKPOINT {attempt_id, path, media_type}` to tdrd. Each call is one
  checkpoint; the last one wins. At most one per 10 s is forwarded (the rest are coalesced).
- tdrd ingests it (producer_job = job id), records it in the journal as the job's
  `latest_checkpoint`, and for a lease reports `JobUpdate{state: RUNNING, checkpoint: ArtifactRef}`
  (fenced) and announces it. The control plane stores `jobs.checkpoint` (ArtifactRef).
- **Resume.** A lease of a job with a checkpoint sets `JobSpec.resume_checkpoint` (artifact id).
  The RemoteExecutor fetches it like an input. JobManager materializes it into the new attempt's
  checkpoint_dir as `resume/<name>`, and `WorkerTask.resume_checkpoint` is its path. SDK
  `context.load_checkpoint()` returns the path (or None). A local retry (JobManager, max_attempts
  > 1) passes the job's latest_checkpoint the same way.
- **Replication.** A checkpoint in the dead node's store is useless to a retry elsewhere, so tdrd
  asks the `[artifacts] checkpoint_replicas` (1) closest CONNECTED peers to copy each new
  checkpoint. It sends `ARTIFACT_REPLICATE {artifact_id}` on the peer link, and the peer fetches it
  over a data link with the sender as the first holder, then announces it. The control plane sets
  `resume_checkpoint` on a lease only if an ONLINE node holds the checkpoint. Otherwise the retry
  starts from scratch, which never burns attempts on fetches that cannot succeed.
- **Retention.** A job's latest checkpoint is protected from GC (like an input) while the job is non-terminal.
  Superseded and finished checkpoints are ordinary artifacts.
- Checkpoints require `ExecutionPolicy.checkpoint = true`. Without it, WORKER_CHECKPOINT gets an
  error reply (BAD_REQUEST "checkpointing not enabled for this job").

## 7. Ranked placement (reserve via pull)

- **Strict jobs:** `gpu_count > 0`, or total input bytes ≥ `[placement] strict_input_mb` (64).
- **Placement sweep** (tdrcp timer, `[placement] interval` 1 s): for each QUEUED strict job
  without a live reservation, rank the eligible ONLINE nodes (Phase 4/5 rules, except that
  free capacity is read from the last heartbeat) by cost and reserve the job for the best node
  not yet tried, for `reserve_window` (5 s). `jobs.reserved_node`, `reserved_until`,
  `reserve_tried` (text[]). After every eligible node has been tried, or after `max_reserve`
  (30 s) since queued_at, the job is unreserved and leasable by anyone (with the Phase 5 locality rule).
- A strict job is leasable only after its first placement pass, which runs within `interval` of
  creation or requeue. Before that pass no node may lease it, so a fast poller can't take it first.
- **lease_jobs:** a job reserved for another node with `reserved_until > now` is skipped. A job
  reserved for the caller is ordered first.
- **Cost** (Description.md §14), each term normalised to roughly 0..1:
  - C_transfer = missing input bytes / total input bytes
  - C_queue = (running + queued) / max(1, max_jobs) (last heartbeat)
  - C_memory = requested / available memory
  - C_contention = cpu_utilization
  - C_compute = 1 − cpu_cores / max cluster cores

  Weights come from `[placement] w_compute, w_transfer, w_queue, w_memory, w_contention`
  (0.1, 0.5, 0.2, 0.1, 0.1).
- `GET /jobs/{id}` shows the reservation (`reserved_node`, `reserved_until`), and `tdrctl cp job`
  prints it.

## 8. API and CLI

| Method | Path | Who | Body / reply |
|---|---|---|---|
| POST | `/api/v1/workflows` | admin, node | CreateWorkflowRequest → Workflow (201) |
| GET | `/api/v1/workflows?state=&campaign=&limit=` | admin, node | WorkflowList (no steps) |
| GET | `/api/v1/workflows/{id}` | admin, node | Workflow with steps |
| POST | `/api/v1/workflows/{id}/cancel` | admin | CancelRequest → Workflow |

- `tdrctl cp workflow-submit FILE` (proto3 JSON CreateWorkflowRequest; `-` = stdin),
  `cp workflows`, `cp workflow ID` (step table: name, state, job, node, deps, spawned-by),
  `cp workflow-cancel ID`, `cp workflow-wait ID [--timeout S]` (exit 0 iff SUCCEEDED).
- tdrd forwards nothing new. Workflows are submitted with admin certs, or with node certs through
  the HTTPS API directly.
- Audit: `workflow.create`, `workflow.cancel`.

## 9. Configuration

- tdrcp `[workflows] sweep_interval = 5`, `max_steps = 10000`, `max_spawn = 1000`.
- tdrcp `[placement] interval = 1`, `reserve_window = 5`, `max_reserve = 30`, `strict_input_mb = 64`,
  weights as in §7.

## 10. Testing

- cpdb:
  - create/validate (cycles, unknown deps, bad names, too many steps)
  - readiness with fan-out subtrees; rules incl. SKIPPED propagation; input wiring (named, `*`,
    spawned, collisions, missing output)
  - expansion validation and atomicity; workflow final states; cancel
  - concurrent completions in one workflow from two connections (no lost or duplicate step jobs)
  - checkpoint stored and put on the next lease's spec
  - reservation chain: best node first, next after the window, anyone after max_reserve; lease skips
    others' reservations
- cpapi: endpoints, auth, audit, JobUpdate.spawn/checkpoint plumbing, sweeps.
- jobs/journal: named outputs (declared, scanned, duplicate), input names to WorkerTask,
  checkpoint ingest + latest per job, resume into the next attempt (local retry), spawn persisted
  across restart, checkpoint disabled → error.
- remote: COMPLETED carries spawn; RUNNING checkpoint update; resume checkpoint fetched.
- SDK: checkpoint/load_checkpoint/spawn/input(name) against the fake daemon.
- E2E (`tests/e2e/test_workflows.py`), tdrcp + 3 nodes:
  - diamond relax → (bands, phonons, elastic) → evaluate, with output wiring checked in content
  - fan-out: a step spawns N children, and a join (`spawned` inputs) sees all N outputs
  - run_if: failure handler runs on failure, and the default chain is SKIPPED; workflow FAILED vs SUCCEEDED
    with allow_failure
  - cancel mid-workflow
  - checkpoint: a step checkpoints, is SIGKILLed with its node, retries on another node and resumes
    from the checkpoint (result shows the resume point)
  - GPU-less strict job reserved for the input holder: lands there although another node polls first

Exit criteria: a multi-branch workflow with fan-out runs across 3 localhost nodes, survives
a node kill mid-step by resuming from a checkpoint elsewhere, and reports a correct final state.

## 11. Status

Implemented as designed (§1–§10), with these additions and choices:

- **Checkpoint replication** (§6): tdrd asks `[artifacts] checkpoint_replicas` (1) closest peers to
  copy each new checkpoint (`ARTIFACT_REPLICATE`); leases get `resume_checkpoint` only if an ONLINE
  node holds it.
- **Strict jobs wait for placement** (§7): no node may lease a strict job before its first
  `place_jobs` pass. A node holding a live reservation skips the Phase 5 locality wait. The
  checkpoint counts toward strictness and transfer cost, not eligibility.
- **Engine details:** a named input is required only from producers that COMPLETED; with
  `spawned`, at least one must match, else the step fails "input X.Y missing". A `.` input
  (spawns only) means "from the spawning step" and is not a dependency; depending on the spawning
  step or its ancestors is rejected. Cancelling a workflow sets CANCELLED at once; later spawns
  are dropped and leased/running jobs get cancel_requested. The max_attempts default is applied
  by the API to step templates and spawns. `[workflows] max_depth` (8) is configurable.
- **Node:** journal schema v2 (output names, spawn, latest checkpoint); RemoteExecutor state file
  v3 (reads v1/v2). Output names: declared name, else the normalised path. A file declared under
  two names is ingested once. A resume checkpoint from the control plane is materialised as
  `checkpoints/resume/checkpoint`.
- **SDK:** `context.input/named_inputs/checkpoint/flush_checkpoints/load_checkpoint[_json]/spawn`,
  `fireforge.Workflow` builder. Checkpoints: at most one send per name per 10 s, the latest
  pending one is flushed before the result. Spawned steps default to the same interpreter and
  script, with `PYTHONPATH` carried over.
- **CLI:** `tdrctl cp workflow-submit|workflows|workflow|workflow-cancel|workflow-wait`; `cp job`
  shows workflow, reservation and checkpoint.

Tests: cpdb 66 cases, cpapi 48, journal 18, jobs 50, remote 30, SDK 72; e2e
`tests/e2e/test_workflows.py` 7 tests.

Limitations:

- `place_jobs` considers at most 200 strict jobs per pass and ranks from the last heartbeat; it
  does not account for jobs reserved earlier in the same pass.
- Spawned steps with the default command assume the same interpreter and script paths on every
  node.
- Superseded checkpoints are not protected from GC (only the latest one is).
- The Phase 4 e2e test with 200 jobs and a 4 s lease TTL can fail on a heavily loaded host
  (load ≈ 20 on 12 cores); Phase 5 fails it the same way.
