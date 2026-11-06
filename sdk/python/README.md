# FireForge Python SDK

Pure-Python worker SDK for the TDR daemon (`tdrd`). A worker is a normal
Python script that `tdrd` launches as a child process; the SDK connects back
over the daemon's Unix socket (`FIREFORGE_IPC_SOCKET`), receives the task,
reports progress and declares its outputs. Protocol: `docs/design/phase1.md`
§2 (48-byte big-endian envelope) and §4 (HELLO → TASK, PROGRESS*, RESULT → ACK,
CANCEL at any time).

```python
from fireforge import Worker, JobResult

worker = Worker()

@worker.task("transport.calculate")
def calculate_transport(context):
    structure = context.load_structure()
    result = calculate_properties(structure=structure, temperatures=[300, 500, 700, 900])
    artifact = context.save_result(result)
    return JobResult(success=True, artifacts=[artifact])

worker.run()
```

## Setup

The protobuf modules are generated, not committed:

```sh
tools/gen_proto.sh          # writes sdk/python/fireforge/tdr/v1/*_pb2.py
```

Run a worker with `PYTHONPATH=sdk/python` (or `pip install sdk/python`).
Dependency: `protobuf>=4.21,<5`. `pymatgen` is optional.

## Context

| | |
|---|---|
| `job_id`, `attempt_id`, `attempt_number`, `operation`, `campaign_id`, `material_id` | from WORKER_TASK |
| `params` | parsed `parameters_json` (`{}` if empty) |
| `inputs` | list of `InputRef(artifact_id, path, name)` (`name` "" = unnamed) |
| `named_inputs`, `input(name)` | name → `Path`; `input()` raises `KeyError` listing the names |
| `workflow_id`, `step` | workflow step identity (`""` outside a workflow) |
| `workdir`, `input_dir`, `output_dir`, `checkpoint_dir` | `Path`s |
| `report_progress(fraction, message="")` | fraction clamped to 0..1 |
| `cancelled`, `check_cancelled()`, `wait_cancelled(timeout)` | `check_cancelled` raises `Cancelled` |
| `save_output(name, data, media_type=None, filename=None)` | bytes / str / dict-or-list (JSON) / Path (copied) → `OutputRef` |
| `save_result(obj)` | `result.json`, `application/json` |
| `load_structure(raw=False)` | first input that looks like a structure (see below) |
| `checkpoint(data, name="checkpoint", media_type=None)` | see Checkpoints |
| `flush_checkpoints()` | send rate-limited checkpoints now |
| `load_checkpoint()`, `load_checkpoint_json(default=None)` | resume checkpoint path / parsed JSON, or None |
| `spawn(name, *, operation, ...)` | see Workflows |

Output names: `save_output(name, ...)` sends `OutputFile.name = name`; other
outputs (scanned or declared by path) are named by their path relative to
`output_dir`. Names must be unique per job (the attempt fails otherwise).

`load_structure()` (Phase 1): inputs are materialized as `inputs/<sha256>`, so
it matches by name (`.cif`, `.json`, `.vasp`, `POSCAR`) or by content (JSON
object, CIF `data_` block). With pymatgen installed it returns a
`pymatgen.core.Structure`; otherwise (or with `raw=True`) a
`StructureFile(path, format, text)` (`.data` parses JSON).

## Task return values

- `JobResult(success=True, artifacts=[...])`: exactly the listed outputs
  (`OutputRef`s or paths relative to `output_dir`). Paths must stay inside
  `output_dir` and exist, otherwise the attempt fails (not retryable).
- `JobResult(success=True)` / `None`: every regular file in `output_dir`.
- `dict`: saved via `save_result`, then as above.
- `JobResult(success=False, error=..., retryable=False)`: explicit failure.
- Raising `WorkerError(msg, retryable=False)`: clean failure; `Cancelled`:
  never retryable; any other exception: retryable, traceback on stderr.

`worker.run()` exits 0 on success, 1 on failure (or if the daemon refuses the
attempt, e.g. bad token), 2 if the `FIREFORGE_*` environment is missing.
`worker.run(exit=False)` returns the code instead.

## Checkpoints (Phase 6)

Requires `ExecutionPolicy.checkpoint = true` (`spawn(..., checkpoint=True)`,
`Workflow.step(..., checkpoint=True)`); otherwise `checkpoint()` raises
`CheckpointError` without sending anything.

```python
state = context.load_checkpoint_json(default={"iter": 0})   # resume point of a retry
for it in range(state["iter"], 1000):
    ...
    context.checkpoint({"iter": it + 1}, name="state")
```

- `data` is written like `save_output` (bytes / str / dict-list as JSON) to
  `checkpoint_dir/<name><ext>`; a Path inside `checkpoint_dir` is used in
  place, any other Path is copied in. The daemon gets `WORKER_CHECKPOINT`
  (path relative to `checkpoint_dir`) and replies ACK or ERROR; ERROR / no reply
  raises `CheckpointError`.
- Rate limit: per name, at most one send per 10 s. A call inside the window
  only rewrites the file; the latest pending version is sent by a timer when
  the window ends, by the next `checkpoint()` call after it, by
  `flush_checkpoints()`, and always before the RESULT (also on failure).
  Errors of timer sends are printed and raised by the next call.
- `load_checkpoint()` is `WorkerTask.resume_checkpoint`
  (`checkpoint_dir/resume/<name>`), or None.

## Workflows (Phase 6)

Fan-out from a running step (only takes effect if the attempt succeeds):

```python
@worker.task("screen")
def screen(context):
    names = [context.spawn(f"cand{i}", operation="relax", params={"cand": cand})
             for i, cand in enumerate(candidates(context.input("structure")))]
    context.spawn("rank", operation="rank", inputs=[f"{n}:result" for n in names])
```

`spawn(name, *, operation=None, argv=None, params=None, inputs=(), depends_on=(),
resources=None, run_if="all_succeeded", allow_failure=False, input_artifacts=(),
capabilities=(), max_attempts=None, timeout=None, checkpoint=False, env=None)`.
`inputs`: `"step"` (all outputs), `"step:output"`, or
`StepInput(step, output="*", spawned=False, as_name=None)`; `"."` is the
spawning step. With `argv=None` the child runs the same program as this
worker (`[sys.executable, abspath(sys.argv[0]), *sys.argv[1:]]`, or
`python -m <module>`), and its env defaults to this process's `PYTHONPATH`
(jobs start with a minimal environment); `env` is merged over that. The paths
must exist on whichever node runs the child.

Static workflows for `tdrctl cp workflow-submit FILE`:

```python
from fireforge import Workflow, StepInput

wf = Workflow("diamond", campaign_id="camp-1", argv=["python3", "/opt/w.py"])
relax = wf.step("relax", operation="relax", input_artifacts={"structure": "artifact://sha256/..."})
bands = wf.step("bands", operation="bands", inputs=[f"{relax}:structure"])
wf.step("report", operation="report", inputs=[bands], run_if="always")
wf.write("wf.json")      # proto3 JSON CreateWorkflowRequest (proto field names)
```

`step()` takes the same keywords as `spawn()` (argv defaults to the
workflow's `argv`; one of them is required). Names (`[A-Za-z0-9_.-]{1,64}`,
unique), known dependencies, acyclicity and the 10 000-step limit are checked
client-side (`WorkflowError`).

## Testing

`fireforge.testing.FakeDaemon` plays the daemon side on a temp Unix socket:

```python
from fireforge.testing import FakeDaemon

with FakeDaemon("transport.calculate", {"t": 300}) as d:
    d.add_input(open("NaCl.cif").read())
    proc = d.run_worker("my_worker.py")       # python my_worker.py, env from d.env()
    assert proc.returncode == 0 and d.result.success
```

Phase 6 options: `add_input(data, name=...)`, `FakeDaemon(workflow_id=, step=,
checkpoint_enabled=, resume_checkpoint=b"..." | Path, checkpoint_error="msg")`;
recorded: `d.checkpoints`, `d.checkpoint_files`, `d.spawned`.

Tests: `tools/pytest.sh` (runs under the shared memory cap).
