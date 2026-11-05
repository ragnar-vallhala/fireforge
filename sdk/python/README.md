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
| `inputs` | list of `(artifact_id, Path)` |
| `workdir`, `input_dir`, `output_dir`, `checkpoint_dir` | `Path`s |
| `report_progress(fraction, message="")` | fraction clamped to 0..1 |
| `cancelled`, `check_cancelled()`, `wait_cancelled(timeout)` | `check_cancelled` raises `Cancelled` |
| `save_output(name, data, media_type=None, filename=None)` | bytes / str / dict-or-list (JSON) / Path (copied) → `OutputRef` |
| `save_result(obj)` | `result.json`, `application/json` |
| `load_structure(raw=False)` | first input that looks like a structure (see below) |

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

## Testing

`fireforge.testing.FakeDaemon` plays the daemon side on a temp Unix socket:

```python
from fireforge.testing import FakeDaemon

with FakeDaemon("transport.calculate", {"t": 300}) as d:
    d.add_input(open("NaCl.cif").read())
    proc = d.run_worker("my_worker.py")       # python my_worker.py, env from d.env()
    assert proc.returncode == 0 and d.result.success
```

Tests: `tools/pytest.sh` (runs under the shared memory cap).
