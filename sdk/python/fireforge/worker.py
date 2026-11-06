"""Worker side of the TDR local worker protocol (docs/design/phase1.md section 4).

Usage (Description.md section 12)::

    from fireforge import Worker, JobResult

    worker = Worker()

    @worker.task("transport.calculate")
    def calculate_transport(context):
        structure = context.load_structure()
        result = calculate_properties(structure=structure, temperatures=[300, 500])
        artifact = context.save_result(result)
        return JobResult(success=True, artifacts=[artifact])

    worker.run()

Flow: HELLO -> TASK, (PROGRESS | CHECKPOINT -> ACK)*, RESULT -> ACK. The
daemon may send CANCEL at any time; a background reader thread sets
``context.cancelled`` and routes replies to the request that is waiting for
them (by correlation id).

Phase 6 (docs/design/phase6.md): named inputs (``context.input(name)``),
checkpoints (``context.checkpoint`` / ``context.load_checkpoint``) and
dynamic fan-out (``context.spawn``) for workflow steps.

Phase 7 (docs/design/phase7.md §8): under MPI (``JobSpec.mpi``) only rank 0
talks to the daemon; other ranks run the task with a detached context (see
``Worker.run`` and ``fireforge.mpi``).

Exit codes of ``Worker.run()``: 0 = task succeeded, 1 = task failed or the
daemon refused the attempt, 2 = FIREFORGE_* environment missing/invalid.
"""

from __future__ import annotations

import dataclasses
import json
import math
import mimetypes
import os
import queue
import shutil
import socket
import sys
import tempfile
import threading
import time
import traceback
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, NamedTuple

from . import _pb, mpi as _mpi
from ._pb import local_pb2
from ._version import __version__
from .protocol import FrameReader, FrameWriter, ProtocolError
from .workflow import MAX_SPAWN, STEP_NAME_RE, WorkflowError, build_step_spec

ACK_TIMEOUT_S = 30.0
CHECKPOINT_INTERVAL_S = 10.0  # at most one WORKER_CHECKPOINT per name per interval
HANDSHAKE_TIMEOUT_S = 30.0

EXIT_OK = 0
EXIT_FAILURE = 1
EXIT_CONFIG = 2

REQUIRED_ENV = ("FIREFORGE_IPC_SOCKET", "FIREFORGE_ATTEMPT_ID", "FIREFORGE_ATTEMPT_TOKEN")

mimetypes.add_type("chemical/x-cif", ".cif")


# ---------------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------------


class WorkerError(Exception):
    """Raise from a task to fail the attempt with a clean message.

    ``retryable`` (default False) tells the daemon whether another attempt
    might succeed. Any other exception type is reported as retryable.
    """

    def __init__(self, message: str = "", retryable: bool = False) -> None:
        super().__init__(message)
        self.retryable = retryable


class Cancelled(WorkerError):
    """The daemon cancelled this attempt. Never retryable."""

    def __init__(self, message: str = "cancelled") -> None:
        super().__init__(message, retryable=False)


class CheckpointError(WorkerError):
    """A checkpoint could not be recorded.

    Raised by ``Context.checkpoint`` / ``flush_checkpoints`` when
    checkpointing is not enabled for the job, the daemon replied ERROR, or no
    reply came. If not caught, the attempt fails (``retryable`` is False for a
    refusal, True for a lost connection / missing reply).
    """


@dataclass(frozen=True)
class OutputRef:
    """A file in the output directory that should be published as an artifact."""

    name: str          # logical name, e.g. "result"
    path: str          # relative to output_dir
    media_type: str = "application/octet-stream"


@dataclass
class JobResult:
    """What a task returns.

    ``artifacts=None`` (the default) declares every file in the output
    directory (files saved with ``save_output`` keep their names/media types).
    An explicit list declares exactly those outputs: ``OutputRef`` objects or
    paths (relative to output_dir, or absolute inside it).
    For failures, ``retryable`` defaults to False.
    """

    success: bool = True
    artifacts: list[OutputRef | str | os.PathLike] | None = None
    error: str = ""
    retryable: bool = False


class InputRef(NamedTuple):
    artifact_id: str
    path: Path
    name: str = ""   # Phase 6: JobSpec.input_names entry ("" = unnamed)


@dataclass
class StructureFile:
    """Returned by ``Context.load_structure()`` when pymatgen is unavailable."""

    path: Path
    format: str     # "cif", "json" or "poscar"
    text: str

    @property
    def data(self) -> Any:
        """Parsed JSON for ``format == "json"``; raises otherwise."""
        if self.format != "json":
            raise ValueError(f"structure is {self.format}, not json")
        return json.loads(self.text)


# ---------------------------------------------------------------------------
# Context
# ---------------------------------------------------------------------------


def _jsonable(obj: Any) -> Any:
    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        return dataclasses.asdict(obj)
    if isinstance(obj, Path):
        return str(obj)
    if isinstance(obj, (set, frozenset, tuple)):
        return list(obj)
    for attr in ("as_dict", "to_dict", "tolist"):  # pymatgen, pandas, numpy
        fn = getattr(obj, attr, None)
        if callable(fn):
            return fn()
    if hasattr(obj, "item") and callable(obj.item):  # numpy scalars
        return obj.item()
    raise TypeError(f"object of type {type(obj).__name__} is not JSON serializable")


def _to_json_bytes(obj: Any) -> bytes:
    return (json.dumps(obj, indent=2, default=_jsonable) + "\n").encode()


def _sniff_structure_format(path: Path, head: str) -> str | None:
    suffix = path.suffix.lower()
    if suffix == ".cif":
        return "cif"
    if suffix == ".json":
        return "json"
    if suffix in (".vasp", ".poscar") or path.name.upper() in ("POSCAR", "CONTCAR"):
        return "poscar"
    stripped = head.lstrip()
    if stripped.startswith("{"):
        return "json"
    if stripped.startswith("data_") or "_cell_length_a" in head:
        return "cif"
    return None


def _unlink_quiet(path) -> None:
    try:
        os.unlink(path)
    except FileNotFoundError:
        pass


def default_worker_argv() -> list[str]:
    """Command that re-runs the current worker program (``spawn`` default)."""
    import __main__

    spec = getattr(__main__, "__spec__", None)
    if spec is not None and spec.name:
        mod = spec.name
        if mod.endswith(".__main__"):
            mod = mod[: -len(".__main__")]
        return [sys.executable, "-m", mod, *sys.argv[1:]]
    a0 = sys.argv[0] if sys.argv else ""
    if not a0 or a0 == "-c" or not os.path.isfile(a0):
        raise WorkerError("spawn: cannot derive the worker command from sys.argv; pass argv=")
    return [sys.executable, os.path.abspath(a0), *sys.argv[1:]]


class _Checkpointer:
    """Rate-limited WORKER_CHECKPOINT sender (one per name per interval).

    All state is guarded by ``lock``; sends happen under it, so the task
    thread and the background timer never interleave requests.
    """

    def __init__(self, ctx: "Context", clock: Callable[[], float], background: bool) -> None:
        self.ctx = ctx
        self.clock = clock
        self.background = background
        self.lock = threading.RLock()
        self.pending: dict[str, tuple[Path, str]] = {}   # name -> (file, media type)
        self.last_sent: dict[str, float] = {}
        self.sent: list[tuple[str, Path]] = []            # (name, file) acknowledged, in order
        self.error: CheckpointError | None = None        # from a background send
        self.timer: threading.Timer | None = None
        self.closed = False

    def _due(self, name: str, now: float) -> bool:
        last = self.last_sent.get(name)
        return last is None or now - last >= CHECKPOINT_INTERVAL_S

    def submit(self, name: str, path: Path, media: str) -> None:
        with self.lock:
            self._raise_stored()
            self.pending[name] = (path, media)
            now = self.clock()
            for n in [n for n in self.pending if self._due(n, now)]:
                self._send(n)
            self._schedule()

    def flush(self, raise_errors: bool) -> None:
        with self.lock:
            if raise_errors:
                self._raise_stored()
            for n in list(self.pending):
                try:
                    self._send(n)
                except CheckpointError as exc:
                    if raise_errors:
                        raise
                    print(f"fireforge: checkpoint {n!r} not recorded: {exc}", file=sys.stderr)

    def close(self) -> None:
        """Flush pending checkpoints (errors are printed) and stop the timer."""
        with self.lock:
            self.closed = True
            if self.timer is not None:
                self.timer.cancel()
                self.timer = None
            self.flush(raise_errors=False)

    def _raise_stored(self) -> None:
        if self.error is not None:
            err, self.error = self.error, None
            raise err

    def _send(self, name: str) -> None:
        path, media = self.pending.pop(name)
        session = self.ctx._session
        if session is None:
            raise CheckpointError("not connected to a daemon", retryable=False)
        base = self.ctx.checkpoint_dir.resolve()
        try:
            rel = path.relative_to(base).as_posix()
        except ValueError:
            rel = str(path)
        msg = local_pb2.WorkerCheckpoint(attempt_id=self.ctx.attempt_id, path=rel,
                                         media_type=media, name=name)
        try:
            mtype, payload = session.request(_pb.WORKER_CHECKPOINT, msg, ACK_TIMEOUT_S)
        except (DaemonError, OSError) as exc:
            raise CheckpointError(f"checkpoint {name!r}: {exc}", retryable=True) from None
        if mtype == _pb.ERROR:
            raise CheckpointError(f"daemon rejected checkpoint {name!r}: {_error_text(payload)}",
                                  retryable=False)
        self.last_sent[name] = self.clock()
        self.sent.append((name, path))

    def _schedule(self) -> None:
        if not self.background or self.closed or not self.pending or self.timer is not None:
            return
        now = self.clock()
        delay = min(max(0.0, CHECKPOINT_INTERVAL_S - (now - self.last_sent.get(n, now)))
                    for n in self.pending)
        self.timer = threading.Timer(delay + 0.01, self._on_timer)
        self.timer.daemon = True
        self.timer.start()

    def _on_timer(self) -> None:
        with self.lock:
            self.timer = None
            if self.closed:
                return
            now = self.clock()
            for n in [n for n in self.pending if self._due(n, now)]:
                try:
                    self._send(n)
                except CheckpointError as exc:
                    print(f"fireforge: checkpoint {n!r} not recorded: {exc}", file=sys.stderr)
                    self.error = exc
            self._schedule()


class Context:
    """Per-attempt view of the job handed to a task function."""

    def __init__(self, task: "local_pb2.WorkerTask", env: dict[str, str],
                 session: "_Session | None" = None, *,
                 clock: Callable[[], float] | None = None,
                 background_flush: bool = True, detached: bool = False) -> None:
        self.job_id: str = task.job_id or env.get("FIREFORGE_JOB_ID", "")
        self.attempt_id: str = task.attempt_id or env.get("FIREFORGE_ATTEMPT_ID", "")
        self.attempt_number: int = task.attempt_number
        self.operation: str = task.operation
        self.campaign_id: str = task.campaign_id
        self.material_id: str = task.material_id
        self.parameters_json: str = task.parameters_json
        self.params: dict[str, Any] = json.loads(task.parameters_json) if task.parameters_json.strip() else {}
        self.inputs: list[InputRef] = [InputRef(i.artifact_id, Path(i.path), i.name) for i in task.inputs]
        # name -> path for named inputs (first one wins if a name repeats).
        self.named_inputs: dict[str, Path] = {}
        for ref in self.inputs:
            if ref.name:
                self.named_inputs.setdefault(ref.name, ref.path)
        # Phase 6: workflow step identity and checkpointing.
        self.workflow_id: str = task.workflow_id
        self.step: str = task.step
        self.checkpoint_enabled: bool = task.checkpoint_enabled
        self.resume_checkpoint: Path | None = Path(task.resume_checkpoint) if task.resume_checkpoint else None

        def _dir(task_value: str, env_key: str, default: str) -> Path:
            return Path(task_value or env.get(env_key) or default)

        self.workdir: Path = _dir("", "FIREFORGE_WORKDIR", os.getcwd())
        self.input_dir: Path = _dir("", "FIREFORGE_INPUT_DIR", str(self.workdir / "inputs"))
        self.output_dir: Path = _dir(task.output_dir, "FIREFORGE_OUTPUT_DIR", str(self.workdir / "outputs"))
        self.checkpoint_dir: Path = _dir(task.checkpoint_dir, "FIREFORGE_CHECKPOINT_DIR",
                                         str(self.workdir / "checkpoints"))
        # Phase 7: MPI rank of this process (0/1 when not under MPI).
        info = _mpi.detect(env)
        self.mpi_rank: int = info.rank
        self.mpi_size: int = info.size
        # A detached context (non-root MPI rank) never talks to the daemon:
        # progress / checkpoints are no-ops, cancellation never fires.
        self.detached: bool = detached
        self._session = None if detached else session
        self._cancel_event = threading.Event()
        self.cancel_reason: str = ""
        self._saved: dict[str, OutputRef] = {}  # by relative path, insertion-ordered
        self._spawned: list[Any] = []           # job_pb2.StepSpec, in call order
        self._ck = _Checkpointer(self, clock or time.monotonic, background_flush)

    @property
    def is_root(self) -> bool:
        """True for MPI rank 0 (and outside MPI): the rank whose outputs count."""
        return self.mpi_rank == 0

    # -- cancellation -------------------------------------------------------

    @property
    def cancelled(self) -> bool:
        return self._cancel_event.is_set()

    def check_cancelled(self) -> None:
        """Raise ``Cancelled`` if the daemon has cancelled this attempt."""
        if self._cancel_event.is_set():
            raise Cancelled(f"cancelled: {self.cancel_reason}" if self.cancel_reason else "cancelled")

    def wait_cancelled(self, timeout: float | None = None) -> bool:
        """Sleep up to ``timeout`` seconds; return True as soon as cancelled."""
        return self._cancel_event.wait(timeout)

    def _set_cancelled(self, reason: str) -> None:
        self.cancel_reason = reason
        self._cancel_event.set()

    # -- progress -----------------------------------------------------------

    def report_progress(self, fraction: float, message: str = "") -> None:
        """Send WORKER_PROGRESS. ``fraction`` is clamped to [0, 1]."""
        try:
            fraction = float(fraction)
        except (TypeError, ValueError):
            fraction = 0.0
        if math.isnan(fraction):
            fraction = 0.0
        fraction = min(1.0, max(0.0, fraction))
        if self._session is not None:
            self._session.send_progress(self.attempt_id, fraction, str(message))

    # -- outputs ------------------------------------------------------------

    def _resolve_output(self, rel: str | os.PathLike) -> tuple[Path, str]:
        """Return (absolute path, path relative to output_dir); reject escapes."""
        p = Path(rel)
        base = self.output_dir.resolve()
        target = (p if p.is_absolute() else self.output_dir / p).resolve()
        try:
            relative = target.relative_to(base)
        except ValueError:
            raise WorkerError(f"output path {str(rel)!r} is outside the output directory") from None
        if str(relative) in ("", "."):
            raise WorkerError(f"output path {str(rel)!r} is the output directory itself")
        return target, relative.as_posix()

    def save_output(self, name: str, data: bytes | str | dict | list | os.PathLike,
                    media_type: str | None = None, filename: str | None = None) -> OutputRef:
        """Write ``data`` into output_dir and return a reference for JobResult.artifacts.

        bytes are written as is, str as UTF-8 text, dict/list as JSON, and a
        Path (or other os.PathLike) is copied. ``filename`` defaults to
        ``name`` plus an extension derived from the data/media type.
        """
        if not name:
            raise ValueError("output name must not be empty")
        src: Path | None = None
        if isinstance(data, os.PathLike):
            src = Path(data)
            if not src.is_file():
                raise WorkerError(f"save_output({name!r}): {src} is not a file")
            default_media = mimetypes.guess_type(src.name)[0] or "application/octet-stream"
            default_ext = "".join(src.suffixes)
            payload = None
        elif isinstance(data, (bytes, bytearray, memoryview)):
            payload, default_media, default_ext = bytes(data), "application/octet-stream", ".bin"
        elif isinstance(data, str):
            payload, default_media, default_ext = data.encode(), "text/plain", ".txt"
        else:
            payload, default_media, default_ext = _to_json_bytes(data), "application/json", ".json"

        media = media_type or default_media
        if filename is None:
            ext = (mimetypes.guess_extension(media) if media_type else None) or default_ext
            if media == "application/json":
                ext = ".json"
            filename = name if Path(name).suffix else name + ext
        target, rel = self._resolve_output(filename)
        target.parent.mkdir(parents=True, exist_ok=True)
        if src is not None:
            shutil.copyfile(src, target)
        else:
            fd, tmp = tempfile.mkstemp(dir=target.parent, prefix=".tmp-")
            try:
                with os.fdopen(fd, "wb") as f:
                    f.write(payload)
                os.replace(tmp, target)
            except BaseException:
                try:
                    os.unlink(tmp)
                except FileNotFoundError:
                    pass
                raise
        ref = OutputRef(name=name, path=rel, media_type=media)
        self._saved[rel] = ref
        return ref

    def save_result(self, obj: Any) -> OutputRef:
        """Save ``obj`` as JSON under the logical name "result" (result.json)."""
        return self.save_output("result", _to_json_bytes(obj), "application/json", "result.json")

    # -- inputs -------------------------------------------------------------

    def load_structure(self, *, raw: bool = False):
        """Load the first input that looks like a crystal structure.

        Inputs are matched by file name (.cif, .json, .vasp, POSCAR) or, since
        materialized artifacts are named by hash, by content sniffing (JSON
        object, CIF ``data_`` block). If pymatgen is importable (and ``raw`` is
        False) a ``pymatgen.core.Structure`` is returned; otherwise a
        ``StructureFile(path, format, text)``. Raises WorkerError (not
        retryable) if no input looks like a structure.
        """
        for inp in self.inputs:
            path = inp.path
            try:
                with open(path, "r", encoding="utf-8", errors="replace") as f:
                    head = f.read(4096)
            except OSError:
                continue
            fmt = _sniff_structure_format(path, head)
            if fmt is None:
                continue
            text = path.read_text(encoding="utf-8")
            if not raw:
                try:
                    from pymatgen.core import Structure  # type: ignore
                except ImportError:
                    pass
                else:
                    if fmt == "json":
                        return Structure.from_dict(json.loads(text))
                    return Structure.from_str(text, fmt=fmt)
            return StructureFile(path=path, format=fmt, text=text)
        raise WorkerError("no structure input (.cif/.json/POSCAR) found among the job inputs")

    def input_path(self, index: int = 0) -> Path:
        return self.inputs[index].path

    def input(self, name: str) -> Path:
        """Path of the input named ``name`` (JobSpec.input_names / step wiring).

        Raises KeyError listing the available names if there is none.
        """
        try:
            return self.named_inputs[name]
        except KeyError:
            avail = ", ".join(sorted(self.named_inputs)) or "none"
            raise KeyError(f"no input named {name!r} (named inputs: {avail}; "
                           f"{len(self.inputs)} input(s) in total)") from None

    # -- checkpoints (phase6.md §6) -----------------------------------------

    def checkpoint(self, data: bytes | str | dict | list | os.PathLike, name: str = "checkpoint",
                   media_type: str | None = None) -> Path:
        """Record a checkpoint; a retry (here or on another node) can resume from it.

        ``data`` is written into ``checkpoint_dir`` like ``save_output`` does
        (bytes as is, str as UTF-8, dict/list as JSON) as ``<name><ext>``. A
        Path inside ``checkpoint_dir`` is used in place; any other Path is
        copied in as ``<name><its suffixes>``. Returns the checkpoint file.

        Rate limit: per ``name`` at most one WORKER_CHECKPOINT every
        ``CHECKPOINT_INTERVAL_S`` (10 s). A call inside the window only
        updates the file and marks it pending; the pending checkpoint is sent
        (latest content wins) by a background timer when the window ends, by
        the next ``checkpoint()`` call after the window, by
        ``flush_checkpoints()``, and in any case before the RESULT is sent
        (also when the task fails). A call that sends waits for the daemon's
        reply and raises CheckpointError on ERROR / no reply; an error from a
        background send is printed and raised by the next ``checkpoint()`` /
        ``flush_checkpoints()`` call.

        Raises CheckpointError before writing anything if checkpointing is not
        enabled for the job (``ExecutionPolicy.checkpoint``).

        In a detached context (non-root MPI rank) nothing is written or sent
        (ranks on the head node share rank 0's ``checkpoint_dir``); the path
        the file would have is returned.
        """
        if self.detached:
            if not isinstance(name, str) or not STEP_NAME_RE.fullmatch(name) or name in (".", ".."):
                raise ValueError(f"invalid checkpoint name {name!r}: must match [A-Za-z0-9_.-]{{1,64}}")
            if isinstance(data, os.PathLike):
                ext = "".join(Path(data).suffixes)
            elif isinstance(data, (bytes, bytearray, memoryview)):
                ext = ".bin"
            elif isinstance(data, str):
                ext = ".txt"
            else:
                ext = ".json"
            return self.checkpoint_dir / (name if Path(name).suffix else name + ext)
        if not self.checkpoint_enabled:
            raise CheckpointError("checkpointing not enabled for this job "
                                  "(ExecutionPolicy.checkpoint / spawn(..., checkpoint=True))")
        if not isinstance(name, str) or not STEP_NAME_RE.fullmatch(name) or name in (".", ".."):
            raise ValueError(f"invalid checkpoint name {name!r}: must match [A-Za-z0-9_.-]{{1,64}}")
        target, media = self._write_checkpoint_file(data, name, media_type)
        self._ck.submit(name, target, media)
        return target

    def flush_checkpoints(self) -> None:
        """Send every pending (rate-limited) checkpoint now and wait for the replies."""
        if self.detached:
            return
        self._ck.flush(raise_errors=True)

    def _write_checkpoint_file(self, data, name: str, media_type: str | None) -> tuple[Path, str]:
        base = self.checkpoint_dir
        base.mkdir(parents=True, exist_ok=True)
        rbase = base.resolve()
        if isinstance(data, os.PathLike):
            src = Path(data)
            if not src.is_file():
                raise CheckpointError(f"checkpoint({name!r}): {src} is not a file")
            media = media_type or mimetypes.guess_type(src.name)[0] or "application/octet-stream"
            rsrc = src.resolve()
            if rsrc.is_relative_to(rbase) and rsrc != rbase:
                return rsrc, media
            filename = name if Path(name).suffix else name + "".join(src.suffixes)
            target = rbase / filename
            fd, tmp = tempfile.mkstemp(dir=rbase, prefix=".tmp-")
            os.close(fd)
            try:
                shutil.copyfile(src, tmp)
                os.replace(tmp, target)
            except BaseException:
                _unlink_quiet(tmp)
                raise
            return target, media
        if isinstance(data, (bytes, bytearray, memoryview)):
            payload, default_media, default_ext = bytes(data), "application/octet-stream", ".bin"
        elif isinstance(data, str):
            payload, default_media, default_ext = data.encode(), "text/plain", ".txt"
        else:
            payload, default_media, default_ext = _to_json_bytes(data), "application/json", ".json"
        media = media_type or default_media
        ext = (mimetypes.guess_extension(media) if media_type else None) or default_ext
        if media == "application/json":
            ext = ".json"
        target = rbase / (name if Path(name).suffix else name + ext)
        fd, tmp = tempfile.mkstemp(dir=rbase, prefix=".tmp-")
        try:
            with os.fdopen(fd, "wb") as f:
                f.write(payload)
            os.replace(tmp, target)
        except BaseException:
            _unlink_quiet(tmp)
            raise
        return target, media

    def load_checkpoint(self) -> Path | None:
        """Path of the checkpoint to resume from (``WorkerTask.resume_checkpoint``), or None.

        Set when a previous attempt of this job recorded a checkpoint; the file
        is materialized under ``checkpoint_dir/resume/``.
        """
        return self.resume_checkpoint

    def load_checkpoint_json(self, default: Any = None) -> Any:
        """Parsed JSON of ``load_checkpoint()``, or ``default`` if there is none."""
        p = self.load_checkpoint()
        if p is None:
            return default
        return json.loads(p.read_text(encoding="utf-8"))

    # -- dynamic fan-out (phase6.md §5) -------------------------------------

    def spawn(self, name: str, *, operation: str | None = None, argv: list[str] | None = None,
              params: Any = None, inputs=(), depends_on=(), resources: dict | None = None,
              run_if: str = "all_succeeded", allow_failure: bool = False, input_artifacts=(),
              capabilities=(), max_attempts: int | None = None, timeout: float | None = None,
              checkpoint: bool = False, env: dict[str, str] | None = None,
              ranks: int | None = None, max_ranks_per_node: int | None = None,
              mpirun_args=()) -> str:
        """Add a step to this job's workflow; returns ``name``.

        The step is stored as ``<this step>/<name>`` by the control plane and
        only takes effect if this attempt succeeds (sent in
        ``WorkerResult.spawn``). ``inputs``: ``"step"``, ``"step:output"`` or
        ``StepInput``; ``"."`` is this step's own outputs, other names are
        siblings spawned in the same call set (short name) or full step names.
        ``depends_on`` likewise. ``run_if``: "all_succeeded" | "any_failed" |
        "always". ``resources``: dict of cpu_cores / memory_bytes /
        disk_bytes / gpu_count. ``input_artifacts``: artifact ids, or a dict
        ``{input_name: artifact_id}``. ``timeout`` in seconds.
        ``checkpoint=True`` enables checkpoints for the child.
        ``ranks=N`` (1..4096) makes the child a multi-node MPI gang job
        (``JobSpec.mpi``; ``resources`` then per rank); ``max_ranks_per_node``
        caps ranks per node (0/None = no cap), ``mpirun_args`` go before the
        program. ``argv`` must not start with mpirun/mpiexec (TDR adds it).
        In a detached context (non-root MPI rank) spawns are never sent.

        ``argv=None`` runs the same worker program as this process
        (``[sys.executable, <abs path of sys.argv[0]>, *sys.argv[1:]]``, or
        ``[sys.executable, "-m", <module>, ...]`` when started with ``-m``),
        so the child is this script dispatching on ``operation``; the
        child's ``command.env`` then defaults to ``{"PYTHONPATH": ...}`` from
        this process (when set), because jobs start with a minimal environment.
        ``env`` (dict) is merged over that default (or used alone with an
        explicit ``argv``). Note that
        the interpreter and script paths must also exist on the node that
        runs the child; pass ``argv`` explicitly otherwise. At least one of
        ``operation`` / ``argv`` is required.

        Outside a workflow (``workflow_id == ""``) the control plane rejects
        spawns (the job fails); the SDK does not check this.
        """
        if operation is None and argv is None:
            raise ValueError(f"spawn({name!r}): pass operation= (and/or argv=)")
        if any(s.name == name for s in self._spawned):
            raise ValueError(f"spawn({name!r}): duplicate spawned step name")
        if len(self._spawned) >= MAX_SPAWN:
            raise ValueError(f"spawn({name!r}): at most {MAX_SPAWN} steps per expansion")
        if argv is None:
            argv = default_worker_argv()
            base_env = {"PYTHONPATH": os.environ["PYTHONPATH"]} if os.environ.get("PYTHONPATH") else {}
            env = {**base_env, **(env or {})}
        try:
            spec = build_step_spec(name, operation=operation, argv=argv, params=params, inputs=inputs,
                                   depends_on=depends_on, resources=resources, run_if=run_if,
                                   allow_failure=allow_failure, input_artifacts=input_artifacts,
                                   capabilities=capabilities, max_attempts=max_attempts,
                                   timeout=timeout, checkpoint=checkpoint, env=env,
                                   ranks=ranks, max_ranks_per_node=max_ranks_per_node,
                                   mpirun_args=mpirun_args, allow_self_input=True)
        except WorkflowError as exc:
            raise ValueError(f"spawn: {exc}") from None
        self._spawned.append(spec)
        return name

    @property
    def spawned(self) -> list:
        """StepSpecs added with ``spawn`` so far (copies)."""
        out = []
        for s in self._spawned:
            c = type(s)()
            c.CopyFrom(s)
            out.append(c)
        return out

    # -- result assembly ----------------------------------------------------

    def _collect_outputs(self, artifacts) -> list[OutputRef]:
        """Validate declared outputs, or collect everything in output_dir."""
        refs: dict[str, OutputRef] = {}
        if artifacts is None:
            refs.update(self._saved)
            if self.output_dir.is_dir():
                for p in sorted(self.output_dir.rglob("*")):
                    if p.is_symlink() or not p.is_file() or p.name.startswith(".tmp-"):
                        continue
                    rel = p.relative_to(self.output_dir).as_posix()
                    if rel not in refs:
                        media = mimetypes.guess_type(p.name)[0] or "application/octet-stream"
                        refs[rel] = OutputRef(name=rel, path=rel, media_type=media)
            # Re-validate saved refs (files may have been removed meanwhile).
            artifacts = list(refs.values())
            refs = {}
        for item in artifacts:
            if isinstance(item, OutputRef):
                target, rel = self._resolve_output(item.path)
                ref = OutputRef(item.name, rel, item.media_type)
            elif isinstance(item, (str, os.PathLike)):
                target, rel = self._resolve_output(item)
                ref = self._saved.get(rel) or OutputRef(
                    name=rel, path=rel,
                    media_type=mimetypes.guess_type(rel)[0] or "application/octet-stream")
            else:
                raise WorkerError(f"unsupported artifact entry {item!r} (use OutputRef or a path)")
            if not target.is_file():
                raise WorkerError(f"declared output {rel!r} does not exist or is not a regular file")
            refs.setdefault(rel, ref)
        names: dict[str, str] = {}
        for ref in refs.values():
            if ref.name in names:
                raise WorkerError(f"duplicate output name {ref.name!r} ({names[ref.name]!r} and "
                                  f"{ref.path!r}); output names must be unique")
            names[ref.name] = ref.path
        return list(refs.values())


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------


class DaemonError(Exception):
    """The daemon replied with ERROR or broke the protocol."""


class _Session:
    """One connection to the daemon: writer (locked) + background reader."""

    def __init__(self, sock: socket.socket) -> None:
        self.sock = sock
        self.writer = FrameWriter(sock)
        self.reader = FrameReader(sock)
        self.replies: "queue.Queue[tuple[int, int, bytes] | None]" = queue.Queue()
        self.context: Context | None = None
        self._thread: threading.Thread | None = None
        self._broken = False
        # Requests waiting for a reply: message_id -> one-slot queue.
        self._plock = threading.Lock()
        self._pending: dict[int, "queue.Queue[tuple[int, bytes] | None]"] = {}
        self._closed = False

    def send(self, msg_type: int, msg, correlation_id: int = 0) -> int:
        return self.writer.send(msg_type, msg, correlation_id)

    def request(self, msg_type: int, msg, timeout: float) -> tuple[int, bytes]:
        """Send ``msg`` and wait for the WORKER_ACK/ERROR answering it.

        Safe from any thread once the reader runs: the waiter is registered
        under the request's message id before the frame is written, and the
        reader hands each reply to the waiter whose id it correlates to.
        Raises DaemonError if the connection closes or ``timeout`` passes.
        """
        slot: "queue.Queue[tuple[int, bytes] | None]" = queue.Queue(maxsize=1)
        ids: list[int] = []

        def register(mid: int) -> None:
            with self._plock:
                if self._closed:
                    raise DaemonError("connection closed")
                self._pending[mid] = slot
            ids.append(mid)

        try:
            self.writer.send(msg_type, msg, 0, on_id=register)
            try:
                item = slot.get(timeout=timeout)
            except queue.Empty:
                raise DaemonError(f"no reply to {_pb.type_name(msg_type)} within {timeout:.0f}s") from None
            if item is None:
                raise DaemonError(f"connection closed before the reply to {_pb.type_name(msg_type)}")
            return item
        finally:
            with self._plock:
                for mid in ids:
                    self._pending.pop(mid, None)

    def _dispatch(self, mtype: int, corr: int, payload: bytes) -> None:
        slot = None
        if mtype in (_pb.WORKER_ACK, _pb.ERROR):
            with self._plock:
                slot = self._pending.pop(corr, None)
                if slot is None and corr == 0 and len(self._pending) == 1:
                    # Uncorrelated reply with exactly one request in flight.
                    slot = self._pending.pop(next(iter(self._pending)))
        if slot is not None:
            slot.put((mtype, payload))
        else:
            self.replies.put((mtype, corr, payload))

    def send_progress(self, attempt_id: str, fraction: float, message: str) -> None:
        if self._broken:
            return
        try:
            self.send(_pb.WORKER_PROGRESS,
                      local_pb2.WorkerProgress(attempt_id=attempt_id, fraction=fraction, message=message))
        except OSError as exc:
            self._broken = True
            print(f"fireforge: progress not delivered: {exc}", file=sys.stderr)

    def start_reader(self, context: Context) -> None:
        self.context = context
        self._thread = threading.Thread(target=self._read_loop, name="fireforge-reader", daemon=True)
        self._thread.start()

    def _read_loop(self) -> None:
        try:
            for header, payload in self.reader:
                if header.message_type == _pb.WORKER_CANCEL:
                    msg = local_pb2.WorkerCancel.FromString(payload)
                    if self.context is not None:
                        self.context._set_cancelled(msg.reason)
                else:
                    self._dispatch(header.message_type, header.correlation_id, payload)
        except Exception as exc:  # reader must never crash loudly
            print(f"fireforge: connection error: {exc}", file=sys.stderr)
        finally:
            with self._plock:
                self._closed = True
                waiting = list(self._pending.values())
                self._pending.clear()
            for slot in waiting:
                slot.put(None)
            self.replies.put(None)

    def close(self) -> None:
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self.sock.close()


def _error_text(payload: bytes) -> str:
    try:
        err = local_pb2.Error.FromString(payload)
        return f"{local_pb2.ErrorCode.Name(err.code)}: {err.message}"
    except Exception:
        return "<undecodable Error payload>"


# ---------------------------------------------------------------------------
# Worker
# ---------------------------------------------------------------------------


TaskFn = Callable[[Context], Any]


class Worker:
    """Registry of task functions plus the protocol driver."""

    def __init__(self) -> None:
        self.tasks: dict[str, TaskFn] = {}

    def task(self, operation: str) -> Callable[[TaskFn], TaskFn]:
        """Decorator registering ``fn`` for ``JobSpec.operation == operation``."""
        if not operation:
            raise ValueError("operation name must not be empty")

        def register(fn: TaskFn) -> TaskFn:
            if operation in self.tasks:
                raise ValueError(f"task {operation!r} already registered")
            self.tasks[operation] = fn
            return fn

        return register

    # -- entry point --------------------------------------------------------

    def run(self, *, exit: bool = True, env: dict[str, str] | None = None,
            mpi_operation: str | None = None) -> int:
        """Run one attempt. Calls sys.exit(code) unless ``exit=False``.

        Under MPI (``fireforge.mpi.detect``) rank 0 runs exactly as without
        MPI. Any other rank never connects to the daemon: it runs the task
        for ``FIREFORGE_MPI_OPERATION`` (env), else ``mpi_operation``, else
        the only registered task, with a detached context; parameters come
        from ``FIREFORGE_MPI_PARAMS`` (JSON, default ``{}``). See
        ``_run_detached``.
        """
        env = dict(os.environ) if env is None else env
        info = _mpi.detect(env)
        if info.active and not info.is_root:
            code = self._run_detached(env, mpi_operation)
        else:
            code = self._run(env)
        sys.stdout.flush()
        sys.stderr.flush()
        if exit:
            sys.exit(code)
        return code

    def _run(self, env: dict[str, str]) -> int:
        missing = [k for k in REQUIRED_ENV if not env.get(k)]
        if missing:
            print("fireforge: missing environment: " + ", ".join(missing) +
                  "\nThis script is a FireForge worker; it must be launched by tdrd "
                  "(or fireforge.testing.FakeDaemon).", file=sys.stderr)
            return EXIT_CONFIG
        sock_path = env["FIREFORGE_IPC_SOCKET"]
        attempt_id = env["FIREFORGE_ATTEMPT_ID"]

        try:
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            sock.settimeout(HANDSHAKE_TIMEOUT_S)
            sock.connect(sock_path)
        except OSError as exc:
            print(f"fireforge: cannot connect to {sock_path}: {exc}", file=sys.stderr)
            return EXIT_FAILURE

        session = _Session(sock)
        try:
            return self._serve(session, env, attempt_id)
        except (OSError, ProtocolError, DaemonError) as exc:
            print(f"fireforge: {exc}", file=sys.stderr)
            return EXIT_FAILURE
        finally:
            session.close()

    def _run_detached(self, env: dict[str, str], mpi_operation: str | None) -> int:
        """Non-root MPI rank: run the task locally, never talk to the daemon.

        The context has the local node's job dirs (FIREFORGE_WORKDIR, ...),
        job/attempt ids from env, ``inputs`` = the files in ``input_dir``
        (unnamed; inputs are materialised on every member node) and no
        resume checkpoint. ``save_output`` writes the file (on the head node
        that is rank 0's ``output_dir``, so guard writes with
        ``context.is_root`` unless that is intended); checkpoints, progress
        and spawns are no-ops; cancellation never fires (tdrd stops the gang
        by killing mpirun). The return value is ignored except that an
        explicit failure (``JobResult(success=False)`` / ``False``) exits 1
        like an exception does (traceback on stderr). Exit 2: no task to run
        or bad ``FIREFORGE_MPI_PARAMS``.
        """
        rank = _mpi.detect(env).rank
        op = env.get(_mpi.ENV_OPERATION) or mpi_operation
        if op:
            fn = self.tasks.get(op)
            if fn is None:
                known = ", ".join(sorted(self.tasks)) or "none"
                print(f"fireforge: MPI rank {rank}: unknown operation {op!r} (registered: {known})",
                      file=sys.stderr)
                return EXIT_CONFIG
        elif len(self.tasks) == 1:
            op, fn = next(iter(self.tasks.items()))
        else:
            known = ", ".join(sorted(self.tasks)) or "none"
            print(f"fireforge: MPI rank {rank}: cannot choose a task (registered: {known}); set "
                  f"{_mpi.ENV_OPERATION} or pass Worker.run(mpi_operation=...)", file=sys.stderr)
            return EXIT_CONFIG
        params_json = env.get(_mpi.ENV_PARAMS, "")
        try:
            if params_json.strip():
                json.loads(params_json)
        except ValueError as exc:
            print(f"fireforge: MPI rank {rank}: invalid {_mpi.ENV_PARAMS}: {exc}", file=sys.stderr)
            return EXIT_CONFIG
        task = local_pb2.WorkerTask(operation=op, parameters_json=params_json)
        context = Context(task, env, None, detached=True, background_flush=False)
        input_dir = context.input_dir
        if input_dir.is_dir():
            for p in sorted(input_dir.iterdir()):
                if p.is_file():
                    aid = (f"artifact://sha256/{p.name}" if len(p.name) == 64
                           and all(c in "0123456789abcdef" for c in p.name) else "")
                    context.inputs.append(InputRef(aid, p, ""))
        try:
            value = fn(context)
        except Exception:
            traceback.print_exc(file=sys.stderr)
            return EXIT_FAILURE
        if value is False or (isinstance(value, JobResult) and not value.success):
            err = value.error if isinstance(value, JobResult) else ""
            print(f"fireforge: MPI rank {rank}: task reported failure{': ' + err if err else ''}",
                  file=sys.stderr)
            return EXIT_FAILURE
        return EXIT_OK

    def _serve(self, session: _Session, env: dict[str, str], attempt_id: str) -> int:
        hello = local_pb2.WorkerHello(attempt_id=attempt_id, token=env["FIREFORGE_ATTEMPT_TOKEN"],
                                      pid=os.getpid(), sdk_version=__version__)
        hello_id = session.send(_pb.WORKER_HELLO, hello)
        frame = session.reader.read_frame()
        if frame is None:
            raise DaemonError("daemon closed the connection after HELLO")
        header, payload = frame
        if header.message_type == _pb.ERROR:
            raise DaemonError(f"daemon refused the attempt: {_error_text(payload)}")
        if header.message_type != _pb.WORKER_TASK:
            raise DaemonError(f"expected WORKER_TASK, got {_pb.type_name(header.message_type)}")
        if header.correlation_id not in (0, hello_id):
            raise DaemonError(f"WORKER_TASK correlation_id {header.correlation_id} != {hello_id}")
        task = local_pb2.WorkerTask.FromString(payload)
        session.sock.settimeout(None)

        try:
            context = Context(task, env, session)
        except ValueError as exc:  # bad parameters_json
            context = Context(local_pb2.WorkerTask(attempt_id=task.attempt_id), env, session)
            result = local_pb2.WorkerResult(attempt_id=context.attempt_id, success=False,
                                            error=f"invalid parameters_json: {exc}", retryable=False)
            session.start_reader(context)
        else:
            session.start_reader(context)
            result = self._execute(context, task.operation)

        # Pending (rate-limited) checkpoints go out before the result, even on
        # failure: a retry should resume from the latest state.
        context._ck.close()
        try:
            mtype, payload = session.request(_pb.WORKER_RESULT, result, ACK_TIMEOUT_S)
        except DaemonError as exc:
            print(f"fireforge: no ACK for WORKER_RESULT: {exc}", file=sys.stderr)
        else:
            if mtype == _pb.ERROR:
                print(f"fireforge: daemon rejected the result: {_error_text(payload)}", file=sys.stderr)
                return EXIT_FAILURE
        return EXIT_OK if result.success else EXIT_FAILURE

    def _execute(self, context: Context, operation: str) -> "local_pb2.WorkerResult":
        res = local_pb2.WorkerResult(attempt_id=context.attempt_id)
        fn = self.tasks.get(operation)
        if fn is None:
            known = ", ".join(sorted(self.tasks)) or "none"
            res.success, res.retryable = False, False
            res.error = f"unknown operation {operation!r} (registered: {known})"
            print(f"fireforge: {res.error}", file=sys.stderr)
            return res
        try:
            value = fn(context)
            outcome = self._normalize(context, value)
            outputs = context._collect_outputs(outcome.artifacts) if outcome.success else []
        except WorkerError as exc:  # includes Cancelled
            res.success, res.retryable = False, exc.retryable
            res.error = f"{type(exc).__name__}: {exc}"
            print(f"fireforge: task failed: {res.error}", file=sys.stderr)
            return res
        except Exception as exc:
            traceback.print_exc(file=sys.stderr)
            res.success, res.retryable = False, True
            res.error = f"{type(exc).__name__}: {exc}"
            return res
        res.success = bool(outcome.success)
        if res.success:
            for ref in outputs:
                res.outputs.add(path=ref.path, media_type=ref.media_type, name=ref.name)
            res.spawn.extend(context._spawned)
        else:
            res.error = outcome.error or "task reported failure"
            res.retryable = bool(outcome.retryable)
        return res

    @staticmethod
    def _normalize(context: Context, value: Any) -> JobResult:
        if value is None:
            return JobResult(success=True)
        if isinstance(value, JobResult):
            return value
        if isinstance(value, bool):
            return JobResult(success=value)
        if isinstance(value, dict):
            context.save_result(value)
            return JobResult(success=True)
        raise WorkerError(f"task returned unsupported type {type(value).__name__} "
                          "(expected JobResult, dict, bool or None)")
