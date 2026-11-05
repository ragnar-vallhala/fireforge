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

Flow: HELLO -> TASK, PROGRESS*, RESULT -> ACK. The daemon may send CANCEL at
any time; a background reader thread sets ``context.cancelled``.

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

from . import _pb
from ._pb import local_pb2
from ._version import __version__
from .protocol import FrameReader, FrameWriter, ProtocolError

ACK_TIMEOUT_S = 30.0
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


class Context:
    """Per-attempt view of the job handed to a task function."""

    def __init__(self, task: "local_pb2.WorkerTask", env: dict[str, str],
                 session: "_Session | None" = None) -> None:
        self.job_id: str = task.job_id or env.get("FIREFORGE_JOB_ID", "")
        self.attempt_id: str = task.attempt_id or env.get("FIREFORGE_ATTEMPT_ID", "")
        self.attempt_number: int = task.attempt_number
        self.operation: str = task.operation
        self.campaign_id: str = task.campaign_id
        self.material_id: str = task.material_id
        self.parameters_json: str = task.parameters_json
        self.params: dict[str, Any] = json.loads(task.parameters_json) if task.parameters_json.strip() else {}
        self.inputs: list[InputRef] = [InputRef(i.artifact_id, Path(i.path)) for i in task.inputs]

        def _dir(task_value: str, env_key: str, default: str) -> Path:
            return Path(task_value or env.get(env_key) or default)

        self.workdir: Path = _dir("", "FIREFORGE_WORKDIR", os.getcwd())
        self.input_dir: Path = _dir("", "FIREFORGE_INPUT_DIR", str(self.workdir / "inputs"))
        self.output_dir: Path = _dir(task.output_dir, "FIREFORGE_OUTPUT_DIR", str(self.workdir / "outputs"))
        self.checkpoint_dir: Path = _dir(task.checkpoint_dir, "FIREFORGE_CHECKPOINT_DIR",
                                         str(self.workdir / "checkpoints"))
        self._session = session
        self._cancel_event = threading.Event()
        self.cancel_reason: str = ""
        self._saved: dict[str, OutputRef] = {}  # by relative path, insertion-ordered

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
                        refs[rel] = OutputRef(name=p.stem, path=rel, media_type=media)
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
                    name=Path(rel).stem, path=rel,
                    media_type=mimetypes.guess_type(rel)[0] or "application/octet-stream")
            else:
                raise WorkerError(f"unsupported artifact entry {item!r} (use OutputRef or a path)")
            if not target.is_file():
                raise WorkerError(f"declared output {rel!r} does not exist or is not a regular file")
            refs.setdefault(rel, ref)
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

    def send(self, msg_type: int, msg, correlation_id: int = 0) -> int:
        return self.writer.send(msg_type, msg, correlation_id)

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
                    self.replies.put((header.message_type, header.correlation_id, payload))
        except Exception as exc:  # reader must never crash loudly
            print(f"fireforge: connection error: {exc}", file=sys.stderr)
        finally:
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

    def run(self, *, exit: bool = True, env: dict[str, str] | None = None) -> int:
        """Run one attempt. Calls sys.exit(code) unless ``exit=False``."""
        code = self._run(dict(os.environ) if env is None else env)
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

        result_id = session.send(_pb.WORKER_RESULT, result)
        reply = self._wait_reply(session, result_id)
        if reply is not None:
            mtype, payload = reply
            if mtype == _pb.ERROR:
                print(f"fireforge: daemon rejected the result: {_error_text(payload)}", file=sys.stderr)
                return EXIT_FAILURE
        return EXIT_OK if result.success else EXIT_FAILURE

    @staticmethod
    def _wait_reply(session: _Session, request_id: int) -> tuple[int, bytes] | None:
        """Wait for the ACK/ERROR answering ``request_id`` (None = none came)."""
        deadline = time.monotonic() + ACK_TIMEOUT_S
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                print("fireforge: no ACK for WORKER_RESULT within "
                      f"{ACK_TIMEOUT_S:.0f}s", file=sys.stderr)
                return None
            try:
                item = session.replies.get(timeout=remaining)
            except queue.Empty:
                continue
            if item is None:
                print("fireforge: connection closed before ACK", file=sys.stderr)
                return None
            mtype, corr, payload = item
            if mtype in (_pb.WORKER_ACK, _pb.ERROR) and corr in (0, request_id):
                return mtype, payload
            # Anything else is unexpected but harmless; keep waiting.

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
