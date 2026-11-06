"""Test helpers: an in-process stand-in for tdrd's worker socket.

``FakeDaemon`` binds a Unix socket in a temp dir, plays the daemon side of
the worker protocol (HELLO -> TASK, PROGRESS*, RESULT -> ACK) in a thread,
records what the worker sent (incl. WORKER_CHECKPOINT, replied ACK or a
configurable ERROR), and can push WORKER_CANCEL::

    with FakeDaemon(operation="demo.add", params={"a": 1}) as d:
        d.add_input(b"data_x\\n_cell_length_a 5.0\\n")
        proc = d.run_worker("my_worker.py")
        assert proc.returncode == 0
        assert d.result.success

Phase 7: ``run_worker(..., mpi_rank=1, mpi_size=4)`` (or ``mpi_env``)
launches the worker as a simulated MPI rank; a non-root rank must leave
``d.connections == 0``.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
from pathlib import Path
from typing import Any

from . import _pb, mpi as _mpi
from ._pb import local_pb2
from .protocol import FrameReader, FrameWriter

SDK_ROOT = Path(__file__).resolve().parent.parent  # directory containing the fireforge package

_MPI_FLAVOURS = {
    "ompi": ("OMPI_COMM_WORLD_RANK", "OMPI_COMM_WORLD_SIZE"),
    "pmi": ("PMI_RANK", "PMI_SIZE"),
    "pmix": ("PMIX_RANK", None),
}


def mpi_env(rank: int, size: int | None = None, *, flavour: str = "ompi",
            operation: str | None = None, params: Any = None) -> dict[str, str]:
    """Env variables that make a worker see itself as MPI rank ``rank``.

    ``flavour``: "ompi" (OMPI_COMM_WORLD_*), "pmi" (PMI_RANK/PMI_SIZE) or
    "pmix" (PMIX_RANK only, no size). ``operation`` / ``params`` set
    FIREFORGE_MPI_OPERATION / FIREFORGE_MPI_PARAMS (params JSON-encoded
    unless a str).
    """
    rank_var, size_var = _MPI_FLAVOURS[flavour]
    env = {rank_var: str(rank)}
    if size_var is not None and size is not None:
        env[size_var] = str(size)
    if operation is not None:
        env[_mpi.ENV_OPERATION] = operation
    if params is not None:
        env[_mpi.ENV_PARAMS] = params if isinstance(params, str) else json.dumps(params)
    return env


class FakeDaemon:
    """Server side of the worker protocol for one (or more) worker connections.

    Parameters:
      operation, params, campaign_id, material_id, attempt_number: the WorkerTask.
      token: expected FIREFORGE_ATTEMPT_TOKEN; a HELLO with another token gets
        ERROR(UNAUTHORIZED). ``env()`` hands out ``worker_token`` (default: token).
      cancel_after_progress: send WORKER_CANCEL after this many PROGRESS frames.
      send_ack: reply ACK to RESULT (False = never answer).
      workflow_id, step, checkpoint_enabled, resume_checkpoint: Phase 6 task
        fields. ``resume_checkpoint`` may be bytes/str content (materialized
        as ``checkpoints/resume/<resume_name>`` like tdrd does) or a path.
      checkpoint_error: reply ERROR(checkpoint_error_code, this message) to
        WORKER_CHECKPOINT instead of ACK. Without it, checkpoints are refused
        with BAD_REQUEST "checkpointing not enabled for this job" when
        ``checkpoint_enabled`` is False (like tdrd), else ACKed.

    Recorded: ``checkpoints`` (WorkerCheckpoint messages, in order),
    ``checkpoint_files`` (file content at receive time, same order),
    ``spawned`` (StepSpecs from the result), ``records`` (CalcRecords from
    the result).
    """

    def __init__(self, operation: str = "test.op", params: dict | str | None = None, *,
                 job_id: str = "job-test", attempt_id: str = "att-test", attempt_number: int = 1,
                 token: str = "s3cret", worker_token: str | None = None,
                 campaign_id: str = "", material_id: str = "",
                 cancel_after_progress: int | None = None, cancel_reason: str = "test cancel",
                 send_ack: bool = True, root: str | os.PathLike | None = None,
                 workflow_id: str = "", step: str = "", checkpoint_enabled: bool = False,
                 resume_checkpoint: bytes | str | os.PathLike | None = None,
                 resume_name: str = "checkpoint.json",
                 checkpoint_error: str | None = None,
                 checkpoint_error_code: int = local_pb2.BAD_REQUEST) -> None:
        self.operation = operation
        if params is None:
            self.parameters_json = ""
        elif isinstance(params, str):
            self.parameters_json = params
        else:
            self.parameters_json = json.dumps(params)
        self.job_id, self.attempt_id, self.attempt_number = job_id, attempt_id, attempt_number
        self.token = token
        self.worker_token = token if worker_token is None else worker_token
        self.campaign_id, self.material_id = campaign_id, material_id
        self.cancel_after_progress = cancel_after_progress
        self.cancel_reason = cancel_reason
        self.send_ack = send_ack

        # Unix socket paths are limited to ~107 bytes: keep the root short.
        self._own_root = root is None
        self.root = Path(root) if root else Path(tempfile.mkdtemp(prefix="ffd-"))
        self.socket_path = self.root / "worker.sock"
        self.workdir = self.root / "attempt"
        self.input_dir = self.workdir / "inputs"
        self.output_dir = self.workdir / "outputs"
        self.checkpoint_dir = self.workdir / "checkpoints"
        for d in (self.input_dir, self.output_dir, self.checkpoint_dir):
            d.mkdir(parents=True, exist_ok=True)
        self.inputs: list[local_pb2.InputFile] = []
        self.workflow_id, self.step = workflow_id, step
        self.checkpoint_enabled = checkpoint_enabled
        self.checkpoint_error, self.checkpoint_error_code = checkpoint_error, checkpoint_error_code
        self.resume_checkpoint = ""
        if isinstance(resume_checkpoint, os.PathLike):
            self.resume_checkpoint = str(resume_checkpoint)
        elif resume_checkpoint is not None:
            rp = self.checkpoint_dir / "resume" / resume_name
            rp.parent.mkdir(parents=True, exist_ok=True)
            rp.write_bytes(resume_checkpoint.encode() if isinstance(resume_checkpoint, str)
                           else bytes(resume_checkpoint))
            self.resume_checkpoint = str(rp)

        # Recorded traffic.
        self.received: list[tuple[int, Any]] = []   # (message_type, decoded message)
        self.hello: local_pb2.WorkerHello | None = None
        self.progress: list[local_pb2.WorkerProgress] = []
        self.result: local_pb2.WorkerResult | None = None
        self.checkpoints: list[local_pb2.WorkerCheckpoint] = []
        self.checkpoint_files: list[bytes | None] = []
        self.cancel_sent = False
        self.errors: list[str] = []
        self.connections = 0   # accepted worker connections

        self._lock = threading.Lock()
        self._result_event = threading.Event()
        self._done_event = threading.Event()
        self._writer: FrameWriter | None = None
        self._listener: socket.socket | None = None
        self._thread: threading.Thread | None = None
        self._conns: list[socket.socket] = []
        self._stopping = False

    # -- setup --------------------------------------------------------------

    def add_input(self, data: bytes | str, artifact_id: str | None = None,
                  name: str = "") -> local_pb2.InputFile:
        """Materialize an input like tdrd does: inputs/<sha256 hex> (``name``: input name)."""
        raw = data.encode() if isinstance(data, str) else bytes(data)
        digest = hashlib.sha256(raw).hexdigest()
        path = self.input_dir / digest
        path.write_bytes(raw)
        f = local_pb2.InputFile(artifact_id=artifact_id or f"artifact://sha256/{digest}", path=str(path),
                                name=name)
        self.inputs.append(f)
        return f

    def env(self, *, include_os_environ: bool = True, pythonpath: bool = True) -> dict[str, str]:
        """Environment for launching a worker against this fake daemon."""
        env = dict(os.environ) if include_os_environ else {}
        # Don't let an outer MPI launcher leak in: the default is "not under MPI".
        for k in (*_mpi.RANK_VARS, *_mpi.SIZE_VARS, _mpi.ENV_OPERATION, _mpi.ENV_PARAMS):
            env.pop(k, None)
        env.update({
            "FIREFORGE_IPC_SOCKET": str(self.socket_path),
            "FIREFORGE_JOB_ID": self.job_id,
            "FIREFORGE_ATTEMPT_ID": self.attempt_id,
            "FIREFORGE_ATTEMPT_TOKEN": self.worker_token,
            "FIREFORGE_WORKDIR": str(self.workdir),
            "FIREFORGE_INPUT_DIR": str(self.input_dir),
            "FIREFORGE_OUTPUT_DIR": str(self.output_dir),
            "FIREFORGE_CHECKPOINT_DIR": str(self.checkpoint_dir),
        })
        if pythonpath:
            parts = [str(SDK_ROOT)] + [p for p in env.get("PYTHONPATH", "").split(os.pathsep) if p]
            env["PYTHONPATH"] = os.pathsep.join(dict.fromkeys(parts))
        return env

    def task_message(self) -> local_pb2.WorkerTask:
        return local_pb2.WorkerTask(
            job_id=self.job_id, attempt_id=self.attempt_id, attempt_number=self.attempt_number,
            operation=self.operation, parameters_json=self.parameters_json, inputs=self.inputs,
            output_dir=str(self.output_dir), checkpoint_dir=str(self.checkpoint_dir),
            campaign_id=self.campaign_id, material_id=self.material_id,
            resume_checkpoint=self.resume_checkpoint, workflow_id=self.workflow_id, step=self.step,
            checkpoint_enabled=self.checkpoint_enabled)

    @property
    def spawned(self) -> list:
        """StepSpecs the worker spawned (``WorkerResult.spawn``; [] without a result)."""
        return list(self.result.spawn) if self.result is not None else []

    @property
    def records(self) -> list:
        """CalcRecords the worker attached (``WorkerResult.records``; [] without a result)."""
        return list(self.result.records) if self.result is not None else []

    def checkpoint_path(self, msg: local_pb2.WorkerCheckpoint) -> Path:
        p = Path(msg.path)
        return p if p.is_absolute() else self.checkpoint_dir / p

    # -- lifecycle ----------------------------------------------------------

    def start(self) -> "FakeDaemon":
        self._listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._listener.bind(str(self.socket_path))
        self._listener.listen(4)
        self._thread = threading.Thread(target=self._accept_loop, name="fake-tdrd", daemon=True)
        self._thread.start()
        return self

    def stop(self) -> None:
        self._stopping = True
        if self._listener is not None:
            try:
                self._listener.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            self._listener.close()
        for c in list(self._conns):
            try:
                c.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
        if self._thread is not None:
            self._thread.join(timeout=5)
        if self._own_root:
            shutil.rmtree(self.root, ignore_errors=True)

    def __enter__(self) -> "FakeDaemon":
        return self.start()

    def __exit__(self, *exc) -> None:
        self.stop()

    # -- running a worker ---------------------------------------------------

    def worker_argv(self, script: str | os.PathLike) -> list[str]:
        return [sys.executable, str(script)]

    def run_worker(self, script: str | os.PathLike, timeout: float = 20.0,
                   extra_env: dict[str, str] | None = None, *, mpi_rank: int | None = None,
                   mpi_size: int | None = None, mpi_flavour: str = "ompi",
                   **kwargs) -> subprocess.CompletedProcess:
        """Run ``python script`` with this daemon's env (cwd = workdir).

        ``mpi_rank`` (with ``mpi_size``, ``mpi_flavour``) simulates an MPI
        rank via ``mpi_env``; ``extra_env`` is applied last.
        """
        env = self.env()
        if mpi_rank is not None:
            env.update(mpi_env(mpi_rank, mpi_size, flavour=mpi_flavour))
        if extra_env:
            env.update(extra_env)
        return subprocess.run(self.worker_argv(script), env=env, cwd=self.workdir,
                              capture_output=True, text=True, timeout=timeout, **kwargs)

    def wait_result(self, timeout: float = 10.0) -> local_pb2.WorkerResult | None:
        self._result_event.wait(timeout)
        return self.result

    def send_cancel(self, reason: str = "cancelled by test") -> None:
        """Push WORKER_CANCEL on the current worker connection."""
        if self._writer is None:
            raise RuntimeError("no worker connected")
        self._writer.send(_pb.WORKER_CANCEL,
                          local_pb2.WorkerCancel(attempt_id=self.attempt_id, reason=reason))
        self.cancel_sent = True

    def output_files(self) -> dict[str, bytes]:
        return {p.relative_to(self.output_dir).as_posix(): p.read_bytes()
                for p in sorted(self.output_dir.rglob("*")) if p.is_file()}

    # -- server -------------------------------------------------------------

    def _accept_loop(self) -> None:
        while not self._stopping:
            try:
                conn, _ = self._listener.accept()
            except OSError:
                return
            self._conns.append(conn)
            self.connections += 1
            try:
                self._handle(conn)
            except Exception as exc:  # recorded for assertions
                self.errors.append(f"{type(exc).__name__}: {exc}")
            finally:
                conn.close()
                self._done_event.set()

    def _record(self, mtype: int, msg: Any) -> None:
        with self._lock:
            self.received.append((mtype, msg))

    def _handle(self, conn: socket.socket) -> None:
        reader, writer = FrameReader(conn), FrameWriter(conn)
        self._writer = writer
        frame = reader.read_frame()
        if frame is None:
            return
        header, payload = frame
        if header.message_type != _pb.WORKER_HELLO:
            writer.send(_pb.ERROR, local_pb2.Error(code=local_pb2.BAD_REQUEST, message="expected HELLO"),
                        header.message_id)
            return
        hello = local_pb2.WorkerHello.FromString(payload)
        self.hello = hello
        self._record(header.message_type, hello)
        if hello.token != self.token or hello.attempt_id != self.attempt_id:
            writer.send(_pb.ERROR, local_pb2.Error(code=local_pb2.UNAUTHORIZED,
                                                   message="bad attempt token"), header.message_id)
            return
        writer.send(_pb.WORKER_TASK, self.task_message(), header.message_id)

        for header, payload in reader:
            if header.message_type == _pb.WORKER_PROGRESS:
                msg = local_pb2.WorkerProgress.FromString(payload)
                self._record(header.message_type, msg)
                self.progress.append(msg)
                if (self.cancel_after_progress is not None and not self.cancel_sent
                        and len(self.progress) >= self.cancel_after_progress):
                    self.send_cancel(self.cancel_reason)
            elif header.message_type == _pb.WORKER_CHECKPOINT:
                msg = local_pb2.WorkerCheckpoint.FromString(payload)
                self._record(header.message_type, msg)
                try:
                    content = self.checkpoint_path(msg).read_bytes()
                except OSError:
                    content = None
                with self._lock:
                    self.checkpoints.append(msg)
                    self.checkpoint_files.append(content)
                if self.checkpoint_error is not None:
                    err = local_pb2.Error(code=self.checkpoint_error_code, message=self.checkpoint_error)
                elif not self.checkpoint_enabled:
                    err = local_pb2.Error(code=local_pb2.BAD_REQUEST,
                                          message="checkpointing not enabled for this job")
                else:
                    err = None
                if err is not None:
                    writer.send(_pb.ERROR, err, header.message_id)
                else:
                    writer.send(_pb.WORKER_ACK, local_pb2.Ack(), header.message_id)
            elif header.message_type == _pb.WORKER_RESULT:
                msg = local_pb2.WorkerResult.FromString(payload)
                self._record(header.message_type, msg)
                self.result = msg
                self._result_event.set()
                if self.send_ack:
                    writer.send(_pb.WORKER_ACK, local_pb2.Ack(), header.message_id)
            else:
                self._record(header.message_type, payload)
                writer.send(_pb.ERROR, local_pb2.Error(code=local_pb2.UNSUPPORTED,
                                                       message="unexpected message"), header.message_id)
