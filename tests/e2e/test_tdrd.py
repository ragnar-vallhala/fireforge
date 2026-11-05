"""End-to-end tests: real tdrd + tdrctl + Python SDK.

Run:  tools/e2e.sh        (builds into build/dev first)
Needs TDR_BUILD (dir with runtime/daemon/tdrd), set by tools/e2e.sh.
"""
import json
import os
import re
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
BUILD = Path(os.environ.get("TDR_BUILD", ROOT / "build/dev"))
TDRD = BUILD / "runtime/daemon/tdrd"
TDRCTL = BUILD / "runtime/ctl/tdrctl"
PY = str(ROOT / ".venv/bin/python")
WORKER = str(ROOT / "sdk/python/tests/worker_script.py")
SDK_ENV = ["--env", f"PYTHONPATH={ROOT / 'sdk/python'}"]


class Daemon:
    def __init__(self, state: Path, tdrd: Path = TDRD, config: Path | None = None):
        self.state = state
        self.sock = state / "run/ctl.sock"
        self.proc = None
        self.tdrd = tdrd
        self.config = config

    def start(self):
        self.log = open(self.state.parent / "tdrd.log", "ab")
        args = [str(self.tdrd), "--state-dir", str(self.state), "--log-level", "debug"]
        if self.config:
            args += ["--config", str(self.config)]
        self.proc = subprocess.Popen(args, stdout=self.log, stderr=self.log)
        for _ in range(200):
            if self.proc.poll() is not None:
                raise RuntimeError(f"tdrd exited with {self.proc.returncode}: " +
                                   (self.state.parent / "tdrd.log").read_text()[-2000:])
            if self.sock.exists():
                try:
                    self.ctl("node")
                    return
                except subprocess.CalledProcessError:
                    pass
            time.sleep(0.02)
        raise RuntimeError("tdrd did not start")

    def stop(self, sig=signal.SIGTERM):
        if self.proc and self.proc.poll() is None:
            self.proc.send_signal(sig)
            self.proc.wait(10)

    def ctl(self, *args, check=True):
        r = subprocess.run([str(TDRCTL), "--socket", str(self.sock), *args], capture_output=True, text=True, timeout=60)
        if check and r.returncode != 0:
            raise subprocess.CalledProcessError(r.returncode, args, r.stdout, r.stderr)
        return r

    def submit(self, *args):
        return self.ctl("submit", *args).stdout.strip()

    def status(self, job):
        return json.loads(self.ctl("status", job, "--json").stdout)

    def wait(self, job, timeout=30):
        self.ctl("wait", job, "--timeout", str(timeout), check=False)
        return self.status(job)


@pytest.fixture
def daemon():
    with tempfile.TemporaryDirectory(prefix="ffe2e-", dir="/tmp") as d:
        dm = Daemon(Path(d) / "st")
        dm.start()
        yield dm
        dm.stop()


def test_shell_job_publishes_output(daemon):
    j = daemon.submit("--", "sh", "-c", "echo hello > $FIREFORGE_OUTPUT_DIR/g.txt")
    st = daemon.wait(j)
    assert st["state"] == "COMPLETED"
    (art,) = st["output_artifacts"]
    digest = art.removeprefix("artifact://sha256/")
    assert (daemon.state / "artifacts/sha256" / digest[:2] / digest).read_text() == "hello\n"


def test_failure_is_retried_then_failed(daemon):
    st = daemon.wait(daemon.submit("--attempts", "3", "--", "sh", "-c", "exit 3"))
    assert st["state"] == "FAILED" and st["attempts_used"] == 3


def test_timeout_expires(daemon):
    st = daemon.wait(daemon.submit("--timeout", "1", "--", "sleep", "30"))
    assert st["state"] == "EXPIRED"


def test_missing_binary_fails(daemon):
    st = daemon.wait(daemon.submit("--", "/nonexistent/binary"))
    assert st["state"] == "FAILED" and "No such file" in st["last_error"]


def test_sdk_task_and_input_artifact(daemon):
    st = daemon.wait(daemon.submit("--operation", "demo.dict", "--params", '{"x": 1}', *SDK_ENV, "--", PY, WORKER))
    assert st["state"] == "COMPLETED"
    art = st["output_artifacts"][0]
    st2 = daemon.wait(daemon.submit("--input", art, "--", "sh", "-c",
                                    "test -s $FIREFORGE_INPUT_DIR/* && cp $FIREFORGE_INPUT_DIR/* $FIREFORGE_OUTPUT_DIR/copy"))
    assert st2["state"] == "COMPLETED"
    assert st2["output_artifacts"] == [art]  # identical content -> identical id


def test_sdk_cancel(daemon):
    j = daemon.submit("--operation", "demo.cancel", *SDK_ENV, "--", PY, WORKER)
    for _ in range(200):
        if daemon.status(j)["state"] == "RUNNING":
            break
        time.sleep(0.02)
    time.sleep(0.3)
    daemon.ctl("cancel", j)
    assert daemon.wait(j)["state"] == "CANCELLED"


def test_sdk_retryable_exception(daemon):
    st = daemon.wait(daemon.submit("--attempts", "2", "--operation", "demo.raise", *SDK_ENV, "--", PY, WORKER))
    assert st["state"] == "FAILED" and st["attempts_used"] == 2


def test_output_escape_rejected(daemon):
    st = daemon.wait(daemon.submit("--operation", "demo.escape", *SDK_ENV, "--", PY, WORKER))
    assert st["state"] in ("FAILED", "VALIDATION_FAILED")


def test_bad_submit_rejected(daemon):
    r = daemon.ctl("submit", "--input", "artifact://sha256/" + "0" * 64, "--", "true", check=False)
    assert r.returncode != 0
    r = daemon.ctl("submit", "--cpus", "100000", "--", "true", check=False)
    assert r.returncode != 0


def test_capacity_limits_concurrency(daemon):
    node = json.loads(daemon.ctl("node", "--json").stdout)
    cores = node["capacity"]["cpu_cores"]
    jobs = [daemon.submit("--cpus", str(cores), "--", "sleep", "0.5") for _ in range(2)]
    time.sleep(0.2)
    states = [daemon.status(j)["state"] for j in jobs]
    assert states == ["RUNNING", "QUEUED"]
    assert all(daemon.wait(j)["state"] == "COMPLETED" for j in jobs)


def test_crash_recovery_retries_job(daemon):
    j = daemon.submit("--attempts", "2", "--", "sh", "-c", "sleep 2; echo ok > $FIREFORGE_OUTPUT_DIR/o")
    for _ in range(200):
        st = daemon.status(j)
        if st["state"] == "RUNNING":
            break
        time.sleep(0.02)
    work = next((daemon.state / "work" / j).iterdir())
    daemon.stop(signal.SIGKILL)
    daemon.start()
    st = daemon.wait(j)
    assert st["state"] == "COMPLETED" and st["attempts_used"] == 2
    assert "abandoned" in st["last_error"]
    assert work.exists()  # abandoned attempt keeps its dir for inspection
