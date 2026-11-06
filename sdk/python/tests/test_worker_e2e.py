import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from fireforge.testing import FakeDaemon

SCRIPT = Path(__file__).with_name("worker_script.py")
CIF = "data_NaCl\n_cell_length_a 5.64\n"


def run(daemon, **kw):
    proc = daemon.run_worker(SCRIPT, **kw)
    return proc


def test_success_with_progress_and_outputs():
    with FakeDaemon("demo.success", {"temperatures": [300, 500]}, material_id="mat-1") as d:
        d.add_input(CIF)
        proc = run(d)
        assert proc.returncode == 0, proc.stderr
        assert d.hello.attempt_id == "att-test" and d.hello.token == "s3cret"
        assert d.hello.pid > 0 and d.hello.sdk_version == "0.1.0"
        assert [p.fraction for p in d.progress] == [0.25, 1.0, 0.0]
        assert d.progress[0].message == "starting" and d.progress[0].attempt_id == "att-test"
        r = d.result
        assert r.success and not r.error and r.attempt_id == "att-test"
        outs = [(o.path, o.name, o.media_type) for o in r.outputs]
        assert outs == [("result.json", "result", "application/json"),
                        ("summary.txt", "summary", "text/plain"),
                        ("sub/blob.bin", "blob", "application/octet-stream")]
        files = d.output_files()
        assert json.loads(files["result.json"]) == {
            "params": {"temperatures": [300, 500]}, "job": "job-test", "attempt": 1,
            "inputs": 1, "material": "mat-1"}
        assert files["summary.txt"] == b"formula=cif\n"
        assert files["sub/blob.bin"] == b"\x00\x01\x02"


def test_unknown_operation():
    with FakeDaemon("no.such.op") as d:
        proc = run(d)
        assert proc.returncode == 1
        assert not d.result.success and not d.result.retryable
        assert "unknown operation" in d.result.error


def test_task_exception_is_retryable():
    with FakeDaemon("demo.raise") as d:
        proc = run(d)
        assert proc.returncode == 1
        assert not d.result.success and d.result.retryable
        assert d.result.error == "RuntimeError: kaboom"
        assert "Traceback" in proc.stderr and "kaboom" in proc.stderr


def test_explicit_failure_result():
    with FakeDaemon("demo.fail") as d:
        proc = run(d)
        assert proc.returncode == 1
        assert not d.result.success and d.result.retryable and d.result.error == "bad input"


def test_cancel_mid_task():
    with FakeDaemon("demo.cancel", cancel_after_progress=1, cancel_reason="user") as d:
        proc = run(d)
        assert proc.returncode == 1, proc.stderr
        assert d.cancel_sent
        assert not d.result.success and not d.result.retryable
        assert "Cancelled" in d.result.error and "user" in d.result.error


def test_missing_env_exit_2():
    env = {k: v for k, v in os.environ.items() if not k.startswith("FIREFORGE_")}
    env["PYTHONPATH"] = str(SCRIPT.parent.parent)
    proc = subprocess.run([sys.executable, str(SCRIPT)], env=env, capture_output=True, text=True, timeout=20)
    assert proc.returncode == 2
    assert "FIREFORGE_IPC_SOCKET" in proc.stderr


def test_bad_token_error_exit_1():
    with FakeDaemon("demo.success", worker_token="wrong") as d:
        proc = run(d)
        assert proc.returncode == 1
        assert "UNAUTHORIZED" in proc.stderr and "bad attempt token" in proc.stderr
        assert d.result is None


def test_declared_output_escape_rejected():
    with FakeDaemon("demo.escape") as d:
        proc = run(d)
        assert proc.returncode == 1
        assert not d.result.success and not d.result.retryable
        assert "outside the output directory" in d.result.error
        assert len(d.result.outputs) == 0


def test_save_output_escape_rejected():
    with FakeDaemon("demo.save_escape") as d:
        proc = run(d)
        assert proc.returncode == 1
        assert not d.result.success and "outside the output directory" in d.result.error
        assert not (d.root / "evil.txt").exists()


def test_dict_return_saved():
    with FakeDaemon("demo.dict") as d:
        proc = run(d)
        assert proc.returncode == 0, proc.stderr
        assert [(o.path, o.name, o.media_type) for o in d.result.outputs] == [
            ("result.json", "result", "application/json")]
        assert json.loads(d.output_files()["result.json"]) == {"value": 42}


def test_none_return_declares_output_dir_files():
    with FakeDaemon("demo.none") as d:
        proc = run(d)
        assert proc.returncode == 0, proc.stderr
        assert [(o.path, o.name) for o in d.result.outputs] == [("plain.dat", "plain.dat")]


def test_bad_parameters_json():
    with FakeDaemon("demo.dict", "{not json") as d:
        proc = run(d)
        assert proc.returncode == 1
        assert not d.result.success and not d.result.retryable
        assert "parameters_json" in d.result.error


def test_no_daemon_socket_exit_1(tmp_path):
    d = FakeDaemon("demo.dict")  # never started: nothing listening
    try:
        proc = run(d)
        assert proc.returncode == 1 and "cannot connect" in proc.stderr
    finally:
        d.stop()
