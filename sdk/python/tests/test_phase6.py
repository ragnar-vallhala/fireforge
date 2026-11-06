"""Phase 6 SDK: named inputs, checkpoints, spawn, workflow builder."""
import json
import sys
import threading
import time
from pathlib import Path

import pytest
from google.protobuf import json_format

from fireforge import (CheckpointError, Context, InputRef, StepInput, Workflow, WorkerError,
                       WorkflowError)
from fireforge import worker as worker_mod
from fireforge.control.v1 import control_pb2
from fireforge.tdr.v1 import envelope_pb2, job_pb2, local_pb2
from fireforge.testing import FakeDaemon

SCRIPT = Path(__file__).with_name("worker_script.py")


# ---------------------------------------------------------------------------
# Context unit tests (stub session, fake clock)
# ---------------------------------------------------------------------------


class StubSession:
    def __init__(self, reply=None):
        self.sent = []          # (WorkerCheckpoint, file content)
        self.reply = reply      # None = ACK, else (code, message) -> ERROR
        self.base = None

    def request(self, mtype, msg, timeout):
        assert mtype == envelope_pb2.WORKER_CHECKPOINT
        self.sent.append((msg, (self.base / msg.path).read_bytes()))
        if self.reply is None:
            return envelope_pb2.WORKER_ACK, b""
        code, text = self.reply
        return envelope_pb2.ERROR, local_pb2.Error(code=code, message=text).SerializeToString()


class Clock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t


def make_ctx(tmp_path, *, enabled=True, session=None, clock=None, inputs=(), **task_kw):
    out, ck = tmp_path / "out", tmp_path / "ck"
    out.mkdir(exist_ok=True)
    task = local_pb2.WorkerTask(job_id="j", attempt_id="a", operation="op", output_dir=str(out),
                                checkpoint_dir=str(ck), checkpoint_enabled=enabled,
                                inputs=[local_pb2.InputFile(artifact_id=a, path=str(p), name=n)
                                        for a, p, n in inputs], **task_kw)
    ctx = Context(task, {"FIREFORGE_WORKDIR": str(tmp_path)}, session, clock=clock,
                  background_flush=False)
    if session is not None:
        session.base = ck.resolve()
    return ctx


def test_workflow_fields_and_named_inputs(tmp_path):
    a, b = tmp_path / "a", tmp_path / "b"
    ctx = make_ctx(tmp_path, inputs=[("artifact://1", a, "structure"), ("artifact://2", b, "")],
                   workflow_id="wf-1", step="fan/child0")
    assert ctx.workflow_id == "wf-1" and ctx.step == "fan/child0"
    assert ctx.inputs == [InputRef("artifact://1", a, "structure"), InputRef("artifact://2", b, "")]
    assert ctx.input_path(1) == b
    assert ctx.named_inputs == {"structure": a} and ctx.input("structure") == a
    with pytest.raises(KeyError, match=r"no input named 'x'.*structure.*2 input"):
        ctx.input("x")
    (tmp_path / "p").mkdir()
    plain = make_ctx(tmp_path / "p")
    assert plain.workflow_id == "" and plain.step == "" and plain.load_checkpoint() is None
    assert plain.load_checkpoint_json(default={"i": 0}) == {"i": 0}


def test_load_checkpoint(tmp_path):
    rp = tmp_path / "ck" / "resume" / "state.json"
    rp.parent.mkdir(parents=True)
    rp.write_text('{"step": 7}')
    ctx = make_ctx(tmp_path, resume_checkpoint=str(rp))
    assert ctx.load_checkpoint() == rp and ctx.load_checkpoint_json() == {"step": 7}


def test_checkpoint_disabled_raises_before_writing(tmp_path):
    s = StubSession()
    ctx = make_ctx(tmp_path, enabled=False, session=s)
    with pytest.raises(CheckpointError, match="not enabled") as e:
        ctx.checkpoint({"x": 1})
    assert not e.value.retryable and s.sent == []
    assert not (tmp_path / "ck" / "checkpoint.json").exists()


def test_checkpoint_kinds_and_paths(tmp_path):
    s = StubSession()
    clock = Clock()
    ctx = make_ctx(tmp_path, session=s, clock=clock)
    ck = (tmp_path / "ck").resolve()
    assert ctx.checkpoint({"i": 1}) == ck / "checkpoint.json"
    assert ctx.checkpoint(b"\x00", name="raw") == ck / "raw.bin"
    assert ctx.checkpoint("txt", name="log") == ck / "log.txt"
    outside = tmp_path / "data.tar.gz"
    outside.write_bytes(b"tgz")
    assert ctx.checkpoint(outside, name="arch") == ck / "arch.tar.gz"     # copied in
    inside = ck / "sub" / "w.npy"
    inside.parent.mkdir()
    inside.write_bytes(b"npy")
    assert ctx.checkpoint(inside, name="weights") == inside                # used in place
    got = [(m.name, m.path, m.media_type, m.attempt_id, data) for m, data in s.sent]
    assert got == [
        ("checkpoint", "checkpoint.json", "application/json", "a", b'{\n  "i": 1\n}\n'),
        ("raw", "raw.bin", "application/octet-stream", "a", b"\x00"),
        ("log", "log.txt", "text/plain", "a", b"txt"),
        ("arch", "arch.tar.gz", "application/x-tar", "a", b"tgz"),
        ("weights", "sub/w.npy", "application/octet-stream", "a", b"npy"),
    ]
    with pytest.raises(ValueError, match="invalid checkpoint name"):
        ctx.checkpoint(b"x", name="../evil")
    with pytest.raises(CheckpointError, match="is not a file"):
        ctx.checkpoint(tmp_path / "nope", name="n")


def test_checkpoint_daemon_error(tmp_path):
    s = StubSession(reply=(local_pb2.BAD_REQUEST, "disk full"))
    ctx = make_ctx(tmp_path, session=s, clock=Clock())
    with pytest.raises(CheckpointError, match="BAD_REQUEST: disk full") as e:
        ctx.checkpoint({"x": 1})
    assert not e.value.retryable


def test_checkpoint_rate_limit(tmp_path):
    s = StubSession()
    clock = Clock()
    ctx = make_ctx(tmp_path, session=s, clock=clock)
    ctx.checkpoint({"i": 0}, name="state")                 # sent
    clock.t += 3
    ctx.checkpoint({"i": 1}, name="state")                 # pending
    ctx.checkpoint(b"o", name="other")                     # other name: own window, sent
    clock.t += 3
    ctx.checkpoint({"i": 2}, name="state")                 # still pending (6 s), file updated
    assert [(m.name, d) for m, d in s.sent] == [("state", b'{\n  "i": 0\n}\n'), ("other", b"o")]
    clock.t += 4.5                                         # 10.5 s since the first send
    ctx.checkpoint(b"o2", name="other")                    # other: inside its window -> pending;
    # ...but the call flushes "state", whose window has passed
    assert [(m.name, d) for m, d in s.sent][2] == ("state", b'{\n  "i": 2\n}\n')
    assert len(s.sent) == 3
    ctx.flush_checkpoints()                                # explicit flush ignores the window
    assert [(m.name, d) for m, d in s.sent][3] == ("other", b"o2")
    ctx.flush_checkpoints()                                # nothing pending
    assert len(s.sent) == 4


def test_checkpoint_background_timer(tmp_path, monkeypatch):
    monkeypatch.setattr(worker_mod, "CHECKPOINT_INTERVAL_S", 0.2)
    s = StubSession()
    ctx = make_ctx(tmp_path, session=s)
    ctx._ck.background = True
    ctx.checkpoint({"i": 0})
    ctx.checkpoint({"i": 1})
    assert len(s.sent) == 1
    deadline = time.monotonic() + 5
    while len(s.sent) < 2 and time.monotonic() < deadline:
        time.sleep(0.02)
    assert [d for _, d in s.sent] == [b'{\n  "i": 0\n}\n', b'{\n  "i": 1\n}\n']
    ctx._ck.close()


def test_checkpoint_background_error_raised_later(tmp_path, monkeypatch):
    monkeypatch.setattr(worker_mod, "CHECKPOINT_INTERVAL_S", 0.1)
    s = StubSession()
    ctx = make_ctx(tmp_path, session=s)
    ctx._ck.background = True
    ctx.checkpoint(b"0")
    s.reply = (local_pb2.BAD_REQUEST, "boom")
    ctx.checkpoint(b"1")
    deadline = time.monotonic() + 5
    while len(s.sent) < 2 and time.monotonic() < deadline:
        time.sleep(0.02)
    time.sleep(0.05)
    with pytest.raises(CheckpointError, match="boom"):
        ctx.flush_checkpoints()
    ctx._ck.close()


def test_session_request_correlates_concurrent_requests():
    """Replies are routed by correlation id, whatever order they come in."""
    import socket
    from fireforge.protocol import FrameReader, FrameWriter

    a, b = socket.socketpair()
    session = worker_mod._Session(a)
    ctx = Context(local_pb2.WorkerTask(attempt_id="x"), {}, session, background_flush=False)
    session.start_reader(ctx)
    reader, writer = FrameReader(b), FrameWriter(b)
    results = {}

    def ask(i):
        mtype, payload = session.request(envelope_pb2.WORKER_CHECKPOINT,
                                         local_pb2.WorkerCheckpoint(name=f"n{i}"), 5)
        results[i] = (mtype, local_pb2.Error.FromString(payload).message if payload else "")

    ts = [threading.Thread(target=ask, args=(i,)) for i in range(3)]
    for t in ts:
        t.start()
    frames = [reader.read_frame() for _ in range(3)]
    for h, p in reversed(frames):  # answer in reverse order
        name = local_pb2.WorkerCheckpoint.FromString(p).name
        writer.send(envelope_pb2.ERROR, local_pb2.Error(message=name), h.message_id)
    for t in ts:
        t.join(5)
    assert results == {i: (envelope_pb2.ERROR, f"n{i}") for i in range(3)}
    b.close()
    session._thread.join(5)
    with pytest.raises(worker_mod.DaemonError, match="closed"):
        session.request(envelope_pb2.WORKER_CHECKPOINT, local_pb2.WorkerCheckpoint(), 2)
    session.close()


# ---------------------------------------------------------------------------
# spawn
# ---------------------------------------------------------------------------


def test_spawn_serialization(tmp_path, monkeypatch):
    monkeypatch.setenv("PYTHONPATH", "/sdk")
    ctx = make_ctx(tmp_path, workflow_id="wf-1", step="fan")
    assert ctx.spawn("c0", operation="op.child", params={"i": 0}, inputs=[".:result", "c1"],
                     depends_on=["prep"], resources={"cpu_cores": 2, "memory_bytes": 1 << 30},
                     run_if="any_failed", allow_failure=True,
                     input_artifacts={"pot": "artifact://sha256/aa"}, capabilities=["vasp"],
                     max_attempts=2, timeout=1.5, checkpoint=True, env={"OMP": "4"}) == "c0"
    ctx.spawn("c1", argv=["/bin/w", "x"], operation="op.child",
              inputs=[StepInput("prep", "out", spawned=True, as_name="o")])
    spec0, spec1 = ctx.spawned
    j = spec0.job
    assert j.operation == "op.child" and json.loads(j.parameters_json) == {"i": 0}
    # default argv: the current interpreter + this process's main program
    assert list(j.command.argv)[0] == sys.executable and len(j.command.argv) >= 2
    assert dict(j.command.env) == {"PYTHONPATH": "/sdk", "OMP": "4"}
    assert (j.resources.cpu_cores, j.resources.memory_bytes) == (2, 1 << 30)
    assert (j.policy.max_attempts, j.policy.timeout_seconds, j.policy.checkpoint) == (2, 2, True)
    assert list(j.input_artifacts) == ["artifact://sha256/aa"] and list(j.input_names) == ["pot"]
    assert list(j.required_capabilities) == ["vasp"]
    assert spec0.run_if == job_pb2.ANY_FAILED and spec0.allow_failure
    assert list(spec0.depends_on) == ["prep"]
    assert [(i.step, i.output, i.spawned, i.input_name) for i in spec0.inputs] == [
        (".", "result", False, ""), ("c1", "*", False, "")]
    assert list(spec1.job.command.argv) == ["/bin/w", "x"] and not spec1.job.command.env
    assert [(i.step, i.output, i.spawned, i.input_name) for i in spec1.inputs] == [
        ("prep", "out", True, "o")]
    assert spec1.run_if == job_pb2.ALL_SUCCEEDED


def test_spawn_errors(tmp_path):
    ctx = make_ctx(tmp_path)
    with pytest.raises(ValueError, match="operation"):
        ctx.spawn("a")
    with pytest.raises(ValueError, match="invalid step name"):
        ctx.spawn("a/b", operation="x")
    with pytest.raises(ValueError, match="run_if"):
        ctx.spawn("a", operation="x", run_if="sometimes")
    with pytest.raises(ValueError, match="resources"):
        ctx.spawn("a", operation="x", resources={"cpus": 1})
    with pytest.raises(ValueError, match="itself"):
        ctx.spawn("a", operation="x", depends_on=["a"])
    ctx.spawn("a", operation="x")
    with pytest.raises(ValueError, match="duplicate"):
        ctx.spawn("a", operation="x")


def test_default_worker_argv(monkeypatch, tmp_path):
    import __main__
    script = tmp_path / "w.py"
    script.write_text("")
    monkeypatch.setattr(__main__, "__spec__", None, raising=False)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "argv", ["w.py", "--flag"])
    assert worker_mod.default_worker_argv() == [sys.executable, str(script), "--flag"]
    monkeypatch.setattr(sys, "argv", ["-c"])
    with pytest.raises(WorkerError, match="argv="):
        worker_mod.default_worker_argv()

    class Spec:
        name = "pkg.__main__"
    monkeypatch.setattr(__main__, "__spec__", Spec)
    monkeypatch.setattr(sys, "argv", ["/x/pkg/__main__.py", "a"])
    assert worker_mod.default_worker_argv() == [sys.executable, "-m", "pkg", "a"]


def test_duplicate_output_names_rejected(tmp_path):
    ctx = make_ctx(tmp_path)
    ctx.save_output("r", "a", filename="a.txt")
    ctx.save_output("r", "b", filename="b.txt")
    with pytest.raises(WorkerError, match="duplicate output name 'r'"):
        ctx._collect_outputs(None)


# ---------------------------------------------------------------------------
# against the fake daemon
# ---------------------------------------------------------------------------


def test_e2e_workflow_fields_inputs_resume():
    with FakeDaemon("demo.wf_info", workflow_id="wf-9", step="relax",
                    resume_checkpoint=b'{"iter": 5}') as d:
        d.add_input("data_x\n", name="structure")
        d.add_input("other")
        proc = d.run_worker(SCRIPT)
        assert proc.returncode == 0, proc.stderr
        out = json.loads(d.output_files()["result.json"])
        assert out["workflow_id"] == "wf-9" and out["step"] == "relax"
        assert out["named"] == ["structure"] and out["structure"] == "data_x\n"
        assert out["names"] == ["structure", ""] and out["resume"] == {"iter": 5}
        assert "no input named 'missing'" in out["missing"]


def test_e2e_checkpoints_rate_limited_and_flushed():
    with FakeDaemon("demo.checkpoint", checkpoint_enabled=True) as d:
        proc = d.run_worker(SCRIPT)
        assert proc.returncode == 0, proc.stderr
        got = [(c.name, c.path, c.media_type, c.attempt_id) for c in d.checkpoints]
        assert got == [("state", "state.json", "application/json", "att-test"),
                       ("ext", "ext.dat", "application/octet-stream", "att-test"),
                       ("state", "state.json", "application/json", "att-test")]
        assert [json.loads(b) if c.name == "state" else b
                for c, b in zip(d.checkpoints, d.checkpoint_files)] == [{"i": 0}, b"ext", {"i": 2}]
        # the flushed checkpoint went out before the result
        types = [t for t, _ in d.received]
        assert types.index(envelope_pb2.WORKER_RESULT) == len(types) - 1


def test_e2e_checkpoint_disabled_gets_clear_error():
    with FakeDaemon("demo.checkpoint_error", checkpoint_enabled=False) as d:
        proc = d.run_worker(SCRIPT)
        assert proc.returncode == 0, proc.stderr
        out = json.loads(d.output_files()["result.json"])
        assert "not enabled" in out["error"] and not out["retryable"]
        assert d.checkpoints == []   # refused client-side, nothing sent


def test_e2e_checkpoint_daemon_error():
    with FakeDaemon("demo.checkpoint_error", checkpoint_enabled=True,
                    checkpoint_error="quota exceeded") as d:
        proc = d.run_worker(SCRIPT)
        assert proc.returncode == 0, proc.stderr
        out = json.loads(d.output_files()["result.json"])
        assert out["error"] == ("daemon rejected checkpoint 'checkpoint': BAD_REQUEST: quota exceeded")
        assert len(d.checkpoints) == 1


def test_e2e_checkpoint_uncaught_fails_attempt():
    with FakeDaemon("demo.checkpoint", checkpoint_enabled=False) as d:
        proc = d.run_worker(SCRIPT)
        assert proc.returncode == 1
        assert not d.result.success and not d.result.retryable
        assert "CheckpointError" in d.result.error


def test_e2e_spawn():
    with FakeDaemon("demo.spawn", workflow_id="wf-1", step="fan") as d:
        pypath = d.env()["PYTHONPATH"] + ":/custom/path"
        proc = d.run_worker(SCRIPT, extra_env={"PYTHONPATH": pypath})
        assert proc.returncode == 0, proc.stderr
        assert [s.name for s in d.spawned] == ["child0", "child1", "join"]
        c0 = d.spawned[0]
        assert list(c0.job.command.argv) == [sys.executable, str(SCRIPT)]
        assert dict(c0.job.command.env) == {"PYTHONPATH": pypath}
        assert json.loads(c0.job.parameters_json) == {"i": 0} and c0.job.resources.cpu_cores == 2
        assert [(i.step, i.output) for i in c0.inputs] == [(".", "result")]
        join = d.spawned[2]
        assert list(join.job.command.argv) == ["/bin/true"] and dict(join.job.command.env) == {"X": "1"}
        assert join.run_if == job_pb2.ALWAYS and join.allow_failure
        assert join.job.policy.checkpoint and join.job.policy.max_attempts == 3
        assert join.job.policy.timeout_seconds == 60
        assert [(i.step, i.output, i.spawned, i.input_name) for i in join.inputs] == [
            ("child0", "*", True, "c0"), ("child1", "*", False, "")]


def test_e2e_spawn_dropped_on_failure():
    with FakeDaemon("demo.spawn_fail", workflow_id="wf-1", step="fan") as d:
        proc = d.run_worker(SCRIPT)
        assert proc.returncode == 1
        assert not d.result.success and d.spawned == []


# ---------------------------------------------------------------------------
# Workflow builder
# ---------------------------------------------------------------------------

ARGV = ["python3", "/opt/w.py"]


def diamond():
    wf = Workflow("diamond", campaign_id="camp-1", argv=ARGV)
    relax = wf.step("relax", operation="dft.relax", params={"encut": 520},
                    input_artifacts={"structure": "artifact://sha256/ab"}, checkpoint=True,
                    max_attempts=3, resources={"cpu_cores": 4, "memory_bytes": 8 << 30})
    b = wf.step("bands", operation="dft.bands", inputs=[f"{relax}:structure"])
    p = wf.step("phonons", operation="dft.phonons", inputs=[relax], env={"OMP_NUM_THREADS": "2"})
    e = wf.step("elastic", operation="dft.elastic", inputs=[StepInput(relax, "structure", as_name="s")],
                argv=["/bin/elastic"])
    wf.step("evaluate", operation="eval", inputs=[b, StepInput(p, spawned=True), e])
    wf.step("on_fail", operation="notify", depends_on=[relax, b, p, e], run_if="any_failed",
            allow_failure=True)
    return wf


def test_workflow_json_round_trip(tmp_path):
    wf = diamond()
    path = wf.write(tmp_path / "wf.json")
    text = path.read_text()
    raw = json.loads(text)
    assert raw["name"] == "diamond" and raw["campaign_id"] == "camp-1"
    assert raw["steps"][0]["job"]["input_artifacts"] == ["artifact://sha256/ab"]   # proto field names
    assert raw["steps"][5]["run_if"] == "ANY_FAILED"
    req = json_format.Parse(text, control_pb2.CreateWorkflowRequest())
    assert req == wf.to_proto()
    steps = {s.name: s for s in req.steps}
    assert list(steps) == ["relax", "bands", "phonons", "elastic", "evaluate", "on_fail"]
    r = steps["relax"].job
    assert list(r.command.argv) == ARGV and r.operation == "dft.relax"
    assert json.loads(r.parameters_json) == {"encut": 520} and list(r.input_names) == ["structure"]
    assert r.policy.checkpoint and r.policy.max_attempts == 3 and r.resources.memory_bytes == 8 << 30
    assert [(i.step, i.output) for i in steps["bands"].inputs] == [("relax", "structure")]
    assert dict(steps["phonons"].job.command.env) == {"OMP_NUM_THREADS": "2"}
    assert list(steps["elastic"].job.command.argv) == ["/bin/elastic"]
    assert steps["elastic"].inputs[0].input_name == "s"
    assert [(i.step, i.spawned) for i in steps["evaluate"].inputs] == [
        ("bands", False), ("phonons", True), ("elastic", False)]
    assert steps["on_fail"].run_if == job_pb2.ANY_FAILED and steps["on_fail"].allow_failure
    assert list(steps["on_fail"].depends_on) == ["relax", "bands", "phonons", "elastic"]
    assert wf.to_dict() == raw


def test_workflow_forward_reference_ok():
    wf = Workflow("w", argv=ARGV)
    wf.step("b", operation="x", depends_on=["a"])
    wf.step("a", operation="x")
    wf.validate()


@pytest.mark.parametrize("build,match", [
    (lambda wf: wf.step("bad name", operation="x"), "invalid step name"),
    (lambda wf: wf.step("x" * 65, operation="x"), "invalid step name"),
    (lambda wf: [wf.step("a", operation="x"), wf.step("a", operation="x")], "duplicate step name 'a'"),
    (lambda wf: wf.step("a", operation="x", depends_on=["zz"]) and wf.to_json(),
     "depends on unknown step 'zz'"),
    (lambda wf: wf.step("a", operation="x", inputs=["zz:out"]) and wf.to_json(),
     "input from unknown step 'zz'"),
    (lambda wf: wf.step("a", operation="x", inputs=["a"]), "inputs from itself"),
    (lambda wf: wf.step("a", operation="x", depends_on=["a"]), "depends on itself"),
    (lambda wf: wf.step("a", operation="x", inputs=["."]), "only valid in context.spawn"),
    (lambda wf: wf.step("a", operation="x", inputs=["b:"]), "empty output"),
    (lambda wf: wf.step("a", operation="x", inputs=[42]), "unsupported step input"),
    (lambda wf: wf.step("a", operation="x", run_if="never"), "invalid run_if"),
    (lambda wf: wf.step("a", operation="x", argv=[]), "argv must be a non-empty"),
    (lambda wf: wf.step("a", operation="x", max_attempts=0), "max_attempts"),
    (lambda wf: wf.step("a", operation="x", params={"s": {1, 2}}), "not JSON serializable"),
    (lambda wf: wf.to_json(), "has no steps"),
])
def test_workflow_validation_errors(build, match):
    wf = Workflow("w", argv=ARGV)
    with pytest.raises(WorkflowError, match=match):
        build(wf)


def test_workflow_cycle_reported():
    wf = Workflow("w", argv=ARGV)
    wf.step("a", operation="x", depends_on=["c"])
    wf.step("b", operation="x", inputs=["a"])
    wf.step("c", operation="x", depends_on=["b"])
    wf.step("d", operation="x")
    with pytest.raises(WorkflowError, match="dependency cycle: a -> c -> b -> a"):
        wf.to_json()


def test_workflow_requires_argv_and_name():
    with pytest.raises(WorkflowError, match="no argv"):
        Workflow("w").step("a", operation="x")
    with pytest.raises(WorkflowError, match="workflow name"):
        Workflow("")
