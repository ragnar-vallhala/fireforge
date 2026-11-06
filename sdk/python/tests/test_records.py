"""Phase 8 SDK: calculation records (context.record)."""
import json
import math
from pathlib import Path

import pytest

from fireforge import MAX_RECORDS, MAX_RECORDS_JSON_BYTES, Context, OutputRef, Worker, WorkerError
from fireforge.tdr.v1 import local_pb2, record_pb2
from fireforge.testing import FakeDaemon

SCRIPT = Path(__file__).with_name("worker_script.py")

NACL = {"lattice": [5.6, 0, 0, 0, 5.6, 0, 0, 0, 5.6],
        "sites": [{"species": "Na", "frac": [0, 0, 0]}, {"species": "Cl", "frac": [0.5, 0.5, 0.5]}],
        "reduced_formula": "NaCl", "spacegroupNumber": 225, "fingerprint": "fp1:abc"}


def make_ctx(tmp_path, env=None, **kw):
    out = tmp_path / "out"
    out.mkdir(parents=True, exist_ok=True)
    task = local_pb2.WorkerTask(job_id="j", attempt_id="a", operation="op", output_dir=str(out))
    return Context(task, {"FIREFORGE_WORKDIR": str(tmp_path), **(env or {})}, None,
                   background_flush=False, **kw)


class ProtoStructure:
    def to_proto(self):
        return record_pb2.Structure(reduced_formula="ZrNiSn", lattice=[1.0] * 9, fingerprint="fp1:z")


class JsonStructure:
    def to_json(self):
        return {"reduced_formula": "TiCoSb", "lattice": [2.0] * 9, "sites": [{"species": "Ti", "frac": [0, 0, 0]}]}


def test_record_serialization(tmp_path):
    ctx = make_ctx(tmp_path)
    r = ctx.record("qe.relax", structure=NACL, properties={"energy_ev": -3.2, "kpts": [4, 4, 4]},
                   conditions={"temperature_k": 300}, convergence={"steps": 7}, uncertainty={"energy_ev": 0.01},
                   method={"solver": "quantum-espresso", "solver_version": "7.2", "engine": "qe",
                           "functional": "PBE", "pseudopotentials": ["Na.upf md5:aa"], "preset": "screening",
                           "settings": {"ecut_ry": 40, "kmesh": [4, 4, 4]}},
                   outputs=["relaxed", OutputRef("log", "log.txt")], input_artifacts=["artifact://sha256/ab"],
                   note="first")
    assert r.kind == "qe.relax" and r.evidence == record_pb2.PREDICTED and not r.structure_id
    assert r.structure.reduced_formula == "NaCl" and r.structure.spacegroup_number == 225
    assert [s.species for s in r.structure.sites] == ["Na", "Cl"] and list(r.structure.sites[1].frac) == [0.5] * 3
    assert json.loads(r.properties_json) == {"energy_ev": -3.2, "kpts": [4, 4, 4]}
    assert json.loads(r.conditions_json) == {"temperature_k": 300}
    assert json.loads(r.convergence_json) == {"steps": 7}
    assert json.loads(r.uncertainty_json) == {"energy_ev": 0.01}
    assert r.method.solver == "quantum-espresso" and r.method.solver_version == "7.2"
    assert r.method.engine == "qe" and r.method.functional == "PBE" and r.method.preset == "screening"
    assert list(r.method.pseudopotentials) == ["Na.upf md5:aa"]
    assert json.loads(r.method.settings_json) == {"ecut_ry": 40, "kmesh": [4, 4, 4]}
    assert list(r.output_names) == ["relaxed", "log"] and list(r.input_artifacts) == ["artifact://sha256/ab"]
    assert r.note == "first"

    # Other structure / method forms; unset JSON fields stay empty.
    a = ctx.record("k.a", structure=ProtoStructure(), method=record_pb2.Method(solver="toy"))
    assert a.structure.reduced_formula == "ZrNiSn" and a.method.solver == "toy"
    assert a.properties_json == "" and a.conditions_json == "" and a.HasField("method")
    b = ctx.record("k.b", structure=JsonStructure(), method={"solver": "toy", "settings_json": '{"x": 1}'})
    assert b.structure.reduced_formula == "TiCoSb" and json.loads(b.method.settings_json) == {"x": 1}
    c = ctx.record("k.c", structure=record_pb2.Structure(reduced_formula="Si"), properties='{"gap_ev": 1.1}')
    assert c.structure.reduced_formula == "Si" and json.loads(c.properties_json) == {"gap_ev": 1.1}
    d = ctx.record("k.d", structure_id="st-1")
    assert d.structure_id == "st-1" and not d.HasField("structure") and not d.HasField("method")
    assert [x.kind for x in ctx.records] == ["qe.relax", "k.a", "k.b", "k.c", "k.d"]
    ctx.records[0].kind = "changed"   # copies
    assert ctx.records[0].kind == "qe.relax"


def test_record_science_structure(tmp_path):
    model = pytest.importorskip("fireforge_science.model")
    s = model.Structure(lattice=[[4.0, 0, 0], [0, 4.0, 0], [0, 0, 4.0]], species=["Si"], frac=[[0, 0, 0]])
    r = make_ctx(tmp_path).record("qe.scf", structure=s)
    assert r.structure.reduced_formula == "Si" and r.structure.fingerprint.startswith("fp1:")


@pytest.mark.parametrize("kwargs,match", [
    ({"kind": ""}, "kind"),
    ({"kind": "x" * 65}, "kind"),
    ({"kind": 3}, "kind"),
    ({"kind": "k"}, "exactly one"),
    ({"kind": "k", "structure": NACL, "structure_id": "st-1"}, "exactly one"),
    ({"kind": "k", "structure": 42}, "structure"),
    ({"kind": "k", "structure": {"no_such_field": 1}}, "structure"),
    ({"kind": "k", "structure": {"lattice": [math.nan] * 9}}, "structure"),
    ({"kind": "k", "structure_id": "s", "properties": {"x": math.nan}}, "properties"),
    ({"kind": "k", "structure_id": "s", "conditions": {"t": math.inf}}, "conditions"),
    ({"kind": "k", "structure_id": "s", "uncertainty": {"t": -math.inf}}, "uncertainty"),
    ({"kind": "k", "structure_id": "s", "properties": [1, 2]}, "properties"),
    ({"kind": "k", "structure_id": "s", "properties": "[1]"}, "JSON object"),
    ({"kind": "k", "structure_id": "s", "properties": '{"a": NaN}'}, "properties"),
    ({"kind": "k", "structure_id": "s", "convergence": {"o": object()}}, "convergence"),
    ({"kind": "k", "structure_id": "s", "method": {"solvr": "x"}}, "unknown key"),
    ({"kind": "k", "structure_id": "s", "method": {"settings": {}, "settings_json": "{}"}}, "not both"),
    ({"kind": "k", "structure_id": "s", "method": {"settings": {"e": math.nan}}}, "settings"),
    ({"kind": "k", "structure_id": "s", "method": {"pseudopotentials": "Si.upf"}}, "pseudopotentials"),
    ({"kind": "k", "structure_id": "s", "method": "qe"}, "method"),
    ({"kind": "k", "structure_id": "s", "outputs": [""]}, "output names"),
    ({"kind": "k", "structure_id": "s", "input_artifacts": [3]}, "input_artifacts"),
])
def test_record_validation(tmp_path, kwargs, match):
    ctx = make_ctx(tmp_path)
    kind = kwargs.pop("kind")
    with pytest.raises(ValueError, match=match):
        ctx.record(kind, **kwargs)
    assert ctx.records == []


def test_record_limits(tmp_path):
    ctx = make_ctx(tmp_path)
    for i in range(MAX_RECORDS):
        ctx.record("k", structure_id=f"st-{i}")
    with pytest.raises(ValueError, match="at most 64"):
        ctx.record("k", structure_id="one-more")
    assert len(ctx.records) == MAX_RECORDS

    ctx = make_ctx(tmp_path)
    ctx.record("big", structure_id="s", note="x" * (MAX_RECORDS_JSON_BYTES // 2))
    with pytest.raises(ValueError, match="bytes of JSON"):
        ctx.record("big", structure_id="s", note="y" * (MAX_RECORDS_JSON_BYTES // 2))
    assert len(ctx.records) == 1


def test_detached_record_is_noop(tmp_path):
    ctx = make_ctx(tmp_path, env={"OMPI_COMM_WORLD_RANK": "1"}, detached=True)
    assert ctx.record("qe.relax", structure=NACL) is None
    assert ctx.records == []


def test_records_in_result_and_output_check(tmp_path):
    w = Worker()

    @w.task("ok")
    def ok(context):
        context.save_output("relaxed", {"e": 1})
        context.record("qe.relax", structure_id="st-1", outputs=["relaxed"])
        return None

    @w.task("bad")
    def bad(context):
        context.record("qe.relax", structure_id="st-1", outputs=["nope"])
        return None

    @w.task("explicit")
    def explicit(context):   # output declared explicitly but not the one named
        ref = context.save_output("a", "x")
        context.save_output("b", "y")
        context.record("k", structure_id="s", outputs=["b"])
        from fireforge import JobResult
        return JobResult(artifacts=[ref])

    @w.task("fail")
    def fail(context):
        context.record("qe.relax", structure_id="st-1")
        return False

    res = w._execute(make_ctx(tmp_path / "1"), "ok")
    assert res.success and [r.kind for r in res.records] == ["qe.relax"]
    assert list(res.records[0].output_names) == ["relaxed"]
    for op in ("bad", "explicit"):
        res = w._execute(make_ctx(tmp_path / op), op)
        assert not res.success and not res.retryable and "is not an output" in res.error, op
        assert len(res.records) == 0
    res = w._execute(make_ctx(tmp_path / "f"), "fail")
    assert not res.success and len(res.records) == 0


def test_e2e_records_reach_daemon():
    with FakeDaemon("demo.record") as d:
        proc = d.run_worker(SCRIPT)
        assert proc.returncode == 0, proc.stderr
        assert d.result.success
        assert [r.kind for r in d.records] == ["qe.relax", "te.evaluate"]
        r0, r1 = d.records
        assert r0.structure.reduced_formula == "NaCl" and list(r0.output_names) == ["relaxed"]
        assert json.loads(r0.method.settings_json) == {"ecut_ry": 40}
        assert {o.name for o in d.result.outputs} == {"relaxed"}
        assert r1.structure_id == "st-1" and json.loads(r1.properties_json) == {"zt_peak": 1.1}


def test_e2e_records_dropped_on_failure():
    with FakeDaemon("demo.record_fail") as d:
        proc = d.run_worker(SCRIPT)
        assert proc.returncode == 1
        assert not d.result.success and d.records == []


def test_e2e_record_unknown_output_fails():
    with FakeDaemon("demo.record_bad_output") as d:
        proc = d.run_worker(SCRIPT)
        assert proc.returncode == 1
        assert not d.result.success and not d.result.retryable
        assert "'bands' is not an output" in d.result.error and d.records == []


def test_worker_argv_override(monkeypatch):
    from fireforge.worker import default_worker_argv, WorkerError
    monkeypatch.setenv("FIREFORGE_WORKER_ARGV", '["fireforge-sci"]')
    assert default_worker_argv() == ["fireforge-sci"]
    monkeypatch.setenv("FIREFORGE_WORKER_ARGV", '[]')
    import pytest
    with pytest.raises(WorkerError):
        default_worker_argv()
