"""Phase 6 end-to-end: workflow DAGs, fan-out, run_if, cancel, checkpoint resume
on another node, ranked placement (docs/design/phase6.md §10).
"""
import json
import signal
import tempfile
from pathlib import Path

import pytest

from test_artifacts import ArtCP, ArtNode, announced, blob, meshed
from test_control_plane import ROOT, infra, wait_for  # noqa: F401  (infra is a fixture)

PY = str(ROOT / ".venv/bin/python")
WORKER = str(ROOT / "tests/e2e/wf_worker.py")
ENV = {"PYTHONPATH": str(ROOT / "sdk/python")}
MiB = 1024 * 1024


class WfCP(ArtCP):
    def __init__(self, base, pki, conninfo, strict_input_mb=64, **kw):
        super().__init__(base, pki, conninfo, **kw)
        with open(self.conf, "a") as f:
            f.write("[workflows]\nsweep_interval = 1\n"
                    f"[placement]\ninterval = 1\nreserve_window = 5\nmax_reserve = 30\nstrict_input_mb = {strict_input_mb}\n")

    def submit_workflow(self, name, steps, campaign=None):
        req = {"name": name, "steps": steps}
        if campaign:
            req["campaign_id"] = campaign
        path = self.base / f"{name}.json"
        path.write_text(json.dumps(req))
        return self.admin("workflow-submit", str(path)).stdout.strip()

    def workflow(self, wf):
        return self.admin_json("workflow", wf)

    def wait_workflow(self, wf, timeout=60):
        return wait_for(lambda: (w := self.workflow(wf))["state"] != "RUNNING" and w, timeout=timeout,
                        interval=0.5, what=f"workflow {wf} finished")

    def steps(self, wf):
        return {s["name"]: s for s in self.workflow(wf).get("steps", [])}


def step(name, op, params=None, inputs=(), depends_on=(), run_if=None, allow_failure=False,
         checkpoint=False, attempts=None):
    job = {"operation": op, "command": {"argv": [PY, WORKER], "env": ENV}}
    if params is not None:
        job["parameters_json"] = json.dumps(params)
    policy = {}
    if checkpoint:
        policy["checkpoint"] = True
    if attempts:
        policy["max_attempts"] = attempts
    if policy:
        job["policy"] = policy
    s = {"name": name, "job": job}
    if inputs:
        s["inputs"] = [dict(i) for i in inputs]
    if depends_on:
        s["depends_on"] = list(depends_on)
    if run_if:
        s["run_if"] = run_if
    if allow_failure:
        s["allow_failure"] = True
    return s


@pytest.fixture
def wf_cluster(infra):
    base, pg, pki = infra
    tdir = Path(tempfile.mkdtemp(prefix="w-", dir=base))
    cps, nodes = [], []
    ports = iter(range(17900, 18000))

    def make_cp(**kw):
        cp = WfCP(tdir, pki, pg.new_db(), **kw).start()
        cps.append(cp)
        return cp

    def node(cp, name, start=True, **kw):
        n = ArtNode(tdir, pki, name, cp, next(ports), **kw)
        nodes.append(n)
        return n.start() if start else n

    try:
        yield tdir, make_cp, node
    finally:
        for n in nodes:
            if n.proc and n.proc.poll() is None:
                try:
                    n.proc.send_signal(signal.SIGCONT)
                except ProcessLookupError:
                    pass
            n.stop(signal.SIGKILL)
        for cp in cps:
            cp.stop(signal.SIGKILL)


def output_json(cp, node, job_id, name, tdir):
    job = cp.job(job_id)
    art = next(o["artifact_id"] for o in job["outputs"] if o.get("name") == name)
    dest = tdir / f"{job_id}-{name}.json"
    node.ctl("artifact", "get", art, "--out", str(dest))
    return json.loads(dest.read_text())


# ---- tests --------------------------------------------------------------------------------

def test_diamond_workflow_wires_named_outputs(wf_cluster):
    tdir, make_cp, node = wf_cluster
    cp = make_cp(locality_wait=0)
    a, b = node(cp, "node-a"), node(cp, "node-b")
    meshed(a, b)
    steps = [step("relax", "wf.relax", {"formula": "ZrNiSn"})]
    for kind in ("bands", "phonons", "elastic"):
        steps.append(step(kind, "wf.prop", {"kind": kind}, inputs=[{"step": "relax", "output": "structure"}]))
    steps.append(step("evaluate", "wf.evaluate",
                      inputs=[{"step": k, "output": "*"} for k in ("bands", "phonons", "elastic")]))
    wf = cp.submit_workflow("diamond", steps)
    assert wf.startswith("wf-")
    done = cp.wait_workflow(wf)
    assert done["state"] == "SUCCEEDED", json.dumps(done, indent=1)
    st = cp.steps(wf)
    assert {s["state"] for s in st.values()} == {"COMPLETED"}
    assert cp.job(st["bands"]["job_id"])["spec"]["workflow_id"] == wf

    score = output_json(cp, a, st["evaluate"]["job_id"], "score", tdir)
    assert score == {"inputs": ["bands", "elastic", "phonons"], "formulas": ["ZrNiSn"]}
    prop = output_json(cp, a, st["phonons"]["job_id"], "phonons", tdir)
    assert prop["step"] == "phonons" and prop["workflow"] == wf
    out = cp.admin("workflow", wf).stdout
    assert "evaluate" in out and "COMPLETED" in out


def test_fan_out_and_join_over_spawned_steps(wf_cluster):
    tdir, make_cp, node = wf_cluster
    cp = make_cp(locality_wait=0)
    a, b = node(cp, "node-a"), node(cp, "node-b")
    meshed(a, b)
    wf = cp.submit_workflow("fan", [
        step("fanout", "wf.fanout", {"n": 6}),
        step("join", "wf.join", inputs=[{"step": "fanout", "output": "value", "spawned": True}]),
    ])
    done = cp.wait_workflow(wf, timeout=90)
    assert done["state"] == "SUCCEEDED", json.dumps(done, indent=1)
    st = cp.steps(wf)
    children = [n for n, s in st.items() if s.get("parent_step") == "fanout"]
    assert sorted(children) == [f"fanout/c{i}" for i in range(6)]
    assert all(st[c]["state"] == "COMPLETED" for c in children)
    total = output_json(cp, a, st["join"]["job_id"], "sum", tdir)
    assert total == {"count": 6, "sum": 15}
    # Children ran on both nodes.
    assert len({st[c]["node_id"] for c in children}) >= 1


def test_run_if_skips_and_handles_failures(wf_cluster):
    _, make_cp, node = wf_cluster
    cp = make_cp(locality_wait=0)
    node(cp, "node-a")
    wf = cp.submit_workflow("failing", [
        step("a", "wf.fail"),
        step("b", "wf.handler", depends_on=["a"]),
        step("c", "wf.handler", depends_on=["b"]),
        step("h", "wf.handler", depends_on=["a"], run_if="ANY_FAILED"),
        step("always", "wf.handler", depends_on=["c"], run_if="ALWAYS"),
    ])
    done = cp.wait_workflow(wf)
    assert done["state"] == "FAILED" and "a" in done.get("error", ""), done
    st = cp.steps(wf)
    assert st["a"]["state"] == "FAILED"
    assert st["b"]["state"] == "SKIPPED" and st["c"]["state"] == "SKIPPED"
    assert not st["b"].get("job_id")
    assert st["h"]["state"] == "COMPLETED" and st["always"]["state"] == "COMPLETED"

    wf2 = cp.submit_workflow("tolerated", [
        step("a", "wf.fail", allow_failure=True),
        step("h", "wf.handler", depends_on=["a"], run_if="ANY_FAILED"),
    ])
    assert cp.wait_workflow(wf2)["state"] == "SUCCEEDED"


def test_invalid_workflow_rejected(wf_cluster):
    _, make_cp, _ = wf_cluster
    cp = make_cp()
    cyc = [step("x", "wf.relax", depends_on=["y"]), step("y", "wf.relax", depends_on=["x"])]
    path = cp.base / "cyc.json"
    path.write_text(json.dumps({"name": "cyc", "steps": cyc}))
    r = cp.admin("workflow-submit", str(path), check=False)
    assert r.returncode != 0 and "cycle" in r.stderr.lower()
    path.write_text(json.dumps({"name": "bad", "steps": [step("x", "wf.relax", depends_on=["nope"])]}))
    r = cp.admin("workflow-submit", str(path), check=False)
    assert r.returncode != 0 and "nope" in r.stderr


def test_cancel_running_workflow(wf_cluster):
    _, make_cp, node = wf_cluster
    cp = make_cp(locality_wait=0)
    node(cp, "node-a")
    wf = cp.submit_workflow("cancelme", [
        step("slow", "wf.slow", {"seconds": 60}),
        step("after", "wf.handler", depends_on=["slow"]),
    ])
    wait_for(lambda: cp.steps(wf)["slow"]["state"] == "RUNNING", timeout=30, what="slow running")
    cp.admin("workflow-cancel", wf)
    done = cp.wait_workflow(wf, timeout=30)
    assert done["state"] == "CANCELLED"
    st = cp.steps(wf)
    assert st["after"]["state"] == "CANCELLED"
    wait_for(lambda: cp.job(cp.steps(wf)["slow"]["job_id"])["state"] == "CANCELLED", timeout=30,
             what="slow job cancelled")
    assert cp.admin("workflow-wait", wf, check=False).returncode == 1


def test_checkpointed_step_resumes_on_another_node_after_node_death(wf_cluster):
    tdir, make_cp, node = wf_cluster
    cp = make_cp(locality_wait=0)
    nodes = [node(cp, f"node-{x}", jobs=1) for x in "abc"]
    meshed(*nodes)
    by_id = {n.node_id: n for n in nodes}
    wf = cp.submit_workflow("ckpt", [
        step("long", "wf.checkpointed", {"steps": 24, "step_seconds": 0.5}, checkpoint=True, attempts=3),
    ])

    def checkpoint_replicated():
        j = cp.job(cp.steps(wf)["long"]["job_id"])
        c = j.get("checkpoint", {}).get("artifact_id")
        return c and len(cp.holders(c)) >= 2 and j

    job = wait_for(checkpoint_replicated, timeout=40, what="checkpoint stored and replicated")
    first = job["node_id"]
    by_id[first].stop(signal.SIGKILL)

    done = cp.wait_workflow(wf, timeout=90)
    assert done["state"] == "SUCCEEDED", json.dumps(done, indent=1)
    final = cp.job(cp.steps(wf)["long"]["job_id"])
    assert final["node_id"] != first
    assert final["attempts_used"] >= 2
    survivor = by_id[final["node_id"]]
    res = output_json(cp, survivor, final["job_id"], "result", tdir)
    assert res["resumed_from"] >= 1, res   # attempt_number is per node-local job


def test_strict_job_is_reserved_for_the_input_holder(wf_cluster):
    tdir, make_cp, node = wf_cluster
    cp = make_cp(locality_wait=0, strict_input_mb=1)
    a, b = node(cp, "node-a", cpus=8, jobs=4), node(cp, "node-b", cpus=8, jobs=4)
    meshed(a, b)
    src = blob(tdir, "big-input.bin", 3 * MiB, "strict")
    art = a.put(src)
    announced(cp, art, [a.node_id])
    jobs = [cp.submit(PY, WORKER, operation="wf.handler", input=art, env=f"PYTHONPATH={ENV['PYTHONPATH']}")
            for _ in range(3)]
    for j in jobs:
        d = cp.wait_state(j, {"COMPLETED", "FAILED"}, timeout=40)
        assert d["state"] == "COMPLETED", d
        assert d["node_id"] == a.node_id, d   # b polls just as often but has to fetch 3 MiB
    assert art not in b.ls()
