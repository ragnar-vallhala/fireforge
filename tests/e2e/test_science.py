"""Phase 8 end-to-end: the thermoelectric workflow on real tdrcp + tdrd with the toy engine
(docs/design/phase8.md §9). Checks the whole plumbing: spawned phonon fan-out, records
stored in the materials database, the validation report, literature import.
"""
import json
import stat

from test_artifacts import ArtNode, meshed
from test_control_plane import ROOT, infra  # noqa: F401  (fixture)
from test_workflows import WfCP, output_json, wf_cluster  # noqa: F401  (fixture)

SCI_CAPS = "sci,phonopy,phono3py,boltztrap2"


def worker_wrapper(tdir):
    """Node-independent worker command, like the fireforge-sci wrapper of a science node."""
    path = tdir / "fireforge-sci"
    path.write_text(
        "#!/usr/bin/env bash\n"
        f"export PYTHONPATH={ROOT}/sdk/python:{ROOT}/science\n"
        f"export FIREFORGE_WORKER_ARGV='[\"{path}\"]'\n"
        f"exec {ROOT}/.venv/bin/python -m fireforge_science.ops\n")
    path.chmod(path.stat().st_mode | stat.S_IXUSR)
    return str(path)


class SciNode(ArtNode):
    """A science node: advertises the Phase 8 capabilities, room for 1 GiB+ steps."""

    def write_conf(self, cpus=None, jobs=None):
        super().write_conf()
        text = self.conf.read_text().replace("[node]\n", f"[node]\ncapabilities = {SCI_CAPS}\n", 1)
        self.conf.write_text(text.replace("max_memory_mb = 2048", "max_memory_mb = 16384"))


def science_node(node, cp, name):
    n = node(cp, name, start=False)
    n.__class__ = SciNode
    return n.start()


def test_toy_validation_workflow_end_to_end(wf_cluster):
    from fireforge_science.workflows import validation_workflow

    tdir, make_cp, node = wf_cluster
    cp = make_cp(locality_wait=0)
    a = science_node(node, cp, "node-a")
    b = science_node(node, cp, "node-b")
    meshed(a, b)

    argv = worker_wrapper(tdir)
    wf = validation_workflow(engine="toy", cpus=1, compounds=["ZrNiSn", "TiNiSn"],
                             argv=[argv], volume_strains=[-0.06, -0.04, -0.02])
    path = wf.write(tdir / "validation.json")
    wf_id = cp.admin("workflow-submit", str(path)).stdout.strip()
    done = cp.wait_workflow(wf_id, timeout=900)
    steps = cp.steps(wf_id)
    bad = {k: (s["state"], s.get("error", "")) for k, s in steps.items() if s["state"] != "COMPLETED"}
    assert done["state"] == "SUCCEEDED" and not bad, json.dumps(bad, indent=1)

    # Phonon fan-out ran as spawned steps; the phono3py tier only for ZrNiSn.
    names = set(steps)
    assert any(n.startswith("ZrNiSn.phonon/") and ".d" in n for n in names), sorted(names)
    assert any(n.startswith("ZrNiSn.evaluate/") and n.endswith("fc3") for n in names), sorted(names)
    assert not any(n.startswith("TiNiSn.evaluate/") for n in names)

    report = output_json(cp, a, steps["validate"]["job_id"], "report.json", tdir)
    assert set(report["compounds"]) == {"ZrNiSn", "TiNiSn"}, report
    assert report["engine"] == "toy" and "kappa_bte" in json.dumps(report["compounds"]["ZrNiSn"])

    # Records reached the materials database, linked to the workflow.
    mats = cp.admin_json("materials")
    formulas = {m["reduced_formula"] for m in mats.get("materials", [])}
    assert {"ZrNiSn", "TiNiSn", "Ni", "Sn", "Zr", "Ti"} <= formulas, formulas
    calcs = cp.admin_json("calcs", "--workflow", wf_id, "--limit", "1000").get("calculations", [])
    kinds = {c["kind"] for c in calcs}
    assert {"qe.relax", "qe.bands", "transport.crta", "phonon.harmonic", "phonon.slack",
            "stability.elastic", "stability.formation", "te.evaluate", "phonon.bte",
            "te.validation"} <= kinds, kinds
    assert all(json.loads(c["method"].get("settings_json") or "{}") is not None for c in calcs)
    assert {c.get("method", {}).get("engine") for c in calcs if c["kind"].startswith("qe.")} == {"toy"}
    zr = next(m for m in mats["materials"] if m["reduced_formula"] == "ZrNiSn")
    mat = cp.admin_json("material", zr["material_id"])
    assert "te.evaluate" in {c["kind"] for c in mat.get("latest", [])}


def test_literature_import(wf_cluster, tmp_path):
    from fireforge_science.validate import literature_import_request

    tdir, make_cp, node = wf_cluster
    cp = make_cp()
    req = literature_import_request()
    path = tmp_path / "lit.json"
    path.write_text(req if isinstance(req, str) else json.dumps(req))
    out = json.loads(cp.admin("calc-import", str(path), "--json").stdout)
    assert len(out.get("calc_ids", [])) >= 5
    lit = cp.admin_json("calcs", "--evidence", "LITERATURE", "--limit", "1000")["calculations"]
    assert {c["reduced_formula"] for c in lit} >= {"ZrNiSn", "HfNiSn", "TiNiSn", "NbFeSb", "ZrCoSb"}
