"""te.validate on synthetic evaluations; literature file schema; import request."""
from __future__ import annotations

import json
import re

import pytest
from google.protobuf import json_format

from conftest import halfheusler
from fireforge_science import validate as va
from fireforge_science.crystalforge.stepio import run_task

ROLES = {"ZrNiSn": ("Zr", "Ni", "Sn"), "HfNiSn": ("Hf", "Ni", "Sn"),
         "TiNiSn": ("Ti", "Ni", "Sn"), "NbFeSb": ("Nb", "Fe", "Sb"),
         "ZrCoSb": ("Zr", "Co", "Sb")}
# synthetic "literature": a, PBE gap (indirect), κ_l 300 K
SYN = {"ZrNiSn": (6.11, 0.50, 8.0), "HfNiSn": (6.08, 0.40, 6.5), "TiNiSn": (5.93, 0.45, 9.5),
       "NbFeSb": (5.95, 0.53, 18.0), "ZrCoSb": (6.07, 1.05, 12.0)}


def syn_literature():
    comps = {}
    for f, (a, g, k) in SYN.items():
        comps[f] = {"roles": list(ROLES[f]),
                    "lattice_constant_angstrom": {"value": a, "doi": "10.0000/syn.a"},
                    "band_gap_pbe_ev": {"value": g, "character": "indirect", "doi": "10.0000/syn.g"},
                    "kappa_l_300k_w_mk": {"value": k, "kind": "lattice", "doi": "10.0000/syn.k"},
                    "zt_peak": {"value": 1.0, "type": "n", "t_k": 900, "doi": "10.0000/syn.z"}}
    return {"format": "fireforge.literature.v1", "compounds": comps}


def evaluation(f, *, engine="qe", a_scale=1.01, dg=0.05, k_scale=1.5, source="slack",
               zt_p=0.5, zt_n=0.5, stable=True):
    a, g, k = SYN[f]
    x, y, z = ROLES[f]
    return {"format": "fireforge.evaluation.v1", "formula": f, "engine": engine,
            "kappa_source": source, "a_angstrom": a * a_scale, "band_gap_ev": g + dg,
            "gap_direct": False, "dynamically_stable": stable, "born_stable": True,
            "formation_ev_per_atom": -0.5, "kappa_l_300k": k * k_scale,
            "zt_peak_p": zt_p, "zt_peak_n": zt_n,
            "structure": halfheusler(x, y, z, a=a * a_scale).to_json()}


def good_set():
    e = {f: evaluation(f) for f in SYN}
    e["NbFeSb"]["zt_peak_p"] = 0.9
    e["ZrNiSn"]["zt_peak_n"] = 0.8
    return e


def write_inputs(tmp_path, evals, lit=None):
    inputs = {}
    for i, (f, d) in enumerate(sorted(evals.items())):
        p = tmp_path / f"ev{i}.json"
        p.write_text(json.dumps(d))
        inputs["evaluation" if i == 0 else f"evaluation#{i + 1}"] = p
    junk = tmp_path / "other.json"
    junk.write_text(json.dumps({"format": "something.else"}))
    inputs["phonon"] = junk
    ref = tmp_path / "lit.json"
    ref.write_text(json.dumps(lit or syn_literature()))
    return inputs, ref


def test_validate_pass(tmp_path):
    ev = good_set()
    ev["ZrNiSn.bte"] = evaluation("ZrNiSn", source="bte", k_scale=1.2)
    inputs, ref = write_inputs(tmp_path, ev)
    res, _, outs = run_task("te.validate", {"reference": str(ref)}, inputs, workdir=tmp_path / "w")
    assert res.success, res.error
    rep = json.loads(outs["report.json"].read_text())
    assert rep["passed"] and rep["n_fail"] == 0, rep
    z = rep["compounds"]["ZrNiSn"]["checks"]
    assert z["lattice"]["status"] == "pass" and z["lattice"]["deviation_rel"] == pytest.approx(0.01)
    assert z["kappa_bte"]["status"] == "pass" and z["kappa_bte"]["ratio"] == pytest.approx(1.2)
    assert z["gap_character"]["status"] == "pass"
    assert rep["set_checks"]["kappa_rank"]["pairs_agree"] == 4
    assert rep["set_checks"]["best_p"]["status"] == "pass"
    assert "| ZrNiSn | lattice | pass" in outs["report.md"].read_text()
    assert len(res.records) == 5 and res.records[0].kind == "te.validation"
    props = json.loads(res.records[0].properties_json)
    assert props["lattice_pass"] is True


def test_validate_fail_paths(tmp_path):
    ev = good_set()
    ev["TiNiSn"] = evaluation("TiNiSn", a_scale=1.03, dg=0.4, k_scale=4.0)
    ev["HfNiSn"] = evaluation("HfNiSn", stable=False)
    ev["HfNiSn"]["gap_direct"] = True
    ev["ZrCoSb"]["zt_peak_p"] = 2.0                           # beats NbFeSb
    inputs, ref = write_inputs(tmp_path, ev)
    res, _, outs = run_task("te.validate", {"reference": str(ref)}, inputs, workdir=tmp_path / "w")
    assert res.success, res.error
    rep = json.loads(outs["report.json"].read_text())
    t = rep["compounds"]["TiNiSn"]["checks"]
    assert t["lattice"]["status"] == t["gap"]["status"] == t["kappa_slack"]["status"] == "fail"
    h = rep["compounds"]["HfNiSn"]["checks"]
    assert h["stable"]["status"] == "fail" and h["gap_character"]["status"] == "fail"
    assert rep["set_checks"]["best_p"]["status"] == "fail"
    assert rep["set_checks"]["best_p"]["computed_best"] == "ZrCoSb"
    assert not rep["passed"] and "FAIL" in outs["report.md"].read_text()
    # ZrNiSn BTE result missing -> no_data, not fail
    assert rep["compounds"]["ZrNiSn"]["checks"]["kappa_bte"]["status"] == "no_data"


def test_kappa_rank_check():
    lit = syn_literature()
    ev = {f: {"slack": evaluation(f)} for f in SYN}
    # swap the computed order of two adjacent compounds (TiNiSn 9.5 vs ZrCoSb 12)
    ev["TiNiSn"]["slack"]["kappa_l_300k"] = 12.0 * 1.5 + 1
    rep = va.validate(ev, lit)
    kr = rep["set_checks"]["kappa_rank"]
    assert kr["pairs"] == 4 and kr["pairs_agree"] < 4
    assert kr["experimental_order"][0] == "HfNiSn"


def test_toy_refused(tmp_path):
    ev = {f: evaluation(f, engine="toy") for f in SYN}
    inputs, ref = write_inputs(tmp_path, ev)
    res, _, _ = run_task("te.validate", {"reference": str(ref)}, inputs, workdir=tmp_path / "w")
    assert not res.success and "toy" in res.error
    res, _, _ = run_task("te.validate", {"reference": str(ref), "allow_toy": True}, inputs,
                         workdir=tmp_path / "w2")
    assert res.success, res.error


def test_no_evaluations(tmp_path):
    res, _, _ = run_task("te.validate", {}, {}, workdir=tmp_path)
    assert not res.success and "no evaluation" in res.error


# ---------------------------------------------------------------------------------------
# the real literature file

QUANTITIES = ("lattice_constant_angstrom", "band_gap_pbe_ev", "kappa_l_300k_w_mk", "zt_peak")
DOI = re.compile(r"^10\.\d{4,9}/\S+$")


def test_literature_schema():
    lit = va.load_literature()
    assert lit["format"] == "fireforge.literature.v1"
    assert set(lit["compounds"]) == set(ROLES)
    for f, c in lit["compounds"].items():
        assert tuple(c["roles"]) == ROLES[f]
        for q in QUANTITIES:
            entry = c[q]
            assert "value" in entry, (f, q)
            if entry["value"] is None:
                assert entry.get("note"), f"{f}.{q}: null without a note"
                continue
            assert isinstance(entry["value"], (int, float))
            assert DOI.match(entry.get("doi") or ""), f"{f}.{q}: value without DOI"
            assert entry.get("citation"), (f, q)
        g = c["band_gap_pbe_ev"]
        if g["value"] is not None:
            assert g.get("character") in ("direct", "indirect")
        z = c["zt_peak"]
        if z["value"] is not None:
            assert z.get("type") in ("p", "n") and z.get("t_k")
        a = c["lattice_constant_angstrom"]["value"]
        if a is not None:
            assert 5.7 < a < 6.3


def test_literature_import_request():
    from fireforge.control.v1 import materials_pb2
    req = va.literature_import_request(campaign_id="validation")
    msg = json_format.ParseDict(req, materials_pb2.ImportCalculationsRequest())
    lit = va.load_literature()
    n = sum(1 for c in lit["compounds"].values() for q in QUANTITIES if c[q]["value"] is not None)
    assert len(msg.records) == n and msg.campaign_id == "validation"
    for r in msg.records:
        assert r.evidence == 1 and r.kind.startswith("literature.")   # LITERATURE
        assert r.structure.spacegroup_number == 216 and "doi:10." in r.note
