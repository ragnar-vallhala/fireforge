"""StabilityForge: formation energies and hull with synthetic energies; Born criteria."""
from __future__ import annotations

import json

import pytest

from conftest import halfheusler
from fireforge_science.crystalforge.stepio import parse_energy, run_task
from fireforge_science.stabilityforge import born_cubic, born_from_elastic, formation_and_hull


def e(formula, epa, engine="qe"):
    return parse_energy({"formula": formula, "energy_ev_per_atom": epa, "engine": engine}, formula)


def test_formation_and_hull_binary():
    refs = [e("Zr", -8.0), e("Ni", -5.0), e("Zr", -7.9)]     # lowest per element is used
    stable = e("ZrNi", -7.0)            # ΔH = -7 - (-6.5) = -0.5
    unstable = e("ZrNi3", -5.6)         # ΔH = -5.6 - (-5.75) = +0.15
    doc = formation_and_hull([stable, unstable], refs)
    s, u = doc["compounds"]
    assert s["formation_ev_per_atom"] == pytest.approx(-0.5)
    assert s["on_hull"] and s["e_above_hull_ev_per_atom"] == 0 and s["decomposition"] == {}
    assert u["formation_ev_per_atom"] == pytest.approx(0.15)
    # hull at x(Ni)=0.75: half ZrNi (-0.5) + half Ni (0) = -0.25 -> above by 0.4
    assert u["e_above_hull_ev_per_atom"] == pytest.approx(0.4)
    assert set(u["decomposition"]) == {"ZrNi", "Ni"} and not u["on_hull"]
    assert doc["hull_phases"] == ["Ni", "ZrNi", "Zr"] or set(doc["hull_phases"]) == {"Ni", "ZrNi", "Zr"}
    assert doc["formula"] == "ZrNi" and doc["references"] == {"Zr": -8.0, "Ni": -5.0}


def test_ternary_competing_phase():
    refs = [e("Zr", -8.0), e("Ni", -5.0), e("Sn", -4.0)]
    hh = e("ZrNiSn", -6.0)          # ΔH = -6 - (-17/3) = -0.333
    comp = e("ZrSn", -6.6)          # ΔH = -0.6 ; hull at ZrNiSn: 2/3*(-0.6) = -0.4
    doc = formation_and_hull([hh], refs + [comp])
    assert doc["formation_ev_per_atom"] == pytest.approx(-1 / 3)
    assert doc["e_above_hull_ev_per_atom"] == pytest.approx(0.4 - 1 / 3)
    assert set(doc["decomposition"]) == {"ZrSn", "Ni"}
    # a competing phase outside the chemical space is ignored
    doc2 = formation_and_hull([hh], refs + [e("Fe", -8.3), e("FeSn", -9.0)])
    assert doc2["on_hull"]


def test_missing_reference():
    with pytest.raises(ValueError, match="Sn"):
        formation_and_hull([e("ZrNiSn", -6.0)], [e("Zr", -8.0), e("Ni", -5.0)])


def test_born():
    assert born_cubic(250, 100, 70)
    assert not born_cubic(100, 120, 50)     # C11 < C12
    assert not born_cubic(200, 100, -5)
    assert not born_cubic(100, -60, 30)     # C11 + 2C12 < 0
    assert born_from_elastic({"c11_gpa": 250, "c12_gpa": 100, "c44_gpa": 70, "born_stable": False})
    assert born_from_elastic({"born_stable": True}) is True
    assert born_from_elastic({}) is None


def _write(p, doc):
    p.write_text(json.dumps(doc))
    return p


def test_op_formation(tmp_path):
    s = halfheusler()
    sdoc = s.to_json()
    inputs = {
        "compound": _write(tmp_path / "c.json", {**sdoc, "energy_ev_per_atom": -6.0, "engine": "qe"}),
        "ref.Zr": _write(tmp_path / "zr.json", {"energy_ev": -16.0, "natoms": 2, "formula": "Zr2",
                                                "engine": "qe"}),
        "ref.Ni": _write(tmp_path / "ni.json", {"energy_ev_per_atom": -5.0, "species": ["Ni"],
                                                "engine": "qe"}),
        "ref.Sn": _write(tmp_path / "sn.json", {"energy_ev_per_atom": -4.0, "formula": "Sn",
                                                "engine": "qe"}),
    }
    res, _, outs = run_task("stability.formation", {"extra_entries": [
        {"formula": "ZrSn", "energy_ev_per_atom": -6.6}]}, inputs, workdir=tmp_path / "w")
    assert res.success, res.error
    doc = json.loads(outs["stability"].read_text())
    assert doc["formation_ev_per_atom"] == pytest.approx(-1 / 3)
    assert doc["e_above_hull_ev_per_atom"] == pytest.approx(0.4 - 1 / 3)
    assert doc["engine"] == "qe"
    (rec,) = res.records
    assert rec.kind == "stability.formation"
    assert rec.structure.fingerprint == s.fingerprint()
    assert json.loads(rec.properties_json)["formation_ev_per_atom"] == pytest.approx(-1 / 3)


def test_op_formation_errors(tmp_path):
    a = _write(tmp_path / "a.json", {"formula": "Zr", "energy_ev_per_atom": -8, "engine": "toy"})
    b = _write(tmp_path / "b.json", {**halfheusler().to_json(), "energy_ev_per_atom": -6,
                                     "engine": "qe"})
    res, _, _ = run_task("stability.formation", {}, {"ref.Zr": a}, workdir=tmp_path / "1")
    assert not res.success and "'compound'" in res.error
    res, _, _ = run_task("stability.formation", {}, {"compound": b, "ref.Zr": a},
                         workdir=tmp_path / "2")
    assert not res.success and "different engines" in res.error
    c = _write(tmp_path / "c.json", {"formula": "ZrNiSn", "engine": "qe"})
    res, _, _ = run_task("stability.formation", {}, {"compound": c}, workdir=tmp_path / "3")
    assert not res.success and "energy_ev_per_atom" in res.error
