"""CrystalForge: half-Heusler enumeration, references, crystal.select (ToyEngine)."""
from __future__ import annotations

import json
import sys
import types

import pytest

from conftest import halfheusler
from fireforge.tdr.v1 import job_pb2
from fireforge_science.crystalforge import (REFERENCES, enumerate_halfheuslers, plan_spawns,
                                            reference_structure, valence_electrons, vec)
from fireforge_science.crystalforge import halfheusler as hh
from fireforge_science.crystalforge.stepio import run_task
from fireforge_science.settings import Settings
from fireforge_science.toy import ToyEngine

VALIDATION = {"ZrNiSn": ("Zr", "Ni", "Sn"), "HfNiSn": ("Hf", "Ni", "Sn"),
              "TiNiSn": ("Ti", "Ni", "Sn"), "NbFeSb": ("Nb", "Fe", "Sb"),
              "ZrCoSb": ("Zr", "Co", "Sb")}


def test_valence_electrons():
    assert [valence_electrons(e) for e in ("Zr", "Ni", "Sn", "Nb", "Fe", "Sb", "Co")] == \
        [4, 10, 4, 5, 8, 5, 9]
    assert valence_electrons("Zn") == 2 and valence_electrons("Li") == 1
    assert valence_electrons("La") == 3 and valence_electrons("Te") == 6
    for x, y, z in VALIDATION.values():
        assert vec([x, y, z]) == 18


def test_enumeration_counts_and_filter():
    c = enumerate_halfheuslers(["Ti", "Zr", "Hf"], ["Ni"], ["Sn"])
    assert len(c) == 9 and {x.composition for x in c} == {"TiNiSn", "ZrNiSn", "HfNiSn"}
    assert len(enumerate_halfheuslers(["Ti", "Zr", "Hf"], ["Ni"], ["Sn"], orderings="known")) == 3
    # electron filter: only the 18-electron compositions survive
    c = enumerate_halfheuslers(["Ti", "Nb"], ["Ni", "Co"], ["Sn", "Sb"])
    assert {x.composition for x in c} == {"TiNiSn", "TiCoSb", "NbCoSn"}
    c19 = enumerate_halfheuslers(["Ti", "Nb"], ["Ni", "Co"], ["Sn", "Sb"], electron_counts=[19])
    assert {x.composition for x in c19} == {"TiNiSb", "NbNiSn", "NbCoSb"}
    # overlapping lists: a composition is kept once; identical elements never combine
    c = enumerate_halfheuslers(["Ti", "Ni"], ["Ni", "Ti"], ["Sn"], electron_counts=[18])
    assert sorted(x.id for x in c) == ["TiNiSn.x4c", "TiNiSn.y4c", "TiNiSn.z4c"]
    assert enumerate_halfheuslers("Zr Hf", "Ni", "Sn", orderings="known")[1].composition == "HfNiSn"


def test_enumeration_errors():
    with pytest.raises(ValueError, match="SSSP"):
        enumerate_halfheuslers(["Zr"], ["Ni"], ["Sn"], allowed_elements={"Zr", "Ni"})
    with pytest.raises(ValueError):
        enumerate_halfheuslers(["Xx"], ["Ni"], ["Sn"])
    with pytest.raises(ValueError):
        enumerate_halfheuslers([], ["Ni"], ["Sn"])
    with pytest.raises(ValueError):
        enumerate_halfheuslers(["Zr"], ["Ni"], ["Sn"], orderings="some")


def test_three_orderings_distinct():
    cands = enumerate_halfheuslers(["Zr"], ["Ni"], ["Sn"])
    assert [c.ordering for c in cands] == ["x4c", "y4c", "z4c"]
    fps = {c.structure.fingerprint() for c in cands}
    assert len(fps) == 3
    for c in cands:
        assert c.structure.spacegroup() == (216, "F-43m")
        assert c.structure.prototype == "half-heusler"
    # site on 4c per label
    sites = {c.ordering: hh.site_assignment("Zr", "Ni", "Sn", c.ordering)["4c"] for c in cands}
    assert sites == {"x4c": "Zr", "y4c": "Ni", "z4c": "Sn"}
    a, b, c = (x.structure for x in cands)
    assert not a.matches(b) and not b.matches(c) and not a.matches(c)
    # 4a <-> 4b swap is the same structure (only three orderings exist)
    swapped = hh.build("Sn", "Ni", "Zr", "y4c", a=6.1)
    assert swapped.matches(hh.build("Zr", "Ni", "Sn", "y4c", a=6.1))


def test_known_ordering_is_conftest_convention():
    for f, (x, y, z) in VALIDATION.items():
        s = hh.build(x, y, z, "y4c", a=6.0)
        assert s.fingerprint() == halfheusler(x, y, z, a=6.0).fingerprint(), f
        cand = enumerate_halfheuslers([x], [y], [z], orderings="known")
        assert len(cand) == 1 and cand[0].to_json()["site_4c"] == y
        assert cand[0].to_json()["known_ordering"]


def test_initial_lattice_constant_rule():
    exp = {"ZrNiSn": 6.11, "HfNiSn": 6.08, "TiNiSn": 5.93, "NbFeSb": 5.95, "ZrCoSb": 6.07}
    for f, (x, y, z) in VALIDATION.items():
        a = hh.initial_a(hh.site_assignment(x, y, z, "y4c"))
        assert abs(a - exp[f]) / exp[f] < 0.04, (f, a)


def test_references_for_validation_elements():
    for el in ("Zr", "Hf", "Ti", "Nb", "Ni", "Fe", "Co", "Sn", "Sb"):
        s = reference_structure(el)
        assert s.elements == [el]
    expect = {"Ti": 194, "Zr": 194, "Hf": 194, "Co": 194, "Ni": 225, "Fe": 229, "Nb": 229,
              "Sn": 227, "Sb": 166, "Te": 152, "Pb": 225, "Bi": 166, "Ge": 227, "Si": 227,
              "Ga": 64, "In": 139}
    for el, sg in expect.items():
        assert reference_structure(el).spacegroup()[0] == sg, el
    assert reference_structure("Fe").magmoms == [2.2]
    assert reference_structure("Co").magmoms == [1.6, 1.6]
    assert reference_structure("Ni").magmoms == [0.6]
    assert reference_structure("Zr").magmoms == []
    # sane volumes per atom (Å^3) for every table entry
    for el in REFERENCES:
        s = reference_structure(el)
        assert 8 < s.volume() / s.nsites < 40, el
    with pytest.raises(KeyError):
        reference_structure("Mn")


def test_op_reference(tmp_path):
    res, _, outs = run_task("crystal.reference", {"element": "Sn"}, workdir=tmp_path)
    assert res.success, res.error
    doc = json.loads(outs["structure"].read_text())
    assert doc["spacegroup_number"] == 227
    res, _, _ = run_task("crystal.reference", {"element": "Mn"}, workdir=tmp_path / "b")
    assert not res.success and "no reference" in res.error


def _spawn(res, name):
    return next(s for s in res.spawn if s.name == name)


def test_op_halfheusler_spawn_shape(tmp_path):
    res, _, outs = run_task("crystal.halfheusler",
                            {"x": ["Zr", "Hf"], "y": ["Ni"], "z": ["Sn"], "engine": "toy",
                             "cpus": 2, "pipeline": True}, workdir=tmp_path)
    assert res.success, res.error
    cands = json.loads(outs["candidates"].read_text())
    assert len(cands) == 6 and "cand.ZrNiSn.y4c" in outs
    names = [s.name for s in res.spawn]
    assert len(names) == 8
    assert {n for n in names if n.startswith("select.")} == {"select.ZrNiSn", "select.HfNiSn"}
    r = _spawn(res, "relax.ZrNiSn.y4c")
    assert r.job.operation == "qe.relax" and r.allow_failure
    assert json.loads(r.job.parameters_json)["preset"] == "screening"
    assert [(i.step, i.output, i.input_name) for i in r.inputs] == \
        [(".", "cand.ZrNiSn.y4c", "structure")]
    assert r.job.resources.cpu_cores == 2 and list(r.job.required_capabilities) == ["sci"]
    sel = _spawn(res, "select.ZrNiSn")
    assert sel.run_if == job_pb2.ALWAYS and sel.job.operation == "crystal.select"
    assert sorted(sel.depends_on) == ["relax.ZrNiSn.x4c", "relax.ZrNiSn.y4c", "relax.ZrNiSn.z4c"]
    wired = {(i.step, i.output, i.input_name) for i in sel.inputs}
    assert ("relax.ZrNiSn.z4c", "energy", "energy.z4c") in wired
    assert ("relax.ZrNiSn.y4c", "structure", "structure.y4c") in wired
    assert json.loads(sel.job.parameters_json)["pipeline"] is True
    # qe engine adds the qe capability
    specs = plan_spawns(enumerate_halfheuslers(["Zr"], ["Ni"], ["Sn"]), engine="qe")
    assert specs[0]["capabilities"] == ["sci", "qe"]


def test_op_halfheusler_no_candidates(tmp_path):
    res, _, _ = run_task("crystal.halfheusler", {"x": ["Zr"], "y": ["Co"], "z": ["Sn"],
                                                 "engine": "toy"}, workdir=tmp_path)
    assert not res.success and "electron-count" in res.error


@pytest.fixture(scope="module")
def relaxed_orderings(tmp_path_factory):
    """Toy-relaxed ZrNiSn in the three orderings, as qe.relax would output them."""
    d = tmp_path_factory.mktemp("orderings")
    toy = ToyEngine()
    inputs = {}
    energies = {}
    for o in hh.ORDERINGS:
        r = toy.relax(hh.build("Zr", "Ni", "Sn", o), Settings())
        s = r.structure
        (d / f"s.{o}.json").write_text(json.dumps(s.to_json()))
        epa = r.energy_ev / s.nsites
        energies[o] = epa
        (d / f"e.{o}.json").write_text(json.dumps(
            {"energy_ev": r.energy_ev, "natoms": s.nsites, "energy_ev_per_atom": epa,
             "formula": s.formula(), "engine": "toy"}))
        inputs[f"structure.{o}"] = d / f"s.{o}.json"
        inputs[f"energy.{o}"] = d / f"e.{o}.json"
    return inputs, energies


def test_op_select_keeps_lowest(tmp_path, relaxed_orderings, monkeypatch):
    inputs, energies = relaxed_orderings
    assert min(energies, key=energies.get) == "y4c"   # toy: known ordering lowest
    # pipeline requested but no builder importable -> select only
    import fireforge_science.crystalforge as cf
    monkeypatch.setattr(cf, "_pipeline_builder", lambda: None)
    res, _, outs = run_task("crystal.select", {"composition": "ZrNiSn", "pipeline": True},
                            inputs, workdir=tmp_path)
    assert res.success, res.error
    sel = json.loads(outs["selection"].read_text())
    assert sel["chosen"] == "y4c" and len(sel["orderings"]) == 3
    assert sel["orderings"][0]["delta_mev_per_atom"] == 0.0
    assert all(o["delta_mev_per_atom"] > 0 for o in sel["orderings"][1:])
    assert "not available" in sel["pipeline"] and len(res.spawn) == 0
    st = json.loads(outs["structure"].read_text())
    assert st["sites"][1]["species"] == "Ni"
    assert json.loads(outs["energy"].read_text())["energy_ev_per_atom"] == energies["y4c"]


def test_op_select_partial_and_pipeline(tmp_path, relaxed_orderings, monkeypatch):
    inputs, _ = relaxed_orderings
    part = {k: v for k, v in inputs.items() if not k.endswith("y4c")}   # failed relax
    calls = []

    def material_pipeline_steps(structure, prefix, params):
        calls.append((structure, prefix, params))
        return [{"name": f"{prefix}.relax", "operation": "qe.relax", "inputs": [structure]},
                {"name": f"{prefix}.scf", "operation": "qe.scf",
                 "inputs": [f"{prefix}.relax:structure"]}]
    mod = types.ModuleType("fireforge_science.workflows")
    mod.material_pipeline_steps = material_pipeline_steps
    monkeypatch.setitem(sys.modules, "fireforge_science.workflows", mod)
    import fireforge_science
    monkeypatch.setattr(fireforge_science, "workflows", mod, raising=False)
    res, _, outs = run_task("crystal.select", {"composition": "ZrNiSn", "pipeline": True,
                                               "preset": "accurate",
                                               "pipeline_params": {"tier2": "off"}},
                            part, workdir=tmp_path)
    assert res.success, res.error
    assert json.loads(outs["selection"].read_text())["chosen"] in ("x4c", "z4c")
    assert [s.name for s in res.spawn] == ["ZrNiSn.relax", "ZrNiSn.scf"]
    si, prefix, params = calls[0]
    assert (si.step, si.output, si.as_name) == (".", "structure", "structure")
    assert params["preset"] == "accurate" and params["tier2"] == "off"
    assert params["engine"] == "toy"


def test_op_select_errors(tmp_path, relaxed_orderings):
    res, _, _ = run_task("crystal.select", {}, {}, workdir=tmp_path)
    assert not res.success and "no relaxed ordering" in res.error
    inputs, _ = relaxed_orderings
    bad = tmp_path / "bad.json"
    bad.write_text(json.dumps({"natoms": 3, "formula": "ZrNiSn"}))
    res, _, _ = run_task("crystal.select", {}, {"energy.x4c": bad,
                                                "structure.x4c": inputs["structure.x4c"]},
                         workdir=tmp_path / "b")
    assert not res.success and "energy_ev_per_atom" in res.error
