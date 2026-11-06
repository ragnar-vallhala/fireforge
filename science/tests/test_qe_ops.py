"""qe.* operations through FakeDaemon with engine=toy (same code path as QE), the save
archive, and the cubic elastic-constant arithmetic."""
from __future__ import annotations

import json
import os
from pathlib import Path

import numpy as np
import pytest

from fireforge.testing import FakeDaemon
from fireforge_science.engine import BandData
from fireforge_science.model import Structure
from fireforge_science.quantumforge import ops

from conftest import halfheusler

WORKER = Path(__file__).with_name("data") / "qe" / "worker.py"
SCIENCE = Path(__file__).resolve().parent.parent      # science/ (fireforge_science)


def run(op, params, inputs, **kw):
    d = FakeDaemon(operation=op, params={"engine": "toy", **params}, **kw)
    d.__enter__()
    for name, data in inputs.items():
        d.add_input(data, name=name)
    pp = [str(SCIENCE)] + [x for x in d.env()["PYTHONPATH"].split(os.pathsep)
                           if x and Path(x).is_absolute()]
    proc = d.run_worker(WORKER, timeout=120, extra_env={"PYTHONPATH": os.pathsep.join(pp)})
    return d, proc


def sjson(s: Structure, **extra) -> str:
    return json.dumps({**s.to_json(), **extra})


def props(rec):
    return json.loads(rec.properties_json)


def test_relax_scf_nscf_chain(relaxed_zrnisn):
    s0 = halfheusler(a=6.0)
    d, proc = run("qe.relax", {}, {"structure": sjson(s0, structure_id="st-123")})
    try:
        assert proc.returncode == 0, proc.stderr
        assert d.result.success
        outs = {o.name: o for o in d.result.outputs}
        assert set(outs) == {"structure", "log", "energy"}
        relaxed = Structure.from_json(json.loads((d.output_dir / outs["structure"].path).read_text()))
        assert relaxed.parent_structure_id == "st-123" and relaxed.source == "qe.relax"
        assert relaxed.matches(relaxed_zrnisn)
        [rec] = d.records
        assert rec.kind == "qe.relax" and rec.structure.parent_structure_id == "st-123"
        assert rec.structure.fingerprint == relaxed.fingerprint()
        p = props(rec)
        assert set(p) >= {"energy_ev_per_atom", "a_angstrom", "volume_a3_per_atom",
                          "pressure_kbar", "magnetization_mub"}
        assert p["a_angstrom"] == pytest.approx((4 * relaxed.volume()) ** (1 / 3), rel=1e-6)
        assert p["volume_a3_per_atom"] == pytest.approx(relaxed.volume() / 3)
        assert rec.method.engine == "toy" and rec.method.preset == "screening"
        assert json.loads(rec.convergence_json)["converged"] is True
        assert list(rec.output_names) == ["structure", "log", "energy"]
        relaxed_text = (d.output_dir / outs["structure"].path).read_text()
    finally:
        d.__exit__(None, None, None)

    d, proc = run("qe.scf", {}, {"structure": relaxed_text})
    try:
        assert proc.returncode == 0, proc.stderr
        outs = {o.name: o for o in d.result.outputs}
        assert set(outs) == {"save", "log"}
        assert outs["save"].path.startswith("save.tar.")
        save = (d.output_dir / outs["save"].path).read_bytes()
        [rec] = d.records
        assert rec.kind == "qe.scf" and set(props(rec)) >= {"energy_ev_per_atom", "fermi_ev"}
    finally:
        d.__exit__(None, None, None)

    d, proc = run("qe.nscf", {}, {"structure": relaxed_text, "save": save})
    try:
        assert proc.returncode == 0, proc.stderr
        outs = {o.name: o for o in d.result.outputs}
        assert set(outs) == {"bands", "log"}
        bd = BandData.load(d.output_dir / outs["bands"].path)
        [rec] = d.records
        p = props(rec)
        assert rec.kind == "qe.bands"
        assert p["band_gap_ev"] == pytest.approx(bd.band_gap()[0]) and p["band_gap_ev"] > 0
        assert p["gap_direct"] is True and p["nelect"] == 4.0
        assert {"vbm_ev", "cbm_ev"} <= set(p)
    finally:
        d.__exit__(None, None, None)


def test_forces_output():
    sc = halfheusler().supercell([2, 1, 1])
    frac = sc.frac_array.copy()
    frac[0] += [0.01, 0, 0]
    d, proc = run("qe.forces", {"label": "disp-001"},
                  {"supercell": sjson(sc.copy(frac=frac.tolist()))})
    try:
        assert proc.returncode == 0, proc.stderr
        [o] = d.result.outputs
        assert o.name == "forces" and not d.records
        f = ops.load_forces(d.output_dir / o.path)
        assert f["forces"].shape == (6, 3) and f["label"] == "disp-001" and f["natoms"] == 6
        assert abs(f["forces"][0, 0]) > 1e-4 and f["stress_kbar"].shape == (3, 3)
        assert f["provenance"]["engine"] == "toy" and f["species"] == sc.species
    finally:
        d.__exit__(None, None, None)


def test_elastic_toy_halfheusler(relaxed_zrnisn):
    d, proc = run("qe.elastic", {"strain": 0.01}, {"structure": sjson(relaxed_zrnisn)},
                  checkpoint_enabled=True)
    try:
        assert proc.returncode == 0, proc.stderr
        [o] = d.result.outputs
        assert o.name == "elastic"
        doc = json.loads((d.output_dir / o.path).read_text())
        [rec] = d.records
        assert rec.kind == "stability.elastic"
        p = props(rec)
        for k in ("c11_gpa", "c12_gpa", "c44_gpa", "bulk_gpa", "shear_gpa", "v_l", "v_t",
                  "theta_d_elastic_k"):
            assert p[k] > 0, k
        assert p["born_stable"] is True and p["v_l"] > p["v_t"]
        assert p["c11_gpa"] > p["c12_gpa"]
        assert doc["format"] == "fireforge.elastic.v1" and set(doc["stress_kbar"]) == {
            "xx_p", "xx_m", "shear_p", "shear_m"}
        assert doc["c11_gpa"] == p["c11_gpa"] and len(doc["c_matrix_gpa"]) == 6
        assert len(d.checkpoints) >= 1 and d.checkpoints[0].name == "elastic"
        last_ck = d.checkpoint_files[-1]
    finally:
        d.__exit__(None, None, None)

    # resume: every strain already done → no recomputation, same result
    d, proc = run("qe.elastic", {"strain": 0.01}, {"structure": sjson(relaxed_zrnisn)},
                  checkpoint_enabled=True, resume_checkpoint=json.dumps(
                      {**json.loads(last_ck), "stress_kbar": doc["stress_kbar"]}))
    try:
        assert proc.returncode == 0, proc.stderr
        assert props(d.records[0])["c11_gpa"] == pytest.approx(p["c11_gpa"])
        assert not d.checkpoints
    finally:
        d.__exit__(None, None, None)


def test_elastic_non_cubic_fails():
    s = Structure([[3.0, 0, 0], [0, 3.0, 0], [0, 0, 4.0]], ["Si"], [[0, 0, 0]])
    d, proc = run("qe.elastic", {}, {"structure": sjson(s)})
    try:
        assert proc.returncode == 1
        assert not d.result.success and "only cubic implemented" in d.result.error
        assert not d.result.retryable
    finally:
        d.__exit__(None, None, None)


def test_bad_params_fail_cleanly():
    d, proc = run("qe.scf", {"settings": {"nope": 1}}, {"structure": sjson(halfheusler())})
    try:
        assert proc.returncode == 1 and "unknown settings: nope" in d.result.error
    finally:
        d.__exit__(None, None, None)
    d, proc = run("qe.scf", {"engine": "vasp"}, {"structure": sjson(halfheusler())})
    try:
        assert proc.returncode == 1 and "unknown engine" in d.result.error
    finally:
        d.__exit__(None, None, None)


def test_strain_rotation_invariance(toy, relaxed_zrnisn):
    """The same cubic strain applied to a rotated cell gives the same standard-frame stress."""
    from fireforge_science.model import spglib_call
    from fireforge_science.settings import Settings
    s = relaxed_zrnisn
    th = 0.3
    rotz = np.array([[np.cos(th), -np.sin(th), 0], [np.sin(th), np.cos(th), 0], [0, 0, 1]])
    s2 = s.copy(lattice=(s.lattice_array @ rotz.T).tolist())
    out = []
    for x in (s, s2):
        ds = spglib_call("get_symmetry_dataset", x.to_spglib_cell(), symprec=1e-3)
        rot = np.asarray(ds.std_rotation_matrix)
        e = ops.strain_set(0.01)[0][1]
        sig = np.asarray(toy.stress(ops.strained(x, e, rot), Settings()))
        out.append(rot @ sig @ rot.T)
    np.testing.assert_allclose(out[0], out[1], atol=1e-6)


def test_cubic_moduli_reference():
    """Cubic Si (C11 166, C12 64, C44 80 GPa; ρ 2329 kg/m³): B = 98 GPa, G_VRH ≈ 66.5 GPa,
    v_l ≈ 9.0 km/s, v_t ≈ 5.3 km/s, θ_D(elastic) ≈ 650 K (lit. 645 K)."""
    v_atom = 5.431 ** 3 / 8
    r = ops.cubic_moduli(166.0, 64.0, 80.0, 2329.0, v_atom)
    assert r["born_stable"] and r["bulk_gpa"] == pytest.approx(98.0)
    assert r["shear_gpa"] == pytest.approx(66.5, abs=0.5)
    assert r["v_l"] == pytest.approx(9000, rel=0.02) and r["v_t"] == pytest.approx(5340, rel=0.02)
    assert r["theta_d_elastic_k"] == pytest.approx(650, rel=0.03)
    bad = ops.cubic_moduli(100.0, 120.0, 50.0, 5000.0, 20.0)
    assert not bad["born_stable"] and bad["theta_d_elastic_k"] == 0.0


def test_save_archive_roundtrip(tmp_path):
    save = tmp_path / "ff.save"
    save.mkdir()
    (save / "data-file-schema.xml").write_text("<x/>")
    (save / "charge-density.dat").write_bytes(b"\0" * 100)
    (save / "wfc1.dat").write_bytes(b"\1" * 100)
    arch = ops.pack_save(save, tmp_path / "save")
    assert arch.name in ("save.tar.zst", "save.tar.gz")
    got = ops.unpack_save(arch, tmp_path / "out")
    assert got == tmp_path / "out" / "ff.save"
    assert sorted(p.name for p in got.iterdir()) == ["charge-density.dat", "data-file-schema.xml"]
    stub = ops.pack_save(None, tmp_path / "toy", {"engine": "toy"})
    assert ops.unpack_save(stub, tmp_path / "out2") is None
