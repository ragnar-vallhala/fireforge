"""Live Quantum ESPRESSO tests (`-m qe`; skipped without pw.x or SSSP). Si 2-atom at a low
cutoff (seconds per run), ZrNiSn scf at screening settings with 2 MPI ranks (minutes)."""
from __future__ import annotations

import json
import os
from pathlib import Path

import numpy as np
import pytest

from fireforge.testing import FakeDaemon
from fireforge_science.engine import BandData
from fireforge_science.model import Structure
from fireforge_science.quantumforge import QeEngine
from fireforge_science.settings import preset

from conftest import halfheusler

pytestmark = [pytest.mark.qe, pytest.mark.pseudo]

WORKER = Path(__file__).with_name("data") / "qe" / "worker.py"
SCIENCE = Path(__file__).resolve().parent.parent
SI_LOW = {"ecutwfc_ry": 20.0, "ecutrho_ry": 160.0}


def si(a=5.43):
    h = a / 2
    return Structure([[0, h, h], [h, 0, h], [h, h, 0]], ["Si", "Si"],
                     [[0, 0, 0], [0.25, 0.25, 0.25]])


def si_settings(**kw):
    return preset("screening", ["Si"], kdensity=0.4, kdensity_dense=0.3, **SI_LOW, **kw)


def test_si_engine_relax_scf_nscf(tmp_path):
    eng = QeEngine(cpus=1)
    st = si_settings()
    r = eng.relax(si(5.35), st, workdir=tmp_path)
    assert r.converged and abs(r.pressure_kbar) < 2.0
    a = (4 * r.structure.volume()) ** (1 / 3)
    assert 5.40 < a < 5.52                                  # PBE Si ≈ 5.47 Å
    assert r.provenance.solver == "quantum-espresso" and r.provenance.solver_version.startswith("6.7")
    assert r.provenance.pseudopotentials[0].startswith("Si.pbe-n-rrkjus_psl.1.0.0.UPF md5:")
    assert r.provenance.settings["calculation"] == "vc-relax"
    sc = eng.scf(r.structure, st, workdir=tmp_path)
    assert sc.converged and sc.save_dir.is_dir() and np.abs(sc.forces).max() < 1e-3
    if sc.provenance.settings["kmesh"] == r.provenance.settings["kmesh"]:   # 0.4 Å⁻¹: edge
        assert sc.energy_ev == pytest.approx(r.energy_ev, abs=2e-3)
    bd = eng.nscf(r.structure, sc.save_dir, st, workdir=tmp_path)
    gap, direct, vbm, cbm = bd.band_gap()
    assert 0.3 < gap < 0.9 and not direct
    assert bd.ebands.shape[0] >= 12 and bd.dosweight == 2.0
    assert bd.provenance.settings["nk_irreducible"] == len(bd.kpoints)


def _run(op, params, inputs, **kw):
    d = FakeDaemon(operation=op, params={"engine": "qe", "settings": SI_LOW, "kdensity": 0.4,
                                         "kdensity_dense": 0.3, **params}, **kw)
    d.__enter__()
    for name, data in inputs.items():
        d.add_input(data, name=name)
    pp = [str(SCIENCE)] + [x for x in d.env()["PYTHONPATH"].split(os.pathsep)
                           if x and Path(x).is_absolute()]
    proc = d.run_worker(WORKER, timeout=900, extra_env={"PYTHONPATH": os.pathsep.join(pp)})
    return d, proc


def test_si_ops_scf_nscf_elastic():
    sj = json.dumps(si(5.47).to_json())
    d, proc = _run("qe.scf", {"cpus": 2}, {"structure": sj})
    try:
        assert proc.returncode == 0, proc.stderr + d.result.error
        outs = {o.name: o for o in d.result.outputs}
        save = (d.output_dir / outs["save"].path).read_bytes()
        assert "PWSCF" in (d.output_dir / outs["log"].path).read_text()
        [rec] = d.records
        assert rec.method.engine == "qe" and json.loads(rec.method.settings_json)["cpus"] == 2
    finally:
        d.__exit__(None, None, None)
    d, proc = _run("qe.nscf", {}, {"structure": sj, "save": save})
    try:
        assert proc.returncode == 0, proc.stderr + d.result.error
        outs = {o.name: o for o in d.result.outputs}
        bd = BandData.load(d.output_dir / outs["bands"].path)
        p = json.loads(d.records[0].properties_json)
        assert p["band_gap_ev"] == pytest.approx(bd.band_gap()[0]) and 0.3 < p["band_gap_ev"] < 0.9
    finally:
        d.__exit__(None, None, None)
    d, proc = _run("qe.elastic", {}, {"structure": sj})
    try:
        assert proc.returncode == 0, proc.stderr + d.result.error
        p = json.loads(d.records[0].properties_json)
        # PBE Si: C11 ≈ 153, C12 ≈ 57, C44 ≈ 75 GPa (low cutoff here: loose bounds)
        assert p["born_stable"] and 120 < p["c11_gpa"] < 190 and 35 < p["c12_gpa"] < 80
        assert 50 < p["c44_gpa"] < 100 and 500 < p["theta_d_elastic_k"] < 750
    finally:
        d.__exit__(None, None, None)


@pytest.mark.skipif(not os.environ.get("FIREFORGE_QE_SLOW"),
                    reason="ZrNiSn screening scf takes minutes (FIREFORGE_QE_SLOW=1)")
def test_zrnisn_scf_screening(tmp_path):
    s = halfheusler()
    st = preset("screening", s.elements)
    r = QeEngine(cpus=2).scf(s, st, workdir=tmp_path)
    assert r.converged and np.abs(r.forces).max() < 1e-3
    assert r.provenance.settings["nspin"] == 2 and r.provenance.settings["kmesh"] == [8, 8, 8]
    assert abs(np.trace(np.asarray(r.stress_kbar)) / 3) < 50        # near the PBE minimum
