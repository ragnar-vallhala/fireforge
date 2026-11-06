"""pw.x 6.7 output parsers and the runner, on canned outputs captured from real pw.x 6.7
runs (data/qe/*.out, *.xml; regenerate with data/qe/generate.py)."""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest

from fireforge_science.engine import HARTREE_EV, RY_EV, Provenance
from fireforge_science.model import Structure
from fireforge_science.quantumforge import parse, runner

DATA = Path(__file__).with_name("data") / "qe"


def si(a=5.43):
    h = a / 2
    return Structure([[0, h, h], [h, 0, h], [h, h, 0]], ["Si", "Si"],
                     [[0, 0, 0], [0.25, 0.25, 0.25]])


PROV = Provenance(solver="quantum-espresso", solver_version="6.7MaX", engine="qe")


def test_si_scf_xml_and_stdout_agree():
    x = parse.parse_xml(DATA / "si_scf.xml")
    o = parse.parse_stdout((DATA / "si_scf.out").read_text())
    assert x.version == o.version == "6.7MaX"
    assert x.calculation == "scf" and x.scf_converged and x.n_scf_steps > 0
    assert x.opt_converged is None and not x.lsda and x.magnetization_mub == 0.0
    assert x.species == ["Si", "Si"] and x.nelec == 8.0
    np.testing.assert_allclose(x.lattice, si().lattice_array, atol=1e-6)
    np.testing.assert_allclose(x.frac, si().frac_array, atol=1e-8)
    assert x.energy_ev == pytest.approx(o.energy_ev, abs=1e-5)
    assert x.energy_ev == pytest.approx(-22.83599696 * RY_EV, abs=1e-5)
    assert x.fermi_ev == pytest.approx(o.fermi_ev, abs=1e-3)
    np.testing.assert_allclose(x.stress_kbar, o.stress_kbar, atol=0.01)
    assert np.trace(x.stress_kbar) / 3 == pytest.approx(o.pressure_kbar, abs=0.01)
    np.testing.assert_allclose(x.forces_ev_a, o.forces_ev_a, atol=1e-6)
    assert x.forces_ev_a.shape == (2, 3)
    assert x.weights.sum() == pytest.approx(1.0) and len(x.kpoints_frac) == x.eigenvalues_ha.shape[0]
    assert x.pseudo_files == {"Si": "Si.pbe-n-rrkjus_psl.1.0.0.UPF"}
    assert o.job_done and o.scf_converged and o.bfgs_converged is None


def test_si_nscf_band_data():
    x = parse.parse_xml(DATA / "si_nscf.xml")
    assert x.calculation == "nscf"
    bd = parse.band_data(x, si(), PROV)
    # irreducible k-points of the Γ-centred dense mesh, Γ first
    assert len(bd.kpoints) < 9 ** 3 and np.allclose(bd.kpoints[0], 0)
    assert bd.ebands.shape == (x.nbnd_up, len(bd.kpoints)) and bd.dosweight == 2.0
    np.testing.assert_allclose(bd.lattvec.T * 0.529177210903, si().lattice_array, atol=1e-6)
    assert bd.get_volume() == pytest.approx(si().volume() / 0.529177210903 ** 3, rel=1e-6)
    gap, direct, vbm, cbm = bd.band_gap()
    assert 0.3 < gap < 0.9 and not direct           # Si: indirect PBE gap
    assert vbm == pytest.approx(bd.ebands[3, 0] * HARTREE_EV, abs=1e-6)   # VBM at Γ
    assert bd.ebands.max() * HARTREE_EV > cbm + 3   # conduction bands for transport
    # k-point fractional conversion: every k is on the mesh (n/N)
    n = np.array([int(s) for s in
                  (DATA / "si_nscf.in").read_text().split("K_POINTS automatic\n")[1].split()[:3]])
    kn = bd.kpoints * n
    np.testing.assert_allclose(kn, np.round(kn), atol=1e-6)


def test_si_vcrelax():
    x = parse.parse_xml(DATA / "si_vcrelax.xml")
    o = parse.parse_stdout((DATA / "si_vcrelax.out").read_text())
    assert x.calculation == "vc-relax" and x.opt_converged and x.n_opt_steps > 0
    assert o.bfgs_converged and len(o.energies_ev) > 2
    s = x.structure(like=si(5.30))
    # relaxed from a = 5.30 (compressed) to a larger cell, Si stays diamond
    assert s.volume() > si(5.30).volume() and s.spacegroup()[0] == 227
    np.testing.assert_allclose(o.final_lattice, x.lattice, atol=1e-6)
    np.testing.assert_allclose(o.final_frac, x.frac, atol=1e-6)
    assert abs(np.trace(x.stress_kbar) / 3) < 1.0
    assert x.energy_ev == pytest.approx(o.energy_ev, abs=1e-5)


def test_si_lsda():
    x = parse.parse_xml(DATA / "si_lsda_scf.xml")
    assert x.lsda and x.nbnd_up == x.nbnd_dw == 8
    assert x.eigenvalues_ha.shape[1] == 16 and abs(x.magnetization_mub) < 1e-3
    bd = parse.band_data(x, si(), PROV)
    assert bd.dosweight == 1.0 and bd.ebands.shape[0] == 16
    up, dw = bd.ebands[:8], bd.ebands[8:]
    np.testing.assert_allclose(up, dw, atol=1e-4)    # non-magnetic: identical channels
    o = parse.parse_stdout((DATA / "si_lsda_scf.out").read_text())
    assert o.magnetization_mub == pytest.approx(0.0, abs=1e-2)


def test_zrnisn_scf():
    if not (DATA / "zrnisn_scf.xml").is_file():
        pytest.skip("zrnisn canned output not generated")
    x = parse.parse_xml(DATA / "zrnisn_scf.xml")
    o = parse.parse_stdout((DATA / "zrnisn_scf.out").read_text())
    assert x.species == ["Zr", "Ni", "Sn"] and x.lsda and x.scf_converged
    assert abs(x.magnetization_mub) < 0.05                # Ni is non-magnetic in ZrNiSn
    assert x.energy_ev == pytest.approx(o.energy_ev, abs=1e-4)
    np.testing.assert_allclose(x.forces_ev_a, 0, atol=1e-4)
    assert "Running on" not in o.version and "2 processors" in (DATA / "zrnisn_scf.out").read_text().replace("     ", " ")


def test_parse_errors(tmp_path):
    with pytest.raises(parse.ParseError):
        parse.parse_xml(tmp_path / "missing.xml")
    p = tmp_path / "x.xml"
    p.write_text("<qes:espresso xmlns:qes='http://www.quantum-espresso.org/ns/qes/qes-1.0'>"
                 "<input/></qes:espresso>")
    with pytest.raises(parse.ParseError, match="output"):
        parse.parse_xml(p)
    o = parse.parse_stdout("garbage")
    assert o.energy_ev is None and not o.job_done and o.version == "unknown"


# ---- runner ------------------------------------------------------------------------------

def fake_pw(tmp_path, body: str) -> Path:
    exe = tmp_path / "fake-pw.x"
    exe.write_text("#!/bin/sh\n" + body)
    exe.chmod(0o755)
    return exe


def test_runner_command(monkeypatch, tmp_path):
    monkeypatch.setenv("FIREFORGE_PW_X", "/opt/qe/pw.x")
    monkeypatch.setenv("FIREFORGE_SCI_MPIRUN", "mpirun --oversubscribe")
    assert runner.command(1) == ["/opt/qe/pw.x", "-nk", "1", "-in", "pw.in"]
    assert runner.command(2) == ["mpirun", "--oversubscribe", "-np", "2", "--bind-to", "none",
                                 "/opt/qe/pw.x", "-nk", "1", "-in", "pw.in"]
    monkeypatch.setenv("FIREFORGE_PW_PRELOAD", "none")
    env = runner.pw_env()
    assert env["OMP_NUM_THREADS"] == "1" and "DISPLAY" not in env


def test_runner_success_and_failures(monkeypatch, tmp_path):
    monkeypatch.setenv("FIREFORGE_PW_PRELOAD", "none")
    ok = (DATA / "si_scf.out")
    monkeypatch.setenv("FIREFORGE_PW_X", str(fake_pw(tmp_path, f"cat {ok}\n")))
    text = runner.run_pw("&CONTROL\n/\n", tmp_path / "a", 1)
    assert "JOB DONE" in text and (tmp_path / "a" / "pw.in").read_text() == "&CONTROL\n/\n"
    assert runner.version(text) == "6.7MaX"

    monkeypatch.setenv("FIREFORGE_PW_X", str(fake_pw(
        tmp_path, "echo '     Program PWSCF v.6.7MaX starts'\necho line2\n"
                  "echo '     convergence NOT achieved after 200 iterations: stopping'\nexit 0\n")))
    with pytest.raises(runner.PwError, match="convergence NOT achieved") as ei:
        runner.run_pw("x", tmp_path / "b", 1)
    assert not ei.value.retryable and "line2" in str(ei.value)

    monkeypatch.setenv("FIREFORGE_PW_X", str(fake_pw(tmp_path, "echo partial\nexit 3\n")))
    with pytest.raises(runner.PwError, match="status 3") as ei:
        runner.run_pw("x", tmp_path / "c", 1)
    assert ei.value.retryable and "partial" in ei.value.log_tail

    monkeypatch.setenv("FIREFORGE_PW_X", str(fake_pw(tmp_path, "echo started\n")))
    with pytest.raises(runner.PwError, match="without JOB DONE"):
        runner.run_pw("x", tmp_path / "d", 1)

    err = (" %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n"
           "     Error in routine readpp (2):\n     file not found\n"
           " %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n")
    (tmp_path / "err.txt").write_text(err)
    monkeypatch.setenv("FIREFORGE_PW_X", str(fake_pw(tmp_path, f"cat {tmp_path}/err.txt\nexit 1\n")))
    with pytest.raises(runner.PwError, match="readpp .2.: file not found") as ei:
        runner.run_pw("x", tmp_path / "e", 1)
    assert not ei.value.retryable


def test_runner_timeout_and_cancel(monkeypatch, tmp_path):
    monkeypatch.setenv("FIREFORGE_PW_PRELOAD", "none")
    monkeypatch.setenv("FIREFORGE_PW_X", str(fake_pw(tmp_path, "echo begin\nsleep 30\n")))
    with pytest.raises(runner.PwError, match="timeout") as ei:
        runner.run_pw("x", tmp_path / "t", 1, timeout=0.3, poll_s=0.05)
    assert ei.value.retryable
    with pytest.raises(runner.PwError, match="cancelled") as ei:
        runner.run_pw("x", tmp_path / "c", 1, cancelled=lambda: True, poll_s=0.05)
    assert not ei.value.retryable


def test_runner_missing_pw(monkeypatch):
    monkeypatch.setenv("FIREFORGE_PW_X", "")
    monkeypatch.setenv("PATH", "/nonexistent")
    with pytest.raises(runner.PwError, match="pw.x not found"):
        runner.command(1)


def test_preload_shim_builds(monkeypatch, tmp_path):
    import shutil
    if not (shutil.which("cc") or shutil.which("gcc")):
        pytest.skip("no C compiler")
    monkeypatch.delenv("FIREFORGE_PW_PRELOAD", raising=False)
    monkeypatch.setattr(runner, "SHIM_DIR", tmp_path / "shim")
    so = runner.preload_shim()
    assert so and Path(so).is_file() and runner.preload_shim() == so
    assert so in runner.pw_env()["LD_PRELOAD"]
    monkeypatch.setenv("FIREFORGE_PW_PRELOAD", "/x/y.so")
    assert runner.preload_shim() == "/x/y.so"
