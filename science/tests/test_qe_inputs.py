"""pw.x input writer (quantumforge.inputs): golden files and formatting."""
from __future__ import annotations

import os
from pathlib import Path

import pytest

from fireforge_science.model import Structure
from fireforge_science.quantumforge import inputs
from fireforge_science.settings import Settings

from conftest import halfheusler

DATA = Path(__file__).with_name("data") / "qe"
PSEUDOS = {"Zr": "Zr_pbe_v1.uspp.F.UPF", "Ni": "ni_pbe_v1.4.uspp.F.UPF",
           "Sn": "Sn_pbe_v1.uspp.F.UPF", "Si": "Si.pbe-n-rrkjus_psl.1.0.0.UPF",
           "Fe": "Fe.pbe-spn-kjpaw_psl.0.2.1.UPF", "Nb": "Nb.pbe-spn-kjpaw_psl.0.3.0.UPF",
           "Sb": "sb_pbe_v1.4.uspp.F.UPF"}


def si(a=5.43):
    h = a / 2
    return Structure([[0, h, h], [h, 0, h], [h, h, 0]], ["Si", "Si"],
                     [[0, 0, 0], [0.25, 0.25, 0.25]])


def golden(name: str, text: str) -> None:
    path = DATA / f"golden_{name}.in"
    if os.environ.get("FF_UPDATE_GOLDEN"):
        path.write_text(text)
    assert text == path.read_text(), f"{path} differs (FF_UPDATE_GOLDEN=1 to regenerate)"


def zrnisn_settings(**kw):
    return Settings(ecutwfc_ry=60.0, ecutrho_ry=480.0, **kw)


def test_golden_zrnisn_scf():
    """spin auto + Ni → nspin 2 with starting_magnetization on Ni only; 8x8x8 mesh."""
    text = inputs.pw_input(halfheusler(), zrnisn_settings(), "scf", pseudo_dir="/pseudo",
                           pseudos=PSEUDOS)
    golden("zrnisn_scf", text)
    assert "nspin = 2" in text and "starting_magnetization(2) = 0.5" in text
    assert "starting_magnetization(1)" not in text
    assert "K_POINTS automatic\n  8 8 8 0 0 0" in text


def test_golden_si_vcrelax_and_relax():
    st = Settings(ecutwfc_ry=30.0, ecutrho_ry=240.0, spin="off")
    text = inputs.pw_input(si(), st, "vc-relax", pseudo_dir="/pseudo", pseudos=PSEUDOS)
    golden("si_vcrelax", text)
    assert "&CELL" in text and "cell_dofree = 'all'" in text and "press_conv_thr = 0.5" in text
    assert "forc_conv_thr = 1d-4" in text and "etot_conv_thr = 2d-5" in text
    relax = inputs.pw_input(si(), st, "relax", pseudo_dir="/pseudo", pseudos=PSEUDOS)
    assert "&IONS" in relax and "&CELL" not in relax and "nspin" not in relax


def test_golden_si_nscf_and_explicit_kpoints():
    st = Settings(ecutwfc_ry=30.0, ecutrho_ry=240.0, spin="off")
    text = inputs.pw_input(si(), st, "nscf", pseudo_dir="/pseudo", pseudos=PSEUDOS, nbnd=16)
    golden("si_nscf", text)
    assert "nbnd = 16" in text and "nosym = .false." in text and "tstress" not in text
    assert f"K_POINTS automatic\n  {' '.join(map(str, st.kmesh(si().lattice, dense=True)))} 0 0 0" in text
    kl = inputs.pw_input(si(), st, "nscf", pseudo_dir="/p", pseudos=PSEUDOS,
                         kpoints=[[0, 0, 0], [0.5, 0, 0]])
    assert "K_POINTS crystal\n  2\n" in kl and kl.rstrip().endswith("0.5000000000")


def test_extra_overrides():
    st = Settings(ecutwfc_ry=30.0, ecutrho_ry=240.0, spin="off",
                  extra={"kmesh": [3, 4, 5], "mixing_beta": 0.2,
                         "pw": {"system": {"ecutfock": 100.0}, "electrons": {"mixing_mode": "local-TF"}}})
    text = inputs.pw_input(si(), st, "scf", pseudo_dir="/p", pseudos=PSEUDOS)
    assert "3 4 5 0 0 0" in text and "mixing_beta = 0.2" in text
    assert "ecutfock = 100.0" in text and "mixing_mode = 'local-TF'" in text


def test_spin_and_magmoms():
    st = Settings(ecutwfc_ry=30.0, ecutrho_ry=240.0)
    assert inputs.starting_magnetization(si(), st) == {}
    assert inputs.starting_magnetization(si(), Settings(spin="on")) == {"Si": 0.2}
    nbfesb = halfheusler("Nb", "Fe", "Sb", 5.95)
    assert inputs.starting_magnetization(nbfesb, st) == {"Nb": 0.0, "Fe": 0.5, "Sb": 0.0}
    m = nbfesb.copy(magmoms=[0.0, 2.5, 0.0])
    assert inputs.starting_magnetization(m, st)["Fe"] == pytest.approx(0.5)
    assert inputs.starting_magnetization(nbfesb, Settings(spin="off")) == {}


def test_fortran_format_and_errors():
    assert inputs.fortran(True) == ".true." and inputs.fortran(3) == "3"
    assert inputs.fortran(3e-9) == "3d-9" and inputs.fortran(0.01) == "0.01"
    assert inputs.fortran(1.5e-10) == "1.5d-10" and inputs.fortran("mv") == "'mv'"
    with pytest.raises(ValueError, match="calculation"):
        inputs.pw_input(si(), Settings(ecutwfc_ry=1, ecutrho_ry=4), "bands",
                        pseudo_dir="/p", pseudos=PSEUDOS)
    with pytest.raises(ValueError, match="not resolved"):
        inputs.pw_input(si(), Settings(), "scf", pseudo_dir="/p", pseudos=PSEUDOS)
    with pytest.raises(ValueError, match="Si"):
        inputs.pw_input(si(), Settings(ecutwfc_ry=1, ecutrho_ry=4), "scf", pseudo_dir="/p",
                        pseudos={})
    assert inputs.default_nbnd(18, False) == 17 and inputs.default_nbnd(8, False) == 12
