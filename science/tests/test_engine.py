"""BandData persistence and band-gap analysis; get_engine (phase8.md §7)."""
from __future__ import annotations

import importlib.util

import numpy as np
import pytest

from fireforge_science.engine import HARTREE_EV, BandData, Provenance, get_engine

from conftest import halfheusler

H = HARTREE_EV


def make_bands(ebands_ev, nelect, dosweight=2.0, fermi_ev=0.0, nk=None) -> BandData:
    eb = np.asarray(ebands_ev, float)
    nk = eb.shape[1]
    k = np.column_stack([np.linspace(0, 0.5, nk), np.zeros(nk), np.zeros(nk)])
    s = halfheusler()
    return BandData(kpoints=k, ebands=eb / H, lattvec=s.lattice_array.T / 0.529177210903,
                    nelect=nelect, fermi=fermi_ev / H, dosweight=dosweight, structure=s,
                    provenance=Provenance(solver="toy", solver_version="t", engine="toy",
                                          settings={"kmesh": [4, 4, 4]}))


def test_save_load_round_trip(tmp_path):
    bd = make_bands([[-2, -1.5, -1], [-0.5, -0.3, 0.0], [1.0, 0.8, 1.2]], 4, fermi_ev=0.4)
    p = tmp_path / "bands"                    # no .npz appended
    bd.save(p)
    assert p.is_file()
    back = BandData.load(p)
    assert np.array_equal(back.kpoints, bd.kpoints)
    assert np.array_equal(back.ebands, bd.ebands)
    assert np.array_equal(back.lattvec, bd.lattvec)
    assert (back.nelect, back.fermi, back.dosweight) == (4.0, bd.fermi, 2.0)
    assert back.structure == bd.structure
    assert back.provenance == bd.provenance
    with np.load(p, allow_pickle=False) as z:          # no pickled objects inside
        assert set(z.files) >= {"kpoints", "ebands", "lattvec", "structure", "provenance"}


def test_gap_indirect():
    bd = make_bands([[-3, -3, -3], [-0.2, 0.0, -0.4], [1.0, 0.9, 0.7]], 4, fermi_ev=0.3)
    gap, direct, vbm, cbm = bd.band_gap()
    assert gap == pytest.approx(0.7) and not direct
    assert (vbm, cbm) == (pytest.approx(0.0), pytest.approx(0.7))


def test_gap_direct():
    bd = make_bands([[-3, -3, -3], [-0.2, 0.0, -0.4], [1.0, 0.5, 0.9]], 4)
    gap, direct, vbm, cbm = bd.band_gap()
    assert gap == pytest.approx(0.5) and direct


def test_metal_overlap_and_odd_count():
    # bands overlap in energy across k: metal although counts are integer
    bd = make_bands([[-3, -3, -3], [-0.2, 0.5, -0.4], [1.0, 0.6, 0.3]], 4, fermi_ev=0.4)
    assert bd.band_gap() == (0.0, False, pytest.approx(0.4), pytest.approx(0.4))
    # odd electron count with dosweight 2: half-filled band
    bd = make_bands([[-3, -3, -3], [-0.2, 0.0, -0.4], [1.0, 0.7, 0.9]], 3)
    assert bd.band_gap()[0] == 0.0
    # fractional / too many electrons
    assert make_bands([[-1, -1], [1, 1]], 2.5).band_gap()[0] == 0.0
    assert make_bands([[-1, -1], [1, 1]], 4).band_gap()[0] == 0.0


def test_gap_spin_polarized_stacked():
    """dosweight 1: both spin channels stacked; 3 electrons fill the 3 lowest states/k."""
    up = [[-3, -3], [-0.1, 0.0]]
    dn = [[-2.5, -2.5], [0.6, 0.8]]
    bd = make_bands(up + dn, 3, dosweight=1.0)       # unsorted along the band axis
    gap, direct, vbm, cbm = bd.band_gap()
    assert vbm == pytest.approx(0.0) and cbm == pytest.approx(0.6)
    assert gap == pytest.approx(0.6) and not direct
    # same bands as non-spin-polarized pairs would need nelect 6 for that filling
    assert make_bands(up + dn, 4, dosweight=1.0).band_gap()[0] == pytest.approx(0.0)


def test_boltztrap_interface():
    bd = make_bands([[-3, -3, -3], [-0.2, 0.0, -0.4], [1.0, 0.5, 0.9]], 4)
    assert bd.atoms.get_chemical_formula() == "NiSnZr"
    assert bd.mommat is None and bd.magmom is None
    assert bd.get_volume() == pytest.approx(halfheusler().volume() / 0.529177210903 ** 3)
    c = bd.boltztrap_data()
    accepted = c.bandana(emin=-1.0 / H, emax=np.inf)
    assert list(accepted) == [False, True, True]
    assert c.nelect == 2.0 and c.ebands.shape[0] == 2
    assert bd.nelect == 4.0 and bd.ebands.shape[0] == 3          # original untouched


def test_shape_check():
    with pytest.raises(ValueError):
        BandData(kpoints=np.zeros((3, 3)), ebands=np.zeros((2, 4)), lattvec=np.eye(3),
                 nelect=2, fermi=0, dosweight=2, structure=halfheusler(),
                 provenance=Provenance("toy", "t", "toy"))


def test_provenance_method():
    p = Provenance("toy", "toy-1", "toy", pseudopotentials=["a md5:1"],
                   settings={"preset": "screening", "kmesh": [8, 8, 8]})
    m = p.method()
    assert m["preset"] == "screening" and m["engine"] == "toy"
    assert '"kmesh": [8, 8, 8]' in m["settings_json"]
    assert Provenance.from_json(p.to_json()) == p


def test_get_engine():
    toy = get_engine("toy")
    assert toy.name == "toy"
    with pytest.raises(ValueError):
        get_engine("vasp")
    if importlib.util.find_spec("fireforge_science.quantumforge") is None:
        with pytest.raises(ImportError):
            get_engine("qe")
