"""ToyEngine: consistency of energy/forces/stress, relaxation, phonons, bands (§7)."""
from __future__ import annotations

import time
import warnings

import numpy as np
import pytest

from fireforge_science.engine import EV_A3_KBAR, BandData
from fireforge_science.model import Structure
from fireforge_science.settings import Settings, kmesh
from fireforge_science.toy import energy_forces_virial, toy_gap_ev

from conftest import halfheusler


def rattled(s: Structure, sigma=0.03, strain=0.01, seed=0) -> Structure:
    rng = np.random.default_rng(seed)
    lat = s.lattice_array @ (np.eye(3) + rng.normal(0, strain, (3, 3)))
    return s.copy(lattice=lat.tolist(),
                  frac=(s.frac_array + rng.normal(0, sigma, s.frac_array.shape)).tolist())


def test_forces_are_minus_energy_gradient(toy, screening):
    s = rattled(halfheusler().supercell([2, 1, 1]), sigma=0.02)
    res = toy.forces(s, screening)
    f = np.array(res.forces)
    cart = s.cart
    h = 1e-5
    for i, a in [(0, 0), (1, 2), (3, 1), (5, 0)]:
        e = []
        for d in (h, -h):
            c = cart.copy()
            c[i, a] += d
            e.append(toy.energy(s.copy(frac=(c @ np.linalg.inv(s.lattice_array)).tolist())))
        assert f[i, a] == pytest.approx(-(e[0] - e[1]) / (2 * h), abs=1e-6)
    assert np.abs(f.sum(axis=0)).max() < 1e-9                   # Newton's third law


def test_stress_is_strain_derivative(toy, screening):
    s = rattled(halfheusler(a=5.9), sigma=0.01, strain=0.02)
    sig = np.array(toy.stress(s, screening))
    assert np.allclose(sig, sig.T, atol=1e-8)
    h = 1e-6
    for a, b in [(0, 0), (1, 2), (2, 2), (0, 1)]:
        e = []
        for d in (h, -h):
            eps = np.zeros((3, 3))
            eps[a, b] = eps[b, a] = d / (2 if a != b else 1)
            e.append(toy.energy(s.copy(lattice=(s.lattice_array @ (np.eye(3) + eps)).tolist())))
        dEde = (e[0] - e[1]) / (2 * h)        # = W_ab (symmetric strain ε_ab = ε_ba = h/2)
        # QE sign: σ = −(1/V) dE/dε
        assert sig[a, b] == pytest.approx(-dEde / s.volume() * EV_A3_KBAR, rel=1e-5, abs=1e-4)
    # compressed cell -> positive pressure (wants to expand)
    p = np.trace(toy.stress(halfheusler(a=5.5), screening)) / 3
    assert p > 0
    assert np.trace(toy.stress(halfheusler(a=6.8), screening)) / 3 < 0


def test_relax_converges(toy, screening):
    start = halfheusler(a=6.4, source="test")
    r = toy.relax(start, screening)
    assert r.converged and r.steps > 0
    assert abs(r.pressure_kbar) < screening.press_conv_thr_kbar
    s = r.structure
    assert s.spacegroup()[0] == 216 and s.source == "test" and s.parent_structure_id == ""
    res = toy.scf(s, screening)
    assert np.abs(res.forces).max() < 1e-3
    assert np.abs(res.stress_kbar).max() < screening.press_conv_thr_kbar
    assert res.energy_ev == pytest.approx(r.energy_ev)
    # a minimum: compressing or expanding raises the energy
    e0 = toy.energy(s)
    assert toy.energy(s.scaled(0.01)) > e0 and toy.energy(s.scaled(-0.01)) > e0
    a = np.linalg.norm(s.lattice_array[0]) * np.sqrt(2)
    assert 5.5 < a < 6.5                                         # half-Heusler-like
    assert r.provenance.engine == "toy" and r.provenance.solver == "toy"
    assert r.magnetization_mub == 0.0 and "final energy" in r.log
    # deterministic
    assert toy.relax(start, screening).energy_ev == r.energy_ev


def test_relax_low_symmetry_and_fix_cell(toy, screening):
    s = rattled(halfheusler().supercell([2, 1, 1]), sigma=0.01, strain=0.01, seed=4)
    r = toy.relax(s, screening)
    assert r.converged
    assert r.structure.primitive().nsites == 3 and r.structure.spacegroup()[0] == 216
    fixed = toy.relax(s, screening, fix_cell=True)
    assert fixed.converged
    assert np.allclose(fixed.structure.lattice, s.lattice)
    assert np.abs(toy.forces(fixed.structure, screening).forces).max() < 3e-3


def test_known_ordering_lowest(toy, screening):
    e = {o: toy.relax(halfheusler(*o), screening).energy_ev
         for o in [("Zr", "Ni", "Sn"), ("Ni", "Zr", "Sn"), ("Zr", "Sn", "Ni")]}
    assert min(e, key=e.get) == ("Zr", "Ni", "Sn")


def test_forces_speed(toy, screening):
    s = rattled(halfheusler().supercell([2, 2, 2]))
    assert s.nsites == 24
    toy.forces(s, screening)
    t = time.perf_counter()
    toy.forces(s, screening)
    assert time.perf_counter() - t < 0.2


def _phonopy(structure, toy, settings, supercell):
    from phonopy import Phonopy
    from phonopy.structure.atoms import PhonopyAtoms
    pa = PhonopyAtoms(symbols=structure.species, cell=structure.lattice_array,
                      scaled_positions=structure.frac_array)
    ph = Phonopy(pa, supercell_matrix=supercell, primitive_matrix="P")
    ph.generate_displacements(distance=0.01)
    forces = [toy.forces(Structure(c.cell, c.symbols, c.scaled_positions), settings).forces
              for c in ph.supercells_with_displacements]
    ph.forces = np.array(forces)
    ph.produce_force_constants()
    return ph


def test_phonons_stable_half_heusler(toy, screening, relaxed_zrnisn):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        ph = _phonopy(relaxed_zrnisn, toy, screening, np.diag([3, 3, 3]))
        ph.run_mesh([8, 8, 8], is_gamma_center=True)
        freqs = ph.mesh.frequencies
        assert freqs.min() > -0.05                               # THz: no imaginary modes
        assert 3.0 < freqs.max() < 15.0                          # physically shaped
        ph.run_qpoints([[0, 0, 0]])
        gamma = np.sort(ph.qpoints.frequencies[0])
    assert np.abs(gamma[:3]).max() < 0.05 and gamma[3] > 1.0     # acoustic → 0, optical > 0


def test_phonons_wrong_ordering_unstable(toy, screening):
    s = toy.relax(halfheusler("Ni", "Zr", "Sn"), screening).structure
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        ph = _phonopy(s, toy, screening, np.diag([2, 2, 2]))
        ph.run_mesh([4, 4, 4], is_gamma_center=True)
        assert ph.mesh.frequencies.min() < -0.5


def test_nscf_band_data(toy, relaxed_zrnisn):
    st = Settings(kdensity_dense=0.2)
    bd = toy.nscf(relaxed_zrnisn, None, st)
    assert isinstance(bd, BandData)
    mesh = kmesh(relaxed_zrnisn.lattice, 0.2)
    assert len(bd.kpoints) < np.prod(mesh)                        # irreducible
    assert np.allclose(np.round(bd.kpoints * mesh), bd.kpoints * mesh)  # on the mesh
    assert bd.ebands.shape == (4, len(bd.kpoints)) and bd.nelect == 4 and bd.dosweight == 2
    gap, direct, vbm, cbm = bd.band_gap()
    assert gap == pytest.approx(toy_gap_ev(relaxed_zrnisn)) and direct
    assert vbm < bd.fermi * 27.211386245988 < cbm
    scf = toy.scf(relaxed_zrnisn, st)
    assert scf.fermi_ev == pytest.approx(bd.fermi * 27.211386245988)
    assert scf.save_dir is None


def test_boltztrap2_on_toy_bands(toy, relaxed_zrnisn):
    from BoltzTraP2 import bandlib as BL
    from BoltzTraP2 import fite, sphere
    bd = toy.nscf(relaxed_zrnisn, None, Settings(kdensity_dense=0.15))
    data = bd.boltztrap_data()
    eq = sphere.get_equivalences(data.atoms, data.magmom, 5 * len(data.kpoints))
    coeffs = fite.fitde3D(data, eq)
    fitted = fite.getBands(data.kpoints, eq, data.get_lattvec(), coeffs)[0]
    assert np.abs(fitted - data.ebands).max() * 27.2 < 1e-3            # eV
    eband, vvband, _ = fite.getBTPbands(eq, coeffs, data.get_lattvec())
    eps, dos, vvdos, _ = BL.BTPDOS(eband, vvband, npts=1500)
    gap, _, vbm, cbm = bd.band_gap()
    H = 27.211386245988
    mur = np.array([(vbm + 0.1) / H, (cbm - 0.1) / H])                 # p-type, n-type
    T = np.array([300.0])
    N, L0, L1, L2, _ = BL.fermiintegrals(eps, dos, vvdos, mur=mur, Tr=T,
                                         dosweight=data.dosweight)
    _, seebeck, _, _ = BL.calc_Onsager_coefficients(L0, L1, L2, mur, T, data.get_volume())
    s_p, s_n = seebeck[0, 0, 0, 0] * 1e6, seebeck[0, 1, 0, 0] * 1e6   # µV/K
    assert s_p > 100 and s_n < -100
    assert N[0, 0] + data.nelect > 0 > N[0, 1] + data.nelect           # holes vs electrons


def test_energy_function_matches_engine(toy, zrnisn):
    e, f, w = energy_forces_virial(zrnisn.lattice, zrnisn.species, zrnisn.frac)
    assert e == toy.energy(zrnisn)
    assert np.abs(f).max() < 1e-10                                     # symmetric sites
