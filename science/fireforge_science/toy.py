"""ToyEngine: fast, deterministic analytic stand-in for Quantum ESPRESSO (phase8.md §7).

For tests and the plumbing e2e only. Results carry provenance solver/engine "toy"; they
must never be mixed with real data (te.validate refuses them).

Energy model: smoothly truncated Morse pair potential plus a volume term,
    E = ½ Σ_{i≠j,T} S(r) · D_ij [(1 − e^{−α(r − r0_ij)})² − 1] + N·C_VOL·(v_ref/v)^P_VOL
with element-pair parameters from tabulated covalent radii (ASE) and Pauling
electronegativities (pymatgen):
    r0_ij = R0_SCALE (r_cov,i + r_cov,j),  D_ij = D0 (1 + D_IONIC |χ_i − χ_j|),  α = ALPHA,
a cosine taper S between r0 + R_ON and r0 + R_CUT, v = V/N and v_ref from the mean
covalent-sphere volume. Forces, stress (virial) and energy
are exact derivatives of each other (tests check this by finite differences). Stress uses
the QE sign: positive = compressive (cell wants to expand), kbar.

Band model (nscf): four bands on the real lattice, built from the cosine star of the
shortest lattice vectors, s(k) = Σ_R (1 − cos 2πk·R)/N (0 at Γ), which is invariant under
the lattice point group (so it interpolates exactly in BoltzTraP2):
    two valence bands   E = −W_V1·s(k),  E = −V2_OFFSET − W_V2·s(k)
    two conduction bands E = Eg + W_C1·s(k),  E = Eg + C2_OFFSET + W_C2·s(k)
Direct gap at Γ, Eg = GAP0 + GAP_IONIC·(χ_max − χ_min) eV, VBM at 0 eV, nelect 4,
non-spin-polarized (dosweight 2), Fermi level mid-gap. k-points: the irreducible part of
the Γ-centred mesh kmesh(lattice, settings.kdensity_dense) (spglib).
"""
from __future__ import annotations

import math
from functools import lru_cache
from pathlib import Path

import numpy as np

from .engine import (BOHR_ANGSTROM, EV_A3_KBAR, HARTREE_EV, RY_EV, BandData, Provenance,
                     RelaxResult, ScfResult)
from .model import Structure, _z, spglib_call
from .settings import Settings, kmesh

TOY_VERSION = "toy-1"

# pair potential (tuned so that half-Heuslers in their known orderings relax to
# a ≈ 5.8–6.1 Å and are dynamically stable; other orderings come out unstable)
R0_SCALE = 1.0
ALPHA = 1.2            # 1/Å
D0 = 0.4               # eV
D_IONIC = 1.0
R_ON = 1.875           # taper start, Å beyond r0
R_CUT = 3.125          # cutoff, Å beyond r0
# volume term (repulsive, depends on cell volume only): N·C_VOL·(v_ref/v)^P_VOL. It
# balances the pair attraction so that bonds sit in tension (positive transverse
# stiffness), which keeps the phonons of half-Heusler-like cells real.
C_VOL = 0.5            # eV/atom
P_VOL = 2.0
V_FAC = 1.953125       # v_ref = V_FAC · mean (4π/3) r_cov³

# band model (eV)
GAP0 = 0.3
GAP_IONIC = 0.4
W_V1, W_V2, V2_OFFSET = 1.2, 0.8, 0.15
W_C1, W_C2, C2_OFFSET = 1.5, 2.0, 0.4
NELECT = 4.0

BOHR_RY_TO_EV_A = RY_EV / BOHR_ANGSTROM       # 1 Ry/Bohr in eV/Å


@lru_cache(maxsize=None)
def _rcov(symbol: str) -> float:
    from ase.data import covalent_radii
    return float(covalent_radii[_z(symbol)])


@lru_cache(maxsize=None)
def _chi(symbol: str) -> float:
    from pymatgen.core import Element
    x = Element(symbol).X
    return 0.0 if x is None or math.isnan(x) else float(x)


def pair_params(a: str, b: str) -> tuple[float, float, float]:
    """(D eV, alpha 1/Å, r0 Å) for an element pair."""
    return (D0 * (1.0 + D_IONIC * abs(_chi(a) - _chi(b))), ALPHA,
            R0_SCALE * (_rcov(a) + _rcov(b)))


def _morse(r, d, alpha, r0):
    """Tapered Morse value and dV/dr (arrays)."""
    e = np.exp(-alpha * (r - r0))
    v = d * ((1.0 - e) ** 2 - 1.0)
    dv = 2.0 * d * alpha * e * (1.0 - e)
    r_on, r_cut = r0 + R_ON, r0 + R_CUT
    x = np.clip((r - r_on) / (r_cut - r_on), 0.0, 1.0)
    s = 0.5 * (1.0 + np.cos(np.pi * x))
    ds = np.where((x > 0) & (x < 1), -0.5 * np.pi * np.sin(np.pi * x) / (r_cut - r_on), 0.0)
    return v * s, dv * s + v * ds


def energy_forces_virial(lattice, species, frac) -> tuple[float, np.ndarray, np.ndarray]:
    """(E eV, forces n×3 eV/Å, W 3×3 eV) with W = dE/dε (symmetric strain derivative,
    W = −V·σ_QE). Pure function of the geometry."""
    lat = np.asarray(lattice, dtype=float)
    f = np.asarray(frac, dtype=float) % 1.0
    cart = f @ lat
    n = len(species)
    uniq = sorted(set(species))
    idx = np.array([uniq.index(s) for s in species])
    k = len(uniq)
    P = np.zeros((3, k, k))
    for a in range(k):
        for b in range(k):
            P[:, a, b] = pair_params(uniq[a], uniq[b])
    rcmax = float((P[2] + R_CUT).max())
    rec = np.linalg.inv(lat).T                          # rows b_k, no 2π
    nr = [int(math.ceil(rcmax * np.linalg.norm(b))) + 1 for b in rec]
    grid = np.array(np.meshgrid(*[np.arange(-m, m + 1) for m in nr], indexing="ij"))
    trans = grid.reshape(3, -1).T @ lat                 # (m,3)

    energy = 0.0
    forces = np.zeros((n, 3))
    virial = np.zeros((3, 3))
    for i in range(n):
        d = cart[None, :, :] + trans[:, None, :] - cart[i]      # (m, n, 3)
        r = np.linalg.norm(d, axis=2)
        pd, pa, pr = P[0][idx[i], idx], P[1][idx[i], idx], P[2][idx[i], idx]
        mask = (r > 1e-8) & (r < (pr + R_CUT)[None, :])
        if not mask.any():
            continue
        jj = np.nonzero(mask)[1]
        rr = r[mask]
        dd = d[mask]
        v, dv = _morse(rr, pd[jj], pa[jj], pr[jj])
        energy += 0.5 * float(v.sum())
        g = (dv / rr)[:, None] * dd                     # dV/dd
        forces[i] += g.sum(axis=0)
        virial += 0.5 * dd.T @ g
    # volume term: E_v = N C (v_ref/v)^P, dE_v/dε_ab = V dE_v/dV δ_ab = −P E_v δ_ab
    vref = V_FAC * float(np.mean([4.0 / 3.0 * math.pi * _rcov(sp) ** 3 for sp in species]))
    vol = abs(np.linalg.det(lat))
    ev = n * C_VOL * (vref / (vol / n)) ** P_VOL
    energy += ev
    virial -= P_VOL * ev * np.eye(3)
    return energy, forces, virial


def stress_kbar(lattice, virial) -> np.ndarray:
    """QE-sign stress (kbar) from the virial W = dE/dε."""
    vol = abs(np.linalg.det(np.asarray(lattice, dtype=float)))
    return -np.asarray(virial) / vol * EV_A3_KBAR


# ---------------------------------------------------------------------------------------
# band model

def _shortest_vectors(lat: np.ndarray) -> np.ndarray:
    """Integer coordinates of the shortest nonzero lattice vectors (one shell)."""
    rng = np.arange(-2, 3)
    n = np.array(np.meshgrid(rng, rng, rng, indexing="ij")).reshape(3, -1).T
    n = n[np.any(n != 0, axis=1)]
    lengths = np.linalg.norm(n @ lat, axis=1)
    return n[lengths < lengths.min() * (1 + 1e-6)]


def toy_gap_ev(s: Structure) -> float:
    chis = [_chi(e) for e in s.elements]
    return GAP0 + GAP_IONIC * (max(chis) - min(chis))


def toy_bands_ev(s: Structure, kpoints) -> np.ndarray:
    """(4, nk) band energies in eV at fractional `kpoints`."""
    lat = s.lattice_array
    vecs = _shortest_vectors(lat)
    k = np.asarray(kpoints, dtype=float).reshape(-1, 3)
    star = (1.0 - np.cos(2 * np.pi * k @ vecs.T)).mean(axis=1)      # 0 at Γ, ≤ 2
    eg = toy_gap_ev(s)
    return np.array([
        -V2_OFFSET - W_V2 * star,
        -W_V1 * star,
        eg + W_C1 * star,
        eg + C2_OFFSET + W_C2 * star,
    ])


def ir_kpoints(s: Structure, mesh) -> np.ndarray:
    """Irreducible k-points (fractional) of the Γ-centred mesh."""
    out = spglib_call("get_ir_reciprocal_mesh", np.asarray(mesh, dtype=int),
                      s.to_spglib_cell(), is_shift=[0, 0, 0])
    if out is None:
        raise RuntimeError("spglib could not reduce the k-point mesh")
    mapping, grid = out
    ir = np.unique(mapping)
    k = grid[ir] / np.asarray(mesh, dtype=float)
    return k - np.round(k)          # into (-0.5, 0.5]


# ---------------------------------------------------------------------------------------

class ToyEngine:
    """Engine protocol implementation (engine.Engine). `workdir` is accepted and ignored
    (nothing is written); every result is a pure function of (structure, settings)."""

    name = "toy"

    def provenance(self, s: Structure, settings: Settings, **used) -> Provenance:
        st = {"preset": settings.preset, "model": "morse+cosine-bands", **used}
        return Provenance(solver="toy", solver_version=TOY_VERSION, engine="toy",
                          functional="toy", pseudopotentials=[], settings=st)

    # ---- energies ---------------------------------------------------------------------
    def _scf(self, s: Structure, settings: Settings, what: str) -> ScfResult:
        e, f, w = energy_forces_virial(s.lattice_array, s.species, s.frac_array)
        sig = stress_kbar(s.lattice_array, w)
        fermi = 0.5 * toy_gap_ev(s)
        log = (f"toy {what}: {s.reduced_formula()} nsites={s.nsites}\n"
               f"!    total energy = {e:.10f} eV\n"
               f"     max force = {np.abs(f).max():.3e} eV/A\n"
               f"     pressure = {np.trace(sig) / 3:.4f} kbar\n")
        return ScfResult(energy_ev=e, fermi_ev=fermi, forces=f.tolist(),
                         stress_kbar=sig.tolist(), converged=True, save_dir=None, log=log,
                         provenance=self.provenance(s, settings,
                                                    kmesh=list(kmesh(s.lattice, settings.kdensity))))

    def scf(self, s: Structure, settings: Settings, *, workdir: Path | None = None) -> ScfResult:
        return self._scf(s, settings, "scf")

    def forces(self, s: Structure, settings: Settings, *, workdir: Path | None = None) -> ScfResult:
        return self._scf(s, settings, "forces")

    def stress(self, s: Structure, settings: Settings, *, workdir: Path | None = None) -> list[list[float]]:
        return self._scf(s, settings, "stress").stress_kbar

    def energy(self, s: Structure) -> float:
        return energy_forces_virial(s.lattice_array, s.species, s.frac_array)[0]

    # ---- relaxation -------------------------------------------------------------------
    def relax(self, s: Structure, settings: Settings, *, fix_cell: bool = False,
              workdir: Path | None = None) -> RelaxResult:
        """L-BFGS on fractional positions and (unless fix_cell) the deformation gradient,
        to |F| < forc_conv_thr and |σ| < press_conv_thr. Positions fixed by symmetry stay
        fixed (gradients vanish by symmetry)."""
        from scipy.optimize import minimize
        n = s.nsites
        f_tol = settings.forc_conv_thr_ry_bohr * BOHR_RY_TO_EV_A
        p_tol = settings.press_conv_thr_kbar
        lat0 = s.lattice_array.copy()
        frac = s.frac_array.copy()
        steps = 0
        lines = [f"toy relax: {s.reduced_formula()} nsites={n} fix_cell={fix_cell}"]

        def unpack(x, lat_ref):
            fr = x[:3 * n].reshape(n, 3)
            if fix_cell:
                return fr, lat_ref, np.eye(3)
            F = x[3 * n:].reshape(3, 3)
            return fr, lat_ref @ F, F

        converged = False
        for cycle in range(8):
            lat_ref = lat0
            x0 = np.concatenate([frac.ravel()] + ([] if fix_cell else [np.eye(3).ravel()]))

            def fun(x):
                fr, lat, F = unpack(x, lat_ref)
                e, forces, w = energy_forces_virial(lat, s.species, fr)
                g_f = -(forces @ lat.T)                       # dE/dfrac
                grad = [g_f.ravel()]
                if not fix_cell:
                    grad.append((np.linalg.inv(F).T @ w).ravel())   # dE/dF
                return e, np.concatenate(grad)

            res = minimize(fun, x0, jac=True, method="L-BFGS-B",
                           options={"maxiter": 2000, "ftol": 1e-15, "gtol": 1e-9})
            steps += int(res.nit)
            frac, lat0, _ = unpack(res.x, lat_ref)
            e, forces, w = energy_forces_virial(lat0, s.species, frac)
            sig = stress_kbar(lat0, w)
            fmax = float(np.abs(forces).max())
            smax = 0.0 if fix_cell else float(np.abs(sig).max())
            lines.append(f"cycle {cycle}: E = {e:.10f} eV  nit={res.nit}  "
                         f"fmax={fmax:.3e} eV/A  |sigma|max={smax:.3e} kbar")
            if fmax < f_tol and smax < p_tol:
                converged = True
                break
        e, forces, w = energy_forces_virial(lat0, s.species, frac)
        sig = stress_kbar(lat0, w)
        out = s.copy(lattice=lat0.tolist(), frac=(frac % 1.0).tolist(), parent_structure_id="")
        lines.append(f"!    final energy = {e:.10f} eV  converged={converged}")
        return RelaxResult(structure=out, energy_ev=e, pressure_kbar=float(np.trace(sig) / 3),
                           magnetization_mub=0.0, converged=converged, steps=steps,
                           log="\n".join(lines) + "\n",
                           provenance=self.provenance(s, settings, fix_cell=fix_cell,
                                                      kmesh=list(kmesh(lat0, settings.kdensity))))

    # ---- bands ------------------------------------------------------------------------
    def nscf(self, s: Structure, save_dir: Path | None, settings: Settings, *,
             workdir: Path | None = None) -> BandData:
        """Band model on the irreducible k-points of kmesh(lattice, kdensity_dense)."""
        mesh = kmesh(s.lattice, settings.kdensity_dense)
        k = ir_kpoints(s, mesh)
        eb = toy_bands_ev(s, k) / HARTREE_EV
        fermi = 0.5 * toy_gap_ev(s) / HARTREE_EV
        return BandData(kpoints=k, ebands=eb, lattvec=s.lattice_array.T / BOHR_ANGSTROM,
                        nelect=NELECT, fermi=fermi, dosweight=2.0, structure=s.copy(),
                        provenance=self.provenance(s, settings, kmesh_dense=list(mesh),
                                                   nbands=4, nk_irreducible=len(k)))
