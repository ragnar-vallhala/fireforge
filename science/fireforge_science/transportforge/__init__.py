"""TransportForge: BoltzTraP2 interpolation and CRTA transport (phase8.md §4
transport.boltztrap).

`transport.boltztrap` — inputs `bands` (BandData npz, qe.nscf), optional `structure`
(used for the record; default the band data's structure). Params: `t_k` (300…1200 step
50), `doping_cm3` (carrier concentrations, magnitudes; default 1e18…1e21, 4 per decade;
each is evaluated as p (holes) and n (electrons)), `lpfac` (5; BoltzTraP2 k-point
multiplier over the irreducible input k-points), `npts` (DOS bins, 4000),
`erange_ev` (1.5: bands within [VBM − erange, CBM + erange] are interpolated), `tau_s`
(1e-14, only for the record's absolute values), `cpus` (BoltzTraP2 workers, capped at 2).

Method: BoltzTraP2 smooth-Fourier interpolation (fite.fitde3D) → band energies and
velocities on the dense grid → transport DOS (BTPDOS) → for each T and doping, the
chemical potential from the electron count N = N_valence − doping·V (bandlib.solve_for_mu,
as `btp2 dope`), then the Fermi integrals and Onsager coefficients at that μ:
σ/τ, S, κ_e/τ = κ_0/τ − T·(σ/τ)·S² (zero-current electronic thermal conductivity;
BoltzTraP2's `kappa`), κ_0 = L22. Tensors are reduced to their isotropic part (trace/3,
exact for cubic cells). Bipolar conduction is included (both bands are in the DOS).

`transport` JSON (format "fireforge.transport.v1"):

    t_k [nT], doping_cm3 [nd] (magnitudes), crta: true, tau_s (assumed, record only),
    gap_ev, gap_direct, vbm_ev, cbm_ev, nelect, lpfac, n_kpoints_input,
    n_equivalences, npts, engine, formula, fingerprint, units {…},
    p, n: {                                  each [nT][nd], null where unreachable
      mu_ev                    chemical potential relative to the VBM (eV)
      carriers_cm3             achieved net carriers (holes > 0 for p, electrons > 0 for n)
      seebeck_uv_k             µV/K (p > 0, n < 0)
      sigma_over_tau_s_m_s     σ/τ, S/(m·s)
      kappa_e_over_tau_w_mks   κ_e/τ, W/(m·K·s)
      kappa0_over_tau_w_mks    κ_0/τ, W/(m·K·s)
      pf_over_tau_w_mk2s       S²σ/τ, W/(m·K²·s)
    }

Multiply the /τ quantities by τ (s) for absolute values; CRTA results are not
absolute conductivities (Description.md §15.4).

Record `transport.crta` (uncertainty {"crta": true, "tau_s": τ}).
"""
from __future__ import annotations

import json
import logging
import math
from typing import Any

import numpy as np

from fireforge import WorkerError

from ..crystalforge import stepio
from ..engine import HARTREE_EV, BandData
from ..model import Structure

T_DEFAULT = [float(t) for t in range(300, 1201, 50)]
DOPING_DEFAULT = [float(f"{10 ** (18 + i / 4):.4g}") for i in range(13)]   # 1e18 … 1e21
TAU_DEFAULT = 1e-14
LPFAC_DEFAULT = 5
NPTS_DEFAULT = 4000
ERANGE_EV = 1.5
CM3_PER_BOHR3 = (0.529177210903e-8) ** 3


def boltztrap_version() -> str:
    try:
        from importlib.metadata import version
        return version("BoltzTraP2")
    except Exception:  # noqa: BLE001
        return "?"


def _iso(t: np.ndarray) -> np.ndarray:
    """Isotropic part (trace/3) of (..., 3, 3) tensors."""
    return np.trace(t, axis1=-2, axis2=-1) / 3.0


def compute_transport(bd: BandData, t_k=None, doping_cm3=None, *, lpfac: int = LPFAC_DEFAULT,
                      npts: int = NPTS_DEFAULT, erange_ev: float = ERANGE_EV,
                      nworkers: int = 1) -> dict[str, Any]:
    """Transport tables (see module doc) from band data. Pure function of its inputs."""
    from BoltzTraP2 import bandlib as BL
    from BoltzTraP2 import fite, sphere
    from BoltzTraP2.units import BOLTZMANN

    logging.getLogger("BoltzTraP2").setLevel(logging.WARNING)
    t_k = np.array(sorted(float(t) for t in (t_k or T_DEFAULT)), dtype=float)
    dop = np.array(sorted(abs(float(d)) for d in (doping_cm3 or DOPING_DEFAULT)), dtype=float)
    if len(t_k) == 0 or np.any(t_k <= 0):
        raise ValueError("t_k must be positive temperatures")
    if len(dop) == 0 or np.any(dop <= 0):
        raise ValueError("doping_cm3 must be non-zero concentrations")
    gap, direct, vbm_ev, cbm_ev = bd.band_gap()
    data = bd.boltztrap_data()
    if gap > 0:
        emin, emax = (vbm_ev - erange_ev) / HARTREE_EV, (cbm_ev + erange_ev) / HARTREE_EV
    else:
        emin = (bd.fermi * HARTREE_EV - erange_ev) / HARTREE_EV
        emax = (bd.fermi * HARTREE_EV + erange_ev) / HARTREE_EV
    data.bandana(emin, emax)
    if data.ebands.shape[0] == 0:
        raise ValueError("no bands inside the energy window")
    nk_in = len(data.kpoints)
    equivalences = sphere.get_equivalences(data.atoms, data.magmom, int(lpfac) * nk_in)
    if len(equivalences) < nk_in:
        raise ValueError(f"interpolation grid ({len(equivalences)}) sparser than the input "
                         f"({nk_in} k-points); raise lpfac")
    coeffs = fite.fitde3D(data, equivalences)
    lattvec = data.get_lattvec()
    eband, vvband, _ = fite.getBTPbands(equivalences, coeffs, lattvec, curvature=False,
                                        nworkers=max(1, min(2, int(nworkers))))
    epsilon, dos, vvdos, _ = BL.BTPDOS(eband, vvband, npts=int(npts))
    vuc = data.get_volume()                       # Bohr^3 (atomic units, as BoltzTraP2)
    vuc_cm3 = vuc * CM3_PER_BOHR3
    nelect = data.nelect
    margin = 9.0 * BOLTZMANN * t_k.max()
    mumin, mumax = epsilon.min() + margin, epsilon.max() - margin

    nT, nd = len(t_k), len(dop)
    out: dict[str, Any] = {}
    for kind, sign in (("p", 1.0), ("n", -1.0)):
        tabs = {k: np.full((nT, nd), np.nan) for k in
                ("mu_ev", "carriers_cm3", "seebeck_uv_k", "sigma_over_tau_s_m_s",
                 "kappa_e_over_tau_w_mks", "kappa0_over_tau_w_mks", "pf_over_tau_w_mk2s")}
        for iT, T in enumerate(t_k):
            lo = (BL.calc_N(epsilon, dos, mumax, T, data.dosweight) + nelect) / vuc_cm3
            hi = (BL.calc_N(epsilon, dos, mumin, T, data.dosweight) + nelect) / vuc_cm3
            mus, idx = [], []
            for idop, d in enumerate(dop):
                target = sign * d                      # net holes per cm^3
                if not (lo < target < hi):
                    continue
                n_el = nelect - target * vuc_cm3
                mu = BL.solve_for_mu(epsilon, dos, n_el, T, data.dosweight, refine=True,
                                     try_center=False)
                mus.append(mu)
                idx.append(idop)
            if not mus:
                continue
            mur = np.array(mus)
            Tr = np.array([T])
            N, L0, L1, L2, _ = BL.fermiintegrals(epsilon, dos, vvdos, mur=mur, Tr=Tr,
                                                 dosweight=data.dosweight)
            sigma, seebeck, kappa, _ = BL.calc_Onsager_coefficients(L0, L1, L2, mur, Tr, vuc)
            sig = sigma[0]
            S = seebeck[0]
            ke = kappa[0]
            k0 = ke + T * np.einsum("mij,mjk,mkl->mil", sig, S, S)
            s_iso, S_iso = _iso(sig), _iso(S)
            for j, idop in enumerate(idx):
                tabs["mu_ev"][iT, idop] = mur[j] * HARTREE_EV - vbm_ev
                tabs["carriers_cm3"][iT, idop] = sign * (N[0, j] + nelect) / vuc_cm3
                tabs["seebeck_uv_k"][iT, idop] = S_iso[j] * 1e6
                tabs["sigma_over_tau_s_m_s"][iT, idop] = s_iso[j]
                tabs["kappa_e_over_tau_w_mks"][iT, idop] = _iso(ke)[j]
                tabs["kappa0_over_tau_w_mks"][iT, idop] = _iso(k0)[j]
                tabs["pf_over_tau_w_mk2s"][iT, idop] = S_iso[j] ** 2 * s_iso[j]
        out[kind] = {k: _nan_to_none(v) for k, v in tabs.items()}

    out.update({
        "format": "fireforge.transport.v1",
        "t_k": t_k.tolist(), "doping_cm3": dop.tolist(), "crta": True,
        "gap_ev": gap, "gap_direct": bool(direct), "vbm_ev": vbm_ev, "cbm_ev": cbm_ev,
        "nelect": float(bd.nelect), "lpfac": int(lpfac), "n_kpoints_input": nk_in,
        "n_equivalences": int(len(equivalences)), "npts": int(npts),
        "erange_ev": float(erange_ev),
        "engine": bd.provenance.engine, "formula": bd.structure.reduced_formula(),
        "fingerprint": bd.structure.fingerprint(),
        "units": {"t_k": "K", "doping_cm3": "cm^-3", "mu_ev": "eV relative to VBM",
                  "seebeck_uv_k": "uV/K", "sigma_over_tau_s_m_s": "S/(m s)",
                  "kappa_e_over_tau_w_mks": "W/(m K s)", "kappa0_over_tau_w_mks": "W/(m K s)",
                  "pf_over_tau_w_mk2s": "W/(m K^2 s)", "carriers_cm3": "cm^-3"},
    })
    return out


def _nan_to_none(a: np.ndarray) -> list:
    return [[None if not math.isfinite(x) else float(x) for x in row] for row in a.tolist()]


def table(tr: dict, kind: str, key: str) -> np.ndarray:
    """A [nT][nd] table of a transport JSON as a float array (NaN for null)."""
    try:
        rows = tr[kind][key]
    except (KeyError, TypeError):
        raise WorkerError(f"transport: missing table {kind}.{key}") from None
    return np.array([[np.nan if v is None else float(v) for v in r] for r in rows], dtype=float)


def summary(tr: dict, tau_s: float) -> dict[str, Any]:
    """Record properties: per type the PF maximum (at τ and per τ), its doping and T,
    Seebeck at 300 / 800 K at that doping."""
    t = np.array(tr["t_k"])
    dop = np.array(tr["doping_cm3"])
    props: dict[str, Any] = {"tau_s": tau_s, "band_gap_ev": tr.get("gap_ev")}
    for kind in ("p", "n"):
        pf = table(tr, kind, "pf_over_tau_w_mk2s")
        S = table(tr, kind, "seebeck_uv_k")
        if not np.isfinite(pf).any():
            continue
        iT, idop = np.unravel_index(np.nanargmax(pf), pf.shape)
        props[f"pf_max_{kind}_over_tau_w_mk2s"] = float(pf[iT, idop])
        props[f"pf_max_{kind}_uw_cmk2"] = float(pf[iT, idop] * tau_s * 1e4)   # W/mK² → µW/cmK²
        props[f"doping_opt_{kind}_cm3"] = float(dop[idop])
        props[f"t_pf_max_{kind}_k"] = float(t[iT])
        for tt in (300.0, 800.0):
            j = np.nonzero(np.isclose(t, tt))[0]
            if len(j) and math.isfinite(S[j[0], idop]):
                props[f"seebeck_{kind}_uv_k_{int(tt)}k"] = float(S[j[0], idop])
    return props


def op_boltztrap(ctx):
    p = ctx.params
    path = ctx.named_inputs.get("bands")
    if path is None:
        raise WorkerError(f"transport.boltztrap: missing input 'bands' (have: "
                          f"{', '.join(sorted(ctx.named_inputs)) or 'none'})")
    try:
        bd = BandData.load(path)
    except (OSError, ValueError, KeyError) as e:
        raise WorkerError(f"transport.boltztrap: input 'bands' unreadable: {e}") from None
    sdoc = stepio.input_json(ctx, "structure", required=False)
    structure = Structure.from_json(sdoc) if sdoc else bd.structure
    tau = float(p.get("tau_s", TAU_DEFAULT))
    lpfac = int(p.get("lpfac", LPFAC_DEFAULT))
    try:
        tr = compute_transport(bd, p.get("t_k"), p.get("doping_cm3"), lpfac=lpfac,
                               npts=int(p.get("npts", NPTS_DEFAULT)),
                               erange_ev=float(p.get("erange_ev", ERANGE_EV)),
                               nworkers=int(p.get("cpus", 1)))
    except ValueError as e:
        raise WorkerError(f"transport.boltztrap: {e}") from None
    tr["tau_s"] = tau
    ref = ctx.save_output("transport", tr)
    bprov = bd.provenance
    settings = {"lpfac": lpfac, "npts": tr["npts"], "erange_ev": tr["erange_ev"],
                "tau_s": tau, "crta": True, "n_equivalences": tr["n_equivalences"],
                "bands": {"solver": bprov.solver, "solver_version": bprov.solver_version,
                          **{k: v for k, v in bprov.settings.items()
                             if isinstance(v, (int, float, str, list))}}}
    ctx.record("transport.crta", structure=structure, properties=summary(tr, tau),
               conditions={"t_k_min": tr["t_k"][0], "t_k_max": tr["t_k"][-1],
                           "doping_cm3_min": tr["doping_cm3"][0],
                           "doping_cm3_max": tr["doping_cm3"][-1]},
               method={"solver": "BoltzTraP2", "solver_version": boltztrap_version(),
                       "engine": bprov.engine, "functional": bprov.functional,
                       "pseudopotentials": list(bprov.pseudopotentials),
                       "preset": str(p.get("preset", bprov.settings.get("preset", ""))),
                       "settings": settings},
               outputs=[ref], uncertainty={"crta": True, "tau_s": tau},
               note="constant relaxation time approximation: sigma and kappa_e scale "
                    "with the assumed tau; not absolute conductivities")
    return None


def register(worker) -> None:
    worker.task("transport.boltztrap")(op_boltztrap)


__all__ = ["compute_transport", "summary", "table", "register", "T_DEFAULT",
           "DOPING_DEFAULT", "TAU_DEFAULT"]
_ = json  # (json used by callers via stepio)
