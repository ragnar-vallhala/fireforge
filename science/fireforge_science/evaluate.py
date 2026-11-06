"""te.evaluate: ZT(T, doping), regime averages, stability flags, element scores, screening
and the phono3py tier (phase8.md §4, Description.md §3).

Inputs (names):

- `transport` (required): transport.boltztrap JSON (transportforge doc).
- `phonon` (phonon.collect JSON): κ_l Slack per T (`kappa_source: "slack"`, default),
  `dynamically_stable`, `min_freq_thz`. `kappa3` (phonon.kappa3 JSON): κ_l BTE per T
  (`kappa_source: "bte"`). κ_l is read from `kappa_l_w_mk` as a list aligned with `t_k`,
  a {T: value} map, or per-T tensors (mean of the diagonal); also nested under
  `slack` / `bte` / `kappa`.
- `stability` (stability.formation), `elastic` (qe.elastic), `bands` (qe.nscf
  BandData npz), `structure` (relaxed structure) — optional; a missing one leaves its
  flag null, which fails the corresponding screening criterion.
- `fc2` (phonon.collect output) — needed only to spawn the phono3py tier.

ZT = S²σT / (κ_e + κ_l) with σ = (σ/τ)·τ, κ_e = (κ_e/τ)·τ, τ = `tau_s` (1e-14 s, CRTA:
results scale with the assumed τ, Description.md §15.4). κ_l(T) is interpolated
log–log between the given temperatures and extrapolated as κ ∝ 1/T (Umklapp) outside.

Regimes (Description.md §3.1): low 300–500 K, intermediate 500–800 K, high 800–1100 K,
extreme > 1100 K (to the highest T computed). `zt_avg_<regime>_<type>`: mean ZT over the
temperatures in the window (trapezoid in T), at the single doping that maximizes it.

Screening (`thresholds` overrides; a criterion set to null is not applied):
dynamically_stable, born_stable, e_above_hull_max_ev 0.1, band_gap_min_ev 0.05,
zt_peak_min 0.3 (max of p and n). A criterion whose data are missing fails.

Tier 2 (`tier2`: "auto" = when screen_pass, "force", "off"; only with kappa_source slack):
the step re-saves its inputs as outputs `in.<name>` and spawns

    fc3          phonon.fc3   inputs structure, fc2  (from "." in.structure / in.fc2)
    evaluate.bte te.evaluate  depends on fc3; kappa3 from fc3's spawned subtree
                              (StepInput("fc3", "kappa3", spawned=True)); transport,
                              phonon, stability, elastic, bands, structure from "." in.*;
                              params = this step's + kappa_source "bte", tier2 "off".

Output `evaluation` JSON (format "fireforge.evaluation.v1"); record `te.evaluate`.
"""
from __future__ import annotations

import json
import math
from functools import lru_cache
from pathlib import Path
from typing import Any

import numpy as np

from fireforge import StepInput, WorkerError

from .crystalforge import stepio

TAU_DEFAULT = 1e-14
REGIMES = {"low": (300.0, 500.0), "intermediate": (500.0, 800.0),
           "high": (800.0, 1100.0), "extreme": (1100.0, math.inf)}
THRESHOLDS = {"dynamically_stable": True, "born_stable": True,
              "e_above_hull_max_ev": 0.1, "band_gap_min_ev": 0.05, "zt_peak_min": 0.3}
DATA_DIR = Path(__file__).parent / "data"
PASS_THROUGH = ("transport", "phonon", "stability", "elastic", "bands", "structure", "fc2")


# ---------------------------------------------------------------------------------------
# κ_l

def _scalar_kappa(v) -> float | None:
    if isinstance(v, (int, float)):
        return stepio.finite(v)
    a = np.asarray(v, dtype=float)
    if a.shape == (3, 3):
        return float(np.trace(a) / 3)
    if a.shape in ((3,), (6,)):
        return float(np.mean(a[:3]))
    return None


def kappa_curve(doc: dict, what: str) -> tuple[np.ndarray, np.ndarray]:
    """(T, κ_l) arrays from a phonon / kappa3 JSON (see module doc). WorkerError naming
    the missing key."""
    if not isinstance(doc, dict):
        raise WorkerError(f"input {what!r}: expected a JSON object")
    for sub in ("slack", "bte", "kappa"):
        if "kappa_l_w_mk" not in doc and isinstance(doc.get(sub), dict):
            doc = {**doc, **doc[sub]}
    k = stepio.require_key(doc, "kappa_l_w_mk", what)
    if isinstance(k, dict):
        pairs = []
        for t, v in k.items():
            tt = stepio.finite(str(t).lower().rstrip("k"))
            vv = _scalar_kappa(v)
            if tt is not None and vv is not None:
                pairs.append((tt, vv))
    else:
        t = stepio.require_key(doc, "t_k", what)
        if len(t) != len(k):
            raise WorkerError(f"input {what!r}: t_k and kappa_l_w_mk lengths differ")
        pairs = [(float(a), _scalar_kappa(b)) for a, b in zip(t, k)]
        pairs = [(a, b) for a, b in pairs if b is not None]
    pairs = sorted((a, b) for a, b in pairs if a > 0 and b > 0)
    if not pairs:
        raise WorkerError(f"input {what!r}: no positive kappa_l_w_mk values")
    return np.array([a for a, _ in pairs]), np.array([b for _, b in pairs])


def kappa_at(tk: np.ndarray, kk: np.ndarray, t) -> np.ndarray:
    """κ_l at temperatures t: log–log interpolation inside, κ ∝ 1/T outside."""
    t = np.asarray(t, dtype=float)
    if len(tk) == 1:
        return kk[0] * tk[0] / t
    inside = np.exp(np.interp(np.log(t), np.log(tk), np.log(kk)))
    lo = kk[0] * tk[0] / t
    hi = kk[-1] * tk[-1] / t
    return np.where(t < tk[0], lo, np.where(t > tk[-1], hi, inside))


# ---------------------------------------------------------------------------------------
# ZT

def zt_tables(tr: dict, kappa_l: np.ndarray, tau_s: float) -> dict[str, dict[str, np.ndarray]]:
    """{type: {zt, pf_uw_cmk2, sigma_s_m, kappa_e_w_mk, seebeck_uv_k}} [nT][nd] arrays;
    kappa_l per T (len nT)."""
    from .transportforge import table
    t = np.asarray(tr["t_k"], dtype=float)[:, None]
    out = {}
    for kind in ("p", "n"):
        S = table(tr, kind, "seebeck_uv_k") * 1e-6
        sig = table(tr, kind, "sigma_over_tau_s_m_s") * tau_s
        ke = table(tr, kind, "kappa_e_over_tau_w_mks") * tau_s
        pf = S ** 2 * sig
        zt = pf * t / (ke + np.asarray(kappa_l, dtype=float)[:, None])
        out[kind] = {"zt": zt, "pf_uw_cmk2": pf * 1e4, "sigma_s_m": sig,
                     "kappa_e_w_mk": ke, "seebeck_uv_k": S * 1e6}
    return out


def regime_average(t_k, zt: np.ndarray, lo: float, hi: float) -> tuple[float | None, int | None]:
    """(best mean ZT over T in [lo, hi], doping index) — mean by trapezoid in T at one
    doping; None if fewer than one temperature lies in the window."""
    t = np.asarray(t_k, dtype=float)
    sel = (t >= lo - 1e-9) & (t <= hi + 1e-9)
    if not sel.any():
        return None, None
    ts, z = t[sel], zt[sel]
    if len(ts) == 1:
        means = z[0]
    else:
        means = np.trapezoid(z, ts, axis=0) / (ts[-1] - ts[0])
    if not np.isfinite(means).any():
        return None, None
    j = int(np.nanargmax(means))
    return float(means[j]), j


def _f(x):
    return None if x is None or not math.isfinite(float(x)) else float(x)


def zt_summary(tr: dict, zt: dict, kappa_l: np.ndarray) -> dict[str, Any]:
    t = np.asarray(tr["t_k"], dtype=float)
    dop = np.asarray(tr["doping_cm3"], dtype=float)
    s: dict[str, Any] = {}
    for kind in ("p", "n"):
        z = zt[kind]["zt"]
        if np.isfinite(z).any():
            iT, j = np.unravel_index(np.nanargmax(z), z.shape)
            s[f"zt_peak_{kind}"] = float(z[iT, j])
            s[f"t_peak_{kind}_k"] = float(t[iT])
            s[f"doping_peak_{kind}_cm3"] = float(dop[j])
            s[f"seebeck_peak_{kind}_uv_k"] = _f(zt[kind]["seebeck_uv_k"][iT, j])
        else:
            s[f"zt_peak_{kind}"] = None
        pf = zt[kind]["pf_uw_cmk2"]
        s[f"pf_peak_{kind}_uw_cmk2"] = float(np.nanmax(pf)) if np.isfinite(pf).any() else None
        for name, (lo, hi) in REGIMES.items():
            v, j = regime_average(t, z, lo, hi)
            s[f"zt_avg_{name}_{kind}"] = v
            s[f"zt_avg_{name}_{kind}_doping_cm3"] = None if j is None else float(dop[j])
    peaks = [s[k] for k in ("zt_peak_p", "zt_peak_n") if s.get(k) is not None]
    s["zt_peak"] = max(peaks) if peaks else None
    pfs = [s[k] for k in ("pf_peak_p_uw_cmk2", "pf_peak_n_uw_cmk2") if s.get(k) is not None]
    s["pf_peak_uw_cmk2"] = max(pfs) if pfs else None
    if 300.0 >= t[0] and 300.0 <= t[-1]:
        s["kappa_l_300k"] = float(np.interp(300.0, t, kappa_l))
    return s


# ---------------------------------------------------------------------------------------
# elements

@lru_cache(maxsize=1)
def element_data() -> dict[str, dict]:
    return json.loads((DATA_DIR / "elements.json").read_text())["elements"]


def element_scores(composition: dict[str, float]) -> dict[str, Any]:
    """abundance_ppm_min (limiting element), cost_usd_kg (mass-weighted element prices),
    toxicity (max class 0..2), elements without data listed in `elements_unknown`."""
    from pymatgen.core import Element
    data = element_data()
    unknown = sorted(e for e in composition if e not in data)
    out: dict[str, Any] = {"elements_unknown": unknown}
    known = {e: n for e, n in composition.items() if e in data}
    if unknown or not known:
        out.update({"abundance_ppm_min": None, "cost_usd_kg": None, "toxicity": None})
        return out
    mass = {e: n * float(Element(e).atomic_mass) for e, n in known.items()}
    total = sum(mass.values())
    out["abundance_ppm_min"] = min(float(data[e]["abundance_ppm"]) for e in known)
    out["abundance_limiting_element"] = min(known, key=lambda e: data[e]["abundance_ppm"])
    out["cost_usd_kg"] = sum(mass[e] / total * float(data[e]["price_usd_per_kg"])
                             for e in known)
    out["toxicity"] = max(int(data[e]["toxicity"]) for e in known)
    out["toxic_elements"] = sorted(e for e in known if int(data[e]["toxicity"]) >= 2)
    out["scores_uncertain"] = sorted({f"{e}.{u}" for e in known for u in data[e].get("uncertain", [])})
    return out


# ---------------------------------------------------------------------------------------
# screening

def screen(flags: dict[str, Any], zt_peak: float | None, thresholds: dict | None = None
           ) -> tuple[bool, dict[str, bool | None]]:
    """(screen_pass, per-criterion pass) — see module doc."""
    th = {**THRESHOLDS, **(thresholds or {})}
    bad = set(th) - set(THRESHOLDS)
    if bad:
        raise WorkerError(f"te.evaluate: unknown thresholds {', '.join(sorted(bad))} "
                          f"(have: {', '.join(THRESHOLDS)})")
    res: dict[str, bool | None] = {}

    def need(name, ok):
        if th[name] is None:
            return
        res[name] = bool(ok) if ok is not None else False

    need("dynamically_stable", flags.get("dynamically_stable") if th["dynamically_stable"] else True)
    need("born_stable", flags.get("born_stable") if th["born_stable"] else True)
    eh = flags.get("e_above_hull_ev_per_atom")
    need("e_above_hull_max_ev", None if eh is None else eh <= th["e_above_hull_max_ev"])
    g = flags.get("band_gap_ev")
    need("band_gap_min_ev", None if g is None else g >= th["band_gap_min_ev"])
    need("zt_peak_min", None if zt_peak is None else zt_peak >= th["zt_peak_min"])
    return all(res.values()), res


# ---------------------------------------------------------------------------------------
# operation

def _structure_info(sdoc: dict | None) -> dict[str, Any]:
    if not sdoc:
        return {}
    from .model import Structure
    s = Structure.from_json(sdoc)
    info: dict[str, Any] = {"formula": s.reduced_formula(), "fingerprint": s.fingerprint(),
                            "spacegroup_number": s.spacegroup()[0],
                            "volume_a3_per_atom": s.volume() / s.nsites}
    try:
        from pymatgen.symmetry.analyzer import SpacegroupAnalyzer
        conv = SpacegroupAnalyzer(s.to_pymatgen(), symprec=1e-2).get_conventional_standard_structure()
        info["a_angstrom"] = float(conv.lattice.a)
    except Exception:  # noqa: BLE001 - lattice constant is informational
        pass
    comp: dict[str, float] = {}
    for sp in s.species:
        comp[sp] = comp.get(sp, 0) + 1
    info["composition"] = comp
    info["_structure"] = s
    return info


def evaluate(ctx) -> dict[str, Any]:
    """Build the evaluation document from the context's inputs (no outputs written)."""
    p = ctx.params
    tau = float(p.get("tau_s", TAU_DEFAULT))
    source = p.get("kappa_source", "slack")
    if source not in ("slack", "bte"):
        raise WorkerError(f"te.evaluate: kappa_source must be 'slack' or 'bte', got {source!r}")
    tr = stepio.input_json(ctx, "transport")
    for key in ("t_k", "doping_cm3", "p", "n"):
        stepio.require_key(tr, key, "transport")
    phonon = stepio.input_json(ctx, "phonon", required=(source == "slack"))
    kdoc = phonon if source == "slack" else stepio.input_json(ctx, "kappa3")
    t = np.asarray(tr["t_k"], dtype=float)
    kappa_note = ""
    try:
        tk, kk = kappa_curve(kdoc, "phonon" if source == "slack" else "kappa3")
        kl = kappa_at(tk, kk, t)
    except WorkerError as e:
        # No usable κ_l (e.g. dynamically unstable, or γ ≤ 0 so Slack does not apply):
        # still evaluate transport and stability; ZT is unknown and screening fails.
        if source == "bte":
            raise
        sub = kdoc.get("slack") if isinstance(kdoc, dict) else None
        kappa_note = (sub or {}).get("note") or str(e)
        kl = np.full(len(t), np.inf)
    zt = zt_tables(tr, kl, tau)
    summ = zt_summary(tr, zt, kl)
    if kappa_note:
        for k in list(summ):
            if k.startswith(("zt_", "t_peak", "doping_peak", "kappa_l", "regime")):
                summ[k] = None

    stab = stepio.input_json(ctx, "stability", required=False)
    elastic = stepio.input_json(ctx, "elastic", required=False)
    sinfo = _structure_info(stepio.input_json(ctx, "structure", required=False))
    gap = direct = None
    engines = [tr.get("engine", "")]
    if "bands" in ctx.named_inputs:
        from .engine import BandData
        try:
            bd = BandData.load(ctx.input("bands"))
        except (OSError, ValueError, KeyError) as e:
            raise WorkerError(f"te.evaluate: input 'bands' unreadable: {e}") from None
        gap, direct, _, _ = bd.band_gap()
        engines.append(bd.provenance.engine)
        if not sinfo:
            sinfo = _structure_info(bd.structure.to_json())
    elif tr.get("gap_ev") is not None:
        gap, direct = float(tr["gap_ev"]), tr.get("gap_direct")
    for d in (phonon, kdoc, stab, elastic):
        if isinstance(d, dict) and d.get("engine"):
            engines.append(str(d["engine"]))
    engine = stepio.check_engines(engines, "te.evaluate")

    from .stabilityforge import born_from_elastic
    flags = {
        "dynamically_stable": None if phonon is None or phonon.get("dynamically_stable") is None
        else bool(phonon["dynamically_stable"]),
        "min_freq_thz": None if phonon is None else stepio.finite(phonon.get("min_freq_thz")),
        "born_stable": born_from_elastic(elastic) if elastic is not None else None,
        "formation_ev_per_atom": None if stab is None else stepio.finite(stab.get("formation_ev_per_atom")),
        "e_above_hull_ev_per_atom": None if stab is None else stepio.finite(stab.get("e_above_hull_ev_per_atom")),
        "band_gap_ev": gap, "gap_direct": direct,
    }
    passed, criteria = screen(flags, summ.get("zt_peak"), p.get("thresholds"))
    comp = sinfo.get("composition")
    if comp is None and tr.get("formula"):
        from pymatgen.core import Composition
        comp = {str(e): float(n) for e, n in Composition(tr["formula"]).items()}
    scores = element_scores(comp) if comp else {}
    kslack = None
    if phonon is not None and source == "bte":
        try:
            ts, ks = kappa_curve(phonon, "phonon")
            kslack = float(kappa_at(ts, ks, [300.0])[0])
        except WorkerError:
            kslack = None
    doc = {
        "format": "fireforge.evaluation.v1",
        "formula": sinfo.get("formula") or tr.get("formula"),
        "fingerprint": sinfo.get("fingerprint") or tr.get("fingerprint"),
        "a_angstrom": sinfo.get("a_angstrom"),
        "spacegroup_number": sinfo.get("spacegroup_number"),
        "engine": engine, "kappa_source": source, "tau_s": tau, "crta": True,
        **summ,
        "kappa_l_300k_slack": summ.get("kappa_l_300k") if source == "slack" else kslack,
        "kappa_l_300k_bte": summ.get("kappa_l_300k") if source == "bte" else None,
        **flags, **scores,
        "theta_d_k": None if phonon is None else stepio.finite(phonon.get("theta_d_k")),
        "gruneisen": None if phonon is None else stepio.finite(phonon.get("gruneisen")),
        "screen_pass": passed, "criteria": criteria,
        "thresholds": {**THRESHOLDS, **(p.get("thresholds") or {})},
        "t_k": t.tolist(), "doping_cm3": list(tr["doping_cm3"]),
        "kappa_l_w_mk": None if kappa_note else kl.tolist(),
        "kappa_note": kappa_note or None,
        "tables": {k: {"zt": None if kappa_note else _rows(v["zt"]),
                       "pf_uw_cmk2": _rows(v["pf_uw_cmk2"])}
                   for k, v in zt.items()},
    }
    doc["_structure"] = sinfo.get("_structure")
    doc["structure"] = doc["_structure"].to_json() if doc["_structure"] is not None else None
    return doc


def _rows(a: np.ndarray) -> list:
    return [[None if not math.isfinite(x) else float(x) for x in r] for r in a.tolist()]


RECORD_KEYS = ("zt_peak_p", "zt_peak_n", "t_peak_p_k", "t_peak_n_k", "doping_peak_p_cm3",
               "doping_peak_n_cm3", "pf_peak_uw_cmk2", "pf_peak_p_uw_cmk2", "pf_peak_n_uw_cmk2",
               "kappa_l_300k", "dynamically_stable", "born_stable", "e_above_hull_ev_per_atom",
               "formation_ev_per_atom", "band_gap_ev", "abundance_ppm_min", "cost_usd_kg",
               "toxicity", "screen_pass")


def op_evaluate(ctx):
    p = ctx.params
    doc = evaluate(ctx)
    structure = doc.pop("_structure")
    tier2 = p.get("tier2", "auto")
    if tier2 not in ("auto", "force", "off"):
        raise WorkerError(f"te.evaluate: tier2 must be auto/force/off, got {tier2!r}")
    spawn_tier2 = (doc["kappa_source"] == "slack"
                   and (tier2 == "force" or (tier2 == "auto" and doc["screen_pass"])))
    doc["tier2_spawned"] = False
    if spawn_tier2:
        missing = [n for n in ("structure", "fc2") if n not in ctx.named_inputs]
        if missing:
            raise WorkerError(f"te.evaluate: tier2 needs input(s) {', '.join(missing)} "
                              "(relaxed structure and phonon.collect fc2)")
        doc["tier2_spawned"] = True
    ref = ctx.save_output("evaluation", doc)
    if spawn_tier2:
        spawn_tier2_steps(ctx, p)

    props = {k: doc.get(k) for k in RECORD_KEYS if doc.get(k) is not None}
    for name in REGIMES:
        for kind in ("p", "n"):
            v = doc.get(f"zt_avg_{name}_{kind}")
            if v is not None:
                props[f"zt_avg_{name}_{kind}"] = v
    if structure is None:
        raise WorkerError("te.evaluate: the record needs a structure: give input "
                          "'structure' or 'bands'")
    ctx.record("te.evaluate", structure=structure, properties=props,
               conditions={"t_k_min": doc["t_k"][0], "t_k_max": doc["t_k"][-1],
                           "kappa_source": doc["kappa_source"]},
               method={"solver": "fireforge_science.evaluate", "engine": doc["engine"] or "unknown",
                       "preset": str(p.get("preset", "")),
                       "settings": {"tau_s": doc["tau_s"], "kappa_source": doc["kappa_source"],
                                    "thresholds": doc["thresholds"], "crta": True}},
               outputs=[ref],
               uncertainty={"crta": True, "tau_s": doc["tau_s"],
                            "kappa_l_factor": 2.0 if doc["kappa_source"] == "slack" else None},
               note="ZT under CRTA with assumed tau; scales with tau")
    return None


def spawn_tier2_steps(ctx, params: dict) -> list[str]:
    """Re-save inputs as `in.<name>` and spawn fc3 + evaluate.bte (module doc)."""
    present = [n for n in PASS_THROUGH if n in ctx.named_inputs]
    for n in present:
        ctx.save_output(f"in.{n}", ctx.input(n), media_type=None,
                        filename=f"in/{n}{Path(ctx.input(n)).suffix or ''}")
    engine = params.get("engine", "qe")
    cpus = int(params.get("cpus", 2))
    t2 = dict(params.get("tier2_params") or {})
    ctx.spawn("fc3", operation="phonon.fc3",
              params={"engine": engine, "cpus": cpus, "preset": params.get("preset", "screening"),
                      **t2},
              inputs=[StepInput(".", "in.structure", as_name="structure"),
                      StepInput(".", "in.fc2", as_name="fc2")],
              **stepio.light_step_kwargs(["phono3py"]))
    child = {k: v for k, v in params.items() if k not in ("tier2", "kappa_source")}
    child.update({"kappa_source": "bte", "tier2": "off"})
    inputs = [StepInput("fc3", "kappa3", spawned=True, as_name="kappa3")]
    inputs += [StepInput(".", f"in.{n}", as_name=n) for n in present if n != "fc2"]
    ctx.spawn("evaluate.bte", operation="te.evaluate", params=child, inputs=inputs,
              depends_on=["fc3"], **stepio.light_step_kwargs())
    return ["fc3", "evaluate.bte"]


def register(worker) -> None:
    worker.task("te.evaluate")(op_evaluate)
