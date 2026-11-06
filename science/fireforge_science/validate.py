"""te.validate: compare computed half-Heusler properties with the literature
(phase8.md §8) and the literature import helper.

Inputs: any number of te.evaluate `evaluation` JSONs (recognized by content: format
"fireforge.evaluation.v1"; input names are free, e.g. `evaluation`, `evaluation#2`, as
produced by wiring every evaluate step with `spawned: true` so the tier-2
`evaluate.bte` results are included). Other inputs are ignored. An evaluation carries
everything checked: a_angstrom, band_gap_ev / gap_direct, dynamically_stable,
born_stable, formation_ev_per_atom, kappa_l_300k (slack or bte), zt_peak_p/n.

Params: `reference` (path of a literature file; default data/literature_halfheusler.json),
`allow_toy` (false: refuse evaluations whose engine is "toy"), `tolerances` overriding
TOLERANCES.

Checks per compound (skipped, with status "no_data", when either side is missing):
lattice (|Δa|/a ≤ 2 %), gap (|ΔEg| ≤ 0.2 eV vs published PBE) and gap character,
stable (dynamically and mechanically stable, formation energy < 0), kappa_slack
(within a factor 3 of experiment), kappa_bte (ZrNiSn: factor 1.5). Set checks:
kappa_rank — the experimental κ_l order of the compounds with both values; the computed
Slack values must order at least `kappa_rank_min_fraction` (0.75) of the pairs adjacent
in experimental rank the same way (phase8.md asks "4 of 5 pairs"; with five compounds
there are four adjacent pairs, so 3 of 4 is required by default — set 1.0 for all);
best_p / best_n: NbFeSb has the highest computed peak p-type ZT, ZrNiSn the highest
n-type, among the compounds evaluated.

Outputs `report.md`, `report.json`; record `te.validation` per compound (deviations,
pass/fail per check).
"""
from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any

from fireforge import WorkerError

from .crystalforge import stepio

DATA_DIR = Path(__file__).parent / "data"
LITERATURE = DATA_DIR / "literature_halfheusler.json"
TOLERANCES = {"lattice_rel": 0.02, "gap_ev": 0.2, "kappa_slack_factor": 3.0,
              "kappa_bte_factor": 1.5, "kappa_rank_min_fraction": 0.75}
BTE_CHECKED = ("ZrNiSn",)
BEST = {"p": "NbFeSb", "n": "ZrNiSn"}


def load_literature(path=None) -> dict[str, Any]:
    return json.loads(Path(path or LITERATURE).read_text())


def lit_value(lit: dict, formula: str, key: str):
    q = ((lit.get("compounds") or {}).get(formula) or {}).get(key)
    if not isinstance(q, dict):
        return None, {}
    return q.get("value"), q


def _check(status: bool | None, **kw) -> dict[str, Any]:
    return {"status": "no_data" if status is None else ("pass" if status else "fail"), **kw}


def collect_evaluations(ctx) -> dict[str, dict[str, dict]]:
    """formula -> {"slack": doc, "bte": doc} from the inputs (by content)."""
    out: dict[str, dict[str, dict]] = {}
    for name, path in sorted(ctx.named_inputs.items()):
        try:
            doc = json.loads(Path(path).read_text())
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            continue
        if not isinstance(doc, dict) or doc.get("format") != "fireforge.evaluation.v1":
            continue
        f = doc.get("formula")
        if not f:
            raise WorkerError(f"te.validate: evaluation {name!r} has no formula")
        out.setdefault(f, {})[doc.get("kappa_source", "slack")] = doc
    return out


def validate(evals: dict[str, dict[str, dict]], lit: dict, tolerances: dict | None = None,
             allow_toy: bool = False) -> dict[str, Any]:
    tol = {**TOLERANCES, **(tolerances or {})}
    engines = sorted({d.get("engine") or "" for e in evals.values() for d in e.values()})
    if "toy" in engines and not allow_toy:
        raise WorkerError("te.validate: refusing toy-engine data (ToyEngine results are not "
                          "physical); pass allow_toy: true only for tests")
    compounds: dict[str, Any] = {}
    for f in sorted(set(evals) | set((lit.get("compounds") or {}))):
        e = evals.get(f, {})
        base = e.get("slack") or e.get("bte")
        if base is None:
            continue
        checks: dict[str, Any] = {}
        a_lit, q = lit_value(lit, f, "lattice_constant_angstrom")
        a = base.get("a_angstrom")
        if a is not None and a_lit:
            dev = (a - a_lit) / a_lit
            checks["lattice"] = _check(abs(dev) <= tol["lattice_rel"], computed=a,
                                       literature=a_lit, deviation_rel=dev, doi=q.get("doi"))
        else:
            checks["lattice"] = _check(None, computed=a, literature=a_lit)
        g_lit, q = lit_value(lit, f, "band_gap_pbe_ev")
        g = base.get("band_gap_ev")
        if g is not None and g_lit is not None:
            dev = g - g_lit
            checks["gap"] = _check(abs(dev) <= tol["gap_ev"], computed=g, literature=g_lit,
                                   deviation_ev=dev, doi=q.get("doi"))
        else:
            checks["gap"] = _check(None, computed=g, literature=g_lit)
        ch = q.get("character") if q else None
        direct = base.get("gap_direct")
        if ch in ("direct", "indirect") and direct is not None:
            checks["gap_character"] = _check((ch == "direct") == bool(direct),
                                             computed="direct" if direct else "indirect",
                                             literature=ch)
        else:
            checks["gap_character"] = _check(None, literature=ch)
        ds, bs, hf = (base.get("dynamically_stable"), base.get("born_stable"),
                      base.get("formation_ev_per_atom"))
        st = None if None in (ds, bs, hf) else bool(ds and bs and hf < 0)
        checks["stable"] = _check(st, dynamically_stable=ds, born_stable=bs,
                                  formation_ev_per_atom=hf)
        k_lit, q = lit_value(lit, f, "kappa_l_300k_w_mk")
        for src, key, ftol in (("slack", "kappa_slack", tol["kappa_slack_factor"]),
                               ("bte", "kappa_bte", tol["kappa_bte_factor"])):
            d = e.get(src)
            k = None if d is None else d.get("kappa_l_300k")
            if src == "bte" and f not in BTE_CHECKED and d is None:
                continue
            if k and k_lit:
                r = k / k_lit
                checks[key] = _check(1 / ftol <= r <= ftol, computed=k, literature=k_lit,
                                     ratio=r, doi=q.get("doi"),
                                     literature_kind=q.get("kind", "lattice"))
            else:
                checks[key] = _check(None, computed=k, literature=k_lit)
        compounds[f] = {"checks": checks, "zt_peak_p": base.get("zt_peak_p"),
                        "zt_peak_n": base.get("zt_peak_n"),
                        "kappa_l_300k": (e.get("slack") or {}).get("kappa_l_300k"),
                        "engine": base.get("engine"), "structure": base.get("structure")}

    sets: dict[str, Any] = {}
    ranked = []
    for f, c in compounds.items():
        k_lit, _ = lit_value(lit, f, "kappa_l_300k_w_mk")
        if k_lit and c["kappa_l_300k"]:
            ranked.append((k_lit, c["kappa_l_300k"], f))
    if len(ranked) >= 2:
        ranked.sort()
        pairs = [(ranked[i], ranked[i + 1]) for i in range(len(ranked) - 1)]
        ok = [b[1] > a[1] for a, b in pairs]
        frac = sum(ok) / len(ok)
        sets["kappa_rank"] = _check(frac >= tol["kappa_rank_min_fraction"] - 1e-9,
                                    experimental_order=[r[2] for r in ranked],
                                    pairs_agree=sum(ok), pairs=len(ok), fraction=frac)
    else:
        sets["kappa_rank"] = _check(None)
    for kind, want in BEST.items():
        vals = {f: c[f"zt_peak_{kind}"] for f, c in compounds.items()
                if c.get(f"zt_peak_{kind}") is not None}
        if want in vals and len(vals) >= 2:
            best = max(vals, key=vals.get)
            sets[f"best_{kind}"] = _check(best == want, expected=want, computed_best=best,
                                          values=vals)
        else:
            sets[f"best_{kind}"] = _check(None, expected=want)
    allc = [c["status"] for comp in compounds.values() for c in comp["checks"].values()]
    allc += [c["status"] for c in sets.values()]
    return {"format": "fireforge.validation.v1", "engine": ",".join(e for e in engines if e),
            "tolerances": tol, "compounds": compounds, "set_checks": sets,
            "n_pass": allc.count("pass"), "n_fail": allc.count("fail"),
            "n_no_data": allc.count("no_data"), "passed": allc.count("fail") == 0}


def _fmt(v, nd=3):
    if v is None:
        return "–"
    if isinstance(v, float):
        return f"{v:.{nd}g}"
    return str(v)


def report_markdown(rep: dict) -> str:
    lines = ["# FireForge half-Heusler validation", "",
             f"Engine: {rep['engine'] or '?'} · checks pass {rep['n_pass']}, fail "
             f"{rep['n_fail']}, no data {rep['n_no_data']} · overall "
             f"**{'PASS' if rep['passed'] else 'FAIL'}**", "",
             "ZT and κ_e use the constant relaxation time approximation with an assumed τ "
             "(not absolute conductivities).", "",
             "| Compound | Check | Status | Computed | Literature | Deviation | DOI |",
             "|---|---|---|---|---|---|---|"]
    for f, c in rep["compounds"].items():
        for name, ch in c["checks"].items():
            dev = ch.get("deviation_rel", ch.get("deviation_ev", ch.get("ratio")))
            lines.append(f"| {f} | {name} | {ch['status']} | {_fmt(ch.get('computed'))} | "
                         f"{_fmt(ch.get('literature'))} | {_fmt(dev)} | {ch.get('doi') or ''} |")
    lines += ["", "## Set checks", ""]
    for name, ch in rep["set_checks"].items():
        extra = {k: v for k, v in ch.items() if k != "status"}
        lines.append(f"- **{name}**: {ch['status']} {json.dumps(extra, default=str)}")
    return "\n".join(lines) + "\n"


def op_validate(ctx):
    p = ctx.params
    evals = collect_evaluations(ctx)
    if not evals:
        avail = ", ".join(sorted(ctx.named_inputs)) or "none"
        raise WorkerError(f"te.validate: no evaluation inputs (have: {avail})")
    ref_path = p.get("reference")
    lit = load_literature(ref_path)
    rep = validate(evals, lit, p.get("tolerances"), bool(p.get("allow_toy", False)))
    md = ctx.save_output("report.md", report_markdown(rep), media_type="text/markdown",
                         filename="report.md")
    js = ctx.save_output("report.json", rep, filename="report.json")
    for f, c in rep["compounds"].items():
        if not c.get("structure"):
            continue
        props: dict[str, Any] = {}
        for name, ch in c["checks"].items():
            props[f"{name}_pass"] = {"pass": True, "fail": False}.get(ch["status"])
            for k in ("deviation_rel", "deviation_ev", "ratio"):
                if ch.get(k) is not None:
                    props[f"{name}_{k}"] = ch[k]
        props = {k: v for k, v in props.items() if v is not None}
        ctx.record("te.validation", structure=c["structure"], properties=props,
                   method={"solver": "fireforge_science.validate", "engine": c["engine"] or "unknown",
                           "settings": {"tolerances": rep["tolerances"],
                                        "reference": str(ref_path or LITERATURE.name)}},
                   outputs=[md, js])
    return None


# ---------------------------------------------------------------------------------------
# literature import (tdrctl cp calc-import FILE)

LIT_KIND = {"lattice_constant_angstrom": ("literature.lattice", "a_angstrom"),
            "band_gap_pbe_ev": ("literature.band_gap", "band_gap_ev"),
            "kappa_l_300k_w_mk": ("literature.kappa_l", "kappa_l_w_mk_300k"),
            "zt_peak": ("literature.zt_peak", "zt_peak")}


def literature_import_request(path=None, campaign_id: str = "") -> dict[str, Any]:
    """ImportCalculationsRequest (proto3 JSON, proto field names) with one LITERATURE
    record per non-null literature value, structure inline (ideal half-Heusler cell at
    the experimental lattice constant, known ordering)."""
    from .crystalforge.halfheusler import build
    lit = load_literature(path)
    records = []
    for f, c in (lit.get("compounds") or {}).items():
        x, y, z = c["roles"]
        a = (c.get("lattice_constant_angstrom") or {}).get("value")
        s = build(x, y, z, "y4c", a=a).copy(source="literature")
        for key, (kind, prop) in LIT_KIND.items():
            q = c.get(key) or {}
            if q.get("value") is None:
                continue
            props = {prop: q["value"]}
            cond: dict[str, Any] = {}
            for extra in ("character", "type", "kind", "t_k", "composition", "functional"):
                if q.get(extra) is not None:
                    (cond if extra in ("t_k", "composition") else props)[
                        {"type": "carrier_type", "kind": "quantity"}.get(extra, extra)] = q[extra]
            records.append({
                "kind": kind, "structure": s.to_json(),
                "properties_json": json.dumps(props), "conditions_json": json.dumps(cond),
                "evidence": "LITERATURE",
                "note": f"{q.get('citation', '')} doi:{q.get('doi', '')} {q.get('note', '')}".strip()})
    out: dict[str, Any] = {"records": records}
    if campaign_id:
        out["campaign_id"] = campaign_id
    return out


def register(worker) -> None:
    worker.task("te.validate")(op_validate)


_ = math
