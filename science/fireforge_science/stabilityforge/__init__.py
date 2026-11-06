"""StabilityForge: formation energy, convex hull, mechanical (Born) stability
(phase8.md §4 stability.formation).

`stability.formation` — inputs (energy documents, see crystalforge.stepio.parse_energy):

- `compound` (or `compound.<k>`, `compound#<n>`): the target compound(s);
- every other named input (`ref.<El>`, `other.<k>`, …): elemental references and
  competing compounds that enter the hull.

Each is the `energy` JSON output of qe.relax ({energy_ev, natoms, energy_ev_per_atom,
formula, engine, …}) or a Structure JSON with an `energy_ev_per_atom` field. Param
`extra_entries` [{formula, energy_ev_per_atom}] adds further hull phases (same engine and
settings only — mixing functionals or codes makes the hull meaningless). Every element of
a target needs an elemental entry (lowest energy per atom over the inputs of that element).

Formation energy per atom: ΔH_f = E/N − Σ_el x_el·μ_el, μ_el the lowest elemental energy per
atom. Hull: pymatgen PhaseDiagram over all entries; e_above_hull and the decomposition
products of each target (empty for a phase on the hull).

Output `stability` JSON: {format "fireforge.stability.v1", formula (first target),
formation_ev_per_atom, e_above_hull_ev_per_atom, on_hull, decomposition {formula: frac},
hull_phases [formulas on the hull], compounds [same per target], references {El:
energy_ev_per_atom}, engine}. Record `stability.formation` per target.
"""
from __future__ import annotations

from typing import Any

from fireforge import WorkerError

from ..crystalforge import stepio

HULL_TOL = 1e-6        # eV/atom; below this a phase counts as on the hull


# ---------------------------------------------------------------------------------------
# Born criteria

def born_cubic(c11: float, c12: float, c44: float) -> bool:
    """Born–Huang criteria for a cubic crystal (Mouhat & Coudert, PRB 90, 224104 (2014),
    doi:10.1103/PhysRevB.90.224104): C11 − C12 > 0, C11 + 2 C12 > 0, C44 > 0."""
    return bool(c11 - c12 > 0 and c11 + 2 * c12 > 0 and c44 > 0)


def born_from_elastic(el: dict) -> bool | None:
    """Mechanical stability from a qe.elastic `elastic` JSON: recomputed from
    c11/c12/c44 (GPa) when present, else its `born_stable`; None if neither is there."""
    if not isinstance(el, dict):
        return None
    cs = [stepio.finite(el.get(k)) for k in ("c11_gpa", "c12_gpa", "c44_gpa")]
    if all(c is not None for c in cs):
        return born_cubic(*cs)
    b = el.get("born_stable")
    return None if b is None else bool(b)


# ---------------------------------------------------------------------------------------
# hull

def formation_and_hull(targets: list[dict], others: list[dict]) -> dict[str, Any]:
    """targets/others: parsed energy docs (stepio.parse_energy). Returns the `stability`
    document (without engine). ValueError if an element lacks an elemental entry."""
    from pymatgen.analysis.phase_diagram import PDEntry, PhaseDiagram
    from pymatgen.core import Composition

    all_docs = targets + others
    refs: dict[str, float] = {}
    for d in all_docs:
        if len(d["composition"]) == 1:
            el = next(iter(d["composition"]))
            refs[el] = min(refs.get(el, float("inf")), d["energy_ev_per_atom"])
    elements = sorted({el for d in targets for el in d["composition"]})
    missing = [el for el in elements if el not in refs]
    if missing:
        raise ValueError(f"no elemental reference energy for {', '.join(missing)} "
                         "(add qe.relax energies of crystal.reference structures as inputs)")

    entries, target_entries = [], []
    for i, d in enumerate(all_docs):
        comp = Composition(d["composition"])
        # only phases inside the targets' chemical space enter the hull
        if not set(map(str, comp.elements)) <= set(elements) and i >= len(targets):
            continue
        e = PDEntry(comp, d["energy_ev_per_atom"] * comp.num_atoms,
                    name=d["formula"], attribute=i)
        entries.append(e)
        if i < len(targets):
            target_entries.append(e)
    pd = PhaseDiagram(entries)
    hull = sorted({e.composition.reduced_formula for e in pd.stable_entries})

    compounds = []
    for d, e in zip(targets, target_entries):
        x = Composition(d["composition"]).fractional_composition
        hf = d["energy_ev_per_atom"] - sum(float(x[el]) * refs[el] for el in map(str, x.elements))
        decomp, ehull = pd.get_decomp_and_e_above_hull(e, allow_negative=True)
        ehull = float(ehull)
        on = ehull <= HULL_TOL
        compounds.append({
            "formula": d["formula"],
            "energy_ev_per_atom": d["energy_ev_per_atom"],
            "formation_ev_per_atom": float(hf),
            "e_above_hull_ev_per_atom": max(0.0, ehull),
            "hull_distance_ev_per_atom": ehull,      # negative: below the hull of the others
            "on_hull": on,
            "decomposition": {} if on else
            {k.composition.reduced_formula: float(v) for k, v in decomp.items()},
        })
    first = compounds[0]
    return {"format": "fireforge.stability.v1", **first, "hull_phases": hull,
            "compounds": compounds, "references": refs,
            "n_entries": len(entries)}


def op_formation(ctx):
    p = ctx.params
    tgt_paths = stepio.inputs_matching(ctx, "compound")
    if not tgt_paths:
        avail = ", ".join(sorted(ctx.named_inputs)) or "none"
        raise WorkerError(f"stability.formation: missing input 'compound' (have: {avail})")
    targets = [stepio.parse_energy(stepio.load_json_path(pth, n), n)
               for n, pth in tgt_paths.items()]
    others = [stepio.parse_energy(stepio.load_json_path(pth, n), n)
              for n, pth in sorted(ctx.named_inputs.items()) if n not in tgt_paths]
    for i, x in enumerate(p.get("extra_entries") or []):
        others.append(stepio.parse_energy(x, f"extra_entries[{i}]"))
    engine = stepio.check_engines([d["engine"] for d in targets + others],
                                  "stability.formation")
    try:
        doc = formation_and_hull(targets, others)
    except ValueError as e:
        raise WorkerError(f"stability.formation: {e}") from None
    doc["engine"] = engine
    ref = ctx.save_output("stability", doc)
    method = {"solver": "pymatgen.PhaseDiagram", "engine": engine or "unknown",
              "preset": str(p.get("preset", "")),
              "settings": {"n_entries": doc["n_entries"], "references": doc["references"]}}
    try:
        from importlib.metadata import version
        method["solver_version"] = version("pymatgen")
    except Exception:  # noqa: BLE001
        pass
    for d, c in zip(targets, doc["compounds"]):
        if d["structure"] is None:
            _no_structure(d)
        ctx.record("stability.formation", structure=d["structure"],
                   properties={"formation_ev_per_atom": c["formation_ev_per_atom"],
                               "e_above_hull_ev_per_atom": c["e_above_hull_ev_per_atom"],
                               "on_hull": c["on_hull"],
                               "hull_phases": doc["hull_phases"]},
                   conditions={"temperature_k": 0, "pressure_gpa": 0},
                   method=method, outputs=[ref],
                   note="hull vs the elements and compounds given to this step only")
    return None


def _no_structure(d: dict):
    raise WorkerError(f"stability.formation: target {d['formula']} has no structure "
                      "(the record needs one): give qe.relax's `energy` JSON with an "
                      "embedded `structure`, or the relaxed Structure JSON with "
                      "energy_ev_per_atom")


def register(worker) -> None:
    worker.task("stability.formation")(op_formation)


__all__ = ["born_cubic", "born_from_elastic", "formation_and_hull", "register"]
