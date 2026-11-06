"""CrystalForge: half-Heusler enumeration, elemental references, ordering selection
(phase8.md §4, §5).

Operations:

`crystal.halfheusler` — params `x`, `y`, `z` (element lists), `electron_counts` [8, 18],
  `orderings` "all" | "known", `engine` ("qe"), `cpus` (2), `relax_preset` ("screening"),
  `pipeline` (false), `preset` (pipeline preset, "screening"), `pipeline_params` ({}),
  `spawn` (default: true inside a workflow). Outputs `candidates` (JSON list, see
  halfheusler.Candidate.to_json) and one structure JSON per candidate,
  `cand.<composition>.<ordering>` (e.g. `cand.ZrNiSn.y4c`). Spawns, per candidate,
  `relax.<composition>.<ordering>` (qe.relax, input `structure` = `.`:`cand.…`,
  allow_failure) and per composition `select.<composition>` (crystal.select, run_if
  ALWAYS, depends on its relaxes, inputs `structure.<ordering>` / `energy.<ordering>`
  from each relax's `structure` / `energy` outputs).

`crystal.select` — inputs `structure.<o>` + `energy.<o>` (any number of orderings;
  missing pairs, e.g. a failed relax, are skipped). Keeps the lowest energy per atom.
  Outputs `structure` (the chosen relaxed structure), `energy` (its energy JSON) and
  `selection` (all orderings with energies, ΔE in meV/atom). With `pipeline: true` it
  spawns `workflows.material_pipeline_steps(StepInput(".", "structure",
  as_name="structure"), <composition>, params)` if that builder is importable.

`crystal.reference` — param `element`; output `structure` (reference.REFERENCES).
"""
from __future__ import annotations

from typing import Any

from fireforge import StepInput, WorkerError

from ..model import Structure
from . import halfheusler, reference, stepio
from .halfheusler import Candidate, enumerate_halfheuslers, valence_electrons, vec
from .reference import REFERENCES, reference_structure

__all__ = ["Candidate", "REFERENCES", "enumerate_halfheuslers", "reference_structure",
           "valence_electrons", "vec", "register", "plan_spawns"]

MAX_SPAWN = 1000


def _sssp_elements(engine: str) -> set[str] | None:
    """Elements of the SSSP table; None (no check) for the toy engine without a table."""
    from .. import pseudo
    try:
        return set(pseudo.sssp_table())
    except FileNotFoundError as e:
        if engine == "toy":
            return None
        raise WorkerError(f"cannot validate elements against SSSP: {e}") from None


def plan_spawns(cands: list[Candidate], *, engine: str = "qe", cpus: int = 2,
                relax_preset: str = "screening", pipeline: bool = False,
                preset: str = "screening", pipeline_params: dict | None = None
                ) -> list[dict[str, Any]]:
    """context.spawn keyword dicts (with "name") for the candidates: relax per candidate,
    select per composition (phase6 §5 naming: siblings by short name, "." = this step)."""
    specs: list[dict[str, Any]] = []
    by_comp: dict[str, list[Candidate]] = {}
    for c in cands:
        by_comp.setdefault(c.composition, []).append(c)
    for comp, group in by_comp.items():
        relax_names = []
        for c in group:
            name = f"relax.{c.id}"
            relax_names.append((name, c.ordering))
            specs.append({"name": name, "operation": "qe.relax",
                          "params": {"engine": engine, "preset": relax_preset,
                                     "cpus": cpus, "spin": "auto"},
                          "inputs": [StepInput(".", f"cand.{c.id}", as_name="structure")],
                          "allow_failure": True,
                          **stepio.qe_step_kwargs(engine, cpus)})
        inputs = []
        for name, o in relax_names:
            inputs.append(StepInput(name, "structure", as_name=f"structure.{o}"))
            inputs.append(StepInput(name, "energy", as_name=f"energy.{o}"))
        specs.append({"name": f"select.{comp}", "operation": "crystal.select",
                      "params": {"composition": comp, "engine": engine, "cpus": cpus,
                                 "preset": preset, "pipeline": bool(pipeline),
                                 "pipeline_params": dict(pipeline_params or {})},
                      "inputs": inputs, "depends_on": [n for n, _ in relax_names],
                      "run_if": "always", **stepio.light_step_kwargs()})
    if len(specs) > MAX_SPAWN:
        raise WorkerError(f"{len(specs)} steps to spawn exceed {MAX_SPAWN} per expansion; "
                          "split the element lists over several crystal.halfheusler steps")
    return specs


def op_halfheusler(ctx):
    p = ctx.params
    engine = p.get("engine", "qe")
    try:
        cands = enumerate_halfheuslers(
            p.get("x"), p.get("y"), p.get("z"),
            electron_counts=p.get("electron_counts", [8, 18]),
            orderings=p.get("orderings", "all"),
            allowed_elements=_sssp_elements(engine))
    except ValueError as e:
        raise WorkerError(f"crystal.halfheusler: {e}") from None
    if not cands:
        raise WorkerError("crystal.halfheusler: no candidate passes the electron-count filter")
    ctx.save_output("candidates", [c.to_json() for c in cands])
    for c in cands:
        ctx.save_output(f"cand.{c.id}", c.structure.to_json())
    do_spawn = p.get("spawn")
    if do_spawn is None:
        do_spawn = bool(ctx.workflow_id)
    if do_spawn:
        specs = plan_spawns(cands, engine=engine, cpus=int(p.get("cpus", 2)),
                            relax_preset=p.get("relax_preset", "screening"),
                            pipeline=bool(p.get("pipeline", False)),
                            preset=p.get("preset", "screening"),
                            pipeline_params=p.get("pipeline_params"))
        stepio.spawn_specs(ctx, specs)
    return None


# ---------------------------------------------------------------------------------------

def collect_orderings(ctx) -> list[dict[str, Any]]:
    """[{ordering, structure, energy_doc, energy_ev_per_atom, engine}] from the
    `structure.<o>` / `energy.<o>` inputs (pairs with a missing half are skipped)."""
    out = []
    for name, path in stepio.inputs_matching(ctx, "energy").items():
        sfx = name[len("energy"):]
        spath = ctx.named_inputs.get("structure" + sfx)
        edoc = stepio.load_json_path(path, name)
        e = stepio.parse_energy(edoc, name)
        if spath is not None:
            s = Structure.from_json(stepio.load_json_path(spath, "structure" + sfx))
        elif e["structure"] is not None:
            s = e["structure"]
        else:
            continue
        out.append({"ordering": sfx.lstrip(".#") or "default", "structure": s,
                    "energy_doc": edoc, "energy_ev_per_atom": e["energy_ev_per_atom"],
                    "engine": e["engine"]})
    return out


def op_select(ctx):
    p = ctx.params
    found = collect_orderings(ctx)
    if not found:
        avail = ", ".join(sorted(ctx.named_inputs)) or "none"
        raise WorkerError(f"crystal.select: no relaxed ordering among the inputs "
                          f"(need structure.<o> + energy.<o>; have: {avail})")
    engine = stepio.check_engines([f["engine"] for f in found], "crystal.select")
    found.sort(key=lambda f: f["energy_ev_per_atom"])
    best = found[0]
    comp = p.get("composition") or best["structure"].reduced_formula()
    sel = {"composition": comp, "chosen": best["ordering"], "engine": engine,
           "orderings": [{"ordering": f["ordering"],
                          "energy_ev_per_atom": f["energy_ev_per_atom"],
                          "delta_mev_per_atom": 1000.0 * (f["energy_ev_per_atom"]
                                                          - best["energy_ev_per_atom"]),
                          "volume_a3_per_atom": f["structure"].volume() / f["structure"].nsites,
                          "spacegroup_number": f["structure"].spacegroup()[0]}
                         for f in found]}
    chosen = best["structure"]
    if not chosen.prototype:
        chosen = chosen.copy(prototype=halfheusler.PROTOTYPE)
    ctx.save_output("structure", chosen.to_json())
    ctx.save_output("energy", best["energy_doc"])
    ctx.save_output("selection", sel)
    if p.get("pipeline"):
        builder = _pipeline_builder()
        if builder is None:
            sel["pipeline"] = "workflows.material_pipeline_steps not available; not spawned"
            ctx.save_output("selection", sel)
        else:
            params = {"engine": engine or p.get("engine", "qe"), "cpus": p.get("cpus", 2),
                      "preset": p.get("preset", "screening"), **(p.get("pipeline_params") or {})}
            specs = builder(StepInput(".", "structure", as_name="structure"), comp, params)
            stepio.spawn_specs(ctx, specs)
    return None


def _pipeline_builder():
    try:
        from .. import workflows
    except ImportError:
        return None
    return getattr(workflows, "material_pipeline_steps", None)


def op_reference(ctx):
    el = ctx.params.get("element")
    if not el:
        raise WorkerError("crystal.reference: parameter 'element' is required")
    try:
        s = reference_structure(str(el))
    except KeyError as e:
        raise WorkerError(f"crystal.reference: {e.args[0]}") from None
    ctx.save_output("structure", s.to_json())
    return None


def register(worker) -> None:
    worker.task("crystal.halfheusler")(op_halfheusler)
    worker.task("crystal.select")(op_select)
    worker.task("crystal.reference")(op_reference)
