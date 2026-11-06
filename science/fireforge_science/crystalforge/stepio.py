"""Small helpers shared by the Phase 8 steps owned by crystalforge, transportforge,
stabilityforge, evaluate and validate: reading named JSON inputs defensively, energies
from qe.relax outputs, spawn resources.

Energy inputs (stability.formation, crystal.select) are accepted in two forms:

- an `energy` JSON (qe.relax output): {"energy_ev", "natoms", "energy_ev_per_atom",
  "formula" | "reduced_formula" | "species" | "structure", "engine"} — at least
  energy_ev_per_atom (or energy_ev + natoms) and something giving the composition;
- a Structure JSON carrying an extra `energy_ev_per_atom` (or `energy_ev`) field.
"""
from __future__ import annotations

import json
import math
import re
from pathlib import Path
from typing import Any

from fireforge import WorkerError

from ..model import Structure


def load_json_path(path: Path, what: str) -> Any:
    try:
        return json.loads(Path(path).read_text())
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as e:
        raise WorkerError(f"input {what!r}: not readable JSON ({e})") from None


def input_json(ctx, name: str, *, required: bool = True) -> Any:
    """Parsed JSON of the named input; WorkerError naming it if missing (required)."""
    p = ctx.named_inputs.get(name)
    if p is None:
        if required:
            avail = ", ".join(sorted(ctx.named_inputs)) or "none"
            raise WorkerError(f"{ctx.operation}: missing input {name!r} (have: {avail})")
        return None
    return load_json_path(p, name)


def inputs_matching(ctx, prefix: str) -> dict[str, Path]:
    """Named inputs whose name is `prefix`, starts with `prefix.` or is `prefix#<n>`
    (workflow name-collision suffix), sorted by name."""
    pat = re.compile(rf"^{re.escape(prefix)}(?:[.#].*)?$")
    return {n: p for n, p in sorted(ctx.named_inputs.items()) if pat.match(n)}


def require_key(d: dict, key: str, what: str):
    """d[key]; WorkerError naming the key and the input otherwise."""
    if not isinstance(d, dict) or key not in d or d[key] is None:
        raise WorkerError(f"input {what!r}: missing key {key!r}")
    return d[key]


def finite(x) -> float | None:
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return v if math.isfinite(v) else None


# ---------------------------------------------------------------------------------------
# energies

def parse_energy(doc: Any, what: str) -> dict[str, Any]:
    """Normalize an energy JSON or a Structure JSON with an energy into
    {"composition": {el: count}, "energy_ev_per_atom", "natoms", "engine", "formula",
     "structure": Structure | None}. WorkerError naming the missing field otherwise."""
    if not isinstance(doc, dict):
        raise WorkerError(f"input {what!r}: expected a JSON object")
    structure = None
    sdoc = doc.get("structure") if isinstance(doc.get("structure"), dict) else None
    if sdoc is None and "lattice" in doc and "sites" in doc:
        sdoc = doc
    if sdoc is not None:
        try:
            structure = Structure.from_json(sdoc)
        except (KeyError, ValueError, TypeError) as e:
            raise WorkerError(f"input {what!r}: bad structure ({e})") from None

    comp: dict[str, float] = {}
    if structure is not None:
        for s in structure.species:
            comp[s] = comp.get(s, 0) + 1
    elif isinstance(doc.get("species"), list):
        for s in doc["species"]:
            comp[str(s)] = comp.get(str(s), 0) + 1
    elif doc.get("formula") or doc.get("reduced_formula"):
        from pymatgen.core import Composition
        try:
            c = Composition(doc.get("formula") or doc.get("reduced_formula"))
        except ValueError as e:
            raise WorkerError(f"input {what!r}: bad formula ({e})") from None
        comp = {str(el): float(n) for el, n in c.items()}
    else:
        raise WorkerError(f"input {what!r}: no composition (need 'formula', 'species' or "
                          "'structure')")
    natoms = finite(doc.get("natoms")) or float(sum(comp.values()))

    epa = finite(doc.get("energy_ev_per_atom"))
    if epa is None:
        e = finite(doc.get("energy_ev"))
        if e is None:
            raise WorkerError(f"input {what!r}: missing key 'energy_ev_per_atom' "
                              "(or 'energy_ev' + 'natoms')")
        epa = e / natoms
    engine = doc.get("engine") or (doc.get("method") or {}).get("engine") or ""
    from pymatgen.core import Composition
    formula = Composition(comp).reduced_formula
    return {"composition": comp, "energy_ev_per_atom": epa, "natoms": natoms,
            "engine": str(engine), "formula": formula, "structure": structure}


def check_engines(engines, what: str) -> str:
    """The single engine among `engines` (blank ones ignored); WorkerError if they mix."""
    es = sorted({e for e in engines if e})
    if len(es) > 1:
        raise WorkerError(f"{what}: inputs come from different engines ({', '.join(es)}); "
                          "toy and real data are never mixed")
    return es[0] if es else ""


# ---------------------------------------------------------------------------------------
# spawning

def qe_step_kwargs(engine: str, cpus: int, extra_caps=()) -> dict[str, Any]:
    """resources + capabilities for a spawned solver step (phase8.md §5)."""
    caps = ["sci"] + (["qe"] if engine == "qe" else []) + list(extra_caps)
    from ..workflows import solver_resources
    return {"resources": solver_resources(engine, cpus), "capabilities": caps}


def light_step_kwargs(extra_caps=()) -> dict[str, Any]:
    from ..workflows import light_resources
    return {"resources": light_resources(), "capabilities": ["sci", *extra_caps]}


def spawn_specs(ctx, specs) -> list[str]:
    """Spawn a list of steps given as context.spawn keyword dicts (with "name") or as
    job_pb2.StepSpec protos (appended as built). Returns the names."""
    names = []
    for s in specs:
        if isinstance(s, dict):
            kw = dict(s)
            name = kw.pop("name")
            names.append(ctx.spawn(name, **kw))
        elif hasattr(s, "job") and hasattr(s, "name"):
            if any(x.name == s.name for x in ctx._spawned):
                raise WorkerError(f"spawn({s.name!r}): duplicate spawned step name")
            ctx._spawned.append(s)
            names.append(s.name)
        else:
            raise WorkerError(f"cannot spawn {s!r} (want a spawn kwargs dict or StepSpec)")
    return names


# ---------------------------------------------------------------------------------------
# in-process execution (tests, local debugging)

OWN_MODULES = ("crystalforge", "transportforge", "stabilityforge", "evaluate", "validate")


def run_task(operation: str, params: dict | None = None, inputs: dict | None = None, *,
             workdir, workflow_id: str = "wf-local", step: str = "step"):
    """Run one registered science operation in-process, through the SDK's result
    assembly (Worker._execute), without a daemon. `inputs`: {name: path}. Only the operations of
    OWN_MODULES are registered (independent of the solver modules). Returns
    (WorkerResult, Context, outputs {name: Path}). Spawns and records are in
    result.spawn / result.records."""
    from pathlib import Path as _P
    from fireforge.tdr.v1 import local_pb2
    import importlib
    from fireforge import Worker
    from fireforge.worker import Context

    workdir = _P(workdir)
    (workdir / "outputs").mkdir(parents=True, exist_ok=True)
    (workdir / "checkpoints").mkdir(parents=True, exist_ok=True)
    task = local_pb2.WorkerTask(job_id="job-local", attempt_id="att-local", attempt_number=1,
                                operation=operation,
                                parameters_json=json.dumps(params or {}),
                                output_dir=str(workdir / "outputs"),
                                checkpoint_dir=str(workdir / "checkpoints"),
                                workflow_id=workflow_id, step=step)
    for name, path in (inputs or {}).items():
        task.inputs.add(artifact_id=f"artifact://local/{name}", path=str(path), name=name)
    ctx = Context(task, {"FIREFORGE_WORKDIR": str(workdir)}, None, background_flush=False)
    worker = Worker()
    for mod in OWN_MODULES:
        importlib.import_module(f"fireforge_science.{mod}").register(worker)
    res = worker._execute(ctx, operation)
    outs = {o.name: workdir / "outputs" / o.path for o in res.outputs}
    return res, ctx, outs
