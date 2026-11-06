"""Workflow builders (docs/design/phase8.md §5).

- `material_pipeline_steps(structure, prefix, params)`: the per-material steps after a
  relaxed structure exists (scf → nscf → transport, phonons, elastic, evaluate). Returns
  `context.spawn` keyword dicts (crystal.select spawns them) that `add_steps` also adds to
  a static `fireforge.Workflow`.
- `validation_workflow(...)`: elemental references, the five half-Heusler validation
  compounds in their known ordering, formation energies, evaluation, `te.validate`.
- `halfheusler_campaign(x, y, z, ...)`: `crystal.halfheusler` enumerates candidates, relaxes
  every ordering and `crystal.select` spawns the pipeline of the lowest-energy ordering.

Steps run `argv` (default `["fireforge-sci"]`, the node's science worker wrapper: see
tools/sci-setup.sh) and dispatch on `operation`. Step names inside a spawn group refer
to siblings by name and to the spawning step as ".". Static workflows prefix every
material step with `<formula>.` so names stay unique.
"""
from __future__ import annotations

from typing import Any, Iterable

from fireforge import StepInput, Workflow

DEFAULT_ARGV = ["fireforge-sci"]

VALIDATION_SET = {          # formula: (X, Y, Z), Y (late transition metal) on 4c
    "ZrNiSn": ("Zr", "Ni", "Sn"),
    "HfNiSn": ("Hf", "Ni", "Sn"),
    "TiNiSn": ("Ti", "Ni", "Sn"),
    "NbFeSb": ("Nb", "Fe", "Sb"),
    "ZrCoSb": ("Zr", "Co", "Sb"),
}
TIER2_FORCED = ("ZrNiSn",)  # phono3py tier in the validation workflow (phase8.md §5)

MIB = 1024 * 1024
QE_MEMORY_MB = {"screening": 6144, "accurate": 12288}   # per QE step (24-atom supercells)
LIGHT_MEMORY_MB = 2048
DAY = 24 * 3600


def _qe(engine: str, cpus: int, preset: str, extra_caps: Iterable[str] = ()) -> dict[str, Any]:
    caps = ["sci"] + (["qe"] if engine == "qe" else []) + list(extra_caps)
    mem = QE_MEMORY_MB.get(preset, QE_MEMORY_MB["screening"]) if engine == "qe" else LIGHT_MEMORY_MB
    return {"resources": {"cpu_cores": max(1, int(cpus)), "memory_bytes": mem * MIB},
            "capabilities": caps, "timeout": DAY, "max_attempts": 2}


def _light(extra_caps: Iterable[str] = (), cpus: int = 1, memory_mb: int = LIGHT_MEMORY_MB):
    return {"resources": {"cpu_cores": cpus, "memory_bytes": memory_mb * MIB},
            "capabilities": ["sci", *extra_caps], "timeout": DAY, "max_attempts": 2}


def solver_resources(engine: str, cpus: int, preset: str = "screening") -> dict[str, Any]:
    """`resources` for a solver step (QE): cores + memory per preset (toy: small)."""
    return _qe(engine, cpus, preset)["resources"]


def light_resources(memory_mb: int = LIGHT_MEMORY_MB, cpus: int = 1) -> dict[str, Any]:
    """`resources` for a pure-Python step (memory also bounds its address space when
    the node runs without cgroups, so never leave it to a small node default)."""
    return {"cpu_cores": cpus, "memory_bytes": memory_mb * MIB}


def _common(params: dict) -> dict[str, Any]:
    return {"engine": params.get("engine", "qe"), "preset": params.get("preset", "screening"),
            "cpus": int(params.get("cpus", 4))}


def material_pipeline_steps(structure: StepInput, prefix: str, params: dict | None = None,
                            *, extra_inputs: dict[str, list[StepInput]] | None = None
                            ) -> list[dict[str, Any]]:
    """Steps for one relaxed structure. `structure`: where the relaxed structure comes from
    (as_name "structure"). `params`: engine, preset, cpus, plus optional `tier2`
    (auto|force|off), `thresholds`, `tau_s`, `volume_strains`, `phonon` (extra
    phonon.harmonic params), `tier2_params`, `argv`. `extra_inputs`: per local step
    ("evaluate", ...) extra StepInputs, e.g. the stability result of a static workflow."""
    p = dict(params or {})
    c = _common(p)
    argv = list(p.get("argv") or DEFAULT_ARGV)
    eng, cpus, preset = c["engine"], c["cpus"], c["preset"]
    n = lambda s: f"{prefix}.{s}" if prefix else s  # noqa: E731
    st = StepInput(structure.step, structure.output, structure.spawned, "structure")
    extra = extra_inputs or {}

    def step(local: str, operation: str, inputs: list, res: dict, prm: dict | None = None,
             **kw) -> dict[str, Any]:
        d = {"name": n(local), "operation": operation, "argv": argv,
             "params": {**c, **(prm or {})}, "inputs": inputs + list(extra.get(local, ())),
             **res, **kw}
        return d

    phonon_params = {"volume_strains": p.get("volume_strains", [-0.02, 0.0, 0.02]),
                     **(p.get("phonon") or {})}
    evaluate_params = {k: p[k] for k in ("tier2", "thresholds", "tau_s", "tier2_params")
                       if k in p}
    if not any(i.as_name == "stability" for i in extra.get("evaluate", ())):
        # No formation step in a spawned pipeline: don't fail the hull criterion on it.
        th = dict(evaluate_params.get("thresholds") or {})
        th.setdefault("e_above_hull_max_ev", None)
        evaluate_params["thresholds"] = th
    return [
        step("scf", "qe.scf", [st], _qe(eng, cpus, preset)),
        step("nscf", "qe.nscf", [st, StepInput(n("scf"), "save", as_name="save")],
             _qe(eng, cpus, preset)),
        step("transport", "transport.boltztrap",
             [StepInput(n("nscf"), "bands", as_name="bands"), st],
             _light(["boltztrap2"], cpus=2, memory_mb=4096 if eng == "qe" else LIGHT_MEMORY_MB),
             {"cpus": 2}),
        step("phonon", "phonon.harmonic", [st], _light(["phonopy"]), phonon_params),
        step("elastic", "qe.elastic", [st], _qe(eng, cpus, preset), checkpoint=True),
        step("evaluate", "te.evaluate",
             [StepInput(n("transport"), "transport", as_name="transport"),
              StepInput(n("phonon"), "phonon", spawned=True, as_name="phonon"),
              StepInput(n("phonon"), "fc2", spawned=True, as_name="fc2"),
              StepInput(n("elastic"), "elastic", as_name="elastic"),
              StepInput(n("nscf"), "bands", as_name="bands"), st],
             _light(), evaluate_params),
    ]


def add_steps(wf: Workflow, specs: Iterable[dict[str, Any]]) -> list[str]:
    """Add `material_pipeline_steps`-style dicts to a static workflow."""
    names = []
    for d in specs:
        kw = dict(d)
        names.append(wf.step(kw.pop("name"), **kw))
    return names


def validation_workflow(*, engine: str = "qe", preset: str = "screening", cpus: int = 4,
                        compounds: Iterable[str] | None = None, tier2: bool = True,
                        campaign_id: str | None = None, argv: list[str] | None = None,
                        allow_toy: bool | None = None, volume_strains=None) -> Workflow:
    """phase8.md §5 validation_workflow. `compounds`: subset of VALIDATION_SET."""
    argv = list(argv or DEFAULT_ARGV)
    names = list(compounds or VALIDATION_SET)
    unknown = [c for c in names if c not in VALIDATION_SET]
    if unknown:
        raise ValueError(f"not in the validation set: {', '.join(unknown)}")
    common = {"engine": engine, "preset": preset, "cpus": cpus}
    wf = Workflow(f"te-validation-{engine}-{preset}", campaign_id, argv=argv)

    elements = sorted({e for c in names for e in VALIDATION_SET[c]})
    for el in elements:
        wf.step(f"ref.{el}", operation="crystal.reference", params={**common, "element": el},
                **_light())
        wf.step(f"refrelax.{el}", operation="qe.relax", params=common,
                inputs=[StepInput(f"ref.{el}", "structure", as_name="structure")],
                **_qe(engine, cpus, preset))

    for comp in names:
        x, y, z = VALIDATION_SET[comp]
        wf.step(f"{comp}.build", operation="crystal.halfheusler",
                params={**common, "x": [x], "y": [y], "z": [z], "orderings": "known",
                        "spawn": False}, **_light())
        wf.step(f"{comp}.relax", operation="qe.relax", params=common,
                inputs=[StepInput(f"{comp}.build", f"cand.{comp}.y4c", as_name="structure")],
                **_qe(engine, cpus, preset))

    for comp in names:
        hull = [StepInput(f"{comp}.relax", "energy", as_name="compound")]
        hull += [StepInput(f"refrelax.{el}", "energy", as_name=f"ref.{el}") for el in elements]
        hull += [StepInput(f"{o}.relax", "energy", as_name=f"other.{o}")
                 for o in names if o != comp]
        wf.step(f"{comp}.formation", operation="stability.formation", params=common,
                inputs=hull, **_light())
        pp = {**common, "tier2": "force" if (tier2 and comp in TIER2_FORCED) else "off"}
        if volume_strains is not None:
            pp["volume_strains"] = list(volume_strains)
        add_steps(wf, material_pipeline_steps(
            StepInput(f"{comp}.relax", "structure"), comp, {**pp, "argv": argv},
            extra_inputs={"evaluate": [StepInput(f"{comp}.formation", "stability",
                                                 as_name="stability")]}))

    vp = {"allow_toy": engine == "toy" if allow_toy is None else bool(allow_toy)}
    wf.step("validate", operation="te.validate", params=vp, run_if="always",
            inputs=[StepInput(f"{c}.evaluate", "evaluation", spawned=True) for c in names],
            **_light())
    return wf


def halfheusler_campaign(x: list[str], y: list[str], z: list[str], *, engine: str = "qe",
                         preset: str = "screening", cpus: int = 4, orderings: str = "all",
                         electron_counts=(8, 18), campaign_id: str | None = None,
                         argv: list[str] | None = None, pipeline_params: dict | None = None
                         ) -> Workflow:
    """phase8.md §5 halfheusler_campaign: enumerate → relax orderings → select → pipeline."""
    argv = list(argv or DEFAULT_ARGV)
    wf = Workflow(f"halfheusler-{engine}-{preset}", campaign_id, argv=argv)
    wf.step("enumerate", operation="crystal.halfheusler",
            params={"x": list(x), "y": list(y), "z": list(z), "engine": engine, "cpus": cpus,
                    "relax_preset": "screening", "preset": preset, "orderings": orderings,
                    "electron_counts": list(electron_counts), "pipeline": True,
                    "pipeline_params": {"argv": argv, **(pipeline_params or {})}},
            **_light())
    return wf
