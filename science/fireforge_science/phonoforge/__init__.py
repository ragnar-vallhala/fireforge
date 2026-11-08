"""PhonoForge: harmonic + quasi-harmonic phonons, Slack κ_l, phono3py tier
(docs/design/phase8.md §4, §5).

Operations and their step wiring (phase6.md §5 spawn conventions):

phonon.harmonic  inputs `structure`; params `supercell` (int, 3 ints or 3×3; default
    2×2×2 of the primitive cell), `volume_strains` ([-0.02, 0, 0.02]; [0] = no QHA),
    `amplitude` (0.01 Å), `engine`, `preset`, `cpus` (2), and `mesh`, `t_k`,
    `gruneisen` passed to collect. Outputs `plan` (JSON) and one
    `supercell.<label>` (Structure JSON) per displaced supercell. Spawns, per supercell,
    step `<label>` = `v<i>.d<nnn>` (volume index in volume_strains, 1-based displacement):
        qe.forces, params {engine, preset, cpus, label}, resources cpu_cores = cpus,
        inputs [{step ".", output "supercell.<label>", input_name "supercell"}]
    and step `collect`:
        phonon.collect, inputs [{step ".", output "plan", input_name "plan"}] +
        [{step "<label>", output "forces", input_name "forces.<label>"} per sibling].
phonon.collect  inputs `plan`, `forces.<label>`...; params `mesh` (20), `t_k`
    ([300, 500, 800, 1100]), `gruneisen` (only used without QHA). Outputs `phonon`
    (JSON), `fc2` (phonopy yaml with force constants, reference volume; phonopy.load()).
    Records phonon.harmonic and phonon.slack.
phonon.fc3  inputs `structure`, `fc2`; params `supercell` (2×2×2), `cutoff_pair_a`
    (4.0 Å; null = all pairs), `amplitude` (0.03 Å), engine/preset/cpus, `mesh`, `t_k`
    for kappa3. Output `plan3` + `supercell.<label>`. Spawns `d<nnnn>` (qe.forces, as
    above) and `kappa3` with inputs plan3 from ".", `forces.<label>` from the siblings
    and the fc2 artifact (input_artifacts {"fc2": <id of this step's fc2 input>}).
phonon.kappa3  inputs `plan3`, `fc2`, `forces.<label>`...; params `mesh` (11), `t_k`.
    Output `kappa3` (JSON); record phonon.bte.
"""
from __future__ import annotations

import json
import math
import tempfile
from pathlib import Path

from ..model import Structure, load_structure
from ..workflows import light_resources, solver_resources
from .common import (FORCES_INPUT_PREFIX, PhononError, collect_forces, common_params,
                     first_provenance, forces_capabilities, mesh_numbers, method,
                     supercell_matrix, temperatures)

OPERATIONS = ("phonon.harmonic", "phonon.collect", "phonon.fc3", "phonon.kappa3")
MAX_SPAWN = 1000


def _worker_error(msg: str):
    from fireforge import WorkerError
    return WorkerError(msg, retryable=False)


def _input(ctx, name: str) -> Path:
    try:
        return ctx.input(name)
    except KeyError as exc:
        raise _worker_error(str(exc)) from None


def _finite(d: dict) -> dict:
    out = {}
    for k, v in d.items():
        if v is None or (isinstance(v, float) and not math.isfinite(v)):
            continue
        out[k] = v
    return out


def _t_key(t: float) -> str:
    return f"kappa_l_w_mk_{t:g}k"


def forces_spin(p: dict, s) -> dict:
    """Spin setting for spawned qe.forces steps. Displaced supercells carry no magmoms, so a
    non-magnetic parent structure is passed on as spin "off"."""
    if p.get("spin") is not None:
        return {"spin": p["spin"]}
    return {"spin": "off"} if s.known_nonmagnetic else {}


def _spawn_forces(ctx, label: str, cp: dict) -> None:
    from fireforge.workflow import StepInput
    name = f"supercell.{label}"
    ctx.spawn(label, operation="qe.forces", params={**cp, "label": label},
              inputs=[StepInput(".", name, as_name="supercell")],
              resources=solver_resources(cp["engine"], cp["cpus"], cp.get("preset", "screening")),
              capabilities=forces_capabilities(cp["engine"]))


def _forces_inputs(labels: list[str]):
    from fireforge.workflow import StepInput
    return [StepInput(lb, "forces", as_name=FORCES_INPUT_PREFIX + lb) for lb in labels]


def _save_supercells(ctx, supercells: list[dict]) -> None:
    for sc in supercells:
        ctx.save_output(f"supercell.{sc['label']}", sc["structure"],
                        filename=f"supercells/{sc['label']}.json")


# ---- operations -------------------------------------------------------------------------

def phonon_harmonic(ctx):
    from fireforge.workflow import StepInput
    from .harmonic import DEFAULT_STRAINS, build_plan, plan_labels
    p = ctx.params
    try:
        cp = common_params(p)
        s = load_structure(_input(ctx, "structure"))
        cp.update(forces_spin(p, s))
        plan = build_plan(s, supercell=supercell_matrix(p.get("supercell")),
                          volume_strains=p.get("volume_strains", DEFAULT_STRAINS),
                          amplitude=float(p.get("amplitude", 0.01)),
                          engine=cp["engine"], preset=cp["preset"])
    except PhononError as exc:
        raise _worker_error(str(exc)) from None
    labels = plan_labels(plan)
    if len(labels) + 1 > MAX_SPAWN:
        raise _worker_error(f"{len(labels)} displaced supercells exceed the spawn limit")
    ctx.save_output("plan", plan)
    for v in plan["volumes"]:
        _save_supercells(ctx, v["supercells"])
    for lb in labels:
        _spawn_forces(ctx, lb, cp)
    cparams = {"engine": cp["engine"], "preset": cp["preset"],
               **{k: p[k] for k in ("mesh", "t_k", "gruneisen") if p.get(k) is not None}}
    ctx.spawn("collect", operation="phonon.collect", params=cparams,
              inputs=[StepInput(".", "plan", as_name="plan"), *_forces_inputs(labels)],
              resources=light_resources(4096), capabilities=["sci", "phonopy"])
    return None


def phonon_collect(ctx):
    import phonopy
    from .harmonic import analyze
    p = ctx.params
    try:
        plan = json.loads(_input(ctx, "plan").read_text())
        forces = collect_forces(ctx)
        mesh = mesh_numbers(p.get("mesh"), 20)
        t_k = temperatures(p.get("t_k"))
        g = p.get("gruneisen")
        phonon, ph = analyze(plan, forces, mesh=mesh, t_k=t_k,
                             gruneisen_param=None if g is None else float(g))
    except PhononError as exc:
        raise _worker_error(str(exc)) from None
    ctx.save_output("phonon", phonon)
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "fc2.yaml"
        ph.save(str(path), settings={"force_constants": True})
        ctx.save_output("fc2", path, media_type="application/yaml", filename="fc2.yaml")

    engine, preset = plan.get("engine", ""), plan.get("preset", "")
    m = method("phonopy", phonopy.__version__, engine, preset,
               {"supercell": plan["supercell_matrix"], "amplitude_a": plan["amplitude_a"],
                "volume_strains": plan["volume_strains"], "mesh": mesh,
                "gruneisen": "dD/dlnV over volumes, acoustic RMS"},
               first_provenance(forces))
    structure = Structure.from_json(plan["structure"])
    det = phonon["gruneisen_detail"]
    props = _finite({
        "min_freq_thz": phonon["min_freq_thz"], "dynamically_stable": phonon["dynamically_stable"],
        "max_freq_thz": phonon["max_freq_thz"],
        "theta_d_k": phonon["theta_d_k"], "theta_d_debye_fit_k": phonon["theta_d_debye_fit_k"],
        "theta_d_sound_k": phonon["theta_d_sound_k"], "theta_a_k": phonon["theta_a_k"],
        "gruneisen": phonon["gruneisen"],
        "gruneisen_acoustic_mean": det.get("gruneisen_acoustic_mean"),
        "gruneisen_cv_300k": (det.get("gruneisen_cv") or {}).get("300"),
        "v_s_m_s": phonon["v_s_m_s"], "v_l_m_s": phonon["v_l_m_s"], "v_t_m_s": phonon["v_t_m_s"],
    })
    conds = {"volume_strains": plan["volume_strains"],
             "reference_strain": plan["volume_strains"][plan["reference"]], "mesh": mesh}
    ctx.record("phonon.harmonic", structure=structure, properties=props, conditions=conds,
               method=m, outputs=["phonon", "fc2"])
    sl = phonon["slack"]
    if sl["kappa_l_w_mk"] is not None:
        sprops = {_t_key(t): k for t, k in zip(t_k, sl["kappa_l_w_mk"])}
        sprops.update(kappa_l_times_t_w_m=sl["kappa_l_times_t_w_m"], gruneisen=sl["gamma"],
                      theta_a_k=sl["theta_a_k"], theta_d_k=phonon["theta_d_k"])
        ctx.record("phonon.slack", structure=structure, properties=_finite(sprops),
                   conditions={"t_k": t_k, "gruneisen_source": det.get("source")},
                   method=m, outputs=["phonon"],
                   uncertainty={"factor": 2, "model": "slack"})
    return None


def phonon_fc3(ctx):
    from fireforge.workflow import StepInput
    from .bte import build_plan3, load_fc2
    p = ctx.params
    try:
        cp = common_params(p)
        s = load_structure(_input(ctx, "structure"))
        cp.update(forces_spin(p, s))
        fc2_path = _input(ctx, "fc2")
        cutoff = p.get("cutoff_pair_a", 4.0)
        plan3 = build_plan3(s, load_fc2(fc2_path), supercell=supercell_matrix(p.get("supercell")),
                            amplitude=float(p.get("amplitude", 0.03)),
                            cutoff_pair_a=None if cutoff is None else float(cutoff),
                            engine=cp["engine"], preset=cp["preset"])
    except PhononError as exc:
        raise _worker_error(str(exc)) from None
    labels = [sc["label"] for sc in plan3["supercells"]]
    if len(labels) + 1 > MAX_SPAWN:
        raise _worker_error(f"{len(labels)} fc3 supercells exceed the spawn limit "
                            f"({MAX_SPAWN - 1}); lower cutoff_pair_a or the supercell")
    ctx.save_output("plan3", plan3)
    _save_supercells(ctx, plan3["supercells"])
    for lb in labels:
        _spawn_forces(ctx, lb, cp)
    fc2_id = next((r.artifact_id for r in ctx.inputs if r.name == "fc2"), "")
    inputs = [StepInput(".", "plan3", as_name="plan3"), *_forces_inputs(labels)]
    extra = {}
    if fc2_id.startswith("artifact://"):
        extra["input_artifacts"] = {"fc2": fc2_id}
    else:                                   # no artifact id (local run): pass a copy along
        ctx.save_output("fc2", fc2_path, media_type="application/yaml", filename="fc2.yaml")
        inputs.append(StepInput(".", "fc2", as_name="fc2"))
    kparams = {"engine": cp["engine"], "preset": cp["preset"],
               **{k: p[k] for k in ("mesh", "t_k") if p.get(k) is not None}}
    ctx.spawn("kappa3", operation="phonon.kappa3", params=kparams, inputs=inputs,
              resources=light_resources(8192), capabilities=["sci", "phono3py"], **extra)
    return None


def phonon_kappa3(ctx):
    import phono3py
    from .bte import load_fc2, run_kappa3
    p = ctx.params
    try:
        plan3 = json.loads(_input(ctx, "plan3").read_text())
        fc2 = load_fc2(_input(ctx, "fc2"))
        forces = collect_forces(ctx)
        mesh = mesh_numbers(p.get("mesh"), 11)
        t_k = temperatures(p.get("t_k"))
        k3 = run_kappa3(plan3, forces, fc2, mesh=mesh, t_k=t_k)
    except PhononError as exc:
        raise _worker_error(str(exc)) from None
    ctx.save_output("kappa3", k3)
    m = method("phono3py", phono3py.__version__, plan3.get("engine", ""), plan3.get("preset", ""),
               {"supercell": plan3["supercell_matrix"],
                "phonon_supercell": plan3["phonon_supercell_matrix"],
                "cutoff_pair_a": plan3["cutoff_pair_a"], "amplitude_a": plan3["amplitude_a"],
                "mesh": mesh, "method": "RTA"}, first_provenance(forces))
    props = {_t_key(t): k for t, k in zip(t_k, k3["kappa_l_w_mk"])}
    ctx.record("phonon.bte", structure=Structure.from_json(plan3["structure"]),
               properties=_finite(props), conditions={"t_k": t_k, "mesh": mesh},
               method=m, outputs=["kappa3"],
               uncertainty={"model": "phono3py-rta", "cutoff_pair_a": plan3["cutoff_pair_a"]})
    return None


def register(worker) -> None:
    worker.task("phonon.harmonic")(phonon_harmonic)
    worker.task("phonon.collect")(phonon_collect)
    worker.task("phonon.fc3")(phonon_fc3)
    worker.task("phonon.kappa3")(phonon_kappa3)
