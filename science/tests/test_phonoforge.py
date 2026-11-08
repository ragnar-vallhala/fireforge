"""PhonoForge (phase8.md §4): plan + spawn wiring, collect on toy forces, Slack formula,
phono3py tier. ToyEngine only; the worker runs against fireforge.testing.FakeDaemon."""
from __future__ import annotations

import io
import json
import time
from pathlib import Path

import numpy as np
import pytest

from fireforge.testing import FakeDaemon
from fireforge_science.model import Structure
from fireforge_science.phonoforge import common, slack
from fireforge_science.phonoforge.harmonic import plan_labels
from fireforge_science.settings import Settings

SCIENCE = Path(__file__).resolve().parent.parent
WORKER = """
from fireforge import Worker
from fireforge_science import phonoforge
w = Worker()
phonoforge.register(w)
w.run()
"""
SC2 = [[2, 0, 0], [0, 2, 0], [0, 0, 2]]


@pytest.fixture(scope="module")
def worker_script(tmp_path_factory) -> Path:
    p = tmp_path_factory.mktemp("phonoworker") / "worker.py"
    p.write_text(WORKER)
    return p


@pytest.fixture(scope="module")
def stable_toy(relaxed_zrnisn) -> Structure:
    """Toy ZrNiSn compressed by 4 % in volume. The toy potential is marginally unstable at
    its own equilibrium volume (a soft TA mode near (−0.3, 0.35, 0.05): −0.6 THz with a
    2×2×2 supercell, −0.1 THz with 3×3×3) and stable under compression, so the plumbing
    tests use this cell; ±2 % around it is stable."""
    return relaxed_zrnisn.scaled(-0.04)


def run(script, operation, params, inputs=(), **kw):
    """Run one operation; returns the FakeDaemon (stopped) with result/spawned/records and
    `files` (output name -> bytes)."""
    d = FakeDaemon(operation, params, workflow_id="wf-1", step="phonon", **kw)
    with d:
        for data, name in inputs:
            d.add_input(data, name=name)
        proc = d.run_worker(script, timeout=120,
                            extra_env={"PYTHONPATH": f"{SCIENCE.parent / 'sdk/python'}:{SCIENCE}"})
        assert proc.returncode == 0, proc.stderr
        assert d.result.success, d.result.error
        d.files = {o.name: (d.output_dir / o.path).read_bytes() for o in d.result.outputs}
    return d


def toy_forces_npz(toy, structure_json: dict, label: str | None, energy=True) -> bytes:
    r = toy.forces(Structure.from_json(structure_json), Settings())
    arrays = {"forces": np.array(r.forces)}
    if energy:
        arrays["energy"] = np.array(r.energy_ev)
    if label is not None:
        arrays["label"] = np.array(label)
    buf = io.BytesIO()
    np.savez(buf, **arrays)
    return buf.getvalue()


@pytest.fixture(scope="module")
def harmonic(worker_script, stable_toy):
    params = {"engine": "toy", "preset": "screening", "cpus": 3, "mesh": 16,
              "t_k": [300, 600, 900]}
    return run(worker_script, "phonon.harmonic", params,
               [(json.dumps(stable_toy.to_json()), "structure")])


@pytest.fixture(scope="module")
def collect(worker_script, harmonic, toy):
    plan = json.loads(harmonic.files["plan"])
    inputs = [(harmonic.files["plan"], "plan")]
    for i, lb in enumerate(plan_labels(plan)):
        sc = json.loads(harmonic.files[f"supercell.{lb}"])
        # every other file without the npz label: matched via the input name
        inputs.append((toy_forces_npz(toy, sc, lb if i % 2 == 0 else None), f"forces.{lb}"))
    spec = next(s for s in harmonic.spawned if s.name == "collect")
    params = json.loads(spec.job.parameters_json)
    return run(worker_script, "phonon.collect", params, inputs)


# ---- parameters ---------------------------------------------------------------------------

def test_supercell_and_mesh_params():
    assert common.supercell_matrix(None) == SC2
    assert common.supercell_matrix(3) == [[3, 0, 0], [0, 3, 0], [0, 0, 3]]
    assert common.supercell_matrix([1, 2, 3])[2] == [0, 0, 3]
    m = [[-1, 1, 1], [1, -1, 1], [1, 1, -1]]
    assert common.supercell_matrix(m) == m
    for bad in ([1, 2], [[1, 0, 0], [0, 1, 0], [1, 0, 0]], 1.5):
        with pytest.raises(common.PhononError):
            common.supercell_matrix(bad)
    assert common.mesh_numbers(None) == [20, 20, 20] and common.mesh_numbers([4, 5, 6]) == [4, 5, 6]
    assert common.temperatures(None) == [300.0, 500.0, 800.0, 1100.0]


# ---- Slack -------------------------------------------------------------------------------

def test_julian_prefactor_units():
    # SI expression (Julian 1965, as eq. 7 of arXiv:1811.09968) == the practical
    # 2.43e-6 W/(m K) (amu, Å) == Slack's 2.43e-8 W/(cm K).
    for g in (0.8, 1.0, 1.5, 2.0, 3.0):
        den = 1 - 0.514 / g + 0.228 / g ** 2
        assert slack.julian_a_practical(g) * den == pytest.approx(2.43e-6, rel=2e-3)
        assert slack.julian_a_practical(g) * den / 100 == pytest.approx(2.43e-8, rel=2e-3)
    with pytest.raises(ValueError):
        slack.julian_a_si(-1.0)


def test_slack_worked_example_silicon():
    # Si: θ_D = 645 K (n = 2 → θ_a = 645/2^{1/3} = 512 K), γ = 1.06, M̄ = 28.086 amu,
    # a = 5.431 Å (δ = (a³/8)^{1/3} = 2.7155 Å). Worked by hand with Slack's practical
    # form (Morelli & Slack 2006: κ = A M̄ θ_a³ δ n^{1/3}/(γ² T), A = 2.43e-6/(1 − 0.514/γ
    # + 0.228/γ²) W/(m K)): A = 3.384e-6, κ(300 K) = 3.384e-6 · 28.086 · 511.94³ · 2.7155 ·
    # 1.2599 / (1.1236 · 300) = 129.4 W/(m K) — within the model's factor of 2 of the
    # measured 148 W/(m K) (Glassbrenner & Slack, Phys. Rev. 134, A1058 (1964)).
    theta_a = 645.0 / 2 ** (1 / 3)
    delta = (5.431 ** 3 / 8) ** (1 / 3)
    kw = dict(mean_mass_amu=28.086, theta_a_k=theta_a, delta_a=delta, n_atoms=2, gamma=1.06)
    k300 = slack.slack_kappa(300.0, **kw)
    assert k300 == pytest.approx(129.4, rel=2e-3)
    assert 148 / 2 < k300 < 148 * 2
    ks = slack.slack_kappa([300, 600, 1200], **kw)
    assert ks[0] / ks[1] == pytest.approx(2.0) and ks[0] / ks[2] == pytest.approx(4.0)
    assert slack.slack_kappa_times_t(**kw) == pytest.approx(300 * k300)


# ---- phonon.harmonic: plan and spawn wiring ---------------------------------------------

def test_harmonic_plan(harmonic, stable_toy):
    plan = json.loads(harmonic.files["plan"])
    assert plan["format"] == "fireforge.phonon-plan.v1"
    assert plan["volume_strains"] == [-0.02, 0.0, 0.02] and plan["reference"] == 1
    assert plan["supercell_matrix"] == SC2 and plan["amplitude_a"] == 0.01
    assert plan["natom_supercell"] == 24 and plan["engine"] == "toy"
    v0 = stable_toy.primitive().volume()
    for i, v in enumerate(plan["volumes"]):
        assert v["label"] == f"v{i}"
        assert v["volume_a3"] == pytest.approx(v0 * (1 + plan["volume_strains"][i]))
        assert v["dataset"]["natom"] == 24
        assert len(v["supercells"]) == len(v["dataset"]["first_atoms"]) == 3   # half-Heusler
        for j, sc in enumerate(v["supercells"]):
            assert sc["label"] == f"v{i}.d{j + 1:03d}"
            s = Structure.from_json(sc["structure"])
            assert s.nsites == 24
            # the separate output is the same structure
            assert json.loads(harmonic.files[f"supercell.{sc['label']}"]) == sc["structure"]
    assert Structure.from_json(plan["structure"]).matches(stable_toy)


def test_harmonic_spawns(harmonic):
    spawned = {s.name: s for s in harmonic.spawned}
    labels = [f"v{i}.d{j:03d}" for i in range(3) for j in (1, 2, 3)]
    assert list(spawned) == labels + ["collect"]
    for lb in labels:
        s = spawned[lb]
        assert s.job.operation == "qe.forces"
        assert json.loads(s.job.parameters_json) == {"engine": "toy", "preset": "screening",
                                                     "cpus": 3, "label": lb}
        assert s.job.resources.cpu_cores == 3
        assert [(i.step, i.output, i.spawned, i.input_name) for i in s.inputs] == \
            [(".", f"supercell.{lb}", False, "supercell")]
        assert list(s.depends_on) == []
    c = spawned["collect"]
    assert c.job.operation == "phonon.collect"
    assert json.loads(c.job.parameters_json) == {"engine": "toy", "preset": "screening",
                                                 "mesh": 16, "t_k": [300, 600, 900]}
    wiring = [(i.step, i.output, i.spawned, i.input_name) for i in c.inputs]
    assert wiring[0] == (".", "plan", False, "plan")
    assert wiring[1:] == [(lb, "forces", False, f"forces.{lb}") for lb in labels]
    assert harmonic.records == []


def test_harmonic_bad_params(worker_script, stable_toy):
    d = FakeDaemon("phonon.harmonic", {"engine": "toy", "volume_strains": [0, 0]},
                   workflow_id="wf", step="p")
    with d:
        d.add_input(json.dumps(stable_toy.to_json()), name="structure")
        d.run_worker(worker_script, timeout=60,
                     extra_env={"PYTHONPATH": f"{SCIENCE.parent / 'sdk/python'}:{SCIENCE}"})
        assert not d.result.success and not d.result.retryable
        assert "volume_strains" in d.result.error


# ---- phonon.collect ----------------------------------------------------------------------

def test_collect_phonon_json(collect):
    ph = json.loads(collect.files["phonon"])
    assert ph["format"] == "fireforge.phonon.v1" and ph["formula"] == "ZrNiSn"
    assert ph["dynamically_stable"] is True and ph["min_freq_thz"] > -0.1
    assert 5.0 < ph["max_freq_thz"] < 12.0
    assert 150 < ph["theta_d_k"] < 600                       # second moment
    assert 100 < ph["theta_d_debye_fit_k"] < 600
    assert 100 < ph["theta_d_sound_k"] < 600
    assert ph["theta_a_k"] == pytest.approx(ph["theta_d_k"] / 3 ** (1 / 3))
    assert 1000 < ph["v_t_m_s"] < ph["v_s_m_s"] < ph["v_l_m_s"] < 9000
    assert ph["gruneisen"] > 0                                # toy: γ_acoustic ≈ 4.7
    det = ph["gruneisen_detail"]
    assert det["source"] == "qha" and det["gruneisen_acoustic_mean"] > 0
    assert set(det["gruneisen_cv"]) == {"300", "600", "900"}
    assert all(g > 0 for g in det["gruneisen_cv"].values())
    sl = ph["slack"]
    assert sl["model"] == "slack" and sl["t_k"] == [300, 600, 900]
    k = sl["kappa_l_w_mk"]
    assert k[0] > k[1] > k[2] > 0
    assert k[0] * 300 == pytest.approx(k[1] * 600) == pytest.approx(sl["kappa_l_times_t_w_m"])
    assert [v["label"] for v in ph["volumes"]] == ["v0", "v1", "v2"]
    assert len(ph["dos"]["frequency_thz"]) == len(ph["dos"]["dos_states_per_thz"]) > 50
    disp = ph["dispersion"]
    assert disp["labels"][0] == "Γ" and "X" in disp["labels"] and "L" in disp["labels"]
    assert len(disp["frequencies_thz"][0][0]) == 9


def test_collect_fc2_rebuilds(collect, tmp_path):
    import phonopy
    p = tmp_path / "fc2.yaml"
    p.write_bytes(collect.files["fc2"])
    ph = phonopy.load(str(p), produce_fc=False, log_level=0)
    assert ph.force_constants.shape in ((3, 24, 3, 3), (24, 24, 3, 3))   # compact or full
    assert np.allclose(ph.supercell_matrix, SC2)
    ph.run_qpoints([[0.5, 0.5, 0.5]])
    f = json.loads(collect.files["phonon"])
    assert ph.qpoints.frequencies.max() <= f["max_freq_thz"] + 1e-6


def test_collect_records(collect):
    recs = {r.kind: r for r in collect.records}
    assert set(recs) == {"phonon.harmonic", "phonon.slack"}
    h = recs["phonon.harmonic"]
    props = json.loads(h.properties_json)
    assert {"min_freq_thz", "dynamically_stable", "theta_d_k", "gruneisen", "v_s_m_s"} <= set(props)
    assert props["dynamically_stable"] is True
    assert list(h.output_names) == ["phonon", "fc2"]
    assert h.structure.reduced_formula == "ZrNiSn" and h.structure.fingerprint.startswith("fp1:")
    assert h.method.solver == "phonopy" and h.method.engine == "toy" and h.method.preset == "screening"
    assert json.loads(h.method.settings_json)["supercell"] == SC2
    assert json.loads(h.conditions_json)["volume_strains"] == [-0.02, 0.0, 0.02]
    s = recs["phonon.slack"]
    sp = json.loads(s.properties_json)
    assert set(k for k in sp if k.startswith("kappa_l_w_mk_")) == \
        {"kappa_l_w_mk_300k", "kappa_l_w_mk_600k", "kappa_l_w_mk_900k"}
    assert sp["kappa_l_w_mk_300k"] > sp["kappa_l_w_mk_900k"] > 0
    assert json.loads(s.uncertainty_json) == {"factor": 2, "model": "slack"}
    assert list(s.output_names) == ["phonon"]


def test_collect_missing_forces(worker_script, harmonic):
    d = FakeDaemon("phonon.collect", {}, workflow_id="wf", step="p")
    with d:
        d.add_input(harmonic.files["plan"], name="plan")
        d.run_worker(worker_script, timeout=60,
                     extra_env={"PYTHONPATH": f"{SCIENCE.parent / 'sdk/python'}:{SCIENCE}"})
        assert not d.result.success and not d.result.retryable
        assert "missing forces for 9" in d.result.error


def test_read_forces_labels(tmp_path, toy, stable_toy):
    s = stable_toy.to_json()
    p = tmp_path / "x.npz"
    p.write_bytes(toy_forces_npz(toy, s, "v1.d002"))
    assert common.read_forces(p, "forces#3")["label"] == "v1.d002"      # npz wins
    p.write_bytes(toy_forces_npz(toy, s, None))
    assert common.read_forces(p, "forces.v0.d001")["label"] == "v0.d001"
    q = tmp_path / "forces.v2.d003.npz"
    q.write_bytes(toy_forces_npz(toy, s, None))
    assert common.read_forces(q, "")["label"] == "v2.d003"


def test_single_volume_no_qha(stable_toy, toy):
    from fireforge_science.phonoforge.harmonic import analyze, build_plan
    plan = build_plan(stable_toy, supercell=SC2, volume_strains=[0], amplitude=0.01,
                      engine="toy", preset="screening")
    forces = {}
    for sc in plan["volumes"][0]["supercells"]:
        r = toy.forces(Structure.from_json(sc["structure"]), Settings())
        forces[sc["label"]] = {"label": sc["label"], "forces": np.array(r.forces),
                               "energy": r.energy_ev, "provenance": None}
    out, _ = analyze(plan, forces, mesh=[8, 8, 8], t_k=[300.0])
    assert out["gruneisen"] is None and out["slack"]["kappa_l_w_mk"] is None
    out, _ = analyze(plan, forces, mesh=[8, 8, 8], t_k=[300.0], gruneisen_param=1.5)
    assert out["gruneisen"] == 1.5 and out["gruneisen_detail"]["source"] == "parameter"
    assert out["slack"]["kappa_l_w_mk"][0] > 0


# ---- tier 2: phono3py --------------------------------------------------------------------

def test_tier2_fc3_kappa3(worker_script, collect, stable_toy, toy):
    t0 = time.monotonic()
    fc3 = run(worker_script, "phonon.fc3",
              {"engine": "toy", "preset": "screening", "cpus": 1, "cutoff_pair_a": 4.0,
               "mesh": 7, "t_k": [300, 600]},
              [(json.dumps(stable_toy.to_json()), "structure"), (collect.files["fc2"], "fc2")])
    plan3 = json.loads(fc3.files["plan3"])
    assert plan3["format"] == "fireforge.phonon-plan3.v1"
    assert plan3["supercell_matrix"] == SC2 and plan3["phonon_supercell_matrix"] == SC2
    labels = [sc["label"] for sc in plan3["supercells"]]
    assert 0 < len(labels) < plan3["n_displacements_total"]       # cutoff drops pairs
    spawned = {s.name: s for s in fc3.spawned}
    assert list(spawned) == labels + ["kappa3"]
    assert all(spawned[lb].job.operation == "qe.forces" for lb in labels)
    assert [(i.step, i.output, i.input_name) for i in spawned[labels[0]].inputs] == \
        [(".", f"supercell.{labels[0]}", "supercell")]
    k = spawned["kappa3"]
    assert k.job.operation == "phonon.kappa3"
    wiring = [(i.step, i.output, i.input_name) for i in k.inputs]
    assert wiring[0] == (".", "plan3", "plan3")
    assert wiring[1:] == [(lb, "forces", f"forces.{lb}") for lb in labels]
    fc2_in = next(i for i in fc3.inputs if i.name == "fc2")
    assert list(k.job.input_artifacts) == [fc2_in.artifact_id]
    assert list(k.job.input_names) == ["fc2"]

    inputs = [(fc3.files["plan3"], "plan3"), (collect.files["fc2"], "fc2")]
    for lb in labels:
        sc = json.loads(fc3.files[f"supercell.{lb}"])
        inputs.append((toy_forces_npz(toy, sc, lb), f"forces.{lb}"))
    k3 = run(worker_script, "phonon.kappa3", json.loads(k.job.parameters_json), inputs)
    elapsed = time.monotonic() - t0
    out = json.loads(k3.files["kappa3"])
    assert out["format"] == "fireforge.kappa3.v1" and out["t_k"] == [300, 600]
    kl = out["kappa_l_w_mk"]
    assert kl[0] > kl[1] > 0 and 1.5 < kl[0] / kl[1] < 2.5           # ~1/T
    assert out["mesh"] == [7, 7, 7] and len(out["kappa_tensor_w_mk"][0]) == 6
    rec = k3.records
    assert [r.kind for r in rec] == ["phonon.bte"]
    props = json.loads(rec[0].properties_json)
    assert props["kappa_l_w_mk_300k"] == pytest.approx(kl[0])
    assert rec[0].method.solver == "phono3py" and list(rec[0].output_names) == ["kappa3"]
    assert elapsed < 60


def test_qe_forces_contract(tmp_path, harmonic):
    """The quantumforge qe.forces op (toy engine) produces what collect reads."""
    import importlib.util
    if importlib.util.find_spec("fireforge_science.quantumforge") is None:
        pytest.skip("quantumforge not installed")
    script = tmp_path / "ops_worker.py"
    script.write_text("from fireforge_science.ops import build_worker\nbuild_worker().run()\n")
    spec = harmonic.spawned[0]
    d = run(script, "qe.forces", json.loads(spec.job.parameters_json),
            [(harmonic.files[f"supercell.{spec.name}"], "supercell")])
    p = tmp_path / "f.npz"
    p.write_bytes(d.files["forces"])
    f = common.read_forces(p, "forces#2")
    assert f["label"] == spec.name and f["forces"].shape == (24, 3)
    assert f["provenance"]["engine"] == "toy"


def test_harmonic_passes_spin_off_for_nonmagnetic(worker_script, stable_toy):
    # qe.relax marks a structure without a moment by all-zero magmoms; displaced supercells
    # carry no magmoms, so the spawned force steps get spin "off" as a parameter.
    nonmag = stable_toy.copy(magmoms=[0.0] * stable_toy.nsites)
    params = {"engine": "toy", "preset": "screening", "cpus": 1}
    d = run(worker_script, "phonon.harmonic", params, [(json.dumps(nonmag.to_json()), "structure")])
    forces = [s for s in d.spawned if s.job.operation == "qe.forces"]
    assert forces and all(json.loads(s.job.parameters_json)["spin"] == "off" for s in forces)
    # an explicit spin parameter wins
    d = run(worker_script, "phonon.harmonic", {**params, "spin": "on"},
            [(json.dumps(nonmag.to_json()), "structure")])
    assert {json.loads(s.job.parameters_json)["spin"] for s in d.spawned
            if s.job.operation == "qe.forces"} == {"on"}


def test_forces_spin_helper(stable_toy):
    from fireforge_science.phonoforge import forces_spin
    assert forces_spin({}, stable_toy) == {}                       # unknown: auto downstream
    nonmag = stable_toy.copy(magmoms=[0.0] * stable_toy.nsites)
    assert forces_spin({}, nonmag) == {"spin": "off"}
    assert forces_spin({"spin": "auto"}, nonmag) == {"spin": "auto"}
    magnetic = stable_toy.copy(magmoms=[0.0, 1.0, 0.0])
    assert forces_spin({}, magnetic) == {}
