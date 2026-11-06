"""te.evaluate: ZT arithmetic on synthetic transport, κ_l interpolation, regimes,
screening, element scores and the tier-2 spawn shape."""
from __future__ import annotations

import json

import numpy as np
import pytest

from conftest import halfheusler
from fireforge.tdr.v1 import job_pb2
from fireforge_science import evaluate as ev
from fireforge_science.crystalforge.stepio import run_task

T = [300.0 + 100 * i for i in range(10)]          # 300 … 1200
DOP = [1e19, 1e20]


def synthetic_transport(engine="qe"):
    """p: S = 200 µV/K, σ/τ = 1e19 (·[1, 10]), κe/τ = 1e14 (·[1, 10]); n: mirror, S < 0."""
    nT = len(T)

    def kind(sign):
        return {"seebeck_uv_k": [[sign * 200.0, sign * 100.0]] * nT,
                "sigma_over_tau_s_m_s": [[1e19, 1e20]] * nT,
                "kappa_e_over_tau_w_mks": [[1e14, 1e15]] * nT,
                "pf_over_tau_w_mk2s": [[4e-8 * 1e19, 1e-8 * 1e20]] * nT}
    return {"format": "fireforge.transport.v1", "t_k": T, "doping_cm3": DOP, "crta": True,
            "gap_ev": 0.5, "gap_direct": False, "engine": engine, "formula": "ZrNiSn",
            "p": kind(1.0), "n": kind(-1.0)}


PHONON = {"t_k": [300, 600], "kappa_l_w_mk": [3.0, 1.5], "dynamically_stable": True,
          "min_freq_thz": 0.0, "theta_d_k": 350.0, "gruneisen": 1.7, "engine": "qe"}
STABILITY = {"formation_ev_per_atom": -0.6, "e_above_hull_ev_per_atom": 0.0, "engine": "qe"}
ELASTIC = {"c11_gpa": 250.0, "c12_gpa": 90.0, "c44_gpa": 80.0, "born_stable": True}


def test_kappa_curve_forms():
    t, k = ev.kappa_curve(PHONON, "phonon")
    assert t.tolist() == [300, 600] and k.tolist() == [3.0, 1.5]
    t, k = ev.kappa_curve({"kappa_l_w_mk": {"300": 3.0, "600K": 1.5}}, "phonon")
    assert k.tolist() == [3.0, 1.5]
    t, k = ev.kappa_curve({"bte": {"t_k": [300], "kappa_l_w_mk": [[[6, 0, 0], [0, 6, 0], [0, 0, 6]]]}},
                          "kappa3")
    assert k.tolist() == [6.0]
    t, k = ev.kappa_curve({"t_k": [300], "kappa_l_w_mk": [[4, 4, 4, 0, 0, 0]]}, "kappa3")
    assert k.tolist() == [4.0]
    with pytest.raises(Exception, match="kappa_l_w_mk"):
        ev.kappa_curve({"t_k": [300]}, "phonon")


def test_kappa_interpolation_and_extrapolation():
    tk, kk = np.array([300.0, 600.0]), np.array([3.0, 1.5])
    k = ev.kappa_at(tk, kk, [300, 424.26, 600, 1200, 150])
    assert k[0] == pytest.approx(3.0) and k[2] == pytest.approx(1.5)
    assert k[1] == pytest.approx(3.0 * (300 / 424.26), rel=1e-3)   # 1/T exact here (log-log)
    assert k[3] == pytest.approx(0.75) and k[4] == pytest.approx(6.0)


def test_zt_arithmetic():
    tr = synthetic_transport()
    kl = ev.kappa_at(np.array([300.0, 600.0]), np.array([3.0, 1.5]), T)
    tabs = ev.zt_tables(tr, kl, 1e-14)
    # 300 K, 1e19: S²σT/(κe+κl) = (2e-4)² · 1e5 · 300 / (1 + 3) = 0.3
    assert tabs["p"]["zt"][0, 0] == pytest.approx(0.3)
    # 300 K, 1e20: (1e-4)² · 1e6 · 300 / (10 + 3)
    assert tabs["n"]["zt"][0, 1] == pytest.approx(3.0 / 13)
    assert tabs["p"]["pf_uw_cmk2"][0, 0] == pytest.approx(40.0)
    # 1200 K, 1e19: 4e-3 · 1200 / (1 + 0.75)
    assert tabs["p"]["zt"][-1, 0] == pytest.approx(4.8 / 1.75)
    # τ scales σ and κe together
    t2 = ev.zt_tables(tr, kl, 2e-14)
    assert t2["p"]["zt"][0, 0] == pytest.approx(4e-3 * 2 * 300 / (2 + 3))
    s = ev.zt_summary(tr, tabs, kl)
    assert s["zt_peak_p"] == pytest.approx(4.8 / 1.75) and s["t_peak_p_k"] == 1200
    assert s["doping_peak_p_cm3"] == 1e19 and s["zt_peak"] == s["zt_peak_p"]
    assert s["kappa_l_300k"] == pytest.approx(3.0)
    assert s["pf_peak_uw_cmk2"] == pytest.approx(100.0)


def test_regime_averages():
    t = np.array(T)
    zt = np.column_stack([t / 1000.0, np.full(len(t), 0.5)])
    v, j = ev.regime_average(t, zt, 300, 500)
    assert v == pytest.approx(0.5) and j in (0, 1)          # both 0.4 avg vs 0.5 -> col 1
    assert j == 1
    v, j = ev.regime_average(t, zt, 800, 1100)
    assert v == pytest.approx(0.95) and j == 0              # mean of linear ramp 0.8..1.1
    v, j = ev.regime_average(t, zt, 1100, np.inf)
    assert v == pytest.approx(1.15)
    assert ev.regime_average(t, zt, 1300, 1400) == (None, None)
    s = ev.zt_summary(synthetic_transport(), ev.zt_tables(synthetic_transport(),
                                                          np.full(len(T), 1.0), 1e-14),
                      np.full(len(T), 1.0))
    for name in ev.REGIMES:
        assert s[f"zt_avg_{name}_p"] is not None and s[f"zt_avg_{name}_n"] is not None


def test_screen_thresholds():
    flags = {"dynamically_stable": True, "born_stable": True, "e_above_hull_ev_per_atom": 0.05,
             "band_gap_ev": 0.3}
    ok, crit = ev.screen(flags, 0.5)
    assert ok and all(crit.values())
    assert not ev.screen(flags, 0.2)[0]
    assert not ev.screen({**flags, "e_above_hull_ev_per_atom": 0.2}, 0.5)[0]
    assert not ev.screen({**flags, "band_gap_ev": 0.01}, 0.5)[0]
    assert not ev.screen({**flags, "dynamically_stable": False}, 0.5)[0]
    missing = {k: v for k, v in flags.items() if k != "born_stable"}
    ok, crit = ev.screen(missing, 0.5)
    assert not ok and crit["born_stable"] is False
    assert ev.screen(missing, 0.5, {"born_stable": None})[0]          # criterion disabled
    assert ev.screen(flags, 0.2, {"zt_peak_min": 0.1})[0]
    assert ev.screen({**flags, "e_above_hull_ev_per_atom": 0.2}, 0.5,
                     {"e_above_hull_max_ev": 0.25})[0]
    with pytest.raises(Exception, match="unknown thresholds"):
        ev.screen(flags, 0.5, {"zt": 1})


def test_element_scores():
    s = ev.element_scores({"Zr": 1, "Ni": 1, "Sn": 1})
    assert s["abundance_limiting_element"] == "Sn" and s["abundance_ppm_min"] == 2.3
    assert s["toxicity"] == 1 and s["toxic_elements"] == []
    assert 15 < s["cost_usd_kg"] < 35
    assert ev.element_scores({"Pb": 1, "Te": 1})["toxicity"] == 2
    hf = ev.element_scores({"Hf": 1, "Ni": 1, "Sn": 1})["cost_usd_kg"]
    assert hf > 1000                                       # Hf dominates the cost
    assert ev.element_scores({"Xe": 1})["elements_unknown"] == ["Xe"]


def test_elements_file_covers_campaign_elements():
    data = ev.element_data()
    for el in ("Ti", "Zr", "Hf", "V", "Nb", "Ta", "Sc", "Y", "Fe", "Co", "Ni", "Ru", "Rh", "Pd",
               "Ir", "Pt", "Mn", "Cr", "Mo", "W", "Sn", "Sb", "Pb", "Te", "Bi", "Ge", "Si",
               "Ga", "In", "Al"):
        d = data[el]
        assert d["abundance_ppm"] > 0 and d["price_usd_per_kg"] > 0 and d["toxicity"] in (0, 1, 2)
        assert d["price_note"]
    src = json.loads((ev.DATA_DIR / "elements.json").read_text())["sources"]
    assert {"abundance_ppm", "price_usd_per_kg", "toxicity"} <= set(src)


def _files(tmp_path, tr=None, **extra):
    docs = {"transport": tr or synthetic_transport(), "phonon": PHONON, "stability": STABILITY,
            "elastic": ELASTIC, "structure": halfheusler().to_json(), **extra}
    out = {}
    for n, d in docs.items():
        p = tmp_path / f"{n}.json"
        p.write_text(json.dumps(d))
        out[n] = p
    return out


def test_op_evaluate_record_and_pass(tmp_path):
    inputs = _files(tmp_path)
    res, _, outs = run_task("te.evaluate", {"tier2": "off"}, inputs, workdir=tmp_path / "w")
    assert res.success, res.error
    doc = json.loads(outs["evaluation"].read_text())
    assert doc["screen_pass"] and doc["kappa_source"] == "slack" and not doc["tier2_spawned"]
    assert doc["a_angstrom"] == pytest.approx(6.113, abs=1e-3)
    assert doc["born_stable"] and doc["dynamically_stable"] and doc["band_gap_ev"] == 0.5
    assert doc["kappa_l_300k_slack"] == pytest.approx(3.0) and doc["structure"]["sites"]
    assert len(res.spawn) == 0
    (rec,) = res.records
    props = json.loads(rec.properties_json)
    assert rec.kind == "te.evaluate" and props["screen_pass"] is True
    assert "zt_avg_high_p" in props and props["zt_peak_p"] == pytest.approx(doc["zt_peak_p"])
    assert json.loads(rec.uncertainty_json)["crta"] is True


def test_op_evaluate_fail_and_missing(tmp_path):
    inputs = _files(tmp_path, stability={"formation_ev_per_atom": 0.1,
                                         "e_above_hull_ev_per_atom": 0.3})
    res, _, outs = run_task("te.evaluate", {}, inputs, workdir=tmp_path / "w")
    assert res.success, res.error
    doc = json.loads(outs["evaluation"].read_text())
    assert not doc["screen_pass"] and doc["criteria"]["e_above_hull_max_ev"] is False
    assert len(res.spawn) == 0                                   # auto: no tier 2
    res, _, _ = run_task("te.evaluate", {}, {"phonon": inputs["phonon"]}, workdir=tmp_path / "x")
    assert not res.success and "'transport'" in res.error
    res, _, _ = run_task("te.evaluate", {"kappa_source": "bte"},
                         {"transport": inputs["transport"], "structure": inputs["structure"]},
                         workdir=tmp_path / "y")
    assert not res.success and "'kappa3'" in res.error
    (tmp_path / "t").mkdir()
    toy = _files(tmp_path / "t", tr=synthetic_transport("toy"))
    res, _, _ = run_task("te.evaluate", {}, toy, workdir=tmp_path / "z")
    assert not res.success and "different engines" in res.error


def test_tier2_spawn_shape(tmp_path):
    fc2 = tmp_path / "fc2.hdf5"
    fc2.write_bytes(b"fc2-bytes")
    inputs = {**_files(tmp_path), "fc2": fc2}
    res, _, outs = run_task("te.evaluate", {"tier2": "auto", "engine": "qe", "cpus": 4,
                                            "tier2_params": {"supercell": [2, 2, 2]}},
                            inputs, workdir=tmp_path / "w")
    assert res.success, res.error
    assert json.loads(outs["evaluation"].read_text())["tier2_spawned"]
    assert outs["in.fc2"].read_bytes() == b"fc2-bytes"
    assert {"in.transport", "in.phonon", "in.structure", "in.stability", "in.elastic"} <= set(outs)
    fc3, bte = res.spawn
    assert fc3.name == "fc3" and fc3.job.operation == "phonon.fc3"
    assert {(i.step, i.output, i.input_name) for i in fc3.inputs} == \
        {(".", "in.structure", "structure"), (".", "in.fc2", "fc2")}
    assert json.loads(fc3.job.parameters_json)["supercell"] == [2, 2, 2]
    assert "phono3py" in fc3.job.required_capabilities
    assert bte.name == "evaluate.bte" and bte.job.operation == "te.evaluate"
    assert list(bte.depends_on) == ["fc3"] and bte.run_if == job_pb2.ALL_SUCCEEDED
    k3 = [i for i in bte.inputs if i.input_name == "kappa3"]
    assert len(k3) == 1 and k3[0].step == "fc3" and k3[0].output == "kappa3" and k3[0].spawned
    rest = {(i.step, i.output, i.input_name) for i in bte.inputs if i.input_name != "kappa3"}
    assert (".", "in.transport", "transport") in rest and (".", "in.phonon", "phonon") in rest
    assert not any(i.input_name == "fc2" for i in bte.inputs)
    p = json.loads(bte.job.parameters_json)
    assert p["kappa_source"] == "bte" and p["tier2"] == "off" and p["cpus"] == 4

    # the follow-up step, run with the wired inputs and a kappa3 from the tier
    k3file = tmp_path / "kappa3.json"
    k3file.write_text(json.dumps({"t_k": [300, 600, 900], "kappa_l_w_mk": [6.0, 3.0, 2.0],
                                  "engine": "qe"}))
    child = {i.input_name: outs[i.output] for i in bte.inputs if i.step == "."}
    child["kappa3"] = k3file
    res2, _, outs2 = run_task("te.evaluate", p, child, workdir=tmp_path / "bte")
    assert res2.success, res2.error
    doc = json.loads(outs2["evaluation"].read_text())
    assert doc["kappa_source"] == "bte" and doc["kappa_l_300k_bte"] == pytest.approx(6.0)
    assert doc["kappa_l_300k_slack"] == pytest.approx(3.0) and len(res2.spawn) == 0


def test_tier2_force_needs_fc2(tmp_path):
    res, _, _ = run_task("te.evaluate", {"tier2": "force"}, _files(tmp_path),
                         workdir=tmp_path / "w")
    assert not res.success and "fc2" in res.error


def test_record_curves_envelope():
    from fireforge_science.evaluate import record_curves
    doc = {"t_k": [300.0, 600.0], "kappa_l_w_mk": [4.0, 2.0],
           "tables": {"p": {"zt": [[0.1, 0.3], [None, 0.5]], "pf_uw_cmk2": [[1, 2], [3, None]]},
                      "n": {"zt": [[None, None], [0.2, 0.1]], "pf_uw_cmk2": [[1, 1], [2, 2]]}}}
    c = record_curves(doc)
    assert c["zt_p"] == [0.3, 0.5] and c["zt_n"] == [None, 0.2]
    assert c["pf_p_uw_cmk2"] == [2, 3] and c["kappa_l_w_mk"] == [4.0, 2.0] and c["t_k"] == [300.0, 600.0]
    assert record_curves({**doc, "kappa_l_w_mk": None})["kappa_l_w_mk"] is None
