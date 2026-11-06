"""TransportForge: BoltzTraP2 on ToyEngine bands (S sign, doping conversion, record)."""
from __future__ import annotations

import json

import numpy as np
import pytest

from conftest import halfheusler
from fireforge_science.crystalforge.stepio import run_task
from fireforge_science.settings import Settings
from fireforge_science.toy import ToyEngine
from fireforge_science.transportforge import DOPING_DEFAULT, T_DEFAULT, summary, table

T = [300.0, 600.0, 900.0]
DOP = [1e18, 1e19, 1e20, 1e21]


@pytest.fixture(scope="module")
def bands_file(tmp_path_factory):
    toy = ToyEngine()
    s = toy.relax(halfheusler(), Settings()).structure
    bd = toy.nscf(s, None, Settings())
    p = tmp_path_factory.mktemp("bands") / "bands.npz"
    bd.save(p)
    return p, bd


@pytest.fixture(scope="module")
def transport(bands_file, tmp_path_factory):
    path, _ = bands_file
    res, ctx, outs = run_task("transport.boltztrap",
                              {"t_k": T, "doping_cm3": DOP, "npts": 2000, "tau_s": 2e-14},
                              {"bands": path}, workdir=tmp_path_factory.mktemp("bt"))
    assert res.success, res.error
    return res, json.loads(outs["transport"].read_text())


def test_defaults():
    assert T_DEFAULT[0] == 300 and T_DEFAULT[-1] == 1200 and len(T_DEFAULT) == 19
    assert DOPING_DEFAULT[0] == 1e18 and DOPING_DEFAULT[-1] == 1e21


def test_seebeck_sign_and_magnitude(transport):
    _, tr = transport
    Sp, Sn = table(tr, "p", "seebeck_uv_k"), table(tr, "n", "seebeck_uv_k")
    assert np.all(Sp > 0) and np.all(Sn < 0)
    assert 350 < Sp[0, 0] < 700 and -700 < Sn[0, 0] < -350     # toy: ≈ ±550 µV/K scale
    # |S| falls with doping at 300 K (at high T and low doping bipolar conduction lowers it)
    assert np.all(np.diff(Sp[0]) < 0) and np.all(np.diff(Sn[0]) > 0)
    assert Sp[-1, 0] < Sp[-1, 1]                     # bipolar: 900 K, 1e18 below 1e19


def test_doping_conversion(transport):
    _, tr = transport
    mup, mun = table(tr, "p", "mu_ev"), table(tr, "n", "mu_ev")
    # more holes -> mu deeper toward/into the valence band; more electrons -> toward CB
    assert np.all(np.diff(mup, axis=1) < 0) and np.all(np.diff(mun, axis=1) > 0)
    for kind in ("p", "n"):
        c = table(tr, kind, "carriers_cm3")
        assert np.allclose(c, np.array(DOP)[None, :], rtol=0.02)
    # low doping: mu inside the gap
    assert 0 < mup[0, 0] < tr["gap_ev"] and 0 < mun[0, 0] < tr["gap_ev"]


def test_tables_consistent(transport):
    _, tr = transport
    for kind in ("p", "n"):
        S = table(tr, kind, "seebeck_uv_k") * 1e-6
        sig = table(tr, kind, "sigma_over_tau_s_m_s")
        pf = table(tr, kind, "pf_over_tau_w_mk2s")
        k0, ke = table(tr, kind, "kappa0_over_tau_w_mks"), table(tr, kind, "kappa_e_over_tau_w_mks")
        assert np.allclose(pf, S ** 2 * sig, rtol=1e-6)
        t = np.array(tr["t_k"])[:, None]
        assert np.allclose(k0 - ke, t * sig * S ** 2, rtol=1e-5)
        assert np.all(sig > 0) and np.all(ke > 0)
        assert np.all(np.diff(sig, axis=1) > 0)          # σ grows with carriers
    assert tr["crta"] and tr["engine"] == "toy" and tr["format"] == "fireforge.transport.v1"


def test_record(transport):
    res, tr = transport
    (rec,) = res.records
    assert rec.kind == "transport.crta" and rec.method.engine == "toy"
    assert rec.method.solver == "BoltzTraP2" and list(rec.output_names) == ["transport"]
    assert json.loads(rec.uncertainty_json) == {"crta": True, "tau_s": 2e-14}
    props = json.loads(rec.properties_json)
    assert props["tau_s"] == 2e-14 and props["seebeck_p_uv_k_300k"] > 0
    assert props["pf_max_n_uw_cmk2"] == pytest.approx(props["pf_max_n_over_tau_w_mk2s"] * 2e-14 * 1e4)
    assert summary(tr, 2e-14)["doping_opt_p_cm3"] in DOP
    assert json.loads(rec.method.settings_json)["lpfac"] == 5


def test_missing_bands(tmp_path):
    res, _, _ = run_task("transport.boltztrap", {}, {}, workdir=tmp_path)
    assert not res.success and "'bands'" in res.error
