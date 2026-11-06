"""Settings presets, k-meshes and the SSSP lookup (phase8.md §4, §6)."""
from __future__ import annotations

import hashlib
import json

import numpy as np
import pytest

from fireforge_science import pseudo
from fireforge_science.settings import PRESETS, Settings, kmesh, preset

from conftest import fcc_lattice


# ---- kmesh --------------------------------------------------------------------------------

def test_kmesh_zrnisn():
    lat = fcc_lattice(6.113)          # |b| = 2π·√3/a = 1.780 Å⁻¹
    assert kmesh(lat, 0.25) == (8, 8, 8)
    assert kmesh(lat, 0.15) == (12, 12, 12)
    assert kmesh(lat, 0.08) == (23, 23, 23)
    assert kmesh(lat, 0.05) == (36, 36, 36)
    assert Settings().kmesh(lat) == (8, 8, 8)
    assert Settings().kmesh(lat, dense=True) == (23, 23, 23)


def test_kmesh_properties():
    lat = np.diag([3.0, 6.0, 30.0])
    n = kmesh(lat, 0.2)
    # spacing <= density along each reciprocal vector, minimal mesh
    for i, L in enumerate((3.0, 6.0, 30.0)):
        b = 2 * np.pi / L
        assert b / n[i] <= 0.2 + 1e-12
        assert n[i] == 1 or b / (n[i] - 1) > 0.2
    assert kmesh(lat, 100.0) == (1, 1, 1)
    assert kmesh(np.eye(3) * 2 * np.pi, 0.25) == (4, 4, 4)   # exact multiple: no extra point
    assert kmesh(np.eye(3).reshape(9), 1.0) == (7, 7, 7)       # flat lattice accepted
    with pytest.raises(ValueError):
        kmesh(lat, 0)


# ---- presets ------------------------------------------------------------------------------

@pytest.mark.pseudo
def test_preset_screening_zrnisn():
    s = preset("screening", ["Zr", "Ni", "Sn"])
    assert (s.ecutwfc_ry, s.ecutrho_ry) == (60.0, 480.0)        # Sn (GBRV) dominates
    assert s.preset == "screening"
    assert s.kdensity == 0.25 and s.kdensity_dense == 0.08
    assert s.conv_thr_ry_per_atom == 1e-9 and s.degauss_ry == 0.01
    assert s.spin_polarized(["Zr", "Ni", "Sn"])                 # auto: Ni present


@pytest.mark.pseudo
def test_preset_accurate():
    s = preset("accurate", ["Zr", "Ni", "Sn"])
    assert (s.kdensity, s.kdensity_dense, s.degauss_ry) == (0.15, 0.05, 0.005)
    assert s.conv_thr_ry_per_atom == 1e-10 and s.forc_conv_thr_ry_bohr == 5e-5
    assert s.press_conv_thr_kbar == 0.2
    assert (s.ecutwfc_ry, s.ecutrho_ry) == (75.0, 600.0)        # SSSP × 1.25
    assert set(PRESETS["accurate"]) <= set(Settings().to_json())


@pytest.mark.pseudo
def test_preset_cutoffs_per_element_max():
    # Fe (PAW, 90/1080) dominates NbFeSb; Hf (ONCV 50/200) with Sn (60/480)
    assert pseudo.cutoffs(["Nb", "Fe", "Sb"]) == (90.0, 1080.0)
    assert pseudo.cutoffs(["Hf", "Ni", "Sn"]) == (60.0, 480.0)
    assert pseudo.cutoffs(["Hf"]) == (50.0, 200.0)
    s = preset("screening", ["Nb", "Fe", "Sb"], ecutwfc_ry=100.0, cpus=4, spin="off")
    assert (s.ecutwfc_ry, s.ecutrho_ry, s.cpus) == (100.0, 1080.0, 4)
    assert not s.spin_polarized(["Fe"])


@pytest.mark.pseudo
def test_preset_errors():
    with pytest.raises(KeyError):
        preset("ultra", ["Si"])
    with pytest.raises(ValueError):
        preset("screening", ["Og"])
    with pytest.raises(TypeError):
        preset("screening", ["Si"], kdenisty=0.1)


def test_settings_json_round_trip():
    s = Settings(preset="accurate", cpus=3, extra={"nbnd": 40})
    assert Settings.from_json(json.loads(json.dumps(s.to_json()))) == s
    with pytest.raises(ValueError):
        Settings(spin="maybe").spin_polarized(["Si"])


# ---- pseudo -------------------------------------------------------------------------------

@pytest.mark.pseudo
def test_pseudo_lookup_and_provenance():
    assert pseudo.filename("Sn") == "Sn_pbe_v1.uspp.F.UPF"
    assert pseudo.path("Sn").is_file()
    prov = pseudo.provenance(["Sn", "Zr", "Ni", "Sn"])
    assert [p.split()[0] for p in prov] == ["ni_pbe_v1.4.uspp.F.UPF", "Sn_pbe_v1.uspp.F.UPF",
                                            "Zr_pbe_v1.uspp.F.UPF"]
    for p, el in zip(prov, ["Ni", "Sn", "Zr"]):
        name, md5 = p.split(" md5:")
        assert md5 == pseudo.entry(el)["md5"]
        assert md5 == hashlib.md5(pseudo.path(el).read_bytes()).hexdigest()
    assert pseudo.verify(["Zr", "Ni", "Sn"]) == {}
    assert "Og" in pseudo.verify(["Og"])


def test_pseudo_env_override(tmp_path, monkeypatch):
    d = tmp_path / "MyPseudos"
    d.mkdir()
    (d / "Si.upf").write_bytes(b"fake si")
    table = {"Si": {"filename": "Si.upf", "md5": hashlib.md5(b"fake si").hexdigest(),
                    "cutoff_wfc": 30.0, "cutoff_rho": 240.0},
             "C": {"filename": "C.upf", "md5": "0" * 32, "cutoff_wfc": 45.0,
                   "cutoff_rho": 360.0}}
    (tmp_path / "MyPseudos.json").write_text(json.dumps(table))
    monkeypatch.setenv("FIREFORGE_PSEUDO_DIR", str(d))
    monkeypatch.delenv("FIREFORGE_SSSP_JSON", raising=False)
    assert pseudo.pseudo_dir() == d
    assert pseudo.sssp_json_path() == tmp_path / "MyPseudos.json"
    assert pseudo.cutoffs(["Si", "C"]) == (45.0, 360.0)
    assert preset("screening", ["Si"]).ecutwfc_ry == 30.0
    assert pseudo.verify() == {"C": f"missing {d / 'C.upf'}"}
    assert pseudo.provenance(["Si"]) == [f"Si.upf md5:{table['Si']['md5']}"]
    with pytest.raises(pseudo.PseudoError):
        pseudo.provenance(["C"])
    with pytest.raises(ValueError):
        pseudo.cutoffs(["Ge"])
    # json inside the directory is found too
    (tmp_path / "MyPseudos.json").rename(d / "table.json")
    assert pseudo.sssp_json_path() == d / "table.json"
    # nothing found
    monkeypatch.setenv("FIREFORGE_PSEUDO_DIR", str(tmp_path / "nope"))
    with pytest.raises(FileNotFoundError):
        pseudo.sssp_json_path()
    with pytest.raises(FileNotFoundError):
        preset("screening", ["Si"])
