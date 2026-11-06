"""Shared fixtures for the science tests (tools/pytest.sh science/tests).

Markers: `qe` (needs pw.x; skipped when it is not on PATH), `pseudo` (needs the SSSP
table + files; skipped when missing). Fixtures: structure builders for half-Heuslers,
`toy` (ToyEngine), `screening` (Settings() defaults, no SSSP needed).
"""
from __future__ import annotations

import shutil
import warnings

import pytest

from fireforge_science.model import Structure
from fireforge_science.settings import Settings

ZRNISN_A = 6.113


def pytest_configure(config):
    config.addinivalue_line("markers", "qe: needs Quantum ESPRESSO pw.x (skipped without it)")
    config.addinivalue_line("markers", "pseudo: needs the SSSP pseudopotentials")
    warnings.filterwarnings("ignore", message=".*pyfftw.*")


def _have_pseudo() -> bool:
    try:
        from fireforge_science import pseudo
        pseudo.sssp_json_path()
        return pseudo.pseudo_dir().is_dir()
    except FileNotFoundError:
        return False


def pytest_collection_modifyitems(config, items):
    no_pw = shutil.which("pw.x") is None
    no_pseudo = not _have_pseudo()
    for item in items:
        if no_pw and "qe" in item.keywords:
            item.add_marker(pytest.mark.skip(reason="pw.x not installed"))
        if no_pseudo and "pseudo" in item.keywords:
            item.add_marker(pytest.mark.skip(reason="SSSP pseudopotentials not installed "
                                                    "(tools/sci-setup.sh)"))


def fcc_lattice(a: float) -> list[list[float]]:
    """Primitive fcc vectors for conventional cubic constant a."""
    h = a / 2
    return [[0.0, h, h], [h, 0.0, h], [h, h, 0.0]]


def halfheusler(x: str = "Zr", y: str = "Ni", z: str = "Sn", a: float = ZRNISN_A,
                **kw) -> Structure:
    """XYZ half-Heusler (F-43m) primitive cell: X at 0, Y at 1/4, Z at 1/2 (Y is the
    tetrahedrally coordinated atom, Ni in ZrNiSn)."""
    return Structure(fcc_lattice(a), [x, y, z],
                     [[0, 0, 0], [0.25, 0.25, 0.25], [0.5, 0.5, 0.5]],
                     prototype=kw.pop("prototype", "half-heusler"), **kw)


@pytest.fixture
def zrnisn() -> Structure:
    return halfheusler()


@pytest.fixture(scope="session")
def toy():
    from fireforge_science.toy import ToyEngine
    return ToyEngine()


@pytest.fixture
def screening() -> Settings:
    return Settings()


@pytest.fixture(scope="session")
def relaxed_zrnisn():
    """ToyEngine-relaxed ZrNiSn (session-cached)."""
    from fireforge_science.toy import ToyEngine
    return ToyEngine().relax(halfheusler(), Settings()).structure
