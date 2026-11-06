"""SSSP pseudopotential lookup (docs/design/phase8.md §6).

Directory: $FIREFORGE_PSEUDO_DIR, else ~/.cache/fireforge/pseudo/SSSP_1.3.0_PBE_efficiency.
Table (SSSP json: element -> filename, md5, cutoff_wfc, cutoff_rho in Ry):
$FIREFORGE_SSSP_JSON, else "<dir>.json" beside the directory, else the single *.json
inside it. tools/sci-setup.sh downloads and checks both.

Provenance strings for Method.pseudopotentials: "<file> md5:<hex>" (md5 of the file on
disk, cached per path/mtime).
"""
from __future__ import annotations

import hashlib
import json
import os
from functools import lru_cache
from pathlib import Path

SSSP_NAME = "SSSP_1.3.0_PBE_efficiency"
DEFAULT_ROOT = Path("~/.cache/fireforge/pseudo")
SSSP_URL = "https://archive.materialscloud.org/api/records/rcyfm-68h65/files/{file}/content"
SSSP_TAR_MD5 = "a58f1b3373f330179fd0832c48bb9a52"
SSSP_JSON_MD5 = "3153c4b20fc90a44fba0236627525644"


class PseudoError(ValueError):
    """Missing element, table or file."""


def pseudo_dir() -> Path:
    """Directory holding the UPF files (not checked for existence)."""
    env = os.environ.get("FIREFORGE_PSEUDO_DIR")
    if env:
        return Path(env).expanduser()
    return (DEFAULT_ROOT / SSSP_NAME).expanduser()


def sssp_json_path() -> Path:
    """The SSSP table; FileNotFoundError (with a setup hint) if none is found."""
    env = os.environ.get("FIREFORGE_SSSP_JSON")
    if env:
        p = Path(env).expanduser()
        if p.is_file():
            return p
        raise FileNotFoundError(f"FIREFORGE_SSSP_JSON={env} does not exist")
    d = pseudo_dir()
    beside = Path(str(d) + ".json")
    if beside.is_file():
        return beside
    inside = sorted(d.glob("*.json")) if d.is_dir() else []
    if len(inside) == 1:
        return inside[0]
    raise FileNotFoundError(
        f"SSSP table not found ({beside} or one *.json in {d}); run tools/sci-setup.sh "
        "or set FIREFORGE_PSEUDO_DIR / FIREFORGE_SSSP_JSON")


@lru_cache(maxsize=8)
def _load_table(path: str, mtime: float) -> dict[str, dict]:
    return json.loads(Path(path).read_text())


def sssp_table() -> dict[str, dict]:
    """element -> {filename, md5, pseudopotential, cutoff_wfc, cutoff_rho}."""
    p = sssp_json_path()
    return _load_table(str(p), p.stat().st_mtime)


def entry(element: str) -> dict:
    """SSSP entry of an element; PseudoError (a ValueError) if absent."""
    t = sssp_table()
    if element not in t:
        raise PseudoError(f"no SSSP pseudopotential for element {element!r}")
    return t[element]


def filename(element: str) -> str:
    return entry(element)["filename"]


def path(element: str) -> Path:
    return pseudo_dir() / filename(element)


def element_cutoffs(element: str) -> tuple[float, float]:
    """(ecutwfc, ecutrho) recommended by SSSP for one element, Ry."""
    e = entry(element)
    return float(e["cutoff_wfc"]), float(e["cutoff_rho"])


def cutoffs(elements) -> tuple[float, float]:
    """(ecutwfc, ecutrho) in Ry: the maximum SSSP recommendation over `elements` (each
    taken separately, so ecutrho follows the dual of the hardest-density pseudo)."""
    els = sorted(set(elements))
    if not els:
        raise PseudoError("no elements")
    cs = [element_cutoffs(e) for e in els]
    return max(c[0] for c in cs), max(c[1] for c in cs)


@lru_cache(maxsize=256)
def _md5(path: str, mtime: float, size: int) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def md5_file(p) -> str:
    st = os.stat(p)
    return _md5(str(p), st.st_mtime, st.st_size)


def provenance(elements) -> list[str]:
    """["<file> md5:<hex>", ...] for the elements (sorted by element), md5 of the files on
    disk; PseudoError if a file is missing."""
    out = []
    for el in sorted(set(elements)):
        p = path(el)
        if not p.is_file():
            raise PseudoError(f"pseudopotential file missing: {p}")
        out.append(f"{p.name} md5:{md5_file(p)}")
    return out


def verify(elements=None) -> dict[str, str]:
    """Check files against the SSSP md5s. Returns element -> problem ("missing" or
    "md5 mismatch ..."); empty dict = all good. elements=None checks the whole table."""
    t = sssp_table()
    problems: dict[str, str] = {}
    for el in sorted(t if elements is None else set(elements)):
        if el not in t:
            problems[el] = "not in SSSP table"
            continue
        p = pseudo_dir() / t[el]["filename"]
        if not p.is_file():
            problems[el] = f"missing {p}"
            continue
        got = md5_file(p)
        if got != t[el]["md5"]:
            problems[el] = f"md5 mismatch {p.name}: {got} != {t[el]['md5']}"
    return problems
