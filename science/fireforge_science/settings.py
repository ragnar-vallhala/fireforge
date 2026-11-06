"""Calculation settings presets (docs/design/phase8.md §4, §6). CONTRACT: fixed by the lead.

Presets (field defaults are the screening values):

| field                  | screening | accurate |
|------------------------|-----------|----------|
| kdensity (Å⁻¹, 2π incl)| 0.25      | 0.15     |
| kdensity_dense         | 0.08      | 0.05     |
| degauss_ry             | 0.01      | 0.005    |
| conv_thr_ry_per_atom   | 1e-9      | 1e-10    |
| forc_conv_thr_ry_bohr  | 1e-4      | 5e-5     |
| press_conv_thr_kbar    | 0.5       | 0.2      |
| ecut scale             | 1.0       | 1.25     |

Cutoffs: the maximum SSSP recommendation over the elements (pseudo.cutoffs), times the
preset's scale (accurate: +25 %, a common convergence margin over SSSP efficiency), unless
given explicitly. k-point density: spacing between k-points along each reciprocal vector,
in Å⁻¹ *including* the 2π (ASE `kspacing` convention): n_i = ceil(2π|b_i| / density),
b_i the reciprocal vectors without 2π. ZrNiSn (a = 6.11 Å): 8³ screening, 12³ accurate.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field, asdict, fields, replace
from typing import Any

import numpy as np


@dataclass
class Settings:
    preset: str = "screening"                # "screening" | "accurate"
    ecutwfc_ry: float = 0.0                  # 0 = max SSSP recommendation over the elements
    ecutrho_ry: float = 0.0                  # 0 = max SSSP recommendation
    kdensity: float = 0.25                   # k-spacing, 2π/Å (screening 0.25, accurate 0.15)
    kdensity_dense: float = 0.08             # NSCF for transport (screening 0.08, accurate 0.05)
    smearing: str = "mv"
    degauss_ry: float = 0.01
    conv_thr_ry_per_atom: float = 1e-9
    forc_conv_thr_ry_bohr: float = 1e-4
    press_conv_thr_kbar: float = 0.5
    spin: str = "auto"                       # "auto" (spin-polarize if Fe/Co/Ni/Mn/Cr present), "on", "off"
    cpus: int = 1
    extra: dict[str, Any] = field(default_factory=dict)

    def to_json(self) -> dict[str, Any]: return asdict(self)

    @classmethod
    def from_json(cls, d: dict[str, Any]) -> "Settings":
        """Inverse of to_json; unknown keys raise TypeError."""
        return cls(**d)

    def spin_polarized(self, elements) -> bool:
        """Resolve `spin` for a set of elements (auto: any of MAGNETIC_ELEMENTS present)."""
        if self.spin == "on":
            return True
        if self.spin == "off":
            return False
        if self.spin != "auto":
            raise ValueError(f"spin must be auto/on/off, got {self.spin!r}")
        return any(e in MAGNETIC_ELEMENTS for e in elements)

    def kmesh(self, lattice, dense: bool = False) -> tuple[int, int, int]:
        return kmesh(lattice, self.kdensity_dense if dense else self.kdensity)


MAGNETIC_ELEMENTS = frozenset({"Fe", "Co", "Ni", "Mn", "Cr"})

# Overrides on top of the dataclass defaults (= screening).
PRESETS: dict[str, dict[str, Any]] = {
    "screening": {},
    "accurate": {
        "kdensity": 0.15,
        "kdensity_dense": 0.05,
        "degauss_ry": 0.005,
        "conv_thr_ry_per_atom": 1e-10,
        "forc_conv_thr_ry_bohr": 5e-5,
        "press_conv_thr_kbar": 0.2,
    },
}
ECUT_SCALE = {"screening": 1.0, "accurate": 1.25}


def preset(name: str, elements: list[str], **overrides) -> Settings:
    """Settings for a preset with SSSP cutoffs resolved for `elements`. KeyError for an
    unknown preset; ValueError for an element without an SSSP pseudopotential.
    Overrides are Settings fields (TypeError for unknown names); ecut overrides of 0 (or
    absent) are resolved from SSSP. Missing SSSP table: FileNotFoundError."""
    if name not in PRESETS:
        raise KeyError(f"unknown preset {name!r} (have: {', '.join(PRESETS)})")
    known = {f.name for f in fields(Settings)}
    bad = set(overrides) - known
    if bad:
        raise TypeError(f"unknown settings: {', '.join(sorted(bad))}")
    s = replace(Settings(), preset=name, **PRESETS[name])
    s = replace(s, **overrides)
    if not s.ecutwfc_ry or not s.ecutrho_ry:
        from . import pseudo
        wfc, rho = pseudo.cutoffs(elements)
        scale = ECUT_SCALE[name]
        if not s.ecutwfc_ry:
            s.ecutwfc_ry = wfc * scale
        if not s.ecutrho_ry:
            s.ecutrho_ry = rho * scale
    return s


def kmesh(lattice, density: float) -> tuple[int, int, int]:
    """Monkhorst-Pack mesh with spacing <= density (2π/Å), each >= 1."""
    if density <= 0:
        raise ValueError("k-point density must be > 0")
    lat = np.asarray(lattice, dtype=float).reshape(3, 3)
    rec = 2.0 * math.pi * np.linalg.inv(lat).T          # rows: reciprocal vectors with 2π
    out = []
    for b in rec:
        x = float(np.linalg.norm(b)) / density
        out.append(max(1, math.ceil(x - 1e-9)))
    return tuple(out)  # type: ignore[return-value]
