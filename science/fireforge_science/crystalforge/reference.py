"""Elemental reference structures for formation energies (phase8.md §4 crystal.reference).

Ground-state (or standard DFT reference) structures with approximate experimental
room-temperature lattice parameters; they are starting geometries, relaxed by qe.relax
before use, so the parameters only need to be close (values: standard crystallographic
tables, e.g. Wyckoff, Crystal Structures; Donohue, The Structures of the Elements).

Choices worth stating:

- Sn: α-Sn (diamond, Fd-3m). It is the T = 0 ground state (α→β at 286 K) and the usual
  DFT reference (e.g. Materials Project's Sn ground state); PBE also puts α below β.
- Sb, Bi, As: rhombohedral A7 (R-3m), 2-atom primitive cell.
- Te, Se: trigonal helical chains (P3_121), 3 atoms.
- Fe bcc (FM 2.2 μB), Co hcp (FM 1.6 μB), Ni fcc (FM 0.6 μB): initial magmoms set, so
  qe.relax (spin auto) runs them spin-polarized. Cr is bcc non-magnetic here (its real
  ground state is an incommensurate SDW, usually approximated so); Mn (α-Mn, 58 atoms,
  complex magnetism) and the lanthanides are not provided.
- Ga: α-Ga (Cmce, 4-atom primitive cell); In: body-centred tetragonal (I4/mmm).
"""
from __future__ import annotations

import math

import numpy as np

from ..model import Structure

# element -> (kind, params, magmom per atom or 0)
#   fcc/bcc/diamond: a;  hcp: a, c;  a7: a_hex, c_hex, u;  trigonal: a, c, x
#   bct (In): a, c;  cmce (Ga): a, b, c, y, z
REFERENCES: dict[str, tuple[str, tuple, float]] = {
    # alkali / alkaline earth / light
    "Li": ("bcc", (3.510,), 0.0),
    "Mg": ("hcp", (3.209, 5.211), 0.0),
    "Al": ("fcc", (4.050,), 0.0),
    "Si": ("diamond", (5.431,), 0.0),
    "Ge": ("diamond", (5.658,), 0.0),
    "Sn": ("diamond", (6.489,), 0.0),     # α-Sn
    "Pb": ("fcc", (4.950,), 0.0),
    "Ga": ("cmce", (4.520, 7.663, 4.526, 0.1549, 0.0810), 0.0),
    "In": ("bct", (3.252, 4.946), 0.0),
    "As": ("a7", (3.760, 10.548, 0.2271), 0.0),
    "Sb": ("a7", (4.307, 11.273, 0.2336), 0.0),
    "Bi": ("a7", (4.546, 11.862, 0.2341), 0.0),
    "Se": ("trigonal", (4.366, 4.954, 0.2254), 0.0),
    "Te": ("trigonal", (4.457, 5.929, 0.2636), 0.0),
    # 3d
    "Sc": ("hcp", (3.309, 5.273), 0.0),
    "Ti": ("hcp", (2.951, 4.686), 0.0),
    "V": ("bcc", (3.024,), 0.0),
    "Cr": ("bcc", (2.910,), 0.0),
    "Fe": ("bcc", (2.867,), 2.2),
    "Co": ("hcp", (2.507, 4.069), 1.6),
    "Ni": ("fcc", (3.524,), 0.6),
    "Cu": ("fcc", (3.615,), 0.0),
    "Zn": ("hcp", (2.665, 4.947), 0.0),
    # 4d
    "Y": ("hcp", (3.647, 5.731), 0.0),
    "Zr": ("hcp", (3.232, 5.147), 0.0),
    "Nb": ("bcc", (3.300,), 0.0),
    "Mo": ("bcc", (3.147,), 0.0),
    "Ru": ("hcp", (2.706, 4.282), 0.0),
    "Rh": ("fcc", (3.803,), 0.0),
    "Pd": ("fcc", (3.891,), 0.0),
    "Ag": ("fcc", (4.086,), 0.0),
    "Cd": ("hcp", (2.979, 5.619), 0.0),
    # 5d
    "Hf": ("hcp", (3.195, 5.051), 0.0),
    "Ta": ("bcc", (3.301,), 0.0),
    "W": ("bcc", (3.165,), 0.0),
    "Re": ("hcp", (2.761, 4.456), 0.0),
    "Os": ("hcp", (2.734, 4.317), 0.0),
    "Ir": ("fcc", (3.839,), 0.0),
    "Pt": ("fcc", (3.924,), 0.0),
    "Au": ("fcc", (4.078,), 0.0),
}


def _hex_lattice(a: float, c: float) -> list[list[float]]:
    return [[a, 0.0, 0.0], [-a / 2.0, a * math.sqrt(3.0) / 2.0, 0.0], [0.0, 0.0, c]]


def _rhombohedral_from_hex(a: float, c: float) -> np.ndarray:
    """Primitive rhombohedral vectors of an R-centred hexagonal cell (obverse)."""
    h = np.array(_hex_lattice(a, c))
    m = np.array([[2, 1, 1], [-1, 1, 1], [-1, -2, 1]]) / 3.0
    return m @ h


def reference_structure(element: str) -> Structure:
    """Reference Structure of an element; KeyError (listing the supported ones) if none."""
    if element not in REFERENCES:
        raise KeyError(f"no reference structure for {element!r} "
                       f"(have: {', '.join(sorted(REFERENCES))})")
    kind, p, mag = REFERENCES[element]
    if kind == "fcc":
        h = p[0] / 2
        lat, frac = [[0, h, h], [h, 0, h], [h, h, 0]], [[0, 0, 0]]
    elif kind == "bcc":
        h = p[0] / 2
        lat, frac = [[-h, h, h], [h, -h, h], [h, h, -h]], [[0, 0, 0]]
    elif kind == "diamond":
        h = p[0] / 2
        lat, frac = [[0, h, h], [h, 0, h], [h, h, 0]], [[0, 0, 0], [0.25, 0.25, 0.25]]
    elif kind == "hcp":
        lat = _hex_lattice(p[0], p[1])
        frac = [[1 / 3, 2 / 3, 0.25], [2 / 3, 1 / 3, 0.75]]
    elif kind == "a7":
        lat = _rhombohedral_from_hex(p[0], p[1]).tolist()
        u = p[2]
        frac = [[u, u, u], [-u, -u, -u]]          # (0,0,±u) hex = (±u,±u,±u) rhombohedral
    elif kind == "trigonal":                      # P3_121, 3a: (x,0,1/3) (0,x,2/3) (-x,-x,0)
        lat = _hex_lattice(p[0], p[1])
        x = p[2]
        frac = [[x, 0, 1 / 3], [0, x, 2 / 3], [-x, -x, 0]]
    elif kind == "bct":                           # I4/mmm 2a, primitive bct vectors
        a, c = p
        lat = [[-a / 2, a / 2, c / 2], [a / 2, -a / 2, c / 2], [a / 2, a / 2, -c / 2]]
        frac = [[0, 0, 0]]
    elif kind == "cmce":                          # α-Ga: Cmce, 8f (0, y, z)
        from pymatgen.core import Lattice, Structure as Pmg
        a, b, c, y, z = p
        conv = Pmg.from_spacegroup(64, Lattice.orthorhombic(a, b, c), [element], [[0, y, z]])
        prim = conv.get_primitive_structure()
        s = Structure.from_pymatgen(prim)
        return s.copy(prototype="alpha-Ga", source=f"crystalforge:reference:{element}",
                      magmoms=[mag] * s.nsites if mag else [])
    else:                                         # pragma: no cover - table typo
        raise ValueError(f"bad reference kind {kind!r}")
    n = len(frac)
    proto = {"a7": "A7", "trigonal": "A8", "bct": "A6"}.get(kind, kind)
    return Structure(lat, [element] * n, frac, prototype=proto,
                     source=f"crystalforge:reference:{element}",
                     magmoms=[mag] * n if mag else [])
