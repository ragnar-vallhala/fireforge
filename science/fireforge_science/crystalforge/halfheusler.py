"""Half-Heusler enumeration (phase8.md §4 crystal.halfheusler, §5).

XYZ half-Heusler, space group F-43m (216), C1b / MgAgAs type. Wyckoff positions of the
conventional cell: 4a (0,0,0), 4b (1/2,1/2,1/2), 4c (1/4,1/4,1/4) (4d (3/4,3/4,3/4) empty).
4a + 4b form a rock-salt sublattice; the 4c atom fills half of the tetrahedral holes
("stuffed" site) and is the only one with eight nearest neighbours of both other kinds.

Swapping the 4a and 4b atoms is a translation by (1/2,1/2,1/2) plus the 4c<->4d
relabelling (inversion of the whole crystal), so a composition {A, B, C} has exactly
three inequivalent orderings, labelled by the element on 4c:
`x4c`, `y4c`, `z4c` (the X, Y or Z role on the tetrahedral site).

Roles: X = electropositive (early transition / rare earth), Y = late transition metal,
Z = main-group element. The known ordering of the 18-electron TM half-Heuslers is Y on
4c, i.e. the late transition metal in the stuffed site (Ni in ZrNiSn, HfNiSn, TiNiSn;
Fe in NbFeSb; Co in ZrCoSb), X and Z forming the rock-salt sublattice — Graf, Felser,
Parkin, Prog. Solid State Chem. 39, 1 (2011), doi:10.1016/j.progsolidstchem.2011.02.001
(§ "Half-Heusler compounds"). `orderings: "known"` keeps only that one (`y4c`).

Primitive cell (fcc vectors a/2·(0,1,1), (1,0,1), (1,1,0)): fractional coordinates equal
the conventional ones, so 4a = (0,0,0), 4c = (1/4,1/4,1/4), 4b = (1/2,1/2,1/2) (same as
science/tests/conftest.halfheusler for `y4c`).

Valence electron count (VEC): groups 1–11 count their group number (s + d), group 12
counts 2 (filled d shell, as in the 8-electron LiZnAs-type compounds), groups 13–18
count group − 10 (s + p), lanthanides 3. 18 (ZrNiSn 4+10+4, NbFeSb 5+8+5, ZrCoSb 4+9+5)
and 8 (LiMgN-type) are the closed-shell counts (`electron_counts`, default [8, 18]).

Initial lattice constant: nearest-neighbour 4c–4a / 4c–4b distance a·√3/4 ≈ sum of
covalent radii (ASE table), averaged over the two bond types and scaled by
A_SCALE = 0.94, which reproduces the experimental a of the validation set to ~3 %
(ZrNiSn 6.10 vs 6.11 Å, TiNiSn 5.94 vs 5.93, ZrCoSb 6.14 vs 6.07, NbFeSb 6.16 vs 5.95).
It is only a starting point for qe.relax.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from itertools import product
from typing import Any

from ..model import Structure, _z

ORDERINGS = ("x4c", "y4c", "z4c")
KNOWN_ORDERING = "y4c"
A_SCALE = 0.94
PROTOTYPE = "half-heusler"

SITE_4A = (0.0, 0.0, 0.0)
SITE_4B = (0.5, 0.5, 0.5)
SITE_4C = (0.25, 0.25, 0.25)


def valence_electrons(el: str) -> int:
    """Valence electrons for the half-Heusler VEC rule (see module doc)."""
    from pymatgen.core import Element
    e = Element(el)
    if e.is_lanthanoid or e.is_actinoid:
        return 3
    g = int(e.group)
    if g <= 11:
        return g
    if g == 12:
        return 2
    return g - 10


def vec(elements) -> int:
    return sum(valence_electrons(e) for e in elements)


def fcc_lattice(a: float) -> list[list[float]]:
    h = a / 2.0
    return [[0.0, h, h], [h, 0.0, h], [h, h, 0.0]]


def site_assignment(x: str, y: str, z: str, ordering: str) -> dict[str, str]:
    """{"4a": el, "4b": el, "4c": el} for an ordering label."""
    if ordering not in ORDERINGS:
        raise ValueError(f"unknown ordering {ordering!r} (have {', '.join(ORDERINGS)})")
    roles = {"x": x, "y": y, "z": z}
    on_c = ordering[0]
    rest = [roles[r] for r in "xyz" if r != on_c]
    return {"4a": rest[0], "4b": rest[1], "4c": roles[on_c]}


def initial_a(sites: dict[str, str]) -> float:
    from ase.data import covalent_radii
    r = {k: float(covalent_radii[_z(v)]) for k, v in sites.items()}
    d = 0.5 * ((r["4c"] + r["4a"]) + (r["4c"] + r["4b"]))
    return A_SCALE * 4.0 * d / math.sqrt(3.0)


def build(x: str, y: str, z: str, ordering: str = KNOWN_ORDERING,
          a: float | None = None) -> Structure:
    """Primitive F-43m cell of XYZ in `ordering`; `a` (conventional, Å) defaults to the
    covalent-radius rule."""
    sites = site_assignment(x, y, z, ordering)
    a = a or initial_a(sites)
    return Structure(fcc_lattice(a), [sites["4a"], sites["4c"], sites["4b"]],
                     [list(SITE_4A), list(SITE_4C), list(SITE_4B)],
                     prototype=PROTOTYPE, source=f"crystalforge:halfheusler:{ordering}")


@dataclass
class Candidate:
    x: str
    y: str
    z: str
    ordering: str
    structure: Structure

    @property
    def composition(self) -> str:       # role order, e.g. "ZrNiSn" (a step-name-safe key)
        return f"{self.x}{self.y}{self.z}"

    @property
    def id(self) -> str:
        return f"{self.composition}.{self.ordering}"

    def to_json(self) -> dict[str, Any]:
        sites = site_assignment(self.x, self.y, self.z, self.ordering)
        return {"id": self.id, "composition": self.composition, "x": self.x, "y": self.y,
                "z": self.z, "ordering": self.ordering, "site_4a": sites["4a"],
                "site_4b": sites["4b"], "site_4c": sites["4c"],
                "vec": vec([self.x, self.y, self.z]),
                "known_ordering": self.ordering == KNOWN_ORDERING,
                "a_initial_angstrom": round(self.structure.lattice_array[0][1] * 2.0, 4),
                "structure": self.structure.to_json()}


def _clean(lst, what: str) -> list[str]:
    if isinstance(lst, str):
        lst = [s for s in lst.replace(",", " ").split() if s]
    if not lst:
        raise ValueError(f"element list {what!r} is empty")
    out = []
    for e in lst:
        e = str(e).strip()
        _z(e)                        # ValueError for unknown symbols
        if e not in out:
            out.append(e)
    return out


def enumerate_halfheuslers(x, y, z, electron_counts=(8, 18), orderings: str = "all",
                           allowed_elements=None) -> list[Candidate]:
    """Candidates for every (x, y, z) of distinct elements with VEC in `electron_counts`,
    in the requested orderings ("all" = the three, "known" = y4c only). Compositions
    reached by several role assignments (overlapping lists) are kept once (first
    assignment in list order); structures are deduplicated by fingerprint and then
    tolerantly with Structure.matches. `allowed_elements`: a set (e.g. the SSSP table);
    ValueError naming any element outside it."""
    xs, ys, zs = _clean(x, "x"), _clean(y, "y"), _clean(z, "z")
    if orderings not in ("all", "known"):
        raise ValueError(f"orderings must be 'all' or 'known', got {orderings!r}")
    if allowed_elements is not None:
        bad = sorted({e for e in xs + ys + zs if e not in allowed_elements})
        if bad:
            raise ValueError(f"no SSSP pseudopotential for element(s) {', '.join(bad)}")
    counts = {int(c) for c in electron_counts}
    ords = ORDERINGS if orderings == "all" else (KNOWN_ORDERING,)
    seen_sets: set[frozenset] = set()
    out: list[Candidate] = []
    for xe, ye, ze in product(xs, ys, zs):
        key = frozenset((xe, ye, ze))
        if len(key) < 3 or key in seen_sets:
            continue
        if vec([xe, ye, ze]) not in counts:
            continue
        seen_sets.add(key)
        group: list[Candidate] = []
        fps: set[str] = set()
        for o in ords:
            c = Candidate(xe, ye, ze, o, build(xe, ye, ze, o))
            fp = c.structure.fingerprint()
            if fp in fps or any(c.structure.matches(g.structure, ltol=0.05, stol=0.1)
                                for g in group):
                continue
            fps.add(fp)
            group.append(c)
        out.extend(group)
    return out
