"""Structure model (docs/design/phase8.md §3). CONTRACT: signatures fixed by the lead.

A Structure is the proto3-JSON form of fireforge.tdr.v1.Structure:
  {"lattice": [9 floats, Å, row-major a,b,c], "sites": [{"species": "Zr", "frac": [x,y,z]}],
   "formula", "reduced_formula", "spacegroup_number", "spacegroup_symbol", "prototype",
   "fingerprint", "parent_structure_id", "source", "magmoms": [...]}  (empty fields omitted)

Derived fields (formula, reduced_formula, spacegroup_*, fingerprint) are always recomputed
from lattice + sites; `from_json` ignores whatever values a document carries for them.

Fingerprint ("fp1:" + sha256, §3), computed by `fingerprint()`:

1. spglib `standardize_cell(to_primitive=True, symprec=1e-3)` (idealized: removes jitter
   below symprec from symmetric structures), species only (magmoms ignored);
2. spglib Niggli reduction, made right-handed;
3. canonical choice among the lattice automorphisms of the reduced cell (integer bases with
   entries in {-1,0,1}, det +1, same metric) and the origins at sites of the rarest species
   (ties: alphabetically first): the lexicographically smallest sorted site list;
4. lengths rounded to 1e-3 Å, angles to 1e-2°, fractional coordinates wrapped to [0,1) and
   rounded to 1e-3; canonical JSON (sorted keys, no spaces, fixed-format numbers); sha256.

Invariant under supercells, site permutation, rigid translation, rotation, lattice choice
and noise below symprec on symmetric structures (positions are idealized). Like any
rounded hash, a lattice length right on a 1e-3 Å rounding edge (e.g. a primitive length
of 4.3225 Å) can flip with tiny noise, so two nearly identical structures can get
different fingerprints; use `Structure.matches` (pymatgen StructureMatcher) where a
tolerant comparison is needed. Enantiomorphs get different
fingerprints (only proper basis changes are used).
"""
from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass, field
from functools import lru_cache
from itertools import product
from pathlib import Path
from typing import Any

import numpy as np

SYMPREC = 1e-3
FP_LEN_DECIMALS = 3       # Å
FP_ANGLE_DECIMALS = 2     # degrees
FP_FRAC_DECIMALS = 3


# ---------------------------------------------------------------------------------------
# element helpers (no pymatgen import needed)

@lru_cache(maxsize=None)
def _z(symbol: str) -> int:
    from ase.data import atomic_numbers
    try:
        return atomic_numbers[symbol]
    except KeyError:
        raise ValueError(f"unknown element symbol {symbol!r}") from None


def _symbol(z: int) -> str:
    from ase.data import chemical_symbols
    return chemical_symbols[int(z)]


def spglib_call(name: str, *args, **kw):
    """spglib.<name>(*args, **kw) with spglib's deprecation warning silenced; None on
    failure (whether spglib returns None or raises SpglibError)."""
    import warnings
    import spglib
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        try:
            return getattr(spglib, name)(*args, **kw)
        except spglib.error.SpglibError:
            return None


def _as_lattice(lattice) -> np.ndarray:
    a = np.asarray(lattice, dtype=float)
    if a.shape == (9,):
        a = a.reshape(3, 3)
    if a.shape != (3, 3):
        raise ValueError(f"lattice must be 3x3 or 9 floats, got shape {a.shape}")
    return a


def _wrap(f: np.ndarray, decimals: int | None = None) -> np.ndarray:
    """Wrap fractional coordinates into [0, 1); with `decimals`, also round so that values
    within rounding of 1.0 become 0.0."""
    f = np.asarray(f, dtype=float) % 1.0
    if decimals is not None:
        f = np.round(f, decimals) % 1.0
        f = np.round(f, decimals)
        f[f == 0.0] = 0.0          # no -0.0
    return f


# ---------------------------------------------------------------------------------------

@dataclass
class Structure:
    lattice: list[list[float]]                 # 3x3, Å, rows are vectors
    species: list[str]
    frac: list[list[float]]                    # n x 3
    prototype: str = ""
    source: str = ""
    parent_structure_id: str = ""
    magmoms: list[float] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.lattice = _as_lattice(self.lattice).tolist()
        self.species = [str(s) for s in self.species]
        fr = np.asarray(self.frac, dtype=float).reshape(-1, 3)
        if len(fr) != len(self.species):
            raise ValueError(f"{len(self.species)} species but {len(fr)} positions")
        if len(fr) == 0:
            raise ValueError("structure without sites")
        for s in set(self.species):
            _z(s)
        self.frac = fr.tolist()
        self.magmoms = [float(m) for m in (self.magmoms or [])]
        if self.magmoms and len(self.magmoms) != len(self.species):
            raise ValueError(f"{len(self.magmoms)} magmoms for {len(self.species)} sites")
        if abs(np.linalg.det(self.lattice_array)) < 1e-6:
            raise ValueError("singular lattice")

    @property
    def known_nonmagnetic(self) -> bool:
        """True when magmoms are given and all zero: a spin-polarized calculation found no
        moment (qe.relax sets this), so later calculations can skip spin polarization."""
        return bool(self.magmoms) and all(abs(m) < 1e-9 for m in self.magmoms)

    # ---- numpy views ------------------------------------------------------------------
    @property
    def lattice_array(self) -> np.ndarray:
        return np.asarray(self.lattice, dtype=float)

    @property
    def frac_array(self) -> np.ndarray:
        return np.asarray(self.frac, dtype=float)

    @property
    def cart(self) -> np.ndarray:
        """Cartesian positions (n x 3, Å)."""
        return self.frac_array @ self.lattice_array

    @property
    def elements(self) -> list[str]:
        """Distinct elements, sorted alphabetically."""
        return sorted(set(self.species))

    def copy(self, **changes) -> "Structure":
        d = dict(lattice=[list(r) for r in self.lattice], species=list(self.species),
                 frac=[list(r) for r in self.frac], prototype=self.prototype,
                 source=self.source, parent_structure_id=self.parent_structure_id,
                 magmoms=list(self.magmoms))
        d.update(changes)
        return Structure(**d)

    # ---- conversions ---------------------------------------------------------------
    @classmethod
    def from_json(cls, d: dict[str, Any]) -> "Structure":
        """Accepts the proto3 JSON form (proto field names or lowerCamelCase), with lattice
        as 9 floats or 3x3, and also a pymatgen Structure.as_dict(). Derived fields in `d`
        are ignored (recomputed)."""
        if isinstance(d, (str, bytes)):
            d = json.loads(d)
        if d.get("@class") == "Structure" and "@module" in d:
            from pymatgen.core import Structure as PmgStructure
            return cls.from_pymatgen(PmgStructure.from_dict(d))
        sites = d.get("sites") or []
        return cls(
            lattice=d["lattice"],
            species=[s["species"] for s in sites],
            frac=[list(s.get("frac") or [0.0, 0.0, 0.0]) for s in sites],
            prototype=d.get("prototype", ""),
            source=d.get("source", ""),
            parent_structure_id=d.get("parent_structure_id", d.get("parentStructureId", "")),
            magmoms=list(d.get("magmoms") or []),
        )

    def to_json(self) -> dict[str, Any]:
        """Full JSON incl. formula, reduced_formula, spacegroup_*, fingerprint (computed)."""
        num, sym = self.spacegroup()
        out: dict[str, Any] = {
            "lattice": [float(x) for x in self.lattice_array.reshape(9)],
            "sites": [{"species": sp, "frac": [float(x) for x in f]}
                      for sp, f in zip(self.species, self.frac)],
            "formula": self.formula(),
            "reduced_formula": self.reduced_formula(),
        }
        if num:
            out["spacegroup_number"] = num
            out["spacegroup_symbol"] = sym
        if self.prototype:
            out["prototype"] = self.prototype
        out["fingerprint"] = self.fingerprint()
        if self.parent_structure_id:
            out["parent_structure_id"] = self.parent_structure_id
        if self.source:
            out["source"] = self.source
        if self.magmoms:
            out["magmoms"] = list(self.magmoms)
        return out

    def to_proto(self):
        """fireforge.tdr.v1.record_pb2.Structure with every derived field filled."""
        from google.protobuf import json_format
        from fireforge.tdr.v1 import record_pb2
        return json_format.ParseDict(self.to_json(), record_pb2.Structure())

    @classmethod
    def from_proto(cls, msg) -> "Structure":
        from google.protobuf import json_format
        return cls.from_json(json_format.MessageToDict(msg, preserving_proto_field_name=True))

    @classmethod
    def from_pymatgen(cls, s, **kw) -> "Structure":
        if not s.is_ordered:
            raise ValueError("disordered structures are not supported")
        magmoms = kw.pop("magmoms", None)
        if magmoms is None and "magmom" in s.site_properties:
            magmoms = [float(m) for m in s.site_properties["magmom"]]
        return cls(lattice=s.lattice.matrix.tolist(),
                   species=[site.specie.symbol for site in s],
                   frac=s.frac_coords.tolist(), magmoms=magmoms or [], **kw)

    def to_pymatgen(self):
        from pymatgen.core import Lattice, Structure as PmgStructure
        props = {"magmom": list(self.magmoms)} if self.magmoms else None
        return PmgStructure(Lattice(self.lattice_array), self.species, self.frac_array,
                            site_properties=props)

    @classmethod
    def from_ase(cls, atoms, **kw) -> "Structure":
        magmoms = kw.pop("magmoms", None)
        if magmoms is None and atoms.has("initial_magmoms"):
            m = atoms.get_initial_magnetic_moments()
            if np.any(m != 0):
                magmoms = [float(x) for x in m]
        return cls(lattice=np.asarray(atoms.get_cell()).tolist(),
                   species=list(atoms.get_chemical_symbols()),
                   frac=atoms.get_scaled_positions(wrap=False).tolist(),
                   magmoms=magmoms or [], **kw)

    def to_ase(self):
        from ase import Atoms
        atoms = Atoms(symbols=self.species, scaled_positions=self.frac_array,
                      cell=self.lattice_array, pbc=True)
        if self.magmoms:
            atoms.set_initial_magnetic_moments(self.magmoms)
        return atoms

    def to_spglib_cell(self) -> tuple:
        """(lattice, frac, atomic numbers) for spglib (magmoms not included)."""
        return (self.lattice_array, self.frac_array,
                np.array([_z(s) for s in self.species], dtype=int))

    # ---- derived ---------------------------------------------------------------------
    @property
    def nsites(self) -> int: return len(self.species)

    def formula(self) -> str:                  # pymatgen Composition.formula
        return _composition(tuple(self.species)).formula

    def reduced_formula(self) -> str:          # pymatgen Composition.reduced_formula
        return _composition(tuple(self.species)).reduced_formula

    def volume(self) -> float:                 # Å^3
        return float(abs(np.linalg.det(self.lattice_array)))

    def spacegroup(self, symprec: float = SYMPREC) -> tuple[int, str]:
        """(number, Hermann-Mauguin symbol) from spglib; (0, "") if spglib fails."""
        ds = spglib_call("get_symmetry_dataset", self.to_spglib_cell(), symprec=symprec)
        if ds is None:
            return 0, ""
        return int(ds.number), str(ds.international)

    def primitive(self, symprec: float = SYMPREC) -> "Structure":
        """spglib standardized (idealized) primitive cell. Sites with different magmoms are
        treated as different types, so a magnetic structure keeps its magnetic cell.
        Metadata (prototype, source, parent) is kept; returns a copy of self if spglib
        fails."""
        types, table = self._typed()
        out = spglib_call("standardize_cell", (self.lattice_array, self.frac_array, types),
                          to_primitive=True, no_idealize=False, symprec=symprec)
        if out is None:
            return self.copy()
        lat, pos, nums = out
        sp = [table[int(t)][0] for t in nums]
        mm = [table[int(t)][1] for t in nums] if self.magmoms else []
        return Structure(lattice=np.asarray(lat).tolist(), species=sp,
                         frac=_wrap(pos).tolist(), prototype=self.prototype,
                         source=self.source, parent_structure_id=self.parent_structure_id,
                         magmoms=mm)

    def _typed(self) -> tuple[np.ndarray, list[tuple[str, float]]]:
        keys = [(s, round(self.magmoms[i], 2) if self.magmoms else 0.0)
                for i, s in enumerate(self.species)]
        table = sorted(set(keys), key=lambda k: (_z(k[0]), k[1]))
        index = {k: i + 1 for i, k in enumerate(table)}
        return np.array([index[k] for k in keys], dtype=int), [None] + table  # 1-based

    def fingerprint(self) -> str:
        """'fp1:' + sha256 of the canonical standardized primitive cell (§3). Invariant under
        supercells, site permutation, translation and lattice choice."""
        payload = canonical_form(self)
        text = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        return "fp1:" + hashlib.sha256(text.encode()).hexdigest()

    def matches(self, other: "Structure", ltol: float = 0.01, stol: float = 0.05,
                angle_tol: float = 0.5) -> bool:
        """Tolerant structural equality (pymatgen StructureMatcher, no volume scaling):
        use it where an exact fingerprint is too strict, e.g. for values on a rounding
        edge or noisy relaxed structures. ltol is fractional, angle_tol in degrees."""
        from pymatgen.analysis.structure_matcher import StructureMatcher
        if sorted(set(self.species)) != sorted(set(other.species)):
            return False
        m = StructureMatcher(ltol=ltol, stol=stol, angle_tol=angle_tol, primitive_cell=True,
                             scale=False, attempt_supercell=True)
        return bool(m.fit(self.to_pymatgen(), other.to_pymatgen()))

    def scaled(self, volume_strain: float) -> "Structure":  # isotropic: V -> V*(1+strain)
        if volume_strain <= -1.0:
            raise ValueError("volume strain must be > -1")
        f = (1.0 + volume_strain) ** (1.0 / 3.0)
        return self.copy(lattice=(self.lattice_array * f).tolist())

    def supercell(self, matrix) -> "Structure":
        """Supercell with integer matrix (3 ints or 3x3, rows: new vectors in old basis)."""
        m = np.asarray(matrix, dtype=int)
        if m.shape == (3,):
            m = np.diag(m)
        atoms = self.to_ase()
        from ase.build import make_supercell
        sc = make_supercell(atoms, m)
        return Structure.from_ase(sc, prototype=self.prototype, source=self.source,
                                  parent_structure_id=self.parent_structure_id)

    def __repr__(self) -> str:
        return (f"Structure({self.reduced_formula()}, nsites={self.nsites}, "
                f"V={self.volume():.3f} Å^3)")


@lru_cache(maxsize=4096)
def _composition(species: tuple[str, ...]):
    from pymatgen.core import Composition
    counts: dict[str, int] = {}
    for s in species:
        counts[s] = counts.get(s, 0) + 1
    return Composition(counts)


# ---------------------------------------------------------------------------------------
# fingerprint internals

def _cell_params(lat: np.ndarray) -> tuple[float, float, float, float, float, float]:
    a, b, c = (float(np.linalg.norm(v)) for v in lat)

    def ang(u, v):
        x = float(np.dot(u, v) / (np.linalg.norm(u) * np.linalg.norm(v)))
        return math.degrees(math.acos(max(-1.0, min(1.0, x))))
    return a, b, c, ang(lat[1], lat[2]), ang(lat[0], lat[2]), ang(lat[0], lat[1])


@lru_cache(maxsize=1)
def _unimodular_candidates() -> np.ndarray:
    """All 3x3 integer matrices with entries in {-1,0,1} and det +1."""
    m = np.array(list(product((-1, 0, 1), repeat=9)), dtype=int).reshape(-1, 3, 3)
    det = np.rint(np.linalg.det(m)).astype(int)
    return m[det == 1]


def _automorphisms(lat: np.ndarray, rtol: float = 1e-3) -> np.ndarray:
    """Integer bases M (det +1, entries -1..1) with M·G·Mᵀ ≈ G for the metric G of `lat`."""
    g = lat @ lat.T
    cands = _unimodular_candidates()
    gm = np.einsum("nij,jk,nlk->nil", cands, g, cands)
    tol = rtol * float(np.max(np.diag(g)))
    ok = np.all(np.abs(gm - g) <= tol, axis=(1, 2))
    return cands[ok]


def canonical_form(s: Structure, symprec: float = SYMPREC) -> dict[str, Any]:
    """The canonical (rounded) payload hashed by `Structure.fingerprint`."""
    cell = s.to_spglib_cell()
    std = spglib_call("standardize_cell", cell, to_primitive=True, no_idealize=False,
                      symprec=symprec)
    if std is None:
        lat, pos, nums = cell
    else:
        lat, pos, nums = (np.asarray(x) for x in std)
    lat = np.asarray(lat, dtype=float)
    pos = np.asarray(pos, dtype=float)
    nums = np.asarray(nums, dtype=int)

    red = spglib_call("niggli_reduce", lat, eps=1e-5)
    if red is not None:
        red = np.asarray(red, dtype=float)
        # positions into the reduced basis: r = f·L = f'·L'  =>  f' = f·L·L'^-1
        pos = pos @ lat @ np.linalg.inv(red)
        lat = red
    if np.linalg.det(lat) < 0:       # right-handed basis
        lat, pos = -lat, -pos
    pos = _wrap(pos)

    species = np.array([_symbol(z) for z in nums])
    counts = {sp: int(np.sum(species == sp)) for sp in set(species.tolist())}
    anchor = min(counts, key=lambda sp: (counts[sp], sp))
    anchors = np.nonzero(species == anchor)[0]
    order_sp = np.argsort(species, kind="stable")

    best = None
    best_lat = None
    for m in _automorphisms(lat):
        # new basis rows L' = M·L  =>  f' = f·M^-1
        f_new = pos @ np.linalg.inv(m)
        for o in anchors:
            f = _wrap(f_new - f_new[o], FP_FRAC_DECIMALS)
            rows = sorted(zip(species[order_sp].tolist(),
                              map(tuple, f[order_sp].tolist())))
            key = tuple(rows)
            if best is None or key < best:
                best = key
                best_lat = m @ lat
    params = _cell_params(best_lat)
    fl = f"{{:.{FP_LEN_DECIMALS}f}}"
    fa = f"{{:.{FP_ANGLE_DECIMALS}f}}"
    ff = f"{{:.{FP_FRAC_DECIMALS}f}}"
    return {
        "v": 1,
        "cell": [fl.format(round(x, FP_LEN_DECIMALS) + 0.0) for x in params[:3]]
        + [fa.format(round(x, FP_ANGLE_DECIMALS) + 0.0) for x in params[3:]],
        "sites": [[sp] + [ff.format(x) for x in f] for sp, f in best],
    }


# ---------------------------------------------------------------------------------------

def load_structure(path) -> Structure:     # JSON file (single structure)
    """Load a Structure from a JSON file (Structure JSON or pymatgen dict). Falls back to
    pymatgen for other formats (CIF, POSCAR) by file content."""
    p = Path(path)
    text = p.read_text()
    try:
        d = json.loads(text)
    except json.JSONDecodeError:
        from pymatgen.core import Structure as PmgStructure
        fmt = "cif" if "data_" in text else "poscar"
        return Structure.from_pymatgen(PmgStructure.from_str(text, fmt=fmt))
    if isinstance(d, dict) and "structure" in d and "lattice" not in d:
        d = d["structure"]
    return Structure.from_json(d)


def save_structure(s: Structure, path) -> None:
    Path(path).write_text(json.dumps(s.to_json(), indent=1) + "\n")
