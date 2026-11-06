"""Engine protocol (docs/design/phase8.md §7). CONTRACT: signatures fixed by the lead.

Engines: "qe" (quantumforge.QeEngine, real Quantum ESPRESSO) and "toy" (toy.ToyEngine,
analytic stand-in for tests). Everything an operation needs from a solver goes through this.
Energies in eV, forces eV/Å, stress kbar (QE sign: positive = compressive), lengths Å.
"""
from __future__ import annotations

import copy
import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Protocol

import numpy as np

from .model import Structure
from .settings import Settings

HARTREE_EV = 27.211386245988
BOHR_ANGSTROM = 0.529177210903
RY_EV = HARTREE_EV / 2.0
EV_A3_KBAR = 1602.1766208          # 1 eV/Å^3 in kbar


@dataclass
class Provenance:
    solver: str                      # "quantum-espresso" | "toy"
    solver_version: str
    engine: str                      # "qe" | "toy"
    functional: str = "PBE"
    pseudopotentials: list[str] = field(default_factory=list)  # "<file> md5:<hex>"
    settings: dict[str, Any] = field(default_factory=dict)     # what was actually used

    def to_json(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_json(cls, d: dict[str, Any]) -> "Provenance":
        return cls(**d)

    def method(self, preset: str = "") -> dict[str, Any]:
        """Keyword arguments for a record's `method` (record.proto Method, proto field
        names; settings as settings_json)."""
        return {"solver": self.solver, "solver_version": self.solver_version,
                "engine": self.engine, "functional": self.functional,
                "pseudopotentials": list(self.pseudopotentials),
                "preset": preset or str(self.settings.get("preset", "")),
                "settings_json": json.dumps(self.settings, sort_keys=True, default=str)}


@dataclass
class RelaxResult:
    structure: Structure             # relaxed (parent_structure_id left to the caller)
    energy_ev: float                 # total, final
    pressure_kbar: float
    magnetization_mub: float         # total cell magnetization (0 if non-spin-polarized)
    converged: bool
    steps: int
    log: str                         # solver stdout (saved as output "log")
    provenance: Provenance


@dataclass
class ScfResult:
    energy_ev: float
    fermi_ev: float
    forces: list[list[float]]        # n x 3
    stress_kbar: list[list[float]]   # 3 x 3
    converged: bool
    save_dir: Path | None            # QE prefix.save (None for toy)
    log: str
    provenance: Provenance


@dataclass
class BandData:
    """BoltzTraP2-compatible band data (saved as output "bands", npz).
    kpoints: (nk,3) fractional, irreducible or full; ebands: (nbands, nk) Hartree;
    lattvec: (3,3) Bohr, columns are vectors; nelect; fermi (Hartree); dosweight (2 if
    non-spin-polarized); structure for BoltzTraP2's atoms.

    Spin-polarized data (dosweight 1) stacks both spin channels along the band axis, as
    BoltzTraP2 does. The object can be passed to BoltzTraP2 directly (`atoms`, `magmom`,
    `mommat`, `get_lattvec`, `get_volume`, `bandana` like BoltzTraP2.dft.DFTData);
    `bandana` mutates, so use `boltztrap_data()` for a disposable copy.

    npz layout: kpoints, ebands, lattvec (float64), nelect, fermi, dosweight (0-d float),
    structure (0-d str: Structure JSON), provenance (0-d str: Provenance JSON),
    format ("fireforge.banddata.v1"). Loads with allow_pickle=False."""
    kpoints: Any
    ebands: Any
    lattvec: Any
    nelect: float
    fermi: float
    dosweight: float
    structure: Structure
    provenance: Provenance

    FORMAT = "fireforge.banddata.v1"
    mommat = None                  # no band derivatives
    magmom = None                  # BoltzTraP2: non-collinear/collinear magmoms (unused)

    def __post_init__(self) -> None:
        self.kpoints = np.asarray(self.kpoints, dtype=float).reshape(-1, 3)
        self.ebands = np.atleast_2d(np.asarray(self.ebands, dtype=float))
        self.lattvec = np.asarray(self.lattvec, dtype=float).reshape(3, 3)
        if self.ebands.shape[1] != len(self.kpoints):
            raise ValueError(f"ebands {self.ebands.shape} does not match "
                             f"{len(self.kpoints)} k-points")
        self.nelect = float(self.nelect)
        self.fermi = float(self.fermi)
        self.dosweight = float(self.dosweight)

    # ---- persistence -------------------------------------------------------------------
    def save(self, path) -> None:
        """Write the npz to `path` exactly (no ".npz" appended)."""
        with open(path, "wb") as f:
            np.savez_compressed(
                f, format=np.array(self.FORMAT), kpoints=self.kpoints, ebands=self.ebands,
                lattvec=self.lattvec, nelect=np.array(self.nelect),
                fermi=np.array(self.fermi), dosweight=np.array(self.dosweight),
                structure=np.array(json.dumps(self.structure.to_json())),
                provenance=np.array(json.dumps(self.provenance.to_json())))

    @classmethod
    def load(cls, path) -> "BandData":
        with np.load(path, allow_pickle=False) as z:
            if "format" in z and str(z["format"]) != cls.FORMAT:
                raise ValueError(f"{path}: unsupported band data format {z['format']}")
            return cls(kpoints=z["kpoints"], ebands=z["ebands"], lattvec=z["lattvec"],
                       nelect=float(z["nelect"]), fermi=float(z["fermi"]),
                       dosweight=float(z["dosweight"]),
                       structure=Structure.from_json(json.loads(str(z["structure"]))),
                       provenance=Provenance.from_json(json.loads(str(z["provenance"]))))

    # ---- BoltzTraP2 data-object interface ------------------------------------------------
    @property
    def atoms(self):
        """ASE Atoms of the structure (BoltzTraP2 symmetry analysis)."""
        return self.structure.to_ase()

    sysname = property(lambda self: self.structure.reduced_formula())

    def get_lattvec(self) -> np.ndarray:
        return self.lattvec

    def get_volume(self) -> float:
        return float(abs(np.linalg.det(self.lattvec)))

    def bandana(self, emin: float = -np.inf, emax: float = np.inf) -> np.ndarray:
        """Keep bands overlapping (emin, emax) (Hartree); adjusts nelect like DFTData."""
        bmin = self.ebands.min(axis=1)
        bmax = self.ebands.max(axis=1)
        ntoolow = int(np.count_nonzero(bmax <= emin))
        accepted = np.logical_and(bmin < emax, bmax > emin)
        self.ebands = self.ebands[accepted]
        self.nelect -= self.dosweight * ntoolow
        return accepted

    def boltztrap_data(self) -> "BandData":
        """A copy safe to hand to BoltzTraP2 (bandana mutates)."""
        c = copy.copy(self)
        c.ebands = self.ebands.copy()
        c.kpoints = self.kpoints.copy()
        return c

    # ---- analysis ---------------------------------------------------------------------
    def band_gap(self) -> tuple[float, bool, float, float]:
        """(gap_ev, direct, vbm_ev, cbm_ev); gap 0 for metals.

        Insulator test by electron count: nocc = nelect/dosweight states per k are filled
        (both spin channels stacked when dosweight 1); the structure is an insulator if
        nocc is an integer and the nocc-th lowest state at every k lies below the
        (nocc+1)-th lowest at every k. VBM/CBM are those extrema; `direct` if the smallest
        vertical gap equals the gap (1 meV). Metals return (0, False, fermi, fermi)."""
        e = np.sort(self.ebands, axis=0) * HARTREE_EV        # sorted states per k
        fermi_ev = self.fermi * HARTREE_EV
        nocc_f = self.nelect / self.dosweight
        nocc = int(round(nocc_f))
        if abs(nocc_f - nocc) > 1e-4 or nocc < 1 or nocc >= e.shape[0]:
            return 0.0, False, fermi_ev, fermi_ev
        vb = e[nocc - 1]
        cb = e[nocc]
        vbm = float(vb.max()) + 0.0       # no -0.0
        cbm = float(cb.min()) + 0.0
        if cbm - vbm <= 1e-6:
            return 0.0, False, fermi_ev, fermi_ev
        gap = cbm - vbm
        direct = bool(float((cb - vb).min()) - gap < 1e-3)
        return gap, direct, vbm + 0.0, cbm + 0.0


class Engine(Protocol):
    name: str
    def relax(self, s: Structure, settings: Settings, *, fix_cell: bool = False, workdir: Path) -> RelaxResult: ...
    def scf(self, s: Structure, settings: Settings, *, workdir: Path) -> ScfResult: ...
    def nscf(self, s: Structure, save_dir: Path | None, settings: Settings, *, workdir: Path) -> BandData: ...
    def forces(self, s: Structure, settings: Settings, *, workdir: Path) -> ScfResult: ...
    # Optional (both engines provide it): stress(s, settings, *, workdir) -> 3x3 kbar.


def get_engine(name: str, *, cpus: int = 1) -> Engine:
    """'qe' -> quantumforge.QeEngine(cpus=cpus); 'toy' -> toy.ToyEngine(). ValueError otherwise.
    Imports are lazy: the core works without quantumforge installed (ImportError then
    only when 'qe' is requested)."""
    if name == "toy":
        from .toy import ToyEngine
        return ToyEngine()
    if name == "qe":
        from .quantumforge import QeEngine
        return QeEngine(cpus=cpus)
    raise ValueError(f"unknown engine {name!r} (qe | toy)")
