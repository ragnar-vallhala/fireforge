"""pw.x output parsers (Quantum ESPRESSO 6.7).

`parse_xml(path)` reads `<prefix>.save/data-file-schema.xml` (QEXSD, qes-1.0 schema; all
quantities in Hartree atomic units: energies Ha, lengths Bohr, forces Ha/Bohr, stress
Ha/Bohr³, k-points cartesian in 2π/alat). `parse_stdout(text)` reads pw.x standard output
(Ry, kbar, Ry/Bohr; used as a fallback and for the version banner and the final
coordinates of a relaxation). `band_data(...)` builds the BoltzTraP2-ready BandData from
the XML of an nscf run. Everything returned is in eV / Å / eV/Å / kbar (QE stress sign:
positive = compressive).
"""
from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field

import numpy as np

from ..engine import BOHR_ANGSTROM, EV_A3_KBAR, HARTREE_EV, RY_EV, BandData, Provenance
from ..model import Structure

HA_BOHR3_KBAR = HARTREE_EV / BOHR_ANGSTROM ** 3 * EV_A3_KBAR    # 2.94210e5
HA_BOHR_EV_A = HARTREE_EV / BOHR_ANGSTROM
RY_BOHR_EV_A = RY_EV / BOHR_ANGSTROM


class ParseError(ValueError):
    pass


# ---------------------------------------------------------------------------------------
# XML

@dataclass
class PwXml:
    version: str
    species: list[str]                 # per site
    lattice: np.ndarray                # 3x3 Å, rows
    frac: np.ndarray                   # n x 3
    energy_ev: float
    fermi_ev: float | None             # smearing (or the single Fermi level)
    homo_ev: float | None
    lumo_ev: float | None
    forces_ev_a: np.ndarray | None     # n x 3
    stress_kbar: np.ndarray | None     # 3 x 3
    lsda: bool
    magnetization_mub: float           # total (0 if not spin-polarized)
    nelec: float
    nbnd_up: int                       # bands per spin channel (nbnd if not lsda)
    nbnd_dw: int
    kpoints_frac: np.ndarray           # nk x 3 (irreducible, as pw.x lists them)
    weights: np.ndarray                # nk, normalized to sum 1
    eigenvalues_ha: np.ndarray         # nk x (nbnd_up + nbnd_dw), Hartree (up then down)
    scf_converged: bool
    n_scf_steps: int
    opt_converged: bool | None         # None if not a relaxation
    n_opt_steps: int
    exit_status: int | None
    calculation: str = ""
    pseudo_files: dict[str, str] = field(default_factory=dict)

    def structure(self, like: Structure | None = None) -> Structure:
        """The (final) structure; metadata copied from `like`."""
        if like is not None:
            if list(like.species) != list(self.species):
                raise ParseError("XML species order differs from the input structure")
            return like.copy(lattice=self.lattice.tolist(), frac=(self.frac % 1.0).tolist(),
                             parent_structure_id="")
        return Structure(self.lattice.tolist(), self.species, (self.frac % 1.0).tolist())


def _floats(el: ET.Element | None) -> np.ndarray:
    if el is None or el.text is None:
        raise ParseError("missing numeric element")
    return np.array([float(x) for x in el.text.split()])


def _float(el: ET.Element | None) -> float | None:
    if el is None or el.text is None or not el.text.strip():
        return None
    return float(el.text.split()[0])


def _bool(el: ET.Element | None) -> bool | None:
    if el is None or el.text is None:
        return None
    return el.text.strip().lower() in ("true", "t", ".true.", "1")


def parse_xml(path) -> PwXml:
    """Parse data-file-schema.xml (QE 6.7 QEXSD). ParseError if `output` is missing."""
    try:
        root = ET.parse(path).getroot()
    except (ET.ParseError, OSError) as e:
        raise ParseError(f"{path}: {e}") from None
    out = root.find("output")
    if out is None:
        raise ParseError(f"{path}: no <output> element (pw.x did not finish?)")
    creator = root.find("general_info/creator")
    version = creator.get("VERSION", "unknown") if creator is not None else "unknown"
    calc_el = root.find("input/control_variables/calculation")
    calculation = calc_el.text.strip() if calc_el is not None and calc_el.text else ""

    # structure (cartesian Bohr)
    ast = out.find("atomic_structure")
    if ast is None:
        raise ParseError(f"{path}: no atomic_structure")
    cell = np.array([_floats(ast.find(f"cell/a{i}")) for i in (1, 2, 3)])
    atoms = ast.findall("atomic_positions/atom")
    species = [a.get("name", "").strip() for a in atoms]
    cart = np.array([_floats(a) for a in atoms]).reshape(-1, 3)
    frac = cart @ np.linalg.inv(cell)
    lattice = cell * BOHR_ANGSTROM
    alat = float(ast.get("alat", "0") or 0) or float(np.linalg.norm(cell[0]))
    pseudo_files = {}
    for sp in out.findall("atomic_species/species"):
        pf = sp.find("pseudo_file")
        if pf is not None and pf.text:
            pseudo_files[sp.get("name", "")] = pf.text.strip()

    etot = _float(out.find("total_energy/etot"))
    if etot is None:
        raise ParseError(f"{path}: no total energy")

    # convergence
    scf_conv = _bool(out.find("convergence_info/scf_conv/convergence_achieved"))
    n_scf = int(_float(out.find("convergence_info/scf_conv/n_scf_steps")) or 0)
    opt_el = out.find("convergence_info/opt_conv")
    opt_conv = _bool(opt_el.find("convergence_achieved")) if opt_el is not None else None
    n_opt = int(_float(opt_el.find("n_opt_steps")) or 0) if opt_el is not None else 0
    exit_status = _float(root.find("exit_status"))

    # magnetization
    lsda = bool(_bool(out.find("magnetization/lsda")))
    mag = _float(out.find("magnetization/total")) or 0.0

    # bands
    bs = out.find("band_structure")
    if bs is None:
        raise ParseError(f"{path}: no band_structure")
    lsda = bool(_bool(bs.find("lsda"))) or lsda
    if _bool(bs.find("noncolin")):
        raise ParseError(f"{path}: non-collinear calculations are not supported")
    nelec = _float(bs.find("nelec")) or 0.0
    fermi = _float(bs.find("fermi_energy"))
    two = bs.find("two_fermi_energies")
    if fermi is None and two is not None:
        fermi = float(np.mean(_floats(two)))
    homo = _float(bs.find("highestOccupiedLevel"))
    lumo = _float(bs.find("lowestUnoccupiedLevel"))
    ks = bs.findall("ks_energies")
    if not ks:
        raise ParseError(f"{path}: no ks_energies")
    kc = np.array([_floats(k.find("k_point")) for k in ks])
    weights = np.array([float(k.find("k_point").get("weight", "1")) for k in ks])
    eig = np.array([_floats(k.find("eigenvalues")) for k in ks])
    nb_up = _float(bs.find("nbnd_up"))
    nb_dw = _float(bs.find("nbnd_dw"))
    if lsda:
        if nb_up is None or nb_dw is None:
            nb_up = nb_dw = eig.shape[1] // 2
        nb_up, nb_dw = int(nb_up), int(nb_dw)
    else:
        nb_up, nb_dw = int(_float(bs.find("nbnd")) or eig.shape[1]), 0
    if eig.shape[1] != nb_up + nb_dw:
        raise ParseError(f"{path}: {eig.shape[1]} eigenvalues per k, expected {nb_up + nb_dw}")
    # cartesian (2π/alat) -> fractional: k_i = k · a_i / alat
    kfrac = kc @ cell.T / alat
    kfrac[np.abs(kfrac) < 1e-12] = 0.0
    weights = weights / weights.sum()

    forces = stress = None
    fe = out.find("forces")
    if fe is not None and fe.text and fe.text.strip():
        forces = _floats(fe).reshape(-1, 3) * HA_BOHR_EV_A    # order F, dims 3 x nat
    se = out.find("stress")
    if se is not None and se.text and se.text.strip():
        stress = _floats(se).reshape(3, 3).T * HA_BOHR3_KBAR

    return PwXml(version=version, species=species, lattice=lattice, frac=frac,
                 energy_ev=etot * HARTREE_EV,
                 fermi_ev=None if fermi is None else fermi * HARTREE_EV,
                 homo_ev=None if homo is None else homo * HARTREE_EV,
                 lumo_ev=None if lumo is None else lumo * HARTREE_EV,
                 forces_ev_a=forces, stress_kbar=stress, lsda=lsda,
                 magnetization_mub=float(mag) if lsda else 0.0, nelec=nelec,
                 nbnd_up=nb_up, nbnd_dw=nb_dw, kpoints_frac=kfrac, weights=weights,
                 eigenvalues_ha=eig, scf_converged=bool(scf_conv), n_scf_steps=n_scf,
                 opt_converged=opt_conv, n_opt_steps=n_opt,
                 exit_status=None if exit_status is None else int(exit_status),
                 calculation=calculation, pseudo_files=pseudo_files)


def band_data(x: PwXml, structure: Structure, provenance: Provenance) -> BandData:
    """BandData from an nscf XML: irreducible k-points as pw.x lists them, eigenvalues in
    Hartree (nbands x nk); spin-polarized: both channels stacked along the band axis,
    dosweight 1 (else 2). lattvec: the XML cell (Bohr), columns; the structure gets the
    XML lattice so the two agree exactly."""
    eb = x.eigenvalues_ha.T                   # (nbnd_up + nbnd_dw) x nk, up then down
    s = x.structure(like=structure)
    fermi = x.fermi_ev if x.fermi_ev is not None else (
        x.homo_ev if x.lumo_ev is None else 0.5 * (x.homo_ev + x.lumo_ev))
    if fermi is None:
        raise ParseError("no Fermi energy / HOMO in the nscf output")
    return BandData(kpoints=x.kpoints_frac, ebands=eb,
                    lattvec=(x.lattice / BOHR_ANGSTROM).T, nelect=x.nelec,
                    fermi=fermi / HARTREE_EV, dosweight=1.0 if x.lsda else 2.0,
                    structure=s, provenance=provenance)


# ---------------------------------------------------------------------------------------
# stdout

@dataclass
class PwStdout:
    version: str
    energies_ev: list[float]               # every "!" total energy, in order
    fermi_ev: float | None
    homo_ev: float | None
    lumo_ev: float | None
    magnetization_mub: float | None        # last "total magnetization"
    forces_ev_a: np.ndarray | None         # last force block
    stress_kbar: np.ndarray | None         # last stress block (kbar)
    pressure_kbar: float | None
    scf_converged: bool                    # last scf converged (no "NOT achieved")
    n_scf_cycles: int
    bfgs_converged: bool | None            # None if no BFGS
    final_lattice: np.ndarray | None       # Å, from "Begin final coordinates"
    final_species: list[str] | None
    final_frac: np.ndarray | None
    job_done: bool

    @property
    def energy_ev(self) -> float | None:
        return self.energies_ev[-1] if self.energies_ev else None


_NUM = r"[-+]?\d*\.?\d+(?:[eEdD][-+]?\d+)?"


def _f(s: str) -> float:
    return float(s.replace("d", "e").replace("D", "e"))


def parse_stdout(text: str) -> PwStdout:
    m = re.search(r"Program PWSCF v\.?\s*(\S+)", text)
    version = m.group(1) if m else "unknown"
    energies = [_f(v) * RY_EV for v in
                re.findall(rf"^!\s+total energy\s+=\s+({_NUM})\s+Ry", text, re.M)]
    fermi = homo = lumo = None
    for m in re.finditer(rf"the Fermi energy is\s+({_NUM})\s+ev", text):
        fermi = _f(m.group(1))
    for m in re.finditer(rf"the spin up/dw Fermi energies are\s+({_NUM})\s+({_NUM})\s+ev", text):
        fermi = 0.5 * (_f(m.group(1)) + _f(m.group(2)))
    for m in re.finditer(rf"highest occupied, lowest unoccupied level \(ev\):\s+({_NUM})\s+({_NUM})",
                         text):
        homo, lumo = _f(m.group(1)), _f(m.group(2))
    for m in re.finditer(rf"highest occupied level \(ev\):\s+({_NUM})", text):
        homo = _f(m.group(1))
    mags = re.findall(rf"total magnetization\s+=\s+({_NUM})\s+Bohr mag/cell", text)
    mag = _f(mags[-1]) if mags else None

    forces = None
    blocks = re.split(r"Forces acting on atoms.*?\n", text)
    if len(blocks) > 1:
        rows = []
        for line in blocks[-1].splitlines():
            fm_ = re.match(rf"\s*atom\s+\d+\s+type\s+\d+\s+force =\s+({_NUM})\s+({_NUM})"
                           rf"\s+({_NUM})", line)
            if fm_:
                rows.append([_f(x) for x in fm_.groups()])
            elif line.strip() and rows:
                break                      # first block only (verbosity high adds terms)
        if rows:
            forces = np.array(rows) * RY_BOHR_EV_A
    stress = pressure = None
    sm = list(re.finditer(rf"total\s+stress\s+\(Ry/bohr\*\*3\)\s+\(kbar\)\s+P=\s*({_NUM})\s*\n"
                          rf"((?:\s*(?:{_NUM}\s+){{5}}{_NUM}\s*\n){{3}})", text))
    if sm:
        pressure = _f(sm[-1].group(1))
        vals = [_f(v) for v in sm[-1].group(2).split()]
        stress = np.array([vals[3:6], vals[9:12], vals[15:18]])
    n_cycles = len(re.findall(r"End of self-consistent calculation", text))
    scf_ok = "convergence NOT achieved" not in text and (
        n_cycles > 0 or "End of band structure calculation" in text)
    bfgs = None
    if "BFGS Geometry Optimization" in text:
        bfgs = bool(re.search(r"bfgs converged in", text)) and \
            "The maximum number of steps has been reached" not in text
    lat = sp = fr = None
    fm = re.search(r"Begin final coordinates(.*?)End final coordinates", text, re.S)
    if fm:
        lat, sp, fr = _final_coordinates(fm.group(1))
    return PwStdout(version=version, energies_ev=energies, fermi_ev=fermi, homo_ev=homo,
                    lumo_ev=lumo, magnetization_mub=mag, forces_ev_a=forces,
                    stress_kbar=stress, pressure_kbar=pressure, scf_converged=scf_ok,
                    n_scf_cycles=n_cycles, bfgs_converged=bfgs, final_lattice=lat,
                    final_species=sp, final_frac=fr, job_done="JOB DONE" in text)


def _final_coordinates(block: str):
    lat = None
    cm = re.search(rf"CELL_PARAMETERS\s*\(?\s*(alat\s*=\s*({_NUM})|angstrom|bohr)\s*\)?\s*\n"
                   rf"((?:\s*{_NUM}\s+{_NUM}\s+{_NUM}\s*\n){{3}})", block)
    if cm:
        vals = np.array([_f(v) for v in cm.group(3).split()]).reshape(3, 3)
        unit = cm.group(1)
        if unit.startswith("alat"):
            vals = vals * _f(cm.group(2)) * BOHR_ANGSTROM
        elif unit == "bohr":
            vals = vals * BOHR_ANGSTROM
        lat = vals
    pm = re.search(r"ATOMIC_POSITIONS\s*\(?\s*(\w+)\s*\)?\s*\n(.*)", block, re.S)
    if not pm:
        return lat, None, None
    unit = pm.group(1)
    species, pos = [], []
    for line in pm.group(2).splitlines():
        parts = line.split()
        if len(parts) < 4 or not re.fullmatch(r"[A-Z][a-z]?\d*", parts[0]):
            if species:
                break
            continue
        try:
            xyz = [_f(v) for v in parts[1:4]]
        except ValueError:
            break
        species.append(re.sub(r"\d+$", "", parts[0]))
        pos.append(xyz)
    p = np.array(pos)
    if unit != "crystal":
        if lat is None:
            return lat, species, None
        scale = {"angstrom": 1.0, "bohr": BOHR_ANGSTROM}.get(unit)
        if scale is None:
            return lat, species, None
        p = (p * scale) @ np.linalg.inv(lat)
    return lat, species, p
