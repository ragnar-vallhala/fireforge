"""pw.x input writer (Quantum ESPRESSO 6.7, docs/design/phase8.md §4, §7).

`pw_input(structure, settings, calculation, ...)` returns the text of a pw.x input file:
&CONTROL / &SYSTEM / &ELECTRONS (+ &IONS, &CELL for relaxations), ATOMIC_SPECIES (SSSP
files, ASE masses), CELL_PARAMETERS angstrom, ATOMIC_POSITIONS crystal and K_POINTS
(automatic Γ-centred mesh from Settings, or an explicit `tpiba`-free crystal list).

Settings.extra keys understood here (all optional):
  kmesh        [n1, n2, n3]  fixed scf mesh (overrides kdensity; used for strained cells)
  kmesh_dense  [n1, n2, n3]  fixed nscf mesh
  nbnd         int           number of bands (nscf picks a default from nelect)
  mixing_beta  float         (default 0.4)
  pw           {"SYSTEM": {"key": value}, ...}  raw namelist overrides (last word)

Output is deterministic (golden-file tested): keys in a fixed order, floats in fixed
formats.
"""
from __future__ import annotations

import math
from typing import Any, Mapping, Sequence

from ..model import Structure
from ..settings import MAGNETIC_ELEMENTS, Settings

PREFIX = "ff"
OUTDIR = "./tmp"
CALCULATIONS = ("scf", "nscf", "relax", "vc-relax")
STARTING_MAGNETIZATION = 0.5       # magnetic elements (Fe/Co/Ni/Mn/Cr) when spin-polarized
STARTING_MAGNETIZATION_OTHER = 0.2 # all species, if none of them is magnetic but spin is on
MAGMOM_SCALE = 5.0                 # starting_magnetization = mean magmom (μB) / 5, clipped


def species_order(s: Structure) -> list[str]:
    """Distinct species in order of first appearance (ATOMIC_SPECIES order)."""
    return list(dict.fromkeys(s.species))


def _mass(el: str) -> float:
    from ase.data import atomic_masses, atomic_numbers
    return float(atomic_masses[atomic_numbers[el]])


def fortran(v: Any) -> str:
    """A namelist value."""
    if isinstance(v, bool):
        return ".true." if v else ".false."
    if isinstance(v, int):
        return str(v)
    if isinstance(v, float):
        if v != 0.0 and (abs(v) < 1e-3 or abs(v) >= 1e6):
            m, e = f"{v:.6e}".split("e")
            m = m.rstrip("0").rstrip(".") if "." in m else m
            return f"{m}d{int(e)}"
        return repr(float(f"{v:.10g}"))
    return "'" + str(v).replace("'", "") + "'"


def starting_magnetization(s: Structure, settings: Settings) -> dict[str, float]:
    """species -> starting_magnetization for nspin=2 (empty if not spin-polarized)."""
    els = species_order(s)
    if not settings.spin_polarized(els):
        return {}
    if s.magmoms and any(m != 0 for m in s.magmoms):
        out = {}
        for el in els:
            ms = [m for sp, m in zip(s.species, s.magmoms) if sp == el]
            out[el] = max(-1.0, min(1.0, sum(ms) / len(ms) / MAGMOM_SCALE))
        if any(out.values()):
            return out
    out = {el: (STARTING_MAGNETIZATION if el in MAGNETIC_ELEMENTS else 0.0) for el in els}
    if not any(out.values()):
        out = {el: STARTING_MAGNETIZATION_OTHER for el in els}
    return out


def default_nbnd(nelect: float, spin: bool) -> int:
    """Bands for an nscf meant for transport: occupied states + ~50 % (at least +8)."""
    nocc = math.ceil(nelect / 2.0) if not spin else math.ceil(nelect / 2.0) + 2
    return int(max(nocc + 8, math.ceil(1.5 * nocc)))


def pw_input(s: Structure, settings: Settings, calculation: str, *,
             pseudo_dir: str, pseudos: Mapping[str, str],
             kpoints: Sequence[Sequence[float]] | None = None,
             kmesh: Sequence[int] | None = None,
             nbnd: int | None = None, tstress: bool = True, tprnfor: bool = True,
             prefix: str = PREFIX, outdir: str = OUTDIR) -> str:
    """pw.x input text.

    pseudos: element -> UPF file name (pseudo.filename). kmesh: explicit automatic mesh
    (else Settings.extra kmesh / kmesh_dense, else Settings.kmesh(dense=nscf)).
    kpoints: explicit list of fractional k-points (weights equal) instead of a mesh.
    ecutwfc/ecutrho must be resolved (settings.preset() does that)."""
    if calculation not in CALCULATIONS:
        raise ValueError(f"calculation must be one of {CALCULATIONS}, got {calculation!r}")
    if not settings.ecutwfc_ry or not settings.ecutrho_ry:
        raise ValueError("ecutwfc/ecutrho not resolved (use settings.preset)")
    els = species_order(s)
    missing = [el for el in els if el not in pseudos]
    if missing:
        raise ValueError(f"no pseudopotential for {', '.join(missing)}")
    nat = s.nsites
    extra = settings.extra or {}
    relax = calculation in ("relax", "vc-relax")

    control: dict[str, Any] = {
        "calculation": calculation,
        "prefix": prefix,
        "outdir": outdir,
        "pseudo_dir": str(pseudo_dir),
        "disk_io": "low",
    }
    if calculation != "nscf":
        control["tprnfor"] = bool(tprnfor or relax)
        control["tstress"] = bool(tstress or calculation == "vc-relax")
    if relax:
        control["nstep"] = 100
        control["etot_conv_thr"] = 1e-5 * nat
        control["forc_conv_thr"] = float(settings.forc_conv_thr_ry_bohr)

    system: dict[str, Any] = {
        "ibrav": 0,
        "nat": nat,
        "ntyp": len(els),
        "ecutwfc": float(settings.ecutwfc_ry),
        "ecutrho": float(settings.ecutrho_ry),
        "occupations": "smearing",
        "smearing": settings.smearing,
        "degauss": float(settings.degauss_ry),
    }
    if calculation == "nscf":
        nb = nbnd if nbnd is not None else extra.get("nbnd")
        if nb:
            system["nbnd"] = int(nb)
        system["nosym"] = False
    mag = starting_magnetization(s, settings)
    if mag:
        system["nspin"] = 2
        for i, el in enumerate(els, start=1):
            if mag[el]:
                system[f"starting_magnetization({i})"] = float(mag[el])

    electrons: dict[str, Any] = {
        "conv_thr": float(settings.conv_thr_ry_per_atom) * nat,
        "mixing_beta": float(extra.get("mixing_beta", 0.4)),
        "electron_maxstep": 200,
    }
    if calculation == "nscf":
        electrons["diago_full_acc"] = True

    namelists: list[tuple[str, dict[str, Any]]] = [
        ("CONTROL", control), ("SYSTEM", system), ("ELECTRONS", electrons)]
    if relax:
        namelists.append(("IONS", {"ion_dynamics": "bfgs"}))
    if calculation == "vc-relax":
        namelists.append(("CELL", {"cell_dynamics": "bfgs",
                                   "press_conv_thr": float(settings.press_conv_thr_kbar),
                                   "cell_dofree": "all"}))
    for nl, over in (extra.get("pw") or {}).items():
        target = next((d for n, d in namelists if n == nl.upper()), None)
        if target is None:
            target = {}
            namelists.append((nl.upper(), target))
        target.update(over)

    lines: list[str] = []
    for name, d in namelists:
        lines.append(f"&{name}")
        for k, v in d.items():
            lines.append(f"  {k} = {fortran(v)}")
        lines.append("/")
    lines.append("ATOMIC_SPECIES")
    for el in els:
        lines.append(f"  {el} {_mass(el):.6f} {pseudos[el]}")
    lines.append("CELL_PARAMETERS angstrom")
    for row in s.lattice_array:
        lines.append("  " + " ".join(f"{x:16.10f}" for x in row))
    lines.append("ATOMIC_POSITIONS crystal")
    for sp, f in zip(s.species, s.frac_array):
        lines.append(f"  {sp:<3s}" + " ".join(f"{x:15.10f}" for x in f))
    if kpoints is not None:
        kp = [list(map(float, k)) for k in kpoints]
        lines.append("K_POINTS crystal")
        lines.append(f"  {len(kp)}")
        w = 1.0 / len(kp)
        for k in kp:
            lines.append("  " + " ".join(f"{x:14.10f}" for x in k) + f" {w:.10f}")
    else:
        mesh = resolve_kmesh(s, settings, calculation == "nscf", kmesh)
        lines.append("K_POINTS automatic")
        lines.append(f"  {mesh[0]} {mesh[1]} {mesh[2]} 0 0 0")
    return "\n".join(lines) + "\n"


def resolve_kmesh(s: Structure, settings: Settings, dense: bool,
                  kmesh: Sequence[int] | None = None) -> tuple[int, int, int]:
    """The automatic mesh pw_input uses (explicit > Settings.extra > density)."""
    if kmesh is None:
        kmesh = (settings.extra or {}).get("kmesh_dense" if dense else "kmesh")
    if kmesh is not None:
        m = tuple(int(x) for x in kmesh)
        if len(m) != 3 or min(m) < 1:
            raise ValueError(f"bad k-point mesh {kmesh!r}")
        return m  # type: ignore[return-value]
    return settings.kmesh(s.lattice, dense=dense)
