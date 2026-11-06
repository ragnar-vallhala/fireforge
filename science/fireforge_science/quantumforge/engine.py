"""QeEngine: the Engine protocol on Quantum ESPRESSO pw.x (docs/design/phase8.md §7).

Every calculation runs in its own directory under `workdir` (pw.in, pw.out, tmp/ff.save):
  relax   vc-relax (relax if fix_cell), BFGS; final structure/energy from the XML
  scf     tprnfor + tstress; ScfResult.save_dir = <workdir>/tmp/ff.save
  nscf    copies a save dir to <workdir>/tmp/ff.save, dense automatic mesh (pw.x reduces it
          to the irreducible wedge; nosym false), nbnd from the saved nelec, same spin mode
  forces  scf with tprnfor (the supercell's own mesh from kdensity)
  stress  scf, 3x3 stress (kbar)
Values come from data-file-schema.xml; stdout is the fallback.
"""
from __future__ import annotations

import shutil
from pathlib import Path
from typing import Callable

import numpy as np

from .. import pseudo
from ..engine import BandData, Provenance, RelaxResult, ScfResult
from ..model import Structure
from ..settings import Settings
from . import inputs, parse, runner

SOLVER = "quantum-espresso"


class QeEngine:
    name = "qe"

    def __init__(self, cpus: int = 1, *, timeout: float | None = None,
                 cancelled: Callable[[], bool] | None = None) -> None:
        self.cpus = max(1, int(cpus))
        self.timeout = timeout
        self.cancelled = cancelled
        self._counter = 0

    # ---- plumbing ---------------------------------------------------------------------
    def _calcdir(self, workdir: Path | None, label: str) -> Path:
        if workdir is None:
            raise ValueError("QeEngine needs a workdir")
        self._counter += 1
        d = Path(workdir) / f"{self._counter:02d}-{label}"
        if d.exists():
            shutil.rmtree(d)
        d.mkdir(parents=True)
        return d

    def _run(self, s: Structure, settings: Settings, calculation: str, d: Path,
             **kw) -> tuple[str, parse.PwXml | None, str]:
        els = inputs.species_order(s)
        text = inputs.pw_input(s, settings, calculation, pseudo_dir=str(pseudo.pseudo_dir()),
                               pseudos={el: pseudo.filename(el) for el in els}, **kw)
        log = runner.run_pw(text, d, self.cpus, timeout=self.timeout, cancelled=self.cancelled)
        xmlp = d / inputs.OUTDIR / f"{inputs.PREFIX}.save" / "data-file-schema.xml"
        x = parse.parse_xml(xmlp) if xmlp.is_file() else None
        return log, x, text

    def provenance(self, s: Structure, settings: Settings, version: str, **used) -> Provenance:
        mag = inputs.starting_magnetization(s, settings)
        st = {"preset": settings.preset, "ecutwfc_ry": settings.ecutwfc_ry,
              "ecutrho_ry": settings.ecutrho_ry, "smearing": settings.smearing,
              "degauss_ry": settings.degauss_ry,
              "conv_thr_ry": settings.conv_thr_ry_per_atom * s.nsites,
              "nspin": 2 if mag else 1, "cpus": self.cpus, **used}
        if mag:
            st["starting_magnetization"] = mag
        return Provenance(solver=SOLVER, solver_version=version, engine="qe", functional="PBE",
                          pseudopotentials=pseudo.provenance(s.elements), settings=st)

    # ---- Engine protocol --------------------------------------------------------------
    def relax(self, s: Structure, settings: Settings, *, fix_cell: bool = False,
              workdir: Path | None = None) -> RelaxResult:
        r, _ = self.relax_with_stress(s, settings, fix_cell=fix_cell, workdir=workdir)
        return r

    def relax_with_stress(self, s: Structure, settings: Settings, *, fix_cell: bool = False,
                          workdir: Path | None = None) -> tuple[RelaxResult, list[list[float]]]:
        """relax / vc-relax; also returns the final stress (kbar) (tstress on)."""
        calc = "relax" if fix_cell else "vc-relax"
        d = self._calcdir(workdir, calc)
        log, x, _ = self._run(s, settings, calc, d, tstress=True)
        so = parse.parse_stdout(log)
        if x is not None:
            out = x.structure(like=s)
            energy = x.energy_ev
            stress = x.stress_kbar
            mag = x.magnetization_mub
            conv = bool(x.opt_converged) if x.opt_converged is not None else bool(so.bfgs_converged)
            steps = x.n_opt_steps or so.n_scf_cycles
            version = x.version
        else:
            if so.final_frac is None or so.energy_ev is None:
                raise runner.PwError("relax: no final structure in the output", retryable=True,
                                     log_tail=runner.tail(log))
            lat = so.final_lattice if so.final_lattice is not None else s.lattice_array
            out = s.copy(lattice=lat.tolist(), frac=(so.final_frac % 1.0).tolist(),
                         parent_structure_id="")
            energy, stress = so.energy_ev, so.stress_kbar
            mag = so.magnetization_mub or 0.0
            conv, steps, version = bool(so.bfgs_converged), so.n_scf_cycles, so.version
        if stress is None:
            stress = so.stress_kbar
        if stress is None:
            raise runner.PwError("relax: no stress in the output", retryable=True,
                                 log_tail=runner.tail(log))
        stress = np.asarray(stress)
        prov = self.provenance(s, settings, version, calculation=calc, fix_cell=fix_cell,
                               kmesh=list(inputs.resolve_kmesh(s, settings, False)),
                               forc_conv_thr_ry_bohr=settings.forc_conv_thr_ry_bohr,
                               press_conv_thr_kbar=settings.press_conv_thr_kbar)
        res = RelaxResult(structure=out, energy_ev=float(energy),
                          pressure_kbar=float(np.trace(stress) / 3.0),
                          magnetization_mub=float(mag), converged=conv, steps=int(steps),
                          log=log, provenance=prov)
        return res, stress.tolist()

    def scf(self, s: Structure, settings: Settings, *, workdir: Path | None = None) -> ScfResult:
        return self._scf(s, settings, workdir, "scf")

    def forces(self, s: Structure, settings: Settings, *, workdir: Path | None = None) -> ScfResult:
        return self._scf(s, settings, workdir, "forces")

    def stress(self, s: Structure, settings: Settings, *,
               workdir: Path | None = None) -> list[list[float]]:
        return self._scf(s, settings, workdir, "stress").stress_kbar

    def _scf(self, s: Structure, settings: Settings, workdir: Path | None,
             label: str) -> ScfResult:
        d = self._calcdir(workdir, label)
        log, x, _ = self._run(s, settings, "scf", d, tstress=True, tprnfor=True)
        so = parse.parse_stdout(log)
        if x is not None:
            energy, version = x.energy_ev, x.version
            fermi = x.fermi_ev if x.fermi_ev is not None else x.homo_ev
            forces, stress, conv = x.forces_ev_a, x.stress_kbar, x.scf_converged
        else:
            energy, version, conv = so.energy_ev, so.version, so.scf_converged
            fermi = so.fermi_ev if so.fermi_ev is not None else so.homo_ev
            forces, stress = so.forces_ev_a, so.stress_kbar
        if forces is None:
            forces = so.forces_ev_a
        if stress is None:
            stress = so.stress_kbar
        if energy is None or forces is None or stress is None:
            raise runner.PwError("scf: energy/forces/stress missing from the output",
                                 retryable=True, log_tail=runner.tail(log))
        save = d / inputs.OUTDIR / f"{inputs.PREFIX}.save"
        prov = self.provenance(s, settings, version, calculation="scf",
                               kmesh=list(inputs.resolve_kmesh(s, settings, False)))
        return ScfResult(energy_ev=float(energy), fermi_ev=float(fermi or 0.0),
                         forces=np.asarray(forces).tolist(),
                         stress_kbar=np.asarray(stress).tolist(), converged=bool(conv),
                         save_dir=save if save.is_dir() else None, log=log, provenance=prov)

    def nscf(self, s: Structure, save_dir: Path | None, settings: Settings, *,
             workdir: Path | None = None) -> BandData:
        if save_dir is None or not (Path(save_dir) / "data-file-schema.xml").is_file():
            raise ValueError(f"nscf needs a pw.x save directory with data-file-schema.xml "
                             f"(got {save_dir})")
        saved = parse.parse_xml(Path(save_dir) / "data-file-schema.xml")
        d = self._calcdir(workdir, "nscf")
        dst = d / inputs.OUTDIR / f"{inputs.PREFIX}.save"
        shutil.copytree(save_dir, dst)
        # same spin mode as the scf that produced the charge density
        spin = "on" if saved.lsda else "off"
        st = Settings(**{**settings.to_json(), "spin": spin})
        nbnd = int((st.extra or {}).get("nbnd") or inputs.default_nbnd(saved.nelec, saved.lsda))
        log, x, _ = self._run(s, st, "nscf", d, nbnd=nbnd)
        if x is None:
            raise runner.PwError("nscf: no data-file-schema.xml written", retryable=True,
                                 log_tail=runner.tail(log))
        mesh = inputs.resolve_kmesh(s, st, True)
        prov = self.provenance(s, st, x.version, calculation="nscf", kmesh_dense=list(mesh),
                               nbnd=nbnd, nk_irreducible=int(len(x.kpoints_frac)))
        bd = parse.band_data(x, s, prov)
        bd.log = log  # type: ignore[attr-defined]  (ops save it as output "log")
        return bd
