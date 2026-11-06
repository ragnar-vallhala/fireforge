"""Tier 2: third-order force constants and RTA Boltzmann transport with phono3py
(phonon.fc3 / phonon.kappa3, phase8.md §4).

phonon.fc3 builds the phono3py displacement dataset on the unit cell of the `fc2` input
(the reference volume of phonon.collect, so the harmonic and anharmonic parts describe
the same crystal), supercell `supercell` (default 2×2×2 primitive), amplitude
`amplitude` (default 0.03 Å, phono3py's default) and `cutoff_pair_a` (default 4.0 Å):
pairs farther apart than the cutoff are not calculated (phono3py sets their fc3 to 0),
which bounds the number of supercells. Only the supercells phono3py needs are spawned,
labelled d<nnnn> = 1-based index into phono3py's supercells_with_displacements.

phonon.kappa3 reads fc2 from the phonopy yaml, rebuilds fc3 from the forces and runs the
RTA thermal conductivity (no isotope scattering, no boundary scattering) on `mesh`
(default 11³, a screening mesh) at `t_k`. κ_l = trace/3 of the tensor.
"""
from __future__ import annotations

from typing import Any

import numpy as np

from ..model import SYMPREC, Structure
from .common import (PhononError, from_phonopy_atoms, jsonable, light_json, quiet,
                     stack_forces, to_phonopy_atoms)

PLAN3_FORMAT = "fireforge.phonon-plan3.v1"
KAPPA3_FORMAT = "fireforge.kappa3.v1"


def load_fc2(path):
    """Phonopy object (with force constants) from the `fc2` yaml of phonon.collect."""
    import phonopy
    with quiet():
        ph = phonopy.load(str(path), produce_fc=False, log_level=0)
    if ph.force_constants is None:
        raise PhononError("fc2 file has no force constants")
    return ph


def _make(unitcell: Structure, sc_matrix, ph_sc_matrix):
    from phono3py import Phono3py
    with quiet():
        return Phono3py(to_phonopy_atoms(unitcell), supercell_matrix=sc_matrix,
                        phonon_supercell_matrix=ph_sc_matrix, primitive_matrix="P",
                        symprec=SYMPREC)


def _ds_to_json(ds: dict) -> dict:
    return jsonable({k: ds[k] for k in ("natom", "first_atoms", "cutoff_distance", "duplicates")
                     if k in ds})


def _ds_from_json(d: dict) -> dict:
    out: dict[str, Any] = {"natom": int(d["natom"]), "first_atoms": []}
    if "cutoff_distance" in d:
        out["cutoff_distance"] = float(d["cutoff_distance"])
    if "duplicates" in d:
        out["duplicates"] = np.asarray(d["duplicates"], dtype=int).reshape(-1, 2)
    for fa in d["first_atoms"]:
        f = {"number": int(fa["number"]), "displacement": np.asarray(fa["displacement"], dtype=float),
             "second_atoms": []}
        if "id" in fa:
            f["id"] = int(fa["id"])
        for sa in fa.get("second_atoms", []):
            s = {"number": int(sa["number"]),
                 "displacement": np.asarray(sa["displacement"], dtype=float)}
            for k in ("id", "pair_distance", "included"):
                if k in sa:
                    s[k] = sa[k]
            f["second_atoms"].append(s)
        out["first_atoms"].append(f)
    return out


def build_plan3(structure: Structure, fc2_ph, *, supercell, amplitude: float,
                cutoff_pair_a: float | None, engine: str, preset: str) -> dict:
    unit = from_phonopy_atoms(fc2_ph.unitcell)
    if not structure.primitive().matches(unit):
        raise PhononError("fc2 was computed for a different structure than `structure`")
    ph_sc = np.asarray(fc2_ph.supercell_matrix, dtype=int).tolist()
    ph3 = _make(unit, supercell, ph_sc)
    with quiet():
        ph3.generate_displacements(distance=amplitude, cutoff_pair_distance=cutoff_pair_a)
    scs = []
    for i, a in enumerate(ph3.supercells_with_displacements):
        if a is not None:
            scs.append({"label": f"d{i + 1:04d}", "index": i,
                        "structure": light_json(from_phonopy_atoms(a))})
    return {"format": PLAN3_FORMAT, "structure": structure.to_json(),
            "unitcell": light_json(unit), "supercell_matrix": supercell,
            "phonon_supercell_matrix": ph_sc, "amplitude_a": amplitude,
            "cutoff_pair_a": cutoff_pair_a, "engine": engine, "preset": preset,
            "natom_supercell": len(ph3.supercell),
            "n_displacements_total": len(ph3.supercells_with_displacements),
            "dataset": _ds_to_json(ph3.dataset), "supercells": scs}


def run_kappa3(plan3: dict, forces_by_label: dict, fc2_ph, *, mesh: list[int],
               t_k: list[float]) -> dict:
    unit = Structure.from_json(plan3["unitcell"])
    fc2_unit = from_phonopy_atoms(fc2_ph.unitcell)
    if not (np.allclose(unit.lattice_array, fc2_unit.lattice_array, atol=1e-5)
            and unit.species == fc2_unit.species):
        raise PhononError("plan3 and fc2 have different unit cells")
    ph3 = _make(unit, plan3["supercell_matrix"], plan3["phonon_supercell_matrix"])
    ph3.dataset = _ds_from_json(plan3["dataset"])
    natom = len(ph3.supercell)
    ntot = len(ph3.supercells_with_displacements)
    forces = np.zeros((ntot, natom, 3))
    labels = [sc["label"] for sc in plan3["supercells"]]
    got = stack_forces(forces_by_label, labels, natom)
    for sc, f in zip(plan3["supercells"], got):
        forces[int(sc["index"])] = f
    ph3.forces = forces
    with quiet():
        ph3.produce_fc3()
        ph3.fc2 = np.asarray(fc2_ph.force_constants)
        ph3.mesh_numbers = mesh
        ph3.init_phph_interaction()
        ph3.run_thermal_conductivity(temperatures=list(t_k), write_kappa=False)
    tc = ph3.thermal_conductivity
    kt = np.asarray(tc.kappa)[0]                     # (nT, 6): xx yy zz yz xz xy
    kl = kt[:, :3].mean(axis=1)
    return jsonable({
        "format": KAPPA3_FORMAT, "method": "phono3py-rta",
        "formula": unit.reduced_formula(), "engine": plan3.get("engine", ""),
        "preset": plan3.get("preset", ""), "t_k": list(t_k),
        "kappa_l_w_mk": kl.tolist(), "kappa_tensor_w_mk": kt.tolist(),
        "tensor_order": ["xx", "yy", "zz", "yz", "xz", "xy"],
        "mesh": list(mesh), "supercell_matrix": plan3["supercell_matrix"],
        "phonon_supercell_matrix": plan3["phonon_supercell_matrix"],
        "cutoff_pair_a": plan3.get("cutoff_pair_a"), "amplitude_a": plan3["amplitude_a"],
        "n_supercells": len(labels), "isotope": False, "boundary": None,
    })
