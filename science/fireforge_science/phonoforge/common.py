"""Shared helpers for the PhonoForge operations: parameters, phonopy conversion, the
`qe.forces` output contract, record method.

`qe.forces` contract (quantumforge): input `supercell` (Structure JSON), parameters
`engine`, `preset`, `cpus`, `label`; output `forces` = npz with `forces` (n×3, eV/Å, in
the order of the supercell's sites), `energy` (eV, 0-d) and — required by PhonoForge for
robust matching — `label` (0-d str, the `label` parameter). Optional `provenance` (0-d
str, engine.Provenance JSON) is used for the record's functional/pseudopotentials.

The collect/kappa3 steps receive each forces file under the input name
`forces.<label>`; the label is taken from the npz `label` key first, then from the input
name, then from the file name (`...forces.<label>.npz` / `<label>.npz`).
"""
from __future__ import annotations

import json
import re
import warnings
from pathlib import Path
from typing import Any

import numpy as np

from ..engine import Provenance
from ..model import Structure

DEFAULT_T_K = [300.0, 500.0, 800.0, 1100.0]
STABILITY_THRESHOLD_THZ = -0.1
FORCES_INPUT_PREFIX = "forces."


class PhononError(ValueError):
    """Bad parameters or inputs (not retryable)."""


# ---- parameters ---------------------------------------------------------------------------

def supercell_matrix(value: Any, default=(2, 2, 2)) -> list[list[int]]:
    """3 ints, one int or a 3×3 integer matrix (rows: new vectors in the unit-cell basis)."""
    v = default if value is None else value
    try:
        m = np.asarray(v, dtype=float)
    except (TypeError, ValueError):
        raise PhononError(f"supercell must be an int, 3 ints or a 3x3 integer matrix, got {value!r}") from None
    if m.ndim == 0:
        m = np.eye(3) * m
    elif m.shape == (3,):
        m = np.diag(m)
    if m.shape != (3, 3) or not np.allclose(m, np.round(m)):
        raise PhononError(f"supercell must be an int, 3 ints or a 3x3 integer matrix, got {value!r}")
    m = np.round(m).astype(int)
    if round(abs(np.linalg.det(m))) < 1:
        raise PhononError(f"singular supercell matrix {m.tolist()}")
    return m.tolist()


def mesh_numbers(value: Any, default: int = 20) -> list[int]:
    v = default if value is None else value
    m = np.atleast_1d(np.asarray(v, dtype=int))
    if m.shape == (1,):
        m = np.repeat(m, 3)
    if m.shape != (3,) or (m < 1).any():
        raise PhononError(f"mesh must be an int or 3 positive ints, got {value!r}")
    return [int(x) for x in m]


def temperatures(value: Any) -> list[float]:
    t = DEFAULT_T_K if value is None else value
    t = [float(x) for x in (t if isinstance(t, (list, tuple)) else [t])]
    if not t or any(x <= 0 for x in t):
        raise PhononError(f"t_k must be positive temperatures, got {value!r}")
    return t


def common_params(params: dict) -> dict:
    """engine / preset / cpus with the phase8 §4/§5 defaults."""
    cpus = int(params.get("cpus", 2))
    if cpus < 1:
        raise PhononError("cpus must be >= 1")
    return {"engine": str(params.get("engine", "qe")),
            "preset": str(params.get("preset", "screening")), "cpus": cpus}


def forces_capabilities(engine: str) -> list[str]:
    return ["sci", "qe"] if engine == "qe" else ["sci"]


# ---- structures ---------------------------------------------------------------------------

def to_phonopy_atoms(s: Structure):
    from phonopy.structure.atoms import PhonopyAtoms
    return PhonopyAtoms(symbols=list(s.species), cell=s.lattice_array,
                        scaled_positions=s.frac_array)


def from_phonopy_atoms(a, **kw) -> Structure:
    return Structure(lattice=np.asarray(a.cell).tolist(), species=list(a.symbols),
                     frac=np.asarray(a.scaled_positions).tolist(), **kw)


def light_json(s: Structure) -> dict:
    """Structure JSON without derived fields (cheap for displaced supercells; parses with
    Structure.from_json)."""
    return {"lattice": [float(x) for x in s.lattice_array.reshape(9)],
            "sites": [{"species": sp, "frac": [float(x) for x in f]}
                      for sp, f in zip(s.species, s.frac)]}


def quiet():
    """Context manager silencing phonopy's informational warnings."""
    cm = warnings.catch_warnings()

    class _Q:
        def __enter__(self):
            cm.__enter__()
            warnings.simplefilter("ignore")

        def __exit__(self, *exc):
            return cm.__exit__(*exc)
    return _Q()


def jsonable(obj: Any) -> Any:
    """numpy → plain JSON types (recursively)."""
    if isinstance(obj, dict):
        return {str(k): jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [jsonable(v) for v in obj]
    if isinstance(obj, np.ndarray):
        return jsonable(obj.tolist())
    if isinstance(obj, np.bool_):
        return bool(obj)
    if isinstance(obj, np.integer):
        return int(obj)
    if isinstance(obj, np.floating):
        return float(obj)
    return obj


# ---- forces inputs ------------------------------------------------------------------------

_FILE_LABEL = re.compile(r"(?:forces\.)?([A-Za-z0-9_.-]+?)(?:\.npz)?$")


def read_forces(path: Path, input_name: str = "") -> dict:
    """Load a `qe.forces` npz → {label, forces (n×3), energy, provenance (dict|None)}.
    The label comes from the npz `label`, else the input name `forces.<label>`, else
    the file name."""
    with np.load(path, allow_pickle=False) as z:
        if "forces" not in z:
            raise PhononError(f"{input_name or path}: npz without 'forces'")
        forces = np.asarray(z["forces"], dtype=float)
        energy = float(z["energy"]) if "energy" in z else float("nan")
        label = str(z["label"]) if "label" in z else ""
        prov = json.loads(str(z["provenance"])) if "provenance" in z else None
    if not label and input_name.startswith(FORCES_INPUT_PREFIX):
        label = input_name[len(FORCES_INPUT_PREFIX):].split("#", 1)[0]
    if not label:
        m = _FILE_LABEL.fullmatch(Path(path).name)
        label = m.group(1) if m else ""
    if not label:
        raise PhononError(f"{input_name or path}: cannot tell which supercell these forces "
                          "belong to (no 'label' in the npz and no label in the name)")
    return {"label": label, "forces": forces, "energy": energy, "provenance": prov}


def collect_forces(context) -> dict[str, dict]:
    """All forces inputs of a context, by label. Inputs named `forces.*` (or `forces`,
    `forces#n` with a label inside the npz)."""
    out: dict[str, dict] = {}
    for ref in context.inputs:
        name = ref.name or ""
        if not (name == "forces" or name.startswith("forces.") or name.startswith("forces#")):
            continue
        f = read_forces(ref.path, name)
        if f["label"] in out:
            raise PhononError(f"two forces inputs for supercell {f['label']!r}")
        out[f["label"]] = f
    return out


def stack_forces(by_label: dict[str, dict], labels: list[str], natom: int) -> np.ndarray:
    missing = [lb for lb in labels if lb not in by_label]
    if missing:
        raise PhononError(f"missing forces for {len(missing)} supercell(s): "
                          f"{', '.join(missing[:8])}{' ...' if len(missing) > 8 else ''}")
    arr = []
    for lb in labels:
        f = by_label[lb]["forces"]
        if f.shape != (natom, 3):
            raise PhononError(f"forces for {lb}: shape {f.shape}, expected ({natom}, 3)")
        arr.append(f)
    return np.array(arr)


# ---- provenance ---------------------------------------------------------------------------

def method(solver: str, version: str, engine: str, preset: str, settings: dict,
           forces_provenance: dict | None = None) -> dict:
    """Record method (engine.Provenance.method) for a phonopy/phono3py result built on
    forces from `engine`."""
    functional = "PBE" if engine == "qe" else engine
    pseudos: list[str] = []
    st = dict(settings)
    if forces_provenance:
        functional = forces_provenance.get("functional", functional)
        pseudos = list(forces_provenance.get("pseudopotentials", []))
        st["forces"] = {"solver": forces_provenance.get("solver", ""),
                        "solver_version": forces_provenance.get("solver_version", ""),
                        "settings": forces_provenance.get("settings", {})}
    return Provenance(solver=solver, solver_version=version, engine=engine,
                      functional=functional, pseudopotentials=pseudos,
                      settings=jsonable(st)).method(preset)


def first_provenance(by_label: dict[str, dict]) -> dict | None:
    for k in sorted(by_label):
        if by_label[k]["provenance"]:
            return by_label[k]["provenance"]
    return None


