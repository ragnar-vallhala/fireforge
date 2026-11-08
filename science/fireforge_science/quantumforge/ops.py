"""qe.* operations (docs/design/phase8.md §4): qe.relax, qe.scf, qe.nscf, qe.forces,
qe.elastic. Each works with `engine` "qe" (pw.x) or "toy" (ToyEngine, same outputs and
records, method.engine = "toy").

Common parameters: `preset` (screening | accurate), `engine` (qe | toy, default qe), `cpus`
(pw.x MPI ranks, default 1), `settings` (dict of Settings overrides, e.g. {"ecutwfc_ry":
40}), `spin` (auto | on | off), `timeout_s` (per pw.x run). Calculations run under
<workdir>/calc.

Outputs (names → files):
  qe.relax   structure (structure.json, Structure JSON), log (relax.log)
  qe.scf     save (save.tar.zst or save.tar.gz: the prefix.save directory "ff.save/…"
             without wavefunctions; toy: a stub "toy.save/scf.json"), log (scf.log)
  qe.nscf    bands (bands.npz, BandData npz "fireforge.banddata.v1"), log (nscf.log)
  qe.forces  forces (forces.npz: format "fireforge.forces.v1", forces (n,3) eV/Å,
             energy eV, stress_kbar (3,3), label, natoms, species (str, space separated),
             provenance (JSON str))
  qe.elastic elastic (elastic.json, see `elastic_constants`)
"""
from __future__ import annotations

import io
import json
import math
import shutil
import subprocess
import tarfile
from dataclasses import fields, replace
from pathlib import Path
from typing import Any

import numpy as np

from ..engine import BandData, get_engine
from ..model import Structure, load_structure, spglib_call
from ..settings import PRESETS, Settings, preset

SAVE_EXCLUDE_PREFIXES = ("wfc",)      # wavefunctions are not needed by nscf
ZSTD_MAGIC = b"\x28\xb5\x2f\xfd"
GZIP_MAGIC = b"\x1f\x8b"


def _worker_error(msg: str, retryable: bool = False):
    from fireforge import WorkerError
    return WorkerError(msg, retryable=retryable)


# ---------------------------------------------------------------------------------------
# helpers

def input_structure(ctx, name: str) -> tuple[Structure, str]:
    """(Structure, structure_id) from input `name` (or the only input). structure_id is the
    document's "structure_id" / "structureId" if it carries one (control-plane exports)."""
    path = ctx.named_inputs.get(name)
    if path is None:
        unnamed = [i.path for i in ctx.inputs if not i.name]
        if len(ctx.inputs) == 1:
            path = ctx.inputs[0].path
        elif len(unnamed) == 1:
            path = unnamed[0]
        else:
            raise _worker_error(f"missing input {name!r}")
    try:
        s = load_structure(path)
    except Exception as e:  # noqa: BLE001 - bad input is the caller's fault
        raise _worker_error(f"input {name!r}: not a structure: {e}") from None
    sid = ""
    try:
        d = json.loads(Path(path).read_text())
        if isinstance(d, dict):
            sid = str(d.get("structure_id") or d.get("structureId") or "")
    except (ValueError, OSError):
        pass
    return s, sid


def make_settings(params: dict[str, Any], elements, engine: str) -> Settings:
    """Settings from params: preset, settings overrides, spin, kdensity, kdensity_dense,
    cpus. The toy engine does not need SSSP (cutoffs stay 0 unless given)."""
    name = params.get("preset", "screening")
    over = dict(params.get("settings") or {})
    for k in ("spin", "kdensity", "kdensity_dense"):
        if k in params:
            over[k] = params[k]
    over["cpus"] = int(params.get("cpus", 1))
    known = {f.name for f in fields(Settings)}
    bad = set(over) - known
    if bad:
        raise _worker_error(f"unknown settings: {', '.join(sorted(bad))}")
    try:
        if engine == "toy":
            if name not in PRESETS:
                raise KeyError(f"unknown preset {name!r}")
            return replace(Settings(), preset=name, **{**PRESETS[name], **over})
        return preset(name, list(elements), **over)
    except (KeyError, TypeError, ValueError) as e:
        raise _worker_error(f"settings: {e}") from None


def make_engine(ctx, params: dict[str, Any]):
    name = params.get("engine", "qe")
    try:
        eng = get_engine(name, cpus=int(params.get("cpus", 1)))
    except ValueError as e:
        raise _worker_error(str(e)) from None
    if name == "qe":
        eng.cancelled = lambda: ctx.cancelled
        if params.get("timeout_s"):
            eng.timeout = float(params["timeout_s"])
    return name, eng


def calc_dir(ctx, op: str) -> Path:
    d = Path(ctx.workdir) / "calc" / op
    d.mkdir(parents=True, exist_ok=True)
    return d


def _wrap(fn):
    """Map PwError / PseudoError to WorkerError with the right retryability."""
    def task(ctx):
        from .runner import PwError
        from ..pseudo import PseudoError
        try:
            return fn(ctx)
        except PwError as e:
            raise _worker_error(str(e), retryable=e.retryable) from None
        except (PseudoError, FileNotFoundError) as e:
            raise _worker_error(f"{type(e).__name__}: {e}", retryable=False) from None
    task.__name__ = fn.__name__
    task.__doc__ = fn.__doc__
    return task


def conventional_a(s: Structure) -> float:
    """Length of the first vector of the spglib conventional standard cell (Å): the
    cubic lattice constant for a half-Heusler."""
    ds = spglib_call("get_symmetry_dataset", s.to_spglib_cell(), symprec=1e-3)
    if ds is None:
        return float(np.linalg.norm(s.lattice_array[0]))
    return float(np.linalg.norm(np.asarray(ds.std_lattice)[0]))


# ---------------------------------------------------------------------------------------
# save directory archive

def _zstd() -> str | None:
    return shutil.which("zstd")


def pack_save(save_dir: Path | None, dest: Path, stub: dict | None = None) -> Path:
    """tar of the save dir (members "<name>.save/…", wavefunctions excluded), zstd when the
    zstd tool exists else gzip. dest is the path without compression suffix; returns the
    file written (".tar.zst" / ".tar.gz"). stub: write a toy archive instead."""
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tar:
        if save_dir is None:
            data = (json.dumps(stub or {}, indent=1, sort_keys=True) + "\n").encode()
            ti = tarfile.TarInfo("toy.save/scf.json")
            ti.size, ti.mtime = len(data), 0
            tar.addfile(ti, io.BytesIO(data))
        else:
            save_dir = Path(save_dir)
            for p in sorted(save_dir.rglob("*")):
                if p.is_file() and not p.name.startswith(SAVE_EXCLUDE_PREFIXES):
                    tar.add(p, arcname=f"{save_dir.name}/{p.relative_to(save_dir).as_posix()}",
                            recursive=False)
    raw = buf.getvalue()
    zstd = _zstd()
    if zstd:
        out = dest.with_name(dest.name + ".tar.zst")
        r = subprocess.run([zstd, "-q", "-3", "-T1", "-c"], input=raw, capture_output=True)
        if r.returncode == 0:
            out.write_bytes(r.stdout)
            return out
    import gzip
    out = dest.with_name(dest.name + ".tar.gz")
    out.write_bytes(gzip.compress(raw, compresslevel=6, mtime=0))
    return out


def unpack_save(archive: Path, dest: Path) -> Path | None:
    """Unpack a `save` archive (zstd or gzip, by magic) into dest; returns the
    "<prefix>.save" directory, or None for a toy stub archive."""
    data = Path(archive).read_bytes()
    if data[:4] == ZSTD_MAGIC:
        zstd = _zstd()
        if not zstd:
            raise _worker_error("save archive is zstd-compressed but zstd is not installed")
        r = subprocess.run([zstd, "-q", "-d", "-c"], input=data, capture_output=True)
        if r.returncode != 0:
            raise _worker_error(f"zstd -d failed: {r.stderr.decode(errors='replace')}")
        data = r.stdout
    elif data[:2] == GZIP_MAGIC:
        import gzip
        data = gzip.decompress(data)
    dest.mkdir(parents=True, exist_ok=True)
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:") as tar:
        names = tar.getnames()
        tar.extractall(dest, filter="data")
    tops = sorted({n.split("/")[0] for n in names})
    if tops == ["toy.save"]:
        return None
    saves = [t for t in tops if t.endswith(".save")]
    if len(saves) != 1:
        raise _worker_error(f"save archive: expected one *.save directory, got {tops}")
    return dest / saves[0]


# ---------------------------------------------------------------------------------------
# operations

# A spin-polarized relaxation that ends below this moment counts as non-magnetic.
NONMAGNETIC_MUB_PER_ATOM = 0.01


def spin_params(params: dict[str, Any], s: Structure) -> dict[str, Any]:
    """params with spin "off" when it is auto and `s` is known non-magnetic."""
    if params.get("spin", "auto") == "auto" and s.known_nonmagnetic:
        return {**params, "spin": "off"}
    return params


def _common(ctx, input_name: str):
    s, sid = input_structure(ctx, input_name)
    params = spin_params(ctx.params, s)
    ename, eng = make_engine(ctx, params)
    st = make_settings(params, s.elements, ename)
    return params, s, sid, ename, eng, st


def _relax(ctx):
    """qe.relax: vc-relax (relax with fix_cell) → structure, log; record qe.relax."""
    from fireforge import JobResult
    params, s, sid, ename, eng, st = _common(ctx, "structure")
    fix_cell = bool(params.get("fix_cell", False))
    ctx.report_progress(0.05, f"relax {s.reduced_formula()} ({ename})")
    r = eng.relax(s, st, fix_cell=fix_cell, workdir=calc_dir(ctx, "relax"))
    out = r.structure.copy(parent_structure_id=sid or s.parent_structure_id,
                           source="qe.relax")
    if st.spin_polarized(s.elements) and \
            abs(r.magnetization_mub) / out.nsites < NONMAGNETIC_MUB_PER_ATOM:
        # No moment survived: mark the structure so later steps skip spin polarization.
        out = out.copy(magmoms=[0.0] * out.nsites)
    refs = [ctx.save_output("structure", (json.dumps(out.to_json(), indent=1) + "\n").encode(),
                            "application/json", "structure.json"),
            ctx.save_output("log", r.log, "text/plain", "relax.log")]
    n = out.nsites
    # energy: what stability.formation / crystal.select read (steps can't read records).
    energy = {"energy_ev": r.energy_ev, "natoms": n, "energy_ev_per_atom": r.energy_ev / n,
              "formula": out.formula(), "reduced_formula": out.reduced_formula(),
              "engine": ename, "preset": st.preset, "converged": r.converged,
              "structure": out.to_json()}
    refs.append(ctx.save_output("energy", (json.dumps(energy, indent=1) + "\n").encode(),
                                "application/json", "energy.json"))
    props = {"energy_ev_per_atom": r.energy_ev / n, "energy_ev": r.energy_ev,
             "a_angstrom": conventional_a(out), "volume_a3_per_atom": out.volume() / n,
             "pressure_kbar": r.pressure_kbar, "magnetization_mub": r.magnetization_mub,
             "magnetization_mub_per_atom": r.magnetization_mub / n}
    ctx.record("qe.relax", structure=out, properties=props,
               conditions={"pressure_kbar": 0.0, "fix_cell": fix_cell},
               method=r.provenance.method(st.preset),
               convergence={"converged": r.converged, "steps": r.steps},
               outputs=["structure", "log", "energy"],
               note="" if r.converged else "relaxation not converged")
    return JobResult(success=True, artifacts=refs)


def _scf(ctx):
    """qe.scf → save (tar of prefix.save), log; record qe.scf."""
    from fireforge import JobResult
    params, s, sid, ename, eng, st = _common(ctx, "structure")
    ctx.report_progress(0.05, f"scf {s.reduced_formula()} ({ename})")
    work = calc_dir(ctx, "scf")
    r = eng.scf(s, st, workdir=work)
    stub = None
    if r.save_dir is None:
        stub = {"engine": ename, "energy_ev": r.energy_ev, "fermi_ev": r.fermi_ev,
                "structure": s.to_json(), "settings": st.to_json()}
    arch = pack_save(r.save_dir, work / "save", stub)
    media = "application/zstd" if arch.name.endswith(".zst") else "application/gzip"
    refs = [ctx.save_output("save", arch, media, arch.name),
            ctx.save_output("log", r.log, "text/plain", "scf.log")]
    n = s.nsites
    props = {"energy_ev_per_atom": r.energy_ev / n, "energy_ev": r.energy_ev,
             "fermi_ev": r.fermi_ev,
             "pressure_kbar": float(np.trace(np.asarray(r.stress_kbar)) / 3.0)}
    _record(ctx, "qe.scf", s, sid, props, r.provenance.method(st.preset),
            {"converged": r.converged}, ["save", "log"])
    return JobResult(success=True, artifacts=refs)


def _record(ctx, kind, s: Structure, sid: str, props, method, convergence, outputs):
    if sid:
        ctx.record(kind, structure_id=sid, properties=props, method=method,
                   convergence=convergence, outputs=outputs)
    else:
        ctx.record(kind, structure=s, properties=props, method=method,
                   convergence=convergence, outputs=outputs)


def _nscf(ctx):
    """qe.nscf (structure, save) → bands (BandData npz), log; record qe.bands."""
    from fireforge import JobResult
    params, s, sid, ename, eng, st = _common(ctx, "structure")
    try:
        archive = ctx.input("save")
    except KeyError as e:
        raise _worker_error(str(e)) from None
    save = unpack_save(archive, calc_dir(ctx, "save-in"))
    if ename == "qe" and save is None:
        raise _worker_error("save input comes from the toy engine; cannot run QE nscf on it")
    ctx.report_progress(0.05, f"nscf {s.reduced_formula()} ({ename})")
    bd: BandData = eng.nscf(s, save, st, workdir=calc_dir(ctx, "nscf"))
    path = calc_dir(ctx, "nscf") / "bands.npz"
    bd.save(path)
    gap, direct, vbm, cbm = bd.band_gap()
    log = getattr(bd, "log", None) or (
        f"{ename} nscf: {s.reduced_formula()} nk={len(bd.kpoints)} nbands={bd.ebands.shape[0]}\n"
        f"gap = {gap:.6f} eV direct={direct} vbm={vbm:.6f} cbm={cbm:.6f}\n")
    refs = [ctx.save_output("bands", path, "application/octet-stream", "bands.npz"),
            ctx.save_output("log", log, "text/plain", "nscf.log")]
    props = {"band_gap_ev": gap, "gap_direct": direct, "vbm_ev": vbm, "cbm_ev": cbm,
             "nelect": bd.nelect, "fermi_ev": bd.fermi * 27.211386245988,
             "nk_irreducible": int(len(bd.kpoints)), "nbands": int(bd.ebands.shape[0]),
             "spin_polarized": bd.dosweight == 1.0}
    _record(ctx, "qe.bands", s, sid, props, bd.provenance.method(st.preset), None,
            ["bands", "log"])
    return JobResult(success=True, artifacts=refs)


def _forces(ctx):
    """qe.forces (supercell) → forces (npz). No record."""
    from fireforge import JobResult
    params, s, sid, ename, eng, st = _common(ctx, "supercell")
    label = str(params.get("label", ""))
    ctx.report_progress(0.05, f"forces {label or s.reduced_formula()} ({ename}, {s.nsites} atoms)")
    r = eng.forces(s, st, workdir=calc_dir(ctx, "forces"))
    path = calc_dir(ctx, "forces") / "forces.npz"
    with open(path, "wb") as f:
        np.savez_compressed(f, format=np.array("fireforge.forces.v1"),
                            forces=np.asarray(r.forces, dtype=float).reshape(-1, 3),
                            energy=np.array(float(r.energy_ev)),
                            stress_kbar=np.asarray(r.stress_kbar, dtype=float).reshape(3, 3),
                            label=np.array(label), natoms=np.array(s.nsites),
                            species=np.array(" ".join(s.species)),
                            provenance=np.array(json.dumps(r.provenance.to_json())))
    refs = [ctx.save_output("forces", path, "application/octet-stream", "forces.npz")]
    return JobResult(success=True, artifacts=refs)


def load_forces(path) -> dict[str, Any]:
    """Read a qe.forces output: forces (n,3) eV/Å, energy, stress_kbar, label, natoms,
    species (list), provenance (dict)."""
    with np.load(path, allow_pickle=False) as z:
        return {"forces": z["forces"], "energy": float(z["energy"]),
                "stress_kbar": z["stress_kbar"], "label": str(z["label"]),
                "natoms": int(z["natoms"]), "species": str(z["species"]).split(),
                "provenance": json.loads(str(z["provenance"]))}


# ---------------------------------------------------------------------------------------
# elastic constants (cubic)
#
# Stress-strain, central differences at fixed cell: strains are set in the conventional
# cubic frame (spglib std_rotation_matrix maps it to the cell's cartesian frame), ±δ
# uniaxial ε_xx → C11 = Δσ_xx/2δ, C12 = Δ(σ_yy+σ_zz)/2/2δ; ±δ engineering shear on xy, yz,
# zx at once → C44 = mean Δσ_ij/2δ. Ions are relaxed in every strained cell (relax with
# fixed cell, tstress; `relax_ions`, default true) because shear moves the 4c site of a
# half-Heusler (internal strain); uniaxial strain leaves forces zero by symmetry, so that
# relax stops after one scf. Every strained cell uses the unstrained cell's k-mesh.
# Central differences cancel the residual stress of the reference cell. Stresses per
# strain are checkpointed ("elastic") and reused on a retry with the same key.

GPA_PER_KBAR = 0.1
AMU_KG = 1.66053906660e-27
HBAR_OVER_KB = 7.6382325822e-12          # s·K
H_OVER_KB = 2 * math.pi * HBAR_OVER_KB


def strain_set(delta: float) -> list[tuple[str, np.ndarray]]:
    """(label, symmetric strain in the conventional cubic frame): ±δ uniaxial along x
    (C11, C12) and ±δ engineering shear on all three pairs (ε_ij = δ/2; C44)."""
    out = []
    for sign, tag in ((1, "p"), (-1, "m")):
        e = np.zeros((3, 3))
        e[0, 0] = sign * delta
        out.append((f"xx_{tag}", e))
    for sign, tag in ((1, "p"), (-1, "m")):
        e = np.full((3, 3), sign * delta / 2.0)
        np.fill_diagonal(e, 0.0)
        out.append((f"shear_{tag}", e))
    return out


def strained(s: Structure, eps_std: np.ndarray, rot: np.ndarray) -> Structure:
    """Apply a strain given in the standard (cubic) frame: ε = Rᵀ ε_std R in the cell's
    cartesian frame, lattice rows a' = a (I + ε)."""
    eps = rot.T @ eps_std @ rot
    lat = s.lattice_array @ (np.eye(3) + eps).T
    return s.copy(lattice=lat.tolist())


def cubic_moduli(c11: float, c12: float, c44: float, density_kg_m3: float,
                 volume_a3_per_atom: float) -> dict[str, Any]:
    """Born criteria, Voigt-Reuss-Hill B and G (GPa), sound velocities (m/s) and the
    elastic Debye temperature θ_D = (h/k_B)(3/(4πΩ))^{1/3} v_m."""
    born = bool(c11 - c12 > 0 and c11 + 2 * c12 > 0 and c44 > 0)
    b = (c11 + 2 * c12) / 3.0
    gv = (c11 - c12 + 3 * c44) / 5.0
    denom = 4 * c44 + 3 * (c11 - c12)
    gr = 5 * (c11 - c12) * c44 / denom if denom != 0 else 0.0
    g = 0.5 * (gv + gr)
    out: dict[str, Any] = {"c11_gpa": c11, "c12_gpa": c12, "c44_gpa": c44, "bulk_gpa": b,
                           "shear_gpa": g, "shear_voigt_gpa": gv, "shear_reuss_gpa": gr,
                           "born_stable": born}
    if born and g > 0 and density_kg_m3 > 0:
        vl = math.sqrt((b + 4.0 * g / 3.0) * 1e9 / density_kg_m3)
        vt = math.sqrt(g * 1e9 / density_kg_m3)
        vm = ((2.0 / vt ** 3 + 1.0 / vl ** 3) / 3.0) ** (-1.0 / 3.0)
        omega = volume_a3_per_atom * 1e-30
        theta = H_OVER_KB * (3.0 / (4.0 * math.pi * omega)) ** (1.0 / 3.0) * vm
        out.update({"v_l": vl, "v_t": vt, "v_m": vm, "theta_d_elastic_k": theta,
                    "poisson": (3 * b - 2 * g) / (2 * (3 * b + g)),
                    "pugh_ratio": b / g})
    else:
        out.update({"v_l": 0.0, "v_t": 0.0, "v_m": 0.0, "theta_d_elastic_k": 0.0})
    return out


def density(s: Structure) -> float:
    from ase.data import atomic_masses, atomic_numbers
    m = sum(float(atomic_masses[atomic_numbers[e]]) for e in s.species) * AMU_KG
    return m / (s.volume() * 1e-30)


def strained_stress(eng, s: Structure, st: Settings, workdir: Path,
                    relax_ions: bool) -> np.ndarray:
    """Stress (kbar, QE sign) of a strained cell at fixed cell shape, ions relaxed when
    `relax_ions` (internal strain: the shear moves the 4c site in half-Heuslers)."""
    if relax_ions and s.nsites > 1:
        both = getattr(eng, "relax_with_stress", None)
        if both is not None:
            _, sig = both(s, st, fix_cell=True, workdir=workdir)
            return np.asarray(sig)
        r = eng.relax(s, st, fix_cell=True, workdir=workdir)
        return np.asarray(eng.stress(r.structure, st, workdir=workdir))
    return np.asarray(eng.stress(s, st, workdir=workdir))


def _elastic(ctx):
    """qe.elastic (structure) → elastic (json); record stability.elastic. Cubic only."""
    from fireforge import JobResult
    from .inputs import resolve_kmesh
    params, s, sid, ename, eng, st = _common(ctx, "structure")
    delta = float(params.get("strain", 0.01))
    if not 0 < delta < 0.1:
        raise _worker_error("elastic: strain must be in (0, 0.1)")
    relax_ions = bool(params.get("relax_ions", True))
    ds = spglib_call("get_symmetry_dataset", s.to_spglib_cell(), symprec=1e-3)
    if ds is None or not 195 <= int(ds.number) <= 230:
        num = None if ds is None else int(ds.number)
        raise _worker_error(f"elastic: only cubic implemented (space group {num})")
    rot = np.asarray(ds.std_rotation_matrix, dtype=float)
    # same k-mesh for every strained cell (no mesh jumps at rounding edges)
    st = replace(st, extra={**(st.extra or {}), "kmesh": list(resolve_kmesh(s, st, False))})
    key = {"fingerprint": s.fingerprint(), "strain": delta, "relax_ions": relax_ions,
           "engine": ename, "settings": st.to_json()}
    done: dict[str, list] = {}
    ck = ctx.load_checkpoint_json(default=None) if ctx.resume_checkpoint else None
    if isinstance(ck, dict) and ck.get("key") == json.loads(json.dumps(key, default=str)):
        done = dict(ck.get("stress_kbar") or {})
    strains = strain_set(delta)
    prov = None
    for i, (label, e_std) in enumerate(strains):
        ctx.check_cancelled()
        if label in done:
            continue
        ctx.report_progress(0.05 + 0.9 * i / len(strains), f"elastic: strain {label}")
        sig = strained_stress(eng, strained(s, e_std, rot), st, calc_dir(ctx, f"elastic-{label}"),
                              relax_ions)
        done[label] = (rot @ sig @ rot.T).tolist()        # in the standard frame
        if ctx.checkpoint_enabled:
            ctx.checkpoint({"key": key, "stress_kbar": done}, name="elastic")
    # physical stress (tensile positive) = −QE stress; GPa
    phys = {k: -np.asarray(v) * GPA_PER_KBAR for k, v in done.items()}
    d_xx = (phys["xx_p"] - phys["xx_m"]) / (2 * delta)
    d_sh = (phys["shear_p"] - phys["shear_m"]) / (2 * delta)
    c11 = float(d_xx[0, 0])
    c12 = float(0.5 * (d_xx[1, 1] + d_xx[2, 2]))
    c44 = float((d_sh[0, 1] + d_sh[1, 2] + d_sh[0, 2]) / 3.0)
    res = cubic_moduli(c11, c12, c44, density(s), s.volume() / s.nsites)
    # provenance from a cheap description (no extra solver run)
    if ename == "toy":
        prov = eng.provenance(s, st, strain=delta, relax_ions=relax_ions)
    else:
        from .engine import SOLVER
        from .. import pseudo
        from ..engine import Provenance
        from .runner import version
        logs = sorted(Path(ctx.workdir, "calc").glob("elastic-*/*/pw.out"))
        ver = version(logs[0].read_text(errors="replace")) if logs else "unknown"
        prov = Provenance(solver=SOLVER, solver_version=ver, engine="qe",
                          pseudopotentials=pseudo.provenance(s.elements),
                          settings={**st.to_json(), "strain": delta, "relax_ions": relax_ions,
                                    "cpus": eng.cpus})
    doc = {"format": "fireforge.elastic.v1", "crystal_system": "cubic",
           "spacegroup_number": int(ds.number), "strain": delta, "relax_ions": relax_ions,
           "units": {"c": "GPa", "v": "m/s", "theta": "K", "stress": "kbar (QE sign)"},
           **res, "density_kg_m3": density(s),
           "c_matrix_gpa": _cubic_matrix(c11, c12, c44),
           "stress_kbar": done, "rotation_to_standard": rot.tolist(),
           "provenance": prov.to_json()}
    refs = [ctx.save_output("elastic", doc, "application/json", "elastic.json")]
    props = {k: res[k] for k in ("c11_gpa", "c12_gpa", "c44_gpa", "bulk_gpa", "shear_gpa",
                                 "born_stable", "v_l", "v_t", "theta_d_elastic_k")}
    props["v_m"] = res["v_m"]
    _record(ctx, "stability.elastic", s, sid, props, prov.method(st.preset),
            {"strain": delta, "relax_ions": relax_ions}, ["elastic"])
    return JobResult(success=True, artifacts=refs)


def _cubic_matrix(c11, c12, c44) -> list[list[float]]:
    c = np.zeros((6, 6))
    c[:3, :3] = c12
    np.fill_diagonal(c[:3, :3], c11)
    c[3, 3] = c[4, 4] = c[5, 5] = c44
    return c.tolist()


TASKS = {"qe.relax": _relax, "qe.scf": _scf, "qe.nscf": _nscf, "qe.forces": _forces,
         "qe.elastic": _elastic}


def register(worker) -> None:
    for op, fn in TASKS.items():
        worker.task(op)(_wrap(fn))
