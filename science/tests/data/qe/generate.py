"""Regenerate the canned pw.x 6.7 outputs in this directory (needs pw.x + SSSP):

    PYTHONPATH=sdk/python:science tools/capped .venv/bin/python science/tests/data/qe/generate.py [names...]

Each case writes <name>.in (pw.in), <name>.out (stdout) and <name>.xml
(data-file-schema.xml). Cases: si_scf, si_nscf, si_vcrelax, si_lsda_scf (2-atom Si, low
cutoff, seconds), zrnisn_scf (screening preset, 2 MPI ranks, spin-polarized because of Ni).
"""
from __future__ import annotations

import sys
import tempfile
from pathlib import Path

from fireforge_science.model import Structure
from fireforge_science.quantumforge.engine import QeEngine
from fireforge_science.settings import preset

HERE = Path(__file__).resolve().parent


def si(a: float = 5.43) -> Structure:
    h = a / 2
    return Structure([[0, h, h], [h, 0, h], [h, h, 0]], ["Si", "Si"],
                     [[0, 0, 0], [0.25, 0.25, 0.25]])


def zrnisn(a: float = 6.113) -> Structure:
    h = a / 2
    return Structure([[0, h, h], [h, 0, h], [h, h, 0]], ["Zr", "Ni", "Sn"],
                     [[0, 0, 0], [0.25, 0.25, 0.25], [0.5, 0.5, 0.5]])


SI = dict(ecutwfc_ry=20.0, ecutrho_ry=160.0, kdensity=0.4, kdensity_dense=0.3)


def keep(name: str, calc: Path) -> None:
    """Copy the files, with local paths replaced by /pseudo and /work."""
    from fireforge_science import pseudo
    subs = [(str(pseudo.pseudo_dir()), "/pseudo"), (str(calc.parent), "/work")]
    for src, ext in ((calc / "pw.in", "in"), (calc / "pw.out", "out"),
                     (calc / "tmp" / "ff.save" / "data-file-schema.xml", "xml")):
        text = src.read_text()
        for a, b in subs:
            text = text.replace(a, b)
        (HERE / f"{name}.{ext}").write_text(text)
    print("wrote", name)


def main(names: list[str]) -> None:
    with tempfile.TemporaryDirectory(prefix="ffqe-gen-") as tmp:
        wd = Path(tmp)
        st = preset("screening", ["Si"], **SI)
        e = QeEngine(cpus=1)
        if not names or {"si_scf", "si_nscf"} & set(names):
            r = e.scf(si(), st, workdir=wd)
            keep("si_scf", r.save_dir.parent.parent)
            e.nscf(si(), r.save_dir, st, workdir=wd)
            keep("si_nscf", sorted(wd.glob("*-nscf"))[-1])
        if not names or "si_vcrelax" in names:
            e.relax(si(5.30).copy(frac=[[0, 0, 0], [0.26, 0.25, 0.24]]), st, workdir=wd)
            keep("si_vcrelax", sorted(wd.glob("*-vc-relax"))[-1])
        if not names or "si_lsda_scf" in names:
            r = e.scf(si(), preset("screening", ["Si"], spin="on", **SI), workdir=wd)
            keep("si_lsda_scf", r.save_dir.parent.parent)
        if not names or "zrnisn_scf" in names:
            r = QeEngine(cpus=2).scf(zrnisn(), preset("screening", ["Zr", "Ni", "Sn"]),
                                     workdir=wd)
            keep("zrnisn_scf", r.save_dir.parent.parent)


if __name__ == "__main__":
    main(sys.argv[1:])
