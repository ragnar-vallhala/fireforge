"""Command line: python -m fireforge_science <command> (docs/design/phase8.md §1, §6).

Commands register themselves with the `command` decorator:

    @command("report", help="print a campaign report",
             configure=lambda p: p.add_argument("campaign_id"))
    def _report(args) -> int:
        ...
        return 0

`configure(parser)` adds the subcommand's arguments; the function receives the parsed
argparse.Namespace and returns the exit code. `main(argv)` builds the parser from the
registry, so commands defined in this module (or in a module imported before `main`
runs) appear automatically.
"""
from __future__ import annotations

import argparse
import importlib
import json
import os
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass
from typing import Callable

from . import __version__


@dataclass
class Command:
    name: str
    help: str
    run: Callable[[argparse.Namespace], int]
    configure: Callable[[argparse.ArgumentParser], None] | None = None


COMMANDS: dict[str, Command] = {}


def command(name: str, *, help: str,
            configure: Callable[[argparse.ArgumentParser], None] | None = None):
    """Decorator registering `fn(args) -> int` as subcommand `name`."""
    def deco(fn):
        if name in COMMANDS:
            raise ValueError(f"command {name!r} registered twice")
        COMMANDS[name] = Command(name, help, fn, configure)
        return fn
    return deco


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="python -m fireforge_science",
                                description="FireForge science (Phase 8) tools")
    p.add_argument("--version", action="version", version=f"fireforge_science {__version__}")
    sub = p.add_subparsers(dest="command", metavar="COMMAND", required=True)
    for c in COMMANDS.values():
        sp = sub.add_parser(c.name, help=c.help, description=c.help)
        if c.configure:
            c.configure(sp)
        sp.set_defaults(_run=c.run)
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return int(args._run(args) or 0)


# ---------------------------------------------------------------------------------------
# doctor

PACKAGES = ("numpy", "scipy", "spglib", "ase", "pymatgen", "phonopy", "phono3py",
            "BoltzTraP2", "h5py")
# capability -> python modules it needs (sci needs the core stack)
CAPABILITY_MODULES = {
    "phonopy": ("phonopy",),
    "phono3py": ("phono3py",),
    "boltztrap2": ("BoltzTraP2",),
}
VALIDATION_ELEMENTS = ("Zr", "Hf", "Ti", "Nb", "Ni", "Fe", "Co", "Sn", "Sb")


def _module_version(name: str) -> tuple[bool, str]:
    try:
        mod = importlib.import_module(name)
    except Exception as e:  # noqa: BLE001 - report anything (ImportError, broken C ext)
        return False, f"{type(e).__name__}: {e}"
    v = getattr(mod, "__version__", None)
    if v is None and "." not in name:
        try:
            from importlib.metadata import version
            v = version(name)
        except Exception:  # noqa: BLE001
            v = None
    return True, str(v or "?")


PW_BANNER = re.compile(r"Program PWSCF v\.?\s*(\S+)")


def pw_version(timeout: float = 30.0) -> tuple[str | None, str]:
    """(path to pw.x or None, version or problem). Runs pw.x with empty input in a
    scratch directory and parses the banner ("Program PWSCF v.6.7MaX starts ...")."""
    exe = os.environ.get("FIREFORGE_PW_X") or shutil.which("pw.x")
    if not exe:
        return None, "pw.x not found on PATH"
    import tempfile
    with tempfile.TemporaryDirectory(prefix="ff-doctor-") as tmp:
        try:
            proc = subprocess.run([exe], input="", capture_output=True, text=True,
                                  timeout=timeout, cwd=tmp,
                                  env={**os.environ, "OMP_NUM_THREADS": "1"})
        except subprocess.TimeoutExpired:
            return exe, "pw.x did not exit (timeout)"
        except OSError as e:
            return exe, f"cannot run pw.x: {e}"
    m = PW_BANNER.search(proc.stdout + proc.stderr)
    return exe, (m.group(1) if m else "unknown version (no PWSCF banner)")


def doctor_report(elements=VALIDATION_ELEMENTS, all_pseudos: bool = False,
                  check_pw: bool = True) -> dict:
    """Everything `doctor` prints, as a dict (also used by tests)."""
    rep: dict = {"version": __version__, "python": sys.version.split()[0], "imports": {},
                 "problems": [], "warnings": []}
    for name in PACKAGES:
        ok, v = _module_version(name)
        rep["imports"][name] = {"ok": ok, "version": v}
        if not ok:
            rep["problems"].append(f"import {name}: {v}")
    ok, v = _module_version("fireforge")
    rep["imports"]["fireforge"] = {"ok": ok, "version": v}
    if ok:
        ok2, v2 = _module_version("fireforge.tdr.v1.record_pb2")
        if not ok2:
            rep["problems"].append(f"fireforge.tdr.v1.record_pb2: {v2} (run tools/gen_proto.sh)")
    else:
        rep["problems"].append(f"import fireforge (SDK): {v}")

    # Quantum ESPRESSO
    if check_pw:
        exe, ver = pw_version()
        rep["pw_x"] = {"path": exe, "version": ver}
        if exe is None or not re.match(r"\d", ver):
            rep["warnings"].append(f"pw.x: {ver}")
        mpirun = os.environ.get("FIREFORGE_SCI_MPIRUN") or shutil.which("mpirun")
        rep["mpirun"] = mpirun
        if not mpirun:
            rep["warnings"].append("mpirun not found (needed for cpus > 1)")
    else:
        rep["pw_x"] = {"path": None, "version": "not checked"}

    # pseudopotentials
    from . import pseudo
    ps: dict = {"dir": str(pseudo.pseudo_dir())}
    try:
        ps["json"] = str(pseudo.sssp_json_path())
        ps["json_md5"] = pseudo.md5_file(ps["json"])
        if ps["json_md5"] != pseudo.SSSP_JSON_MD5:
            rep["warnings"].append(f"SSSP json md5 {ps['json_md5']} != {pseudo.SSSP_JSON_MD5} "
                                   "(not the pinned SSSP 1.3.0 table)")
        checked = None if all_pseudos else list(elements)
        bad = pseudo.verify(checked)
        ps["checked"] = len(pseudo.sssp_table()) if checked is None else len(checked)
        ps["problems"] = bad
        for el, msg in bad.items():
            rep["problems"].append(f"pseudo {el}: {msg}")
    except FileNotFoundError as e:
        ps["problems"] = {"*": str(e)}
        rep["problems"].append(str(e))
    rep["pseudo"] = ps

    # capabilities
    caps = []
    core_ok = all(rep["imports"][n]["ok"] for n in ("numpy", "scipy", "spglib", "ase",
                                                      "pymatgen", "fireforge"))
    pseudo_ok = not ps.get("problems")
    if core_ok:
        caps.append("sci")
        if pseudo_ok and rep["pw_x"]["path"] and re.match(r"\d", rep["pw_x"]["version"]):
            caps.append("qe")
        for cap, mods in CAPABILITY_MODULES.items():
            if all(rep["imports"][m]["ok"] for m in mods):
                caps.append(cap)
    rep["capabilities"] = caps
    return rep


def _doctor_configure(p: argparse.ArgumentParser) -> None:
    p.add_argument("--elements", default=",".join(VALIDATION_ELEMENTS),
                   help="elements whose pseudopotentials to verify (comma separated)")
    p.add_argument("--all-pseudos", action="store_true",
                   help="verify every file of the SSSP table")
    p.add_argument("--no-pw", action="store_true", help="skip the pw.x check")
    p.add_argument("--json", action="store_true", help="machine-readable output")


@command("doctor", help="check the science stack: imports, pw.x, SSSP pseudopotentials; "
                        "print the tdrd capabilities line", configure=_doctor_configure)
def _doctor(args) -> int:
    els = [e.strip() for e in args.elements.split(",") if e.strip()]
    rep = doctor_report(els, all_pseudos=args.all_pseudos, check_pw=not args.no_pw)
    if args.json:
        print(json.dumps(rep, indent=1))
        return 1 if rep["problems"] else 0
    print(f"fireforge_science {rep['version']} (python {rep['python']})")
    for name, r in rep["imports"].items():
        print(f"  {'ok ' if r['ok'] else 'BAD'} {name:<11} {r['version']}")
    pw = rep["pw_x"]
    print(f"  pw.x: {pw['path'] or '-'} ({pw['version']})")
    if "mpirun" in rep:
        print(f"  mpirun: {rep['mpirun'] or '-'}")
    ps = rep["pseudo"]
    print(f"  pseudo dir: {ps['dir']}")
    if "json" in ps:
        print(f"  SSSP table: {ps['json']} (md5 {ps['json_md5']}); "
              f"{ps.get('checked', 0)} files checked, {len(ps.get('problems', {}))} bad")
    for w in rep["warnings"]:
        print(f"warning: {w}")
    for p in rep["problems"]:
        print(f"PROBLEM: {p}")
    print("\nSuggested tdrd config:\n  [node]\n  capabilities = " + ",".join(rep["capabilities"]))
    return 1 if rep["problems"] else 0


# ---- workflow builders (phase8.md §5) ---------------------------------------------------

def _wf_common(p: argparse.ArgumentParser) -> None:
    p.add_argument("--engine", default="qe", choices=["qe", "toy"])
    p.add_argument("--preset", default="screening", choices=["screening", "accurate"])
    p.add_argument("--cpus", type=int, default=4, help="cores per QE step (default 4)")
    p.add_argument("--campaign", default="", help="campaign id")
    p.add_argument("--argv", default="fireforge-sci",
                   help="worker command on the nodes (default: the fireforge-sci wrapper)")
    p.add_argument("-o", "--out", default="-", help="CreateWorkflowRequest JSON file ('-' = stdout)")


def _write_wf(wf, out: str) -> int:
    if out == "-":
        print(wf.to_json())
    else:
        wf.write(out)
        print(f"wrote {out} ({len(wf.steps)} steps); submit with: tdrctl cp workflow-submit {out}",
              file=sys.stderr)
    return 0


def _validation_configure(p: argparse.ArgumentParser) -> None:
    _wf_common(p)
    p.add_argument("--compounds", default="", help="comma-separated subset of the validation set")
    p.add_argument("--no-tier2", action="store_true", help="skip the phono3py tier for ZrNiSn")


@command("submit-validation", help="build the half-Heusler validation workflow (phase8.md §5, §8)",
         configure=_validation_configure)
def _cmd_validation(args) -> int:
    from .workflows import validation_workflow
    comps = [c.strip() for c in args.compounds.split(",") if c.strip()] or None
    wf = validation_workflow(engine=args.engine, preset=args.preset, cpus=args.cpus,
                             compounds=comps, tier2=not args.no_tier2,
                             campaign_id=args.campaign or None, argv=args.argv.split())
    return _write_wf(wf, args.out)


def _campaign_configure(p: argparse.ArgumentParser) -> None:
    _wf_common(p)
    p.add_argument("--x", required=True, help="X elements (4a), comma-separated")
    p.add_argument("--y", required=True, help="Y elements (late transition metals), comma-separated")
    p.add_argument("--z", required=True, help="Z elements (4b), comma-separated")
    p.add_argument("--orderings", default="all", choices=["all", "known"])


@command("submit-campaign", help="build a half-Heusler screening campaign workflow (phase8.md §5)",
         configure=_campaign_configure)
def _cmd_campaign(args) -> int:
    from .workflows import halfheusler_campaign
    split = lambda s: [e.strip() for e in s.split(",") if e.strip()]  # noqa: E731
    wf = halfheusler_campaign(split(args.x), split(args.y), split(args.z), engine=args.engine,
                              preset=args.preset, cpus=args.cpus, orderings=args.orderings,
                              campaign_id=args.campaign or None, argv=args.argv.split())
    return _write_wf(wf, args.out)


@command("literature-import", help="print the literature values as an ImportCalculationsRequest "
                                   "(for tdrctl cp calc-import)",
         configure=lambda p: p.add_argument("--campaign", default=""))
def _cmd_literature(args) -> int:
    import json as _json
    from .validate import literature_import_request
    r = literature_import_request(campaign_id=args.campaign)
    print(r if isinstance(r, str) else _json.dumps(r, indent=1))
    return 0
