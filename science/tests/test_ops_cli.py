"""Worker assembly (ops.build_worker) and the CLI (doctor)."""
from __future__ import annotations

import importlib.machinery
import json
import sys
import types

import pytest

from fireforge_science import cli, ops


def test_build_worker_skips_missing_modules(monkeypatch):
    monkeypatch.setattr(ops, "MODULES", ("does_not_exist_yet",))
    w = ops.build_worker()
    assert w.tasks == {}


def test_build_worker_registers(monkeypatch):
    name = "fireforge_science.fake_ops_mod"
    mod = types.ModuleType(name)
    mod.__spec__ = importlib.machinery.ModuleSpec(name, None)

    def register(worker):
        worker.task("crystal.reference")(lambda ctx: {"ok": True})
    mod.register = register
    monkeypatch.setitem(sys.modules, name, mod)
    monkeypatch.setattr(ops, "MODULES", ("does_not_exist_yet", "fake_ops_mod"))
    assert ops.available_modules() == ["fake_ops_mod"]
    w = ops.build_worker()
    assert set(w.tasks) == {"crystal.reference"} <= set(ops.OPERATIONS)


def test_build_worker_real():
    """Whatever modules exist today register only known operation names."""
    w = ops.build_worker()
    assert set(w.tasks) <= set(ops.OPERATIONS)


def test_cli_registry():
    p = cli.build_parser()
    args = p.parse_args(["doctor", "--no-pw", "--elements", "Zr,Ni"])
    assert args.command == "doctor" and args.no_pw and args._run is cli._doctor
    with pytest.raises(ValueError):
        cli.command("doctor", help="dup")(lambda a: 0)
    with pytest.raises(SystemExit):
        p.parse_args([])


@pytest.mark.pseudo
def test_doctor_json(capsys):
    rc = cli.main(["doctor", "--no-pw", "--json", "--elements", "Zr,Ni,Sn"])
    rep = json.loads(capsys.readouterr().out)
    assert rc == 0 and rep["problems"] == []
    assert rep["imports"]["spglib"]["ok"]
    assert rep["pseudo"]["checked"] == 3 and rep["pseudo"]["problems"] == {}
    assert rep["capabilities"][0] == "sci" and "qe" not in rep["capabilities"]
    assert {"phonopy", "phono3py", "boltztrap2"} <= set(rep["capabilities"])


def test_doctor_pw_version(tmp_path, monkeypatch):
    fake = tmp_path / "pw.x"
    fake.write_text("#!/bin/sh\necho '     Program PWSCF v.6.7MaX starts on  6Nov2026 at 10:00:00'\n"
                    "echo 'Error: reading input' >&2\nexit 1\n")
    fake.chmod(0o755)
    monkeypatch.setenv("FIREFORGE_PW_X", str(fake))
    assert cli.pw_version() == (str(fake), "6.7MaX")
    monkeypatch.setenv("FIREFORGE_PW_X", "")
    monkeypatch.setenv("PATH", str(tmp_path / "empty"))
    assert cli.pw_version()[0] is None


def test_doctor_text_and_missing_pseudo(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("FIREFORGE_PSEUDO_DIR", str(tmp_path / "none"))
    monkeypatch.delenv("FIREFORGE_SSSP_JSON", raising=False)
    rc = cli.main(["doctor", "--no-pw"])
    out = capsys.readouterr().out
    assert rc == 1 and "PROBLEM" in out and "capabilities = sci" in out
