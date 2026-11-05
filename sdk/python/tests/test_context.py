import json
from pathlib import Path

import pytest

from fireforge import Cancelled, Context, OutputRef, StructureFile, WorkerError
from fireforge.tdr.v1 import local_pb2


def make_ctx(tmp_path, inputs=(), params=""):
    out = tmp_path / "out"
    out.mkdir()
    task = local_pb2.WorkerTask(job_id="j", attempt_id="a", attempt_number=2, operation="op",
                                parameters_json=params, output_dir=str(out),
                                checkpoint_dir=str(tmp_path / "ck"),
                                inputs=[local_pb2.InputFile(artifact_id=i, path=str(p)) for i, p in inputs])
    return Context(task, {"FIREFORGE_WORKDIR": str(tmp_path), "FIREFORGE_INPUT_DIR": str(tmp_path / "in")})


def test_basic_fields(tmp_path):
    ctx = make_ctx(tmp_path, params='{"x": 1}')
    assert ctx.params == {"x": 1} and ctx.attempt_number == 2
    assert ctx.workdir == tmp_path and ctx.input_dir == tmp_path / "in"
    assert ctx.output_dir == tmp_path / "out" and ctx.checkpoint_dir == tmp_path / "ck"
    (tmp_path / "empty").mkdir()
    assert make_ctx(tmp_path / "empty").params == {}


def test_missing_generated_modules_message(tmp_path):
    import shutil, subprocess, sys
    pkg = Path(__file__).resolve().parent.parent / "fireforge"
    shutil.copytree(pkg, tmp_path / "fireforge", ignore=shutil.ignore_patterns("*_pb2.py", "__pycache__"))
    proc = subprocess.run([sys.executable, "-c", "import fireforge"], cwd=tmp_path,
                          env={"PYTHONPATH": str(tmp_path)}, capture_output=True, text=True, timeout=20)
    assert proc.returncode != 0 and "run tools/gen_proto.sh" in proc.stderr


def test_cancel(tmp_path):
    ctx = make_ctx(tmp_path)
    assert not ctx.cancelled
    ctx.check_cancelled()
    ctx._set_cancelled("stop")
    assert ctx.cancelled
    with pytest.raises(Cancelled) as e:
        ctx.check_cancelled()
    assert not e.value.retryable


def test_save_output_kinds(tmp_path):
    ctx = make_ctx(tmp_path)
    src = tmp_path / "s.cif"
    src.write_text("data_x\n")
    assert ctx.save_output("a", {"k": 1}) == OutputRef("a", "a.json", "application/json")
    assert ctx.save_output("b", "hi") == OutputRef("b", "b.txt", "text/plain")
    assert ctx.save_output("c", b"\x00") == OutputRef("c", "c.bin", "application/octet-stream")
    assert ctx.save_output("d", src) == OutputRef("d", "d.cif", "chemical/x-cif")
    assert (ctx.output_dir / "d.cif").read_text() == "data_x\n"
    with pytest.raises(WorkerError):
        ctx.save_output("e", "x", filename="/etc/passwd")
    refs = ctx._collect_outputs(None)
    assert [r.path for r in refs] == ["a.json", "b.txt", "c.bin", "d.cif"]


def test_load_structure_sniffs_hash_named_inputs(tmp_path):
    p1 = tmp_path / "aaaa"
    p1.write_bytes(b"\x89PNG not a structure")
    p2 = tmp_path / "bbbb"
    p2.write_text(json.dumps({"lattice": {}, "sites": []}))
    ctx = make_ctx(tmp_path, inputs=[("artifact://sha256/aa", p1), ("artifact://sha256/bb", p2)])
    s = ctx.load_structure(raw=True)
    assert isinstance(s, StructureFile) and s.format == "json" and s.path == p2
    assert s.data == {"lattice": {}, "sites": []}


def test_load_structure_none(tmp_path):
    with pytest.raises(WorkerError, match="no structure"):
        make_ctx(tmp_path).load_structure()
