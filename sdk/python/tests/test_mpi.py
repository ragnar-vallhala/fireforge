"""Phase 7 SDK: MPI rank detection, detached non-root ranks, MpiSpec in spawn/Workflow."""
import json
from pathlib import Path

import pytest

from fireforge import Context, Workflow, WorkflowError, mpi
from fireforge.tdr.v1 import local_pb2
from fireforge.testing import FakeDaemon, mpi_env

MPI_SCRIPT = Path(__file__).with_name("mpi_worker_script.py")
MULTI_SCRIPT = Path(__file__).with_name("worker_script.py")
CIF = "data_NaCl\n_cell_length_a 5.64\n"


# ---------------------------------------------------------------------------
# Rank detection
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("env,expected", [
    ({}, (0, 1, False)),
    ({"OMPI_COMM_WORLD_RANK": "3", "OMPI_COMM_WORLD_SIZE": "8"}, (3, 8, True)),
    ({"PMI_RANK": "2", "PMI_SIZE": "4"}, (2, 4, True)),
    ({"PMIX_RANK": "5"}, (5, 0, True)),
    ({"PMIX_RANK": "1", "OMPI_COMM_WORLD_SIZE": "6"}, (1, 6, True)),
    ({"PMIX_RANK": "1", "PMI_SIZE": "3"}, (1, 3, True)),
    ({"OMPI_COMM_WORLD_RANK": "0", "PMI_RANK": "7", "OMPI_COMM_WORLD_SIZE": "2"}, (0, 2, True)),
    ({"OMPI_COMM_WORLD_RANK": "x", "PMI_RANK": "1", "PMI_SIZE": "2"}, (1, 2, True)),
    ({"OMPI_COMM_WORLD_RANK": "bad"}, (0, 1, False)),
])
def test_detect(env, expected):
    info = mpi.detect(env)
    assert (info.rank, info.size, info.active) == expected
    assert info.is_root == (expected[0] == 0)
    assert mpi.rank(env) == expected[0] and mpi.size(env) == expected[1]
    assert mpi.is_root(env) == (expected[0] == 0)


@pytest.mark.parametrize("flavour,env_key", [("ompi", "OMPI_COMM_WORLD_RANK"), ("pmi", "PMI_RANK"),
                                             ("pmix", "PMIX_RANK")])
def test_mpi_env_helper(flavour, env_key):
    env = mpi_env(2, 4, flavour=flavour, operation="op", params={"a": 1})
    assert env[env_key] == "2"
    assert env["FIREFORGE_MPI_OPERATION"] == "op" and json.loads(env["FIREFORGE_MPI_PARAMS"]) == {"a": 1}
    info = mpi.detect(env)
    assert info.rank == 2 and info.size == (0 if flavour == "pmix" else 4)


def test_context_attributes(tmp_path):
    task = local_pb2.WorkerTask(job_id="j", attempt_id="a", operation="op")
    plain = Context(task, {"FIREFORGE_WORKDIR": str(tmp_path)})
    assert (plain.mpi_rank, plain.mpi_size, plain.is_root, plain.detached) == (0, 1, True, False)
    ctx = Context(task, {"FIREFORGE_WORKDIR": str(tmp_path), "PMI_RANK": "3", "PMI_SIZE": "4"})
    assert (ctx.mpi_rank, ctx.mpi_size, ctx.is_root) == (3, 4, False)


def test_detached_context_noops(tmp_path):
    task = local_pb2.WorkerTask(operation="op")
    ctx = Context(task, {"FIREFORGE_WORKDIR": str(tmp_path), "OMPI_COMM_WORLD_RANK": "1"},
                  session=object(), detached=True, background_flush=False)
    assert ctx._session is None
    ctx.report_progress(0.5)
    p = ctx.checkpoint({"x": 1}, name="state")      # not enabled, still no error
    assert p == tmp_path / "checkpoints" / "state.json" and not p.exists()
    assert ctx.checkpoint(b"x").name == "checkpoint.bin"
    with pytest.raises(ValueError):
        ctx.checkpoint(b"x", name="../bad")
    ctx.flush_checkpoints()
    ref = ctx.save_output("r", {"a": 1})
    assert (tmp_path / "outputs" / ref.path).is_file()
    assert not ctx.cancelled and not ctx.wait_cancelled(0.01)


# ---------------------------------------------------------------------------
# Worker.run under MPI (subprocess, FakeDaemon)
# ---------------------------------------------------------------------------


def _rank_info(d, rank):
    return json.loads((d.output_dir / f"rank-{rank}.json").read_text())


def test_root_rank_unchanged():
    with FakeDaemon("mpi.solve", {"n": 1}, checkpoint_enabled=True, workflow_id="wf",
                    step="s") as d:
        proc = d.run_worker(MPI_SCRIPT, mpi_rank=0, mpi_size=4)
        assert proc.returncode == 0, proc.stderr
        assert d.connections == 1 and d.result.success
        assert [p.fraction for p in d.progress] == [0.5]
        assert [c.name for c in d.checkpoints] == ["state"]
        assert [s.name for s in d.spawned] == ["child"] and d.spawned[0].job.mpi.ranks == 2
        info = _rank_info(d, 0)
        assert info["root"] and not info["detached"] and info["size"] == 4
        assert info["params"] == {"n": 1} and info["operation"] == "mpi.solve"
        assert json.loads(d.output_files()["result.json"]) == {"rank": 0}


@pytest.mark.parametrize("flavour", ["ompi", "pmi", "pmix"])
def test_non_root_never_connects(flavour):
    with FakeDaemon("mpi.solve", {"n": 1}) as d:
        d.add_input(CIF)
        proc = d.run_worker(MPI_SCRIPT, mpi_rank=2, mpi_size=4, mpi_flavour=flavour)
        assert proc.returncode == 0, proc.stderr
        assert d.connections == 0 and d.hello is None and d.result is None
        info = _rank_info(d, 2)
        assert info["rank"] == 2 and not info["root"] and info["detached"]
        assert info["size"] == (0 if flavour == "pmix" else 4)
        assert info["operation"] == "mpi.solve" and info["params"] == {}
        assert info["job"] == "job-test" and not info["cancelled"]
        assert len(info["inputs"]) == 1 and len(info["inputs"][0]) == 64
        assert info["checkpoint"].endswith("checkpoints/state.json")
        assert not Path(info["checkpoint"]).exists()
        assert info["output_dir"] == str(d.output_dir) and info["ref"] == "rank-2.json"
        assert "result.json" not in d.output_files()     # dict return value ignored


def test_non_root_with_unreachable_socket(tmp_path):
    """FIREFORGE_IPC_SOCKET points nowhere: a connection attempt would fail with exit 1."""
    with FakeDaemon("mpi.solve") as d:
        bogus = {"FIREFORGE_IPC_SOCKET": str(tmp_path / "nope.sock")}
        proc = d.run_worker(MPI_SCRIPT, mpi_rank=1, mpi_size=2, extra_env=bogus)
        assert proc.returncode == 0, proc.stderr
        assert "cannot connect" not in proc.stderr
        # Rank 0 with the same env does try to connect (and fails).
        proc0 = d.run_worker(MPI_SCRIPT, mpi_rank=0, mpi_size=2, extra_env=bogus)
        assert proc0.returncode == 1 and "cannot connect" in proc0.stderr


def test_non_root_without_fireforge_env(tmp_path):
    import os
    import subprocess
    import sys
    from fireforge.testing import SDK_ROOT
    env = {"PATH": os.environ.get("PATH", ""), "PYTHONPATH": str(SDK_ROOT),
           "FIREFORGE_WORKDIR": str(tmp_path), **mpi_env(1, 2)}
    proc = subprocess.run([sys.executable, str(MPI_SCRIPT)], env=env, capture_output=True,
                          text=True, timeout=20, cwd=tmp_path)
    assert proc.returncode == 0, proc.stderr
    assert (tmp_path / "outputs" / "rank-1.json").is_file()


def test_non_root_exception_and_failure_exit_1():
    with FakeDaemon("mpi.solve") as d:
        proc = d.run_worker(MPI_SCRIPT, mpi_rank=1, mpi_size=2, extra_env={"MPI_TEST_MODE": "raise"})
        assert proc.returncode == 1
        assert "Traceback" in proc.stderr and "rank 1 exploded" in proc.stderr
        proc = d.run_worker(MPI_SCRIPT, mpi_rank=1, mpi_size=2, extra_env={"MPI_TEST_MODE": "fail"})
        assert proc.returncode == 1 and "rank says no" in proc.stderr
        assert d.connections == 0


def test_non_root_multi_task_needs_operation():
    with FakeDaemon("demo.dict") as d:
        proc = d.run_worker(MULTI_SCRIPT, mpi_rank=1, mpi_size=2)
        assert proc.returncode == 2
        assert "FIREFORGE_MPI_OPERATION" in proc.stderr and "demo.dict" in proc.stderr
        proc = d.run_worker(MULTI_SCRIPT, extra_env=mpi_env(1, 2, operation="no.such"))
        assert proc.returncode == 2 and "unknown operation 'no.such'" in proc.stderr
        proc = d.run_worker(MULTI_SCRIPT, extra_env=mpi_env(1, 2, operation="demo.wf_info"))
        assert proc.returncode == 1   # runs, then KeyError: no named inputs when detached
        assert "KeyError" in proc.stderr
        proc = d.run_worker(MULTI_SCRIPT, extra_env=mpi_env(1, 2, operation="demo.none"))
        assert proc.returncode == 0, proc.stderr
        assert (d.output_dir / "plain.dat").is_file()
        assert d.connections == 0


def test_non_root_params_env():
    with FakeDaemon("mpi.solve", {"ignored": True}) as d:
        proc = d.run_worker(MPI_SCRIPT, extra_env=mpi_env(3, 4, params={"k": [1, 2]}))
        assert proc.returncode == 0, proc.stderr
        assert _rank_info(d, 3)["params"] == {"k": [1, 2]}
        proc = d.run_worker(MPI_SCRIPT, extra_env=mpi_env(3, 4, params="{bad"))
        assert proc.returncode == 2 and "FIREFORGE_MPI_PARAMS" in proc.stderr


def test_run_mpi_operation_argument(tmp_path, capsys):
    from fireforge import Worker
    w = Worker()
    seen = []
    w.task("a")(lambda ctx: seen.append(("a", ctx.mpi_rank)))
    w.task("b")(lambda ctx: seen.append(("b", ctx.mpi_rank)))
    env = {"FIREFORGE_WORKDIR": str(tmp_path), "FIREFORGE_IPC_SOCKET": str(tmp_path / "x"),
           **mpi_env(1, 2)}
    assert w.run(exit=False, env=env, mpi_operation="b") == 0
    assert w.run(exit=False, env={**env, "FIREFORGE_MPI_OPERATION": "a"}, mpi_operation="b") == 0
    assert seen == [("b", 1), ("a", 1)]
    assert w.run(exit=False, env=env) == 2


# ---------------------------------------------------------------------------
# MpiSpec in spawn / Workflow
# ---------------------------------------------------------------------------


def test_workflow_step_mpi():
    wf = Workflow("w", argv=["python3", "/opt/w.py"])
    wf.step("solve", operation="solve", ranks=64, max_ranks_per_node=16,
            mpirun_args=["--bind-to", "core"], resources={"cpu_cores": 1})
    wf.step("post", operation="post", inputs=["solve"])
    d = wf.to_dict()
    steps = {s["name"]: s for s in d["steps"]}
    assert steps["solve"]["job"]["mpi"] == {"ranks": 64, "max_ranks_per_node": 16,
                                            "mpirun_args": ["--bind-to", "core"]}
    assert "mpi" not in steps["post"]["job"]
    wf2 = Workflow("w", argv=["a.out"])
    wf2.step("s", ranks=1)
    assert wf2.steps[0].job.mpi.ranks == 1 and wf2.steps[0].job.mpi.max_ranks_per_node == 0


@pytest.mark.parametrize("kw,match", [
    ({"ranks": 0}, "ranks must be"),
    ({"ranks": 4097}, "ranks must be"),
    ({"ranks": True}, "ranks must be"),
    ({"ranks": 2.0}, "ranks must be"),
    ({"ranks": 2, "max_ranks_per_node": -1}, "max_ranks_per_node"),
    ({"max_ranks_per_node": 2}, "need ranks"),
    ({"mpirun_args": ["-x", "A"]}, "need ranks"),
    ({"ranks": 2, "argv": ["mpirun", "-n", "2", "a.out"]}, "TDR adds mpirun"),
    ({"ranks": 2, "argv": ["/usr/bin/mpiexec", "a.out"]}, "TDR adds mpirun"),
    ({"ranks": 2, "argv": ["orterun", "a.out"]}, "TDR adds mpirun"),
    ({"ranks": 2, "argv": ["prterun", "a.out"]}, "TDR adds mpirun"),
    ({"ranks": 2, "argv": ["mpirun.openmpi", "a.out"]}, "TDR adds mpirun"),
])
def test_workflow_mpi_validation(kw, match):
    wf = Workflow("w", argv=["a.out"])
    with pytest.raises(WorkflowError, match=match):
        wf.step("s", **kw)


def test_mpi_launcher_allowed_without_ranks():
    wf = Workflow("w")
    wf.step("s", argv=["mpirun", "-n", "2", "a.out"])     # plain single-node job: user's choice
    wf.step("t", argv=["mpirun_helper"], ranks=2)          # only the exact launcher names
    assert wf.steps[1].job.mpi.ranks == 2


def test_spawn_mpi(tmp_path):
    task = local_pb2.WorkerTask(operation="op")
    ctx = Context(task, {"FIREFORGE_WORKDIR": str(tmp_path)})
    ctx.spawn("big", operation="solve", argv=["/opt/solver"], ranks=128, max_ranks_per_node=32,
              mpirun_args=("--map-by", "socket"))
    ctx.spawn("small", operation="post", argv=["/opt/post"])
    big, small = ctx.spawned
    assert big.job.mpi.ranks == 128 and big.job.mpi.max_ranks_per_node == 32
    assert list(big.job.mpi.mpirun_args) == ["--map-by", "socket"]
    assert not small.HasField("job") or not small.job.HasField("mpi")
    with pytest.raises(ValueError, match="ranks must be"):
        ctx.spawn("bad", argv=["/opt/x"], ranks=5000)
    with pytest.raises(ValueError, match="TDR adds mpirun"):
        ctx.spawn("bad", argv=["mpiexec", "/opt/x"], ranks=4)
    with pytest.raises(ValueError, match="need ranks"):
        ctx.spawn("bad", argv=["/opt/x"], max_ranks_per_node=4)
