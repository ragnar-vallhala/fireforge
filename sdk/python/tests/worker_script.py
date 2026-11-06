"""Worker used by the end-to-end tests (launched as a subprocess)."""
import sys
import time

from fireforge import Cancelled, JobResult, Worker

worker = Worker()


@worker.task("demo.success")
def success(context):
    context.report_progress(0.25, "starting")
    structure = context.load_structure(raw=True)
    context.report_progress(7, "clamped high")
    context.report_progress(-1, "clamped low")
    a = context.save_output("summary", "formula=" + structure.format + "\n")
    b = context.save_output("blob", b"\x00\x01\x02", media_type="application/octet-stream",
                            filename="sub/blob.bin")
    r = context.save_result({"params": context.params, "job": context.job_id,
                             "attempt": context.attempt_number, "inputs": len(context.inputs),
                             "material": context.material_id})
    (context.output_dir / "undeclared.txt").write_text("not declared")
    return JobResult(success=True, artifacts=[r, a, b])


@worker.task("demo.raise")
def boom(context):
    raise RuntimeError("kaboom")


@worker.task("demo.cancel")
def cancellable(context):
    context.report_progress(0.1, "waiting for cancel")
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        context.check_cancelled()
        time.sleep(0.01)
    return JobResult(success=True)


@worker.task("demo.escape")
def escape(context):
    (context.output_dir.parent / "outside.txt").write_text("x")
    return JobResult(success=True, artifacts=["../outside.txt"])


@worker.task("demo.save_escape")
def save_escape(context):
    context.save_output("evil", "x", filename="../../evil.txt")


@worker.task("demo.dict")
def as_dict(context):
    return {"value": 42}


@worker.task("demo.none")
def as_none(context):
    (context.output_dir / "plain.dat").write_bytes(b"abc")


@worker.task("demo.fail")
def fail(context):
    return JobResult(success=False, error="bad input", retryable=True)


@worker.task("demo.wf_info")
def wf_info(context):
    try:
        context.input("missing")
    except KeyError as exc:
        missing = str(exc)
    return {"workflow_id": context.workflow_id, "step": context.step,
            "named": sorted(context.named_inputs),
            "structure": context.input("structure").read_text(),
            "names": [i.name for i in context.inputs],
            "resume": context.load_checkpoint_json(), "missing": missing}


@worker.task("demo.checkpoint")
def checkpoint(context):
    for i in range(3):
        context.checkpoint({"i": i}, name="state")
    ext = context.workdir / "external.dat"
    ext.write_bytes(b"ext")
    context.checkpoint(ext, name="ext")
    return {"ok": True}


@worker.task("demo.checkpoint_error")
def checkpoint_error(context):
    from fireforge import CheckpointError
    try:
        context.checkpoint(b"x")
    except CheckpointError as exc:
        return {"error": str(exc), "retryable": exc.retryable}
    return {"error": None}


@worker.task("demo.spawn")
def spawn(context):
    from fireforge import StepInput
    for i in range(2):
        context.spawn(f"child{i}", operation="demo.child", params={"i": i},
                      inputs=[".:result"], resources={"cpu_cores": 2})
    context.spawn("join", argv=["/bin/true"], operation="demo.join",
                  inputs=[StepInput("child0", spawned=True, as_name="c0"), "child1"],
                  run_if="always", allow_failure=True, checkpoint=True, max_attempts=3,
                  timeout=60, env={"X": "1"})
    return {"spawned": True}


@worker.task("demo.spawn_fail")
def spawn_fail(context):
    context.spawn("child", operation="demo.child")
    return JobResult(success=False, error="nope")


@worker.task("demo.record")
def record(context):
    context.save_output("relaxed", {"energy_ev": -3.2})
    context.record("qe.relax",
                   structure={"lattice": [5.6, 0, 0, 0, 5.6, 0, 0, 0, 5.6],
                              "sites": [{"species": "Na", "frac": [0, 0, 0]},
                                        {"species": "Cl", "frac": [0.5, 0.5, 0.5]}],
                              "reduced_formula": "NaCl", "fingerprint": "fp1:abc"},
                   properties={"energy_ev": -3.2}, conditions={"pressure_gpa": 0},
                   method={"solver": "toy", "engine": "toy", "settings": {"ecut_ry": 40}},
                   outputs=["relaxed"])
    context.record("te.evaluate", structure_id="st-1", properties={"zt_peak": 1.1})
    return None


@worker.task("demo.record_fail")
def record_fail(context):
    context.record("qe.relax", structure_id="st-1")
    return JobResult(success=False, error="nope")


@worker.task("demo.record_bad_output")
def record_bad_output(context):
    context.save_output("relaxed", {"energy_ev": -3.2})
    context.record("qe.relax", structure_id="st-1", outputs=["relaxed", "bands"])
    return None


if __name__ == "__main__":
    worker.run()
