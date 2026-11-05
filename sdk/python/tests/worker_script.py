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


if __name__ == "__main__":
    worker.run()
