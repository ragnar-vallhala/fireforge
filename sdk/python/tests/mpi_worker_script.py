"""Single-task MPI worker for test_mpi.py (launched as a subprocess).

Each rank writes rank-<n>.json into output_dir describing what it saw.
MPI_TEST_MODE: "ok" (default), "raise", "fail".
"""
import json
import os

from fireforge import JobResult, Worker

worker = Worker()


@worker.task("mpi.solve")
def solve(context):
    context.report_progress(0.5, "halfway")          # no-op when detached
    ck = None
    if context.checkpoint_enabled or context.detached:
        ck = str(context.checkpoint({"rank": context.mpi_rank}, name="state"))
    context.spawn("child", operation="mpi.solve", ranks=2)   # never sent when detached
    mode = os.environ.get("MPI_TEST_MODE", "ok")
    info = {"rank": context.mpi_rank, "size": context.mpi_size, "root": context.is_root,
            "detached": context.detached, "operation": context.operation,
            "params": context.params, "job": context.job_id, "cancelled": context.cancelled,
            "inputs": [i.path.name for i in context.inputs], "checkpoint": ck,
            "output_dir": str(context.output_dir)}
    ref = context.save_output(f"rank-{context.mpi_rank}", info)
    info["ref"] = ref.path
    (context.output_dir / f"rank-{context.mpi_rank}.json").write_text(json.dumps(info))
    if mode == "raise":
        raise RuntimeError(f"rank {context.mpi_rank} exploded")
    if mode == "fail":
        return JobResult(success=False, error="rank says no")
    return {"rank": context.mpi_rank}


if __name__ == "__main__":
    worker.run()
