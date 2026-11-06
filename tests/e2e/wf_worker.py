"""Worker for the Phase 6 workflow end-to-end tests (tests/e2e/test_workflows.py)."""
import json
import time

from fireforge import JobResult, Worker

worker = Worker()


def read_json(path):
    return json.loads(open(path).read())


@worker.task("wf.relax")
def relax(ctx):
    ctx.save_output("structure", {"formula": ctx.params.get("formula", "TiNiSn"), "relaxed": True})


@worker.task("wf.prop")
def prop(ctx):
    s = read_json(ctx.input("structure"))
    ctx.save_output(ctx.params["kind"], {"kind": ctx.params["kind"], "formula": s["formula"],
                                         "step": ctx.step, "workflow": ctx.workflow_id})


@worker.task("wf.evaluate")
def evaluate(ctx):
    seen = {name: read_json(p) for name, p in sorted(ctx.named_inputs.items())}
    ctx.save_output("score", {"inputs": sorted(seen), "formulas": sorted({v["formula"] for v in seen.values()})})


@worker.task("wf.fanout")
def fanout(ctx):
    n = int(ctx.params["n"])
    for i in range(n):
        ctx.spawn(f"c{i}", operation="wf.child", params={"i": i})
    ctx.save_output("plan", {"children": n})


@worker.task("wf.child")
def child(ctx):
    ctx.save_output("value", {"i": ctx.params["i"]})


@worker.task("wf.join")
def join(ctx):
    values = [read_json(p)["i"] for name, p in ctx.named_inputs.items() if name.startswith("value")]
    ctx.save_output("sum", {"count": len(values), "sum": sum(values)})


@worker.task("wf.fail")
def fail(ctx):
    return JobResult(success=False, error="intentional failure", retryable=False)


@worker.task("wf.handler")
def handler(ctx):
    ctx.save_output("handled", {"ok": True})


@worker.task("wf.slow")
def slow(ctx):
    deadline = time.monotonic() + float(ctx.params.get("seconds", 30))
    while time.monotonic() < deadline:
        ctx.check_cancelled()
        time.sleep(0.1)
    ctx.save_output("done", {"ok": True})


@worker.task("wf.checkpointed")
def checkpointed(ctx):
    total = int(ctx.params["steps"])
    state = ctx.load_checkpoint_json() or {"step": 0}
    start = state["step"]
    for i in range(start, total):
        time.sleep(float(ctx.params.get("step_seconds", 0.5)))
        ctx.checkpoint({"step": i + 1})
    ctx.save_output("result", {"resumed_from": start, "steps": total, "attempt": ctx.attempt_number})


if __name__ == "__main__":
    worker.run()
