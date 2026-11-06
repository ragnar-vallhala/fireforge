"""Workflow steps (docs/design/phase6.md §3, §5).

Two users of the same step model:

* ``Context.spawn(...)`` (inside a running worker) adds steps to the running
  workflow (dynamic fan-out). See ``fireforge.worker``.
* ``Workflow`` (on the client side) builds a ``CreateWorkflowRequest`` as
  proto3 JSON for ``tdrctl cp workflow-submit FILE``::

      from fireforge.workflow import Workflow, StepInput

      wf = Workflow("nacl-screen", campaign_id="camp-1",
                    argv=["python3", "/opt/workers/dft.py"])
      relax = wf.step("relax", operation="dft.relax", params={"encut": 520},
                      input_artifacts={"structure": "artifact://sha256/..."})
      bands = wf.step("bands", operation="dft.bands", inputs=["relax:structure"])
      phon = wf.step("phonons", operation="dft.phonons", inputs=[relax])
      wf.step("evaluate", operation="dft.evaluate",
              inputs=[StepInput(bands, "bands", as_name="bands"), phon])
      wf.step("on_failure", operation="notify", depends_on=[relax, bands, phon],
              run_if="any_failed")
      wf.write("wf.json")

Step inputs (``inputs=``):

* ``"step"``: all outputs of ``step`` (and makes ``step`` a dependency),
* ``"step:output"``: the output named ``output``,
* ``StepInput(step, output="*", spawned=False, as_name=None)``: full control;
  ``spawned=True`` also takes the same selection from every COMPLETED step in
  ``step``'s spawned subtree, ``as_name`` renames the input for the child.

Inside ``spawn`` a step may also be ``"."`` (the spawning step's own outputs)
or a sibling's short name.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping

from fireforge.tdr.v1 import job_pb2

STEP_NAME_RE = re.compile(r"[A-Za-z0-9_.-]{1,64}")
MAX_WORKFLOW_STEPS = 10_000
MAX_SPAWN = 1_000

_RUN_IF = {
    "all_succeeded": job_pb2.ALL_SUCCEEDED,
    "any_failed": job_pb2.ANY_FAILED,
    "always": job_pb2.ALWAYS,
}
_RESOURCE_KEYS = ("cpu_cores", "memory_bytes", "disk_bytes", "gpu_count")


class WorkflowError(ValueError):
    """An invalid step or workflow (bad name, unknown dependency, cycle, ...)."""


@dataclass(frozen=True)
class StepInput:
    """Wire outputs of ``step`` into a step's inputs.

    ``output``: an output name, or ``"*"`` / ``""`` for all outputs.
    ``spawned``: also the same selection from ``step``'s spawned subtree.
    ``as_name``: input name seen by the child (default: the output name).
    """

    step: str
    output: str = "*"
    spawned: bool = False
    as_name: str | None = None

    def to_proto(self) -> "job_pb2.StepInput":
        return job_pb2.StepInput(step=self.step, output=self.output or "*",
                                 spawned=bool(self.spawned), input_name=self.as_name or "")


def _check_name(name: Any, what: str = "step name") -> str:
    if not isinstance(name, str) or not STEP_NAME_RE.fullmatch(name):
        raise WorkflowError(f"invalid {what} {name!r}: must match [A-Za-z0-9_.-]{{1,64}}")
    return name


def parse_input(item: Any) -> StepInput:
    """Normalise one ``inputs=`` entry (str, ``StepInput`` or proto)."""
    if isinstance(item, StepInput):
        si = item
    elif isinstance(item, job_pb2.StepInput):
        si = StepInput(item.step, item.output or "*", item.spawned, item.input_name or None)
    elif isinstance(item, str):
        step, sep, output = item.partition(":")
        if sep and not output:
            raise WorkflowError(f"input {item!r}: empty output name after ':'")
        si = StepInput(step, output if sep else "*")
    else:
        raise WorkflowError(f"unsupported step input {item!r} (use 'step', 'step:output' or StepInput)")
    if not si.step:
        raise WorkflowError(f"input {item!r}: empty step name")
    if si.step != ".":
        _check_name(si.step, "input step")
    return si


def _parse_run_if(run_if: Any) -> int:
    if isinstance(run_if, int) and run_if in _RUN_IF.values():
        return run_if
    if isinstance(run_if, str) and run_if.lower() in _RUN_IF:
        return _RUN_IF[run_if.lower()]
    raise WorkflowError(f"invalid run_if {run_if!r} (expected one of {', '.join(_RUN_IF)})")


def _parse_resources(resources: Any) -> "job_pb2.ResourceRequest | None":
    if resources is None:
        return None
    if isinstance(resources, job_pb2.ResourceRequest):
        return resources
    if not isinstance(resources, Mapping):
        raise WorkflowError(f"resources must be a dict with keys {', '.join(_RESOURCE_KEYS)}")
    unknown = sorted(set(resources) - set(_RESOURCE_KEYS))
    if unknown:
        raise WorkflowError(f"unknown resources key(s) {', '.join(unknown)} "
                            f"(allowed: {', '.join(_RESOURCE_KEYS)})")
    rr = job_pb2.ResourceRequest()
    for k, v in resources.items():
        if not isinstance(v, int) or isinstance(v, bool) or v < 0:
            raise WorkflowError(f"resources[{k!r}] must be a non-negative int, got {v!r}")
        setattr(rr, k, v)
    return rr


def _as_list(value: Any, what: str) -> list:
    if value is None:
        return []
    if isinstance(value, (str, bytes)):
        return [value]  # a single name, not a sequence of characters
    try:
        return list(value)
    except TypeError:
        raise WorkflowError(f"{what} must be a list, got {value!r}") from None


def build_step_spec(name: str, *, operation: str | None = None, argv: Iterable[str] | None = None,
                    params: Any = None, inputs: Iterable[Any] = (), depends_on: Iterable[str] = (),
                    resources: Any = None, run_if: Any = "all_succeeded", allow_failure: bool = False,
                    input_artifacts: Any = (), capabilities: Iterable[str] = (),
                    max_attempts: int | None = None, timeout: float | None = None,
                    checkpoint: bool = False, env: Mapping[str, str] | None = None,
                    allow_self_input: bool = False) -> "job_pb2.StepSpec":
    """Build one ``StepSpec``; shared by ``Workflow.step`` and ``Context.spawn``.

    ``argv`` must be given (callers apply their defaults first). ``params``: a
    dict/list (JSON-encoded), a str (used as is) or None. ``input_artifacts``:
    a list of artifact ids, or a dict ``{input_name: artifact_id}``.
    ``timeout`` is in seconds (rounded up to whole seconds).
    """
    _check_name(name)
    argv = [str(a) for a in _as_list(argv, "argv")]
    if not argv or not argv[0]:
        raise WorkflowError(f"step {name!r}: argv must be a non-empty command")
    if operation is not None and not isinstance(operation, str):
        raise WorkflowError(f"step {name!r}: operation must be a str")

    job = job_pb2.JobSpec(operation=operation or "")
    job.command.argv.extend(argv)
    if env:
        for k, v in env.items():
            job.command.env[str(k)] = str(v)
    if params is None:
        pass
    elif isinstance(params, str):
        job.parameters_json = params
    else:
        try:
            job.parameters_json = json.dumps(params)
        except (TypeError, ValueError) as exc:
            raise WorkflowError(f"step {name!r}: params are not JSON serializable: {exc}") from None

    rr = _parse_resources(resources)
    if rr is not None:
        job.resources.CopyFrom(rr)
    if max_attempts is not None:
        if not isinstance(max_attempts, int) or max_attempts < 1:
            raise WorkflowError(f"step {name!r}: max_attempts must be an int >= 1")
        job.policy.max_attempts = max_attempts
    if timeout is not None:
        if timeout <= 0:
            raise WorkflowError(f"step {name!r}: timeout must be > 0 seconds")
        job.policy.timeout_seconds = int(-(-timeout // 1))
    if checkpoint:
        job.policy.checkpoint = True

    if isinstance(input_artifacts, Mapping):
        for iname, aid in input_artifacts.items():
            job.input_artifacts.append(str(aid))
            job.input_names.append(str(iname))
    else:
        job.input_artifacts.extend(str(a) for a in _as_list(input_artifacts, "input_artifacts"))
    job.required_capabilities.extend(str(c) for c in _as_list(capabilities, "capabilities"))

    spec = job_pb2.StepSpec(name=name, run_if=_parse_run_if(run_if), allow_failure=bool(allow_failure))
    spec.job.CopyFrom(job)
    seen: set[str] = set()
    for dep in _as_list(depends_on, "depends_on"):
        _check_name(dep, "dependency")
        if dep == name:
            raise WorkflowError(f"step {name!r} depends on itself")
        if dep not in seen:
            seen.add(dep)
            spec.depends_on.append(dep)
    for item in _as_list(inputs, "inputs"):
        si = parse_input(item)
        if si.step == "." and not allow_self_input:
            raise WorkflowError(f"step {name!r}: input step '.' is only valid in context.spawn()")
        if si.step == name:
            raise WorkflowError(f"step {name!r} takes inputs from itself")
        spec.inputs.append(si.to_proto())
    return spec


def step_dependencies(spec: "job_pb2.StepSpec") -> list[str]:
    """Explicit plus implicit (input) dependencies, without '.'."""
    deps = list(spec.depends_on)
    deps += [i.step for i in spec.inputs if i.step != "." and i.step not in deps]
    return deps


def find_cycle(graph: Mapping[str, list[str]]) -> list[str] | None:
    """Return one cycle (``[a, b, ..., a]``) in ``name -> deps``, or None."""
    WHITE, GREY, BLACK = 0, 1, 2
    color = {n: WHITE for n in graph}
    for root in graph:
        if color[root] != WHITE:
            continue
        stack: list[tuple[str, int]] = [(root, 0)]
        path: list[str] = [root]
        color[root] = GREY
        while stack:
            node, i = stack[-1]
            deps = [d for d in graph[node] if d in graph]
            if i < len(deps):
                stack[-1] = (node, i + 1)
                d = deps[i]
                if color[d] == GREY:
                    return path[path.index(d):] + [d]
                if color[d] == WHITE:
                    color[d] = GREY
                    stack.append((d, 0))
                    path.append(d)
            else:
                color[node] = BLACK
                stack.pop()
                path.pop()
    return None


class Workflow:
    """Client-side builder for a ``CreateWorkflowRequest`` (phase6.md §3, §8).

    ``argv``: default command for steps that don't pass their own (tdrd needs
    a command for every job; the SDK worker dispatches on ``operation``).
    Steps may be added in any order; dependencies are checked by
    ``validate()`` / ``to_json()`` / ``write()``.
    """

    def __init__(self, name: str, campaign_id: str | None = None, *,
                 argv: Iterable[str] | None = None) -> None:
        if not isinstance(name, str) or not name.strip():
            raise WorkflowError("workflow name must be a non-empty string")
        self.name = name
        self.campaign_id = campaign_id or ""
        self.argv = [str(a) for a in argv] if argv is not None else None
        self._steps: dict[str, job_pb2.StepSpec] = {}

    @property
    def steps(self) -> list["job_pb2.StepSpec"]:
        return list(self._steps.values())

    def step(self, name: str, *, operation: str | None = None, argv: Iterable[str] | None = None,
             params: Any = None, inputs: Iterable[Any] = (), depends_on: Iterable[str] = (),
             run_if: Any = "all_succeeded", allow_failure: bool = False, resources: Any = None,
             checkpoint: bool = False, max_attempts: int | None = None, timeout: float | None = None,
             input_artifacts: Any = (), capabilities: Iterable[str] = (),
             env: Mapping[str, str] | None = None) -> str:
        """Add a step and return its name (use it in later ``inputs``/``depends_on``)."""
        _check_name(name)
        if name in self._steps:
            raise WorkflowError(f"duplicate step name {name!r}")
        if len(self._steps) >= MAX_WORKFLOW_STEPS:
            raise WorkflowError(f"too many steps (max {MAX_WORKFLOW_STEPS})")
        if argv is None:
            argv = self.argv
        if argv is None:
            raise WorkflowError(f"step {name!r}: no argv (pass argv= or Workflow(..., argv=[...]))")
        self._steps[name] = build_step_spec(
            name, operation=operation, argv=argv, params=params, inputs=inputs,
            depends_on=depends_on, resources=resources, run_if=run_if,
            allow_failure=allow_failure, input_artifacts=input_artifacts,
            capabilities=capabilities, max_attempts=max_attempts, timeout=timeout,
            checkpoint=checkpoint, env=env)
        return name

    def validate(self) -> None:
        """Raise WorkflowError unless the DAG is valid (phase6.md §3 rules)."""
        if not self._steps:
            raise WorkflowError(f"workflow {self.name!r} has no steps")
        graph = {}
        for name, spec in self._steps.items():
            for dep in spec.depends_on:
                if dep not in self._steps:
                    raise WorkflowError(f"step {name!r} depends on unknown step {dep!r}")
            for si in spec.inputs:
                if si.step not in self._steps:
                    raise WorkflowError(f"step {name!r} takes input from unknown step {si.step!r}")
            graph[name] = step_dependencies(spec)
        cycle = find_cycle(graph)
        if cycle:
            raise WorkflowError("dependency cycle: " + " -> ".join(cycle))

    def to_proto(self):
        """The validated ``fireforge.control.v1.CreateWorkflowRequest``."""
        from fireforge.control.v1 import control_pb2

        self.validate()
        return control_pb2.CreateWorkflowRequest(name=self.name, campaign_id=self.campaign_id,
                                                 steps=self.steps)

    def to_json(self, indent: int | None = 2) -> str:
        """proto3 JSON (proto field names) for ``tdrctl cp workflow-submit``."""
        from google.protobuf import json_format

        return json_format.MessageToJson(self.to_proto(), preserving_proto_field_name=True,
                                         indent=indent, sort_keys=False)

    def to_dict(self) -> dict:
        return json.loads(self.to_json())

    def write(self, path: str | os.PathLike) -> Path:
        """Write ``to_json()`` to ``path`` and return it."""
        p = Path(path)
        p.write_text(self.to_json() + "\n", encoding="utf-8")
        return p
