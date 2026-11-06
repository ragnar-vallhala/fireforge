"""FireForge Python worker SDK (local TDR worker protocol; Phase 6 workflows; Phase 7 MPI;
Phase 8 calculation records)."""

from . import mpi
from ._version import __version__
from .worker import (
    Cancelled,
    CheckpointError,
    Context,
    InputRef,
    JobResult,
    MAX_RECORDS,
    MAX_RECORDS_JSON_BYTES,
    OutputRef,
    StructureFile,
    Worker,
    WorkerError,
)
from .workflow import StepInput, Workflow, WorkflowError

__all__ = [
    "Cancelled",
    "CheckpointError",
    "Context",
    "InputRef",
    "JobResult",
    "MAX_RECORDS",
    "MAX_RECORDS_JSON_BYTES",
    "OutputRef",
    "StepInput",
    "StructureFile",
    "Worker",
    "WorkerError",
    "Workflow",
    "WorkflowError",
    "__version__",
    "mpi",
]
