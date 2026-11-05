"""FireForge Python worker SDK (Phase 1: local TDR worker protocol)."""

from ._version import __version__
from .worker import (
    Cancelled,
    Context,
    InputRef,
    JobResult,
    OutputRef,
    StructureFile,
    Worker,
    WorkerError,
)

__all__ = [
    "Cancelled",
    "Context",
    "InputRef",
    "JobResult",
    "OutputRef",
    "StructureFile",
    "Worker",
    "WorkerError",
    "__version__",
]
