"""MPI awareness for workers (docs/design/phase7.md §8).

A gang job (``JobSpec.mpi.ranks > 0``) runs ``command.argv`` once per rank
under an mpirun that TDR adds. Only rank 0 (on the head node) talks to tdrd;
every other rank runs the task with a *detached* context (see
``fireforge.worker``). This module only detects the rank:

* rank: ``OMPI_COMM_WORLD_RANK``, else ``PMI_RANK``, else ``PMIX_RANK``;
* size: ``OMPI_COMM_WORLD_SIZE``, else ``PMI_SIZE``.

Not under MPI (none of the rank variables set): rank 0, size 1. Under MPI
with a rank but no size variable (bare PMIx), ``size`` is 0 (unknown).
Unparsable values are ignored as if unset.

Optional variables read by non-root ranks (tdrd does not set them; set them
yourself, e.g. ``mpirun_args=["-x", "FIREFORGE_MPI_OPERATION=..."]`` or
``command.env``, when one worker script registers several tasks):

* ``FIREFORGE_MPI_OPERATION``: operation the non-root rank runs;
* ``FIREFORGE_MPI_PARAMS``: its parameters as JSON (default ``{}``).
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Mapping

RANK_VARS = ("OMPI_COMM_WORLD_RANK", "PMI_RANK", "PMIX_RANK")
SIZE_VARS = ("OMPI_COMM_WORLD_SIZE", "PMI_SIZE")
ENV_OPERATION = "FIREFORGE_MPI_OPERATION"
ENV_PARAMS = "FIREFORGE_MPI_PARAMS"
MAX_RANKS = 4096


@dataclass(frozen=True)
class MpiInfo:
    rank: int = 0
    size: int = 1
    active: bool = False    # a rank variable was found

    @property
    def is_root(self) -> bool:
        return self.rank == 0


def _int(env: Mapping[str, str], keys) -> int | None:
    for k in keys:
        v = env.get(k)
        if v is None:
            continue
        try:
            n = int(v.strip())
        except ValueError:
            continue
        if n >= 0:
            return n
    return None


def detect(env: Mapping[str, str] | None = None) -> MpiInfo:
    """Rank/size of this process from ``env`` (default ``os.environ``)."""
    env = os.environ if env is None else env
    rank = _int(env, RANK_VARS)
    if rank is None:
        return MpiInfo()
    size = _int(env, SIZE_VARS)
    return MpiInfo(rank=rank, size=size if size is not None else 0, active=True)


def rank(env: Mapping[str, str] | None = None) -> int:
    return detect(env).rank


def size(env: Mapping[str, str] | None = None) -> int:
    return detect(env).size


def is_root(env: Mapping[str, str] | None = None) -> bool:
    return detect(env).is_root
