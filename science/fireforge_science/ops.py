"""The science worker (docs/design/phase8.md §4). CONTRACT: operation names are fixed.

Run as a TDR job: argv [<venv python>, -m, fireforge_science.ops]. One fireforge.Worker
registers every operation; each module adds its tasks via register(worker).
"""
from __future__ import annotations

import importlib

OPERATIONS = (
    "crystal.halfheusler", "crystal.reference", "crystal.select",
    "qe.relax", "qe.scf", "qe.nscf", "qe.forces", "qe.elastic",
    "phonon.harmonic", "phonon.collect", "phonon.fc3", "phonon.kappa3",
    "transport.boltztrap",
    "stability.formation",
    "te.evaluate", "te.validate",
)


def build_worker():
    """fireforge.Worker with all OPERATIONS registered (crystalforge, quantumforge,
    phonoforge, transportforge, stabilityforge, evaluate, validate each expose
    register(worker)).

    Modules are imported lazily, in MODULES order; a module that does not exist yet is
    skipped, so the worker runs with whatever is installed. An ImportError *inside* an
    existing module (e.g. a missing dependency) propagates."""
    from fireforge import Worker
    worker = Worker()
    for name in available_modules():
        importlib.import_module(f"{_PKG}.{name}").register(worker)
    return worker


_PKG = __name__.rpartition(".")[0] or "fireforge_science"   # also under -m (__main__)

# Submodules (or subpackages) that expose register(worker).
MODULES = ("crystalforge", "quantumforge", "phonoforge", "transportforge",
           "stabilityforge", "evaluate", "validate")


def available_modules() -> list[str]:
    """MODULES that exist in this installation (by import spec, without importing)."""
    import importlib.util
    return [m for m in MODULES
            if importlib.util.find_spec(f"{_PKG}.{m}") is not None]


if __name__ == "__main__":
    build_worker().run()
