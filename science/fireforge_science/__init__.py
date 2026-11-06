"""FireForge science modules (docs/design/phase8.md).

Core API (re-exported): Structure / load_structure / save_structure (model), Settings /
preset / kmesh (settings), BandData / Provenance / get_engine (engine). Solver modules
(quantumforge, phonoforge, ...) are imported on demand; `python -m fireforge_science.ops`
is the worker, `python -m fireforge_science doctor` checks a node.
"""
__version__ = "0.8.0"

from .model import Structure, load_structure, save_structure  # noqa: E402
from .settings import Settings, kmesh, preset  # noqa: E402
from .engine import BandData, Provenance, get_engine  # noqa: E402

__all__ = ["BandData", "Provenance", "Settings", "Structure", "get_engine", "kmesh",
           "load_structure", "preset", "save_structure", "__version__"]
