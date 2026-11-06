"""QuantumForge: Quantum ESPRESSO (pw.x 6.7) engine and the qe.* operations
(docs/design/phase8.md §4, §6, §7).

  inputs.py   pw.x input writer        runner.py  pw.x launcher (mpirun, log, errors)
  parse.py    XML / stdout parsers     engine.py  QeEngine (Engine protocol)
  ops.py      qe.relax, qe.scf, qe.nscf, qe.forces, qe.elastic (engine qe | toy)
"""
from .engine import QeEngine
from .ops import register

__all__ = ["QeEngine", "register"]
