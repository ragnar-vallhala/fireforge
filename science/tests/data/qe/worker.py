"""FakeDaemon worker for the qe.* tests: only quantumforge's operations."""
from fireforge import Worker
from fireforge_science.quantumforge import register

w = Worker()
register(w)
w.run()
