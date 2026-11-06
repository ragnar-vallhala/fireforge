# FireForge science (Phase 8)

`fireforge_science`: CrystalForge, QuantumForge (Quantum ESPRESSO), PhonoForge (Phonopy,
phono3py), TransportForge (BoltzTraP2), StabilityForge and the thermoelectric evaluation,
run as FireForge SDK workers. Design: `docs/design/phase8.md`.

Setup per node: `sudo apt install quantum-espresso`, then `tools/sci-setup.sh`.
Tests: `tools/pytest.sh science/tests` (QE-backed tests: `-m qe`).

## Running

```sh
python -m fireforge_science doctor                 # check this node; prints the [node] capabilities line
python -m fireforge_science submit-validation -o val.json [--engine toy] [--cpus 4]
python -m fireforge_science submit-campaign --x Zr,Hf,Ti --y Ni,Co --z Sn,Sb -o hh.json
python -m fireforge_science literature-import > lit.json
tdrctl cp workflow-submit val.json; tdrctl cp calc-import lit.json
tdrctl cp materials; tdrctl cp calcs --kind te.evaluate
```

Each science node needs a `fireforge-sci` wrapper on tdrd's PATH, which runs
`.venv/bin/python -m fireforge_science.ops` with `PYTHONPATH=sdk/python:science` and
`FIREFORGE_WORKER_ARGV='["fireforge-sci"]'`, plus `[node] capabilities` from `doctor`.
