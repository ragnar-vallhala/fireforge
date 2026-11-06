# Phase 8 — Scientific integration: a complete thermoelectric workflow

Status: design (2026-11-06). Builds on Phases 1–7. Vision: `Description.md` §3, §15–§17.

Goal: one FireForge workflow takes a crystal structure to a ZT estimate with stability
checks, using real solvers (Quantum ESPRESSO, Phonopy, phono3py, BoltzTraP2), runs it
distributed over TDR nodes, and stores every calculated property with provenance in the
control plane. It is validated on five known half-Heuslers before a first candidate
campaign.

Decisions (user, 2026-11-06):

- Science stack: Quantum ESPRESSO from apt (`quantum-espresso` 6.7), Python packages
  pinned in the project venv (`tools/sci-setup.sh`), SSSP 1.3.0 PBE efficiency
  pseudopotentials downloaded and checksummed into a cache. Nodes advertise capabilities.
- κ_l: tiered. Slack model from quasi-harmonic phonons for every candidate; phono3py
  (3rd-order force constants, RTA Boltzmann transport) only for candidates that pass a
  screening threshold.
- Results: new control-plane tables (materials, structures, calculations) with REST API
  and `tdrctl cp` commands. Raw solver files stay artifacts and are referenced.
- Validation: half-Heusler set ZrNiSn, HfNiSn, TiNiSn, NbFeSb, ZrCoSb.

## 1. Components

```
science/fireforge_science/        Python package (imports fireforge SDK)
  model.py          Structure (JSON form of tdr.v1.Structure), fingerprint, pymatgen/ASE conversion
  settings.py       presets (screening, accurate), cutoffs from SSSP json, k-point density
  pseudo.py         SSSP lookup: FIREFORGE_PSEUDO_DIR or ~/.cache/fireforge/pseudo/SSSP_1.3.0_PBE_efficiency
  engine.py         Engine protocol + BandData + results dataclasses; get_engine(name)
  quantumforge/     QeEngine: pw.x input writer, output parser, runner (mpirun -np cpus pw.x)
  toy.py            ToyEngine: fast analytic stand-in (tests, plumbing e2e). Never mixed with real data.
  crystalforge/     half-Heusler enumeration, elemental reference structures, dedup
  phonoforge/       phonopy harmonic + QHA Grüneisen + Slack κ_l; phono3py tier; elastic constants
  transportforge/   BoltzTraP2 interpolation + transport tensors; ZT
  stabilityforge/   formation energy, convex hull (pymatgen), dynamical + mechanical (Born)
  evaluate.py       te.evaluate: ZT(T, doping), regime averages, abundance/cost/toxicity, screening
  validate.py       te.validate: compare with literature data, report
  data/             literature_halfheusler.json (with citations), elements.json (abundance, cost, toxicity)
  ops.py            the worker: registers every operation below (one Worker, many tasks)
  workflows.py      builders: material_pipeline(), validation_workflow(), halfheusler_campaign()
  cli.py            python -m fireforge_science {submit-validation, submit-campaign, report, doctor}
science/tests/      pytest (ToyEngine; QE-backed tests marked `qe`, skipped without pw.x)
tools/sci-setup.sh  pip install -r science/requirements.txt; download + md5-check SSSP; doctor
proto/fireforge/tdr/v1/record.proto      CalcRecord, Structure, Site, Method, Evidence
proto/fireforge/control/v1/materials.proto  Material, StructureInfo, Calculation, list/import API
control/db, api   migration 6 + repository + API (/api/v1/materials, /structures, /calculations)
runtime: WorkerResult.records → journal → JobStatus.records → UpdateJobRequest.records
```

Workers run as ordinary TDR jobs: argv `[<venv python>, -m, fireforge_science.ops]`, with
`operation` and `parameters_json` selecting the task. Each operation is idempotent given its
inputs and parameters and writes only to its job sandbox.

## 2. Records and the materials database

A worker attaches records to its successful result (like Phase 6 spawns):
`context.record(kind, structure=..., properties=..., conditions=..., method=..., outputs=[...])`.
They travel `WorkerResult.records` → JobManager journal (committed with the outputs) →
`JobStatus.records` → RemoteExecutor → `UpdateJobRequest.records`, and the control plane
stores them in the same transaction that makes the job COMPLETED. Fenced like every update;
a stale or retried attempt can't add records twice. Local (non-control-plane) jobs keep
them in `tdrctl status` only.

`CalcRecord` (record.proto):

- `kind`: operation-defined, e.g. `qe.relax`, `phonon.slack`, `te.evaluate` (§4 table).
- `structure`: `structure_id` if the worker knows it, else an inline `Structure`. The
  control plane dedups structures by `fingerprint` (§3) and materials by reduced formula.
- `properties_json`: flat object of scalars or small arrays, names with units in the key
  (`band_gap_ev`, `kappa_l_w_mk_300k`, `zt_peak_p`). Large tables go in outputs.
- `conditions_json`: temperature, pressure, doping, volume strain, etc.
- `method`: solver, solver version, functional, pseudopotentials (file + md5), preset,
  settings_json (ecut, k-mesh, smearing, conv_thr, supercell, τ), engine (`qe` or `toy`).
- `output_names`: names of this job's outputs holding the raw data; resolved to artifact
  ids on store.
- `input_artifacts`, `convergence_json`, `uncertainty_json`, `note`.
- `evidence`: PREDICTED (default), LITERATURE, MEASURED. Only admin import may set
  LITERATURE/MEASURED; worker records are always PREDICTED.

Limits: 64 records per result, 256 KiB of record JSON per result (else the job update is
rejected 400 and the job FAILED, not retryable).

Tables (migration 6): `materials(material_id, reduced_formula unique, elements text[],
nsites_min, created_at)`, `structures(structure_id, material_id, fingerprint unique,
structure jsonb, spacegroup_number, prototype, source_job_id, parent_structure_id,
created_at)`, `calculations(calc_id, structure_id, material_id, kind, properties jsonb,
conditions jsonb, method jsonb, convergence jsonb, uncertainty jsonb, evidence,
output_artifacts jsonb (name → artifact id), input_artifacts text[], job_id, attempt_id,
workflow_id, step, campaign_id, node_id, note, created_at)` with indexes on (material_id,
kind), (campaign_id, kind), (workflow_id) and GIN on properties.

API (mTLS, roles as Phase 2): `GET /api/v1/materials?formula=&element=&campaign_id=&limit=`,
`GET /api/v1/materials/{id}` (structures + latest calculation per kind),
`GET /api/v1/structures/{id}`, `GET /api/v1/calculations?material_id=&structure_id=&kind=&campaign_id=&workflow_id=&evidence=&limit=&after=`,
`GET /api/v1/calculations/{id}`, `POST /api/v1/calculations:import` (admin; LITERATURE or
MEASURED records, structure inline). `tdrctl cp materials`, `cp material ID`,
`cp calcs [--material ID] [--kind K] [--campaign ID] [--workflow ID] [--evidence E]`,
`cp calc ID`, `cp calc-import FILE`.

## 3. Structures

`Structure` JSON (same as proto3 JSON of tdr.v1.Structure): `lattice` (9 floats, Å, rows
are vectors), `sites` [{`species`, `frac` [3]}], `formula`, `reduced_formula`,
`spacegroup_number`, `spacegroup_symbol`, `prototype` (e.g. `half-heusler`), `fingerprint`,
`parent_structure_id`, `source`, `magmoms` (optional, μB per site).

Fingerprint: `sha256` of the spglib-standardized primitive cell: species sorted, lattice
reduced (Niggli), lengths rounded to 1e-3 Å, angles to 1e-2°, fractional coordinates
wrapped and rounded to 1e-3, as canonical JSON; prefixed `fp1:`. A relaxed structure is a
new structure with `parent_structure_id` = the input's.

## 4. Operations

All operations accept `preset` (`screening` | `accurate`), `engine` (`qe` | `toy`), and
`cpus` (pw.x MPI ranks on the local node; the job's `resources.cpu_cores` must match). Each
saves its main data as named outputs and attaches the listed records.

| Operation | Inputs (names) | Parameters | Outputs | Record kind: key properties |
|---|---|---|---|---|
| `crystal.halfheusler` | — | `x`,`y`,`z` element lists, `electron_counts` [8,18], `orderings` (all/known) | `candidates` (list of structures) | — (spawns, §5) |
| `crystal.reference` | — | `element` | `structure` | — |
| `qe.relax` | `structure` | `spin` (auto), `fix_cell` | `structure`, `log` | `qe.relax`: `energy_ev_per_atom`, `a_angstrom`, `volume_a3_per_atom`, `pressure_kbar`, `magnetization_mub` |
| `qe.scf` | `structure` | `kdensity` | `save` (tar of prefix.save), `log` | `qe.scf`: `energy_ev_per_atom`, `fermi_ev` |
| `qe.nscf` | `structure`, `save` | `kdensity_dense` | `bands` (BandData npz), `log` | `qe.bands`: `band_gap_ev`, `gap_direct`, `vbm_ev`, `cbm_ev`, `nelect` |
| `qe.forces` | `supercell` | `label` | `forces` (npz: forces, energy) | — |
| `qe.elastic` | `structure` | `strain` (0.01) | `elastic` (json) | `stability.elastic`: `c11_gpa`, `c12_gpa`, `c44_gpa`, `bulk_gpa`, `shear_gpa`, `born_stable`, `v_l`, `v_t`, `theta_d_elastic_k` |
| `phonon.harmonic` | `structure` | `supercell`, `volume_strains` [−0.02,0,+0.02], `amplitude` | `plan` (displacements, supercells) | — (spawns qe.forces per supercell + `phonon.collect`) |
| `phonon.collect` | `plan`, spawned `forces` | `mesh`, `t_k` | `phonon` (json: DOS, dispersion, γ, θ_D, v), `fc2` | `phonon.harmonic`: `min_freq_thz`, `dynamically_stable`, `theta_d_k`, `gruneisen`, `v_s_m_s`; `phonon.slack`: `kappa_l_w_mk` at 300/500/800/1100 K |
| `phonon.fc3` | `structure`, `fc2` | `supercell`, `cutoff_pair_a` | `plan3` | — (spawns qe.forces + `phonon.kappa3`) |
| `phonon.kappa3` | `plan3`, spawned `forces`, `fc2` | `mesh`, `t_k` | `kappa3` (json) | `phonon.bte`: `kappa_l_w_mk` at the same T |
| `transport.boltztrap` | `bands`, `structure` | `t_k` (300…1200 step 50), `doping_cm3` (±1e18…1e21), `lpfac`, `tau_s` | `transport` (json tables) | `transport.crta`: per type p/n best `pf_uw_cmk2_over_tau`, `seebeck_uv_k` at 300/800 K, optimal doping, `tau_s` assumed |
| `stability.formation` | `relax` energies via inputs (`compound`, `refs`) | — | `stability` (json) | `stability.formation`: `formation_ev_per_atom`, `e_above_hull_ev_per_atom` (vs elements + compounds in this workflow), `hull_phases` |
| `te.evaluate` | `transport`, `phonon`, `stability`, `elastic`, `bands` | `tau_s`, `kappa_source` (slack/bte), `thresholds` | `evaluation` (json) | `te.evaluate`: `zt_peak_p`, `zt_peak_n`, `t_peak_k`, `zt_avg_<regime>_<type>`, `pf_peak`, `kappa_l_300k`, `stable` flags, `abundance`, `cost_usd_kg`, `toxicity`, `screen_pass`; spawns the phono3py tier when `screen_pass` and `tier2` |
| `te.validate` | spawned/static `evaluation`s + `relax`/`bands`/`phonon` | `reference` (data file) | `report.md`, `report.json` | `te.validation`: per material deviations, pass/fail per check |

ZT = S²σT / (κ_e + κ_l), with σ and κ_e from BoltzTraP2 under the constant relaxation time
approximation (CRTA), τ = `tau_s` (default 1e-14 s). Results computed with an assumed τ are
recorded with `method.settings.tau_s` and `uncertainty.crta = true`; Description.md §15.4
applies: they are not absolute conductivities. κ_l Slack: κ = A·M̄·θ_a³·δ·n^{1/3}/(γ²T),
A(γ) from Julian, θ_a = θ_D·n^{−1/3}, θ_D from the phonon DOS second moment, γ from the
QHA mode Grüneisen average (acoustic-weighted). Uncertainty recorded as a factor of 2.

Screening thresholds (defaults): `dynamically_stable`, `born_stable`,
`e_above_hull ≤ 0.1 eV/atom`, `band_gap ≥ 0.05 eV`, and `max(zt_peak_p, zt_peak_n) ≥ 0.3`
(with Slack κ_l). The phono3py tier re-runs `te.evaluate` with `kappa_source=bte`.

## 5. Workflows

`material_pipeline(structure, name)` (one candidate):

```
relax ─┬─ scf ── nscf ── transport ─┐
       ├─ phonon (spawns forces×N + collect) ─┤
       ├─ elastic ─────────────────────┼─ evaluate (may spawn phono3py tier)
       └─ (formation, with refs) ──────┘
```

`validation_workflow()`: elemental references (`crystal.reference` → `qe.relax`) for every
element of the set, `material_pipeline` for the five compounds in their known orderings,
one `stability.formation` per compound over all relaxed compounds and references,
`te.validate` at the end (run_if ALWAYS). The phono3py tier is forced for ZrNiSn
(`tier2: "force"`) and off for the others to keep runtime bounded.

`halfheusler_campaign(x, y, z)`: `crystal.halfheusler` spawns, per composition, the three
orderings' `qe.relax` (screening preset) and a `crystal.select` join keeping the lowest
energy ordering per composition, which spawns its `material_pipeline`; references as above.

Resources per step: QE steps request `cpus` cores (default 2) and `memory` per preset;
light steps 1 core. All science steps require capability `sci` (plus `qe`, `phonopy`,
`phono3py`, `boltztrap2` as relevant). Steps are checkpointed where they run several pw.x
calculations (elastic strains, multi-volume collect is pure Python).

## 6. Node setup

- `tools/sci-setup.sh`: `pip install -r science/requirements.txt` (pinned), SSSP download
  from `https://archive.materialscloud.org/api/records/rcyfm-68h65/files/<file>/content`
  with md5 `a58f1b3373f330179fd0832c48bb9a52` (tar.gz) / `3153c4b20fc90a44fba0236627525644`
  (json), unpacked to `~/.cache/fireforge/pseudo/SSSP_1.3.0_PBE_efficiency`, then
  `python -m fireforge_science doctor` (pw.x version, pseudo checksums, imports).
- tdrd `[node] capabilities = sci,qe,phonopy,phono3py,boltztrap2` (new key: extra
  capabilities appended to the detected ones). Doctor prints the line to use.
- pw.x runs as `mpirun -np <cpus> --bind-to none pw.x -nk 1 -in pw.in` with
  `OMP_NUM_THREADS=1`; `[sci] mpirun` override via `FIREFORGE_SCI_MPIRUN`. Multi-node gangs
  (Phase 7) are not needed for 3-atom cells and are not used in this phase.

## 7. Engines

`Engine` protocol (engine.py): `relax(structure, settings) -> RelaxResult`,
`scf(structure, settings) -> ScfResult` (+ save dir), `nscf(structure, save, settings) ->
BandData`, `forces(supercell, settings) -> ForcesResult`, `stress(structure, settings)`.
`BandData` (npz): `kpoints` (frac, full grid), `ebands` (nbands×nk, Hartree), `lattvec`
(Bohr, columns), `atoms` (ASE), `nelect`, `fermi`, `dosweight`, `mommat` absent — directly
usable as a BoltzTraP2 data object. QeEngine fills it from `data-file-schema.xml`.

ToyEngine: analytic pair potential (energies, forces, stress consistent), parabolic
two-band model for bands, deterministic. Records it produces carry `method.engine = toy`
and `evidence PREDICTED`; tests and the plumbing e2e use it; `te.validate` refuses toy data.

## 8. Validation

`data/literature_halfheusler.json`: for each compound, lattice constant (exp), PBE band gap
(literature DFT), κ_l at 300 K (exp), Seebeck or peak ZT where reported, each with a
citation (DOI). `te.validate` checks:

- lattice constant within 2 % of experiment (PBE overestimates ~1 %),
- band gap within 0.2 eV of published PBE values and the right gap character,
- dynamically and mechanically stable, formation energy < 0,
- κ_l (Slack) within a factor of 3 of experiment; ZrNiSn phono3py within a factor of 1.5,
- ordering: the computed κ_l ranking matches experiment for at least 4 of 5 pairs
  adjacent in rank, and NbFeSb (p) / ZrNiSn (n) have the highest peak ZT of their type.

Literature values are also imported as LITERATURE calculations (`cp calc-import`), so
predicted and measured values sit side by side in the database (§16, §17).

## 9. Testing

- SDK/runtime/control: records round trip (worker → journal → executor → control plane),
  limits, fencing (stale attempt can't add), import, list filters, dedup by fingerprint.
- science unit tests (ToyEngine): fingerprint invariance (supercell, permutation,
  translation), half-Heusler enumeration counts, QE input writer (golden files), QE
  output parser (canned outputs captured from pw.x 6.7 runs, committed under
  science/tests/data), Slack formula against published numbers, ZT arithmetic,
  BoltzTraP2 on toy bands (S sign for p/n), hull with synthetic energies, Born criteria.
- QE-backed tests (`-m qe`): Si 2-atom relax/scf/nscf at low cutoff (seconds), ZrNiSn scf
  at screening settings.
- e2e (`tests/e2e/test_science.py`): two localhost nodes, ToyEngine, the full
  `material_pipeline` with spawned phonons and the tier-2 path, records queryable, node
  kill mid-workflow recovers; one QE-backed e2e (Si) when pw.x exists.
- Validation run (manual, long): `python -m fireforge_science submit-validation` on the
  local cluster; report committed to `docs/results/phase8-validation.md`.

Exit criteria: the validation workflow completes on ≥ 2 nodes with QE, its checks pass
(or every miss is explained in the report), all properties are queryable via
`tdrctl cp calcs`, and a 10–20 candidate half-Heusler campaign runs and ranks candidates.

## 10. Status

Implemented (2026-11-06):

- **Records path**: `context.record(...)` (SDK) → `WorkerResult.records` → JobManager, which
  checks limits and commits records with the outputs (journal schema v3, column
  `records_pb`) → `JobStatus.records` → RemoteExecutor (state file v5) →
  `JobUpdate.records`. The control plane stores them in the COMPLETED transaction.
  Invalid records fail the job without retry: the worker sees `invalid result: …`, and a
  control-plane 400 on a COMPLETED update is replaced by a FAILED update for the same
  attempt.
- **Materials database** (migration 6): fingerprint dedup with ON CONFLICT, materials by
  reduced formula, calculations with job, attempt, workflow, step, campaign and node
  context, the read API, admin import, and `tdrctl cp materials|material|calcs|calc|calc-import`.
  The 256 KiB limit is counted as protobuf bytes on the control plane and as proto3 JSON in
  tdrd and the SDK.
- **Science package** (`science/fireforge_science`): model and fingerprint, settings,
  pseudo, engine, toy, quantumforge (QE 6.7), phonoforge (phonopy, Slack, phono3py),
  crystalforge, transportforge (BoltzTraP2), stabilityforge, evaluate, validate, workflows
  and the CLI (`doctor`, `submit-validation`, `submit-campaign`, `literature-import`).
- **Tests**: science 144 passed, 1 skipped (`-m qe` live tests against pw.x included); SDK
  142; `tests/e2e/test_science.py` (toy validation workflow on two tdrd nodes, records in
  the database, literature import); C++ suites for journal, jobs, remote, cpdb and cpapi,
  each run plain and with ASan.

Choices made during implementation:

- **Worker command**: steps run `fireforge-sci`, a wrapper on each science node, so the
  same workflow runs on any node. `FIREFORGE_WORKER_ARGV` (SDK) makes spawned steps use it
  too.
- **Step resources**: every step requests memory, set by `workflows.solver_resources` /
  `light_resources`. QE steps get 6 GiB on the screening preset and 12 GiB on accurate.
  Without cgroups, the memory request also bounds the job's address space.
- **pw.x crash workaround**: Ubuntu's QE 6.7 package aborts with a false-positive
  `__snprintf_chk` overflow while computing pseudopotential md5s. The runner
  LD_PRELOADs a small shim, built with `cc` on first use; `FIREFORGE_PW_PRELOAD=none`
  disables it.
- **`qe.relax` energy output**: the step also writes `energy` (JSON with energy_ev, natoms,
  energy_ev_per_atom, formula, engine and the structure). `crystal.select` and
  `stability.formation` read it.
- **Missing κ_l**: if the phonons give no usable κ_l (dynamically unstable, or γ ≤ 0),
  `te.evaluate` still reports transport, power factor and stability, sets ZT to null with
  a `kappa_note`, and screening fails.
- **Spawned pipelines** (`crystal.select`) have no formation step, so the hull criterion is
  disabled there. Static workflows wire `stability.formation` in.
- **ToyEngine**: relaxed toy ZrNiSn has a soft TA mode at an incommensurate q (−0.1 to
  −0.6 THz, depending on the supercell). The e2e test therefore uses compressed volumes.
  This affects test data only.

Deviations:

- **Single compute node**: the validation run uses one compute node, the HP Z4
  workstation (20 cores and 30 GB given to FireForge), with the control plane on a second
  machine. Multi-node placement is covered by the e2e tests. The exit criterion "≥ 2
  nodes with QE" is therefore met only by the e2e plumbing, not by the production run.
- **Slack test**: Morelli & Slack's table was not accessible. The test checks the SI Julian
  prefactor and a hand-worked Si example: 129 W/m·K computed against 148 W/m·K measured
  at 300 K.

Validation results: `docs/results/phase8-validation.md`, written when the run finishes.
