# FireForge
## Distributed Computational Platform for Materials Discovery

**Document:** Project Description and Technical Architecture  
**Version:** 0.2  
**Status:** Architecture decisions recorded  
**Last updated:** 2026-11-05  
**Primary Research Focus:** Discovery of next-generation thermoelectric materials

---

# 1. Executive Summary

FireForge is a distributed computational materials-discovery platform designed to generate, evaluate, and prioritize new materials using first-principles physics, numerical simulation, scientific data management, and machine learning.

Its initial research focus is thermoelectric materials: compounds that convert a temperature difference directly into electrical energy through solid-state effects such as the Seebeck effect.

The long-term platform is intended to support additional materials-science domains, including photovoltaics, batteries, catalysts, superconductors, magnetic materials, and thermal-management materials.

FireForge consists of two major systems:

1. **Scientific Discovery Suite:** A collection of computational engines for crystal generation, electronic structure, phonon physics, transport calculations, material stability, and AI-assisted discovery.
2. **Distributed Execution Runtime (TDR):** A lightweight distributed computing infrastructure using native Linux services, a central control plane, direct peer-to-peer communication, resource management, and scientific artifact transfer.

The fundamental design principle is:

> Build a lightweight distributed system that spends its computational resources on scientific discovery rather than infrastructure management.

# 2. Vision and Objectives

## 2.1 Vision

Develop a reproducible, extensible, and computationally efficient platform capable of searching beyond existing material databases to identify promising hypothetical compounds for experimental investigation.

FireForge should help answer questions such as:

- Which unexplored compositions and crystal structures could exhibit exceptional thermoelectric properties?
- Which candidates combine favourable electronic transport with low lattice thermal conductivity?
- How do material properties change with temperature, carrier concentration, and structural modifications?
- Which candidates are thermodynamically, dynamically, mechanically, and thermally stable?
- Which computational experiments provide the greatest information gain?
- Which theoretical candidates have realistic synthesis potential?

## 2.2 Initial Objectives

1. Establish a validated computational baseline using established thermoelectric materials.
2. Generate hypothetical compositions and crystal structures.
3. Screen candidates for structural and thermodynamic plausibility.
4. Calculate electronic properties and transport coefficients.
5. Calculate phonon properties and lattice thermal conductivity.
6. Estimate temperature-dependent thermoelectric performance.
7. Rank candidates using multiple scientific and practical objectives.
8. Introduce machine learning and active learning to accelerate discovery.
9. Maintain complete computational provenance and reproducibility.

## 2.3 Long-Term Objectives

- Support multiple materials-science domains.
- Integrate experimental measurements with computational predictions.
- Scale from a few Linux machines to heterogeneous compute clusters.
- Develop specialized numerical methods where they offer scientific or performance advantages.
- Provide uncertainty-aware and explainable candidate recommendations.

# 3. Thermoelectric Research Foundation

The initial research problem is the discovery of materials capable of converting thermal gradients into electricity.

The Seebeck effect can be expressed as:

\[
V=S\Delta T
\]

The principal thermoelectric performance metric is the dimensionless figure of merit:

\[
ZT=\frac{S^2\sigma T}{\kappa_e+\kappa_l}
\]

Where:

- \(S\): Seebeck coefficient.
- \(\sigma\): Electrical conductivity.
- \(T\): Absolute temperature.
- \(\kappa_e\): Electronic thermal conductivity.
- \(\kappa_l\): Lattice thermal conductivity.

A good thermoelectric material requires favourable electronic transport while suppressing parasitic thermal transport.

## 3.1 Temperature Regimes

| Temperature | Research regime |
|---|---|
| 300–500 K | Low temperature |
| 500–800 K | Intermediate temperature |
| 800–1100 K | High temperature |
| Above 1100 K | Extreme temperature |

These are initial screening windows, not universal material operating limits.

## 3.2 Multi-objective Evaluation

FireForge will evaluate:

- Peak and average ZT.
- Useful operating-temperature range.
- Power factor.
- Electrical conductivity.
- Carrier mobility.
- Electronic thermal conductivity.
- Lattice thermal conductivity.
- Thermodynamic stability.
- Dynamical stability.
- Mechanical stability.
- High-temperature chemical stability.
- Elemental abundance and cost.
- Toxicity.
- Synthesis feasibility.
- Prediction uncertainty.

The objective is not simply to discover a material with the highest theoretical ZT, but to identify materials that offer a realistic combination of efficiency, stability, durability, and manufacturability.

# 4. High-Level Platform Architecture

FireForge is a modular distributed scientific platform.

Its architecture consists of five logical layers:

1. **Research Interface:** Campaign management, candidate exploration, visualization, and reporting.
2. **Control Plane:** Node registration, peer discovery, health monitoring, resource policies, scheduling, and campaign metadata.
3. **Distributed Runtime:** Native Linux daemon, peer manager, job manager, process supervisor, resource manager, and artifact manager.
4. **Scientific Engines:** Independent computational modules.
5. **Scientific Data Layer:** Structures, simulation outputs, checkpoints, datasets, and provenance.

## 4.1 Scientific Modules

| Module | Responsibility |
|---|---|
| CrystalForge | Composition and crystal-structure generation |
| QuantumForge | Electronic-structure calculations |
| PhonoForge | Phonon and lattice-dynamics calculations |
| TransportForge | Electronic and thermoelectric transport |
| StabilityForge | Thermodynamic and mechanical stability |
| DiscoverForge | Machine learning and automated discovery |
| FireForge Studio | Unified research interface |

These are logical module boundaries. They can initially be libraries or worker capabilities rather than independent network services.

# 5. TDR — Distributed Execution Runtime

TDR is the distributed execution and coordination foundation of FireForge.

## 5.1 Architectural Decisions

- C++ implementation for the runtime.
- Python interfaces for scientific workloads.
- Native Linux system services.
- Persistent daemon with isolated child processes per calculation.
- HTTP(S) control-plane communication.
- Direct peer-to-peer communication.
- Optional Tailscale connectivity.
- Central resource policies with local enforcement.
- Direct artifact transfer.
- Durable execution state and recovery.

## 5.2 Linux Installation Model

Each compute machine runs a systemd-managed TDR daemon.

```text
/etc/fireforge/
    tdr.conf
    node.key
    certificates/

/usr/bin/
    tdrd
    tdrctl

/var/lib/fireforge/
    state/
    artifacts/
    checkpoints/
    cache/

/var/log/fireforge/
    tdr.log
```

The daemon starts at boot, loads its identity, detects resources, registers with the control plane, discovers peers, and begins accepting or requesting work.

Example configuration:

```ini
[control]
address = 192.168.1.10
port = 8080
scheme = https

[node]
name = research-node-01
advertise_address = auto

[network]
listen_address = 0.0.0.0
listen_port = 7400
tailscale = auto

[resources]
max_cpu_cores = 12
max_memory_mb = 24576
max_jobs = 4
max_disk_mb = 200000
```

The control-plane address and port are the initial bootstrap configuration. Other peer information and resource policies are retrieved after registration.

# 6. Control Plane

The control plane is responsible for:

- Node registration.
- Node identity management.
- Peer directory.
- Network address discovery.
- Health and heartbeat monitoring.
- Resource profiles and per-node caps.
- Capability discovery.
- Job ownership and assignment.
- Lease management.
- Campaign management.
- Artifact metadata.
- Administrative controls.

It should remain lightweight and should not execute expensive scientific calculations.

## 6.1 Node Registration

The startup sequence:

1. TDR daemon starts.
2. Reads configuration and loads node identity.
3. Detects CPU, memory, GPU, storage, and capabilities.
4. Connects to the configured control plane using HTTPS.
5. Authenticates and registers.
6. Receives node ID and resource policy.
7. Retrieves the peer directory.
8. Establishes direct peer connections.
9. Begins periodic heartbeats.

## 6.2 Initial HTTP API

| Endpoint | Method | Purpose |
|---|---|---|
| `/api/v1/nodes/register` | POST | Register node |
| `/api/v1/nodes/heartbeat` | POST | Report health |
| `/api/v1/nodes` | GET | Retrieve peer directory |
| `/api/v1/nodes/{id}` | GET | Retrieve node details |
| `/api/v1/nodes/{id}/limits` | PATCH | Update resource limits |
| `/api/v1/nodes/{id}/offline` | POST | Graceful shutdown |
| `/api/v1/campaigns` | POST/GET | Campaign management |
| `/api/v1/jobs` | POST/GET | Job management |
| `/api/v1/jobs/{id}/status` | POST | Update job state |
| `/api/v1/artifacts/{id}` | GET | Artifact metadata |

Control-plane APIs will use versioned JSON over HTTPS.

## 6.3 Control-Plane Availability

An outage should not immediately terminate running calculations.

Workers should retain local state, allow active child processes to continue, preserve completed outputs, and reconcile state when connectivity returns.

The control plane is the sole authority for campaign state, held in PostgreSQL. Recovery is a restart and reload from the database; nodes keep running their jobs during the outage and reconcile afterwards.

Initially, a single control-plane instance is sufficient. Replication can be introduced when operational requirements justify it.

# 7. Peer Networking and Tailscale

The control plane provides peer discovery through its peer directory (LAN multicast is not used), while nodes establish direct connections for job communication and artifact transfer.

A node may advertise multiple addresses:

```json
{
  "node_id": "ff-node-04",
  "addresses": [
    {
      "type": "lan",
      "address": "192.168.1.104",
      "port": 7400
    },
    {
      "type": "tailscale",
      "address": "100.82.14.21",
      "port": 7400
    }
  ]
}
```

Connection preference:

1. LAN connection where reachable.
2. Tailscale connection where available.
3. Retry using bounded exponential backoff.
4. Mark the peer unavailable when no path works.

Tailscale is optional. TDR should treat it as a networking mechanism rather than a dependency.

Application-level authentication remains necessary even when using Tailscale.

# 8. Resource Management and Health Monitoring

The control plane manages resource policies, while each node enforces its own limits.

## 8.1 Resource Categories

- Physical capacity.
- Configured capacity.
- Allocated capacity.
- Available capacity.
- Observed utilization.

## 8.2 Resource Limits

| Resource | Configuration |
|---|---|
| CPU | Maximum allocated cores |
| Memory | Maximum memory budget |
| GPU | Maximum assigned devices |
| GPU memory | Device-specific limits where supported |
| Concurrent jobs | Maximum simultaneous calculations |
| Disk | Maximum working and artifact storage |
| Network | Optional bandwidth limit |
| Runtime | Maximum job duration |

Linux cgroups v2 will be considered for CPU and memory enforcement.

GPU resource enforcement will require device-specific integration.

## 8.3 Node Profiles

| Profile | CPU cap | RAM cap | Max jobs |
|---|---:|---:|---:|
| Small | 4 | 8 GB | 2 |
| Medium | 12 | 24 GB | 4 |
| Compute | 32 | 96 GB | 8 |
| GPU | 16 | 48 GB | 4 |

These are illustrative profiles and should be adjusted to the actual machines.

## 8.4 Health Reporting

Heartbeats should include:

- Node ID and timestamp.
- Daemon version.
- CPU capacity and utilization.
- Memory allocation and availability.
- GPU status.
- Disk availability.
- Running and queued jobs.
- Failed job count.
- Network reachability.

An initial heartbeat interval of 5–10 seconds is appropriate.

# 9. TDR Communication Protocol

The control plane uses HTTPS. Direct peer communication initially uses TCP.

Local IPC (daemon-to-worker and `tdrctl`-to-daemon) uses Unix domain sockets with the same envelope as the peer protocol.

The peer protocol uses a versioned binary message envelope: a custom fixed-size header for framing and routing, followed by a Protocol Buffers (proto3) payload.

Header:

| Field | Size |
|---|---:|
| Magic | 4 bytes |
| Protocol version | 2 bytes |
| Message type | 2 bytes |
| Flags | 4 bytes |
| Payload length | 4 bytes |
| Message ID | 8 bytes |
| Correlation ID | 8 bytes |
| Sender ID | 16 bytes |

Total: 48 bytes, excluding transport and authentication framing.

All header integers are big-endian. The payload is the serialized protobuf message implied by the message type. Native C++ structures must not be transmitted directly.

Message types are values of a protobuf enum (`proto/fireforge/tdr/v1/envelope.proto`). Initial message types:

```text
NODE_HELLO
NODE_HEARTBEAT
NODE_STATUS
PEER_ADVERTISE

JOB_SUBMIT
JOB_REQUEST
JOB_ASSIGN
JOB_ACCEPT
JOB_START
JOB_PROGRESS
JOB_COMPLETE
JOB_FAIL
JOB_CANCEL

ARTIFACT_QUERY
ARTIFACT_OFFER
ARTIFACT_REQUEST
ARTIFACT_TRANSFER

CHECKPOINT_CREATE
CHECKPOINT_RESTORE
```

# 10. Job Execution and Workflow Model

Every scientific operation is represented as a job.

A job includes:

- Unique ID.
- Campaign and material IDs.
- Operation and solver.
- Solver version.
- Resource requirements.
- Required capabilities.
- Input artifact references.
- Expected output.
- Retry policy.
- Checkpoint policy.
- Dependencies.

Example conceptual C++ representation:

```cpp
struct ResourceRequest {
    uint32_t cpu_cores;
    uint64_t memory_bytes;
    uint64_t disk_bytes;
    bool requires_gpu;
    uint32_t gpu_count;
};

struct JobSpec {
    JobId id;
    CampaignId campaign;
    MaterialId material;

    std::string operation;
    std::string solver;
    std::string solver_version;

    ResourceRequest resources;

    std::vector<ArtifactId> inputs;
    ArtifactId expected_output;

    ExecutionPolicy policy;
};
```

## 10.1 Job Lifecycle

```text
CREATED
   |
QUEUED
   |
LEASED
   |
ASSIGNED
   |
RUNNING
   |
COMMITTING
   |
COMPLETED
```

Additional states include FAILED, RETRY_PENDING, CANCELLED, EXPIRED, and VALIDATION_FAILED.

## 10.2 Job Ownership

Each job assignment receives a lease.

A worker that disappears can have its lease expire, allowing recovery.

To prevent stale workers from publishing results:

- Use fencing tokens.
- Give each execution attempt a unique ID.
- Write outputs to attempt-specific temporary locations.
- Allow only the current valid owner to commit.
- Publish final artifacts atomically.

## 10.3 DAG Execution

Scientific workflows often contain parallel branches.

For example, one relaxed structure may independently generate:

- Electronic band calculations.
- Phonon calculations.
- Mechanical stability calculations.

Their results are subsequently combined for candidate evaluation.

TDR should support DAG execution, dependencies, conditional jobs, retries, cancellation, checkpoints, and resource-aware placement.

# 11. Process Supervision

The daemon remains persistent but launches an isolated child process for each scientific calculation.

Its responsibilities include:

1. Creating the working directory.
2. Materializing inputs.
3. Preparing the environment.
4. Reserving resources.
5. Launching the executable.
6. Capturing stdout and stderr.
7. Monitoring resource use.
8. Handling cancellation.
9. Validating outputs.
10. Publishing results.

Potential Linux mechanisms:

- `posix_spawn()` or `fork()` and `execve()`.
- `pidfd`.
- Process groups.
- cgroups v2.
- `rlimit`.
- Restricted users and filesystem permissions.

MPI support should begin with single-node jobs. Multi-node MPI will be built into TDR as Phase 7, after the workflow engine (Phase 6), once distributed resource allocation and coordinated process launch are properly implemented, rather than delegated to Slurm.

# 12. Python Scientific SDK

The C++ daemon should not embed Python.

Instead, Python workers run as child processes and communicate through Unix domain sockets or a defined local IPC protocol.

The SDK provides:

- Job context.
- Artifact loading.
- Artifact saving.
- Structure access.
- Progress reporting.
- Cancellation checks.
- Checkpoint support.

Example:

```python
from fireforge import Worker, JobResult

worker = Worker()

@worker.task("transport.calculate")
def calculate_transport(context):
    structure = context.load_structure()

    result = calculate_properties(
        structure=structure,
        temperatures=[300, 500, 700, 900]
    )

    artifact = context.save_result(result)

    return JobResult(
        success=True,
        artifacts=[artifact]
    )

worker.run()
```

The SDK provides infrastructure, not a replacement for scientific libraries.

# 13. Artifact and Data Management

FireForge uses peer-local content-addressed storage (`/var/lib/fireforge/artifacts`) with direct peer-to-peer transfer. There is no shared filesystem.

Artifacts use content-addressed identifiers.

Example:

```text
artifact://sha256/8f4d...a920
```

Metadata includes:

- Artifact ID.
- Checksum.
- Size.
- Media type.
- Producer job.
- Creation time.
- Storage locations.
- Retention policy.

Potential scientific formats:

| Data | Format |
|---|---|
| Crystal structures | CIF, JSON |
| Solver inputs | Native text |
| Wavefunctions | Solver-native binary |
| Charge density | HDF5 |
| Phonon data | HDF5 |
| Transport properties | HDF5, Parquet |
| ML datasets | Parquet |
| Checkpoints | Solver-native |

## 13.1 Direct Artifact Transfer

1. Worker B requests artifact metadata.
2. Worker A advertises a valid local copy.
3. Worker B requests the artifact directly.
4. Worker A streams it.
5. Worker B verifies checksums.
6. Worker B registers its local copy.

The control plane does not relay scientific payloads.

Large transfers should support chunking, resumption, and integrity checks.

# 14. Scheduling

Scheduling is hybrid:

- **Push:** the control plane assigns jobs with strict placement needs (GPU, data locality) to a chosen node.
- **Pull:** workers request leases for all other work when they have spare capacity.

Pull is implemented first; push is added in Phase 6. The exact policy dividing the two remains open (§21).

Placement decisions have two stages.

**Eligibility:** Filter nodes that cannot execute the job.

**Ranking:** Select the most suitable eligible node.

An estimated cost function can be used:

\[
C(n,j)=
w_1C_{compute}+
w_2C_{transfer}+
w_3C_{queue}+
w_4C_{memory}+
w_5C_{contention}
\]

The weights should eventually be calibrated using actual execution measurements.

Data locality should be considered when selecting a worker.

# 15. Scientific Modules

## 15.1 CrystalForge

Responsibilities:

- Composition generation.
- Crystal structure generation.
- Structure prototype enumeration.
- Stoichiometric variation.
- Element substitution.
- Chemical constraints.
- Duplicate detection.
- Candidate metadata.

Potential integrations include pymatgen, ASE, and PyXtal.

The first candidate-generation campaign targets half-Heusler compounds.

## 15.2 QuantumForge

Responsibilities:

- Structural relaxation.
- Electronic band structures.
- Density of states.
- Band gaps.
- Effective masses.
- Fermi surfaces.
- Charge distributions.
- Spin-orbit coupling where relevant.

The first integration is Quantum ESPRESSO, validated first against known thermoelectrics (Bi2Te3, PbTe).

## 15.3 PhonoForge

Responsibilities:

- Phonon dispersion.
- Phonon density of states.
- Group velocities.
- Anharmonic interactions.
- Lattice thermal conductivity.
- Temperature-dependent vibrational properties.

The first integration is Phonopy. Phono3py and ShengBTE are potential later integrations. Hot paths such as phonon-phonon scattering sums are candidates for in-house CUDA kernels once profiling confirms them.

## 15.4 TransportForge

Responsibilities:

- Seebeck coefficient.
- Electrical conductivity.
- Electronic thermal conductivity.
- Power factor.
- Carrier concentration dependence.
- Temperature-dependent transport.
- p-type and n-type evaluation.
- Figure of merit.

The first integration is BoltzTraP2. Constant relaxation-time calculations must not be confused with experimentally established absolute conductivity.

## 15.5 StabilityForge

Responsibilities:

- Formation energy.
- Energy above convex hull.
- Thermodynamic stability.
- Dynamical stability.
- Mechanical stability.
- Thermal expansion.
- Phase transitions.
- Decomposition.
- Chemical compatibility.
- Finite-temperature behaviour.

## 15.6 DiscoverForge

Responsibilities:

- Candidate ranking.
- Surrogate models.
- Uncertainty estimation.
- Bayesian optimization.
- Active learning.
- Pareto optimization.
- Automated candidate selection.

Potential technologies include PyTorch, MatGL, M3GNet, and scikit-learn.

# 16. Universal Material Representation

Every scientific module should use a shared material representation.

| Category | Information |
|---|---|
| Identity | Material ID, composition, structure identity |
| Geometry | Lattice vectors, atomic coordinates, symmetry |
| Electronic | Bands, DOS, effective masses |
| Vibrational | Phonon frequencies and eigenvectors |
| Transport | Seebeck, conductivity, thermal conductivity |
| Stability | Formation energy, hull distance |
| Conditions | Temperature, pressure, carrier concentration |
| Provenance | Solver, version, inputs, convergence |
| Uncertainty | Model uncertainty and numerical error |
| Evidence | Prediction or experimental measurement |

Multiple crystal structures may correspond to one chemical composition. Each calculation must be associated with the exact structure and calculation conditions.

# 17. Machine Learning and Active Learning

Machine learning should be introduced after a trustworthy physics-based dataset exists.

The discovery loop:

1. Generate candidate structures.
2. Calculate inexpensive descriptors.
3. Select promising candidates.
4. Run higher-cost simulations.
5. Curate the results.
6. Train predictive models.
7. Estimate uncertainty.
8. Select the next candidates.
9. Repeat.

The optimization process should consider peak and average ZT, temperature range, stability, cost, abundance, toxicity, synthesis feasibility, and prediction uncertainty.

A predicted material must remain distinguishable from an experimentally synthesized and measured material.

The first candidate-generation campaign covers half-Heusler compounds: small 3-atom cells, a large combinatorial space, good high-temperature stability, and a rich literature for validation.

# 18. Technology Stack

| Layer | Initial direction |
|---|---|
| Runtime | C++ |
| Scientific interfaces | Python |
| Control API | HTTP(S), JSON |
| Peer protocol | 48-byte binary header + Protocol Buffers (proto3) payload over TCP |
| Local IPC | Same envelope over Unix domain sockets |
| C++ libraries | Boost (Asio, JSON, Test) |
| Service management | systemd |
| Resource enforcement | cgroups v2 |
| Control metadata | PostgreSQL, single instance on the control node; replication later |
| Local journal | SQLite |
| Scientific arrays | HDF5 / Zarr |
| Tabular datasets | Parquet |
| Numerical engines | C++, Fortran, Python |
| GPU | CUDA kernels for profiled hot paths |
| Machine learning | PyTorch |

Containers are not mandatory initially.

Multi-node MPI will be handled by TDR itself (§11, §20) rather than by Slurm or another HPC scheduler.

# 19. Reliability and Security

The system must assume that nodes disconnect, processes crash, networks partition, jobs run for days, and machines restart unexpectedly.

| Failure | Recovery |
|---|---|
| Child process crash | Mark attempt failed |
| Daemon crash | Recover local journal |
| Worker disconnect | Lease expiry and reconciliation |
| Node power loss | Reassign eligible work |
| Partial artifact transfer | Resume and verify |
| Invalid output | Reject result |
| Duplicate execution | Fencing tokens prevent stale commit |
| Control-plane outage | Preserve running jobs and reconcile later |

Exactly-once execution should not be assumed. Use durable state, idempotent operations where possible, attempt IDs, leases, deduplication, and fencing tokens.

Security requirements:

- Unique cryptographic node identities.
- Authenticated control-plane registration.
- Authenticated peer connections.
- Encrypted communication.
- Authorization for jobs and artifacts.
- Restricted child processes.
- No untrusted scientific code running as root.
- Local resource enforcement.
- Administrative audit records.

# 20. Development Roadmap

| Phase | Scope | Main deliverable |
|---|---|---|
| 1 | TDR foundation | Reliable local job execution |
| 2 | Control plane | Multi-node registration and health |
| 3 | Peer networking | Direct peer connectivity |
| 4 | Distributed execution | Remote job execution and recovery |
| 5 | Artifact infrastructure | Direct scientific data transfer |
| 6 | Workflow engine | Distributed DAG execution |
| 7 | Multi-node execution | Coordinated multi-node MPI allocation and launch |
| 8 | Scientific integration | Complete thermoelectric workflow |
| 9 | Discovery intelligence | Automated candidate discovery |

Designs: [Phase 1](docs/design/phase1.md), [Phase 2](docs/design/phase2.md), [Phase 3](docs/design/phase3.md), [Phase 4](docs/design/phase4.md), [Phase 5](docs/design/phase5.md).

## First Demonstration

Start with two Linux machines:

- Machine A: Control plane and TDR daemon.
- Machine B: TDR daemon.

Both machines register, report health, exchange peer information, and execute remote jobs.

Submit 10,000 independent synthetic numerical jobs and measure:

- Dispatch latency.
- Process startup overhead.
- Jobs completed per second.
- CPU utilization.
- Memory overhead.
- Network traffic.
- Scheduler overhead.
- Recovery after worker failure.

Test child-process crashes, daemon crashes, network disconnection, node restart, interrupted artifact transfer, control-plane outage, and stale result commitment.

Provisional targets:

| Metric | Target |
|---|---:|
| Idle daemon RAM | Under 30 MB |
| LAN dispatch overhead | Under 10 ms |
| Local setup overhead | Under 100 ms, excluding solver startup |
| Heartbeat interval | 5–10 seconds |
| Queued jobs | Thousands |
| Artifact transfer | Streaming and resumable |

These are targets to benchmark, not guaranteed performance.

# 21. Architectural Decisions

Decided 2026-11-05.

| # | Decision | Choice | Rationale |
|---:|---|---|---|
| 1 | Peer protocol | Custom 48-byte big-endian header for framing and routing; Protocol Buffers (proto3) payloads; message types from a protobuf enum (`proto/fireforge/tdr/v1/envelope.proto`) | Cheap fixed-size framing with a versioned, schema-evolving payload |
| 2 | Metadata | Single PostgreSQL instance co-located with the control plane on the control node; replication later | Simple to operate at current scale |
| 3 | Scheduling | Hybrid: control plane pushes jobs with strict placement needs (GPU, data locality); workers pull everything else via lease requests. Pull first, push added in Phase 6 | Pull is simple and self-balancing; push is needed where placement matters |
| 4 | Campaign authority | Control plane owns all campaign state in Postgres; recovery is restart and reload; nodes keep running jobs during an outage and reconcile afterwards | One source of truth without halting computation |
| 5 | Peer discovery | Control-plane peer directory only; no LAN multicast | Works across LAN and Tailscale alike |
| 6 | Artifact storage | Peer-local content-addressed storage (`/var/lib/fireforge/artifacts`) with direct peer-to-peer transfer; no shared filesystem | No shared-storage dependency; integrity from content addressing |
| 7 | First solvers | Quantum ESPRESSO, Phonopy, BoltzTraP2; validated first against Bi2Te3 and PbTe | Established open tools with known reference results |
| 8 | Multi-node MPI | Built into TDR after Phase 6 (coordinated multi-node allocation and launch), not delegated to Slurm | Keeps one scheduler and one resource model |
| 9 | In-house numerics | CUDA kernels for hot paths once profiling identifies them (e.g. phonon-phonon scattering sums) | Effort goes only where measurements justify it |
| 10 | First campaign | Half-Heusler compounds | Small 3-atom cells, large combinatorial space, good high-temperature stability, rich literature for validation |
| 11 | Control-plane implementation | C++ (`tdrcp`: Boost.Beast HTTPS + libpq), shared code with TDR | One toolchain, one protocol/TLS stack |
| 12 | Node and peer authentication | mTLS with a private FireForge CA (TLS 1.3); node certs (`OU=node`) authenticate to the control plane and to peers | One trust root for control and peer traffic; works over LAN and Tailscale |
| 13 | Artifact retention | Per-node disk cap (`max_disk_mb`); LRU eviction only of copies another reachable node holds, confirmed atomically by the control plane; the last copy is never auto-deleted; admins can pin | Bounded disks without risking data loss |

**Still open:**

- Exact hybrid push/pull policy (which jobs are pushed, and how push and pull interact).
- cgroup delegation policy.

# 22. Project Principles

- **Scientific correctness before impressive predictions.**
- **Reproducibility by design.**
- **Lightweight orchestration.**
- **Direct scientific data movement.**
- **Modular scientific engines.**
- **Reuse established physics where appropriate.**
- **Evidence-driven optimization.**
- **Clear distinction between prediction and experimental validation.**

# 23. Long-Term Direction

The initial application is thermoelectric material discovery, motivated by direct thermal-to-electrical conversion.

The same runtime, artifact system, workflow engine, material representation, and scientific infrastructure could later support:

- Photovoltaic materials.
- Battery materials.
- Catalysts.
- Superconductors.
- Magnetic materials.
- Thermal barrier and heat-management materials.

The scientific modules will evolve, but the distributed execution foundation should remain reusable.

The ultimate goal is not simply to simulate known materials faster. It is to establish a systematic computational process for proposing, evaluating, and experimentally validating materials that have not yet been realized.

---

# 24. Project Summary

**FireForge** is a distributed computational materials-discovery platform, initially focused on next-generation thermoelectric materials.

Its scientific suite combines crystal generation, electronic structure, phonon physics, transport calculations, stability analysis, and AI-assisted discovery.

Its execution foundation, **TDR**, uses native Linux daemons, a lightweight HTTP(S) control plane, direct peer networking, optional Tailscale connectivity, locally enforced resource caps, isolated child processes, and direct scientific artifact transfer.

The platform begins by integrating established scientific solvers and validating calculations against known materials. It then expands toward hypothetical structure generation, multi-objective screening, active learning, and experimental validation.

**Core principle: Build a lightweight distributed system that spends its resources discovering materials, not managing infrastructure.**

# 25. Changelog

| Version | Date | Changes |
|---|---|---|
| 0.1 | — | Initial architecture specification. |
| 0.2 | 2026-11-05 | Architectural decisions recorded (§21); roadmap gains a multi-node execution phase (§20). |
| 0.3 | 2026-11-06 | Artifact retention decided (§21 #13); Phase 4 and 5 designs linked. |
