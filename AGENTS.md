# Agent Guidelines

FireForge: distributed materials-discovery platform. Vision: `Description.md`.
Phase 1 (TDR local job execution, `docs/design/phase1.md`) is done.
Phase 2 (control plane, `docs/design/phase2.md`) is done.
Phase 3 (peer networking, `docs/design/phase3.md`) is done.
Phase 4 (distributed execution: central queue + leases, `docs/design/phase4.md`) is done.
Phase 5 (artifact infrastructure: peer transfer, placement, retention, `docs/design/phase5.md`) is done.
Phase 6 (workflow engine: DAGs, fan-out, checkpoints, ranked placement, `docs/design/phase6.md`) is done.
Phase 7 (multi-node MPI: gang placement, reserve-and-drain, launch agent, `docs/design/phase7.md`) is done.
Phase 8 (scientific integration: QE/phonopy/BoltzTraP2 thermoelectric workflow, materials database, `docs/design/phase8.md`) is implemented; the validation run is in progress.

## Memory cap — mandatory

All builds, tests and other heavy commands MUST run through `tools/capped`.
It puts the process in the shared `fireforge-build.slice`, which has ONE
8 GiB memory limit (no swap) and a 4-core CPU quota for everything running in it, including several
agents building in parallel.

```sh
tools/build.sh [-B build/<name>] [-t <target>] [--test] [-R <ctest regex>]
tools/pytest.sh [pytest args]       # SDK tests; pass science/tests for the science package
tools/gen_proto.sh                 # regenerate Python protobuf code
tools/e2e.sh [pytest args]         # real tdrd + tdrctl + tdrcp end-to-end tests
tools/pg.sh start|stop|psql|conninfo  # private dev Postgres in build/pg (no sudo)
tools/sci-setup.sh                 # science venv packages + SSSP pseudopotentials + doctor
tools/capped <any other command>
```

- Never run `cmake --build`, `ninja`, `make`, `g++`, `ctest`, `pytest` or test
  binaries directly. Always prefix with `tools/capped`.
- Don't raise parallelism above `-j2` (the default from `tools/capped`).
- When agents work in parallel, each uses its own build dir: `-B build/<module>`.

## Layout

```
proto/fireforge/tdr/v1/   protobuf contracts (envelope, job, local IPC)
proto/fireforge/control/  control-plane API payloads (sent as proto3 JSON)
runtime/<module>/         C++ TDR runtime: include/tdr/<module>/, src/, tests/
runtime/tls, http, agent  mTLS/PKI, HTTPS server+client, tdrd control-plane agent
runtime/peer              peer links: TLS transport + PeerManager (Phase 3)
runtime/remote            RemoteExecutor: control-plane leases run as local jobs (Phase 4)
runtime/transfer          ArtifactService (peer artifact transfer) + StoreGc (Phase 5)
runtime/gang              GangService: cross-node MPI launch relay between peers (Phase 7)
runtime/launch            tdr-launch (head/agent/member/rank) + gang file and mpirun argv builders (Phase 7)
runtime/daemon, ctl       tdrd and tdrctl executables
control/db, api, server   tdrcp: Postgres repository, API handlers, executable
sdk/python/fireforge/     Python worker SDK (tests in sdk/python/tests)
docs/design/              design docs
deploy/                   systemd unit, example config
tools/                    build / cap scripts
science/fireforge_science/ science worker (Phase 8): crystalforge, quantumforge, phonoforge, transportforge, stabilityforge, evaluate/validate, workflows
```

## Conventions

- C++23, g++ 13, CMake + Ninja. Dependencies come from system packages only:
  Boost 1.83 (Asio, Beast, JSON, Test, program_options), protobuf 3.21, SQLite,
  OpenSSL, libpq (PostgreSQL 16).
  No network fetches in the build.
- Public headers under `runtime/*/include/tdr/` are the contracts between
  modules. Don't change another module's header; if a change is unavoidable,
  keep it minimal and record it in `docs/design/phase1.md`.
- New `.cpp` files in `src/` and `tests/` are picked up automatically (glob).
- Postgres tests use throwaway clusters (`tdr::cpdb::testing::TempCluster`),
  never the system server.
- Tests: Boost.Test (`BOOST_AUTO_TEST_CASE`) in `runtime/<module>/tests/`, no
  `BOOST_TEST_MODULE` (it lives in `runtime/testing/test_main.cpp`).
- Single-threaded daemon: module code runs on one io_context thread and needs no locks.
- Wire integers are big-endian. Never send raw C++ structs.

## Commits

- Do not add AI attribution to commit messages or pull requests. No `Co-Authored-By: Claude ...` trailers, no `Claude-Session:` links, and no "Generated with Claude Code" footers.
