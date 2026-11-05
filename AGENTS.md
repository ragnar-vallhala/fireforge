# Agent Guidelines

FireForge: distributed materials-discovery platform. Vision: `Description.md`.
Current work: Phase 1 (TDR local job execution), design in `docs/design/phase1.md`.

## Memory cap — mandatory

All builds, tests and other heavy commands MUST run through `tools/capped`.
It puts the process in the shared `fireforge-build.slice`, which has ONE
8 GiB memory limit (no swap) for everything running in it, including several
agents building in parallel.

```sh
tools/build.sh [-B build/<name>] [-t <target>] [--test] [-R <ctest regex>]
tools/pytest.sh [pytest args]
tools/gen_proto.sh                 # regenerate Python protobuf code
tools/e2e.sh [pytest args]         # real tdrd + tdrctl end-to-end tests
tools/capped <any other command>
```

- Never run `cmake --build`, `ninja`, `make`, `g++`, `ctest`, `pytest` or test
  binaries directly. Always prefix with `tools/capped`.
- Don't raise parallelism above `-j2` (the default from `tools/capped`).
- When agents work in parallel, each uses its own build dir: `-B build/<module>`.

## Layout

```
proto/fireforge/tdr/v1/   protobuf contracts (envelope, job, local IPC)
runtime/<module>/         C++ TDR runtime: include/tdr/<module>/, src/, tests/
runtime/daemon, ctl       tdrd and tdrctl executables
sdk/python/fireforge/     Python worker SDK (tests in sdk/python/tests)
docs/design/              design docs
deploy/                   systemd unit, example config
tools/                    build / cap scripts
control/, science/        later phases
```

## Conventions

- C++23, g++ 13, CMake + Ninja. Dependencies come from system packages only:
  Boost 1.83 (Asio, JSON, Test, program_options), protobuf 3.21, SQLite, OpenSSL.
  No network fetches in the build.
- Public headers under `runtime/*/include/tdr/` are the contracts between
  modules. Don't change another module's header; if a change is unavoidable,
  keep it minimal and record it in `docs/design/phase1.md`.
- New `.cpp` files in `src/` and `tests/` are picked up automatically (glob).
- Tests: Boost.Test (`BOOST_AUTO_TEST_CASE`) in `runtime/<module>/tests/`, no
  `BOOST_TEST_MODULE` (it lives in `runtime/testing/test_main.cpp`).
- Single-threaded daemon: module code runs on one io_context thread and needs no locks.
- Wire integers are big-endian. Never send raw C++ structs.

## Commits

- Do not add AI attribution to commit messages or pull requests. No `Co-Authored-By: Claude ...` trailers, no `Claude-Session:` links, and no "Generated with Claude Code" footers.
