#!/usr/bin/env bash
# Build tdrd/tdrctl and run the end-to-end tests, all under the memory cap.
set -euo pipefail
cd "$(dirname "$0")/.."
BUILD="${TDR_BUILD:-build/dev}"
tools/build.sh -B "$BUILD" -t tdrd
tools/build.sh -B "$BUILD" -t tdrctl
[[ -f sdk/python/fireforge/tdr/v1/job_pb2.py ]] || tools/gen_proto.sh
TDR_BUILD="$PWD/$BUILD" PYTHONPATH=sdk/python exec tools/capped .venv/bin/python -m pytest tests/e2e "$@"
