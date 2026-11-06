#!/usr/bin/env bash
# Run Python tests under the shared memory cap. Default: the SDK tests; pass test
# paths (e.g. science/tests) to run those instead.
set -euo pipefail
cd "$(dirname "$0")/.."
paths=()
for a in "$@"; do [[ "$a" != -* && -e "${a%%::*}" ]] && paths+=("$a"); done
default=()
[[ ${#paths[@]} -eq 0 ]] && default=(sdk/python/tests)
PYTHONPATH=sdk/python:science exec tools/capped .venv/bin/python -m pytest "${default[@]}" "$@"
