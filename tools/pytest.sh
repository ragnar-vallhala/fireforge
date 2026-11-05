#!/usr/bin/env bash
# Run the Python SDK tests under the shared memory cap.
set -euo pipefail
cd "$(dirname "$0")/.."
PYTHONPATH=sdk/python exec tools/capped .venv/bin/python -m pytest sdk/python/tests "$@"
