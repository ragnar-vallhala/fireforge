#!/usr/bin/env bash
# Generate Python protobuf modules into sdk/python/fireforge/tdr/v1/.
set -euo pipefail
cd "$(dirname "$0")/.."
tools/capped protoc -I proto --python_out=sdk/python proto/fireforge/tdr/v1/*.proto
touch sdk/python/fireforge/tdr/__init__.py sdk/python/fireforge/tdr/v1/__init__.py
