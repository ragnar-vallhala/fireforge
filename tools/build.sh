#!/usr/bin/env bash
# Configure + build + (optionally) test, always under the shared 8 GiB cap.
#   tools/build.sh                     # build/dev, all targets
#   tools/build.sh -B build/x -t tdr_journal_tests   # own build dir, one target
#   tools/build.sh --test              # also run ctest
#   tools/build.sh --test -R tdr_jobs  # ctest regex
set -euo pipefail
cd "$(dirname "$0")/.."
BUILD=build/dev TARGET="" TEST=0 REGEX="" EXTRA=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    -B) BUILD="$2"; shift 2;;
    -t) TARGET="$2"; shift 2;;
    --test) TEST=1; shift;;
    -R) REGEX="$2"; shift 2;;
    -D*) EXTRA+=("$1"); shift;;
    *) echo "unknown arg $1" >&2; exit 2;;
  esac
done
if [[ ! -f "$BUILD/build.ninja" || ${#EXTRA[@]} -gt 0 ]]; then
  tools/capped cmake -S . -B "$BUILD" -G Ninja "${EXTRA[@]}"
fi
tools/capped cmake --build "$BUILD" ${TARGET:+--target "$TARGET"}
if [[ $TEST == 1 ]]; then
  tools/capped ctest --test-dir "$BUILD" --output-on-failure ${REGEX:+-R "$REGEX"}
fi
