#!/usr/bin/env bash
# Dev setup for the restricted job user (docs/design/phase1.md §11). Run with sudo:
#
#   sudo tools/setup-job-user.sh              # create user + install caps binary
#   sudo tools/setup-job-user.sh --caps-only  # after rebuilding tdrd
#
# Creates system user/group "fireforge-job" (no login, no home), then copies
# build/dev/runtime/daemon/tdrd (or $TDR_BUILD/...) to build/caps/tdrd, owned
# by the invoking user, with file capabilities cap_setuid,cap_setgid,cap_kill.
# Run that copy, never the plain build output, when testing job_user.
# Also installs a caps copy of tdr_supervisor_tests for its privileged test.
set -euo pipefail
[[ $EUID -eq 0 ]] || { echo "run with sudo" >&2; exit 1; }
cd "$(dirname "$0")/.."
OWNER="${SUDO_USER:?run via sudo so the binary is owned by you}"
JOB_USER="${JOB_USER:-fireforge-job}"
SRC="${TDR_BUILD:-build/dev}/runtime/daemon/tdrd"

if [[ "${1:-}" != "--caps-only" ]]; then
  if ! getent group "$JOB_USER" >/dev/null; then groupadd --system "$JOB_USER"; fi
  if ! id "$JOB_USER" >/dev/null 2>&1; then
    useradd --system --gid "$JOB_USER" --no-create-home --home-dir /nonexistent \
            --shell /usr/sbin/nologin "$JOB_USER"
  fi
  echo "user: $(id "$JOB_USER")"
fi

[[ -x "$SRC" ]] || { echo "build tdrd first: $SRC missing" >&2; exit 1; }
install -d -o "$OWNER" -g "$OWNER" -m 0755 build/caps
install -o "$OWNER" -g "$OWNER" -m 0750 "$SRC" build/caps/tdrd
setcap cap_setuid,cap_setgid,cap_kill+ep build/caps/tdrd
echo "installed: $(getcap build/caps/tdrd)"

# Supervisor unit tests include a privileged run_as case (switch to nobody).
T="${TDR_BUILD:-build/dev}/runtime/supervisor/tdr_supervisor_tests"
if [[ -x "$T" ]]; then
  install -o "$OWNER" -g "$OWNER" -m 0750 "$T" build/caps/tdr_supervisor_tests
  setcap cap_setuid,cap_setgid,cap_kill+ep build/caps/tdr_supervisor_tests
  echo "installed: $(getcap build/caps/tdr_supervisor_tests)"
fi
