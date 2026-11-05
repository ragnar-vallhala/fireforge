#!/usr/bin/env bash
# Private PostgreSQL dev cluster in build/pg (no sudo, Unix socket only).
#
#   tools/pg.sh start     initdb on first use, then start (under tools/capped)
#   tools/pg.sh stop
#   tools/pg.sh status
#   tools/pg.sh env       print `export PGHOST=... PGDATABASE=...` (eval "$(tools/pg.sh env)")
#   tools/pg.sh conninfo  print the libpq conninfo for tdrcp
#   tools/pg.sh psql [args]
#   tools/pg.sh reset     stop and delete the cluster
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
dir="$root/build/pg"
data="$dir/data"
sock="$dir"
port="${FF_PG_PORT:-55432}"
db=fireforge

bin="${PG_BIN:-}"
if [[ -z "$bin" ]]; then
    if [[ -x /usr/lib/postgresql/16/bin/initdb ]]; then bin=/usr/lib/postgresql/16/bin
    else bin="$(pg_config --bindir)"; fi
fi

running() { "$bin/pg_ctl" -D "$data" status >/dev/null 2>&1; }

case "${1:-}" in
start)
    mkdir -p "$dir"
    if [[ ! -f "$data/PG_VERSION" ]]; then
        "$root/tools/capped" "$bin/initdb" -D "$data" -U "$USER" --auth=trust -E UTF8 --no-instructions >"$dir/initdb.log"
        cat >>"$data/postgresql.conf" <<EOF
listen_addresses = ''
unix_socket_directories = '$sock'
port = $port
shared_buffers = 32MB
max_connections = 50
EOF
    fi
    if ! running; then
        # pg_ctl forks the server; it inherits the capped scope's slice.
        "$root/tools/capped" "$bin/pg_ctl" -D "$data" -l "$dir/server.log" -w start >/dev/null
    fi
    "$bin/psql" -h "$sock" -p "$port" -d postgres -Atqc "select 1 from pg_database where datname='$db'" | grep -q 1 \
        || "$bin/createdb" -h "$sock" -p "$port" "$db"
    echo "postgres running: $("$0" conninfo)"
    ;;
stop)   running && "$bin/pg_ctl" -D "$data" -m fast -w stop >/dev/null; echo stopped ;;
status) running && echo "running: $("$0" conninfo)" || { echo "not running"; exit 1; } ;;
env)    echo "export PGHOST='$sock' PGPORT=$port PGDATABASE=$db" ;;
conninfo) echo "host=$sock port=$port dbname=$db" ;;
psql)   shift; exec "$bin/psql" -h "$sock" -p "$port" -d "$db" "$@" ;;
reset)  running && "$bin/pg_ctl" -D "$data" -m fast -w stop >/dev/null || true; rm -rf "$dir"; echo removed ;;
*) sed -n 2,11p "$0"; exit 2 ;;
esac
