#!/usr/bin/env bash
# Phase 8 node setup (docs/design/phase8.md §6). Idempotent.
#   1. venv .venv + pip install -r science/requirements.txt (pinned), under tools/capped
#   2. SSSP 1.3.0 PBE efficiency: download + md5 check (skipped when present and valid),
#      unpack to $FIREFORGE_PSEUDO_DIR or ~/.cache/fireforge/pseudo/SSSP_1.3.0_PBE_efficiency
#   3. python -m fireforge_science doctor
# Quantum ESPRESSO itself comes from apt: sudo apt install quantum-espresso
#
#   tools/sci-setup.sh [--no-pip] [--no-pseudo]
set -euo pipefail
cd "$(dirname "$0")/.."

do_pip=1 do_pseudo=1
for a in "$@"; do
    case "$a" in
        --no-pip) do_pip=0 ;;
        --no-pseudo) do_pseudo=0 ;;
        -h|--help) sed -n '2,10p' "$0"; exit 0 ;;
        *) echo "unknown option: $a" >&2; exit 2 ;;
    esac
done

NAME=SSSP_1.3.0_PBE_efficiency
URL=https://archive.materialscloud.org/api/records/rcyfm-68h65/files
TAR_MD5=a58f1b3373f330179fd0832c48bb9a52
JSON_MD5=3153c4b20fc90a44fba0236627525644
PSEUDO_DIR="${FIREFORGE_PSEUDO_DIR:-$HOME/.cache/fireforge/pseudo/$NAME}"
ROOT="$(dirname "$PSEUDO_DIR")"

if (( do_pip )); then
    [[ -x .venv/bin/python ]] || python3 -m venv .venv
    tools/capped .venv/bin/python -m pip install -q -r science/requirements.txt
fi

md5_ok() { [[ -f "$1" ]] && [[ "$(md5sum "$1" | cut -d' ' -f1)" == "$2" ]]; }

fetch() {   # fetch <file> <md5> <dest>
    local file=$1 md5=$2 dest=$3
    if md5_ok "$dest" "$md5"; then
        echo "ok: $dest"
        return
    fi
    echo "downloading $file"
    curl -fL --retry 3 -o "$dest.part" "$URL/$file/content"
    if ! md5_ok "$dest.part" "$md5"; then
        echo "md5 mismatch for $file (expected $md5)" >&2
        rm -f "$dest.part"
        exit 1
    fi
    mv "$dest.part" "$dest"
}

if (( do_pseudo )); then
    mkdir -p "$ROOT"
    fetch "$NAME.json" "$JSON_MD5" "$ROOT/$NAME.json"
    # Unpacked and complete? (every file of the table present with its md5)
    if ! .venv/bin/python - "$ROOT/$NAME.json" "$PSEUDO_DIR" <<'EOF'
import hashlib, json, sys
from pathlib import Path
table, d = json.loads(Path(sys.argv[1]).read_text()), Path(sys.argv[2])
bad = [v["filename"] for v in table.values()
       if not (d / v["filename"]).is_file()
       or hashlib.md5((d / v["filename"]).read_bytes()).hexdigest() != v["md5"]]
sys.exit(1 if bad else 0)
EOF
    then
        fetch "$NAME.tar.gz" "$TAR_MD5" "$ROOT/$NAME.tar.gz"
        mkdir -p "$PSEUDO_DIR"
        tar -xzf "$ROOT/$NAME.tar.gz" -C "$PSEUDO_DIR" --no-same-owner
        echo "unpacked to $PSEUDO_DIR"
    else
        echo "ok: $PSEUDO_DIR (all files match the SSSP table)"
    fi
    if [[ "$PSEUDO_DIR" != "$HOME/.cache/fireforge/pseudo/$NAME" ]]; then
        # pseudo.py looks for "<dir>.json" beside the directory
        [[ -f "$PSEUDO_DIR.json" ]] || cp "$ROOT/$NAME.json" "$PSEUDO_DIR.json" 2>/dev/null || true
    fi
fi

PYTHONPATH=sdk/python:science FIREFORGE_PSEUDO_DIR="$PSEUDO_DIR" \
    tools/capped .venv/bin/python -m fireforge_science doctor
