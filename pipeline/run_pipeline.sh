#!/bin/bash
# StablecoinRatings — Daily Pipeline Runner
# Installed by: cp pipeline/run_pipeline.sh /usr/local/bin/stablecoinratings-runner
# Triggered by: systemd timer (see ../systemd/stablecoinratings.timer)
#
# Usage:
#   ./run_pipeline.sh           — run now (foreground)
#   ./run_pipeline.sh --dry-run — show what would be fetched without fetching

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
LOG="$REPO_ROOT/pipeline.log"
LOCK="$REPO_ROOT/.pipeline.lock"

# ── Lock to prevent concurrent runs ─────────────────────────────────────────
acquire_lock() {
    if [[ -f "$LOCK" ]]; then
        LOCK_PID=$(cat "$LOCK" 2>/dev/null || echo "")
        if [[ -n "$LOCK_PID" ]] && kill -0 "$LOCK_PID" 2>/dev/null; then
            echo "[$(date -u)] Lock held by PID $LOCK_PID — exiting" | tee -a "$LOG"
            exit 0
        else
            echo "[$(date -u)] Stale lock found (PID $LOCK_PID) — removing" | tee -a "$LOG"
            rm -f "$LOCK"
        fi
    fi
    echo $$ > "$LOCK"
    trap 'rm -f "$LOCK"' EXIT
}

# ── Main ────────────────────────────────────────────────────────────────────
acquire_lock

echo "[$(date -u)] StablecoinRatings pipeline starting" >> "$LOG"

cd "$REPO_ROOT"

# Activate virtual environment if present
if [[ -f "$REPO_ROOT/.venv/bin/activate" ]]; then
    source "$REPO_ROOT/.venv/bin/activate"
fi

# Run the pipeline
python3 pipeline/pipeline.py >> "$LOG" 2>&1
EXIT_CODE=$?

if [[ $EXIT_CODE -eq 0 ]]; then
    echo "[$(date -u)] Pipeline completed successfully" >> "$LOG"
else
    echo "[$(date -u)] Pipeline FAILED with exit code $EXIT_CODE" >> "$LOG"
    exit $EXIT_CODE
fi
