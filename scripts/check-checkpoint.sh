#!/usr/bin/env bash
set -euo pipefail

# check-checkpoint.sh — Check the checkpoint status of a running Flink job
#
# Usage:
#   ./scripts/check-checkpoint.sh [OPTIONS]
#
# Options:
#   -i, --job-id    Flink job ID (required)
#   -h, --host      JobManager host (default: localhost)
#   -p, --port      JobManager REST port (default: 8081)
#   -w, --watch     Watch mode: continuously poll every N seconds
#   --help          Show this help message

JOBMANAGER_HOST="${FLINK_JOBMANAGER_HOST:-localhost}"
JOBMANAGER_PORT="${FLINK_JOBMANAGER_PORT:-8081}"
JOB_ID=""
WATCH_INTERVAL=0

usage() {
    grep '^#' "$0" | sed 's/^# \{0,1\}//'
    exit 0
}

log()   { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }
error() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: $*" >&2; exit 1; }

while [[ $# -gt 0 ]]; do
    case "$1" in
        -i|--job-id) JOB_ID="$2"; shift 2 ;;
        -h|--host)   JOBMANAGER_HOST="$2"; shift 2 ;;
        -p|--port)   JOBMANAGER_PORT="$2"; shift 2 ;;
        -w|--watch)  WATCH_INTERVAL="${2:-30}"; shift 2 ;;
        --help)      usage ;;
        *) error "Unknown argument: $1" ;;
    esac
done

[[ -z "$JOB_ID" ]] && error "--job-id is required"

FLINK_REST_URL="http://${JOBMANAGER_HOST}:${JOBMANAGER_PORT}"

print_checkpoint_status() {
    local cp_data
    cp_data=$(curl -sf "${FLINK_REST_URL}/jobs/${JOB_ID}/checkpoints")

    echo "═══════════════════════════════════════════════════════════"
    echo " Checkpoint Status — Job: $JOB_ID"
    echo " Timestamp: $(date '+%Y-%m-%d %H:%M:%S')"
    echo "═══════════════════════════════════════════════════════════"

    echo "$cp_data" | python3 - <<'PYEOF'
import sys, json, datetime

data = json.load(sys.stdin)
counts = data.get("counts", {})
summary = data.get("summary", {})
latest = data.get("latest", {})

print(f"\n{'─'*55}")
print(" Counts:")
print(f"   Restored:   {counts.get('restored', 0)}")
print(f"   Total:      {counts.get('total', 0)}")
print(f"   In Progress:{counts.get('in_progress', 0)}")
print(f"   Completed:  {counts.get('completed', 0)}")
print(f"   Failed:     {counts.get('failed', 0)}")

if summary:
    state_size = summary.get("state_size", {})
    duration = summary.get("end_to_end_duration", {})
    print(f"\n{'─'*55}")
    print(" Summary:")
    print(f"   Avg State Size: {state_size.get('avg', 0) // 1024 // 1024} MB")
    print(f"   Max State Size: {state_size.get('max', 0) // 1024 // 1024} MB")
    print(f"   Avg Duration:   {duration.get('avg', 0)} ms")
    print(f"   Max Duration:   {duration.get('max', 0)} ms")

completed = latest.get("completed")
if completed:
    ts = completed.get("trigger_timestamp", 0)
    dt = datetime.datetime.fromtimestamp(ts / 1000).strftime('%Y-%m-%d %H:%M:%S')
    print(f"\n{'─'*55}")
    print(" Latest Completed Checkpoint:")
    print(f"   ID:         {completed.get('id')}")
    print(f"   Triggered:  {dt}")
    print(f"   Duration:   {completed.get('end_to_end_duration')} ms")
    state_mb = completed.get('state_size', 0) // 1024 // 1024
    print(f"   State Size: {state_mb} MB")
    print(f"   Path:       {completed.get('external_path', 'N/A')}")

failed = latest.get("failed")
if failed:
    print(f"\n{'─'*55}")
    print(" Latest Failed Checkpoint:")
    print(f"   ID:     {failed.get('id')}")
    print(f"   Reason: {failed.get('failure_message', 'Unknown')[:120]}")

print(f"\n{'═'*55}")
PYEOF
}

if [[ "$WATCH_INTERVAL" -gt 0 ]]; then
    log "Watch mode: refreshing every ${WATCH_INTERVAL}s (Ctrl+C to stop)"
    while true; do
        clear 2>/dev/null || true
        print_checkpoint_status
        sleep "$WATCH_INTERVAL"
    done
else
    print_checkpoint_status
fi
