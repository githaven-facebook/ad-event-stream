#!/usr/bin/env bash
set -euo pipefail

# cancel-job.sh — Cancel a running Flink job
#
# Usage:
#   ./scripts/cancel-job.sh [OPTIONS]
#
# Options:
#   -i, --job-id       Flink job ID to cancel (required)
#   -h, --host         JobManager host (default: localhost)
#   -p, --port         JobManager REST port (default: 8081)
#   -s, --savepoint    Trigger a savepoint before cancellation
#   --savepoint-dir    Directory for savepoint (default: from Flink config)
#   --help             Show this help message

JOBMANAGER_HOST="${FLINK_JOBMANAGER_HOST:-localhost}"
JOBMANAGER_PORT="${FLINK_JOBMANAGER_PORT:-8081}"
JOB_ID=""
TRIGGER_SAVEPOINT=false
SAVEPOINT_DIR=""

usage() {
    grep '^#' "$0" | sed 's/^# \{0,1\}//'
    exit 0
}

log()   { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }
error() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: $*" >&2; exit 1; }

while [[ $# -gt 0 ]]; do
    case "$1" in
        -i|--job-id)       JOB_ID="$2"; shift 2 ;;
        -h|--host)         JOBMANAGER_HOST="$2"; shift 2 ;;
        -p|--port)         JOBMANAGER_PORT="$2"; shift 2 ;;
        -s|--savepoint)    TRIGGER_SAVEPOINT=true; shift ;;
        --savepoint-dir)   SAVEPOINT_DIR="$2"; shift 2 ;;
        --help)            usage ;;
        *) error "Unknown argument: $1" ;;
    esac
done

[[ -z "$JOB_ID" ]] && error "--job-id is required"

FLINK_REST_URL="http://${JOBMANAGER_HOST}:${JOBMANAGER_PORT}"

log "Checking JobManager at $FLINK_REST_URL ..."
if ! curl -sf "${FLINK_REST_URL}/overview" > /dev/null 2>&1; then
    error "Cannot reach Flink JobManager at $FLINK_REST_URL"
fi

# Verify job exists and is running
JOB_STATUS=$(curl -sf "${FLINK_REST_URL}/jobs/${JOB_ID}" | \
    python3 -c "import sys,json; print(json.load(sys.stdin).get('state', 'UNKNOWN'))")
log "Current job state: $JOB_STATUS"

if [[ "$JOB_STATUS" != "RUNNING" ]]; then
    error "Job $JOB_ID is not in RUNNING state (current: $JOB_STATUS)"
fi

if [[ "$TRIGGER_SAVEPOINT" == "true" ]]; then
    log "Triggering savepoint before cancellation..."
    SAVEPOINT_PAYLOAD="{}"
    [[ -n "$SAVEPOINT_DIR" ]] && SAVEPOINT_PAYLOAD="{\"target-directory\": \"${SAVEPOINT_DIR}\"}"

    SAVEPOINT_RESPONSE=$(curl -sf -X POST \
        -H "Content-Type: application/json" \
        -d "$SAVEPOINT_PAYLOAD" \
        "${FLINK_REST_URL}/jobs/${JOB_ID}/savepoints")

    REQUEST_ID=$(echo "$SAVEPOINT_RESPONSE" | python3 -c "import sys,json; print(json.load(sys.stdin)['request-id'])")
    log "Savepoint triggered, request-id: $REQUEST_ID"

    # Poll for savepoint completion
    MAX_WAIT=120
    ELAPSED=0
    while [[ $ELAPSED -lt $MAX_WAIT ]]; do
        SP_STATUS=$(curl -sf "${FLINK_REST_URL}/jobs/${JOB_ID}/savepoints/${REQUEST_ID}" | \
            python3 -c "import sys,json; d=json.load(sys.stdin); print(d['status']['id'])")
        if [[ "$SP_STATUS" == "COMPLETED" ]]; then
            SP_PATH=$(curl -sf "${FLINK_REST_URL}/jobs/${JOB_ID}/savepoints/${REQUEST_ID}" | \
                python3 -c "import sys,json; d=json.load(sys.stdin); print(d['operation']['location'])")
            log "Savepoint completed: $SP_PATH"
            break
        elif [[ "$SP_STATUS" == "FAILED" ]]; then
            error "Savepoint failed"
        fi
        sleep 5
        ELAPSED=$((ELAPSED + 5))
    done
    [[ $ELAPSED -ge $MAX_WAIT ]] && error "Savepoint timed out after ${MAX_WAIT}s"
fi

log "Cancelling job $JOB_ID ..."
curl -sf -X PATCH "${FLINK_REST_URL}/jobs/${JOB_ID}?mode=cancel" > /dev/null

# Wait for termination
MAX_WAIT=60
ELAPSED=0
while [[ $ELAPSED -lt $MAX_WAIT ]]; do
    FINAL_STATE=$(curl -sf "${FLINK_REST_URL}/jobs/${JOB_ID}" | \
        python3 -c "import sys,json; print(json.load(sys.stdin).get('state', 'UNKNOWN'))")
    if [[ "$FINAL_STATE" == "CANCELED" || "$FINAL_STATE" == "FINISHED" || "$FINAL_STATE" == "FAILED" ]]; then
        log "Job $JOB_ID reached terminal state: $FINAL_STATE"
        break
    fi
    sleep 3
    ELAPSED=$((ELAPSED + 3))
done
[[ $ELAPSED -ge $MAX_WAIT ]] && log "WARNING: Job did not terminate within ${MAX_WAIT}s"
