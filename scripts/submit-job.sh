#!/usr/bin/env bash
set -euo pipefail

# submit-job.sh — Submit a Flink job to a running JobManager
#
# Usage:
#   ./scripts/submit-job.sh [OPTIONS]
#
# Options:
#   -j, --job-type     Job type: ssp | dsp (required)
#   -m, --job-mode     Job mode: aggregation | storage | all (default: all)
#   -h, --host         JobManager host (default: localhost)
#   -p, --port         JobManager REST port (default: 8081)
#   -P, --parallelism  Job parallelism (default: 4)
#   -c, --config       Path to job properties file
#   --help             Show this help message

JOBMANAGER_HOST="${FLINK_JOBMANAGER_HOST:-localhost}"
JOBMANAGER_PORT="${FLINK_JOBMANAGER_PORT:-8081}"
JOB_TYPE=""
JOB_MODE="all"
PARALLELISM=4
CONFIG_FILE=""

usage() {
    grep '^#' "$0" | sed 's/^# \{0,1\}//'
    exit 0
}

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }
error() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: $*" >&2; exit 1; }

while [[ $# -gt 0 ]]; do
    case "$1" in
        -j|--job-type)    JOB_TYPE="$2"; shift 2 ;;
        -m|--job-mode)    JOB_MODE="$2"; shift 2 ;;
        -h|--host)        JOBMANAGER_HOST="$2"; shift 2 ;;
        -p|--port)        JOBMANAGER_PORT="$2"; shift 2 ;;
        -P|--parallelism) PARALLELISM="$2"; shift 2 ;;
        -c|--config)      CONFIG_FILE="$2"; shift 2 ;;
        --help)           usage ;;
        *) error "Unknown argument: $1" ;;
    esac
done

[[ -z "$JOB_TYPE" ]] && error "--job-type is required (ssp | dsp)"
[[ "$JOB_TYPE" != "ssp" && "$JOB_TYPE" != "dsp" ]] && error "Invalid job type: $JOB_TYPE. Must be ssp or dsp"
[[ "$JOB_MODE" != "all" && "$JOB_MODE" != "aggregation" && "$JOB_MODE" != "storage" ]] && \
    error "Invalid job mode: $JOB_MODE. Must be all, aggregation, or storage"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

case "$JOB_TYPE" in
    ssp)
        JAR_PATH="${PROJECT_ROOT}/ssp-stream/build/libs/ssp-stream-1.0.0-SNAPSHOT-all.jar"
        MAIN_CLASS="com.facebook.ads.stream.ssp.SSPStreamApp"
        ;;
    dsp)
        JAR_PATH="${PROJECT_ROOT}/dsp-stream/build/libs/dsp-stream-1.0.0-SNAPSHOT-all.jar"
        MAIN_CLASS="com.facebook.ads.stream.dsp.DSPStreamApp"
        ;;
esac

[[ ! -f "$JAR_PATH" ]] && error "JAR not found: $JAR_PATH. Run './gradlew :${JOB_TYPE}-stream:shadowJar' first."

FLINK_REST_URL="http://${JOBMANAGER_HOST}:${JOBMANAGER_PORT}"

log "Checking JobManager availability at $FLINK_REST_URL ..."
if ! curl -sf "${FLINK_REST_URL}/overview" > /dev/null 2>&1; then
    error "Cannot reach Flink JobManager at $FLINK_REST_URL"
fi

log "Uploading JAR: $JAR_PATH"
UPLOAD_RESPONSE=$(curl -sf -X POST \
    -H "Expect:" \
    -F "jarfile=@${JAR_PATH}" \
    "${FLINK_REST_URL}/jars/upload")

JAR_ID=$(echo "$UPLOAD_RESPONSE" | python3 -c "import sys,json; print(json.load(sys.stdin)['filename'].split('/')[-1])")
log "JAR uploaded: $JAR_ID"

PROGRAM_ARGS="--job.mode ${JOB_MODE}"
[[ -n "$CONFIG_FILE" ]] && PROGRAM_ARGS="$PROGRAM_ARGS --config ${CONFIG_FILE}"

SUBMIT_PAYLOAD=$(cat <<EOF
{
  "entryClass": "${MAIN_CLASS}",
  "parallelism": ${PARALLELISM},
  "programArgsList": $(echo "$PROGRAM_ARGS" | python3 -c "import sys,json; args=sys.stdin.read().split(); print(json.dumps(args))")
}
EOF
)

log "Submitting ${JOB_TYPE^^} job (mode=$JOB_MODE, parallelism=$PARALLELISM) ..."
SUBMIT_RESPONSE=$(curl -sf -X POST \
    -H "Content-Type: application/json" \
    -d "$SUBMIT_PAYLOAD" \
    "${FLINK_REST_URL}/jars/${JAR_ID}/run")

JOB_ID=$(echo "$SUBMIT_RESPONSE" | python3 -c "import sys,json; print(json.load(sys.stdin)['jobid'])")
log "Job submitted successfully. Job ID: $JOB_ID"
log "Dashboard: ${FLINK_REST_URL}/#/job/${JOB_ID}"
