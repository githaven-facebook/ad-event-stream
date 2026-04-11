#!/usr/bin/env bash
set -euo pipefail

# local-dev.sh — Local development environment management
#
# Usage:
#   ./scripts/local-dev.sh [COMMAND]
#
# Commands:
#   start     Start local dev environment (Flink, Kafka, MinIO)
#   stop      Stop and remove containers
#   restart   Restart containers
#   status    Show container status
#   logs      Tail logs (optionally: logs <service>)
#   build     Build fat JARs
#   deploy    Build and submit jobs to local Flink
#   clean     Stop containers and remove volumes
#   --help    Show this help message

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
COMPOSE_FILE="${PROJECT_ROOT}/docker-compose.yml"

log()   { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }
error() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: $*" >&2; exit 1; }

usage() {
    grep '^#' "$0" | sed 's/^# \{0,1\}//'
    exit 0
}

wait_for_service() {
    local name="$1"
    local url="$2"
    local max_wait="${3:-60}"
    local elapsed=0

    log "Waiting for $name to be ready..."
    while [[ $elapsed -lt $max_wait ]]; do
        if curl -sf "$url" > /dev/null 2>&1; then
            log "$name is ready"
            return 0
        fi
        sleep 3
        elapsed=$((elapsed + 3))
    done
    error "$name did not become ready within ${max_wait}s"
}

cmd_start() {
    log "Starting local development environment..."
    docker compose -f "$COMPOSE_FILE" up -d

    wait_for_service "Flink JobManager" "http://localhost:8081/overview"
    wait_for_service "Kafka" "http://localhost:9092" 30 || true  # Kafka doesn't have HTTP
    wait_for_service "MinIO" "http://localhost:9000/minio/health/live"

    log "Creating Kafka topics..."
    docker compose -f "$COMPOSE_FILE" exec kafka \
        kafka-topics.sh --create --if-not-exists \
        --bootstrap-server localhost:9092 \
        --partitions 4 --replication-factor 1 \
        --topic ad-events-ssp || true

    docker compose -f "$COMPOSE_FILE" exec kafka \
        kafka-topics.sh --create --if-not-exists \
        --bootstrap-server localhost:9092 \
        --partitions 4 --replication-factor 1 \
        --topic ad-events-dsp || true

    docker compose -f "$COMPOSE_FILE" exec kafka \
        kafka-topics.sh --create --if-not-exists \
        --bootstrap-server localhost:9092 \
        --partitions 2 --replication-factor 1 \
        --topic ad-metrics-ssp || true

    docker compose -f "$COMPOSE_FILE" exec kafka \
        kafka-topics.sh --create --if-not-exists \
        --bootstrap-server localhost:9092 \
        --partitions 2 --replication-factor 1 \
        --topic ad-metrics-dsp || true

    docker compose -f "$COMPOSE_FILE" exec kafka \
        kafka-topics.sh --create --if-not-exists \
        --bootstrap-server localhost:9092 \
        --partitions 1 --replication-factor 1 \
        --topic ad-events-dlq || true

    log "Creating MinIO bucket..."
    docker compose -f "$COMPOSE_FILE" exec minio \
        mc alias set local http://localhost:9000 minioadmin minioadmin 2>/dev/null || true
    docker compose -f "$COMPOSE_FILE" exec minio \
        mc mb --ignore-existing local/ad-event-data || true

    log ""
    log "Local dev environment is running!"
    log "  Flink Web UI:    http://localhost:8081"
    log "  MinIO Console:   http://localhost:9001  (user: minioadmin, pass: minioadmin)"
    log "  Kafka:           localhost:9092"
}

cmd_stop() {
    log "Stopping local dev environment..."
    docker compose -f "$COMPOSE_FILE" down
}

cmd_restart() {
    cmd_stop
    cmd_start
}

cmd_status() {
    docker compose -f "$COMPOSE_FILE" ps
}

cmd_logs() {
    local service="${1:-}"
    if [[ -n "$service" ]]; then
        docker compose -f "$COMPOSE_FILE" logs -f "$service"
    else
        docker compose -f "$COMPOSE_FILE" logs -f
    fi
}

cmd_build() {
    log "Building fat JARs..."
    cd "$PROJECT_ROOT"
    ./gradlew :ssp-stream:shadowJar :dsp-stream:shadowJar --info
    log "Build complete."
    ls -lh ssp-stream/build/libs/*-all.jar dsp-stream/build/libs/*-all.jar
}

cmd_deploy() {
    cmd_build

    log "Deploying SSP and DSP jobs to local Flink..."
    FLINK_JOBMANAGER_HOST=localhost FLINK_JOBMANAGER_PORT=8081 \
        "$SCRIPT_DIR/submit-job.sh" --job-type ssp --job-mode all \
        --config "${SCRIPT_DIR}/local-dev.properties"

    FLINK_JOBMANAGER_HOST=localhost FLINK_JOBMANAGER_PORT=8081 \
        "$SCRIPT_DIR/submit-job.sh" --job-type dsp --job-mode all \
        --config "${SCRIPT_DIR}/local-dev.properties"
}

cmd_clean() {
    log "Stopping containers and removing volumes..."
    docker compose -f "$COMPOSE_FILE" down -v --remove-orphans
}

COMMAND="${1:-}"
[[ -z "$COMMAND" || "$COMMAND" == "--help" ]] && usage

case "$COMMAND" in
    start)   cmd_start ;;
    stop)    cmd_stop ;;
    restart) cmd_restart ;;
    status)  cmd_status ;;
    logs)    cmd_logs "${2:-}" ;;
    build)   cmd_build ;;
    deploy)  cmd_deploy ;;
    clean)   cmd_clean ;;
    *) error "Unknown command: $COMMAND. Run with --help for usage." ;;
esac
