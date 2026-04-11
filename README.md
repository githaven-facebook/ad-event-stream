# ad-event-stream

Apache Flink stream processing service for the Facebook ad platform. Consumes SSP and DSP
events from Kafka, aggregates metrics in real-time over tumbling and sliding time windows,
and persists raw event data to S3 in Parquet format for downstream analytics.

```
                    ┌──────────────────────────────────────────────────────────┐
                    │                  ad-event-stream                         │
                    │                                                          │
  Kafka             │  SSP Pipeline                                            │
  ┌──────────┐      │  ┌─────────┐   ┌─────────────┐   ┌──────────────────┐  │
  │ad-events-│─────►│  │ Filter  │──►│  Aggregate  │──►│  Kafka Sink      │  │
  │   ssp    │      │  │(invalid)│   │(1min/5min)  │   │(ad-metrics-ssp)  │  │
  └──────────┘      │  └────┬────┘   └─────────────┘   └──────────────────┘  │
                    │       │                                                  │
                    │       └──────────────────────────►┌──────────────────┐  │
                    │                                   │  S3 Parquet Sink │  │
                    │  DSP Pipeline                     │(SSP partitioned) │  │
  ┌──────────┐      │  ┌─────────┐   ┌─────────────┐   └──────────────────┘  │
  │ad-events-│─────►│  │ Filter  │──►│  Aggregate  │──►┌──────────────────┐  │
  │   dsp    │      │  │(invalid)│   │(bid metrics)│   │  Kafka Sink      │  │
  └──────────┘      │  └────┬────┘   └─────────────┘   │(ad-metrics-dsp)  │  │
                    │       │                           └──────────────────┘  │
                    │       └──────────────────────────►┌──────────────────┐  │
                    │                                   │  S3 Parquet Sink │  │
                    │  Dead Letter                      │(DSP partitioned) │  │
                    │  ┌───────────────────────────────►│                  │  │
                    │  │  (invalid events → DLQ)        └──────────────────┘  │
                    └──────────────────────────────────────────────────────────┘
```

## Modules

| Module | Description |
|--------|-------------|
| `stream-common` | Shared data models (`SSPEvent`, `DSPEvent`, `AggregatedMetric`), Jackson serializers, `EventValidator`, `TimeUtils`, `StreamConfig` |
| `ssp-stream` | SSP event filtering, windowed aggregation, and S3 Parquet storage jobs |
| `dsp-stream` | DSP event filtering, bidding metrics aggregation, and S3 Parquet storage jobs |

## Architecture

### Event Models

- **`SSPEvent`** — Publisher-side events: impressions, clicks, ad requests, renders, viewability
- **`DSPEvent`** — Advertiser-side events: bid requests, bid responses, wins, losses, timeouts, spend
- **`EventAction`** enum covers both SSP and DSP action types with validation helpers

### Processing Pipelines

Each pipeline (SSP and DSP) runs two independent Flink jobs:

**1. Aggregation Job**
- Consumes raw events from Kafka
- Filters invalid events via `EventValidator` (field checks, timestamp bounds, action type validation)
- Routes invalid events to dead-letter Kafka topic via Flink side output
- Keys stream by `(campaignId, action)` for SSP or `(campaignId, advertiserId, action)` for DSP
- Applies tumbling 1-minute and sliding 5-minute event-time windows
- Sinks aggregated metrics to Kafka output topics

**2. Storage Job**
- Independent Kafka consumer group (avoids interfering with aggregation)
- Enriches events with partition path metadata
- Writes Parquet files to S3 via Flink `FileSink` with checkpoint-based rolling
- Partitioned by `year=YYYY/month=MM/day=DD/hour=HH`

### Checkpointing

- Mode: `EXACTLY_ONCE`
- State backend: RocksDB (incremental)
- Interval: 60 seconds (configurable)
- Storage: S3

## Configuration Reference

All configuration is passed via Flink `ParameterTool` (CLI args or properties file):

| Parameter | Default | Description |
|-----------|---------|-------------|
| `kafka.bootstrap.servers` | *(required)* | Kafka broker list |
| `kafka.group.id` | `ad-event-stream` | Consumer group ID |
| `kafka.topic.ssp.source` | `ad-events-ssp` | SSP source topic |
| `kafka.topic.dsp.source` | `ad-events-dsp` | DSP source topic |
| `kafka.topic.ssp.aggregated` | `ad-metrics-ssp` | SSP metrics output topic |
| `kafka.topic.dsp.aggregated` | `ad-metrics-dsp` | DSP metrics output topic |
| `kafka.topic.dead-letter` | `ad-events-dlq` | Dead-letter queue topic |
| `s3.output.bucket` | *(required)* | S3 bucket for Parquet output |
| `s3.output.prefix` | `ad-events` | S3 key prefix |
| `s3.region` | `us-east-1` | AWS region |
| `flink.checkpointing.enabled` | `true` | Enable Flink checkpointing |
| `flink.checkpoint.interval-ms` | `60000` | Checkpoint interval (ms) |
| `window.tumbling.size-ms` | `60000` | Tumbling window size (ms) |
| `window.sliding.size-ms` | `300000` | Sliding window size (ms) |
| `window.sliding.slide-ms` | `60000` | Sliding window slide interval (ms) |
| `window.allowed-lateness-ms` | `30000` | Allowed event lateness (ms) |
| `flink.parallelism.default` | `4` | Default job parallelism |
| `job.mode` | `all` | Job mode: `aggregation`, `storage`, or `all` |

## Building

```bash
# Build all modules
./gradlew build

# Build fat JARs for deployment
./gradlew :ssp-stream:shadowJar :dsp-stream:shadowJar

# Run lint
./gradlew detekt

# Run tests
./gradlew test
```

## Local Development

Requires Docker and Docker Compose.

```bash
# Start local environment (Flink + Kafka + MinIO)
./scripts/local-dev.sh start

# Build and deploy jobs to local Flink
./scripts/local-dev.sh deploy

# View logs
./scripts/local-dev.sh logs flink-jobmanager

# Stop and clean up
./scripts/local-dev.sh clean
```

Service URLs after startup:

| Service | URL |
|---------|-----|
| Flink Web UI | http://localhost:8081 |
| MinIO Console | http://localhost:9001 (minioadmin / minioadmin) |
| Kafka | localhost:9092 |

## Deployment

### Kubernetes via Helm

```bash
# Deploy SSP stream
helm upgrade --install ad-event-stream-ssp ./helm/ad-event-stream \
  --namespace ad-platform \
  --set image.tag=1.0.0 \
  --set ssp.enabled=true \
  --set dsp.enabled=false \
  --set kafka.bootstrapServers=kafka:9092 \
  --set s3.outputBucket=my-ad-events-bucket

# Deploy DSP stream
helm upgrade --install ad-event-stream-dsp ./helm/ad-event-stream \
  --namespace ad-platform \
  --set image.tag=1.0.0 \
  --set ssp.enabled=false \
  --set dsp.enabled=true \
  --set kafka.bootstrapServers=kafka:9092 \
  --set s3.outputBucket=my-ad-events-bucket
```

### Manual job submission

```bash
# Submit SSP aggregation + storage jobs
./scripts/submit-job.sh --job-type ssp --job-mode all \
  --host flink-jobmanager --port 8081

# Submit DSP aggregation job only
./scripts/submit-job.sh --job-type dsp --job-mode aggregation

# Cancel a running job (with savepoint)
./scripts/cancel-job.sh --job-id <JOB_ID> --savepoint

# Check checkpoint status
./scripts/check-checkpoint.sh --job-id <JOB_ID> --watch 30
```

## CI/CD

| Workflow | Trigger | Actions |
|----------|---------|---------|
| `ci.yml` | Push / PR | Detekt lint, unit tests, shadowJar build |
| `cd.yml` | Push to `main`, version tags | Docker build+push, Helm upgrade to staging/production |

Production deployments require a semver tag (`v*.*.*`) and manual approval via GitHub Environments.

## Monitoring

- Flink exposes Prometheus metrics on port `9090` (configurable)
- Key metrics: checkpoint duration, state size, records-per-second, operator backpressure
- Configure `metrics.reporter.prom.class` in `flink-conf.yaml` to enable scraping

## S3 Data Layout

```
s3://<bucket>/ad-events/
├── ssp/
│   └── year=2024/month=03/day=15/hour=14/
│       ├── part-0-0.parquet
│       └── part-1-0.parquet
└── dsp/
    └── year=2024/month=03/day=15/hour=14/
        ├── part-0-0.parquet
        └── part-1-0.parquet
```

Parquet schema fields match the `SSPEvent` and `DSPEvent` data classes respectively.

## Requirements

- JDK 11
- Apache Flink 1.18.1
- Kotlin 1.9.22
- Gradle 8.5
- Kafka 3.6+
- S3-compatible object storage (AWS S3 or MinIO for local dev)
