# ad-event-stream

Apache Flink stream processing service for Facebook ad system events. Consumes SSP and DSP events from Kafka, aggregates metrics in real-time, and persists raw event data to S3 in Parquet format.

## Modules

- **stream-common** - Shared data models, serializers, and utilities
- **ssp-stream** - SSP (Supply-Side Platform) event processing jobs
- **dsp-stream** - DSP (Demand-Side Platform) event processing jobs

## Requirements

- JDK 11+
- Apache Flink 1.18.1
- Kotlin 1.9.22
