package com.facebook.ads.stream.common.config

import org.apache.flink.api.java.utils.ParameterTool

/**
 * Centralised configuration holder for Flink stream jobs.
 * Loaded from Flink [ParameterTool] (CLI args, properties files, system properties).
 */
data class StreamConfig(
    // Kafka
    val kafkaBootstrapServers: String,
    val kafkaGroupId: String,
    val kafkaAutoOffsetReset: String,
    val kafkaEnableAutoCommit: Boolean,
    val kafkaSslEnabled: Boolean,
    val kafkaSaslMechanism: String,

    // Source topics
    val sspSourceTopic: String,
    val dspSourceTopic: String,

    // Sink topics
    val sspAggregatedTopic: String,
    val dspAggregatedTopic: String,
    val deadLetterTopic: String,

    // S3 / Storage
    val s3OutputBucket: String,
    val s3OutputPrefix: String,
    val s3Region: String,
    val parquetRowGroupSize: Int,
    val parquetPageSize: Int,

    // Flink checkpointing
    val checkpointingEnabled: Boolean,
    val checkpointIntervalMs: Long,
    val checkpointTimeoutMs: Long,
    val checkpointMinPauseBetweenMs: Long,
    val maxConcurrentCheckpoints: Int,
    val checkpointStoragePath: String,

    // Window configuration
    val tumblingWindowSizeMs: Long,
    val slidingWindowSizeMs: Long,
    val slidingWindowSlideMs: Long,
    val allowedLatenessMs: Long,

    // Parallelism
    val defaultParallelism: Int,
    val sourceParallelism: Int,
    val sinkParallelism: Int,

    // Monitoring
    val metricsEnabled: Boolean,
    val metricsHost: String,
    val metricsPort: Int
) {
    companion object {
        private const val DEFAULT_TUMBLING_WINDOW_MS = 60_000L       // 1 minute
        private const val DEFAULT_SLIDING_WINDOW_MS = 300_000L       // 5 minutes
        private const val DEFAULT_SLIDING_SLIDE_MS = 60_000L         // 1 minute
        private const val DEFAULT_ALLOWED_LATENESS_MS = 30_000L      // 30 seconds
        private const val DEFAULT_CHECKPOINT_INTERVAL_MS = 60_000L   // 1 minute
        private const val DEFAULT_CHECKPOINT_TIMEOUT_MS = 120_000L   // 2 minutes
        private const val DEFAULT_ROW_GROUP_SIZE = 134_217_728        // 128 MB
        private const val DEFAULT_PAGE_SIZE = 1_048_576               // 1 MB

        fun fromParameterTool(params: ParameterTool): StreamConfig = StreamConfig(
            // Kafka
            kafkaBootstrapServers = params.getRequired("kafka.bootstrap.servers"),
            kafkaGroupId = params.get("kafka.group.id", "ad-event-stream"),
            kafkaAutoOffsetReset = params.get("kafka.auto.offset.reset", "latest"),
            kafkaEnableAutoCommit = params.getBoolean("kafka.enable.auto.commit", false),
            kafkaSslEnabled = params.getBoolean("kafka.ssl.enabled", false),
            kafkaSaslMechanism = params.get("kafka.sasl.mechanism", "PLAIN"),

            // Source topics
            sspSourceTopic = params.get("kafka.topic.ssp.source", "ad-events-ssp"),
            dspSourceTopic = params.get("kafka.topic.dsp.source", "ad-events-dsp"),

            // Sink topics
            sspAggregatedTopic = params.get("kafka.topic.ssp.aggregated", "ad-metrics-ssp"),
            dspAggregatedTopic = params.get("kafka.topic.dsp.aggregated", "ad-metrics-dsp"),
            deadLetterTopic = params.get("kafka.topic.dead-letter", "ad-events-dlq"),

            // S3
            s3OutputBucket = params.getRequired("s3.output.bucket"),
            s3OutputPrefix = params.get("s3.output.prefix", "ad-events"),
            s3Region = params.get("s3.region", "us-east-1"),
            parquetRowGroupSize = params.getInt("parquet.row-group-size", DEFAULT_ROW_GROUP_SIZE),
            parquetPageSize = params.getInt("parquet.page-size", DEFAULT_PAGE_SIZE),

            // Checkpointing
            checkpointingEnabled = params.getBoolean("flink.checkpointing.enabled", true),
            checkpointIntervalMs = params.getLong("flink.checkpoint.interval-ms", DEFAULT_CHECKPOINT_INTERVAL_MS),
            checkpointTimeoutMs = params.getLong("flink.checkpoint.timeout-ms", DEFAULT_CHECKPOINT_TIMEOUT_MS),
            checkpointMinPauseBetweenMs = params.getLong("flink.checkpoint.min-pause-ms", 30_000L),
            maxConcurrentCheckpoints = params.getInt("flink.checkpoint.max-concurrent", 1),
            checkpointStoragePath = params.get("flink.checkpoint.storage-path", "s3://checkpoints/ad-event-stream"),

            // Windows
            tumblingWindowSizeMs = params.getLong("window.tumbling.size-ms", DEFAULT_TUMBLING_WINDOW_MS),
            slidingWindowSizeMs = params.getLong("window.sliding.size-ms", DEFAULT_SLIDING_WINDOW_MS),
            slidingWindowSlideMs = params.getLong("window.sliding.slide-ms", DEFAULT_SLIDING_SLIDE_MS),
            allowedLatenessMs = params.getLong("window.allowed-lateness-ms", DEFAULT_ALLOWED_LATENESS_MS),

            // Parallelism
            defaultParallelism = params.getInt("flink.parallelism.default", 4),
            sourceParallelism = params.getInt("flink.parallelism.source", 2),
            sinkParallelism = params.getInt("flink.parallelism.sink", 2),

            // Monitoring
            metricsEnabled = params.getBoolean("metrics.enabled", true),
            metricsHost = params.get("metrics.host", "localhost"),
            metricsPort = params.getInt("metrics.port", 9090)
        )
    }

    fun kafkaProperties(): java.util.Properties = java.util.Properties().apply {
        setProperty("bootstrap.servers", kafkaBootstrapServers)
        setProperty("group.id", kafkaGroupId)
        setProperty("auto.offset.reset", kafkaAutoOffsetReset)
        setProperty("enable.auto.commit", kafkaEnableAutoCommit.toString())
        if (kafkaSslEnabled) {
            setProperty("security.protocol", "SASL_SSL")
            setProperty("sasl.mechanism", kafkaSaslMechanism)
        }
    }
}
