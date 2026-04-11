package com.facebook.ads.stream.ssp.job

import com.facebook.ads.stream.common.config.StreamConfig
import com.facebook.ads.stream.common.model.SSPEvent
import com.facebook.ads.stream.common.serialization.SSPEventDeserializationSchema
import com.facebook.ads.stream.common.util.TimeUtils
import com.facebook.ads.stream.ssp.sink.S3ParquetSinkFactory
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.slf4j.LoggerFactory
import java.time.Duration

/**
 * SSP Storage pipeline:
 * Kafka (ssp-events) → transform/enrich → StreamingFileSink (S3, Parquet)
 *
 * Partitioned by: event_type=SSP/year=YYYY/month=MM/day=DD/hour=HH
 * Format: Parquet with Snappy compression
 */
class SSPStorageJob(
    private val config: StreamConfig,
    private val env: StreamExecutionEnvironment
) {

    fun build() {
        val source = buildKafkaSource()

        val rawStream = env
            .fromSource(source, buildWatermarkStrategy(), "SSP Storage Kafka Source")
            .uid("ssp-storage-kafka-source")
            .name("SSP Storage Kafka Source")
            .setParallelism(config.sourceParallelism)

        val enrichedStream = rawStream
            .filter { event -> event.eventId.isNotBlank() }
            .uid("ssp-storage-filter")
            .name("SSP Storage Pre-filter")
            .map { event ->
                // Enrich with partition metadata in the metadata map
                event.copy(
                    metadata = event.metadata + mapOf(
                        "partition_path" to TimeUtils.toPartitionPath(event.timestamp, "SSP"),
                        "processing_time" to System.currentTimeMillis().toString()
                    )
                )
            }
            .uid("ssp-storage-enrich")
            .name("SSP Storage Enrichment")

        val s3Path = "s3://${config.s3OutputBucket}/${config.s3OutputPrefix}/ssp"
        val sink = S3ParquetSinkFactory.createSSPSink(s3Path, config)

        enrichedStream
            .sinkTo(sink)
            .uid("ssp-s3-parquet-sink")
            .name("SSP S3 Parquet Sink")
            .setParallelism(config.sinkParallelism)

        LOG.info("SSP storage job pipeline constructed, output path: $s3Path")
    }

    private fun buildKafkaSource(): KafkaSource<SSPEvent> {
        return KafkaSource.builder<SSPEvent>()
            .setBootstrapServers(config.kafkaBootstrapServers)
            .setTopics(config.sspSourceTopic)
            .setGroupId("${config.kafkaGroupId}-storage")
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(SSPEventDeserializationSchema())
            .setProperties(config.kafkaProperties())
            .build()
    }

    private fun buildWatermarkStrategy(): WatermarkStrategy<SSPEvent> {
        return WatermarkStrategy
            .forBoundedOutOfOrderness<SSPEvent>(Duration.ofSeconds(60))
            .withTimestampAssigner { event, _ -> event.timestamp.toEpochMilli() }
            .withIdleness(Duration.ofSeconds(60))
    }

    companion object {
        private val LOG = LoggerFactory.getLogger(SSPStorageJob::class.java)
    }
}
