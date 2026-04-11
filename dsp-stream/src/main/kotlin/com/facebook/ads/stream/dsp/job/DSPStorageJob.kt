package com.facebook.ads.stream.dsp.job

import com.facebook.ads.stream.common.config.StreamConfig
import com.facebook.ads.stream.common.model.DSPEvent
import com.facebook.ads.stream.common.serialization.DSPEventDeserializationSchema
import com.facebook.ads.stream.common.util.TimeUtils
import com.facebook.ads.stream.dsp.sink.S3ParquetSinkFactory
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.slf4j.LoggerFactory
import java.time.Duration

/**
 * DSP Storage pipeline:
 * Kafka (dsp-events) → transform/enrich → FileSink (S3, Parquet)
 *
 * Partitioned by: event_type=DSP/year=YYYY/month=MM/day=DD/hour=HH
 * Format: Parquet with Snappy compression
 *
 * DSP events are partitioned differently from SSP to allow independent
 * query patterns over bidding vs. publisher inventory data.
 */
class DSPStorageJob(
    private val config: StreamConfig,
    private val env: StreamExecutionEnvironment
) {

    fun build() {
        val source = buildKafkaSource()

        val rawStream = env
            .fromSource(source, buildWatermarkStrategy(), "DSP Storage Kafka Source")
            .uid("dsp-storage-kafka-source")
            .name("DSP Storage Kafka Source")
            .setParallelism(config.sourceParallelism)

        val enrichedStream = rawStream
            .filter { event -> event.eventId.isNotBlank() }
            .uid("dsp-storage-filter")
            .name("DSP Storage Pre-filter")
            .map { event ->
                event.copy(
                    metadata = event.metadata + mapOf(
                        "partition_path" to TimeUtils.toPartitionPath(event.timestamp, "DSP"),
                        "processing_time" to System.currentTimeMillis().toString(),
                        "advertiser_id" to event.advertiserId
                    )
                )
            }
            .uid("dsp-storage-enrich")
            .name("DSP Storage Enrichment")

        val s3Path = "s3://${config.s3OutputBucket}/${config.s3OutputPrefix}/dsp"
        val sink = S3ParquetSinkFactory.createDSPSink(s3Path, config)

        enrichedStream
            .sinkTo(sink)
            .uid("dsp-s3-parquet-sink")
            .name("DSP S3 Parquet Sink")
            .setParallelism(config.sinkParallelism)

        LOG.info("DSP storage job pipeline constructed, output path: $s3Path")
    }

    private fun buildKafkaSource(): KafkaSource<DSPEvent> {
        return KafkaSource.builder<DSPEvent>()
            .setBootstrapServers(config.kafkaBootstrapServers)
            .setTopics(config.dspSourceTopic)
            .setGroupId("${config.kafkaGroupId}-storage")
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(DSPEventDeserializationSchema())
            .setProperties(config.kafkaProperties())
            .build()
    }

    private fun buildWatermarkStrategy(): WatermarkStrategy<DSPEvent> {
        return WatermarkStrategy
            .forBoundedOutOfOrderness<DSPEvent>(Duration.ofSeconds(60))
            .withTimestampAssigner { event, _ -> event.timestamp.toEpochMilli() }
            .withIdleness(Duration.ofSeconds(60))
    }

    companion object {
        private val LOG = LoggerFactory.getLogger(DSPStorageJob::class.java)
    }
}
