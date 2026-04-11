package com.facebook.ads.stream.ssp.job

import com.facebook.ads.stream.common.config.StreamConfig
import com.facebook.ads.stream.common.model.SSPEvent
import com.facebook.ads.stream.common.model.SSPAggregatedMetric
import com.facebook.ads.stream.common.serialization.SSPEventDeserializationSchema
import com.facebook.ads.stream.common.serialization.SSPAggregatedMetricSerializationSchema
import com.facebook.ads.stream.ssp.function.SSPAggregateFunction
import com.facebook.ads.stream.ssp.function.SSPEventFilterFunction
import com.facebook.ads.stream.ssp.function.SSPWindowResultFunction
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.connector.kafka.sink.KafkaSink
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.OutputTag
import org.slf4j.LoggerFactory
import java.time.Duration

/**
 * SSP Aggregation pipeline:
 * Kafka (ssp-events) → filter invalid → key by (campaignId, action)
 *   → tumbling 1-min window → aggregate → Kafka (ssp-metrics)
 *   → sliding 5-min window  → aggregate → Kafka (ssp-metrics)
 * Invalid events are routed to dead-letter Kafka topic via side output.
 */
class SSPAggregationJob(
    private val config: StreamConfig,
    private val env: StreamExecutionEnvironment
) {

    fun build() {
        val source = buildKafkaSource()
        val deadLetterTag = OutputTag<SSPEvent>("ssp-dead-letter")

        val rawStream: DataStream<SSPEvent> = env
            .fromSource(source, buildWatermarkStrategy(), "SSP Kafka Source")
            .uid("ssp-kafka-source")
            .name("SSP Kafka Source")
            .setParallelism(config.sourceParallelism)

        val filterFunction = SSPEventFilterFunction(deadLetterTag)
        val filteredStream = rawStream
            .process(filterFunction)
            .uid("ssp-event-filter")
            .name("SSP Event Filter")

        // Route dead letters to DLQ topic
        filteredStream.getSideOutput(deadLetterTag)
            .sinkTo(buildDeadLetterSink())
            .uid("ssp-dead-letter-sink")
            .name("SSP Dead Letter Sink")
            .setParallelism(config.sinkParallelism)

        val keyedStream = filteredStream
            .keyBy { event -> "${event.campaignId}|${event.action.name}" }

        // Tumbling 1-minute window aggregation
        val tumblingAggregated: DataStream<SSPAggregatedMetric> = keyedStream
            .window(TumblingEventTimeWindows.of(Time.milliseconds(config.tumblingWindowSizeMs)))
            .allowedLateness(Time.milliseconds(config.allowedLatenessMs))
            .aggregate(SSPAggregateFunction(), SSPWindowResultFunction(config.tumblingWindowSizeMs))
            .uid("ssp-tumbling-aggregate")
            .name("SSP Tumbling 1-Min Aggregate")

        tumblingAggregated
            .sinkTo(buildMetricsSink())
            .uid("ssp-tumbling-metrics-sink")
            .name("SSP Tumbling Metrics Kafka Sink")
            .setParallelism(config.sinkParallelism)

        // Sliding 5-minute window aggregation
        val slidingAggregated: DataStream<SSPAggregatedMetric> = keyedStream
            .window(SlidingEventTimeWindows.of(
                Time.milliseconds(config.slidingWindowSizeMs),
                Time.milliseconds(config.slidingWindowSlideMs)
            ))
            .allowedLateness(Time.milliseconds(config.allowedLatenessMs))
            .aggregate(SSPAggregateFunction(), SSPWindowResultFunction(config.slidingWindowSizeMs))
            .uid("ssp-sliding-aggregate")
            .name("SSP Sliding 5-Min Aggregate")

        slidingAggregated
            .sinkTo(buildMetricsSink())
            .uid("ssp-sliding-metrics-sink")
            .name("SSP Sliding Metrics Kafka Sink")
            .setParallelism(config.sinkParallelism)

        LOG.info("SSP aggregation job pipeline constructed")
    }

    private fun buildKafkaSource(): KafkaSource<SSPEvent> {
        return KafkaSource.builder<SSPEvent>()
            .setBootstrapServers(config.kafkaBootstrapServers)
            .setTopics(config.sspSourceTopic)
            .setGroupId(config.kafkaGroupId)
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(SSPEventDeserializationSchema())
            .setProperties(config.kafkaProperties())
            .build()
    }

    private fun buildMetricsSink(): KafkaSink<SSPAggregatedMetric> {
        return KafkaSink.builder<SSPAggregatedMetric>()
            .setBootstrapServers(config.kafkaBootstrapServers)
            .setRecordSerializer(
                KafkaRecordSerializationSchema.builder<SSPAggregatedMetric>()
                    .setTopic(config.sspAggregatedTopic)
                    .setValueSerializationSchema(SSPAggregatedMetricSerializationSchema())
                    .build()
            )
            .setKafkaProducerConfig(config.kafkaProperties())
            .build()
    }

    private fun buildDeadLetterSink(): KafkaSink<SSPEvent> {
        return KafkaSink.builder<SSPEvent>()
            .setBootstrapServers(config.kafkaBootstrapServers)
            .setRecordSerializer(
                KafkaRecordSerializationSchema.builder<SSPEvent>()
                    .setTopic(config.deadLetterTopic)
                    .setValueSerializationSchema(com.facebook.ads.stream.common.serialization.SSPEventSerializationSchema())
                    .build()
            )
            .setKafkaProducerConfig(config.kafkaProperties())
            .build()
    }

    private fun buildWatermarkStrategy(): WatermarkStrategy<SSPEvent> {
        return WatermarkStrategy
            .forBoundedOutOfOrderness<SSPEvent>(Duration.ofMillis(config.allowedLatenessMs))
            .withTimestampAssigner { event, _ -> event.timestamp.toEpochMilli() }
            .withIdleness(Duration.ofSeconds(30))
    }

    companion object {
        private val LOG = LoggerFactory.getLogger(SSPAggregationJob::class.java)
    }
}
