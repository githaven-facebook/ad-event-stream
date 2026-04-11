package com.facebook.ads.stream.dsp.job

import com.facebook.ads.stream.common.config.StreamConfig
import com.facebook.ads.stream.common.model.DSPAggregatedMetric
import com.facebook.ads.stream.common.model.DSPEvent
import com.facebook.ads.stream.common.serialization.DSPAggregatedMetricSerializationSchema
import com.facebook.ads.stream.common.serialization.DSPEventDeserializationSchema
import com.facebook.ads.stream.common.serialization.DSPEventSerializationSchema
import com.facebook.ads.stream.dsp.function.DSPAggregateFunction
import com.facebook.ads.stream.dsp.function.DSPEventFilterFunction
import com.facebook.ads.stream.dsp.function.DSPWindowResultFunction
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema
import org.apache.flink.connector.kafka.sink.KafkaSink
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.OutputTag
import org.slf4j.LoggerFactory
import java.time.Duration

/**
 * DSP Aggregation pipeline:
 * Kafka (dsp-events) → filter invalid → key by (campaignId, advertiserId, action)
 *   → tumbling 1-min window → aggregate bid metrics → Kafka (dsp-metrics)
 *   → sliding 5-min window  → aggregate bid metrics → Kafka (dsp-metrics)
 *
 * DSP-specific metrics: bid requests, bid responses, wins, losses, timeouts,
 * win rate, average bid price, average clearing price, total spend.
 */
class DSPAggregationJob(
    private val config: StreamConfig,
    private val env: StreamExecutionEnvironment
) {

    fun build() {
        val source = buildKafkaSource()
        val deadLetterTag = OutputTag<DSPEvent>("dsp-dead-letter")

        val rawStream: DataStream<DSPEvent> = env
            .fromSource(source, buildWatermarkStrategy(), "DSP Kafka Source")
            .uid("dsp-kafka-source")
            .name("DSP Kafka Source")
            .setParallelism(config.sourceParallelism)

        val filterFunction = DSPEventFilterFunction(deadLetterTag)
        val filteredStream = rawStream
            .process(filterFunction)
            .uid("dsp-event-filter")
            .name("DSP Event Filter")

        filteredStream.getSideOutput(deadLetterTag)
            .sinkTo(buildDeadLetterSink())
            .uid("dsp-dead-letter-sink")
            .name("DSP Dead Letter Sink")
            .setParallelism(config.sinkParallelism)

        // Key by campaignId + advertiserId + action for granular bidding metrics
        val keyedStream = filteredStream
            .keyBy { event -> "${event.campaignId}|${event.advertiserId}|${event.action.name}" }

        // Tumbling 1-minute window
        val tumblingAggregated: DataStream<DSPAggregatedMetric> = keyedStream
            .window(TumblingEventTimeWindows.of(Time.milliseconds(config.tumblingWindowSizeMs)))
            .allowedLateness(Time.milliseconds(config.allowedLatenessMs))
            .aggregate(DSPAggregateFunction(), DSPWindowResultFunction(config.tumblingWindowSizeMs))
            .uid("dsp-tumbling-aggregate")
            .name("DSP Tumbling 1-Min Aggregate")

        tumblingAggregated
            .sinkTo(buildMetricsSink())
            .uid("dsp-tumbling-metrics-sink")
            .name("DSP Tumbling Metrics Kafka Sink")
            .setParallelism(config.sinkParallelism)

        // Sliding 5-minute window
        val slidingAggregated: DataStream<DSPAggregatedMetric> = keyedStream
            .window(
                SlidingEventTimeWindows.of(
                    Time.milliseconds(config.slidingWindowSizeMs),
                    Time.milliseconds(config.slidingWindowSlideMs)
                )
            )
            .allowedLateness(Time.milliseconds(config.allowedLatenessMs))
            .aggregate(DSPAggregateFunction(), DSPWindowResultFunction(config.slidingWindowSizeMs))
            .uid("dsp-sliding-aggregate")
            .name("DSP Sliding 5-Min Aggregate")

        slidingAggregated
            .sinkTo(buildMetricsSink())
            .uid("dsp-sliding-metrics-sink")
            .name("DSP Sliding Metrics Kafka Sink")
            .setParallelism(config.sinkParallelism)

        LOG.info("DSP aggregation job pipeline constructed")
    }

    private fun buildKafkaSource(): KafkaSource<DSPEvent> {
        return KafkaSource.builder<DSPEvent>()
            .setBootstrapServers(config.kafkaBootstrapServers)
            .setTopics(config.dspSourceTopic)
            .setGroupId(config.kafkaGroupId)
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(DSPEventDeserializationSchema())
            .setProperties(config.kafkaProperties())
            .build()
    }

    private fun buildMetricsSink(): KafkaSink<DSPAggregatedMetric> {
        return KafkaSink.builder<DSPAggregatedMetric>()
            .setBootstrapServers(config.kafkaBootstrapServers)
            .setRecordSerializer(
                KafkaRecordSerializationSchema.builder<DSPAggregatedMetric>()
                    .setTopic(config.dspAggregatedTopic)
                    .setValueSerializationSchema(DSPAggregatedMetricSerializationSchema())
                    .build()
            )
            .setKafkaProducerConfig(config.kafkaProperties())
            .build()
    }

    private fun buildDeadLetterSink(): KafkaSink<DSPEvent> {
        return KafkaSink.builder<DSPEvent>()
            .setBootstrapServers(config.kafkaBootstrapServers)
            .setRecordSerializer(
                KafkaRecordSerializationSchema.builder<DSPEvent>()
                    .setTopic(config.deadLetterTopic)
                    .setValueSerializationSchema(DSPEventSerializationSchema())
                    .build()
            )
            .setKafkaProducerConfig(config.kafkaProperties())
            .build()
    }

    private fun buildWatermarkStrategy(): WatermarkStrategy<DSPEvent> {
        return WatermarkStrategy
            .forBoundedOutOfOrderness<DSPEvent>(Duration.ofMillis(config.allowedLatenessMs))
            .withTimestampAssigner { event, _ -> event.timestamp.toEpochMilli() }
            .withIdleness(Duration.ofSeconds(30))
    }

    companion object {
        private val LOG = LoggerFactory.getLogger(DSPAggregationJob::class.java)
    }
}
