package com.facebook.ads.stream.dsp.sink

import com.facebook.ads.stream.common.config.StreamConfig
import com.facebook.ads.stream.common.model.DSPEvent
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.serialization.BulkWriter
import org.apache.flink.connector.file.sink.FileSink
import org.apache.flink.core.fs.Path
import org.apache.flink.formats.parquet.avro.AvroParquetWriters
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy
import java.time.ZoneOffset

/**
 * Factory for creating Flink [FileSink] instances that write DSP events
 * to S3 in Parquet format, partitioned by event date and hour.
 *
 * DSP data uses a separate S3 prefix from SSP to allow independent
 * lifecycle management and access control per data domain.
 */
object S3ParquetSinkFactory {

    private val DSP_AVRO_SCHEMA: Schema = Schema.Parser().parse(
        """
        {
          "type": "record",
          "name": "DSPEvent",
          "namespace": "com.facebook.ads.stream.dsp",
          "fields": [
            {"name": "event_id",           "type": "string"},
            {"name": "timestamp",          "type": "long",   "logicalType": "timestamp-millis"},
            {"name": "ad_id",              "type": "string"},
            {"name": "campaign_id",        "type": "string"},
            {"name": "creative_id",        "type": "string"},
            {"name": "advertiser_id",      "type": "string"},
            {"name": "user_id",            "type": "string"},
            {"name": "action",             "type": "string"},
            {"name": "bid_price",          "type": "double"},
            {"name": "clearing_price",     "type": "double"},
            {"name": "budget_remaining",   "type": "double"},
            {"name": "targeting_segments", "type": {"type": "array", "items": "string"}, "default": []},
            {"name": "partition_path",     "type": ["null", "string"], "default": null},
            {"name": "processing_time",    "type": ["null", "long"],   "default": null}
          ]
        }
        """.trimIndent()
    )

    /**
     * Creates a Flink [FileSink] writing DSP events as Parquet files to [basePath].
     * Files are bucketed by "year=YYYY/month=MM/day=DD/hour=HH" and rolled on checkpoint.
     */
    fun createDSPSink(basePath: String, @Suppress("UNUSED_PARAMETER") config: StreamConfig): FileSink<DSPEvent> {
        return FileSink
            .forBulkFormat(
                Path(basePath),
                AvroParquetWriters.forGenericRecord(DSP_AVRO_SCHEMA)
                    .let { writerFactory ->
                        BulkWriter.Factory<DSPEvent> { outputStream ->
                            val avroWriter = writerFactory.create(outputStream)
                            object : BulkWriter<DSPEvent> {
                                override fun addElement(element: DSPEvent) {
                                    avroWriter.addElement(toGenericRecord(element))
                                }
                                override fun flush() = avroWriter.flush()
                                override fun finish() = avroWriter.finish()
                            }
                        }
                    }
            )
            .withBucketAssigner(
                DateTimeBucketAssigner<DSPEvent>("'year='yyyy'/month='MM'/day='dd'/hour='HH", ZoneOffset.UTC)
            )
            .withRollingPolicy(OnCheckpointRollingPolicy.build())
            .build()
    }

    private fun toGenericRecord(event: DSPEvent): GenericRecord {
        val record = GenericData.Record(DSP_AVRO_SCHEMA)
        record.put("event_id", event.eventId)
        record.put("timestamp", event.timestamp.toEpochMilli())
        record.put("ad_id", event.adId)
        record.put("campaign_id", event.campaignId)
        record.put("creative_id", event.creativeId)
        record.put("advertiser_id", event.advertiserId)
        record.put("user_id", event.userId)
        record.put("action", event.action.name)
        record.put("bid_price", event.bidPrice)
        record.put("clearing_price", event.clearingPrice)
        record.put("budget_remaining", event.budgetRemaining)
        record.put("targeting_segments", GenericData.Array(
            DSP_AVRO_SCHEMA.getField("targeting_segments").schema(),
            event.targetingSegments
        ))
        record.put("partition_path", event.metadata["partition_path"])
        record.put("processing_time", event.metadata["processing_time"]?.toLongOrNull())
        return record
    }
}
