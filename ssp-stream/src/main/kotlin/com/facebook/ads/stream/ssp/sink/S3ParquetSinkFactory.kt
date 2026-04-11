package com.facebook.ads.stream.ssp.sink

import com.facebook.ads.stream.common.config.StreamConfig
import com.facebook.ads.stream.common.model.SSPEvent
import com.facebook.ads.stream.common.util.TimeUtils
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.flink.connector.file.sink.FileSink
import org.apache.flink.core.fs.Path
import org.apache.flink.formats.parquet.avro.AvroParquetWriters
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy
import java.time.ZoneOffset

/**
 * Factory for creating Flink [FileSink] instances that write SSP events
 * to S3 in Parquet format, partitioned by event date and hour.
 */
object S3ParquetSinkFactory {

    private val SSP_AVRO_SCHEMA: Schema = Schema.Parser().parse(
        """
        {
          "type": "record",
          "name": "SSPEvent",
          "namespace": "com.facebook.ads.stream.ssp",
          "fields": [
            {"name": "event_id",      "type": "string"},
            {"name": "timestamp",     "type": "long",   "logicalType": "timestamp-millis"},
            {"name": "ad_id",         "type": "string"},
            {"name": "campaign_id",   "type": "string"},
            {"name": "creative_id",   "type": "string"},
            {"name": "publisher_id",  "type": "string"},
            {"name": "advertiser_id", "type": "string"},
            {"name": "user_id",       "type": "string"},
            {"name": "action",        "type": "string"},
            {"name": "floor_price",   "type": "double"},
            {"name": "winning_bid",   "type": "double"},
            {"name": "ad_slot",       "type": "string"},
            {"name": "page_url",      "type": "string"},
            {"name": "partition_path","type": ["null", "string"], "default": null},
            {"name": "processing_time","type": ["null", "long"], "default": null}
          ]
        }
        """.trimIndent()
    )

    /**
     * Creates a Flink [FileSink] writing SSP events as Parquet files to [basePath].
     * Files are bucketed by "year=YYYY/month=MM/day=DD/hour=HH" and rolled on checkpoint.
     */
    fun createSSPSink(basePath: String, config: StreamConfig): FileSink<SSPEvent> {
        return FileSink
            .forBulkFormat(
                Path(basePath),
                AvroParquetWriters.forGenericRecord(SSP_AVRO_SCHEMA)
                    .let { writerFactory ->
                        // Wrap to convert SSPEvent → GenericRecord
                        org.apache.flink.api.common.serialization.BulkWriter.Factory<SSPEvent> { outputStream ->
                            val avroWriter = writerFactory.create(outputStream)
                            object : org.apache.flink.api.common.serialization.BulkWriter<SSPEvent> {
                                override fun addElement(element: SSPEvent) {
                                    avroWriter.addElement(toGenericRecord(element))
                                }
                                override fun flush() = avroWriter.flush()
                                override fun finish() = avroWriter.finish()
                            }
                        }
                    }
            )
            .withBucketAssigner(
                DateTimeBucketAssigner<SSPEvent>("'year='yyyy'/month='MM'/day='dd'/hour='HH", ZoneOffset.UTC)
            )
            .withRollingPolicy(OnCheckpointRollingPolicy.build())
            .build()
    }

    private fun toGenericRecord(event: SSPEvent): GenericRecord {
        val record = GenericData.Record(SSP_AVRO_SCHEMA)
        record.put("event_id", event.eventId)
        record.put("timestamp", event.timestamp.toEpochMilli())
        record.put("ad_id", event.adId)
        record.put("campaign_id", event.campaignId)
        record.put("creative_id", event.creativeId)
        record.put("publisher_id", event.publisherId)
        record.put("advertiser_id", event.advertiserId)
        record.put("user_id", event.userId)
        record.put("action", event.action.name)
        record.put("floor_price", event.floorPrice)
        record.put("winning_bid", event.winningBid)
        record.put("ad_slot", event.adSlot)
        record.put("page_url", event.pageUrl)
        record.put("partition_path", event.metadata["partition_path"])
        record.put("processing_time", event.metadata["processing_time"]?.toLongOrNull())
        return record
    }
}
