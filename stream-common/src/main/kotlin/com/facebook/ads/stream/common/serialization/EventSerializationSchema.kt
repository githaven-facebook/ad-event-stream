package com.facebook.ads.stream.common.serialization

import com.facebook.ads.stream.common.model.AggregatedMetric
import com.facebook.ads.stream.common.model.DSPAggregatedMetric
import com.facebook.ads.stream.common.model.DSPEvent
import com.facebook.ads.stream.common.model.SSPAggregatedMetric
import com.facebook.ads.stream.common.model.SSPEvent
import org.apache.flink.api.common.serialization.SerializationSchema
import org.slf4j.LoggerFactory

/**
 * Flink SerializationSchema for writing SSPEvent objects to Kafka as JSON bytes.
 */
class SSPEventSerializationSchema : SerializationSchema<SSPEvent> {

    @Transient
    private var mapper: com.fasterxml.jackson.databind.ObjectMapper? = null

    private fun getMapper() = mapper ?: buildObjectMapper().also { mapper = it }

    override fun serialize(element: SSPEvent): ByteArray =
        getMapper().writeValueAsBytes(element)
}

/**
 * Flink SerializationSchema for writing DSPEvent objects to Kafka as JSON bytes.
 */
class DSPEventSerializationSchema : SerializationSchema<DSPEvent> {

    @Transient
    private var mapper: com.fasterxml.jackson.databind.ObjectMapper? = null

    private fun getMapper() = mapper ?: buildObjectMapper().also { mapper = it }

    override fun serialize(element: DSPEvent): ByteArray =
        getMapper().writeValueAsBytes(element)
}

/**
 * Flink SerializationSchema for writing AggregatedMetric objects to Kafka as JSON bytes.
 */
class AggregatedMetricSerializationSchema : SerializationSchema<AggregatedMetric> {

    @Transient
    private var mapper: com.fasterxml.jackson.databind.ObjectMapper? = null

    private fun getMapper() = mapper ?: buildObjectMapper().also { mapper = it }

    override fun serialize(element: AggregatedMetric): ByteArray =
        getMapper().writeValueAsBytes(element)
}

/**
 * Flink SerializationSchema for SSP aggregated metrics.
 */
class SSPAggregatedMetricSerializationSchema : SerializationSchema<SSPAggregatedMetric> {

    @Transient
    private var mapper: com.fasterxml.jackson.databind.ObjectMapper? = null

    private fun getMapper() = mapper ?: buildObjectMapper().also { mapper = it }

    override fun serialize(element: SSPAggregatedMetric): ByteArray {
        return try {
            getMapper().writeValueAsBytes(element)
        } catch (e: Exception) {
            LOG.error("Failed to serialize SSPAggregatedMetric for campaign=${element.base.campaignId}: ${e.message}", e)
            ByteArray(0)
        }
    }

    companion object {
        private val LOG = LoggerFactory.getLogger(SSPAggregatedMetricSerializationSchema::class.java)
    }
}

/**
 * Flink SerializationSchema for DSP aggregated metrics.
 */
class DSPAggregatedMetricSerializationSchema : SerializationSchema<DSPAggregatedMetric> {

    @Transient
    private var mapper: com.fasterxml.jackson.databind.ObjectMapper? = null

    private fun getMapper() = mapper ?: buildObjectMapper().also { mapper = it }

    override fun serialize(element: DSPAggregatedMetric): ByteArray {
        return try {
            getMapper().writeValueAsBytes(element)
        } catch (e: Exception) {
            LOG.error("Failed to serialize DSPAggregatedMetric for campaign=${element.base.campaignId}: ${e.message}", e)
            ByteArray(0)
        }
    }

    companion object {
        private val LOG = LoggerFactory.getLogger(DSPAggregatedMetricSerializationSchema::class.java)
    }
}
