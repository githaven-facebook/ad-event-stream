package com.facebook.ads.stream.common.serialization

import com.facebook.ads.stream.common.model.AdEvent
import com.facebook.ads.stream.common.model.DSPEvent
import com.facebook.ads.stream.common.model.SSPEvent
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.exc.MismatchedInputException
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinFeature
import com.fasterxml.jackson.module.kotlin.KotlinModule
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.slf4j.LoggerFactory

/**
 * Flink DeserializationSchema for SSP events consumed from Kafka.
 */
class SSPEventDeserializationSchema : DeserializationSchema<SSPEvent> {

    @Transient
    private var mapper: ObjectMapper? = null

    private fun getMapper(): ObjectMapper {
        if (mapper == null) {
            mapper = buildObjectMapper()
        }
        return mapper!!
    }

    override fun deserialize(message: ByteArray): SSPEvent? {
        return try {
            getMapper().readValue(message, SSPEvent::class.java)
        } catch (e: MismatchedInputException) {
            LOG.warn("Failed to deserialize SSP event - mismatched input: ${e.message}")
            null
        } catch (e: Exception) {
            LOG.error("Unexpected error deserializing SSP event: ${e.message}", e)
            null
        }
    }

    override fun isEndOfStream(nextElement: SSPEvent?): Boolean = false

    override fun getProducedType(): TypeInformation<SSPEvent> =
        TypeInformation.of(SSPEvent::class.java)

    companion object {
        private val LOG = LoggerFactory.getLogger(SSPEventDeserializationSchema::class.java)
    }
}

/**
 * Flink DeserializationSchema for DSP events consumed from Kafka.
 */
class DSPEventDeserializationSchema : DeserializationSchema<DSPEvent> {

    @Transient
    private var mapper: ObjectMapper? = null

    private fun getMapper(): ObjectMapper {
        if (mapper == null) {
            mapper = buildObjectMapper()
        }
        return mapper!!
    }

    override fun deserialize(message: ByteArray): DSPEvent? {
        return try {
            getMapper().readValue(message, DSPEvent::class.java)
        } catch (e: MismatchedInputException) {
            LOG.warn("Failed to deserialize DSP event - mismatched input: ${e.message}")
            null
        } catch (e: Exception) {
            LOG.error("Unexpected error deserializing DSP event: ${e.message}", e)
            null
        }
    }

    override fun isEndOfStream(nextElement: DSPEvent?): Boolean = false

    override fun getProducedType(): TypeInformation<DSPEvent> =
        TypeInformation.of(DSPEvent::class.java)

    companion object {
        private val LOG = LoggerFactory.getLogger(DSPEventDeserializationSchema::class.java)
    }
}

/**
 * Generic AdEvent deserialization schema that inspects a type discriminator field.
 * Expects JSON payloads to contain an "event_type" field with value "SSP" or "DSP".
 */
class AdEventDeserializationSchema : DeserializationSchema<AdEvent> {

    @Transient
    private var mapper: ObjectMapper? = null

    private fun getMapper(): ObjectMapper {
        if (mapper == null) {
            mapper = buildObjectMapper()
        }
        return mapper!!
    }

    override fun deserialize(message: ByteArray): AdEvent? {
        return try {
            val node = getMapper().readTree(message)
            when (val eventType = node.get("event_type")?.asText()?.uppercase()) {
                "SSP" -> getMapper().treeToValue(node, SSPEvent::class.java)
                "DSP" -> getMapper().treeToValue(node, DSPEvent::class.java)
                else -> {
                    LOG.warn("Unknown event_type '$eventType' in payload, skipping")
                    null
                }
            }
        } catch (e: Exception) {
            LOG.error("Failed to deserialize AdEvent: ${e.message}", e)
            null
        }
    }

    override fun isEndOfStream(nextElement: AdEvent?): Boolean = false

    override fun getProducedType(): TypeInformation<AdEvent> =
        TypeInformation.of(AdEvent::class.java)

    companion object {
        private val LOG = LoggerFactory.getLogger(AdEventDeserializationSchema::class.java)
    }
}

internal fun buildObjectMapper(): ObjectMapper = ObjectMapper().apply {
    registerModule(JavaTimeModule())
    registerModule(
        KotlinModule.Builder()
            .withReflectionCacheSize(512)
            .configure(KotlinFeature.NullToEmptyCollection, false)
            .configure(KotlinFeature.NullToEmptyMap, false)
            .configure(KotlinFeature.NullIsSameAsDefault, false)
            .configure(KotlinFeature.SingletonSupport, false)
            .configure(KotlinFeature.StrictNullChecks, false)
            .build()
    )
    configure(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    configure(com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
}
