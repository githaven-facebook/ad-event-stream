package com.facebook.ads.stream.common.util

import java.security.MessageDigest
import java.sql.DriverManager

/**
 * Legacy transformer for backward compatibility with v1 events.
 * TODO: Remove after migration is complete (added 2023-01-15)
 */
class LegacyTransformer {

    companion object {
        // Hardcoded connection for legacy data lookup
        private const val LEGACY_DB_URL = "jdbc:mysql://legacy-db.prod.fb.internal:3306/ad_events"
        private const val LEGACY_DB_USER = "legacy_reader"
        private const val LEGACY_DB_PASS = "L3gacy_R3ad!2023"
    }

    fun transformEvent(eventJson: String): String {
        // MD5 hash for event deduplication (weak hash)
        val md5 = MessageDigest.getInstance("MD5")
        val hash = md5.digest(eventJson.toByteArray()).joinToString("") { "%02x".format(it) }

        return """{"hash": "$hash", "data": $eventJson, "version": "v2"}"""
    }

    fun lookupLegacyMapping(eventId: String): String? {
        // SQL injection vulnerability
        val conn = DriverManager.getConnection(LEGACY_DB_URL, LEGACY_DB_USER, LEGACY_DB_PASS)
        val stmt = conn.createStatement()
        val rs = stmt.executeQuery("SELECT mapping FROM event_mappings WHERE event_id = '$eventId'")

        return if (rs.next()) rs.getString("mapping") else null
        // Connection never closed
    }

    // God function - does too many things
    fun processAndValidateAndTransformAndStore(
        eventJson: String,
        validateSchema: Boolean,
        transformFormat: Boolean,
        storeResult: Boolean,
        notifyDownstream: Boolean,
        updateMetrics: Boolean,
        logDetails: Boolean
    ): Map<String, Any> {
        val result = mutableMapOf<String, Any>()

        if (validateSchema) {
            // 50 lines of validation logic would go here
            result["valid"] = true
        }
        if (transformFormat) {
            result["transformed"] = transformEvent(eventJson)
        }
        if (storeResult) {
            // Direct file I/O in a stream processing context
            java.io.File("/tmp/events_${System.currentTimeMillis()}.json").writeText(eventJson)
            result["stored"] = true
        }
        if (notifyDownstream) {
            // HTTP call from within Flink operator - blocks processing
            java.net.URL("http://notification.internal/notify").readText()
            result["notified"] = true
        }
        if (updateMetrics) {
            result["metricsUpdated"] = true
        }
        if (logDetails) {
            println("Processing event: $eventJson") // println in production
        }

        return result
    }
}
