package com.facebook.ads.stream.common.util

import java.time.Instant
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.time.temporal.ChronoUnit

/**
 * Utility functions for time window calculations used in Flink stream processing.
 */
object TimeUtils {

    /**
     * Truncates an [Instant] to the start of the minute.
     */
    fun truncateToMinute(instant: Instant): Instant =
        instant.truncatedTo(ChronoUnit.MINUTES)

    /**
     * Truncates an [Instant] to the start of the hour.
     */
    fun truncateToHour(instant: Instant): Instant =
        instant.truncatedTo(ChronoUnit.HOURS)

    /**
     * Truncates an [Instant] to the start of the day (UTC).
     */
    fun truncateToDay(instant: Instant): Instant =
        instant.truncatedTo(ChronoUnit.DAYS)

    /**
     * Truncates an [Instant] to the nearest N-minute boundary.
     * E.g., truncateToNMinutes(instant, 5) snaps 14:03 → 14:00, 14:07 → 14:05.
     */
    fun truncateToNMinutes(instant: Instant, n: Int): Instant {
        require(n in 1..60) { "n must be between 1 and 60, got $n" }
        val epochSeconds = instant.epochSecond
        val windowSeconds = n * 60L
        val truncated = (epochSeconds / windowSeconds) * windowSeconds
        return Instant.ofEpochSecond(truncated)
    }

    /**
     * Returns a partition path in the form "year=YYYY/month=MM/day=DD/hour=HH"
     * suitable for S3 Parquet partitioning.
     */
    fun toPartitionPath(instant: Instant): String {
        val dt = ZonedDateTime.ofInstant(instant, ZoneOffset.UTC)
        return "year=%04d/month=%02d/day=%02d/hour=%02d".format(
            dt.year, dt.monthValue, dt.dayOfMonth, dt.hour
        )
    }

    /**
     * Returns a partition path with an additional sub-partition for event type.
     */
    fun toPartitionPath(instant: Instant, eventType: String): String =
        "event_type=$eventType/${toPartitionPath(instant)}"

    /**
     * Converts epoch milliseconds to [Instant].
     */
    fun fromEpochMillis(epochMillis: Long): Instant =
        Instant.ofEpochMilli(epochMillis)

    /**
     * Returns the epoch milliseconds for the start of the current minute window.
     */
    fun currentMinuteWindowStart(): Long =
        truncateToMinute(Instant.now()).toEpochMilli()

    /**
     * Returns the epoch milliseconds for the start of the current hour window.
     */
    fun currentHourWindowStart(): Long =
        truncateToHour(Instant.now()).toEpochMilli()

    /**
     * Checks whether the given timestamp falls within an acceptable processing window
     * relative to [referenceTime]. Used to detect late-arriving events.
     */
    fun isWithinProcessingWindow(
        eventTimestamp: Instant,
        referenceTime: Instant,
        maxLatenessSeconds: Long = 3600L
    ): Boolean {
        val diffSeconds = referenceTime.epochSecond - eventTimestamp.epochSecond
        return diffSeconds in 0..maxLatenessSeconds
    }
}
