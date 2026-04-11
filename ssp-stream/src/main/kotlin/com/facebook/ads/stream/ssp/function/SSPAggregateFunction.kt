package com.facebook.ads.stream.ssp.function

import com.facebook.ads.stream.common.model.AggregatedMetric
import com.facebook.ads.stream.common.model.EventAction
import com.facebook.ads.stream.common.model.MetricAccumulator
import com.facebook.ads.stream.common.model.MetricWindow
import com.facebook.ads.stream.common.model.SSPAggregatedMetric
import com.facebook.ads.stream.common.model.SSPEvent
import com.facebook.ads.stream.common.model.WindowType
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.functions.windowing.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import java.time.Instant

/**
 * Flink [AggregateFunction] that incrementally accumulates SSP event metrics
 * within a window. Tracks impressions, clicks, conversions, revenue, and unique users.
 */
class SSPAggregateFunction : AggregateFunction<SSPEvent, SSPAccumulator, SSPAccumulator> {

    override fun createAccumulator(): SSPAccumulator = SSPAccumulator()

    override fun add(event: SSPEvent, accumulator: SSPAccumulator): SSPAccumulator {
        accumulator.base.eventCount++
        accumulator.base.userIds.add(event.userId)

        when (event.action) {
            EventAction.IMPRESSION -> {
                accumulator.base.impressions++
                accumulator.base.totalRevenue += event.winningBid
            }
            EventAction.CLICK -> accumulator.base.clicks++
            EventAction.CONVERSION -> accumulator.base.conversions++
            EventAction.VIEW, EventAction.AD_VIEWABLE -> accumulator.base.views++
            EventAction.AD_REQUEST -> accumulator.adRequests++
            EventAction.AD_RESPONSE, EventAction.AD_RENDER -> accumulator.adResponses++
            else -> { /* other SSP actions counted in eventCount */ }
        }

        accumulator.totalFloorPrice += event.floorPrice
        accumulator.totalWinningBid += event.winningBid
        accumulator.priceCount++

        // Track last seen campaign/advertiser/publisher for the window result
        if (accumulator.campaignId.isEmpty()) {
            accumulator.campaignId = event.campaignId
            accumulator.advertiserId = event.advertiserId
            accumulator.publisherId = event.publisherId
            accumulator.action = event.action
        }

        return accumulator
    }

    override fun getResult(accumulator: SSPAccumulator): SSPAccumulator = accumulator

    override fun merge(a: SSPAccumulator, b: SSPAccumulator): SSPAccumulator {
        val merged = SSPAccumulator()
        merged.base = a.base.merge(b.base)
        merged.adRequests = a.adRequests + b.adRequests
        merged.adResponses = a.adResponses + b.adResponses
        merged.totalFloorPrice = a.totalFloorPrice + b.totalFloorPrice
        merged.totalWinningBid = a.totalWinningBid + b.totalWinningBid
        merged.priceCount = a.priceCount + b.priceCount
        merged.campaignId = a.campaignId.ifEmpty { b.campaignId }
        merged.advertiserId = a.advertiserId.ifEmpty { b.advertiserId }
        merged.publisherId = a.publisherId.ifEmpty { b.publisherId }
        merged.action = a.action
        return merged
    }
}

/**
 * Mutable accumulator state for SSP window aggregation.
 */
class SSPAccumulator {
    var base: MetricAccumulator = MetricAccumulator()
    var adRequests: Long = 0L
    var adResponses: Long = 0L
    var totalFloorPrice: Double = 0.0
    var totalWinningBid: Double = 0.0
    var priceCount: Long = 0L
    var campaignId: String = ""
    var advertiserId: String = ""
    var publisherId: String = ""
    var action: EventAction = EventAction.IMPRESSION
}

/**
 * Flink [WindowFunction] that converts the final [SSPAccumulator] into an [SSPAggregatedMetric]
 * after the window closes.
 */
class SSPWindowResultFunction(
    private val windowSizeMs: Long
) : WindowFunction<SSPAccumulator, SSPAggregatedMetric, String, TimeWindow> {

    override fun apply(
        key: String,
        window: TimeWindow,
        input: Iterable<SSPAccumulator>,
        out: Collector<SSPAggregatedMetric>
    ) {
        val acc = input.iterator().next()

        val windowType = when {
            windowSizeMs <= 60_000L -> WindowType.TUMBLING_1MIN
            windowSizeMs <= 300_000L -> WindowType.TUMBLING_5MIN
            else -> WindowType.TUMBLING_1HOUR
        }

        val metricWindow = MetricWindow(
            windowStart = Instant.ofEpochMilli(window.start),
            windowEnd = Instant.ofEpochMilli(window.end),
            windowType = windowType
        )

        val baseMetric = AggregatedMetric(
            window = metricWindow,
            campaignId = acc.campaignId,
            advertiserId = acc.advertiserId,
            action = acc.action,
            impressions = acc.base.impressions,
            clicks = acc.base.clicks,
            conversions = acc.base.conversions,
            views = acc.base.views,
            totalRevenue = acc.base.totalRevenue,
            totalSpend = acc.base.totalSpend,
            uniqueUsers = acc.base.userIds.size.toLong(),
            eventCount = acc.base.eventCount
        )

        val fillRate = if (acc.adRequests > 0) {
            acc.adResponses.toDouble() / acc.adRequests.toDouble()
        } else {
            0.0
        }

        val avgFloorPrice = if (acc.priceCount > 0) acc.totalFloorPrice / acc.priceCount else 0.0
        val avgWinningBid = if (acc.priceCount > 0) acc.totalWinningBid / acc.priceCount else 0.0

        out.collect(
            SSPAggregatedMetric(
                base = baseMetric,
                publisherId = acc.publisherId,
                adRequests = acc.adRequests,
                adResponses = acc.adResponses,
                fillRate = fillRate,
                viewabilityRate = if (baseMetric.impressions > 0) {
                    baseMetric.views.toDouble() / baseMetric.impressions.toDouble()
                } else {
                    0.0
                },
                avgFloorPrice = avgFloorPrice,
                avgWinningBid = avgWinningBid
            )
        )
    }
}
