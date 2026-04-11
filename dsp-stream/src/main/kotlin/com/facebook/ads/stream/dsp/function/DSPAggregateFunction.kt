package com.facebook.ads.stream.dsp.function

import com.facebook.ads.stream.common.model.AggregatedMetric
import com.facebook.ads.stream.common.model.DSPAggregatedMetric
import com.facebook.ads.stream.common.model.DSPEvent
import com.facebook.ads.stream.common.model.EventAction
import com.facebook.ads.stream.common.model.MetricAccumulator
import com.facebook.ads.stream.common.model.MetricWindow
import com.facebook.ads.stream.common.model.WindowType
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.functions.windowing.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import java.time.Instant

/**
 * Flink [AggregateFunction] that incrementally accumulates DSP event metrics.
 * Tracks bid requests, responses, wins, losses, timeouts, spend, and unique users.
 */
class DSPAggregateFunction : AggregateFunction<DSPEvent, DSPAccumulator, DSPAccumulator> {

    override fun createAccumulator(): DSPAccumulator = DSPAccumulator()

    override fun add(event: DSPEvent, accumulator: DSPAccumulator): DSPAccumulator {
        accumulator.base.eventCount++
        accumulator.base.userIds.add(event.userId)

        when (event.action) {
            EventAction.IMPRESSION -> {
                accumulator.base.impressions++
                accumulator.base.totalSpend += event.clearingPrice
            }
            EventAction.CLICK -> accumulator.base.clicks++
            EventAction.CONVERSION -> accumulator.base.conversions++
            EventAction.BID_REQUEST -> accumulator.bidRequests++
            EventAction.BID_RESPONSE -> {
                accumulator.bidResponses++
                accumulator.totalBidPrice += event.bidPrice
            }
            EventAction.BID_WIN -> {
                accumulator.bidWins++
                accumulator.totalClearingPrice += event.clearingPrice
                accumulator.totalBudgetSpent += event.clearingPrice
            }
            EventAction.BID_LOSS -> accumulator.bidLosses++
            EventAction.BID_TIMEOUT -> accumulator.bidTimeouts++
            EventAction.BUDGET_EXHAUSTED -> accumulator.budgetExhaustedCount++
            else -> { /* other actions counted in eventCount */ }
        }

        if (accumulator.campaignId.isEmpty()) {
            accumulator.campaignId = event.campaignId
            accumulator.advertiserId = event.advertiserId
            accumulator.action = event.action
        }

        return accumulator
    }

    override fun getResult(accumulator: DSPAccumulator): DSPAccumulator = accumulator

    override fun merge(a: DSPAccumulator, b: DSPAccumulator): DSPAccumulator {
        val merged = DSPAccumulator()
        merged.base = a.base.merge(b.base)
        merged.bidRequests = a.bidRequests + b.bidRequests
        merged.bidResponses = a.bidResponses + b.bidResponses
        merged.bidWins = a.bidWins + b.bidWins
        merged.bidLosses = a.bidLosses + b.bidLosses
        merged.bidTimeouts = a.bidTimeouts + b.bidTimeouts
        merged.budgetExhaustedCount = a.budgetExhaustedCount + b.budgetExhaustedCount
        merged.totalBidPrice = a.totalBidPrice + b.totalBidPrice
        merged.totalClearingPrice = a.totalClearingPrice + b.totalClearingPrice
        merged.totalBudgetSpent = a.totalBudgetSpent + b.totalBudgetSpent
        merged.campaignId = a.campaignId.ifEmpty { b.campaignId }
        merged.advertiserId = a.advertiserId.ifEmpty { b.advertiserId }
        merged.action = a.action
        return merged
    }
}

/**
 * Mutable accumulator state for DSP window aggregation.
 */
class DSPAccumulator {
    var base: MetricAccumulator = MetricAccumulator()
    var bidRequests: Long = 0L
    var bidResponses: Long = 0L
    var bidWins: Long = 0L
    var bidLosses: Long = 0L
    var bidTimeouts: Long = 0L
    var budgetExhaustedCount: Long = 0L
    var totalBidPrice: Double = 0.0
    var totalClearingPrice: Double = 0.0
    var totalBudgetSpent: Double = 0.0
    var campaignId: String = ""
    var advertiserId: String = ""
    var action: EventAction = EventAction.BID_REQUEST
}

/**
 * Flink [WindowFunction] that converts the final [DSPAccumulator] into a [DSPAggregatedMetric]
 * when the window closes.
 */
class DSPWindowResultFunction(
    private val windowSizeMs: Long
) : WindowFunction<DSPAccumulator, DSPAggregatedMetric, String, TimeWindow> {

    override fun apply(
        key: String,
        window: TimeWindow,
        input: Iterable<DSPAccumulator>,
        out: Collector<DSPAggregatedMetric>
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
            totalRevenue = acc.base.totalRevenue,
            totalSpend = acc.base.totalSpend,
            uniqueUsers = acc.base.userIds.size.toLong(),
            eventCount = acc.base.eventCount
        )

        val totalBids = acc.bidRequests
        val winRate = if (totalBids > 0) acc.bidWins.toDouble() / totalBids.toDouble() else 0.0
        val avgBidPrice = if (acc.bidResponses > 0) acc.totalBidPrice / acc.bidResponses.toDouble() else 0.0
        val avgClearingPrice = if (acc.bidWins > 0) acc.totalClearingPrice / acc.bidWins.toDouble() else 0.0

        out.collect(
            DSPAggregatedMetric(
                base = baseMetric,
                bidRequests = acc.bidRequests,
                bidResponses = acc.bidResponses,
                bidWins = acc.bidWins,
                bidLosses = acc.bidLosses,
                bidTimeouts = acc.bidTimeouts,
                winRate = winRate,
                avgBidPrice = avgBidPrice,
                avgClearingPrice = avgClearingPrice,
                totalBudgetSpent = acc.totalBudgetSpent
            )
        )
    }
}
