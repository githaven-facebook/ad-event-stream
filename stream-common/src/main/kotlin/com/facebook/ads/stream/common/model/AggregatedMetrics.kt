package com.facebook.ads.stream.common.model

import com.fasterxml.jackson.annotation.JsonProperty
import java.time.Instant

/**
 * Represents a time window for metric aggregation.
 */
data class MetricWindow(
    @JsonProperty("window_start") val windowStart: Instant,
    @JsonProperty("window_end") val windowEnd: Instant,
    @JsonProperty("window_type") val windowType: WindowType
) {
    val durationSeconds: Long
        get() = windowEnd.epochSecond - windowStart.epochSecond
}

enum class WindowType {
    TUMBLING_1MIN,
    TUMBLING_5MIN,
    TUMBLING_1HOUR,
    SLIDING_5MIN_1MIN,
    SESSION
}

/**
 * Aggregated ad metrics for a campaign within a time window.
 */
data class AggregatedMetric(
    @JsonProperty("window") val window: MetricWindow,
    @JsonProperty("campaign_id") val campaignId: String,
    @JsonProperty("advertiser_id") val advertiserId: String,
    @JsonProperty("action") val action: EventAction,
    @JsonProperty("impressions") val impressions: Long = 0L,
    @JsonProperty("clicks") val clicks: Long = 0L,
    @JsonProperty("conversions") val conversions: Long = 0L,
    @JsonProperty("views") val views: Long = 0L,
    @JsonProperty("total_revenue") val totalRevenue: Double = 0.0,
    @JsonProperty("total_spend") val totalSpend: Double = 0.0,
    @JsonProperty("unique_users") val uniqueUsers: Long = 0L,
    @JsonProperty("event_count") val eventCount: Long = 0L,
    @JsonProperty("created_at") val createdAt: Instant = Instant.now()
) {
    val clickThroughRate: Double
        get() = if (impressions > 0) clicks.toDouble() / impressions.toDouble() else 0.0

    val conversionRate: Double
        get() = if (clicks > 0) conversions.toDouble() / clicks.toDouble() else 0.0

    val costPerClick: Double
        get() = if (clicks > 0) totalSpend / clicks.toDouble() else 0.0

    val costPerConversion: Double
        get() = if (conversions > 0) totalSpend / conversions.toDouble() else 0.0
}

/**
 * Accumulator used during Flink window aggregation.
 */
data class MetricAccumulator(
    var impressions: Long = 0L,
    var clicks: Long = 0L,
    var conversions: Long = 0L,
    var views: Long = 0L,
    var totalRevenue: Double = 0.0,
    var totalSpend: Double = 0.0,
    var eventCount: Long = 0L,
    val userIds: MutableSet<String> = mutableSetOf()
) {
    fun merge(other: MetricAccumulator): MetricAccumulator = MetricAccumulator(
        impressions = this.impressions + other.impressions,
        clicks = this.clicks + other.clicks,
        conversions = this.conversions + other.conversions,
        views = this.views + other.views,
        totalRevenue = this.totalRevenue + other.totalRevenue,
        totalSpend = this.totalSpend + other.totalSpend,
        eventCount = this.eventCount + other.eventCount,
        userIds = (this.userIds + other.userIds).toMutableSet()
    )
}

/**
 * SSP-specific aggregated metrics including publisher-side data.
 */
data class SSPAggregatedMetric(
    @JsonProperty("base") val base: AggregatedMetric,
    @JsonProperty("publisher_id") val publisherId: String,
    @JsonProperty("ad_requests") val adRequests: Long = 0L,
    @JsonProperty("ad_responses") val adResponses: Long = 0L,
    @JsonProperty("fill_rate") val fillRate: Double = 0.0,
    @JsonProperty("viewability_rate") val viewabilityRate: Double = 0.0,
    @JsonProperty("avg_floor_price") val avgFloorPrice: Double = 0.0,
    @JsonProperty("avg_winning_bid") val avgWinningBid: Double = 0.0
)

/**
 * DSP-specific aggregated metrics including bidding performance data.
 */
data class DSPAggregatedMetric(
    @JsonProperty("base") val base: AggregatedMetric,
    @JsonProperty("bid_requests") val bidRequests: Long = 0L,
    @JsonProperty("bid_responses") val bidResponses: Long = 0L,
    @JsonProperty("bid_wins") val bidWins: Long = 0L,
    @JsonProperty("bid_losses") val bidLosses: Long = 0L,
    @JsonProperty("bid_timeouts") val bidTimeouts: Long = 0L,
    @JsonProperty("win_rate") val winRate: Double = 0.0,
    @JsonProperty("avg_bid_price") val avgBidPrice: Double = 0.0,
    @JsonProperty("avg_clearing_price") val avgClearingPrice: Double = 0.0,
    @JsonProperty("total_budget_spent") val totalBudgetSpent: Double = 0.0
)
