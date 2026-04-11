package com.facebook.ads.stream.common.model

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonProperty
import java.time.Instant

/**
 * Base sealed class representing any ad system event.
 */
sealed class AdEvent {
    abstract val eventId: String
    abstract val timestamp: Instant
    abstract val adId: String
    abstract val campaignId: String
    abstract val creativeId: String
    abstract val advertiserId: String
    abstract val userId: String
    abstract val action: EventAction
    abstract val metadata: Map<String, String>
}

/**
 * SSP (Supply-Side Platform) event — emitted by publishers when ad inventory is filled.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
data class SSPEvent(
    @JsonProperty("event_id") override val eventId: String,
    @JsonProperty("timestamp") override val timestamp: Instant,
    @JsonProperty("ad_id") override val adId: String,
    @JsonProperty("campaign_id") override val campaignId: String,
    @JsonProperty("creative_id") override val creativeId: String,
    @JsonProperty("publisher_id") val publisherId: String,
    @JsonProperty("advertiser_id") override val advertiserId: String,
    @JsonProperty("user_id") override val userId: String,
    @JsonProperty("action") override val action: EventAction,
    @JsonProperty("floor_price") val floorPrice: Double = 0.0,
    @JsonProperty("winning_bid") val winningBid: Double = 0.0,
    @JsonProperty("ad_slot") val adSlot: String = "",
    @JsonProperty("page_url") val pageUrl: String = "",
    @JsonProperty("metadata") override val metadata: Map<String, String> = emptyMap()
) : AdEvent()

/**
 * DSP (Demand-Side Platform) event — emitted by advertisers/bidders during RTB auction.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
data class DSPEvent(
    @JsonProperty("event_id") override val eventId: String,
    @JsonProperty("timestamp") override val timestamp: Instant,
    @JsonProperty("ad_id") override val adId: String,
    @JsonProperty("campaign_id") override val campaignId: String,
    @JsonProperty("creative_id") override val creativeId: String,
    @JsonProperty("advertiser_id") override val advertiserId: String,
    @JsonProperty("user_id") override val userId: String,
    @JsonProperty("action") override val action: EventAction,
    @JsonProperty("bid_price") val bidPrice: Double = 0.0,
    @JsonProperty("clearing_price") val clearingPrice: Double = 0.0,
    @JsonProperty("budget_remaining") val budgetRemaining: Double = 0.0,
    @JsonProperty("targeting_segments") val targetingSegments: List<String> = emptyList(),
    @JsonProperty("metadata") override val metadata: Map<String, String> = emptyMap()
) : AdEvent()

/**
 * Enumeration of all possible ad event action types.
 */
enum class EventAction {
    // Common actions
    IMPRESSION,
    CLICK,
    CONVERSION,
    VIEW,

    // SSP-specific actions
    AD_REQUEST,
    AD_RESPONSE,
    AD_RENDER,
    AD_VIEWABLE,
    AD_BLOCKED,

    // DSP-specific actions
    BID_REQUEST,
    BID_RESPONSE,
    BID_WIN,
    BID_LOSS,
    BID_TIMEOUT,
    BUDGET_EXHAUSTED;

    companion object {
        private val lookup = entries.associateBy { it.name }

        fun fromString(value: String): EventAction =
            lookup[value.uppercase()] ?: throw IllegalArgumentException("Unknown action: $value")

        fun sspActions(): Set<EventAction> =
            setOf(IMPRESSION, CLICK, CONVERSION, VIEW, AD_REQUEST, AD_RESPONSE, AD_RENDER, AD_VIEWABLE, AD_BLOCKED)

        fun dspActions(): Set<EventAction> =
            setOf(IMPRESSION, CLICK, CONVERSION, BID_REQUEST, BID_RESPONSE, BID_WIN, BID_LOSS, BID_TIMEOUT, BUDGET_EXHAUSTED)
    }
}
