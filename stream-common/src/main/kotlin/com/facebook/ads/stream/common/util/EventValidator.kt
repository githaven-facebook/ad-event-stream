package com.facebook.ads.stream.common.util

import com.facebook.ads.stream.common.model.AdEvent
import com.facebook.ads.stream.common.model.DSPEvent
import com.facebook.ads.stream.common.model.EventAction
import com.facebook.ads.stream.common.model.SSPEvent
import java.time.Duration
import java.time.Instant

/**
 * Validates ad events before they enter the processing pipeline.
 * Returns a [ValidationResult] indicating whether the event is valid and why if not.
 */
object EventValidator {

    private val MAX_EVENT_AGE: Duration = Duration.ofHours(24)
    private val MAX_FUTURE_SKEW: Duration = Duration.ofMinutes(5)
    private const val MAX_BID_PRICE = 1_000.0
    private const val MAX_FLOOR_PRICE = 500.0

    fun validate(event: AdEvent): ValidationResult {
        validateCommonFields(event)?.let { return it }
        return when (event) {
            is SSPEvent -> validateSSPEvent(event)
            is DSPEvent -> validateDSPEvent(event)
        }
    }

    private fun validateCommonFields(event: AdEvent): ValidationResult? {
        if (event.eventId.isBlank()) {
            return ValidationResult.Invalid("event_id is blank")
        }
        if (event.adId.isBlank()) {
            return ValidationResult.Invalid("ad_id is blank")
        }
        if (event.campaignId.isBlank()) {
            return ValidationResult.Invalid("campaign_id is blank")
        }
        if (event.creativeId.isBlank()) {
            return ValidationResult.Invalid("creative_id is blank")
        }
        if (event.advertiserId.isBlank()) {
            return ValidationResult.Invalid("advertiser_id is blank")
        }
        if (event.userId.isBlank()) {
            return ValidationResult.Invalid("user_id is blank")
        }

        val now = Instant.now()
        val age = Duration.between(event.timestamp, now)
        if (age > MAX_EVENT_AGE) {
            return ValidationResult.Invalid("event timestamp too old: age=${age.toHours()}h exceeds ${MAX_EVENT_AGE.toHours()}h")
        }
        if (event.timestamp.isAfter(now.plus(MAX_FUTURE_SKEW))) {
            return ValidationResult.Invalid("event timestamp too far in the future: ${event.timestamp}")
        }

        return null
    }

    private fun validateSSPEvent(event: SSPEvent): ValidationResult {
        if (event.publisherId.isBlank()) {
            return ValidationResult.Invalid("publisher_id is blank for SSP event")
        }
        if (event.action !in EventAction.sspActions()) {
            return ValidationResult.Invalid("invalid SSP action: ${event.action}")
        }
        if (event.floorPrice < 0) {
            return ValidationResult.Invalid("floor_price cannot be negative: ${event.floorPrice}")
        }
        if (event.floorPrice > MAX_FLOOR_PRICE) {
            return ValidationResult.Invalid("floor_price exceeds maximum: ${event.floorPrice} > $MAX_FLOOR_PRICE")
        }
        if (event.winningBid < 0) {
            return ValidationResult.Invalid("winning_bid cannot be negative: ${event.winningBid}")
        }
        return ValidationResult.Valid
    }

    private fun validateDSPEvent(event: DSPEvent): ValidationResult {
        if (event.action !in EventAction.dspActions()) {
            return ValidationResult.Invalid("invalid DSP action: ${event.action}")
        }
        if (event.bidPrice < 0) {
            return ValidationResult.Invalid("bid_price cannot be negative: ${event.bidPrice}")
        }
        if (event.bidPrice > MAX_BID_PRICE) {
            return ValidationResult.Invalid("bid_price exceeds maximum: ${event.bidPrice} > $MAX_BID_PRICE")
        }
        if (event.clearingPrice < 0) {
            return ValidationResult.Invalid("clearing_price cannot be negative: ${event.clearingPrice}")
        }
        if (event.budgetRemaining < 0) {
            return ValidationResult.Invalid("budget_remaining cannot be negative: ${event.budgetRemaining}")
        }
        return ValidationResult.Valid
    }
}

sealed class ValidationResult {
    object Valid : ValidationResult()
    data class Invalid(val reason: String) : ValidationResult()

    val isValid: Boolean get() = this is Valid
}
