package com.facebook.ads.stream.common.util

import com.facebook.ads.stream.common.model.DSPEvent
import com.facebook.ads.stream.common.model.EventAction
import com.facebook.ads.stream.common.model.SSPEvent
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.time.Instant

class EventValidatorTest {

    private val now = Instant.now()

    private fun validSSPEvent() = SSPEvent(
        eventId = "ssp-valid-001",
        timestamp = now.minusSeconds(30),
        adId = "ad-123",
        campaignId = "camp-456",
        creativeId = "creative-789",
        publisherId = "pub-001",
        advertiserId = "adv-001",
        userId = "user-abc",
        action = EventAction.IMPRESSION,
        floorPrice = 1.50,
        winningBid = 2.00
    )

    private fun validDSPEvent() = DSPEvent(
        eventId = "dsp-valid-001",
        timestamp = now.minusSeconds(30),
        adId = "ad-123",
        campaignId = "camp-456",
        creativeId = "creative-789",
        advertiserId = "adv-001",
        userId = "user-abc",
        action = EventAction.BID_WIN,
        bidPrice = 2.50,
        clearingPrice = 2.00,
        budgetRemaining = 500.0
    )

    @Test
    fun `valid SSP event should pass validation`() {
        val result = EventValidator.validate(validSSPEvent())
        assertThat(result.isValid).isTrue()
    }

    @Test
    fun `valid DSP event should pass validation`() {
        val result = EventValidator.validate(validDSPEvent())
        assertThat(result.isValid).isTrue()
    }

    @Test
    fun `SSP event with blank eventId should fail`() {
        val result = EventValidator.validate(validSSPEvent().copy(eventId = ""))
        assertThat(result.isValid).isFalse()
        assertThat((result as ValidationResult.Invalid).reason).contains("event_id")
    }

    @Test
    fun `SSP event with blank campaignId should fail`() {
        val result = EventValidator.validate(validSSPEvent().copy(campaignId = ""))
        assertThat(result.isValid).isFalse()
        assertThat((result as ValidationResult.Invalid).reason).contains("campaign_id")
    }

    @Test
    fun `SSP event with blank publisherId should fail`() {
        val result = EventValidator.validate(validSSPEvent().copy(publisherId = ""))
        assertThat(result.isValid).isFalse()
        assertThat((result as ValidationResult.Invalid).reason).contains("publisher_id")
    }

    @Test
    fun `SSP event with timestamp older than 24 hours should fail`() {
        val oldEvent = validSSPEvent().copy(timestamp = now.minusSeconds(25 * 3600L))
        val result = EventValidator.validate(oldEvent)
        assertThat(result.isValid).isFalse()
        assertThat((result as ValidationResult.Invalid).reason).contains("too old")
    }

    @Test
    fun `SSP event with future timestamp beyond skew should fail`() {
        val futureEvent = validSSPEvent().copy(timestamp = now.plusSeconds(600L))
        val result = EventValidator.validate(futureEvent)
        assertThat(result.isValid).isFalse()
        assertThat((result as ValidationResult.Invalid).reason).contains("future")
    }

    @Test
    fun `SSP event with negative floor price should fail`() {
        val result = EventValidator.validate(validSSPEvent().copy(floorPrice = -0.01))
        assertThat(result.isValid).isFalse()
        assertThat((result as ValidationResult.Invalid).reason).contains("floor_price")
    }

    @Test
    fun `SSP event with invalid action type should fail`() {
        val result = EventValidator.validate(validSSPEvent().copy(action = EventAction.BID_WIN))
        assertThat(result.isValid).isFalse()
        assertThat((result as ValidationResult.Invalid).reason).contains("invalid SSP action")
    }

    @Test
    fun `DSP event with negative bid price should fail`() {
        val result = EventValidator.validate(validDSPEvent().copy(bidPrice = -1.0))
        assertThat(result.isValid).isFalse()
        assertThat((result as ValidationResult.Invalid).reason).contains("bid_price")
    }

    @Test
    fun `DSP event with bid price exceeding maximum should fail`() {
        val result = EventValidator.validate(validDSPEvent().copy(bidPrice = 1001.0))
        assertThat(result.isValid).isFalse()
        assertThat((result as ValidationResult.Invalid).reason).contains("bid_price exceeds maximum")
    }

    @Test
    fun `DSP event with invalid action type should fail`() {
        val result = EventValidator.validate(validDSPEvent().copy(action = EventAction.AD_RENDER))
        assertThat(result.isValid).isFalse()
        assertThat((result as ValidationResult.Invalid).reason).contains("invalid DSP action")
    }

    @Test
    fun `DSP event with negative budget remaining should fail`() {
        val result = EventValidator.validate(validDSPEvent().copy(budgetRemaining = -100.0))
        assertThat(result.isValid).isFalse()
        assertThat((result as ValidationResult.Invalid).reason).contains("budget_remaining")
    }

    @Test
    fun `ValidationResult Valid should have isValid true`() {
        assertThat(ValidationResult.Valid.isValid).isTrue()
    }

    @Test
    fun `ValidationResult Invalid should have isValid false`() {
        val result = ValidationResult.Invalid("some reason")
        assertThat(result.isValid).isFalse()
        assertThat(result.reason).isEqualTo("some reason")
    }
}
