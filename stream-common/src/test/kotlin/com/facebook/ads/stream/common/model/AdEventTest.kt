package com.facebook.ads.stream.common.model

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import java.time.Instant

class AdEventTest {

    private val baseTimestamp = Instant.parse("2024-03-15T10:00:00Z")

    private fun buildSSPEvent(
        eventId: String = "ssp-evt-001",
        action: EventAction = EventAction.IMPRESSION
    ) = SSPEvent(
        eventId = eventId,
        timestamp = baseTimestamp,
        adId = "ad-123",
        campaignId = "camp-456",
        creativeId = "creative-789",
        publisherId = "pub-001",
        advertiserId = "adv-001",
        userId = "user-abc",
        action = action,
        floorPrice = 1.50,
        winningBid = 2.00,
        adSlot = "top-banner",
        pageUrl = "https://example.com/article",
        metadata = mapOf("country" to "US", "device" to "mobile")
    )

    private fun buildDSPEvent(
        eventId: String = "dsp-evt-001",
        action: EventAction = EventAction.BID_WIN
    ) = DSPEvent(
        eventId = eventId,
        timestamp = baseTimestamp,
        adId = "ad-123",
        campaignId = "camp-456",
        creativeId = "creative-789",
        advertiserId = "adv-001",
        userId = "user-abc",
        action = action,
        bidPrice = 2.50,
        clearingPrice = 2.00,
        budgetRemaining = 500.0,
        targetingSegments = listOf("sports", "tech"),
        metadata = mapOf("bid_strategy" to "target_cpa")
    )

    @Test
    fun `SSPEvent should be an AdEvent`() {
        val event = buildSSPEvent()
        assertThat(event).isInstanceOf(AdEvent::class.java)
        assertThat(event).isInstanceOf(SSPEvent::class.java)
    }

    @Test
    fun `DSPEvent should be an AdEvent`() {
        val event = buildDSPEvent()
        assertThat(event).isInstanceOf(AdEvent::class.java)
        assertThat(event).isInstanceOf(DSPEvent::class.java)
    }

    @Test
    fun `SSPEvent data class equality should work correctly`() {
        val event1 = buildSSPEvent()
        val event2 = buildSSPEvent()
        assertThat(event1).isEqualTo(event2)
        assertThat(event1.hashCode()).isEqualTo(event2.hashCode())
    }

    @Test
    fun `SSPEvent copy should produce independent instance with modified fields`() {
        val original = buildSSPEvent()
        val modified = original.copy(winningBid = 3.00, adSlot = "sidebar")
        assertThat(modified.winningBid).isEqualTo(3.00)
        assertThat(modified.adSlot).isEqualTo("sidebar")
        assertThat(modified.eventId).isEqualTo(original.eventId)
    }

    @Test
    fun `DSPEvent targetingSegments should default to empty list`() {
        val event = DSPEvent(
            eventId = "dsp-001",
            timestamp = baseTimestamp,
            adId = "ad-1",
            campaignId = "camp-1",
            creativeId = "creative-1",
            advertiserId = "adv-1",
            userId = "user-1",
            action = EventAction.BID_REQUEST
        )
        assertThat(event.targetingSegments).isEmpty()
        assertThat(event.metadata).isEmpty()
    }

    @Test
    fun `EventAction fromString should handle uppercase and lowercase`() {
        assertThat(EventAction.fromString("impression")).isEqualTo(EventAction.IMPRESSION)
        assertThat(EventAction.fromString("BID_WIN")).isEqualTo(EventAction.BID_WIN)
        assertThat(EventAction.fromString("Click")).isEqualTo(EventAction.CLICK)
    }

    @Test
    fun `EventAction fromString should throw for unknown action`() {
        assertThatThrownBy { EventAction.fromString("UNKNOWN_ACTION") }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("Unknown action")
    }

    @Test
    fun `EventAction sspActions should not contain DSP-only actions`() {
        val sspActions = EventAction.sspActions()
        assertThat(sspActions).doesNotContain(EventAction.BID_REQUEST, EventAction.BID_RESPONSE)
        assertThat(sspActions).contains(EventAction.IMPRESSION, EventAction.AD_REQUEST)
    }

    @Test
    fun `EventAction dspActions should not contain SSP-only actions`() {
        val dspActions = EventAction.dspActions()
        assertThat(dspActions).doesNotContain(EventAction.AD_REQUEST, EventAction.AD_RENDER)
        assertThat(dspActions).contains(EventAction.BID_WIN, EventAction.BID_LOSS)
    }

    @Test
    fun `when expression on sealed class should be exhaustive`() {
        val sspEvent: AdEvent = buildSSPEvent()
        val dspEvent: AdEvent = buildDSPEvent()

        fun processEvent(event: AdEvent): String = when (event) {
            is SSPEvent -> "ssp:${event.publisherId}"
            is DSPEvent -> "dsp:${event.advertiserId}"
        }

        assertThat(processEvent(sspEvent)).isEqualTo("ssp:pub-001")
        assertThat(processEvent(dspEvent)).isEqualTo("dsp:adv-001")
    }
}
