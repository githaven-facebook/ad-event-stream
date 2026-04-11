package com.facebook.ads.stream.ssp.function

import com.facebook.ads.stream.common.model.EventAction
import com.facebook.ads.stream.common.model.SSPEvent
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Instant

class SSPAggregateFunctionTest {

    private lateinit var aggregateFunction: SSPAggregateFunction

    @BeforeEach
    fun setup() {
        aggregateFunction = SSPAggregateFunction()
    }

    private fun event(
        action: EventAction = EventAction.IMPRESSION,
        userId: String = "user-1",
        winningBid: Double = 2.0,
        floorPrice: Double = 1.0
    ) = SSPEvent(
        eventId = "evt-${System.nanoTime()}",
        timestamp = Instant.now().minusSeconds(10),
        adId = "ad-1",
        campaignId = "camp-1",
        creativeId = "creative-1",
        publisherId = "pub-1",
        advertiserId = "adv-1",
        userId = userId,
        action = action,
        floorPrice = floorPrice,
        winningBid = winningBid
    )

    @Test
    fun `createAccumulator should return empty accumulator`() {
        val acc = aggregateFunction.createAccumulator()
        assertThat(acc.base.impressions).isEqualTo(0L)
        assertThat(acc.base.clicks).isEqualTo(0L)
        assertThat(acc.base.eventCount).isEqualTo(0L)
        assertThat(acc.campaignId).isEmpty()
    }

    @Test
    fun `impression event should increment impressions and revenue`() {
        val acc = aggregateFunction.createAccumulator()
        val result = aggregateFunction.add(event(action = EventAction.IMPRESSION, winningBid = 2.50), acc)

        assertThat(result.base.impressions).isEqualTo(1L)
        assertThat(result.base.clicks).isEqualTo(0L)
        assertThat(result.base.totalRevenue).isEqualTo(2.50)
        assertThat(result.base.eventCount).isEqualTo(1L)
    }

    @Test
    fun `click event should increment clicks only`() {
        val acc = aggregateFunction.createAccumulator()
        val result = aggregateFunction.add(event(action = EventAction.CLICK, winningBid = 0.0), acc)

        assertThat(result.base.impressions).isEqualTo(0L)
        assertThat(result.base.clicks).isEqualTo(1L)
        assertThat(result.base.totalRevenue).isEqualTo(0.0)
    }

    @Test
    fun `ad request event should increment adRequests`() {
        val acc = aggregateFunction.createAccumulator()
        val result = aggregateFunction.add(event(action = EventAction.AD_REQUEST), acc)

        assertThat(result.adRequests).isEqualTo(1L)
        assertThat(result.adResponses).isEqualTo(0L)
    }

    @Test
    fun `ad render event should increment adResponses`() {
        val acc = aggregateFunction.createAccumulator()
        val result = aggregateFunction.add(event(action = EventAction.AD_RENDER), acc)

        assertThat(result.adResponses).isEqualTo(1L)
    }

    @Test
    fun `multiple events from same user should count as one unique user`() {
        var acc = aggregateFunction.createAccumulator()
        acc = aggregateFunction.add(event(userId = "user-1"), acc)
        acc = aggregateFunction.add(event(userId = "user-1"), acc)
        acc = aggregateFunction.add(event(userId = "user-2"), acc)

        assertThat(acc.base.userIds).hasSize(2)
        assertThat(acc.base.eventCount).isEqualTo(3L)
    }

    @Test
    fun `merge should combine two accumulators correctly`() {
        var accA = aggregateFunction.createAccumulator()
        accA = aggregateFunction.add(event(action = EventAction.IMPRESSION, winningBid = 1.0, userId = "user-1"), accA)
        accA = aggregateFunction.add(event(action = EventAction.IMPRESSION, winningBid = 1.0, userId = "user-2"), accA)

        var accB = aggregateFunction.createAccumulator()
        accB = aggregateFunction.add(event(action = EventAction.CLICK, userId = "user-3"), accB)

        val merged = aggregateFunction.merge(accA, accB)

        assertThat(merged.base.impressions).isEqualTo(2L)
        assertThat(merged.base.clicks).isEqualTo(1L)
        assertThat(merged.base.totalRevenue).isEqualTo(2.0)
        assertThat(merged.base.userIds).hasSize(3)
        assertThat(merged.base.eventCount).isEqualTo(3L)
    }

    @Test
    fun `accumulator campaign fields should be populated from first event`() {
        var acc = aggregateFunction.createAccumulator()
        acc = aggregateFunction.add(
            event().copy(campaignId = "camp-abc", advertiserId = "adv-xyz"),
            acc
        )

        assertThat(acc.campaignId).isEqualTo("camp-abc")
        assertThat(acc.advertiserId).isEqualTo("adv-xyz")
    }

    @Test
    fun `floor price and winning bid averages should be tracked`() {
        var acc = aggregateFunction.createAccumulator()
        acc = aggregateFunction.add(event(floorPrice = 1.0, winningBid = 2.0), acc)
        acc = aggregateFunction.add(event(floorPrice = 2.0, winningBid = 4.0), acc)

        assertThat(acc.totalFloorPrice).isEqualTo(3.0)
        assertThat(acc.totalWinningBid).isEqualTo(6.0)
        assertThat(acc.priceCount).isEqualTo(2L)
    }
}
