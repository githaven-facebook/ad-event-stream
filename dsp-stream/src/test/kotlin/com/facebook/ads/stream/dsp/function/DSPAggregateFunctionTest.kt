package com.facebook.ads.stream.dsp.function

import com.facebook.ads.stream.common.model.DSPEvent
import com.facebook.ads.stream.common.model.EventAction
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.within
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Instant

class DSPAggregateFunctionTest {

    private lateinit var aggregateFunction: DSPAggregateFunction

    @BeforeEach
    fun setup() {
        aggregateFunction = DSPAggregateFunction()
    }

    private fun event(
        action: EventAction = EventAction.BID_WIN,
        userId: String = "user-1",
        bidPrice: Double = 2.50,
        clearingPrice: Double = 2.00,
        budgetRemaining: Double = 500.0
    ) = DSPEvent(
        eventId = "evt-${System.nanoTime()}",
        timestamp = Instant.now().minusSeconds(10),
        adId = "ad-1",
        campaignId = "camp-1",
        creativeId = "creative-1",
        advertiserId = "adv-1",
        userId = userId,
        action = action,
        bidPrice = bidPrice,
        clearingPrice = clearingPrice,
        budgetRemaining = budgetRemaining
    )

    @Test
    fun `createAccumulator should return zero state`() {
        val acc = aggregateFunction.createAccumulator()
        assertThat(acc.bidRequests).isEqualTo(0L)
        assertThat(acc.bidWins).isEqualTo(0L)
        assertThat(acc.totalBudgetSpent).isEqualTo(0.0)
        assertThat(acc.campaignId).isEmpty()
    }

    @Test
    fun `bid request event should increment bidRequests`() {
        val acc = aggregateFunction.createAccumulator()
        val result = aggregateFunction.add(event(action = EventAction.BID_REQUEST), acc)

        assertThat(result.bidRequests).isEqualTo(1L)
        assertThat(result.bidResponses).isEqualTo(0L)
        assertThat(result.bidWins).isEqualTo(0L)
    }

    @Test
    fun `bid response should increment bidResponses and track bid price`() {
        val acc = aggregateFunction.createAccumulator()
        val result = aggregateFunction.add(event(action = EventAction.BID_RESPONSE, bidPrice = 3.00), acc)

        assertThat(result.bidResponses).isEqualTo(1L)
        assertThat(result.totalBidPrice).isEqualTo(3.00)
    }

    @Test
    fun `bid win should increment wins and track clearing price and spend`() {
        val acc = aggregateFunction.createAccumulator()
        val result = aggregateFunction.add(event(action = EventAction.BID_WIN, clearingPrice = 2.50), acc)

        assertThat(result.bidWins).isEqualTo(1L)
        assertThat(result.totalClearingPrice).isEqualTo(2.50)
        assertThat(result.totalBudgetSpent).isEqualTo(2.50)
    }

    @Test
    fun `bid loss should increment losses`() {
        val acc = aggregateFunction.createAccumulator()
        val result = aggregateFunction.add(event(action = EventAction.BID_LOSS), acc)

        assertThat(result.bidLosses).isEqualTo(1L)
        assertThat(result.bidWins).isEqualTo(0L)
    }

    @Test
    fun `bid timeout should increment timeouts`() {
        val acc = aggregateFunction.createAccumulator()
        val result = aggregateFunction.add(event(action = EventAction.BID_TIMEOUT), acc)

        assertThat(result.bidTimeouts).isEqualTo(1L)
    }

    @Test
    fun `impression should increment impressions and spend`() {
        val acc = aggregateFunction.createAccumulator()
        val result = aggregateFunction.add(event(action = EventAction.IMPRESSION, clearingPrice = 1.80), acc)

        assertThat(result.base.impressions).isEqualTo(1L)
        assertThat(result.base.totalSpend).isEqualTo(1.80)
    }

    @Test
    fun `multiple unique users should be tracked`() {
        var acc = aggregateFunction.createAccumulator()
        acc = aggregateFunction.add(event(userId = "user-1"), acc)
        acc = aggregateFunction.add(event(userId = "user-2"), acc)
        acc = aggregateFunction.add(event(userId = "user-1"), acc)

        assertThat(acc.base.userIds).hasSize(2)
        assertThat(acc.base.eventCount).isEqualTo(3L)
    }

    @Test
    fun `merge should correctly combine two DSP accumulators`() {
        var accA = aggregateFunction.createAccumulator()
        accA = aggregateFunction.add(event(action = EventAction.BID_REQUEST, userId = "user-1"), accA)
        accA = aggregateFunction.add(event(action = EventAction.BID_WIN, clearingPrice = 2.0, userId = "user-2"), accA)

        var accB = aggregateFunction.createAccumulator()
        accB = aggregateFunction.add(event(action = EventAction.BID_LOSS, userId = "user-3"), accB)
        accB = aggregateFunction.add(event(action = EventAction.BID_TIMEOUT, userId = "user-4"), accB)

        val merged = aggregateFunction.merge(accA, accB)

        assertThat(merged.bidRequests).isEqualTo(1L)
        assertThat(merged.bidWins).isEqualTo(1L)
        assertThat(merged.bidLosses).isEqualTo(1L)
        assertThat(merged.bidTimeouts).isEqualTo(1L)
        assertThat(merged.totalBudgetSpent).isEqualTo(2.0)
        assertThat(merged.base.userIds).hasSize(4)
        assertThat(merged.base.eventCount).isEqualTo(4L)
    }

    @Test
    fun `accumulator should populate campaign fields from first event`() {
        var acc = aggregateFunction.createAccumulator()
        acc = aggregateFunction.add(
            event().copy(campaignId = "camp-xyz", advertiserId = "adv-abc"),
            acc
        )

        assertThat(acc.campaignId).isEqualTo("camp-xyz")
        assertThat(acc.advertiserId).isEqualTo("adv-abc")
    }

    @Test
    fun `total bid price accumulation across multiple responses`() {
        var acc = aggregateFunction.createAccumulator()
        acc = aggregateFunction.add(event(action = EventAction.BID_RESPONSE, bidPrice = 1.00), acc)
        acc = aggregateFunction.add(event(action = EventAction.BID_RESPONSE, bidPrice = 2.00), acc)
        acc = aggregateFunction.add(event(action = EventAction.BID_RESPONSE, bidPrice = 3.00), acc)

        assertThat(acc.bidResponses).isEqualTo(3L)
        assertThat(acc.totalBidPrice).isCloseTo(6.00, within(0.001))
    }
}
