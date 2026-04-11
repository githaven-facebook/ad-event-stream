package com.facebook.ads.stream.ssp.function

import com.facebook.ads.stream.common.model.EventAction
import com.facebook.ads.stream.common.model.SSPEvent
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector
import org.apache.flink.util.OutputTag
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Instant

class SSPEventFilterFunctionTest {

    private lateinit var deadLetterTag: OutputTag<SSPEvent>
    private lateinit var filterFunction: SSPEventFilterFunction
    private lateinit var collector: Collector<SSPEvent>
    private lateinit var ctx: ProcessFunction<SSPEvent, SSPEvent>.Context

    @BeforeEach
    fun setup() {
        deadLetterTag = OutputTag("ssp-dead-letter")
        filterFunction = SSPEventFilterFunction(deadLetterTag)
        collector = mockk(relaxed = true)
        ctx = mockk(relaxed = true)
    }

    private fun validEvent(
        eventId: String = "ssp-001",
        action: EventAction = EventAction.IMPRESSION
    ) = SSPEvent(
        eventId = eventId,
        timestamp = Instant.now().minusSeconds(5),
        adId = "ad-1",
        campaignId = "camp-1",
        creativeId = "creative-1",
        publisherId = "pub-1",
        advertiserId = "adv-1",
        userId = "user-1",
        action = action,
        floorPrice = 1.0,
        winningBid = 1.5
    )

    @Test
    fun `valid event should be collected to main output`() {
        val event = validEvent()
        filterFunction.processElement(event, ctx, collector)

        val captured = slot<SSPEvent>()
        verify { collector.collect(capture(captured)) }
        assertThat(captured.captured.eventId).isEqualTo("ssp-001")
    }

    @Test
    fun `event with blank eventId should go to dead letter`() {
        val event = validEvent().copy(eventId = "")
        filterFunction.processElement(event, ctx, collector)

        verify(exactly = 0) { collector.collect(any()) }
        verify { ctx.output(deadLetterTag, event) }
    }

    @Test
    fun `event with blank campaignId should go to dead letter`() {
        val event = validEvent().copy(campaignId = "")
        filterFunction.processElement(event, ctx, collector)

        verify(exactly = 0) { collector.collect(any()) }
        verify { ctx.output(deadLetterTag, event) }
    }

    @Test
    fun `event with DSP-only action should go to dead letter`() {
        val event = validEvent(action = EventAction.BID_WIN)
        filterFunction.processElement(event, ctx, collector)

        verify(exactly = 0) { collector.collect(any()) }
        verify { ctx.output(deadLetterTag, event) }
    }

    @Test
    fun `event with very old timestamp should go to dead letter`() {
        val event = validEvent().copy(timestamp = Instant.now().minusSeconds(25 * 3600L))
        filterFunction.processElement(event, ctx, collector)

        verify(exactly = 0) { collector.collect(any()) }
        verify { ctx.output(deadLetterTag, event) }
    }

    @Test
    fun `multiple valid events should all pass through`() {
        val events = (1..5).map { validEvent(eventId = "ssp-$it") }
        events.forEach { filterFunction.processElement(it, ctx, collector) }

        verify(exactly = 5) { collector.collect(any()) }
        verify(exactly = 0) { ctx.output(deadLetterTag, any()) }
    }

    @Test
    fun `mix of valid and invalid events should route correctly`() {
        filterFunction.processElement(validEvent(eventId = "ssp-valid-1"), ctx, collector)
        filterFunction.processElement(validEvent().copy(eventId = ""), ctx, collector)
        filterFunction.processElement(validEvent(eventId = "ssp-valid-2"), ctx, collector)

        verify(exactly = 2) { collector.collect(any()) }
        verify(exactly = 1) { ctx.output(deadLetterTag, any()) }
    }
}
