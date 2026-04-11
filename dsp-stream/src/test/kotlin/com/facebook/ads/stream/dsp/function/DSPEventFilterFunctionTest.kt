package com.facebook.ads.stream.dsp.function

import com.facebook.ads.stream.common.model.DSPEvent
import com.facebook.ads.stream.common.model.EventAction
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

class DSPEventFilterFunctionTest {

    private lateinit var deadLetterTag: OutputTag<DSPEvent>
    private lateinit var filterFunction: DSPEventFilterFunction
    private lateinit var collector: Collector<DSPEvent>
    private lateinit var ctx: ProcessFunction<DSPEvent, DSPEvent>.Context

    @BeforeEach
    fun setup() {
        deadLetterTag = OutputTag("dsp-dead-letter")
        filterFunction = DSPEventFilterFunction(deadLetterTag)
        collector = mockk(relaxed = true)
        ctx = mockk(relaxed = true)
    }

    private fun validEvent(
        eventId: String = "dsp-001",
        action: EventAction = EventAction.BID_WIN
    ) = DSPEvent(
        eventId = eventId,
        timestamp = Instant.now().minusSeconds(5),
        adId = "ad-1",
        campaignId = "camp-1",
        creativeId = "creative-1",
        advertiserId = "adv-1",
        userId = "user-1",
        action = action,
        bidPrice = 2.50,
        clearingPrice = 2.00,
        budgetRemaining = 500.0
    )

    @Test
    fun `valid DSP event should be collected to main output`() {
        val event = validEvent()
        filterFunction.processElement(event, ctx, collector)

        val captured = slot<DSPEvent>()
        verify { collector.collect(capture(captured)) }
        assertThat(captured.captured.eventId).isEqualTo("dsp-001")
    }

    @Test
    fun `event with blank eventId should go to dead letter`() {
        val event = validEvent().copy(eventId = "")
        filterFunction.processElement(event, ctx, collector)

        verify(exactly = 0) { collector.collect(any()) }
        verify { ctx.output(deadLetterTag, event) }
    }

    @Test
    fun `event with blank advertiserId should go to dead letter`() {
        val event = validEvent().copy(advertiserId = "")
        filterFunction.processElement(event, ctx, collector)

        verify(exactly = 0) { collector.collect(any()) }
        verify { ctx.output(deadLetterTag, event) }
    }

    @Test
    fun `event with SSP-only action should go to dead letter`() {
        val event = validEvent(action = EventAction.AD_RENDER)
        filterFunction.processElement(event, ctx, collector)

        verify(exactly = 0) { collector.collect(any()) }
        verify { ctx.output(deadLetterTag, event) }
    }

    @Test
    fun `event with negative bid price should go to dead letter`() {
        val event = validEvent().copy(bidPrice = -1.0)
        filterFunction.processElement(event, ctx, collector)

        verify(exactly = 0) { collector.collect(any()) }
        verify { ctx.output(deadLetterTag, event) }
    }

    @Test
    fun `event with excessively high bid price should go to dead letter`() {
        val event = validEvent().copy(bidPrice = 9999.0)
        filterFunction.processElement(event, ctx, collector)

        verify(exactly = 0) { collector.collect(any()) }
        verify { ctx.output(deadLetterTag, event) }
    }

    @Test
    fun `event too far in past should go to dead letter`() {
        val event = validEvent().copy(timestamp = Instant.now().minusSeconds(25 * 3600L))
        filterFunction.processElement(event, ctx, collector)

        verify(exactly = 0) { collector.collect(any()) }
        verify { ctx.output(deadLetterTag, event) }
    }

    @Test
    fun `multiple valid events should all pass through`() {
        val events = (1..4).map { validEvent(eventId = "dsp-$it") }
        events.forEach { filterFunction.processElement(it, ctx, collector) }

        verify(exactly = 4) { collector.collect(any()) }
        verify(exactly = 0) { ctx.output(deadLetterTag, any()) }
    }

    @Test
    fun `bid request action should be valid for DSP`() {
        val event = validEvent(action = EventAction.BID_REQUEST)
        filterFunction.processElement(event, ctx, collector)

        verify(exactly = 1) { collector.collect(any()) }
        verify(exactly = 0) { ctx.output(deadLetterTag, any()) }
    }
}
