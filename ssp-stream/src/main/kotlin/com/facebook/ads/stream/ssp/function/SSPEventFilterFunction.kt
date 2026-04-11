package com.facebook.ads.stream.ssp.function

import com.facebook.ads.stream.common.model.SSPEvent
import com.facebook.ads.stream.common.util.EventValidator
import com.facebook.ads.stream.common.util.ValidationResult
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector
import org.apache.flink.util.OutputTag
import org.slf4j.LoggerFactory

/**
 * Flink ProcessFunction that filters invalid SSP events.
 *
 * Valid events are emitted to the main output stream.
 * Invalid events are routed to the [deadLetterTag] side output for DLQ processing.
 */
class SSPEventFilterFunction(
    private val deadLetterTag: OutputTag<SSPEvent>
) : ProcessFunction<SSPEvent, SSPEvent>() {

    @Transient
    private var processedCount = 0L

    @Transient
    private var rejectedCount = 0L

    override fun processElement(
        event: SSPEvent,
        ctx: Context,
        out: Collector<SSPEvent>
    ) {
        processedCount++
        when (val result = EventValidator.validate(event)) {
            is ValidationResult.Valid -> out.collect(event)
            is ValidationResult.Invalid -> {
                rejectedCount++
                LOG.warn(
                    "Rejecting invalid SSP event eventId={} campaignId={} reason={}",
                    event.eventId, event.campaignId, result.reason
                )
                ctx.output(deadLetterTag, event)

                if (rejectedCount % LOG_INTERVAL == 0L) {
                    LOG.info(
                        "SSP filter stats: processed={} rejected={} rejection_rate={:.2f}%",
                        processedCount,
                        rejectedCount,
                        rejectedCount.toDouble() / processedCount.toDouble() * 100.0
                    )
                }
            }
        }
    }

    companion object {
        private val LOG = LoggerFactory.getLogger(SSPEventFilterFunction::class.java)
        private const val LOG_INTERVAL = 10_000L
    }
}
