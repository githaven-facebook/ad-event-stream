package com.facebook.ads.stream.dsp

import com.facebook.ads.stream.common.config.StreamConfig
import com.facebook.ads.stream.dsp.job.DSPAggregationJob
import com.facebook.ads.stream.dsp.job.DSPStorageJob
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.slf4j.LoggerFactory

/**
 * Main entry point for the DSP event stream processing application.
 *
 * Starts two parallel Flink pipelines:
 *  1. [DSPAggregationJob] — filters invalid events, aggregates bidding metrics by window, sinks to Kafka
 *  2. [DSPStorageJob] — transforms events and writes raw data to S3 in Parquet format
 */
object DSPStreamApp {

    private val LOG = LoggerFactory.getLogger(DSPStreamApp::class.java)

    @JvmStatic
    fun main(args: Array<String>) {
        val params = ParameterTool.fromArgs(args).let { argsParams ->
            val propsFile = argsParams.get("config", null)
            if (propsFile != null) {
                ParameterTool.fromPropertiesFile(propsFile).mergeWith(argsParams)
            } else {
                ParameterTool.fromSystemProperties().mergeWith(argsParams)
            }
        }

        val config = StreamConfig.fromParameterTool(params)
        LOG.info("Starting DSP Stream App with kafka.bootstrap.servers=${config.kafkaBootstrapServers}")

        val env = StreamExecutionEnvironment.getExecutionEnvironment()
        env.parallelism = config.defaultParallelism
        env.config.globalJobParameters = params

        if (config.checkpointingEnabled) {
            env.enableCheckpointing(config.checkpointIntervalMs, CheckpointingMode.EXACTLY_ONCE)
            env.checkpointConfig.checkpointTimeout = config.checkpointTimeoutMs
            env.checkpointConfig.minPauseBetweenCheckpoints = config.checkpointMinPauseBetweenMs
            env.checkpointConfig.maxConcurrentCheckpoints = config.maxConcurrentCheckpoints
            LOG.info(
                "Checkpointing enabled: interval={}ms, storage={}",
                config.checkpointIntervalMs,
                config.checkpointStoragePath
            )
        }

        val jobMode = params.get("job.mode", "all")
        when (jobMode) {
            "aggregation" -> {
                LOG.info("Running DSP aggregation job only")
                DSPAggregationJob(config, env).build()
            }
            "storage" -> {
                LOG.info("Running DSP storage job only")
                DSPStorageJob(config, env).build()
            }
            "all" -> {
                LOG.info("Running all DSP jobs")
                DSPAggregationJob(config, env).build()
                DSPStorageJob(config, env).build()
            }
            else -> {
                LOG.error("Unknown job.mode='$jobMode'. Expected: aggregation | storage | all")
                throw IllegalArgumentException("Unknown job.mode='$jobMode'")
            }
        }

        LOG.info("Submitting DSP stream execution graph")
        env.execute("DSP Event Stream Processor")
    }
}
