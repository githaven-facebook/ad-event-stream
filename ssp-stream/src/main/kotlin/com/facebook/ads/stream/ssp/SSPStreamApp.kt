package com.facebook.ads.stream.ssp

import com.facebook.ads.stream.common.config.StreamConfig
import com.facebook.ads.stream.ssp.job.SSPAggregationJob
import com.facebook.ads.stream.ssp.job.SSPStorageJob
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.CheckpointingMode
import org.slf4j.LoggerFactory

/**
 * Main entry point for the SSP event stream processing application.
 *
 * Starts two parallel Flink pipelines:
 *  1. [SSPAggregationJob] — filters, aggregates by window, and sinks metrics to Kafka
 *  2. [SSPStorageJob] — transforms events and writes raw data to S3 in Parquet format
 */
object SSPStreamApp {

    private val LOG = LoggerFactory.getLogger(SSPStreamApp::class.java)

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
        LOG.info("Starting SSP Stream App with kafka.bootstrap.servers=${config.kafkaBootstrapServers}")

        val env = StreamExecutionEnvironment.getExecutionEnvironment()
        env.parallelism = config.defaultParallelism
        env.config.globalJobParameters = params

        if (config.checkpointingEnabled) {
            env.enableCheckpointing(config.checkpointIntervalMs, CheckpointingMode.EXACTLY_ONCE)
            env.checkpointConfig.checkpointTimeout = config.checkpointTimeoutMs
            env.checkpointConfig.minPauseBetweenCheckpoints = config.checkpointMinPauseBetweenMs
            env.checkpointConfig.maxConcurrentCheckpoints = config.maxConcurrentCheckpoints
            LOG.info("Checkpointing enabled: interval={}ms, storage={}", config.checkpointIntervalMs, config.checkpointStoragePath)
        }

        val jobMode = params.get("job.mode", "all")
        when (jobMode) {
            "aggregation" -> {
                LOG.info("Running SSP aggregation job only")
                SSPAggregationJob(config, env).build()
            }
            "storage" -> {
                LOG.info("Running SSP storage job only")
                SSPStorageJob(config, env).build()
            }
            "all" -> {
                LOG.info("Running all SSP jobs")
                SSPAggregationJob(config, env).build()
                SSPStorageJob(config, env).build()
            }
            else -> {
                LOG.error("Unknown job.mode='$jobMode'. Expected: aggregation | storage | all")
                throw IllegalArgumentException("Unknown job.mode='$jobMode'")
            }
        }

        LOG.info("Submitting SSP stream execution graph")
        env.execute("SSP Event Stream Processor")
    }
}
