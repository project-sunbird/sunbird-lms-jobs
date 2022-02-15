package org.sunbird.job.util

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.sunbird.job.BaseJobConfig

object FlinkUtil {
    def getExecutionContext(config: BaseJobConfig) : StreamExecutionEnvironment = {
        val env : StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.getConfig.setUseSnapshotCompression(config.enableCompressedCheckpointing)
        env.enableCheckpointing(config.checkpointingInterval)
        env.getCheckpointConfig.setCheckpointTimeout(config.checkpointingTimeout)
    
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(config.restartAttempts, config.delayBetweenAttempts))
        env
    }
}
