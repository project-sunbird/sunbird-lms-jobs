package org.sunbird.job.notification.task

import java.io.File

import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.util.{FlinkUtil}
import org.sunbird.job.notification.domain.{Event, NotificationMessage}
import org.sunbird.job.notification.function.NotificationFunction

class NotificationStreamTask(config: NotificationConfig, kafkaConnector: FlinkKafkaConnector) {
    def process(): Unit = {
        implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
        implicit val eventTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])
        implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])
        implicit val notificationFailedMetaTypeInfo: TypeInformation[NotificationMessage] = TypeExtractor.getForClass(classOf[NotificationMessage])
    
    
        val processStreamTask = env.addSource(kafkaConnector.kafkaJobRequestSource[Event](config.kafkaInputTopic))
            .name(config.notificationConsumer)
            .uid(config.notificationConsumer).setParallelism(config.kafkaConsumerParallelism)
            .rebalance
            .keyBy(new NotificationKeySelector)
            .process(new NotificationFunction(config))
            .name("notification-trigger")
            .uid("notification-trigger")
            .setParallelism(config.parallelism)
        
        processStreamTask.getSideOutput(config.notificationFailedOutputTag)
            .addSink(kafkaConnector.kafkaStringSink(config.kafkaInputTopic))
            .name(config.notificationFailedProducer)
            .uid(config.notificationFailedProducer)
        
        env.execute(config.jobName)
        
    }
    
}

object NotificationStreamTask {
    
    def main(args: Array[String]): Unit = {
        val configFilePath = Option(ParameterTool.fromArgs(args).get("config.file.path"))
        val config = configFilePath.map {
            path => ConfigFactory.parseFile(new File(path)).resolve()
        }.getOrElse(ConfigFactory.load("notification-config.conf").withFallback(ConfigFactory.systemEnvironment()))
        val notificationConfig = new NotificationConfig(config)
        val kafkaUtil = new FlinkKafkaConnector(notificationConfig)
        val task = new NotificationStreamTask(notificationConfig, kafkaUtil)
        task.process()
    }
    
}

class NotificationKeySelector extends KeySelector[Event, String] {
    override def getKey(event: Event): String = Set(event.msgId, event.edataMap.get("iteration").asInstanceOf[Int]).mkString("_")
}