package org.sunbird.jobs.samza.task;

import java.util.HashMap;
import java.util.Map;

import org.apache.samza.config.Config;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.jobs.samza.service.IndexerService;

public class IndexerTask implements StreamTask, InitableTask {

  private static IndexerService service = new IndexerService();

  @Override
  public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator)
      throws Exception {
    Map<String, Object> event = getMessage(envelope);
    ProjectLogger.log("IndexerTask:process: event = " + event, LoggerEnum.INFO);
    service.process(event);
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> getMessage(IncomingMessageEnvelope envelope) {
    try {
      return (Map<String, Object>) envelope.getMessage();
    } catch (Exception e) {
      e.printStackTrace();
      ProjectLogger.log("IndexerTask:getMessage: Invalid message = " + envelope.getMessage() + " with error : " + e,
          LoggerEnum.ERROR);
      return new HashMap<String, Object>();
    }
  }

  @Override
  public void init(Config config, TaskContext taskContext) throws Exception {
    ProjectLogger.log("IndexerTask:init: config = " + config.toString(), LoggerEnum.INFO);
    service.load(config);
  }
}
