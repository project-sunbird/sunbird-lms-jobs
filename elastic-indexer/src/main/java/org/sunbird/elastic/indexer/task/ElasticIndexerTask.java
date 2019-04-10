
package org.sunbird.elastic.indexer.task;

import org.apache.samza.config.Config;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.sunbird.elastic.indexer.service.Service;

public class ElasticIndexerTask implements StreamTask, InitableTask {

  private String outputKafkatopic;
  private Service service;

  @Override
  public void init(Config config, TaskContext context) {
    service = new Service();
//		outputKafkatopic = config.get("ouput.topic");
  }

  @Override
  public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator taskCoordinator)
      throws Exception {
    String message = (String) envelope.getMessage();
    message = message + " completed";
    System.out.println(message);
    // i need to read message and after reading i will be able to complete the
    // processing ,
    // please make it to read a message from a well defined topic.
    service.process(message);
    System.out.println(
        "*************************************************************************************************************");

  }

}
