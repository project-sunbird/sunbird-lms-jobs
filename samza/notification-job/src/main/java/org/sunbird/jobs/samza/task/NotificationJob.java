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

import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.jobs.samza.service.NotificationService;
import org.sunbird.jobs.samza.util.JobLogger;

/**
 * 
 * @author manzarul
 *
 */
public class NotificationJob implements StreamTask, InitableTask {

    private static NotificationService service = new NotificationService();
    private JobLogger Logger = new JobLogger(NotificationJob.class);

    @Override
    public void init(Config config, TaskContext context) throws Exception {
        try {
            service.initialize(config);
            Logger.info("NotificationJob:init: Task initialized");
        } catch (Exception e) {
            Logger.error("NotificationJob:init: Task initialization failed", e);
            throw e;
        }
    }

    @Override
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
        Map<String, Object> message = getMessage(envelope);
        try {
            service.processMessage(message, collector);
        } catch (ProjectCommonException e) {
            Logger.error("NotificationJob:process: Error while processing message", message, e);
        } catch (Exception e) {
            Logger.error("NotificationJob:process: Generic error while processing message", message, e);
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getMessage(IncomingMessageEnvelope envelope) {
        try {
            return (Map<String, Object>) envelope.getMessage();
        } catch (Exception e) {
            Logger.error("NotificationJob:getMessage: Invalid message = " + envelope.getMessage(), e);
            return new HashMap();
        }
    }

}
