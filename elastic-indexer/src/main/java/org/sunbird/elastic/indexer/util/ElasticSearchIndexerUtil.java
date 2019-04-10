package org.sunbird.elastic.indexer.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.elastic.indexer.util.ConnectionClient;; 

public class ElasticSearchIndexerUtil {
  public static ConnectionClient connectionManager;
  private static final List<String> upsertResults = new ArrayList<String>(Arrays.asList("CREATED", "UPDATED", "NOOP"));

  static {
    connectionManager = new ConnectionClient();
  }

  public static boolean upsertData(String index, String type, String identifier, Map<String, Object> data) {
    long startTime = System.currentTimeMillis();
    Client client = connectionManager.createConnection();
    ProjectLogger.log("ElasticSearchUtil upsertData method started at ==" + startTime + " for Type " + type,
        LoggerEnum.PERF_LOG);
    if (!StringUtils.isBlank(index) && !StringUtils.isBlank(type) && !StringUtils.isBlank(identifier) && data != null
        && data.size() > 0) {
      IndexRequest indexRequest = new IndexRequest(index, type, identifier).source(data);
      UpdateRequest updateRequest = new UpdateRequest(index, type, identifier).doc(data).upsert(indexRequest);
      UpdateResponse response = null;
      try {

        response = client.update(updateRequest).get();

      } catch (InterruptedException e) {
        ProjectLogger.log(e.getMessage(), e);
        return false;
      } catch (ExecutionException e) {
        ProjectLogger.log(e.getMessage(), e);
        return false;
      }
      ProjectLogger.log("updated response==" + response.getResult().name());
      if (upsertResults.contains(response.getResult().name())) {
        long stopTime = System.currentTimeMillis();
        long elapsedTime = stopTime - startTime;
        ProjectLogger.log("ElasticSearchUtil upsertData method end at ==" + stopTime + " for Type " + type
            + " ,Total time elapsed = " + elapsedTime, LoggerEnum.PERF_LOG);
        return true;
      }
    } else {
      ProjectLogger.log("Requested data is invalid.");
    }
    long stopTime = System.currentTimeMillis();
    long elapsedTime = stopTime - startTime;
    ProjectLogger.log("ElasticSearchUtil upsertData method end at ==" + stopTime + " for Type " + type
        + " ,Total time elapsed = " + elapsedTime, LoggerEnum.PERF_LOG);
    return false;
  }

  /**
   * This method will provide data form ES based on incoming identifier. we can
   * get data by passing index and identifier values , or all the three
   *
   * @param type       String
   * @param identifier String
   * @return Map<String,Object> or null
   */
  public static Map<String, Object> getDataByIdentifier(String index, String type, String identifier) {
    Client client = connectionManager.createConnection();
    long startTime = System.currentTimeMillis();
    ProjectLogger.log("ElasticSearchUtil getDataByIdentifier method started at ==" + startTime + " for Type " + type,
        LoggerEnum.PERF_LOG);
    GetResponse response = null;
    if (StringUtils.isBlank(index) || StringUtils.isBlank(identifier)) {
      ProjectLogger.log("Invalid request is coming.");
      return new HashMap<>();
    } else if (StringUtils.isBlank(type)) {
      response = client.prepareGet().setIndex(index).setId(identifier).get();
    } else {
      response = client.prepareGet(index, type, identifier).get();
    }
    if (response == null || null == response.getSource()) {
      return new HashMap<>();
    }
    long stopTime = System.currentTimeMillis();
    long elapsedTime = stopTime - startTime;
    ProjectLogger.log("ElasticSearchUtil getDataByIdentifier method end at ==" + stopTime + " for Type " + type
        + " ,Total time elapsed = " + elapsedTime, LoggerEnum.PERF_LOG);
    return response.getSource();
  }

}
