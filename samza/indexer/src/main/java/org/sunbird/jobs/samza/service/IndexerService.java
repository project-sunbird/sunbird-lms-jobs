package org.sunbird.jobs.samza.service;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.samza.config.Config;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.PropertiesCache;
import org.sunbird.jobs.samza.util.JSONUtils;
import org.sunbird.models.Constants;
import org.sunbird.models.Message;
import org.sunbird.models.MessageCreator;
import org.sunbird.validator.MessageValidator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValue;

public class IndexerService {

  private static final String INDEX = ProjectUtil.EsIndex.sunbird.getIndexName();

  private static MessageCreator messageCreator = null;
  private static MessageValidator messageValidator = null;
  private final static String confFile = "indexer.conf";
  private static com.typesafe.config.Config config = null;
  private static Map<String, String> properties = null;
  private static ObjectMapper mapper = null;
  private Config appConfig = null;

  public IndexerService() {
    messageCreator = new MessageCreator();
    messageValidator = new MessageValidator();
    mapper = new ObjectMapper();
    initProps();
    ProjectLogger.log("IndexerService is intialised", LoggerEnum.INFO);
  }

  public void load(Config config) {
    JSONUtils.loadProperties(config);
    appConfig = config;
    ProjectUtil.propertiesCache.saveConfigProperty((JsonKey.SUNBIRD_ES_CLUSTER),
        appConfig.get(JsonKey.SUNBIRD_ES_CLUSTER));
    ProjectUtil.propertiesCache.saveConfigProperty((JsonKey.SUNBIRD_ES_IP),
        appConfig.get(JsonKey.SUNBIRD_ES_IP));
    ProjectUtil.propertiesCache.saveConfigProperty((JsonKey.SUNBIRD_ES_PORT),
        appConfig.get(JsonKey.SUNBIRD_ES_PORT));
  }

  private void initProps() {
    config = ConfigFactory.parseResources(confFile);
    properties = new HashMap<>();
    Set<Entry<String, ConfigValue>> configSet = config.entrySet();
    for (Entry<String, ConfigValue> confEntry : configSet) {
      properties.put(confEntry.getKey(), confEntry.getValue().unwrapped().toString());
    }
  }

  public void process(Map<String, Object> messageMap) {
    messageValidator.validateMessage(messageMap);

    Message message = messageCreator.getMessage(messageMap);

    String objectType = message.getObjectType();
    updateES(getIndex(objectType), getKey(objectType), message);
  }

  private void updateES(String type, String key, Message message) {
    Map<String, Object> properties = prepareData(message, type);
    if (!Constants.DELETE.equals(message.getOperationType())) {
      String identifier = (String) properties.get(key);
      ProjectLogger.log("IndexerService:updateES: Upsert data for identifier = " + identifier, LoggerEnum.INFO);
      ElasticSearchUtil.upsertData(INDEX, type, identifier, properties);
    } else {
      String identifier = (String) properties.get(key);
      ProjectLogger.log("IndexerService:updateES: Remove data for identifier = " + identifier, LoggerEnum.INFO);
      ElasticSearchUtil.removeData(INDEX, type, identifier);
    }
  }

  private Map<String, Object> prepareData(Message message, String type) {
    Map<String, Object> data = null;
    data = message.getProperties();
    return data;
  }

  private String getIndex(String objectType) {
    String key = objectType + Constants.DOT + Constants.INDEX;
    if (properties.containsKey(key)) {
      return properties.get(key);
    }
    return null;
  }

  private String getKey(String objectType) {
    String key = objectType + Constants.DOT + Constants.KEY;
    if (properties.containsKey(key)) {
      return properties.get(key);
    }
    return null;
  }
}
