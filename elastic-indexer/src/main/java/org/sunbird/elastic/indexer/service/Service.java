package org.sunbird.elastic.indexer.service;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.datasecurity.DecryptionService;
import org.sunbird.common.models.util.datasecurity.EncryptionService;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.elastic.indexer.models.Message;
import org.sunbird.elastic.indexer.util.ElasticSearchIndexerUtil;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author iostream04
 *
 */
public class Service {
  private static ObjectMapper mapper;

  private static Map<String, String> keyMap;
  private static Map<String, String> typeMap;
  private static Map<String, String> tableKeyMap;
  private static final String ORGANISATION = "organisation";
  private static final String ORG_EXTERNAL_IDENTITY = "org_external_identity";
  private static final String LOCATION = "location";
  private static final String DELETE_ROW = "DELETE_ROW";
  private static final String INDEX = ProjectUtil.EsIndex.sunbird.getIndexName();
  private static EncryptionService encryptionService = org.sunbird.common.models.util.datasecurity.impl.ServiceFactory
      .getEncryptionServiceInstance(null);
  private static DecryptionService decService = org.sunbird.common.models.util.datasecurity.impl.ServiceFactory
      .getDecryptionServiceInstance(null);
  private static List<String> userTypeMapping = null;

  public Service() {

    mapper = new ObjectMapper();
    keyMap = new HashMap<>();
    typeMap = new HashMap<>();
    userTypeMapping = Arrays.asList("user", "usr_external_identity", "address", "user_education", "user_job_profile",
        "user_org", "user_badge_assertion", "user_skills", "user_courses");
    typeMap.put(ORG_EXTERNAL_IDENTITY, ORGANISATION);
    typeMap.put(JsonKey.ORGANISATION, JsonKey.ORGANISATION);
    typeMap.put(JsonKey.USER, JsonKey.USER);
    typeMap.put("usr_external_identity", JsonKey.USER);
    typeMap.put("address", JsonKey.USER);
    typeMap.put("user_education", JsonKey.USER);
    typeMap.put("user_job_profile", JsonKey.USER);
    typeMap.put("user_org", JsonKey.USER);
    typeMap.put("user_badge_assertion", JsonKey.USER);
    typeMap.put("user_skills", JsonKey.USER);
    typeMap.put("user_courses", "usercourses");

    keyMap.put(ORG_EXTERNAL_IDENTITY, "orgid");
    keyMap.put(JsonKey.ORGANISATION, JsonKey.ORGANISATION_ID);
    keyMap.put(JsonKey.LOCATION, JsonKey.ID);
    keyMap.put(JsonKey.USER, JsonKey.ID);
    keyMap.put("usr_external_identity", JsonKey.USER_ID);
    keyMap.put("address", JsonKey.USER_ID);
    keyMap.put("user_education", JsonKey.USER_ID);
    keyMap.put("user_job_profile", JsonKey.USER_ID);
    keyMap.put("user_org", JsonKey.USER_ID);
    keyMap.put("user_badge_assertion", JsonKey.USER_ID);
    keyMap.put("user_skills", JsonKey.USER_ID);
    keyMap.put("user_courses", JsonKey.USER_ID);
    keyMap.put("course_batch", JsonKey.ID);

    tableKeyMap.put(JsonKey.USER_COURSES, "batchId");

  }

  public void process(String messageString) {
    Message message = getMessage(messageString);
    Map<String, String> esTypesToUpdate = getEsTypeToUpdate(message);
    System.out.println(message.toString());
    updateToEs(esTypesToUpdate, message);
  }

  private Map<String, String> getEsTypeToUpdate(Message message) {
    Map<String, String> esTypesToUpdate = new HashMap<>();
    String table = message.getTable();
    if (table.equalsIgnoreCase("organisation")) {
      esTypesToUpdate.put("organisation", getPrimaryKey(message));
    } else if (table.equalsIgnoreCase("org_external_identity")) {
      esTypesToUpdate.put("organisation", getPrimaryKey(message));
    } else if (table.equalsIgnoreCase("location")) {
      esTypesToUpdate.put("location", getPrimaryKey(message));
    } else if (userTypeMapping.contains(table)) {
      esTypesToUpdate.put("user", getPrimaryKey(message));
    } else if (table.equalsIgnoreCase("course_batch")) {
      esTypesToUpdate.put("cbatch", getPrimaryKey(message));
    }
    if (table.equalsIgnoreCase("user_courses")) {
      esTypesToUpdate.put("usercourses", "id");
    }
    return esTypesToUpdate;
  }

  private void updateToEs(Map<String, String> esTypesToUpdate, Message message) {
    Set<String> keys = esTypesToUpdate.keySet();
    for (String key : keys) {
      String type = key;
      Map<String, Object> properties = prepareData(message, type);
      String identifier = (String) properties.get(esTypesToUpdate.get(key));
      ElasticSearchIndexerUtil.upsertData(INDEX, type, identifier, properties);
    }
  }

  private Map<String, Object> prepareData(Message message, String type) {
    Map<String, Object> data = null;
    if (type.equalsIgnoreCase(ORGANISATION)) {
      data = prepareOrgData(message.getProperties());
    } else if (type.equalsIgnoreCase(JsonKey.USER)) {
      data = prepareUserData(message);
    } else {
      data = message.getProperties();
    }
    return data;
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> prepareUserData(Message message) {
    Map<String, Object> data = null;
    if (message.getTable().equals(JsonKey.USER)) {
      data = message.getProperties();
    } else {
      String table = message.getTable();
      if (table.equalsIgnoreCase("usr_external_identity")) {
        String userId = (String) data.get(JsonKey.USER_ID);
        Map<String, Object> esMap = ElasticSearchIndexerUtil.getDataByIdentifier(INDEX, JsonKey.USER, userId);
        esMap.put(JsonKey.PROVIDER, data.get(JsonKey.PROVIDER));
        esMap.put(JsonKey.EXTERNAL_ID, data.get(JsonKey.EXTERNAL_ID));
      } else if (table.equalsIgnoreCase("address")) {
        String userId = (String) data.get(JsonKey.USER_ID);
        userId = decryptData(userId);
        Map<String, Object> esMap = ElasticSearchIndexerUtil.getDataByIdentifier(INDEX, JsonKey.USER, userId);
        data = updateNestedData(data, esMap, JsonKey.ADDRESS, message.getOperationType());
      } else if (table.equalsIgnoreCase("user_education")) {
        String userId = (String) data.get(JsonKey.USER_ID);
        Map<String, Object> esMap = ElasticSearchIndexerUtil.getDataByIdentifier(INDEX, JsonKey.USER, userId);
        data = updateNestedData(data, esMap, JsonKey.EDUCATION, message.getOperationType());

      } else if (table.equalsIgnoreCase("user_job_profile")) {
        String userId = (String) data.get(JsonKey.USER_ID);
        Map<String, Object> esMap = ElasticSearchIndexerUtil.getDataByIdentifier(INDEX, JsonKey.USER, userId);
        data = updateNestedData(data, esMap, JsonKey.JOB_PROFILE, message.getOperationType());

      } else if (table.equalsIgnoreCase("user_org")) {
        String userId = (String) data.get(JsonKey.USER_ID);
        Map<String, Object> esMap = ElasticSearchIndexerUtil.getDataByIdentifier(INDEX, JsonKey.USER, userId);
        data = updateNestedData(data, esMap, JsonKey.ORGANISATIONS, message.getOperationType());
      } else if (table.equalsIgnoreCase("user_badge_assertion")) {
        String userId = (String) data.get(JsonKey.USER_ID);
        Map<String, Object> esMap = ElasticSearchIndexerUtil.getDataByIdentifier(INDEX, JsonKey.USER, userId);
        esMap.put(JsonKey.BADGE_ASSERTIONS, data);
        data = esMap;
      } else if (table.equalsIgnoreCase("user_skills")) {
        String userId = (String) data.get(JsonKey.USER_ID);
        Map<String, Object> esMap = ElasticSearchIndexerUtil.getDataByIdentifier(INDEX, JsonKey.USER, userId);
        data = updateNestedData(data, esMap, JsonKey.SKILLS, message.getOperationType());
      } else if (table.equalsIgnoreCase("user_courses")) {
        String userId = (String) data.get(JsonKey.USER_ID);
        Map<String, Object> esMap = ElasticSearchIndexerUtil.getDataByIdentifier(INDEX, JsonKey.USER, userId);
        data = addUserCourses(data);
        data = updateNestedData(data, esMap, JsonKey.USER_COURSES, message.getOperationType());
      }
    }
    return data;
  }

  private Map<String, Object> addUserCourses(Map<String, Object> data) {
    Map<String, Object> tempMap = new HashMap<>();
    tempMap.put(JsonKey.ENROLLED_ON, data.get(JsonKey.COURSE_ENROLL_DATE));
    tempMap.put(JsonKey.COURSE_ID, data.get(JsonKey.COURSE_ID));
    tempMap.put(JsonKey.BATCH_ID, data.get(JsonKey.BATCH_ID));
    tempMap.put(JsonKey.PROGRESS, data.get(JsonKey.PROGRESS));
    tempMap.put(JsonKey.LAST_ACCESSED_ON, data.get(JsonKey.DATE_TIME));
    return tempMap;
  }

  @SuppressWarnings("unchecked")
  public Map<String, Object> updateNestedData(Map<String, Object> data, Map<String, Object> esMap, String attribute,
      String operationType) {
    if (esMap.get(attribute) != null) {
      boolean isAlreadyPresent = updateIfAlreadyExist((List) esMap.get(attribute), data, attribute, operationType);
      if (!isAlreadyPresent) {
        ((List) esMap.get(attribute)).add(data);
      }

    } else {
      List<Map<String, Object>> list = new ArrayList<>();
      list.add(data);
      esMap.put(attribute, list);
      data = esMap;
    }
    return data;
  }

  private boolean updateIfAlreadyExist(List<Map<String, Object>> esDataList, Map<String, Object> data, String attribute,
      String operationType) {
    for (Map<String, Object> esData : esDataList) {
      String id = tableKeyMap.getOrDefault(attribute, JsonKey.ID);
      if (esData.get(id).equals(data.get(id))) {
        if (operationType.equalsIgnoreCase(DELETE_ROW)
            || (data.get(JsonKey.IS_DELETED) != null && (boolean) data.get(JsonKey.IS_DELETED))) {
          esDataList.remove(esData);
        } else {
          esData = data;
        }
        return true;
      }
    }
    return false;
  }

  private String decryptData(String value) {
    return decService.decryptData(value);
  }

  private String encryptData(String value) {
    try {
      return encryptionService.encryptData(value);
    } catch (Exception e) {
      throw new ProjectCommonException(ResponseCode.userDataEncryptionError.getErrorCode(),
          ResponseCode.userDataEncryptionError.getErrorMessage(), ResponseCode.SERVER_ERROR.getResponseCode());
    }
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> prepareOrgData(Map<String, Object> map) {
    String contactDetails = (String) map.get(JsonKey.CONTACT_DETAILS);
    if (StringUtils.isNotBlank(contactDetails)) {
      Object[] arr;
      try {
        arr = mapper.readValue(contactDetails, Object[].class);
        map.put(JsonKey.CONTACT_DETAILS, arr);
      } catch (IOException e) {
        map.put(JsonKey.CONTACT_DETAILS, new Object[] {});
        ProjectLogger.log(e.getMessage(), e);
      }
    } else {
      map.put(JsonKey.CONTACT_DETAILS, new Object[] {});
    }

    if (MapUtils.isNotEmpty((Map<String, Object>) map.get(JsonKey.ADDRESS))) {
      map.put(JsonKey.ADDRESS, map.get(JsonKey.ADDRESS));
    } else {
      map.put(JsonKey.ADDRESS, new HashMap<>());
    }

    return map;
  }

  @SuppressWarnings("unchecked")
  private Message getMessage(String messageString) {

    Message message = new Message();
    JsonNode res = null;
    try {
      res = mapper.readTree(messageString);
    } catch (IOException e) {
      System.out.println("error while reading message :" + messageString);
      e.printStackTrace();
    }
    message.setEts(res.get("ets").asLong());
    message.setEventType(res.get("eventType").asText());
    message.setIdentifier(res.get("ets"));
    message.setKeyspace(res.get("keyspace").asText());
    message.setOperationType(res.get("operationType").asText());
    message.setProperties(mapper.convertValue(res.get("properties"), Map.class));
    message.setTable(res.get("table").asText());
    return message;
  }

  private String getPrimaryKey(Message message) {
    String key = keyMap.get(message.getTable());
    if (key != null) {
      return key;
    }
    ProjectCommonException.throwClientErrorException(ResponseCode.SERVER_ERROR);
    return null;
  }

}
