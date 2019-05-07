package org.sunbird.jobs.samza.service;

import static org.powermock.api.mockito.PowerMockito.when;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.jobs.samza.service.IndexerService;
import org.sunbird.models.Constants;
import org.sunbird.models.Message;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ ElasticSearchUtil.class, })
@PowerMockIgnore({ "javax.management.*" })
public class IndexerServiceTest {

  @Before
  public void beforeEachTest() {
    PowerMockito.mockStatic(ElasticSearchUtil.class);
  }

  @Test
  public void testProcessSuccessForUpsert() {
    Map<String, Object> messageMap = createMessageMap();

    IndexerService service = new IndexerService();
    ProjectCommonException error = null;

    try {
      service.process(messageMap);
    } catch (ProjectCommonException e) {
      error = e;
    }

    Assert.assertTrue(error == null);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testProcessFailureForUpsert() {
    when(ElasticSearchUtil.upsertData(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
        .thenThrow(throwException());

    Map<String, Object> messageMap = createMessageMap();

    IndexerService service = new IndexerService();
    ProjectCommonException error = null;

    try {
      service.process(messageMap);
    } catch (ProjectCommonException e) {
      error = e;
    }

    Assert.assertTrue(error != null);
  }

  @Test
  public void testProcessSuccessForOrgUpsert() {
    Map<String, Object> messageMap = createMessageMapForOrg(createMessageMap());

    IndexerService service = new IndexerService();
    ProjectCommonException error = null;

    try {
      service.process(messageMap);
    } catch (ProjectCommonException e) {
      error = e;
    }

    Assert.assertTrue(error == null);
  }

  @Test
  public void testProcessSuccessForDelete() {
    Map<String, Object> messageMap = createMessageMap();
    messageMap.put(Constants.OPERATION_TYPE, Constants.DELETE);

    IndexerService service = new IndexerService();
    ProjectCommonException error = null;

    try {
      service.process(messageMap);
    } catch (ProjectCommonException e) {
      error = e;
    }

    Assert.assertTrue(error == null);
  }

  @Test
  public void testProcessFailureForDelete() {
    when(ElasticSearchUtil.removeData(Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
        .thenThrow(throwException());

    Map<String, Object> messageMap = createMessageMap();
    messageMap.put(Constants.OPERATION_TYPE, Constants.DELETE);

    IndexerService service = new IndexerService();
    ProjectCommonException error = null;

    try {
      service.process(messageMap);
    } catch (ProjectCommonException e) {
      error = e;
    }

    Assert.assertTrue(error != null);
  }

  private Map<String, Object> createMessageMap() {
    Map<String, Object> messageMap = new HashMap<>();
    messageMap.put(Constants.IDENTIFIER, "1234567");
    messageMap.put(Constants.OPERATION_TYPE, Constants.UPSERT);
    messageMap.put(Constants.EVENT_TYPE, Message.TRANSACTIONAL);
    messageMap.put(Constants.OBJECT_TYPE, Constants.LOCATION);

    Map<String, Object> event = new HashMap<>();
    Map<String, Object> properties = new HashMap<>();
    Map<String, Object> name = new HashMap<>();
    Map<String, Object> id = new HashMap<>();

    name.put(Constants.NV, "BLR");
    id.put(Constants.NV, "0001");
    properties.put("name", name);
    properties.put("id", id);
    event.put("properties", properties);
    messageMap.put(Constants.EVENT, event);
    messageMap.put(Constants.ETS, 123456L);
    messageMap.put(Constants.USER_ID, "ANONYMOUS");
    messageMap.put(Constants.CREATED_ON, "1556018741532");

    return messageMap;
  }

  private Map<String, Object> createMessageMapForOrg(Map<String, Object> messageMap) {
    messageMap.put(Constants.OBJECT_TYPE, Constants.ORGANISATION);
    Map<String, Object> event = new HashMap<>();
    Map<String, Object> properties = new HashMap<>();
    Map<String, Object> name = new HashMap<>();
    Map<String, Object> id = new HashMap<>();
    name.put(Constants.NV, "BLR-org");
    id.put(Constants.NV, "0001");
    properties.put("orgName", name);
    properties.put("id", id);
    event.put("properties", properties);
    messageMap.put(Constants.EVENT, event);
    return messageMap;
  }
  
  private ProjectCommonException throwException() {
    return new ProjectCommonException("", "", 0);
  }
}
