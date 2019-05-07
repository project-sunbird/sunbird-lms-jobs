package org.sunbird.models;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import org.sunbird.models.Constants;
import org.sunbird.models.Message;
import org.sunbird.models.MessageCreator;

public class MessageCreatorTest {

  @Test
  public void testGetMessageSuccess() {
    MessageCreator creator = new MessageCreator();
    Map<String, Object> messageMap = createMessageMap();
    Message message = creator.getMessage(messageMap);
    Assert.assertTrue(message.getCreatedOn().equals(messageMap.get(Constants.CREATED_ON)));
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
}
