package org.sunbird.validator;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.sunbird.exception.ProjectCommonException;
import org.sunbird.jobs.samza.common.ResponseCode;
import org.sunbird.models.Constants;
import org.sunbird.models.Message;

import org.junit.Assert;

public class MessageValidatorTest {

  @Test
  public void testValidateMessageSuccess() {
    Map<String, Object> messageMap = createMessageMap();
    ProjectCommonException error = null;
    MessageValidator validator = new MessageValidator();

    try {
      validator.validateMessage(messageMap);
    } catch (ProjectCommonException e) {
      error = e;
    }
    
    Assert.assertTrue(error == null);
  }

  @Test
  public void testValidateMessageFailureWithoutObjectType() {
    Map<String, Object> messageMap = createMessageMap();
    messageMap.remove(Constants.OBJECT_TYPE);
    ProjectCommonException error = null;

    MessageValidator validator = new MessageValidator();
    try {
      validator.validateMessage(messageMap);
    } catch (ProjectCommonException e) {
      error = e;
    }

    Assert.assertTrue(error.getCode().equals(ResponseCode.mandatoryParamsMissing.getErrorCode()));
  }

  @Test
  public void testMessageValidationFailureWithoutOperationType() {
    Map<String, Object> messageMap = createMessageMap();
    messageMap.remove(Constants.OPERATION_TYPE);
    ProjectCommonException error = null;

    MessageValidator validator = new MessageValidator();
    try {
      validator.validateMessage(messageMap);
    } catch (ProjectCommonException e) {
      error = e;
    }

    Assert.assertTrue(error.getCode().equals(ResponseCode.mandatoryParamsMissing.getErrorCode()));
  }

  @Test
  public void testMessageValidationFailureWithoutEvent() {
    Map<String, Object> messageMap = createMessageMap();
    messageMap.remove(Constants.EVENT);
    ProjectCommonException error = null;

    MessageValidator validator = new MessageValidator();
    try {
      validator.validateMessage(messageMap);
    } catch (ProjectCommonException e) {
      error = e;
    }

    Assert.assertTrue(error.getCode().equals(ResponseCode.mandatoryParamsMissing.getErrorCode()));
  }

  @Test
  public void testMessageValidationFailureWithoutIdentifier() {
    Map<String, Object> messageMap = createMessageMap();
    messageMap.remove(Constants.IDENTIFIER);
    ProjectCommonException error = null;
    MessageValidator validator = new MessageValidator();

    try {
      validator.validateMessage(messageMap);
    } catch (ProjectCommonException e) {
      error = e;
    }

    Assert.assertTrue(error.getCode().equals(ResponseCode.mandatoryParamsMissing.getErrorCode()));
  }

  @Test
  public void testMessageValidationFailureWithoutEventType() {
    Map<String, Object> messageMap = createMessageMap();
    messageMap.remove(Constants.EVENT_TYPE);
    ProjectCommonException error = null;
    MessageValidator validator = new MessageValidator();

    try {
      validator.validateMessage(messageMap);
    } catch (ProjectCommonException e) {
      error = e;
    }

    Assert.assertTrue(error.getCode().equals(ResponseCode.mandatoryParamsMissing.getErrorCode()));
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
    id.put(Constants.NV, "1234567");
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
