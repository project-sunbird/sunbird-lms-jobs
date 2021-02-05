package org.sunbird.validator;

import java.text.MessageFormat;

import org.sunbird.exception.ProjectCommonException;
import org.sunbird.jobs.samza.common.ResponseCode;
import org.sunbird.jobs.samza.utils.StringFormatter;
import org.sunbird.models.Message;
import java.util.Map;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.models.Constants;

public class MessageValidator {

  public void validateMessage(Map<String, Object> message) {

    validate(message, Constants.IDENTIFIER);
    validate(message, Constants.OBJECT_TYPE);
    validate(message, Constants.OPERATION_TYPE);
    validate(message, Constants.EVENT_TYPE);
    validateEvent(message, Constants.EVENT);
    validateEventType(message);
    validateOperationType(message);
  }

  @SuppressWarnings("unchecked")
  private void validateEvent(Map<String, Object> message, String attribute) {
    if (!message.containsKey(attribute) || (!(message.get(attribute) instanceof Map))
        || (!MapUtils.isNotEmpty((Map<String, Object>) message.get(attribute)))) {
      throw new ProjectCommonException(ResponseCode.mandatoryParamsMissing.getErrorCode(),
          MessageFormat.format(ResponseCode.mandatoryParamsMissing.getErrorMessage(), attribute),
          ResponseCode.mandatoryParamsMissing.getResponseCode());
    }

  }

  private void validateEventType(Map<String, Object> message) {
    if (!Message.TRANSACTIONAL.equals(message.get(Constants.EVENT_TYPE))) {
      throw new ProjectCommonException(ResponseCode.dataTypeError.getErrorCode(),
          MessageFormat.format(ResponseCode.dataTypeError.getErrorMessage(), Message.TRANSACTIONAL),
          ResponseCode.dataTypeError.getResponseCode());
    }

  }

  private void validateOperationType(Map<String, Object> message) {
    if ((!Constants.UPSERT.equals(message.get(Constants.OPERATION_TYPE)))
        && (!Constants.DELETE.equals(message.get(Constants.OPERATION_TYPE)))) {
      throw new ProjectCommonException(ResponseCode.dataTypeError.getErrorCode(),
          MessageFormat.format(ResponseCode.dataTypeError.getErrorMessage(), Constants.OPERATION_TYPE,
              StringFormatter.joinByComma(Constants.UPSERT, Constants.DELETE)),
          ResponseCode.dataTypeError.getResponseCode());
    }

  }

  public void validate(Map<String, Object> message, String attribute) {
    if (!message.containsKey(attribute) || StringUtils.isBlank((String) message.get(attribute))) {
      throw new ProjectCommonException(ResponseCode.mandatoryParamsMissing.getErrorCode(),
          MessageFormat.format(ResponseCode.mandatoryParamsMissing.getErrorMessage(), attribute),
          ResponseCode.mandatoryParamsMissing.getResponseCode());
    }
  }

}
