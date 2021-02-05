package org.sunbird.responsecode;

import org.apache.commons.lang3.StringUtils;

/** @author Manzarul */
public enum ResponseCode {
  
  dataTypeError(ResponseMessage.Key.DATA_TYPE_ERROR, ResponseMessage.Message.DATA_TYPE_ERROR),
  
  mandatoryParamsMissing(
      ResponseMessage.Key.MANDATORY_PARAMETER_MISSING,
      ResponseMessage.Message.MANDATORY_PARAMETER_MISSING),
  
  OK(200),
  CLIENT_ERROR(400),
  SERVER_ERROR(500),
  RESOURCE_NOT_FOUND(404),
  UNAUTHORIZED(401),
  FORBIDDEN(403),
  REDIRECTION_REQUIRED(302),
  TOO_MANY_REQUESTS(429),
  SERVICE_UNAVAILABLE(503),
  PARTIAL_SUCCESS_RESPONSE(206),
  IM_A_TEAPOT(418);
  private int responseCode;
  /** error code contains String value */
  private String errorCode;
  /** errorMessage contains proper error message. */
  private String errorMessage;

  /**
   * @param errorCode String
   * @param errorMessage String
   */
  private ResponseCode(String errorCode, String errorMessage) {
    this.errorCode = errorCode;
    this.errorMessage = errorMessage;
  }

  /**
   * @param errorCode
   * @return
   */
  public String getMessage(int errorCode) {
    return "";
  }

  /** @return */
  public String getErrorCode() {
    return errorCode;
  }

  /** @param errorCode */
  public void setErrorCode(String errorCode) {
    this.errorCode = errorCode;
  }

  /** @return */
  public String getErrorMessage() {
    return errorMessage;
  }

  /** @param errorMessage */
  public void setErrorMessage(String errorMessage) {
    this.errorMessage = errorMessage;
  }

  /**
   * This method will provide status message based on code
   *
   * @param code
   * @return String
   */
  public static String getResponseMessage(String code) {
    if (StringUtils.isBlank(code)) {
      return "";
    }
    ResponseCode responseCodes[] = ResponseCode.values();
    for (ResponseCode actionState : responseCodes) {
      if (actionState.getErrorCode().equals(code)) {
        return actionState.getErrorMessage();
      }
    }
    return "";
  }

  private ResponseCode(int responseCode) {
    this.responseCode = responseCode;
  }

  public int getResponseCode() {
    return responseCode;
  }

 
}
