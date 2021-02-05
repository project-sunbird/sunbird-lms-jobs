package org.sunbird.jobs.samza.common;

import org.apache.commons.lang3.StringUtils;
import org.sunbird.models.Constants;

public enum ResponseCode {
    unAuthorized(ResponseMessage.Key.UNAUTHORIZED_USER, ResponseMessage.Message.UNAUTHORIZED_USER),
    invalidOrgData(ResponseMessage.Key.INVALID_ORG_DATA, ResponseMessage.Message.INVALID_ORG_DATA),
    dataTypeError(ResponseMessage.Key.DATA_TYPE_ERROR, ResponseMessage.Message.DATA_TYPE_ERROR),
    mandatoryParamsMissing(ResponseMessage.Key.MANDATORY_PARAMETER_MISSING, ResponseMessage.Message.MANDATORY_PARAMETER_MISSING),

    OK(200),
    CLIENT_ERROR(400),
    SERVER_ERROR(500),
    RESOURCE_NOT_FOUND(404),
    UNAUTHORIZED(401),
    FORBIDDEN(403),
    REDIRECTION_REQUIRED(302),
    TOO_MANY_REQUESTS(429);

    private int responseCode;
    private String errorCode;
    private String errorMessage;

    private ResponseCode(String errorCode, String errorMessage) {
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
    }

    private ResponseCode(String errorCode, String errorMessage, int responseCode) {
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
        this.responseCode = responseCode;
    }

    public String getMessage(int errorCode) {
        return "";
    }

    public String getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(String errorCode) {
        this.errorCode = errorCode;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

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

    public void setResponseCode(int responseCode) {
        this.responseCode = responseCode;
    }

    public static ResponseCode getHeaderResponseCode(int code) {
        if (code > 0) {
            try {
                ResponseCode[] arr = ResponseCode.values();
                if (null != arr) {
                    for (ResponseCode rc : arr) {
                        if (rc.getResponseCode() == code) return rc;
                    }
                }
            } catch (Exception e) {
                return ResponseCode.SERVER_ERROR;
            }
        }
        return ResponseCode.SERVER_ERROR;
    }

    public static ResponseCode getResponse(String errorCode) {
        if (StringUtils.isBlank(errorCode)) {
            return null;
        } else if (Constants.UNAUTHORIZED.equals(errorCode)) {
            return ResponseCode.unAuthorized;
        } else {
            ResponseCode value = null;
            ResponseCode responseCodes[] = ResponseCode.values();
            for (ResponseCode response : responseCodes) {
                if (response.getErrorCode().equals(errorCode)) {
                    return response;
                }
            }
            return value;
        }
    }
}
