package org.sunbird.jobs.samza.util;

import org.apache.commons.lang3.StringUtils;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.jobs.samza.common.ResponseCode;

import java.text.MessageFormat;
import java.util.Map;

public class SSOAccountUpdaterMessageValidator {

    public void validateMessage(Map<String, Object> message, String[] mandatoryParams) {
        for (String param : mandatoryParams) {
            validateField(param, message);
        }
    }

    private void validateField(String param, Map<String, Object> message) {
        if (StringUtils.isBlank((String) message.get(param))) {
            throwMandatoryParamsMissingException(param);
        }
    }

    private void throwMandatoryParamsMissingException(String fieldName) {
        throw new ProjectCommonException(
                ResponseCode.mandatoryParamsMissing.getErrorCode(),
                MessageFormat.format(ResponseCode.mandatoryParamsMissing.getErrorMessage(), fieldName),
                ResponseCode.CLIENT_ERROR.getResponseCode());
    }
}
