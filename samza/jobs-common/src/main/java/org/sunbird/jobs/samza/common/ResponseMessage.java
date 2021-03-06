package org.sunbird.jobs.samza.common;

public interface ResponseMessage {
    interface Message {
        String UNAUTHORIZED_USER = "You are not authorized.";
        String INVALID_ORG_DATA = "Given organization doesn't exist.";
        String MANDATORY_PARAMETER_MISSING = "Mandatory parameter {0} is missing.";
    }

    interface Key {
        String UNAUTHORIZED_USER = "UNAUTHORIZED_USER";
        String INVALID_ORG_DATA = "INVALID_ORGANIZATION_DATA";
        String MANDATORY_PARAMETER_MISSING = "MANDATORY_PARAMETER_MISSING";
    }
}
