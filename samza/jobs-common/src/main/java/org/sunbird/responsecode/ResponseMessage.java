package org.sunbird.responsecode;

/**
 * This interface will hold all the response key and message
 *
 * @author Manzarul
 */
public interface ResponseMessage {

  interface Message {
    String DATA_TYPE_ERROR = "Data type of {0} should be {1}.";
    String MANDATORY_PARAMETER_MISSING = "Mandatory parameter {0} is missing.";
  }

  interface Key {
    String DATA_TYPE_ERROR = "DATA_TYPE_ERROR";
    String MANDATORY_PARAMETER_MISSING = "MANDATORY_PARAMETER_MISSING";
  }
}
