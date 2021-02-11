package org.sunbird.exception;

/**
 * This exception will be used across all backend code. This will send status code and error message
 *
 * @author Manzarul.Haque
 */
public class ProjectCommonException extends RuntimeException {

  /** serialVersionUID. */
  private static final long serialVersionUID = 1L;
  /** code String code ResponseCode. */
  private String code;
  /** message String ResponseCode. */
  private String message;
  /** responseCode int ResponseCode. */
  private int responseCode;
  
  /**
   * This code is for client to identify the error and based on that do the message localization.
   *
   * @return String
   */
  public String getCode() {
    return code;
  }

  /**
   * message for client in english.
   *
   * @return String
   */
  @Override
  public String getMessage() {
    return message;
  }

  /** @param message String */
  public void setMessage(String message) {
    this.message = message;
  }

  /**
   * three argument constructor.
   *
   * @param code String
   * @param message String
   * @param responseCode int
   */
  public ProjectCommonException(String code, String message, int responseCode) {
    super();
    this.code = code;
    this.message = message;
    this.responseCode = responseCode;
  }

  
}
