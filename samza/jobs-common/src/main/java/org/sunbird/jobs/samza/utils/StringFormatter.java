package org.sunbird.jobs.samza.utils;

/**
 * Helper class for String formatting operations.
 *
 * @author Amit Kumar
 */
public class StringFormatter {

  public static final String DOT = ".";
  public static final String AND = " and ";
  public static final String OR = " or ";
  public static final String COMMA = ", ";

  private StringFormatter() {}

  /**
   * Helper method to construct and formatted string.
   *
   * @param params One or more strings to be joined by comma
   * @return and formatted string
   */
  public static String joinByComma(String... params) {
    return String.join(COMMA, params);
  }
}
