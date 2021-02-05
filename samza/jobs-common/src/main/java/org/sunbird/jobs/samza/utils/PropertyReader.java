package org.sunbird.jobs.samza.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * This class will be used to read cassandratablecolumn properties file.
 *
 * @author Amit Kumar
 */
public class PropertyReader {
  private JobLogger logger = new JobLogger(PropertyReader.class);

  private final Properties properties = new Properties();
  private static final String file = "cassandratablecolumn.properties";
  private static PropertyReader propertyReader = null;

  /** private default constructor */
  private PropertyReader() {
    InputStream in = this.getClass().getClassLoader().getResourceAsStream(file);
    try {
      properties.load(in);
    } catch (IOException e) {
      logger.error("Error in properties cache", e);
    }
  }

  public static PropertyReader getInstance() {
    if (null == propertyReader) {
      synchronized (PropertyReader.class) {
        if (null == propertyReader) {
          propertyReader = new PropertyReader();
        }
      }
    }
    return propertyReader;
  }

  /**
   * Method to read value from resource file .
   *
   * @param key property value to read
   * @return value corresponding to given key if found else will return key itself.
   */
  public String readProperty(String key) {
    return properties.getProperty(key) != null ? properties.getProperty(key) : key;
  }
}
