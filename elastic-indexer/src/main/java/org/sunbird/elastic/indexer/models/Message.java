package org.sunbird.elastic.indexer.models;

import java.util.Map;

/**
 * @author iostream04
 *
 */
public class Message {

  private String table;
  private String eventType;
  private String operationType;
  private long ets;
  private String keyspace;
  private Object identifier;
  private Map<String, Object> properties;

  public String getTable() {
    return table;
  }

  public void setTable(String table) {
    this.table = table;
  }

  public String getEventType() {
    return eventType;
  }

  public void setEventType(String eventType) {
    this.eventType = eventType;
  }

  public String getOperationType() {
    return operationType;
  }

  public void setOperationType(String operationType) {
    this.operationType = operationType;
  }

  public long getEts() {
    return ets;
  }

  public void setEts(long ets) {
    this.ets = ets;
  }

  public String getKeyspace() {
    return keyspace;
  }

  public void setKeyspace(String keyspace) {
    this.keyspace = keyspace;
  }

  public Object getIdentifier() {
    return identifier;
  }

  public void setIdentifier(Object identifier) {
    this.identifier = identifier;
  }

  public Map<String, Object> getProperties() {
    return properties;
  }

  public void setProperties(Map<String, Object> properties) {
    this.properties = properties;
  }

  @Override
  public String toString() {
    return "Message [table=" + table + ", eventType=" + eventType + ", operationType=" + operationType + ", ets=" + ets
        + ", keyspace=" + keyspace + ", identifier=" + identifier + ", properties=" + properties + "]";
  }

}
