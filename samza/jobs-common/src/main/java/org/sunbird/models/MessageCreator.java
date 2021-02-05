package org.sunbird.models;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.sunbird.jobs.samza.utils.PropertyReader;

public class MessageCreator {
  private static final PropertyReader propertiesCache = PropertyReader.getInstance();

  @SuppressWarnings("unchecked")
  public Message getMessage(Map<String, Object> res) {

    Message message = new Message();

    message.setEts((Long) res.get(Constants.ETS));
    message.setEventType((String) res.get(Constants.EVENT_TYPE));
    message.setIdentifier((String) res.get(Constants.IDENTIFIER));
    message.setOperationType((String) res.get(Constants.OPERATION_TYPE));
    Map<String, Object> event = (Map<String, Object>) res.get(Constants.EVENT);
    Map<String, Object> props = (Map<String, Object>) event.get(Constants.PROPERTIES);
    message.setProperties(getSimpleProperties(props));

    message.setObjectType((String) res.get(Constants.OBJECT_TYPE));
    message.setUserId((String) res.get(Constants.USER_ID));
    message.setCreatedOn((String) res.get(Constants.CREATED_ON));
    return message;
  }

  private Map<String, Object> getSimpleProperties(Map<String, Object> props) {
    Map<String, Object> properties = new HashMap<>();
    Set<String> keys = props.keySet();
    for (String key : keys) {
      Map<String, Object> value = (Map<String, Object>) props.get(key);
      Object obj = null;
      if (value != null) {
        obj = value.get(Constants.NV);
        String elasticKey = propertiesCache.readProperty(key);
        properties.put(elasticKey, obj);
      }
    }
    return properties;
  }

}
