package org.sunbird.elastic.indexer.util;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;

/**
 * @author iostream04
 *
 */
public class ConnectionClient {
  @SuppressWarnings({ "resource" })
  public Client createConnection() {
    Client client = null;
    try {
      client = new PreBuiltTransportClient(
          Settings.builder().put("client.transport.sniff", true).put("cluster.name", "elasticsearch").build())
              .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9300));
    } catch (UnknownHostException e) {
      ProjectLogger.log("ConnectionClient : error wjile creating connection",LoggerEnum.INFO);
      e.printStackTrace();
    }
    return client;

  }

}
