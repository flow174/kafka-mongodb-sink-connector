package com.beck.kafka.sink.connector;

import static com.beck.kafka.sink.connector.Constants.CONNECTOR_CLASS;
import static com.beck.kafka.sink.connector.Constants.CONNECTOR_CLASS_DESCRIPTION;
import static com.beck.kafka.sink.connector.Constants.MONGODB_COLLECTION_NAME;
import static com.beck.kafka.sink.connector.Constants.MONGODB_COLLECTION_NAME_DESCRIPTION;
import static com.beck.kafka.sink.connector.Constants.MONGODB_CONNECTION_STRING;
import static com.beck.kafka.sink.connector.Constants.MONGODB_CONNECTION_STRING_DESCRIPTION;
import static com.beck.kafka.sink.connector.Constants.MONGODB_DATABASE_NAME;
import static com.beck.kafka.sink.connector.Constants.MONGODB_DATABASE_NAME_DESCRIPTION;
import static com.beck.kafka.sink.connector.Constants.NAME;
import static com.beck.kafka.sink.connector.Constants.NAME_DESCRIPTION;
import static com.beck.kafka.sink.connector.Constants.TASKS_MAX;
import static com.beck.kafka.sink.connector.Constants.TASKS_MAX_DESCRIPTION;
import static com.beck.kafka.sink.connector.Constants.TOPICS;
import static com.beck.kafka.sink.connector.Constants.TOPICS_DESCRIPTION;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongodbSinkConnector extends SinkConnector {

  private static final Logger log = LoggerFactory.getLogger(MongodbSinkConnector.class);

  Map<String, String> configProperties;

  /**
   * This method is called after the connector is instantiated for the first time or terminated (the connector will automatically listen to port 8083
   * after it is started, by sending a put request to PUT /connectors/{name}/pause, see the official documentation for details)
   *
   * @param map our configuration
   */
  @Override
  public void start(Map<String, String> map) {
    configProperties = map;
  }

  /**
   * get sink task class
   *
   * @return MongodbSinkTask.class
   */
  @Override
  public Class<? extends Task> taskClass() {
    return MongodbSinkTask.class;
  }

  /**
   * configure sink task
   *
   * @param maxTasks mask tasks
   * @return configuration list
   */
  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    ArrayList<Map<String, String>> configs = new ArrayList<>();
    Map<String, String> config = new HashMap<>(configProperties);
    for (int i = 0; i < maxTasks; i++) {
      configs.add(config);
    }
    return configs;
  }

  /**
   * stop connector
   */
  @Override
  public void stop() {
    log.info("connector stop");
  }

  /**
   * get ConfigDef. When extending SinkTask, we will use the CONFIG_DEF to parse the incoming configuration item. Moreover, in the runtime will also
   * verify the configuration by calling the config method.
   *
   * @return ConfigDef
   */
  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  static final ConfigDef CONFIG_DEF = new ConfigDef()
    .define(NAME, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, NAME_DESCRIPTION)
    .define(CONNECTOR_CLASS, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, CONNECTOR_CLASS_DESCRIPTION)
    .define(TASKS_MAX, ConfigDef.Type.INT, ConfigDef.Importance.HIGH, TASKS_MAX_DESCRIPTION)
    .define(TOPICS, ConfigDef.Type.LIST, ConfigDef.Importance.HIGH, TOPICS_DESCRIPTION)
    .define(MONGODB_CONNECTION_STRING, Type.STRING, ConfigDef.Importance.HIGH, MONGODB_CONNECTION_STRING_DESCRIPTION)
    .define(MONGODB_DATABASE_NAME, Type.STRING, ConfigDef.Importance.HIGH, MONGODB_DATABASE_NAME_DESCRIPTION)
    .define(MONGODB_COLLECTION_NAME, Type.STRING, ConfigDef.Importance.HIGH, MONGODB_COLLECTION_NAME_DESCRIPTION);

  /**
   * get connector version
   *
   * @return connector version
   */
  @Override
  public String version() {
    return "1.0";
  }
}
