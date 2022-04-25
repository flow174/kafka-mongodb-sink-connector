package com.beck.kafka.sink.connector;

public class Constants {

  public static final String NAME = "name";
  public static final String NAME_DESCRIPTION = "Unique name for the connector. Attempting to register again with the same name will fail.";

  public static final String CONNECTOR_CLASS = "connector.class";
  public static final String CONNECTOR_CLASS_DESCRIPTION = "The Java class for the connector.";

  public static final String TASKS_MAX = "tasks.max";
  public static final String TASKS_MAX_DESCRIPTION = "The maximum number of tasks that should be created for this  connector. The connector may create fewer tasks if it cannot achieve this level of parallelism.";

  public static final String TOPICS = "topics";
  public static final String TOPICS_DESCRIPTION = "A comma-separated list of topics to use as input for this connector.";

  public static final String MONGODB_CONNECTION_STRING = "mongodb.connection.string";
  public static final String MONGODB_CONNECTION_STRING_DESCRIPTION = "The mongodb connection string used to connect to mongodb.";

  public static final String MONGODB_DATABASE_NAME = "mongodb.database.name";
  public static final String MONGODB_DATABASE_NAME_DESCRIPTION = "Mongodb database used to save kafka data.";

  public static final String MONGODB_COLLECTION_NAME = "mongodb.collection.name";
  public static final String MONGODB_COLLECTION_NAME_DESCRIPTION = "Mongodb database collection name.";

  public static final String RECORD_KEY = "key";
  public static final String RECORD_VALUE = "value";
  public static final String KAFKA_TOPIC = "topic";
  public static final String KAFKA_PARTITION = "partition";
  public static final String KAFKA_OFFSET = "offset";
}
