package com.beck.kafka.sink.connector;

import static com.beck.kafka.sink.connector.Constants.KAFKA_OFFSET;
import static com.beck.kafka.sink.connector.Constants.KAFKA_PARTITION;
import static com.beck.kafka.sink.connector.Constants.KAFKA_TOPIC;
import static com.beck.kafka.sink.connector.Constants.MONGODB_COLLECTION_NAME;
import static com.beck.kafka.sink.connector.Constants.MONGODB_CONNECTION_STRING;
import static com.beck.kafka.sink.connector.Constants.MONGODB_DATABASE_NAME;
import static com.beck.kafka.sink.connector.Constants.RECORD_KEY;
import static com.beck.kafka.sink.connector.Constants.RECORD_VALUE;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import java.util.Collection;
import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongodbSinkTask extends SinkTask {

  private static final Logger log = LoggerFactory.getLogger(MongodbSinkTask.class);

  MongoCollection<Document> collection;

  MongoClient mongoClient;

  /**
   * Return sink task version, same as the connector
   *
   * @return sink task version
   */
  @Override
  public String version() {
    return new MongodbSinkConnector().version();
  }

  /**
   * Start a sink task. No exception is handled here. If there is an exception, the service cannot be started and the problem needs to be solved.
   *
   * @param map configuration
   */
  @Override
  public void start(Map<String, String> map) {
    AbstractConfig abstractConfig = new AbstractConfig(MongodbSinkConnector.CONFIG_DEF, map);
    String connectionString = abstractConfig.getString(MONGODB_CONNECTION_STRING);
    String databaseName = abstractConfig.getString(MONGODB_DATABASE_NAME);
    String collectionName = abstractConfig.getString(MONGODB_COLLECTION_NAME);

    log.info("connect to mongodb : {} : {} : {}", connectionString, databaseName, collectionName);
    MongoClientURI uri = new MongoClientURI(connectionString);
    mongoClient = new MongoClient(uri);

    if (hasDatabase(databaseName)) {
      MongoDatabase database = mongoClient.getDatabase(databaseName);
      // if collection not exist should create it and create index
      if (!hasCollection(database, collectionName)) {
        createCollection(database, collectionName);
        createIndex();
      } else {
        getCollection(database, collectionName);
      }
    } else {
      throw new RuntimeException("No database : " + databaseName);
    }
  }

  private void createCollection(MongoDatabase database, String collectionName) {
    database.createCollection(collectionName);
    getCollection(database, collectionName);
  }

  private void getCollection(MongoDatabase database, String collectionName) {
    collection = database.getCollection(collectionName);
  }

  private void createIndex() {
    IndexOptions indexOptions = new IndexOptions().unique(true).background(false).name("key-unique-index");
    Document doc = new Document();
    doc.append(RECORD_KEY, 1);
    collection.createIndex(doc, indexOptions);
  }

  private boolean hasDatabase(String databaseName) {
    MongoIterable<String> databaseNames = mongoClient.listDatabaseNames();
    for (String name : databaseNames) {
      if (name.equals(databaseName)) {
        return true;
      }
    }
    return false;
  }

  private boolean hasCollection(MongoDatabase database, String collectionName) {
    MongoIterable<String> collectionNames = database.listCollectionNames();
    for (String name : collectionNames) {
      if (name.equals(collectionName)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Batch processing kafka sink record. The strong consistency operation is used here, either the insertion is successful or the insertion fails.
   * When it fails, an error will be thrown and the program will exit. At this time, the offset of kafka will not be updated, and it can continue to
   * consume from here next time.
   *
   * @param records kafka sink records
   */
  @Override
  public void put(Collection<SinkRecord> records) {
    log.info("start put records : {}", records.size());
    if (records.isEmpty()) {
      return;
    }

    for (SinkRecord record : records) {
      JSONObject jsonObject = new JSONObject();
      jsonObject.put(RECORD_KEY, record.key());
      jsonObject.put(RECORD_VALUE, record.value());
      jsonObject.put(KAFKA_TOPIC, record.topic());
      jsonObject.put(KAFKA_PARTITION, record.kafkaPartition());
      jsonObject.put(KAFKA_OFFSET, record.kafkaOffset());
      Document document = Document.parse(JSON.toJSONString(jsonObject));
      Bson filter = Filters.eq(RECORD_KEY, record.key());
      if (collection.findOneAndReplace(filter, document) == null) {
        collection.insertOne(document);
      }
    }
  }

  @Override
  public void stop() {
    if (mongoClient != null) {
      mongoClient.close();
    }
  }
}
