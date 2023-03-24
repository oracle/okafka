/*
** Kafka Connect for TxEventQ version 1.0.
**
** Copyright (c) 2019, 2022 Oracle and/or its affiliates.
** Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.
*/

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package oracle.jdbc.txeventq.kafka.connect.sink.utils;

import java.io.Closeable;
import java.io.IOException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.CallableStatement;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import oracle.jdbc.OracleConnection;
import oracle.jdbc.aq.AQFactory;
import oracle.jdbc.aq.AQMessageProperties;
import oracle.jdbc.internal.JMSEnqueueOptions;
import oracle.jdbc.internal.JMSFactory;
import oracle.jdbc.internal.JMSMessage;
import oracle.jdbc.internal.JMSMessageProperties;

public class TxEventQProducer implements Closeable {
    protected static final Logger log = LoggerFactory.getLogger(TxEventQProducer.class);

    private OracleConnection conn;
    private TxEventQSinkConfig config = null;
    private static final String TXEVENTQ$_TRACK_OFFSETS = "TXEVENTQ$_TRACK_OFFSETS";

    public TxEventQProducer(TxEventQSinkConfig config) {
        this.config = config;
    }

    /**
     * Uses the Oracle wallet to connect to the database.
     */
    public OracleConnection connect() {
        try {
            System.setProperty("oracle.net.wallet_location",
                    this.config.getString(TxEventQSinkConfig.DATABASE_WALLET_CONFIG));
            System.setProperty("oracle.net.tns_admin",
                    this.config.getString(TxEventQSinkConfig.DATABASE_TNSNAMES_CONFIG));
            DriverManager.registerDriver(new oracle.jdbc.OracleDriver());
            String url = "jdbc:oracle:thin:@" + this.config.getString(TxEventQSinkConfig.DATABASE_TNS_ALIAS_CONFIG);
            this.conn = (OracleConnection) DriverManager.getConnection(url);
            this.conn.setAutoCommit(false);
            log.info("[{}:{}] Oracle TxEventQ connection opened!", Thread.currentThread().getId(), this.conn);
            return this.conn;
        } catch (SQLException sqlex) {
            throw new ConnectException("Couldn't establish a connection to the database: " + sqlex.toString());
        }
    }

    /**
     * Gets the partition size for the specified Kafka topic.
     * 
     * @param topic The Kafka topic to get the partition size for.
     * @return The size of the partition for the specified topic.
     */
    public int getKafkaTopicPartitionSize(String topic) {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                this.config.getList(TxEventQSinkConfig.BOOTSTRAP_SERVERS_CONFIG));
        Map<String, TopicDescription> kafkaTopic;
        int partitionSize = 0;
        try (AdminClient adminClient = AdminClient.create(properties);) {
            kafkaTopic = adminClient.describeTopics(Collections.singletonList(topic)).allTopicNames().get();
            partitionSize = kafkaTopic.get(topic).partitions().size();
        } catch (InterruptedException e) {
            log.error("Unable to get Kafka partition size for topic {}: {}", topic, e.toString());
            // Restore interrupted state
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            log.error("Unable to get Kafka partition size for topic {}: {}", topic, e.toString());
        }
        return partitionSize;
    }

    /**
     * Validates if the Kafka topic exist.
     * 
     * @param topic The Kafka topic to check existence for.
     * @return True if the Kafka topic exists false otherwise.
     */
    public boolean kafkaTopicExists(String topic) {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                this.config.getList(TxEventQSinkConfig.BOOTSTRAP_SERVERS_CONFIG));
        Set<String> currentTopicList = null;
        try (AdminClient adminClient = AdminClient.create(properties);) {
            ListTopicsOptions options = new ListTopicsOptions();
            // includes internal topics such as __consumer_offsets
            options.listInternal(true);
            ListTopicsResult topics = adminClient.listTopics(options);
            currentTopicList = topics.names().get();

        } catch (InterruptedException e) {
            log.error("Unable to validate if Kafka topic {} exist: {}", topic, e.toString());
            // Restore interrupted state
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            log.error("Unable to validate if Kafka topic {} exist: {}", topic, e.toString());
        }
        return currentTopicList != null && currentTopicList.contains(topic);
    }

    /**
     * Checks if the specified queue name exists in the database.
     * 
     * @param conn The database connection.
     * @param queueName The name of the queue to check existence for.
     * @return True if the queue exists otherwise false.
     * @throws SQLException
     */
    public boolean txEventQueueExists(OracleConnection conn, String queueName) throws SQLException {
        DatabaseMetaData meta = this.conn.getMetaData();
        ResultSet resultSet = meta.getTables(null, null, queueName, new String[] { "TABLE" });

        return resultSet.next();
    }

    /**
     * Creates the TXEVENTQ_TRACK_OFFSETS table if it does not already exist.
     * 
     * @param conn The database connection.
     * @return True if the table exist or false if it does not exist.
     */
    public boolean createOffsetInfoTable(OracleConnection conn) {
        boolean offsetTableExist = false;

        String createTableQuery = "Create table if not exists " + TXEVENTQ$_TRACK_OFFSETS
                + "(kafka_topic_name varchar2(128) NOT NULL, queue_name varchar2(128) NOT NULL, queue_schema varchar2(128) NOT NULL, partition int NOT NULL, offset number NOT NULL)";
        try (PreparedStatement statement = conn.prepareStatement(createTableQuery);
                ResultSet rs = statement.executeQuery();) {
            DatabaseMetaData meta = conn.getMetaData();
            ResultSet resultSet = meta.getTables(null, null, TXEVENTQ$_TRACK_OFFSETS, new String[] { "TABLE" });
            if (resultSet.next()) {
                log.info("The TXEVENTQ$_TRACK_OFFSETS table successfully created.");
                offsetTableExist = true;
            }
        } catch (SQLException ex) {
            throw new ConnectException("Error attempting to create TXEVENTQ$_TRACK_OFFSETS table: " + ex.toString());
        }
        return offsetTableExist;
    }

    /**
     * Gets the number of shards for the specified queue.
     * 
     * @param conn The database connection.
     * @param queue The queue to get the number of shards for.
     * @return The number of shards.
     * @throws java.sql.SQLException
     */
    public int getNumOfShardsForQueue(OracleConnection conn, String queue) throws java.sql.SQLException {
        CallableStatement getnumshrdStmt = null;
        int numshard;
        getnumshrdStmt = conn.prepareCall("{call dbms_aqadm.get_queue_parameter(?,?,?)}");
        getnumshrdStmt.setString(1, queue);
        getnumshrdStmt.setString(2, "SHARD_NUM");
        getnumshrdStmt.registerOutParameter(3, Types.INTEGER);
        getnumshrdStmt.execute();
        numshard = getnumshrdStmt.getInt(3);
        getnumshrdStmt.close();
        return numshard;
    }

    /**
     * Enqueues the message from the SinkRecord into the specified TxEventQ.
     * 
     * @param conn       The Oracle database connection.
     * @param queueName  The name of the TxEventQ to enqueue message to.
     * @param sinkRecord The message to be enqueued.
     * @throws SQLException
     */
    public void enqueueMessage(OracleConnection conn, String queueName, SinkRecord sinkRecord) throws SQLException {
        int numberOfProperties = 1;
        int lengthOfJMSDeliveryModeProperty = 15;
        int lengthOfAQInternalPartitionProperty = 20;
        int stringPropertyValueType = 27;
        int numberPropertyValueType = 24;
        int lenthOfPersistentProperty = 10;
        
        JMSMessageProperties jmsMesgProp = JMSFactory.createJMSMessageProperties();
        jmsMesgProp.setHeaderProperties(numberOfProperties + "," + lengthOfJMSDeliveryModeProperty + ",JMSDeliveryMode,"
                + stringPropertyValueType + "," + lenthOfPersistentProperty + ",PERSISTENT");
        if (sinkRecord.kafkaPartition() != null) {
            String id = "" + 2 * sinkRecord.kafkaPartition();
            jmsMesgProp.setUserProperties(
                    numberOfProperties + "," + lengthOfAQInternalPartitionProperty + ",AQINTERNAL_PARTITION,"
                            + numberPropertyValueType + "," + id.length() + "," + 2 * sinkRecord.kafkaPartition());
        }
        jmsMesgProp.setJMSMessageType(JMSMessageProperties.JMSMessageType.BYTES_MESSAGE);
        JMSMessage mesg = JMSFactory.createJMSMessage(jmsMesgProp);

        byte[] nullPayload = null;

        if (sinkRecord.value() != null) {
            mesg.setPayload((sinkRecord.value().toString()).getBytes());
        } else {
            mesg.setPayload(nullPayload);
        }

        // We want to retrieve the message id after enqueue:
        JMSEnqueueOptions opt = new JMSEnqueueOptions();
        opt.setRetrieveMessageId(true);
        opt.setVisibility(JMSEnqueueOptions.VisibilityOption.ON_COMMIT);
        opt.setDeliveryMode(oracle.jdbc.internal.JMSEnqueueOptions.DeliveryMode.PERSISTENT);
        AQMessageProperties aqProp = AQFactory.createAQMessageProperties();
        aqProp.setPriority(4);
        // TODO: check the length
        if (sinkRecord.key() != null)
            aqProp.setCorrelation(sinkRecord.key().toString());
        mesg.setAQMessageProperties(aqProp);

        // execute the actual enqueue operation:
        ((oracle.jdbc.internal.OracleConnection) conn).jmsEnqueue(queueName, opt, mesg, aqProp);
    }

    /**
     * Enqueues the Kafka records into the specified TxEventQ. Also keeps track of
     * the offset for a particular topic and partition in database table
     * TXEVENTQ_TRACK_OFFSETS.
     * 
     * @param records The records to enqueue into the TxEventQ.
     */
    public void put(Collection<SinkRecord> records) {
        try {
            Map<String, Map<Integer, Long>> topicInfoMap = new HashMap<>();
            for (SinkRecord sinkRecord : records) {
                log.debug("[{}:{}] Enqueuing record: {}", Thread.currentThread().getId(), this.conn, sinkRecord.value());
                enqueueMessage(this.conn, this.config.getString(TxEventQSinkConfig.TXEVENTQ_QUEUE_NAME), sinkRecord);
                if (topicInfoMap.containsKey(sinkRecord.topic())) {
                    Map<Integer, Long> offsetInfoMap = topicInfoMap.get(sinkRecord.topic());
                    if (offsetInfoMap.containsKey(sinkRecord.kafkaPartition())) {
                        offsetInfoMap.replace(sinkRecord.kafkaPartition(), sinkRecord.kafkaOffset());
                    } else {
                        offsetInfoMap.put(sinkRecord.kafkaPartition(), sinkRecord.kafkaOffset());
                    }
                } else {
                    Map<Integer, Long> offsetInfoMap = new HashMap<>();
                    offsetInfoMap.put(sinkRecord.kafkaPartition(), sinkRecord.kafkaOffset());
                    topicInfoMap.put(sinkRecord.topic(), offsetInfoMap);
                }
            }

            for (Map.Entry<String, Map<Integer, Long>> topicEntry : topicInfoMap.entrySet()) {
                String topicKey = topicEntry.getKey();
                Map<Integer, Long> offsetInfoValue = topicEntry.getValue();
                for (Map.Entry<Integer, Long> offsetInfoEntry : offsetInfoValue.entrySet()) {
                    setOffsetInfoInDatabase(this.conn, topicKey,this.config.getString(TxEventQSinkConfig.TXEVENTQ_QUEUE_NAME), this.config.getString(TxEventQSinkConfig.TXEVENTQ_QUEUE_SCHEMA), offsetInfoEntry.getKey(), offsetInfoEntry.getValue());
                }
            }

            this.conn.commit();

        } catch (SQLException e) {
            throw new ConnectException("Error putting records into TxEventQ: " + e.toString());

        }
    }

    /**
     * Populates the TXEVENTQ$_TRACK_OFFSETS table with the kafka topic, TxEventQ
     * queue name, schema for the queue, partition, and offset information of the
     * messages that have been enqueued.
     * 
     * @param conn        The connection to the database.
     * @param topic       The kafka topic name.
     * @param queueName   The TxEventQ queue name.
     * @param queueSchema The schema for the queue.
     * @param partition   The partition number.
     * @param offset      The offset value.
     */
    private void setOffsetInfoInDatabase(OracleConnection conn, String topic, String queueName, String queueSchema,
            int partition, long offset) {
        String mergeSqlStatment = "MERGE INTO " + TXEVENTQ$_TRACK_OFFSETS
                + " tab1 USING (SELECT ? kafka_topic_name, ? queue_name, ? queue_schema, ? partition)"
                + " tab2 ON (tab1.kafka_topic_name = tab2.kafka_topic_name AND tab1.queue_name = tab2.queue_name AND tab1.queue_schema = tab2.queue_schema AND tab1.partition = tab2.partition)"
                + " WHEN MATCHED THEN UPDATE set offset=? WHEN NOT MATCHED THEN"
                + " INSERT (kafka_topic_name, queue_name, queue_schema, partition, offset) values (?,?,?,?,?)";

        try (PreparedStatement statement = conn.prepareStatement(mergeSqlStatment)) {
            statement.setString(1, topic);
            statement.setString(2, queueName);
            statement.setString(3, queueSchema);
            statement.setInt(4, partition);
            statement.setLong(5, offset + 1);
            statement.setString(6, topic);
            statement.setString(7, queueName);
            statement.setString(8, queueSchema);
            statement.setInt(9, partition);
            statement.setLong(10, offset + 1);
            statement.execute();

        } catch (Exception e) {
            throw new ConnectException("Error attempting to insert or update offset information: " + e.toString());
        }
    }

    /**
     * Gets the offset for the specified kafka topic, TxEventQ queue name, schema,
     * and partition. The offset will be used to determine which message to start
     * consuming.
     * 
     * @param conn        The connection to the database.
     * @param topic       The kafka topic name.
     * @param queueName   The TxEventQ queue name.
     * @param queueSchema The schema for the queue.
     * @param partition   The partition number.
     * @return
     */
    public long getOffsetInDatabase(OracleConnection conn, String topic, String queueName, String queueSchema,
            int partition) {
        long offsetVal = 0;
        try (PreparedStatement statement = conn.prepareStatement("SELECT offset FROM " + TXEVENTQ$_TRACK_OFFSETS
                + " where kafka_topic_name=? and queue_name = ? and queue_schema = ? and partition=?")) {
            statement.setString(1, topic);
            statement.setString(2, queueName);
            statement.setString(3, queueSchema);
            statement.setInt(4, partition);

            try (ResultSet rs = statement.executeQuery()) {
                if (rs.next()) {
                    offsetVal = rs.getLong("offset");
                }
            }
        } catch (Exception e) {
            throw new ConnectException("Error getting the offset value: " + e.toString());
        }
        return offsetVal;
    }

    /**
     * Gets the connection being used.
     * 
     * @return The OracleConnection.
     * 
     */
    public OracleConnection getConnection() {
        return this.conn;
    }

    /**
	 * Checks if the database connection is open and valid.
	 * 
	 * @return True if the database is open and valid, otherwise false.
	 * @throws SQLException 
	 */
	public boolean isConnOpen(OracleConnection conn) throws SQLException {
		return conn != null && !conn.isClosed();
	}

    @Override
    public void close() throws IOException {
        log.info("[{}] Close Oracle TxEventQ Connections.", Thread.currentThread().getId());
        try {
            this.conn.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
