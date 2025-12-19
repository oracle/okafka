/*
** Kafka Connect for TxEventQ.
**
** Copyright (c) 2024, 2025 Oracle and/or its affiliates.
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

package oracle.jdbc.txeventq.kafka.connect.source.utils;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.CallableStatement;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import oracle.AQ.AQOracleSQLException;
import oracle.jdbc.OracleConnection;
import oracle.jdbc.aq.AQDequeueOptions;
import oracle.jdbc.aq.AQMessage;
import oracle.jdbc.txeventq.kafka.connect.common.utils.Constants;
import oracle.jdbc.txeventq.kafka.connect.common.utils.MessageUtils;
import oracle.jdbc.txeventq.kafka.connect.schema.JmsMessage;
import oracle.jdbc.txeventq.kafka.connect.schema.Key;
import oracle.jms.AQjmsBytesMessage;
import oracle.jms.AQjmsConsumer;
import oracle.jms.AQjmsFactory;
import oracle.jms.AQjmsMapMessage;
import oracle.jms.AQjmsMessage;
import oracle.jms.AQjmsSession;
import oracle.jms.AQjmsTextMessage;
import oracle.sql.RAW;
import oracle.sql.json.OracleJsonDatum;

public class TxEventQConsumer implements Closeable {
    protected static final Logger log = LoggerFactory.getLogger(TxEventQConsumer.class);

    // Indicates whether connected to TxEventQ
    private boolean connected = false;

    // Indicates whether messages are in-flight in current transaction
    private boolean inflight = false;

    // Indicates whether close has been requested
    private AtomicBoolean closeNow = new AtomicBoolean();

    // Delay between repeated reconnect attempts
    private long reconnectDelayMillis = reconnectDelayMillisMin;
    private static long reconnectDelayMillisMin = 64L;
    private static long reconnectDelayMillisMax = 8192L;
    private static final int DLENGTH_SIZE = 4;

    private TxEventQConnectorConfig config = null;
    private OracleConnection conn;

    private TopicConnectionFactory tcf;
    private TopicConnection tconn;
    private TopicSession tSess;
    private Topic topic;
    private TopicSubscriber topicDurSubscr1;
    private String getQueueType;
    private boolean useSchemaForJmsMsg;
    private boolean mapShardToKafkaPartition;

    private static final int MINIMUM_VERSION = 23;
    private int databaseMajorVersion = 0;
    private int databaseMinorVersion = 0;
    private List<String> denylistedHeaders;
    private List<String> allowlistedJmsHeaders;

    public TxEventQConsumer(TxEventQConnectorConfig config) {
        this.config = config;
    }

    /**
     * Uses the Oracle wallet to connect to the database.
     */
    public void connect() {
        try {
            log.debug("[{}] Attempting to open database connection.",
                    Thread.currentThread().getId());
            System.setProperty("oracle.net.wallet_location",
                    this.config.getString(TxEventQConnectorConfig.DATABASE_WALLET_CONFIG));
            System.setProperty("oracle.net.tns_admin",
                    this.config.getString(TxEventQConnectorConfig.DATABASE_TNSNAMES_CONFIG));

            DriverManager.registerDriver(new oracle.jdbc.OracleDriver());
            String url = "jdbc:oracle:thin:@"
                    + this.config.getString(TxEventQConnectorConfig.DATABASE_TNS_ALIAS_CONFIG);
            tcf = AQjmsFactory.getTopicConnectionFactory(url, null);
            tconn = tcf.createTopicConnection();
            tSess = tconn.createTopicSession(true, Session.CLIENT_ACKNOWLEDGE);
            this.conn = (OracleConnection) ((AQjmsSession) (tSess)).getDBConnection();

            versionCheck();

            String userName = this.conn.getUserName().toUpperCase();

            topic = ((AQjmsSession) (tSess)).getTopic(userName, this.config
                    .getString(TxEventQConnectorConfig.TXEVENTQ_QUEUE_NAME).toUpperCase());
            topicDurSubscr1 = ((AQjmsSession) (tSess)).getDurableSubscriber(topic,
                    this.config.getString(TxEventQConnectorConfig.TXEVENTQ_SUBSCRIBER_CONFIG));
            tconn.start();
            this.conn.setAutoCommit(false);
            this.getQueueType = getQueueTableType(this.config
                    .getString(TxEventQConnectorConfig.TXEVENTQ_QUEUE_NAME).toUpperCase());

            this.useSchemaForJmsMsg = this.config
                    .getBoolean(TxEventQConnectorConfig.USE_SCHEMA_FOR_JMS_MESSAGES_CONFIG);

            this.denylistedHeaders = this.config
                    .getList(TxEventQConnectorConfig.KAFKA_HEADER_DENYLIST_CONFIG);

            this.allowlistedJmsHeaders = this.config
                    .getList(TxEventQConnectorConfig.KAFKA_HEADER_JMS_ALLOWLIST_CONFIG);

            this.mapShardToKafkaPartition = this.config.getBoolean(
                    TxEventQConnectorConfig.TXEVENTQ_MAP_SHARD_TO_KAFKA_PARTITION_CONFIG);

            this.connected = true;
            log.debug("[{}] Oracle TxEventQ connection [{}] opened.",
                    Thread.currentThread().getId(), this.conn);
        } catch (SQLException | JMSException ex) {
            log.debug("[{}] Connection to TxEventQ could not be established",
                    Thread.currentThread().getId());
            handleException(ex);
        }
    }

    /**
     * Check whether the database version is 23 or later
     * 
     * @throws ConnectException If we cannot get the database metadata or if the database version is
     *                          less than 23
     */
    private void versionCheck() {
        DatabaseMetaData md = null;
        try {
            md = this.conn.getMetaData();
            this.databaseMajorVersion = md.getDatabaseMajorVersion();
            this.databaseMinorVersion = md.getDatabaseMinorVersion();

            log.debug("Database major version: {}", this.databaseMajorVersion);
            log.debug("Database minor version: [{}]", this.databaseMinorVersion);

        } catch (SQLException e) {
            throw new ConnectException("Unable to obtain a database connection");
        }

        if (this.databaseMajorVersion < MINIMUM_VERSION) {
            throw new ConnectException("TxEventQ Connector requires Oracle Database 23 or greater");
        }
    }

    /**
     * Gets the database major version that is being using.
     * 
     * @return The database major version that is being used.
     */
    public int getDatabaseMajorVersion() {
        return this.databaseMajorVersion;
    }

    /**
     * Gets the database minor version that is being using.
     * 
     * @return The database minor version that is being used.
     */
    public int getDatabaseMinorVersion() {
        return this.databaseMinorVersion;
    }

    /**
     * Gets the database connection that is currently being used.
     * 
     * @return The current database connection that is being used.
     */
    public OracleConnection getDatabaseConnection() {
        log.trace("[{}]:[{}]: Entry {}.getDatabaseConnection", Thread.currentThread().getId(),
                this.conn, this.getClass().getName());

        return this.conn;
    }

    /**
     * Internal method to close the connection.
     */
    private void closeConnectionInternal() {
        log.trace("[{}]:[{}]: Entry {}.closeConnectionInternal", Thread.currentThread().getId(),
                this.conn, this.getClass().getName());

        try {
            this.inflight = false;
            this.connected = false;

            if (tSess != null) {
                this.tSess.rollback();
                log.debug("Session rollback occurred.");
                this.tSess.close();
                log.debug("Session closed.");
            }

            if (this.conn != null) {
                log.debug("Connection will be closed.");
                this.conn.close();
            }

            if (this.tconn != null) {
                log.debug("Topic Connection will be closed.");
                this.tconn.close();
            }

        } catch (SQLException | JMSException ex) {
            log.error("{}: {}", ex.getClass().getName(), ex);

        } finally {
            this.conn = null;
            this.tSess = null;
            this.tconn = null;
            log.debug("Connection to TxEventQ closed.");
        }

        log.trace("[{}]:[{}]: Exit {}.closeConnectionInternal", Thread.currentThread().getId(),
                this.conn, this.getClass().getName());
    }

    /**
     * Internal method to connect to TxEventQ.
     * 
     * @return true if the connection can be used, false otherwise
     */
    private boolean connectConnectionInternal() {
        if (this.connected) {
            return true;
        }

        if (closeNow.get()) {
            log.debug("Closing connection now");
            return false;
        }

        log.trace("[{}] Entry {}.connectConnectionInternal", Thread.currentThread().getId(),
                this.getClass().getName());
        try {
            log.debug("[{}] Attempting to open database connection.",
                    Thread.currentThread().getId());
            System.setProperty("oracle.net.wallet_location",
                    this.config.getString(TxEventQConnectorConfig.DATABASE_WALLET_CONFIG));
            System.setProperty("oracle.net.tns_admin",
                    this.config.getString(TxEventQConnectorConfig.DATABASE_TNSNAMES_CONFIG));
            DriverManager.registerDriver(new oracle.jdbc.OracleDriver());
            String url = "jdbc:oracle:thin:@"
                    + this.config.getString(TxEventQConnectorConfig.DATABASE_TNS_ALIAS_CONFIG);
            tcf = AQjmsFactory.getTopicConnectionFactory(url, null);
            tconn = tcf.createTopicConnection();
            tSess = tconn.createTopicSession(true, Session.CLIENT_ACKNOWLEDGE);
            this.conn = (OracleConnection) ((AQjmsSession) (tSess)).getDBConnection();

            versionCheck();

            String userName = this.conn.getUserName().toUpperCase();

            topic = ((AQjmsSession) (tSess)).getTopic(userName, this.config
                    .getString(TxEventQConnectorConfig.TXEVENTQ_QUEUE_NAME).toUpperCase());
            topicDurSubscr1 = ((AQjmsSession) (tSess)).getDurableSubscriber(topic,
                    this.config.getString(TxEventQConnectorConfig.TXEVENTQ_SUBSCRIBER_CONFIG));
            tconn.start();
            this.conn.setAutoCommit(false);
            this.getQueueType = getQueueTableType(this.config
                    .getString(TxEventQConnectorConfig.TXEVENTQ_QUEUE_NAME).toUpperCase());

            this.useSchemaForJmsMsg = this.config
                    .getBoolean(TxEventQConnectorConfig.USE_SCHEMA_FOR_JMS_MESSAGES_CONFIG);

            this.denylistedHeaders = this.config
                    .getList(TxEventQConnectorConfig.KAFKA_HEADER_DENYLIST_CONFIG);

            this.allowlistedJmsHeaders = this.config
                    .getList(TxEventQConnectorConfig.KAFKA_HEADER_JMS_ALLOWLIST_CONFIG);

            this.mapShardToKafkaPartition = this.config.getBoolean(
                    TxEventQConnectorConfig.TXEVENTQ_MAP_SHARD_TO_KAFKA_PARTITION_CONFIG);

            log.debug("[{}] Oracle TxEventQ connection [{}] opened.",
                    Thread.currentThread().getId(), this.conn);
            this.connected = true;
        } catch (SQLException | JMSException ex) {
            // Delay slightly so that repeated reconnect loops don't run too fast
            try {
                Thread.sleep(reconnectDelayMillis);
            } catch (final InterruptedException ie) {
                // Restore interrupted state...
                Thread.currentThread().interrupt();

            }

            if (reconnectDelayMillis < reconnectDelayMillisMax) {
                reconnectDelayMillis = reconnectDelayMillis * 2;
            }

            handleException(ex);
            log.debug("[{}]  Exit {}.connectConnectionInternal, retval=false",
                    Thread.currentThread().getId(), this.getClass().getName());
            return false;
        }

        log.trace("[{}]:[{}] Exit {}.connectConnectionInternal, retval=true",
                Thread.currentThread().getId(), this.conn, this.getClass().getName());
        return true;
    }

    /**
     * Processes the RAW message that was dequeued and creates a new TxEventQSourceRecord.
     * 
     * @param msgId The byte ID value of the the message.
     * @param msg   The message that has been dequeued.
     * @return A TxEventQSourceRecord containing the message information, Kafka topic, and
     *         partitions location for Kafka to store the message at.
     * @throws SQLException
     */
    private TxEventQSourceRecord processRawPayload(byte[] msgId, AQMessage msg)
            throws SQLException {
        log.trace("[{}] Entry {}.processRawPayload", this.conn, this.getClass().getName());
        if (msgId == null) {
            throw new SQLException("Message Id for RAW message is null.");
        }

        RAW rawPayload = msg.getRAWPayload();
        String kafkaTopic = this.config.getString(TxEventQConnectorConfig.KAFKA_TOPIC);
        String msgIdStr = byteArrayToHex(msgId);
        String correlationId = msg.getMessageProperties().getCorrelation();
        int shardNum = getShardId(msgIdStr);

        log.debug("[{}] Processing RAW Type message:[msgId: {}, shardNum: {}]", this.conn, msgIdStr,
                shardNum);

        log.trace("[{}] Exit {}.processRawPayload", this.conn, this.getClass().getName());
        return new TxEventQSourceRecord(null, null, kafkaTopic,
                this.mapShardToKafkaPartition ? shardNum / 2 : null, null, correlationId, null,
                rawPayload.getBytes(), TxEventQSourceRecord.PayloadType.RAW, msgId);
    }

    /**
     * Processes the JSON message that was dequeued and creates a new TxEventQSourceRecord.
     * 
     * @param msgId The byte ID value of the the message.
     * @param msg   The message that has been dequeued.
     * @return A TxEventQSourceRecord containing the message information, Kafka topic, and
     *         partitions location for Kafka to store the message at.
     * @throws SQLException
     */
    private TxEventQSourceRecord processJsonPayload(byte[] msgId, AQMessage msg)
            throws SQLException {
        log.trace("[{}] Entry {}.processJsonPayload", this.conn, this.getClass().getName());
        if (msgId == null) {
            throw new SQLException("Message Id for JSON message is null.");
        }

        OracleJsonDatum jsonPayload = msg.getJSONPayload();
        String msgIdStr = byteArrayToHex(msgId);
        String correlationId = msg.getMessageProperties().getCorrelation();
        int shardNum = getShardId(msgIdStr);
        String kafkaTopic = this.config.getString(TxEventQConnectorConfig.KAFKA_TOPIC);

        log.debug("[{}] Processing JSON Type message:[msgId: {}, shardNum: {}]", this.conn,
                msgIdStr, shardNum);

        log.trace("[{}] Exit {}.processJsonPayload", this.conn, this.getClass().getName());
        return new TxEventQSourceRecord(null, null, kafkaTopic,
                this.mapShardToKafkaPartition ? shardNum / 2 : null, null, correlationId, null,
                jsonPayload.getBytes(), TxEventQSourceRecord.PayloadType.JSON, msgId);
    }

    /**
     * Checks the type of TxEventQ that the dequeue will be performed on and calls the appropriate
     * method to perform the dequeue.
     * 
     * @param batchSize The maximum number of messages to dequeue.
     * 
     * @return A list of SourceRecords containing the message that has been dequeued.
     */
    public List<SourceRecord> receive(int batchSize) {
        if (!connectConnectionInternal()) {
            log.trace("[{}]  Exit {}.receive, retval=null", this.conn, this.getClass().getName());
            return Collections.emptyList();
        }

        log.trace("[{}]  Entry {}.receive", this.conn, this.getClass().getName());

        log.debug("[{}] Queue table {} is a {} type table.", this.conn,
                this.config.getString(TxEventQConnectorConfig.TXEVENTQ_QUEUE_NAME),
                this.getQueueType);

        if (this.getQueueType != null && this.getQueueType.equalsIgnoreCase("JMS_BYTES")) {
            log.trace("[{}]  Exit {}.receive", this.conn, this.getClass().getName());
            return receiveJmsMessages(batchSize);
        } else if (this.getQueueType != null && this.getQueueType.equalsIgnoreCase("RAW")) {
            log.trace("[{}]  Exit {}.receive", this.conn, this.getClass().getName());
            return receiveRawAQMessages(this.getQueueType, batchSize);
        } else if (this.getQueueType != null && this.getQueueType.equalsIgnoreCase("JSON")) {
            log.trace("[{}]  Exit {}.receive", this.conn, this.getClass().getName());
            return receiveJsonAQMessages(this.getQueueType, batchSize);
        } else {
            log.error("Supported queue types are: JMS_BYTES, RAW, and JSON");
            log.trace("[{}]  Exit {}.receive", this.conn, this.getClass().getName());
            return Collections.emptyList();
        }
    }

    /**
     * Dequeues RAW messages from the TxEventQ.
     * 
     * @param queueType A String indicating the type of queue that the dequeue will be performed on.
     * @param batchSize The maximum number of messages to dequeue.
     * 
     * @return A list of SourceRecords containing the messages that has been dequeued.
     */
    public List<SourceRecord> receiveRawAQMessages(String queueType, int batchSize) {
        log.trace("[{}]  Entry {}.receiveRawAQMessage", this.conn, this.getClass().getName());
        AQDequeueOptions deqopt = new AQDequeueOptions();

        deqopt.setRetrieveMessageId(true);
        try {
            String subscriber = this.config
                    .getString(TxEventQConnectorConfig.TXEVENTQ_SUBSCRIBER_CONFIG);
            deqopt.setConsumerName(subscriber);
            deqopt.setDequeueMode(AQDequeueOptions.DequeueMode.REMOVE);
            deqopt.setVisibility(AQDequeueOptions.VisibilityOption.ON_COMMIT);
            deqopt.setNavigation(AQDequeueOptions.NavigationOption.NEXT_MESSAGE);
            deqopt.setDeliveryFilter(AQDequeueOptions.DeliveryFilter.PERSISTENT);
        } catch (SQLException e) {
            log.error("Error setting AQDequeueOptions: {}", e.getMessage());
            return Collections.emptyList();
        }

        AQMessage[] msg = null;
        byte[] msgId = new byte[0];
        List<SourceRecord> records = new ArrayList<>();
        SourceRecord sr = null;

        try {
            String txEventQTopic = this.config
                    .getString(TxEventQConnectorConfig.TXEVENTQ_QUEUE_NAME);

            if (this.conn != null) {
                msg = this.conn.dequeue(txEventQTopic, deqopt, queueType, batchSize);

                if (msg != null && msg.length != 0) {
                    inflight = true;
                    for (int i = 0; i < msg.length; i++) {

                        msgId = msg[i].getMessageId();
                        sr = processRawPayload(msgId, msg[i]);
                        records.add(sr);
                    }
                }
            }
        } catch (SQLException ex) {
            handleException(ex);
            records.clear();
        } catch (final ConnectException exc) {
            attemptRollback();
            throw exc;
        }

        log.trace("[{}]  Exit {}.receiveRawAQMessage, retvalArrSize={}", this.conn,
                this.getClass().getName(), records.size());

        return records;
    }

    /**
     * Dequeues JSON messages from the TxEventQ.
     * 
     * @param queueType A String indicating the type of queue that the dequeue will be performed on.
     * @param batchSize The maximum number of messages to dequeue.
     * 
     * @return A list of SourceRecords containing the messages that has been dequeued.
     */
    public List<SourceRecord> receiveJsonAQMessages(String queueType, int batchSize) {
        log.trace("[{}]  Entry {}.receiveJsonAQMessage", this.conn, this.getClass().getName());
        AQDequeueOptions deqopt = new AQDequeueOptions();
        int msgCount = 0;

        deqopt.setRetrieveMessageId(true);
        try {
            String subscriber = this.config
                    .getString(TxEventQConnectorConfig.TXEVENTQ_SUBSCRIBER_CONFIG);
            deqopt.setConsumerName(subscriber);
            deqopt.setDequeueMode(AQDequeueOptions.DequeueMode.REMOVE);
            deqopt.setVisibility(AQDequeueOptions.VisibilityOption.ON_COMMIT);
            deqopt.setNavigation(AQDequeueOptions.NavigationOption.NEXT_MESSAGE);
            deqopt.setDeliveryFilter(AQDequeueOptions.DeliveryFilter.PERSISTENT);
        } catch (SQLException e) {
            log.error("Error setting AQDequeueOptions: {}", e.getMessage());
            return Collections.emptyList();
        }

        AQMessage msg = null;
        byte[] msgId = new byte[0];
        List<SourceRecord> records = new ArrayList<>();
        SourceRecord sr = null;

        try {
            String txEventQTopic = this.config
                    .getString(TxEventQConnectorConfig.TXEVENTQ_QUEUE_NAME);
            do {
                if (this.conn != null) {
                    msg = this.conn.dequeue(txEventQTopic, deqopt, queueType);

                    if (msg != null) {
                        inflight = true;

                        msgId = msg.getMessageId();
                        sr = processJsonPayload(msgId, msg);
                        records.add(sr);
                        msgCount++;

                    }
                }
            } while (msg != null && msgCount < batchSize);

        } catch (SQLException ex) {
            handleException(ex);
            records.clear();
        } catch (final ConnectException exc) {
            attemptRollback();
            throw exc;
        }

        log.trace("[{}]  Exit {}.receiveJsonAQMessage, retvalArrSize={}", this.conn,
                this.getClass().getName(), records.size());

        return records;
    }

    /**
     * Processes the JMS messages that was dequeued and creates a new TxEventQSourceRecord. The
     * method determines whether the message is a JMS bytes, JMS text, or JMS map message and
     * processes accordingly.
     * 
     * @param msgId The message Id of the message that was dequeued.
     * @param msg   The message that was dequeued.
     * @return A TxEventQSourceRecord containing the message information, Kafka topic, and
     *         partitions location for Kafka to store the message at.
     * @throws JMSException
     * @throws SQLException
     */
    private TxEventQSourceRecord processJmsMessage(String msgId, Message msg)
            throws JMSException, SQLException {
        log.trace("[{}] Entry {}.processJmsMessage", this.conn, this.getClass().getName());
        if (msgId == null) {
            throw new JMSException("Message Id for JMS message is null.");
        }

        String kafkaTopic = this.config.getString(TxEventQConnectorConfig.KAFKA_TOPIC);
        String correlationId = msg.getJMSCorrelationID();
        Key correlationKey = new Key(correlationId);
        int shardNum = getShardId(msg.getJMSMessageID());

        log.trace("[{}] Exit {}.processJmsMessage", this.conn, this.getClass().getName());

        if (msg instanceof AQjmsBytesMessage) {
            return processJmsBytesMessage(msgId, msg, kafkaTopic, correlationId, correlationKey,
                    shardNum);
        } else if (msg instanceof AQjmsTextMessage) {
            return processJmsTextMessage(msgId, msg, kafkaTopic, correlationId, correlationKey,
                    shardNum);
        } else if (msg instanceof AQjmsMapMessage) {
            return processJmsMapMessage(msgId, msg, kafkaTopic, correlationId, correlationKey,
                    shardNum);
        }

        return null;
    }

    /**
     * Process JmsMapMessage received from Oracle Transactional Event Queue into a
     * TxEventQSourceRecord that will be sent to the specified Kafka topic.
     * 
     * @param msgId          The message Id of the message that was dequeued.
     * @param msg            The message that was dequeued.
     * @param kafkaTopic     The Kafka topic to put the message in.
     * @param correlationId  The correlation key from the message.
     * @param correlationKey A Key object that is used to create a schema for the correlation id
     *                       value.
     * @param shardNum       The shard number that the message is stored in the TxEventQ.
     * @return A TxEventQSourceRecord containing the JmsMapMessage message information, Kafka topic,
     *         key if available in message, and partition location for Kafka to store the message
     *         at.
     * @throws JMSException
     * @throws SQLException
     */
    private TxEventQSourceRecord processJmsMapMessage(String msgId, Message msg, String kafkaTopic,
            String correlationId, Key correlationKey, int shardNum)
            throws JMSException, SQLException {
        log.trace("[{}] Entry {}.processJmsMapMessage", this.conn, this.getClass().getName());
        AQjmsMapMessage mapMsg = (AQjmsMapMessage) msg;
        JmsMessage jmsMsg = new JmsMessage(mapMsg, 1, null);
        Headers header = new org.apache.kafka.connect.header.ConnectHeaders();
        addJMSMessagePropertyToKafkaHeader(mapMsg, jmsMsg, header);

        log.debug("[{}] Processing JMS Map message:[msgIdStr: {}, shardNum: {}, correlationId: {}]",
                this.conn, msgId, shardNum, correlationId);

        log.trace("[{}] Exit {}.processJmsMapMessage", this.conn, this.getClass().getName());

        return new TxEventQSourceRecord(null, null, kafkaTopic,
                this.mapShardToKafkaPartition ? shardNum / 2 : null,
                this.useSchemaForJmsMsg && correlationId != null ? Key.SCHEMA_KEY_V1 : null,
                this.useSchemaForJmsMsg && correlationId != null ? correlationKey.toKeyStructV1()
                        : correlationId,
                this.useSchemaForJmsMsg ? JmsMessage.SCHEMA_JMSMESSAGE_V1 : null,
                this.useSchemaForJmsMsg ? jmsMsg.toJmsMessageStructV1() : mapMsg.getMapNames(),
                null, header, TxEventQSourceRecord.PayloadType.JMS_MAP,
                mapMsg.getJMSMessageIDAsBytes());
    }

    /**
     * Process JmsTextMessage received from Oracle Transactional Event Queue into a
     * TxEventQSourceRecord that will be sent to the specified Kafka topic.
     * 
     * @param msgId          The message Id of the message that was dequeued.
     * @param msg            The message that was dequeued.
     * @param kafkaTopic     The Kafka topic to put the message in.
     * @param correlationId  The correlation key from the message.
     * @param correlationKey A Key object that is used to create a schema for the correlation id
     *                       value.
     * @param shardNum       The shard number that the message is stored in the TxEventQ.
     * @return A TxEventQSourceRecord containing the JmsTextMessage message information, Kafka
     *         topic, key if available in message, and partition location for Kafka to store the
     *         message at.
     * @throws JMSException
     * @throws SQLException
     */
    private TxEventQSourceRecord processJmsTextMessage(String msgId, Message msg, String kafkaTopic,
            String correlationId, Key correlationKey, int shardNum)
            throws JMSException, SQLException {
        log.trace("[{}] Entry {}.processJmsTextMessage", this.conn, this.getClass().getName());
        AQjmsTextMessage textMsg = (AQjmsTextMessage) msg;
        JmsMessage jmsMsg = new JmsMessage(textMsg, 1, null);
        Headers header = new org.apache.kafka.connect.header.ConnectHeaders();
        addJMSMessagePropertyToKafkaHeader(textMsg, jmsMsg, header);

        log.debug(
                "[{}] Processing JMS Text message:[msgIdStr: {}, shardNum: {}, correlationId: {}]",
                this.conn, msgId, shardNum, correlationId);

        log.trace("[{}] Exit {}.processJmsTextMessage", this.conn, this.getClass().getName());

        return new TxEventQSourceRecord(null, null, kafkaTopic,
                this.mapShardToKafkaPartition ? shardNum / 2 : null,
                this.useSchemaForJmsMsg && correlationId != null ? Key.SCHEMA_KEY_V1 : null,
                this.useSchemaForJmsMsg && correlationId != null ? correlationKey.toKeyStructV1()
                        : correlationId,
                this.useSchemaForJmsMsg ? JmsMessage.SCHEMA_JMSMESSAGE_V1 : null,
                this.useSchemaForJmsMsg ? jmsMsg.toJmsMessageStructV1() : textMsg.getText(), null,
                header, TxEventQSourceRecord.PayloadType.JMS_TEXT,
                textMsg.getJMSMessageIDAsBytes());

    }

    /**
     * Add the JMS Message properties to the Kafka header if the JMS Message property is listed in
     * the source connector configuration property "header.jms.allowlist" and the
     * "use.schema.for.jms.msgs" configuration property is set to false. If
     * "use.schema.for.jms.msgs" property is set to true all the JMS Message properties will be
     * stored as a JSON schema as part of the Kafka message payload.
     * 
     * @param aqJmsMsg     The JMS message that has been consumed from TxEventQ.
     * @param jmsMsgSchema The JMS message schema that will be used to store the JMS message
     *                     property information.
     * @param header       The Kafka header object to store the header information to.
     * @return The header information to store in the Kafka Header one the SourceRecord is created.
     * @throws JMSException
     */
    private Headers addJMSMessagePropertyToKafkaHeader(AQjmsMessage aqJmsMsg,
            JmsMessage jmsMsgSchema, Headers header) throws JMSException {
        if (!this.allowlistedJmsHeaders.isEmpty() && !this.useSchemaForJmsMsg) {
            for (String jmsMsgProp : this.allowlistedJmsHeaders) {
                if (header.lastWithName(jmsMsgProp) == null) {
                    switch (jmsMsgProp) {
                    case "jmsMessageType":
                    case "jmsMessageId":
                    case "jmsCorrelationId":
                        header.addBytes(jmsMsgProp, jmsMsgSchema.toJmsMessageStructV1()
                                .getString(jmsMsgProp).getBytes());
                        break;
                    case "jmsDestination":
                        header.addBytes(jmsMsgProp,
                                aqJmsMsg.getJMSDestination() != null
                                        ? jmsMsgSchema.toJmsMessageStructV1().getStruct(jmsMsgProp)
                                                .toString().getBytes()
                                        : null);
                        break;
                    case "jmsReplyTo":
                        header.addBytes(jmsMsgProp,
                                aqJmsMsg.getJMSReplyTo() != null
                                        ? jmsMsgSchema.toJmsMessageStructV1().getStruct(jmsMsgProp)
                                                .toString().getBytes()
                                        : null);

                        break;
                    case "jmsPriority":
                    case "jmsDeliveryMode":
                    case "jmsRetry_count":
                        header.addBytes(jmsMsgProp, jmsMsgSchema.toJmsMessageStructV1()
                                .getInt32(jmsMsgProp).toString().getBytes());
                        break;
                    case "jmsExpiration":
                    case "jmsTimestamp":
                        header.addBytes(jmsMsgProp, jmsMsgSchema.toJmsMessageStructV1()
                                .getInt64(jmsMsgProp).toString().getBytes());
                        break;
                    case "jmsType":
                        header.addBytes(jmsMsgProp,
                                aqJmsMsg.getJMSType() != null
                                        ? jmsMsgSchema.toJmsMessageStructV1().getString(jmsMsgProp)
                                                .getBytes()
                                        : null);
                        break;
                    case "jmsRedelivered":
                        header.addBytes(jmsMsgProp, jmsMsgSchema.toJmsMessageStructV1()
                                .getBoolean(jmsMsgProp).toString().getBytes());
                        break;
                    case "jmsProperties":
                        header.addBytes(jmsMsgProp,
                                JmsMessage.propertiesMap(aqJmsMsg).toString().getBytes());
                        break;
                    default:
                        log.warn("Invalid JMS Message Property: {}", jmsMsgProp);
                        break;
                    }
                }
            }
        }
        return header;
    }

    /**
     * Process JmsBytesMessage by first determining if the MessageVersion is version 2 which means
     * that header information can possibly be stored in the payload. If dealing with a message with
     * a version of 2 the header information from the payload will be extracted and stored in the
     * TxEventQSourceRecord as header values. If the message is not a version 2 message then only
     * the message and if correlation key is specified will be placed into the TxEventQSourceRecord.
     * 
     * @param msgId          The message Id of the message that was dequeued.
     * @param msg            The message that was dequeued.
     * @param kafkaTopic     The Kafka topic to put the message in.
     * @param correlationId  The correlation key from the message.
     * @param correlationKey A Key object that is used to create a schema for the correlation id
     *                       value.
     * @param shardNum       The shard number that the message is stored in the TxEventQ.
     * @return A TxEventQSourceRecord containing the JmsBytesMessage message information, Kafka
     *         topic, key if available in message, headers if available in message, and partition
     *         location for Kafka to store the message at.
     * @throws JMSException
     * @throws SQLException
     */
    private TxEventQSourceRecord processJmsBytesMessage(String msgId, Message msg,
            String kafkaTopic, String correlationId, Key correlationKey, int shardNum)
            throws JMSException, SQLException {
        log.trace("[{}] Entry {}.processJmsBytesMessage", this.conn, this.getClass().getName());

        int messageVersion = 1;
        byte[] keyArray = null;
        byte[] valueArray = null;
        int keyLen = 0;
        int valueLen = 0;
        Headers header = new org.apache.kafka.connect.header.ConnectHeaders();
        AQjmsBytesMessage byteMessage = (AQjmsBytesMessage) msg;

        /*
         * Attempts to get the MESSAGEVERSION. If the MESSAGEVERSION is not set it will be assumed
         * that the MESSAGEVERSION is 1 and no header information has been stored. If the
         * MESSAGEVERSION is 2 then header information maybe available in the payload.
         */
        try {
            messageVersion = byteMessage.getIntProperty("AQINTERNAL_MESSAGEVERSION");
        } catch (Exception e) {
            messageVersion = 1;
        }

        /*
         * Received Byte Payload in below format: | KEY LENGTH (4 Bytes Fixed) | KEY | | VALUE
         * LENGTH (4 BYTES FIXED) | VALUE | | HEADER NAME LENGTH(4 BYTES FIXED) | HEADER NAME | |
         * HEADER VALUE LENGTH (4 BYTES FIXED) | HEADER VALUE | | HEADER NAME LENGTH(4 BYTES FIXED)
         * | HEADER NAME | | HEADER VALUE LENGTH (4 BYTES FIXED) | HEADER VALUE |
         * 
         * For records with null key , KEY LENGTH is set to 0. For records with null value, VALUE
         * LENGTH is set to 0. Number of headers are set in property "AQINTERNAL_HEADERCOUNT"
         * 
         */
        if (messageVersion == 2) {
            byte[] payloadArray = byteMessage.getBytesData();
            byte[] bLength = new byte[DLENGTH_SIZE];

            // Read Key First
            ByteBuffer pBuffer = ByteBuffer.wrap(payloadArray);
            pBuffer.get(bLength, 0, DLENGTH_SIZE);
            keyLen = MessageUtils.convertToInt(bLength);
            keyArray = new byte[keyLen];
            pBuffer.get(keyArray, 0, keyLen);

            log.debug("KeyArray value: {}", Arrays.toString(keyArray));
            // Get Actual Payload
            pBuffer.get(bLength, 0, DLENGTH_SIZE);
            valueLen = MessageUtils.convertToInt(bLength);

            valueArray = new byte[valueLen];
            pBuffer.get(valueArray, 0, valueLen);

            int hCount = 0;
            try {
                hCount = byteMessage.getIntProperty("AQINTERNAL_HEADERCOUNT");
            } catch (Exception e) {
                hCount = 0;
            }
            int hKeyLen = 0;
            int hValueLen = 0;

            for (int i = 0; i < hCount; i++) {
                pBuffer.get(bLength, 0, DLENGTH_SIZE);
                hKeyLen = MessageUtils.convertToInt(bLength);
                if (hKeyLen > 0) {
                    byte[] hKeyArray = new byte[hKeyLen];
                    pBuffer.get(hKeyArray, 0, hKeyLen);
                    String hKey = new String(hKeyArray);
                    pBuffer.get(bLength, 0, DLENGTH_SIZE);
                    hValueLen = MessageUtils.convertToInt(bLength);
                    byte[] hValueArray = new byte[hValueLen];
                    pBuffer.get(hValueArray, 0, hValueLen);

                    // Add the header if it is not on the deny list.
                    if (this.denylistedHeaders.isEmpty()
                            || !this.denylistedHeaders.contains(hKey)) {
                        header.addBytes(hKey, hValueArray);
                    }
                }
            }
        }

        JmsMessage jmsMsg = new JmsMessage(byteMessage, messageVersion,
                valueLen != 0 ? valueArray : null);

        addJMSMessagePropertyToKafkaHeader(byteMessage, jmsMsg, header);

        log.debug("[{}] Processing JMS Bytes message:[msgId: {}, shardNum: {}, correlationId: {}]",
                this.conn, msgId, shardNum, correlationId);

        log.trace("[{}] Exit {}.processJmsBytesMessage", this.conn, this.getClass().getName());
        if (messageVersion == 2) {
            Key msgV2CorrelationKey = new Key(new String(keyArray, StandardCharsets.UTF_8));
            return new TxEventQSourceRecord(null, null, kafkaTopic,
                    this.mapShardToKafkaPartition ? shardNum / 2 : null,
                    this.useSchemaForJmsMsg && keyLen != 0 ? Key.SCHEMA_KEY_V1 : null,
                    this.useSchemaForJmsMsg && keyLen != 0 ? msgV2CorrelationKey.toKeyStructV1()
                            : keyLen != 0 ? keyArray : null,
                    this.useSchemaForJmsMsg ? JmsMessage.SCHEMA_JMSMESSAGE_V1 : null,
                    this.useSchemaForJmsMsg ? jmsMsg.toJmsMessageStructV1()
                            : valueLen != 0 ? valueArray : null,
                    null, header, TxEventQSourceRecord.PayloadType.JMS_BYTES,
                    byteMessage.getJMSMessageIDAsBytes());
        } else {
            return new TxEventQSourceRecord(null, null, kafkaTopic,
                    this.mapShardToKafkaPartition ? shardNum / 2 : null,
                    this.useSchemaForJmsMsg && correlationId != null ? Key.SCHEMA_KEY_V1 : null,
                    this.useSchemaForJmsMsg && correlationId != null
                            ? correlationKey.toKeyStructV1()
                            : correlationId,
                    this.useSchemaForJmsMsg ? JmsMessage.SCHEMA_JMSMESSAGE_V1 : null,
                    this.useSchemaForJmsMsg ? jmsMsg.toJmsMessageStructV1()
                            : byteMessage.getBytesData(),
                    null, header, TxEventQSourceRecord.PayloadType.JMS_BYTES,
                    byteMessage.getJMSMessageIDAsBytes());
        }
    }

    /**
     * Gets the shard number that the message is located in the TxEventQ by looking at the message
     * Id string.
     * 
     * @param messageId The message Id string to obtain the shard number information from.
     * @return The shard number the message is being stored at.
     */
    private static int getShardId(String messageId) {
        if (messageId == null || messageId.length() != 32)
            return -1;
        String shardIdStr = messageId.substring(16, 24);
        String endian = messageId.substring(26, 28);
        if (endian.equals("66")) {
            char[] sId = shardIdStr.toCharArray();
            char swap = 0;
            // Pair wise reverse
            for (int i = 0; i < sId.length; i = i + 2) {
                swap = sId[i];
                sId[i] = sId[i + 1];
                sId[i + 1] = swap;
            }
            // Reverse the String
            shardIdStr = new StringBuilder(new String(sId)).reverse().toString();
        }
        return Integer.parseInt(shardIdStr, 16);
    }

    /**
     * Gets the number of shards for the specified queue.
     * 
     * @param queue The queue to get the number of shards for.
     * @return The number of shards.
     */
    public int getNumOfShardsForQueue(String queue) {
        log.trace("[{}] Entry {}.getNumOfShardsForQueue", this.conn, this.getClass().getName());

        int numShard;
        try (CallableStatement getnumshrdStmt = this.conn
                .prepareCall("{call dbms_aqadm.get_queue_parameter(?,?,?)}")) {
            getnumshrdStmt.setString(1, queue);
            getnumshrdStmt.setString(2, "SHARD_NUM");
            getnumshrdStmt.registerOutParameter(3, Types.INTEGER);
            getnumshrdStmt.execute();
            numShard = getnumshrdStmt.getInt(3);
        } catch (SQLException e) {
            throw new ConnectException(
                    "Error attempting to get number of shards for the specified queue: "
                            + e.getMessage());
        }

        log.debug("Number of shards for {}: {}", queue, numShard);
        log.trace("[{}] Exit {}.getNumOfShardsForQueue", this.conn, this.getClass().getName());
        return numShard;
    }

    /**
     * Determines whether the queue is using sticky dequeue or not.
     * 
     * @param queue The queue to get the sticky dequeue for.
     * @return True if the queue is using sticky dequeue, otherwise false.
     */
    public boolean isStickyDequeue(String queue) {
        log.trace("[{}] Entry {}.isStickyDequeue", this.conn, this.getClass().getName());

        int stickyDequeue;
        try (CallableStatement getnumshrdStmt = this.conn
                .prepareCall("{call dbms_aqadm.get_queue_parameter(?,?,?)}")) {
            getnumshrdStmt.setString(1, queue);
            getnumshrdStmt.setString(2, "STICKY_DEQUEUE");
            getnumshrdStmt.registerOutParameter(3, Types.INTEGER);
            getnumshrdStmt.execute();
            stickyDequeue = getnumshrdStmt.getInt(3);
        } catch (SQLException e) {
            throw new ConnectException(
                    "Error attempting to get sticky dequeue value for the specified queue: "
                            + e.getMessage());
        }

        log.debug("Sticky dequeue for {}: {}", queue, stickyDequeue);
        log.trace("[{}] Exit {}.isStickyDequeue", this.conn, this.getClass().getName());

        return stickyDequeue == 1;
    }

    /**
     * Gets the partition size for the specified Kafka topic.
     * 
     * @param topic The Kafka topic to get the partition size for.
     * @return The size of the partition for the specified topic.
     */
    public int getKafkaTopicPartitionSize(String topic) {
        log.trace("[{}] Entry {}.getKafkaTopicPartitionSize", this.conn, this.getClass().getName());
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                this.config.getList(TxEventQConnectorConfig.BOOTSTRAP_SERVERS_CONFIG));
        Map<String, TopicDescription> kafkaTopic;
        int partitionSize = 0;
        try (AdminClient adminClient = AdminClient.create(properties);) {
            kafkaTopic = adminClient.describeTopics(Collections.singletonList(topic))
                    .allTopicNames().get();
            partitionSize = kafkaTopic.get(topic).partitions().size();
        } catch (InterruptedException e) {
            log.error(
                    "An InterruptedException occurred, unable to get Kafka partition size for topic {}: {}",
                    topic, e);
            // Restore interrupted state
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            throw new ConnectException("Unable to get Kafka partition size: " + e.getMessage());
        }
        log.trace("[{}] Exit {}.getKafkaTopicPartitionSize, retval={}", this.conn,
                this.getClass().getName(), partitionSize);
        return partitionSize;
    }

    /**
     * Dequeues a specified number if JMS bytes messages from the TxEventQ and creates a list of
     * SourceRecords that will be used by Kafka to populate the specified Kafka topic.
     * 
     * @param batchSize The maximum number of messages to dequeue.
     * @return A list of SourceRecords to be used by Kafka.
     */
    public List<SourceRecord> receiveJmsMessages(int batchSize) {
        log.trace("[{}]  Entry {}.receiveJmsMessages", this.conn, this.getClass().getName());
        Message[] jmsMesg = null;
        List<SourceRecord> records = new ArrayList<>();
        SourceRecord sr = null;

        /* Dequeue */
        try {
            if (topicDurSubscr1 != null) {
                jmsMesg = ((AQjmsConsumer) topicDurSubscr1).bulkReceive(batchSize,
                        (long) batchSize * 1000);

                if (jmsMesg != null && jmsMesg.length != 0) {
                    this.inflight = true;
                    for (int i = 0; i < jmsMesg.length; i++) {
                        String msgId = jmsMesg[i].getJMSMessageID();
                        sr = processJmsMessage(msgId, jmsMesg[i]);
                        log.debug("The returned record is: {}", sr);
                        records.add(sr);
                    }
                }
            }
        } catch (JMSException | SQLException e) {
            handleException(e);
            records.clear();
        } catch (final ConnectException exc) {
            attemptRollback();
            throw exc;
        }

        log.trace("[{}]  Exit {}.receiveJmsMessages, retvalArrSize={}", this.conn,
                this.getClass().getName(), records.size());

        return records;
    }

    /**
     * Handles exceptions from TxEventQ. Some exceptions are treated as retriable meaning that the
     * connector can keep running and just trying again is likely to fix things.
     */
    private ConnectException handleException(final Throwable exc) {
        log.trace("[{}]  Entry {}.handleException", this.conn, this.getClass().getName());
        boolean isRetriable = false;
        boolean mustClose = true;

        int errorCode = getErrorCode(exc);

        switch (errorCode) {
        /*
         * These reason codes indicate that the connection needs to be closed, but just retrying
         * later will probably recover
         */
        case Constants.ORA_17002:
        case Constants.ORA_17008:
        case Constants.ORA_12541:
        case Constants.ORA_17868:
        case Constants.ORA_01033:
        case Constants.ORA_01034:
        case Constants.ORA_01089:
        case Constants.ORA_24221:
        case Constants.ORA_25348:
        case Constants.ORA_01109:
        case Constants.JMS_131:
        case Constants.ORA_17009:
        case Constants.ORA_17800:
        case Constants.ORA_01017:
        case Constants.ORA_18730:
        case Constants.ORA_03113:
        case Constants.ORA_12521:
            isRetriable = true;
            break;
        case Constants.ORA_25228:
        case Constants.ORA_17410:
            isRetriable = true;
            mustClose = false;
            break;
        default:
            isRetriable = false;
            mustClose = false;
            break;

        }

        if (mustClose) {
            // Delay so that repeated reconnect loops don't run too fast
            try {
                Thread.sleep(reconnectDelayMillisMax);
            } catch (final InterruptedException ie) {
                // Restore interrupted state...
                Thread.currentThread().interrupt();
            }

            closeConnectionInternal();
        }

        if (isRetriable) {
            log.trace("[{}]:[{}]  Exit {}.handleException", Thread.currentThread().getId(),
                    this.conn, this.getClass().getName());
            return new RetriableException(exc);
        }

        log.trace("[{}]  Exit {}.handleException", this.conn, this.getClass().getName());
        return new ConnectException(exc);
    }

    /**
     * Checks the exception and gets the error code from it.
     * 
     * @param exc The exception to get the error code from.
     * @return The error code from the exception.
     */
    private int getErrorCode(final Throwable exc) {
        log.trace("[{}]  Entry {}.getErrorCode", this.conn, this.getClass().getName());
        int errorCode = -1;

        if (exc instanceof SQLException) {
            log.debug("In instanceof SQLException");
            final SQLException sqlExcep = (SQLException) exc;
            log.error("{}:[{}] {}", sqlExcep.getClass().getName(), sqlExcep.getErrorCode(),
                    sqlExcep.getMessage());
            errorCode = sqlExcep.getErrorCode();

        } else if (exc instanceof JMSException) {
            log.debug("In instanceof JMSException");
            final JMSException jmse = (JMSException) exc;
            Throwable e = jmse.getCause();
            if (e != null) {
                if (e instanceof SQLRecoverableException) {
                    log.debug("In instanceof SQLRecoverableException");
                    final SQLRecoverableException sqlre = (SQLRecoverableException) e;
                    log.error("{} caused by {}: [{}] {}", jmse.getClass().getName(),
                            sqlre.getClass().getName(), sqlre.getErrorCode(), sqlre.getMessage());
                    errorCode = sqlre.getErrorCode();
                } else if (e instanceof SQLException) {
                    log.debug("In instanceof SQLException");
                    final SQLException sqlExcep = (SQLException) e;
                    log.error("{} caused by {}: [{}] {}", jmse.getClass().getName(),
                            sqlExcep.getClass().getName(), sqlExcep.getErrorCode(),
                            sqlExcep.getMessage());
                    errorCode = sqlExcep.getErrorCode();
                } else if (e instanceof AQOracleSQLException) {
                    log.debug("In instanceof AQOracleSQLException");
                    final AQOracleSQLException aqOracleSqlExcep = (AQOracleSQLException) e;
                    log.error("{} caused by {}: [{}] {}", jmse.getClass().getName(),
                            aqOracleSqlExcep.getClass().getName(), aqOracleSqlExcep.getErrorCode(),
                            aqOracleSqlExcep.getMessage());
                    errorCode = aqOracleSqlExcep.getErrorCode();
                }
            } else {
                log.error("{}:[{}] {}", jmse.getClass().getName(), jmse.getErrorCode(),
                        jmse.getMessage());

                if (jmse.getErrorCode() != null) {
                    errorCode = Integer.parseInt(jmse.getErrorCode());
                } else {
                    if (jmse.getMessage().contains("ORA-")) {
                        int indexOfOraPhrase = jmse.getMessage().indexOf("ORA-");
                        String errorNum = jmse.getMessage().substring(indexOfOraPhrase,
                                indexOfOraPhrase + 9);
                        String[] splitOraPhrase = errorNum.split("-");
                        log.debug("Ora error from error message: [{}]", errorNum);
                        if (splitOraPhrase[1].startsWith("0")) {
                            String errorNumOnly = splitOraPhrase[1].substring(1,
                                    splitOraPhrase[1].length());
                            errorCode = Integer.parseInt(errorNumOnly);
                        } else {
                            errorCode = Integer.parseInt(splitOraPhrase[1]);
                        }
                    }
                }
            }
        }
        log.trace("[{}]  Exit {}.getErrorCode", this.conn, this.getClass().getName());
        return errorCode;
    }

    /**
     * Gets the type of queue table.
     * 
     * @param queueName The name of the queue table to check the type for.
     * @return A string value indicating queue tables type.
     */
    private String getQueueTableType(String queueName) {
        String queueTableType = null;

        log.trace("[{}] Entry {}.getQueueTableType", this.conn, this.getClass().getName());
        try (PreparedStatement statement = this.conn.prepareStatement(
                "SELECT type, queue_table from user_queue_tables where queue_table = ?")) {
            statement.setString(1, queueName);
            try (ResultSet rs = statement.executeQuery()) {
                if (rs.next()) {
                    queueTableType = rs.getString("type");
                }
            }
        } catch (SQLException e) {
            handleException(e);
        }

        log.trace("[{}]  Exit {}.getQueueTableType, retval={}", this.conn,
                this.getClass().getName(), queueTableType);
        return queueTableType;
    }

    /**
     * Closes this stream and releases any system resources associated with it. If the stream is
     * already closed then invoking this method has no effect.
     *
     * <p>
     * As noted in {@link AutoCloseable#close()}, cases where the close may fail require careful
     * attention. It is strongly advised to relinquish the underlying resources and to internally
     * <em>mark</em> the {@code Closeable} as closed, prior to throwing the {@code IOException}.
     *
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void close() throws IOException {
        log.trace("[{}] Entry {}.close", this.conn, this.getClass().getName());
        closeNow.set(true);
        closeConnectionInternal();
        log.trace("[{}]  Exit {}.close", this.conn, this.getClass().getName());
    }

    /**
     * Returns messages received from TxEventQ. Called process failed to transform the messages and
     * return them to Connector for producing to Kafka.
     */
    public void attemptRollback() {
        log.trace("[{}] Entry {}.attemptRollback", this.conn, this.getClass().getName());
        try {
            if (this.tSess != null) {
                this.tSess.rollback();
            }
        } catch (final JMSException ex) {
            log.error("Rollback failed.", ex);
        }
        log.trace("[{}]  Exit {}.attemptRollback", this.conn, this.getClass().getName());
    }

    /**
     * Commits the current transaction.
     */
    public void commit() {
        log.trace("[{}] Entry {}.commit", this.conn, this.getClass().getName());

        if (!connectConnectionInternal()) {
            return;
        }
        try {
            if (this.inflight) {
                this.inflight = false;

                log.debug("[{}] Attempting to Commit session transaction.", this.conn);

                if (this.tSess != null) {
                    this.tSess.commit();
                }
            }
        } catch (JMSException e) {
            handleException(e);
        } catch (final ConnectException exc) {
            attemptRollback();
            throw exc;
        }

        log.trace("[{}] Exit {}.commit", this.conn, this.getClass().getName());
    }

    /**
     * Converts a byte array to a hex string.
     * 
     * @param a The byte array to perform the conversion on.
     * @return The hex string.
     */
    private static String byteArrayToHex(byte[] a) {
        StringBuilder sb = new StringBuilder(a.length * 2);
        for (byte b : a)
            sb.append(String.format("%02x", b));
        return sb.toString();
    }
}