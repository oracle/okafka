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
import java.sql.CallableStatement;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;
import javax.jms.TopicSession;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import oracle.jdbc.OracleConnection;
import oracle.jdbc.aq.AQFactory;
import oracle.jdbc.aq.AQMessageProperties;
import oracle.jdbc.internal.JMSEnqueueOptions;
import oracle.jdbc.internal.JMSFactory;
import oracle.jdbc.internal.JMSMessage;
import oracle.jdbc.internal.JMSMessageProperties;
import oracle.jdbc.txeventq.kafka.connect.common.utils.Constants;
import oracle.jms.AQjmsBytesMessage;
import oracle.jms.AQjmsFactory;
import oracle.jms.AQjmsProducer;
import oracle.jms.AQjmsSession;

public class TxEventQProducer implements Closeable {
    protected static final Logger log = LoggerFactory.getLogger(TxEventQProducer.class);
    // Indicates whether connected to TxEventQ
    private boolean connected = false;

    private String jmsDeliveryModeStr = "JMSDeliveryMode";
    private String persistentStr = "PERSISTENT";
    private String aqInternalPartitionStr = "AQINTERNAL_PARTITION";

    private int numberOfProperties = 1;
    private int stringPropertyValueType = 27;
    private int numberPropertyValueType = 24;

    private String partialUserPropertiesStr = numberOfProperties + ","
            + aqInternalPartitionStr.length() + "," + aqInternalPartitionStr + ","
            + numberPropertyValueType + ",";

    private String mergeSqlStatement = "MERGE INTO " + TXEVENTQ$_TRACK_OFFSETS
            + " tab1 USING (SELECT ? kafka_topic_name, ? queue_name, ? queue_schema, ? partition)"
            + " tab2 ON (tab1.kafka_topic_name = tab2.kafka_topic_name AND tab1.queue_name = tab2.queue_name AND tab1.queue_schema = tab2.queue_schema AND tab1.partition = tab2.partition)"
            + " WHEN MATCHED THEN UPDATE SET offset=? WHEN NOT MATCHED THEN"
            + " INSERT (kafka_topic_name, queue_name, queue_schema, partition, offset) VALUES (?,?,?,?,?)";

    private String selectOffsetSqlStatement = "SELECT offset FROM " + TXEVENTQ$_TRACK_OFFSETS
            + " WHERE kafka_topic_name=? AND queue_name = ? AND queue_schema = ? AND partition=?";

    private PreparedStatement preparedMergeStatement;
    private PreparedStatement preparedSelectOffsetStatement;

    private JMSMessageProperties jmsMesgProp;

    private OracleConnection conn;

    private TopicConnectionFactory tcf;
    private TopicConnection tconn;
    private TopicSession tSess;
    private Topic topic;
    private MessageProducer tProducer;
    private TxEventQSinkConfig config = null;
    private long reconnectDelayMillis = RECONNECT_DELAY_MILLIS_MIN;
    private boolean isDatabaseRac;

    private static final long RECONNECT_DELAY_MILLIS_MIN = 64L;
    private static final long RECONNECT_DELAY_MILLIS_MAX = 8192L;

    private static final String TXEVENTQ$_TRACK_OFFSETS = "TXEVENTQ$_TRACK_OFFSETS";
    private static final int MINIMUM_VERSION = 21;

    public TxEventQProducer(TxEventQSinkConfig config) {
        this.config = config;

        try {
            jmsMesgProp = JMSFactory.createJMSMessageProperties();
        } catch (SQLException e) {
            throw new ConnectException(
                    "Unable to create JMS message properties: " + e.getMessage());
        }

        /*
         * We are trying to send JMS messages using JDBC APIs. JMS messages have Header and User
         * properties. Since we cannot directly use APIs in JDBC to set these, we construct the
         * Linearly formatted Header and User property strings as JMS does internally. Formatting
         * constructs a string for the Header and User properties as follows <number of properties,
         * property name length, property name, property value type, property value length, property
         * value,...>
         */
        String headerProperties = String.format("%1$d,%2$d,%3$s,%4$d,%5$d,%6$s", numberOfProperties,
                jmsDeliveryModeStr.length(), jmsDeliveryModeStr, stringPropertyValueType,
                persistentStr.length(), persistentStr);

        jmsMesgProp.setHeaderProperties(headerProperties);
        jmsMesgProp.setJMSMessageType(JMSMessageProperties.JMSMessageType.BYTES_MESSAGE);
    }

    /**
     * Uses the Oracle wallet to connect to the database.
     */
    public void connect() {
        log.trace("[{}] Entry {}.connect", Thread.currentThread().getId(),
                this.getClass().getName());

        try {
            System.setProperty("oracle.net.wallet_location",
                    this.config.getString(TxEventQSinkConfig.DATABASE_WALLET_CONFIG));
            System.setProperty("oracle.net.tns_admin",
                    this.config.getString(TxEventQSinkConfig.DATABASE_TNSNAMES_CONFIG));
            DriverManager.registerDriver(new oracle.jdbc.OracleDriver());
            String url = "jdbc:oracle:thin:@"
                    + this.config.getString(TxEventQSinkConfig.DATABASE_TNS_ALIAS_CONFIG);

            this.tcf = AQjmsFactory.getTopicConnectionFactory(url, null);
            this.tconn = this.tcf.createTopicConnection();
            this.tSess = this.tconn.createTopicSession(true, Session.CLIENT_ACKNOWLEDGE);
            this.conn = (OracleConnection) ((AQjmsSession) (this.tSess)).getDBConnection();
            this.tconn.start();

            versionCheck();

            String userName = this.conn.getUserName().toUpperCase();

            this.topic = ((AQjmsSession) (this.tSess)).getTopic(userName,
                    this.config.getString(TxEventQSinkConfig.TXEVENTQ_QUEUE_NAME).toUpperCase());
            this.tProducer = this.tSess.createProducer(this.topic);

            this.conn.setAutoCommit(false);
            this.preparedMergeStatement = conn.prepareStatement(this.mergeSqlStatement);
            this.preparedSelectOffsetStatement = conn
                    .prepareStatement(this.selectOffsetSqlStatement);
            this.connected = true;
            this.isDatabaseRac = isClusterDatabase();

            log.debug("[{}:{}] Oracle TxEventQ connection opened!", Thread.currentThread().getId(),
                    this.conn);
        } catch (SQLException | JMSException ex) {
            log.debug("[{}] Connection to TxEventQ could not be established",
                    Thread.currentThread().getId());
            handleException(ex);
        }

        log.trace("[{}]:[{}]  Exit {}.connect", Thread.currentThread().getId(), this.conn,
                this.getClass().getName());

    }

    /**
     * Internal method to connect to TxEventQ.
     */
    private void connectConnectionInternal() {
        log.trace("[{}] Entry {}.connectConnectionInternal", Thread.currentThread().getId(),
                this.getClass().getName());

        if (this.connected) {
            return;
        }

        try {
            System.setProperty("oracle.net.wallet_location",
                    this.config.getString(TxEventQSinkConfig.DATABASE_WALLET_CONFIG));
            System.setProperty("oracle.net.tns_admin",
                    this.config.getString(TxEventQSinkConfig.DATABASE_TNSNAMES_CONFIG));
            DriverManager.registerDriver(new oracle.jdbc.OracleDriver());
            String url = "jdbc:oracle:thin:@"
                    + this.config.getString(TxEventQSinkConfig.DATABASE_TNS_ALIAS_CONFIG);

            this.tcf = AQjmsFactory.getTopicConnectionFactory(url, null);
            this.tconn = this.tcf.createTopicConnection();
            this.tSess = this.tconn.createTopicSession(true, Session.CLIENT_ACKNOWLEDGE);
            this.conn = (OracleConnection) ((AQjmsSession) (this.tSess)).getDBConnection();
            this.tconn.start();

            versionCheck();

            String userName = this.conn.getUserName().toUpperCase();

            this.topic = ((AQjmsSession) (this.tSess)).getTopic(userName,
                    this.config.getString(TxEventQSinkConfig.TXEVENTQ_QUEUE_NAME).toUpperCase());
            this.tProducer = this.tSess.createProducer(this.topic);

            this.conn.setAutoCommit(false);
            this.preparedMergeStatement = conn.prepareStatement(this.mergeSqlStatement);
            this.preparedSelectOffsetStatement = conn
                    .prepareStatement(this.selectOffsetSqlStatement);
            this.reconnectDelayMillis = RECONNECT_DELAY_MILLIS_MIN;
            this.connected = true;
            this.isDatabaseRac = isClusterDatabase();

            log.debug("[{}:{}] Oracle TxEventQ connection opened!", Thread.currentThread().getId(),
                    this.conn);
        } catch (SQLException | JMSException ex) {
            // Delay slightly so that repeated reconnect loops don't run too fast
            try {
                Thread.sleep(this.reconnectDelayMillis);
            } catch (final InterruptedException ie) {
                // Restore interrupted state...
                Thread.currentThread().interrupt();

            }

            if (this.reconnectDelayMillis < RECONNECT_DELAY_MILLIS_MAX) {
                this.reconnectDelayMillis = this.reconnectDelayMillis * 2;
            }

            log.trace("[{}]  Exit {}.connectConnectionInternal, retval=false",
                    Thread.currentThread().getId(), this.getClass().getName());
            throw handleException(ex);
        }

        log.trace("[{}] Exit {}.connectConnectionInternal", Thread.currentThread().getId(),
                this.getClass().getName());

    }

    /**
     * Handles exceptions from TxEventQ. Some exceptions are treated as retriable meaning that the
     * connector can keep running and just trying again is likely to fix things.
     */
    private ConnectException handleException(final Throwable exc) {
        log.trace("[{}]:[{}]  Entry {}.handleException", Thread.currentThread().getId(), this.conn,
                this.getClass().getName());
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
            try {
                close();
            } catch (IOException e) {
                log.error("Unable to close connections.");
            }
        }

        if (isRetriable) {
            log.trace("[{}]:[{}]  Exit {}.handleException", Thread.currentThread().getId(),
                    this.conn, this.getClass().getName());
            return new RetriableException(exc);
        }

        log.trace("[{}]:[{}]  Exit {}.handleException", Thread.currentThread().getId(), this.conn,
                this.getClass().getName());
        return new ConnectException(exc);
    }

    /**
     * Checks the exception and gets the error code from it.
     * 
     * @param exc The exception to get the error code from.
     * @return The error code from the exception.
     */
    private int getErrorCode(final Throwable exc) {
        int errorCode = -1;

        if (exc instanceof SQLException) {
            final SQLException sqlExcep = (SQLException) exc;
            log.error("{}:[{}] {}", sqlExcep.getClass().getName(), sqlExcep.getErrorCode(),
                    sqlExcep.getMessage());
            errorCode = sqlExcep.getErrorCode();

        } else if (exc instanceof JMSException) {
            final JMSException jmse = (JMSException) exc;
            Throwable e = jmse.getCause();
            if (e != null) {
                if (e instanceof SQLRecoverableException) {
                    final SQLRecoverableException sqlre = (SQLRecoverableException) e;
                    log.error("{} caused by {}: [{}] {}", jmse.getClass().getName(),
                            sqlre.getClass().getName(), sqlre.getErrorCode(), sqlre.getMessage());
                    errorCode = sqlre.getErrorCode();
                } else if (e instanceof SQLException) {
                    final SQLException sqlExcep = (SQLException) e;
                    log.error("{} caused by {}: [{}] {}", jmse.getClass().getName(),
                            sqlExcep.getClass().getName(), sqlExcep.getErrorCode(),
                            sqlExcep.getMessage());
                    errorCode = sqlExcep.getErrorCode();
                }
            } else {
                log.error("{}:[{}] {}", jmse.getClass().getName(), jmse.getErrorCode(),
                        jmse.getMessage());

                if (jmse.getErrorCode() != null) {
                    errorCode = Integer.parseInt(jmse.getErrorCode());
                }
            }
        }
        return errorCode;
    }

    /**
     * Check whether the database is version 21 or later
     * 
     * @throws ConnectException If we cannot get the database metadata or if the database version is
     *                          less than 21
     */
    private void versionCheck() {
        DatabaseMetaData md = null;
        int version = 0;
        try {
            md = this.conn.getMetaData();
            version = md.getDatabaseMajorVersion();
            log.debug("DB Version: {}", version);

        } catch (SQLException e) {
            throw new ConnectException("Unable to obtain a database connection");
        }

        if (version < MINIMUM_VERSION) {
            throw new ConnectException(
                    "TxEventQ Connector requires Oracle Database 21c or greater");
        }
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
     * Gets the partition size for the specified Kafka topic.
     * 
     * @param topic The Kafka topic to get the partition size for.
     * @return The size of the partition for the specified topic.
     */
    public int getKafkaTopicPartitionSize(String topic) {
        log.trace("[{}]:[{}] Entry {}.getKafkaTopicPartitionSize,", Thread.currentThread().getId(),
                this.conn, this.getClass().getName());

        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                this.config.getList(TxEventQSinkConfig.BOOTSTRAP_SERVERS_CONFIG));
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

        log.trace("[{}]:[{}] Exit {}.getKafkaTopicPartitionSize,", Thread.currentThread().getId(),
                this.conn, this.getClass().getName());
        return partitionSize;
    }

    /**
     * Validates if the Kafka topic exist.
     * 
     * @param topic The Kafka topic to check existence for.
     * @return True if the Kafka topic exists false otherwise.
     */
    public boolean kafkaTopicExists(String topic) {
        log.trace("[{}]:[{}] Entry {}.kafkaTopicExists,", Thread.currentThread().getId(), this.conn,
                this.getClass().getName());

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
            log.error(
                    "An InterruptedException occurred, unable to validate if Kafka topic {} exist: {}",
                    topic, e);
            // Restore interrupted state
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            log.error(
                    "An ExecutionException occurred, unable to validate if Kafka topic {} exist: {}",
                    topic, e);
        }

        log.trace("[{}]:[{}] Exit {}.kafkaTopicExists,", Thread.currentThread().getId(), this.conn,
                this.getClass().getName());
        return currentTopicList != null && currentTopicList.contains(topic);
    }

    /**
     * Checks if the specified queue name exists in the database.
     * 
     * @param queueName The name of the queue to check existence for.
     * @return True if the queue exists otherwise false.
     * @throws SQLException
     */
    public boolean txEventQueueExists(String queueName) throws SQLException {
        log.trace("[{}]:[{}] Entry {}.txEventQueueExists,", Thread.currentThread().getId(),
                this.conn, this.getClass().getName());

        DatabaseMetaData meta = this.conn.getMetaData();
        try (ResultSet resultSet = meta.getTables(null, null, queueName,
                new String[] { "TABLE" })) {
            log.trace("[{}] Exit {}.txEventQueueExists,", Thread.currentThread().getId(),
                    this.getClass().getName());
            return resultSet.next();
        }
    }

    /**
     * Query the database to check if it is a cluster database or not.
     * 
     * @return True if cluster database, false otherwise.
     * @throws SQLException
     */
    private boolean isClusterDatabase() throws SQLException {
        log.trace("[{}]:[{}] Entry {}.isClusterDatabase,", Thread.currentThread().getId(),
                this.conn, this.getClass().getName());

        String getIsClusterDatabaseVal = "SELECT VALUE FROM V$PARAMETER WHERE UPPER(NAME) = 'CLUSTER_DATABASE'";
        boolean isRac;

        try (Statement stmt1 = this.conn.createStatement();
                ResultSet rs1 = stmt1.executeQuery(getIsClusterDatabaseVal);) {
            rs1.next();
            isRac = rs1.getBoolean("VALUE");
        }

        log.trace("[{}]:[{}] Exit {}.isClusterDatabase, isRac=[{}]", Thread.currentThread().getId(),
                this.conn, this.getClass().getName(), isRac);

        return isRac;
    }

    /**
     * Creates the TXEVENTQ_TRACK_OFFSETS table if it does not already exist.
     * 
     * @return True if the table exist or false if it does not exist.
     */
    public boolean createOffsetInfoTable() {
        log.trace("[{}]:[{}] Entry {}.createOffsetInfoTable,", Thread.currentThread().getId(),
                this.conn, this.getClass().getName());
        boolean offsetTableExist = false;

        String createTableQuery = "CREATE TABLE IF NOT EXISTS " + TXEVENTQ$_TRACK_OFFSETS
                + "(kafka_topic_name varchar2(128) NOT NULL, queue_name varchar2(128) NOT NULL, queue_schema varchar2(128) NOT NULL, partition int NOT NULL, offset number NOT NULL, primary key(kafka_topic_name, queue_name, queue_schema, partition))";
        try (PreparedStatement statement = this.conn.prepareStatement(createTableQuery);
                ResultSet rs = statement.executeQuery();) {
            DatabaseMetaData meta = this.conn.getMetaData();
            ResultSet resultSet = meta.getTables(null, null, TXEVENTQ$_TRACK_OFFSETS,
                    new String[] { "TABLE" });
            if (resultSet.next()) {
                log.debug("The TXEVENTQ$_TRACK_OFFSETS table successfully created.");
                offsetTableExist = true;
            }
            resultSet.close();
        } catch (SQLException e) {
            throw handleException(e);
        }

        log.trace("[{}]:[{}] Exit {}.createOffsetInfoTable,", Thread.currentThread().getId(),
                this.conn, this.getClass().getName());
        return offsetTableExist;
    }

    /**
     * Gets the number of shards for the specified queue.
     * 
     * @param queue The queue to get the number of shards for.
     * @return The number of shards.
     * @throws SQLException
     */
    public int getNumOfShardsForQueue(String queue) {
        log.trace("[{}]:[{}] Entry {}.getNumOfShardsForQueue,", Thread.currentThread().getId(),
                this.conn, this.getClass().getName());

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

        log.trace("[{}]:[{}] Exit {}.getNumOfShardsForQueue,", Thread.currentThread().getId(),
                this.conn, this.getClass().getName());
        return numShard;
    }

    /**
     * Enqueues the message from the SinkRecord into the specified TxEventQ.
     * 
     * @param queueName  The name of the TxEventQ to enqueue message to.
     * @param sinkRecord The message to be enqueued.
     * @throws SQLException
     */
    public void enqueueMessage(String queueName, SinkRecord sinkRecord) throws SQLException {

        log.trace("[{}]:[{}] Entry {}.enqueueMessage,", Thread.currentThread().getId(), this.conn,
                this.getClass().getName());

        if (sinkRecord.kafkaPartition() != null) {
            String id = "" + 2 * sinkRecord.kafkaPartition();
            jmsMesgProp.setUserProperties(
                    partialUserPropertiesStr + id.length() + "," + 2 * sinkRecord.kafkaPartition());
        }

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
        ((oracle.jdbc.internal.OracleConnection) this.conn).jmsEnqueue(queueName, opt, mesg,
                aqProp);

        log.trace("[{}]:[{}] Exit {}.enqueueMessage,", Thread.currentThread().getId(), this.conn,
                this.getClass().getName());
    }

    /**
     * Enqueues an array of messages from the SinkRecord into the specified TxEventQ.
     *
     * @param queueName   The name of the TxEventQ to enqueue message to.
     * @param records     The message to be enqueued.
     * @param msgProducer The message producer that is going to be used to send the message to the
     *                    destination.
     * @throws JMSException
     */
    public void enqueueBulkMessage(String queueName, Collection<SinkRecord> records,
            MessageProducer msgProducer) throws JMSException {
        final List<AQjmsBytesMessage> messages = new ArrayList<>();
        AQjmsBytesMessage[] msgs = null;
        int[] deliveryMode = new int[records.size()];
        int[] priorities = new int[records.size()];

        log.trace("[{}]:[{}] Entry {}.enqueueBulkMessage,", Thread.currentThread().getId(),
                this.conn, this.getClass().getName());

        connectConnectionInternal();

        int i = 0;
        for (Iterator<SinkRecord> sinkRecord = records.iterator(); sinkRecord.hasNext();) {
            SinkRecord sr = sinkRecord.next();
            log.debug(
                    "[{}:{}] Enqueuing record from partition {} at offset {} with timestamp of {}.",
                    Thread.currentThread().getId(), this.conn, sr.kafkaPartition(),
                    sr.kafkaOffset(), sr.timestamp());

            AQjmsBytesMessage msg = createBytesMessage(tSess, sr);

            msg.setJMSDeliveryMode(DeliveryMode.PERSISTENT);
            msg.setJMSPriority(4);
            deliveryMode[i] = msg.getJMSDeliveryMode();
            priorities[i] = msg.getJMSPriority();

            messages.add(msg);
            i++;
        }

        log.debug("Total number of messages to enqueue: {}", messages.size());

        msgs = messages.toArray(new AQjmsBytesMessage[0]);
        ((AQjmsProducer) msgProducer).bulkSend(msgs, deliveryMode, priorities, null);

        log.trace("[{}]:[{}] Exit {}.enqueueBulkMessage,", Thread.currentThread().getId(),
                this.conn, this.getClass().getName());
    }

    /**
     * Creates AQjmsBytesMessage from ByteBuffer's key, value and headers.
     * 
     * @param session The topic session.
     * @param sinkRec The message.
     */
    private AQjmsBytesMessage createBytesMessage(TopicSession session, SinkRecord sinkRec)
            throws JMSException {
        AQjmsBytesMessage msg = null;
        msg = (AQjmsBytesMessage) (session.createBytesMessage());
        byte[] nullPayload = null;

        if (sinkRec.value() != null) {
            msg.writeBytes((sinkRec.value().toString()).getBytes());
        } else {
            msg.writeBytes(nullPayload);
        }

        if (sinkRec.key() != null) {
            msg.setJMSCorrelationID(sinkRec.key().toString());
        }
        msg.setStringProperty("AQINTERNAL_PARTITION",
                Integer.toString(sinkRec.kafkaPartition() * 2));
        return msg;
    }

    /**
     * Enqueues the Kafka records into the specified TxEventQ. Also keeps track of the offset for a
     * particular topic and partition in database table TXEVENTQ_TRACK_OFFSETS.
     * 
     * @param records The records to enqueue into the TxEventQ.
     */
    public void put(Collection<SinkRecord> records) {
        log.trace("[{}]:[{}] Entry {}.put,", Thread.currentThread().getId(), this.conn,
                this.getClass().getName());

        connectConnectionInternal();

        try {

            if (!this.isDatabaseRac) {
                log.debug("Performing bulk enqueue because not RAC database");
                enqueueBulkMessage(this.config.getString(TxEventQSinkConfig.TXEVENTQ_QUEUE_NAME),
                        records, tProducer);
            }

            Map<String, Map<Integer, Long>> topicInfoMap = new HashMap<>();
            for (SinkRecord sinkRecord : records) {
                if (this.isDatabaseRac) {
                    log.debug("Performing single enqueue because RAC database");
                    enqueueMessage(this.config.getString(TxEventQSinkConfig.TXEVENTQ_QUEUE_NAME),
                            sinkRecord);
                }

                topicInfoMap.computeIfPresent(sinkRecord.topic(), (k, v) -> {
                    v.computeIfPresent(sinkRecord.kafkaPartition(),
                            (x, y) -> sinkRecord.kafkaOffset());
                    return v;
                });

                topicInfoMap.computeIfAbsent(sinkRecord.topic(), k -> (new HashMap<>()))
                        .put(sinkRecord.kafkaPartition(), sinkRecord.kafkaOffset());

            }

            for (Map.Entry<String, Map<Integer, Long>> topicEntry : topicInfoMap.entrySet()) {
                String topicKey = topicEntry.getKey();
                Map<Integer, Long> offsetInfoValue = topicEntry.getValue();
                for (Map.Entry<Integer, Long> offsetInfoEntry : offsetInfoValue.entrySet()) {
                    setOffsetInfoInDatabase(this.preparedMergeStatement, topicKey,
                            this.config.getString(TxEventQSinkConfig.TXEVENTQ_QUEUE_NAME),
                            this.config.getString(TxEventQSinkConfig.TXEVENTQ_QUEUE_SCHEMA),
                            offsetInfoEntry.getKey(), offsetInfoEntry.getValue());
                }
            }

            this.conn.commit();

        } catch (SQLException | JMSException e) {
            throw handleException(e);
        }

        log.trace("[{}]:[{}] Exit {}.put,", Thread.currentThread().getId(), this.conn,
                this.getClass().getName());
    }

    /**
     * Populates the TXEVENTQ$_TRACK_OFFSETS table with the kafka topic, TxEventQ queue name, schema
     * for the queue, partition, and offset information of the messages that have been enqueued.
     * 
     * @param mergePrepareStatement The prepared statement containing the sql merge query for the
     *                              TXEVENTQ$_TRACK_OFFSETS
     * @param topic                 The kafka topic name.
     * @param queueName             The TxEventQ queue name.
     * @param queueSchema           The schema for the queue.
     * @param partition             The partition number.
     * @param offset                The offset value.
     * @throws SQLException
     */
    private void setOffsetInfoInDatabase(PreparedStatement mergePrepareStatement, String topic,
            String queueName, String queueSchema, int partition, long offset) throws SQLException {
        log.trace("[{}]:[{}] Entry {}.setOffsetInfoInDatabase,", Thread.currentThread().getId(),
                this.conn, this.getClass().getName());

        mergePrepareStatement.setString(1, topic);
        mergePrepareStatement.setString(2, queueName);
        mergePrepareStatement.setString(3, queueSchema);
        mergePrepareStatement.setInt(4, partition);
        mergePrepareStatement.setLong(5, offset + 1);
        mergePrepareStatement.setString(6, topic);
        mergePrepareStatement.setString(7, queueName);
        mergePrepareStatement.setString(8, queueSchema);
        mergePrepareStatement.setInt(9, partition);
        mergePrepareStatement.setLong(10, offset + 1);
        mergePrepareStatement.execute();

        log.trace("[{}]:[{}] Exit {}.setOffsetInfoInDatabase,", Thread.currentThread().getId(),
                this.conn, this.getClass().getName());
    }

    /**
     * Gets the offset for the specified kafka topic, TxEventQ queue name, schema, and partition.
     * The offset will be used to determine which message to start consuming.
     * 
     * @param topic       The kafka topic name.
     * @param queueName   The TxEventQ queue name.
     * @param queueSchema The schema for the queue.
     * @param partition   The partition number.
     * @return The offset value.
     */
    public long getOffsetInDatabase(String topic, String queueName, String queueSchema,
            int partition) {
        log.trace("[{}]:[{}] Entry {}.getOffsetInDatabase,", Thread.currentThread().getId(),
                this.conn, this.getClass().getName());
        long offsetVal = 0;
        try {
            this.preparedSelectOffsetStatement.setString(1, topic);
            this.preparedSelectOffsetStatement.setString(2, queueName);
            this.preparedSelectOffsetStatement.setString(3, queueSchema);
            this.preparedSelectOffsetStatement.setInt(4, partition);

            try (ResultSet rs = this.preparedSelectOffsetStatement.executeQuery()) {
                if (rs.next()) {
                    offsetVal = rs.getLong("offset");
                }
            }
        } catch (SQLException e) {
            throw new ConnectException("Error getting the offset value: " + e.getMessage());
        }

        log.trace("[{}]:[{}] Exit {}.getOffsetInDatabase,", Thread.currentThread().getId(),
                this.conn, this.getClass().getName());
        return offsetVal;

    }

    @Override
    public void close() throws IOException {
        log.trace("[{}] Entry {}.close,", Thread.currentThread().getId(),
                this.getClass().getName());
        try {
            if (this.tSess != null) {
                this.tSess.rollback();
            }
        } catch (JMSException e) {
            log.error("{}: {}", e.getClass().getName(), e);
        }

        try {
            if (this.preparedMergeStatement != null) {
                log.debug("preparedMergeStatement will be closed.");
                this.preparedMergeStatement.close();
            }
        } catch (SQLException e) {
            log.error("{}: {}", e.getClass().getName(), e);
        }

        try {
            if (this.preparedSelectOffsetStatement != null) {
                log.debug("preparedSelectOffsetStatement will be closed.");
                this.preparedSelectOffsetStatement.close();
            }
        } catch (SQLException e) {
            log.error("{}: {}", e.getClass().getName(), e);
        }

        try {
            this.connected = false;

            if (tSess != null) {
                log.debug("Session will be closed.");
                tSess.close();
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

        log.trace("[{}] Exit {}.close,", Thread.currentThread().getId(), this.getClass().getName());
    }
}
