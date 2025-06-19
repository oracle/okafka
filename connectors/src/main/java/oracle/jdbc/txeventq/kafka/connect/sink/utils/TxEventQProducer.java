/*
** Kafka Connect for TxEventQ.
**
** Copyright (c) 2023, 2024 Oracle and/or its affiliates.
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
import java.util.Map.Entry;
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

import oracle.AQ.AQOracleSQLException;
import oracle.jdbc.OracleConnection;
import oracle.jdbc.aq.AQFactory;
import oracle.jdbc.aq.AQMessageProperties;
import oracle.jdbc.internal.JMSEnqueueOptions;
import oracle.jdbc.internal.JMSFactory;
import oracle.jdbc.internal.JMSMessage;
import oracle.jdbc.internal.JMSMessageProperties;
import oracle.jdbc.pool.OracleDataSource;
import oracle.jdbc.txeventq.kafka.connect.common.utils.Constants;
import oracle.jdbc.txeventq.kafka.connect.common.utils.Node;
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

    private Map<Integer, Integer> partitionInstanceOwner = new HashMap<>();
    private Map<Integer, String> instances = new HashMap<>();
    private Map<Integer, MessageProducerForInstance> messageProducerForInstanceMaps = new HashMap<>();

    private static final long RECONNECT_DELAY_MILLIS_MIN = 64L;
    private static final long RECONNECT_DELAY_MILLIS_MAX = 8192L;

    private static final String TXEVENTQ$_TRACK_OFFSETS = "TXEVENTQ$_TRACK_OFFSETS";
    private static final int MINIMUM_VERSION = 21;
    private boolean errorInSessCommitProcess = false;
    private boolean isClusterDatabase = false;
    private String userName;

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
        log.trace("Entry {}.connect", this.getClass().getName());

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
            this.conn.setAutoCommit(false);
            this.tconn.start();

            versionCheck();

            this.userName = this.conn.getUserName().toUpperCase();

            this.topic = ((AQjmsSession) (this.tSess)).getTopic(this.userName,
                    this.config.getString(TxEventQSinkConfig.TXEVENTQ_QUEUE_NAME).toUpperCase());
            this.tProducer = this.tSess.createProducer(this.topic);

            this.preparedMergeStatement = this.conn.prepareStatement(this.mergeSqlStatement);
            this.preparedSelectOffsetStatement = this.conn
                    .prepareStatement(this.selectOffsetSqlStatement);
            this.connected = true;

            this.isClusterDatabase = isClusterDatabase();

            log.debug("[{}:{}] Oracle TxEventQ connection opened!", Thread.currentThread().getId(),
                    this.conn);
        } catch (SQLException | JMSException ex) {
            log.debug("[{}] Connection to TxEventQ could not be established",
                    Thread.currentThread().getId());
            throw handleException(ex);
        }
        log.trace("[{}]  Exit {}.connect", this.conn, this.getClass().getName());
    }

    /**
     * Queries the database for the required information to store in the Node object for all the
     * node instances. Creates a Node object with the required information and returns the list of
     * Node.
     * 
     * @return The list of Node in the cluster database.
     * 
     * @throws SQLException
     */
    private List<Node> getNodes() throws SQLException {
        log.trace("[{}]  Entry {}.getNodes", this.conn, this.getClass().getName());
        List<Node> nodes = new ArrayList<>();
        String getInstanceQry = "SELECT INST_ID, INSTANCE_NAME FROM GV$INSTANCE";

        try (Statement stmt = this.conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY); ResultSet rs = stmt.executeQuery(getInstanceQry);) {
            while (rs.next()) {
                instances.put(rs.getInt(1), rs.getString(2));
                nodes.add(new Node(rs.getInt(1), rs.getString(2)));
            }
        }

        log.trace("[{}]  Exit {}.getNodes", this.conn, this.getClass().getName());
        return nodes;

    }

    /**
     * Query the database to get the partition that is owned by all the available instances in the
     * cluster database and store that information into a map.
     * 
     * @param nodes The list of nodes in the cluster database
     * @param topic The name of the topic
     * @throws SQLException
     */
    private void getPartitionInstanceOwnershipInfo(List<Node> nodes, String topic)
            throws SQLException {
        log.trace("[{}]  Entry {}.getPartitionInstanceOwnershipInfo", this.conn,
                this.getClass().getName());

        if (nodes.isEmpty() || topic == null) {
            return;
        }

        this.partitionInstanceOwner.clear();
        log.debug("partitionInstanceOwner map cleared.");

        String queryQShard = "SELECT SHARD_ID, OWNER_INSTANCE FROM USER_QUEUE_SHARDS WHERE  NAME = ? and owner = ?";

        try (PreparedStatement stmt1 = this.conn.prepareStatement(queryQShard)) {
            stmt1.setString(1, topic);
            stmt1.setString(2, this.userName);
            try (ResultSet rslt = stmt1.executeQuery()) {
                // If any row exist
                if (rslt.isBeforeFirst()) {
                    while (rslt.next()) {
                        int partNum = rslt.getInt(1) / 2;
                        int nodeNum = rslt.getInt(2);

                        this.partitionInstanceOwner.put(partNum, nodeNum);
                        log.debug("Partition: [{}], Instance owned: [{}]", partNum, nodeNum);
                    }
                }
            }
        }

        log.trace("[{}]  Exit {}.getPartitionInstanceOwnershipInfo", this.conn,
                this.getClass().getName());
    }

    /**
     * Gets the key for the specified value from the map.
     * 
     * @param <K>   The type of the map keys
     * @param <V>   The type of the map values
     * @param map   The map to get the key for the specified value from.
     * @param value The value to get the key value for.
     * @return The key associated with the value.
     */
    private <K, V> K getKey(Map<K, V> map, V value) {
        for (Entry<K, V> entry : map.entrySet()) {
            if (entry.getValue().equals(value)) {
                return entry.getKey();
            }
        }
        return null;
    }

    /**
     * Internal method to connect to TxEventQ.
     */
    private void connectConnectionInternal() {
        log.trace("Entry {}.connectConnectionInternal", this.getClass().getName());

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
            this.conn.setAutoCommit(false);
            this.tconn.start();

            versionCheck();

            this.userName = this.conn.getUserName().toUpperCase();

            this.topic = ((AQjmsSession) (this.tSess)).getTopic(this.userName,
                    this.config.getString(TxEventQSinkConfig.TXEVENTQ_QUEUE_NAME).toUpperCase());
            this.tProducer = this.tSess.createProducer(this.topic);

            this.preparedMergeStatement = this.conn.prepareStatement(this.mergeSqlStatement);
            this.preparedSelectOffsetStatement = this.conn
                    .prepareStatement(this.selectOffsetSqlStatement);
            this.reconnectDelayMillis = RECONNECT_DELAY_MILLIS_MIN;
            this.connected = true;

            this.isClusterDatabase = isClusterDatabase();

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

            log.trace("Exit {}.connectConnectionInternal", this.getClass().getName());
            throw handleException(ex);
        }

        log.trace("Exit {}.connectConnectionInternal", this.getClass().getName());
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
        log.info("ErrorCode returned for handle exception: {}", errorCode);

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
        case Constants.ORA_62187:
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
            try {
                close();
            } catch (IOException e) {
                log.error("Unable to close connections.");
            }
        }

        if (isRetriable) {
            log.trace("[{}]  Exit {}.handleException", this.conn, this.getClass().getName());
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
            log.debug("JMSE exception cause: {}", e);
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
                    // Get the error number from the error message if the getErrorCode is null
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
        log.trace("[{}]: Entry {}.getDatabaseConnection", this.conn, this.getClass().getName());

        return this.conn;
    }

    /**
     * Gets the partition size for the specified Kafka topic.
     * 
     * @param topic The Kafka topic to get the partition size for.
     * @return The size of the partition for the specified topic.
     */
    public int getKafkaTopicPartitionSize(String topic) {
        log.trace("[{}] Entry {}.getKafkaTopicPartitionSize,", this.conn,
                this.getClass().getName());

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

        log.trace("[{}] Exit {}.getKafkaTopicPartitionSize,", this.conn, this.getClass().getName());
        return partitionSize;
    }

    /**
     * Validates if the Kafka topic exist.
     * 
     * @param topic The Kafka topic to check existence for.
     * @return True if the Kafka topic exists false otherwise.
     */
    public boolean kafkaTopicExists(String topic) {
        log.trace("[{}] Entry {}.kafkaTopicExists,", this.conn, this.getClass().getName());

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

        log.trace("[{}] Exit {}.kafkaTopicExists,", this.conn, this.getClass().getName());
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
        log.trace("[{}] Entry {}.txEventQueueExists,", this.conn, this.getClass().getName());

        DatabaseMetaData meta = this.conn.getMetaData();
        try (ResultSet resultSet = meta.getTables(null, null, queueName,
                new String[] { "TABLE" })) {
            log.trace("[{}] Exit {}.txEventQueueExists,", this.conn, this.getClass().getName());
            return resultSet.next();
        }
    }

    /**
     * Query the database to check if it is a cluster database or not.
     * 
     * @return True if cluster database, false otherwise.
     */
    private boolean isClusterDatabase() {
        log.trace("[{}] Entry {}.isClusterDatabase,", this.conn, this.getClass().getName());

        String getIsClusterDatabaseVal = "SELECT VALUE FROM V$PARAMETER WHERE UPPER(NAME) = 'CLUSTER_DATABASE'";
        boolean isRac;

        try (Statement stmt1 = this.conn.createStatement();
                ResultSet rs1 = stmt1.executeQuery(getIsClusterDatabaseVal);) {
            rs1.next();
            isRac = rs1.getBoolean("VALUE");
        } catch (SQLException e) {
            throw handleException(e);
        }

        log.trace("[{}] Exit {}.isClusterDatabase, isRac=[{}]", this.conn,
                this.getClass().getName(), isRac);

        return isRac;
    }

    /**
     * Creates the TXEVENTQ$_TRACK_OFFSETS table if it does not already exist.
     * 
     * @return True if the table exist or false if it does not exist.
     */
    public boolean createOffsetInfoTable() {
        log.trace("[{}] Entry {}.createOffsetInfoTable,", this.conn, this.getClass().getName());
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

        log.trace("[{}] Exit {}.createOffsetInfoTable,", this.conn, this.getClass().getName());
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
        log.trace("[{}] Entry {}.getNumOfShardsForQueue,", this.conn, this.getClass().getName());

        int numShard;
        try (CallableStatement getnumshrdStmt = this.conn
                .prepareCall("{call dbms_aqadm.get_queue_parameter(?,?,?)}")) {
            getnumshrdStmt.setString(1, queue);
            getnumshrdStmt.setString(2, "SHARD_NUM");
            getnumshrdStmt.registerOutParameter(3, Types.INTEGER);
            getnumshrdStmt.execute();
            numShard = getnumshrdStmt.getInt(3);
        } catch (SQLException e) {
            throw handleException(e);
        }

        log.trace("[{}] Exit {}.getNumOfShardsForQueue,", this.conn, this.getClass().getName());
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

        log.trace("[{}] Entry {}.enqueueMessage,", this.conn, this.getClass().getName());

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

        // We want to retrieve the message id after enqueue.
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

        // execute the actual enqueue operation
        ((oracle.jdbc.internal.OracleConnection) this.conn).jmsEnqueue(queueName, opt, mesg,
                aqProp);

        log.trace("[{}] Exit {}.enqueueMessage,", this.conn, this.getClass().getName());
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
        log.trace("[{}] Entry {}.enqueueBulkMessage,", this.conn, this.getClass().getName());

        final List<AQjmsBytesMessage> messages = new ArrayList<>();
        AQjmsBytesMessage[] msgs = null;
        int[] deliveryMode = new int[records.size()];
        int[] priorities = new int[records.size()];

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

        msgs = messages.toArray(new AQjmsBytesMessage[0]);
        ((AQjmsProducer) msgProducer).bulkSend(msgs, deliveryMode, priorities, null);

        log.trace("[{}] Exit {}.enqueueBulkMessage, numMsgEnqueued = {}", this.conn,
                this.getClass().getName(), msgs.length);
    }

    /**
     * Creates AQjmsBytesMessage from ByteBuffer's key, value and headers.
     * 
     * @param session The topic session.
     * @param sinkRec The message.
     */
    private AQjmsBytesMessage createBytesMessage(TopicSession session, SinkRecord sinkRec)
            throws JMSException {
        log.trace("[{}] Entry {}.createBytesMessage,", this.conn, this.getClass().getName());

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
        msg.setIntProperty("AQINTERNAL_PARTITION", sinkRec.kafkaPartition() * 2);

        log.trace("[{}] Exit {}.createBytesMessage,", this.conn, this.getClass().getName());
        return msg;
    }

    /**
     * Enqueues the Kafka records into the specified TxEventQ. Also keeps track of the offset for a
     * particular topic and partition in database table TXEVENTQ$_TRACK_OFFSETS.
     * 
     * @param records The records to enqueue into the TxEventQ.
     */
    public void put(Collection<SinkRecord> records) {
        log.trace("[{}] Entry {}.put,", this.conn, this.getClass().getName());

        connectConnectionInternal();
        try {
            if (this.isClusterDatabase) {

                if (this.messageProducerForInstanceMaps.isEmpty()) {
                    log.debug("Getting instance information: mpMap[{}]",
                            this.messageProducerForInstanceMaps.isEmpty());

                    genMessageProducerAndPartitionInfoForNodes();
                } else {
                    log.debug("Don't need to get instance information.");
                }

                Map<Integer, Collection<SinkRecord>> recordsBelongingToInstance = sortSinkRecordsByInstances(
                        records);

                enqueueOnClusterDatabase(recordsBelongingToInstance);

            } else {
                enqueueOnNonClusterDatabase(records);
            }
        } catch (SQLException | JMSException e) {
            throw handleException(e);
        }
        log.trace("[{}] Exit {}.put,", this.conn, this.getClass().getName());
    }

    /**
     * When dealing with a cluster database (RAC) the sink records will need to be sorted according
     * to the instances that the partitions belong to. In the event that a retriable error should
     * occur during the enqueue process this method will check the errorInSessCommitProcess flag. If
     * the errorInSessCommitProcess is true we will get the current offset stored in the database
     * and make sure that the retried messages have not already been committed.
     * 
     * @param records A collection records to that can be sent
     * @return A collection of records that will be sent minus any records that have already been
     *         committed.
     */
    private Map<Integer, Collection<SinkRecord>> sortSinkRecordsByInstances(
            Collection<SinkRecord> records) {
        log.trace("[{}] Entry {}.sortSinkRecordsByInstances,", this.conn,
                this.getClass().getName());

        Map<Integer, Collection<SinkRecord>> recordsBelongingToInstance = new HashMap<>();
        HashMap<Integer, Long> trackOffset = new HashMap<>();
        for (Iterator<SinkRecord> sinkRecord = records.iterator(); sinkRecord.hasNext();) {
            SinkRecord sr = sinkRecord.next();
            Integer instanceOwnedByPart = partitionInstanceOwner.get(sr.kafkaPartition());

            if (this.errorInSessCommitProcess && !trackOffset.containsKey(sr.kafkaPartition())) {
                Long offset = getOffsetInDatabase(sr.topic(),
                        this.config.getString(TxEventQSinkConfig.TXEVENTQ_QUEUE_NAME),
                        this.config.getString(TxEventQSinkConfig.TXEVENTQ_QUEUE_SCHEMA),
                        sr.kafkaPartition());
                trackOffset.put(sr.kafkaPartition(), offset);
                log.debug(
                        "A retriable error occurred, query database to get offset to check if prior commit occurred: partition={}, offset={}",
                        sr.kafkaPartition(), offset);
            }

            try {
                String instanceVal = this.conn.getServerSessionInfo()
                        .getProperty("AUTH_SC_INSTANCE_NAME");
                Integer instanceKey = getKey(instances, instanceVal);

                if ((!trackOffset.isEmpty()
                        && sr.kafkaOffset() >= trackOffset.get(sr.kafkaPartition()))
                        || !this.errorInSessCommitProcess) {
                    recordsBelongingToInstance.computeIfAbsent(
                            instanceOwnedByPart == null ? instanceKey : instanceOwnedByPart,
                            k -> (new ArrayList<SinkRecord>())).add(sr);
                    log.debug("recordsBelongingToInstance: instance = [{}], partition = [{}]",
                            instanceOwnedByPart == null ? instanceKey : instanceOwnedByPart,
                            sr.kafkaPartition());
                }

            } catch (SQLException e) {
                throw handleException(e);
            }
        }
        log.trace("[{}] Exit {}.sortSinkRecordsByInstances,", this.conn, this.getClass().getName());
        return recordsBelongingToInstance;
    }

    /**
     * Generates a map of MessageProducerForInstance for a cluster database and creates a map that
     * shows the partition to instance ownership.
     * 
     * @throws SQLException
     * @throws JMSException
     */
    private void genMessageProducerAndPartitionInfoForNodes() throws SQLException, JMSException {
        log.trace("[{}] Entry {}.genMessageProducerAndPartitionInfoForNodes,", this.conn,
                this.getClass().getName());

        List<Node> nodes = getNodes();

        log.debug("Retrieved list of nodes with a size of [{}].", nodes.size());

        for (Node n : nodes) {
            if (!this.messageProducerForInstanceMaps.containsKey(n.getId())) {
                this.messageProducerForInstanceMaps.put(n.getId(), new MessageProducerForInstance(n,
                        this.conn.getUserName().toUpperCase(), this.config));
            }
        }

        log.debug("The messageProducerForInstanceMaps size: {}",
                this.messageProducerForInstanceMaps.size());
        getPartitionInstanceOwnershipInfo(nodes,
                this.config.getString(TxEventQSinkConfig.TXEVENTQ_QUEUE_NAME).toUpperCase());

        log.trace("[{}] Exit {}.genMessageProducerAndPartitionInfoForNodes,", this.conn,
                this.getClass().getName());
    }

    /**
     * Performs a bulk enqueue on a non cluster database.
     * 
     * @param records A collection records to send
     * @throws JMSException
     * @throws SQLException
     */
    private void enqueueOnNonClusterDatabase(Collection<SinkRecord> records)
            throws JMSException, SQLException {
        log.trace("[{}] Entry {}.enqueueOnNonClusterDatabase,", this.conn,
                this.getClass().getName());

        enqueueBulkMessage(this.config.getString(TxEventQSinkConfig.TXEVENTQ_QUEUE_NAME), records,
                tProducer);

        Map<String, Map<Integer, Long>> topicInfoMap = getTopicPartitionOffsetMapInfo(records);

        processTopicPartitionOffsetMapInfoInDatabase(topicInfoMap, this.preparedMergeStatement);

        this.conn.commit();

        log.trace("[{}] Exit {}.enqueueOnNonClusterDatabase,", this.conn,
                this.getClass().getName());
    }

    /**
     * Goes through the SinkRecords and creates a map for the topic containing another map for the
     * value containing the topic partition and offset.
     * 
     * @param records A collection of records to process for the topic partition and offset to be
     *                stored in a map.
     * @return A map for the topic containing the partition number and offset.
     */
    private Map<String, Map<Integer, Long>> getTopicPartitionOffsetMapInfo(
            Collection<SinkRecord> records) {
        log.trace("[{}] Entry {}.getTopicPartitionOffsetMapInfo,", this.conn,
                this.getClass().getName());
        Map<String, Map<Integer, Long>> topicInfoMap = new HashMap<>();
        for (SinkRecord sinkRecord : records) {

            topicInfoMap.computeIfPresent(sinkRecord.topic(), (k, v) -> {
                v.computeIfPresent(sinkRecord.kafkaPartition(), (x, y) -> sinkRecord.kafkaOffset());
                return v;
            });

            topicInfoMap.computeIfAbsent(sinkRecord.topic(), k -> (new HashMap<>()))
                    .put(sinkRecord.kafkaPartition(), sinkRecord.kafkaOffset());
        }
        log.trace("[{}] Exit {}.getTopicPartitionOffsetMapInfo,", this.conn,
                this.getClass().getName());
        return topicInfoMap;
    }

    /**
     * Go through the map containing the topic's partition and offset information and store the
     * information into the database.
     * 
     * @param topicInfoMap          The map containing the topic's partition and offset information.
     * @param mergePrepareStatement The prepared statement containing the sql merge query for the
     *                              TXEVENTQ$_TRACK_OFFSETS
     * @throws SQLException
     */
    private void processTopicPartitionOffsetMapInfoInDatabase(
            Map<String, Map<Integer, Long>> topicInfoMap, PreparedStatement mergePrepareStatement)
            throws SQLException {
        log.trace("[{}] Entry {}.processTopicPartitionOffsetMapInfoInDatabase,", this.conn,
                this.getClass().getName());

        for (Map.Entry<String, Map<Integer, Long>> topicEntry : topicInfoMap.entrySet()) {
            String topicKey = topicEntry.getKey();
            Map<Integer, Long> offsetInfoValue = topicEntry.getValue();
            for (Map.Entry<Integer, Long> offsetInfoEntry : offsetInfoValue.entrySet()) {
                setOffsetInfoInDatabase(mergePrepareStatement, topicKey,
                        this.config.getString(TxEventQSinkConfig.TXEVENTQ_QUEUE_NAME),
                        this.config.getString(TxEventQSinkConfig.TXEVENTQ_QUEUE_SCHEMA),
                        offsetInfoEntry.getKey(), offsetInfoEntry.getValue());
            }
        }

        log.trace("[{}] Exit {}.processTopicPartitionOffsetMapInfoInDatabase,", this.conn,
                this.getClass().getName());
    }

    /**
     * Performs a bulk enqueue on a cluster database. In order to perform a bulk enqueue on a
     * cluster database the method will determine which instance owns the partition and gather all
     * records that belong to the instance. Using the appropriate MessageProducer for the instance
     * node all records owned by the instance will be enqueued.
     * 
     * @param recordsBelongingToInstance A map containing all the records that are owned by an
     *                                   instance.
     * @throws SQLException, JMSException
     */
    private void enqueueOnClusterDatabase(
            Map<Integer, Collection<SinkRecord>> recordsBelongingToInstance)
            throws SQLException, JMSException {
        log.trace("[{}] Entry {}.enqueueOnClusterDatabase,", this.conn, this.getClass().getName());

        boolean useDiffInstanceConn = useDifferentInstanceConnection(recordsBelongingToInstance);

        if (this.errorInSessCommitProcess) {
            this.errorInSessCommitProcess = false;
        }

        try {
            // Getting an iterator
            Iterator<Entry<java.lang.Integer, java.util.Collection<SinkRecord>>> recsBelongToInstanceIterator = recordsBelongingToInstance
                    .entrySet().iterator();

            log.debug("recordsBelongingToInstance size: [{}]", recordsBelongingToInstance.size());

            while (recsBelongToInstanceIterator.hasNext()) {
                Map.Entry<java.lang.Integer, java.util.Collection<SinkRecord>> mapElement = recsBelongToInstanceIterator
                        .next();
                Collection<SinkRecord> filteredRecords = mapElement.getValue();

                MessageProducerForInstance msgProducer = getMessageProducerForInstance(
                        useDiffInstanceConn, mapElement);

                log.debug("msgProducer details: {}, mapElement key: [{}]", msgProducer,
                        mapElement.getKey());

                enqueueBulkMessage(this.config.getString(TxEventQSinkConfig.TXEVENTQ_QUEUE_NAME),
                        filteredRecords,
                        msgProducer != null ? msgProducer.getMsgProducer() : tProducer);

                Map<String, Map<Integer, Long>> topicInfoMap = getTopicPartitionOffsetMapInfo(
                        filteredRecords);

                processTopicPartitionOffsetMapInfoInDatabase(topicInfoMap,
                        msgProducer != null ? msgProducer.getPreparedMergeStatement()
                                : this.preparedMergeStatement);

                if (msgProducer != null) {
                    log.debug("Committing for msgProducerInstanceMap.");
                    msgProducer.getSession().commit();
                } else {
                    log.debug("Committing for tSess.");
                    this.tSess.commit();
                }
            }

        } catch (SQLException | JMSException e) {
            this.errorInSessCommitProcess = true;
            throw e;
        }

        log.trace("[{}] Exit {}.enqueueOnClusterDatabase,", this.conn, this.getClass().getName());
    }

    /**
     * Gets the MessageProducerForInstance associated with the node instance.
     * 
     * @param useDiffInstanceConn   Boolean value indicating whether different instance connection
     *                              should be used.
     * @param instanceSinkRecordMap A map containing all the SinkRecords associated with the node
     *                              instance.
     * @return The MessageProducerForInstance associated with the node instance, null if no
     *         MessageProducerForInstance applicable.
     */
    private MessageProducerForInstance getMessageProducerForInstance(boolean useDiffInstanceConn,
            Map.Entry<java.lang.Integer, java.util.Collection<SinkRecord>> instanceSinkRecordMap) {
        log.trace("[{}] Entry {}.getMessageProducerForInstance,", this.conn,
                this.getClass().getName());
        MessageProducerForInstance msgProducerForInstance = null;

        if (!this.messageProducerForInstanceMaps.isEmpty() && useDiffInstanceConn) {
            log.debug("The MessageProducerForInstance for the instance [{}]: [{}]",
                    instanceSinkRecordMap.getKey(),
                    this.messageProducerForInstanceMaps.get(instanceSinkRecordMap.getKey()));
            msgProducerForInstance = this.messageProducerForInstanceMaps
                    .get(instanceSinkRecordMap.getKey());
        }

        log.trace("[{}] Exit {}.getMessageProducerForInstance,", this.conn,
                this.getClass().getName());
        return msgProducerForInstance;
    }

    /**
     * Checks if a different connection to an instance node is required.
     * 
     * @param recordsBelongingToInstance A map containing all the records that are owned by an
     *                                   instance.
     * @return True if a different instance connection is required, false otherwise.
     */
    private boolean useDifferentInstanceConnection(
            Map<Integer, Collection<SinkRecord>> recordsBelongingToInstance) {
        log.trace("[{}] Entry {}.useDifferentInstanceConnection,", this.conn,
                this.getClass().getName());
        boolean useDiffInstanceConn = true;

        /*
         * If the recordsBelongingToInstance size is equal to 1 check whether the current connection
         * belongs to that instance. If it does not then a different connection will need to be used
         * in order to enqueue the messages.
         */
        if (recordsBelongingToInstance.size() == 1) {
            try {
                String instanceVal = this.conn.getServerSessionInfo()
                        .getProperty("AUTH_SC_INSTANCE_NAME");

                log.debug("Connection is connected to instance: [{}]", instanceVal);

                Integer instanceKey = getKey(instances, instanceVal);

                if (recordsBelongingToInstance.containsKey(instanceKey)) {
                    log.debug(
                            "Current collection of records belongs to the current connection for instance [{}], don't need to change connection.",
                            instanceKey);
                    useDiffInstanceConn = false;
                }
            } catch (SQLException e) {
                throw handleException(e);
            }
        }

        log.trace("[{}] Exit {}.useDifferentInstanceConnection, retval={},", this.conn,
                this.getClass().getName(), useDiffInstanceConn);
        return useDiffInstanceConn;
    }

    /**
     * Populates the TXEVENTQ$_TRACK_OFFSETS table with the kafka topic, TxEventQ queue name, schema
     * for the queue, partition, and offset information of the messages that have been enqueued.
     * 
     * @param mergePrepareStatement The prepared statement containing the sql merge query for the
     *                              TXEVENTQ$_TRACK_OFFSETS
     * @param topic                 The Kafka topic name.
     * @param queueName             The TxEventQ queue name.
     * @param queueSchema           The schema for the queue.
     * @param partition             The partition number.
     * @param offset                The offset value.
     * @throws SQLException
     */
    private void setOffsetInfoInDatabase(PreparedStatement mergePrepareStatement, String topic,
            String queueName, String queueSchema, int partition, long offset) throws SQLException {
        log.trace("[{}] Entry {}.setOffsetInfoInDatabase,", mergePrepareStatement.getConnection(),
                this.getClass().getName());

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
        mergePrepareStatement.executeUpdate();

        log.trace("[{}] Exit {}.setOffsetInfoInDatabase,", mergePrepareStatement.getConnection(),
                this.getClass().getName());

    }

    /**
     * Gets the offset for the specified Kafka topic, TxEventQ queue name, schema, and partition.
     * The offset will be used to determine which message to start consuming.
     * 
     * @param topic       The Kafka topic name.
     * @param queueName   The TxEventQ queue name.
     * @param queueSchema The schema for the queue.
     * @param partition   The partition number.
     * @return The offset value.
     */
    public long getOffsetInDatabase(String topic, String queueName, String queueSchema,
            int partition) {
        log.trace("[{}] Entry {}.getOffsetInDatabase,", this.conn, this.getClass().getName());
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
            throw handleException(e);
        }

        log.trace("[{}] Exit {}.getOffsetInDatabase,", this.conn, this.getClass().getName());
        return offsetVal;

    }

    @Override
    public void close() throws IOException {
        log.trace("Entry {}.close,", this.getClass().getName());

        if (tSess != null) {
            log.debug("Session rollback attempted.");
            try {
                this.tSess.rollback();
            } catch (JMSException e) {
                log.error("[{}]: {}", e.getClass().getName(), e.getMessage());
            }
        }

        try {
            if (this.preparedMergeStatement != null) {
                log.debug("preparedMergeStatement will be closed.");
                this.preparedMergeStatement.close();
            }
        } catch (SQLException e) {
            log.error("[{}]: {}", e.getClass().getName(), e.getMessage());
        }

        try {
            if (this.preparedSelectOffsetStatement != null) {
                log.debug("preparedSelectOffsetStatement will be closed.");
                this.preparedSelectOffsetStatement.close();
            }
        } catch (SQLException e) {
            log.error("[{}]: {}", e.getClass().getName(), e.getMessage());
        }

        try {
            this.connected = false;

            this.partitionInstanceOwner.clear();
            this.instances.clear();

            if (!this.messageProducerForInstanceMaps.isEmpty()) {
                Iterator<Entry<java.lang.Integer, MessageProducerForInstance>> msgProducersIterator = this.messageProducerForInstanceMaps
                        .entrySet().iterator();
                while (msgProducersIterator.hasNext()) {
                    Map.Entry<Integer, MessageProducerForInstance> entry = msgProducersIterator
                            .next();
                    entry.getValue().close();
                    log.debug("Connection for instance [{}] is being closed.", entry.getKey());
                }
                this.messageProducerForInstanceMaps.clear();
            }

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
            log.error("[{}]: {}", ex.getClass().getName(), ex.getMessage());

        } finally {
            this.conn = null;
            this.tSess = null;
            this.tconn = null;
            log.debug("Connection to TxEventQ closed.");
        }

        log.trace("Exit {}.close,", this.getClass().getName());
    }

    private final class MessageProducerForInstance {

        private String mergeSqlStatement = "MERGE INTO " + TXEVENTQ$_TRACK_OFFSETS
                + " tab1 USING (SELECT ? kafka_topic_name, ? queue_name, ? queue_schema, ? partition)"
                + " tab2 ON (tab1.kafka_topic_name = tab2.kafka_topic_name AND tab1.queue_name = tab2.queue_name AND tab1.queue_schema = tab2.queue_schema AND tab1.partition = tab2.partition)"
                + " WHEN MATCHED THEN UPDATE SET offset=? WHEN NOT MATCHED THEN"
                + " INSERT (kafka_topic_name, queue_name, queue_schema, partition, offset) VALUES (?,?,?,?,?)";
        private TopicConnection topicConn;
        private TopicSession sess;
        private Topic topic;
        private MessageProducer msgProducer;
        private String userName;
        private int instanceId;
        private TxEventQSinkConfig config;
        private PreparedStatement preparedMergeStatement;
        private OracleConnection oracleConn;

        public MessageProducerForInstance(Node node, String userName, TxEventQSinkConfig config)
                throws JMSException, SQLException {
            this(node, Session.CLIENT_ACKNOWLEDGE, userName, config);
        }

        public MessageProducerForInstance(Node node, int mode, String userName,
                TxEventQSinkConfig config) throws JMSException, SQLException {
            this.config = config;
            this.userName = userName;
            this.instanceId = node.getId();
            this.topicConn = this.createTopicConnection(node);
            this.sess = this.createTopicSession(mode);
            this.msgProducer = this.createMessageProducer();
        }

        /**
         * Creates topic connection to node
         * 
         * @param node Destination to which connection is needed
         * @return Established topic connection for the node
         * @throws SQLException
         * @throws JMSException
         */
        public TopicConnection createTopicConnection(Node node) throws SQLException, JMSException {
            log.trace("Entry {}.createTopicConnection,", this.getClass().getName());
            Properties props = new Properties();
            String urlBuilder = "jdbc:oracle:thin:@"
                    + this.config.getString(TxEventQSinkConfig.DATABASE_TNS_ALIAS_CONFIG);

            OracleDataSource dataSource;
            props.put("oracle.jdbc.targetInstanceName", node.getInstanceName());
            dataSource = new OracleDataSource();
            dataSource.setConnectionProperties(props);
            dataSource.setURL(urlBuilder);
            log.debug("Create topic connection for node instance [{}] with url [{}]",
                    node.getInstanceName(), dataSource.getURL());
            log.debug("Get datasource connection: {}", dataSource.getConnection());

            TopicConnectionFactory connFactory = AQjmsFactory.getTopicConnectionFactory(dataSource);
            log.trace("Exit {}.createTopicConnection,", this.getClass().getName());
            return connFactory.createTopicConnection();
        }

        /**
         * Creates topic session from established connection
         * 
         * @param mode Mode of acknowledgement with which session has to be created
         * @return Created topic session
         * @throws JMSException
         * @throws SQLException
         */
        public TopicSession createTopicSession(int mode) throws JMSException, SQLException {
            log.trace("Entry {}.createTopicSession,", this.getClass().getName());
            if (this.sess != null)
                return this.sess;

            this.sess = this.topicConn.createTopicSession(true, mode);
            this.oracleConn = (OracleConnection) ((AQjmsSession) (this.sess)).getDBConnection();
            this.preparedMergeStatement = this.oracleConn.prepareStatement(this.mergeSqlStatement);
            this.topicConn.start();
            log.trace("Exit {}.createTopicSession,", this.getClass().getName());
            return this.sess;
        }

        /**
         * Gets the PreparedStatement for merging the offset information into the
         * TXEVENTQ$_TRACK_OFFSETS.
         * 
         * @return The PreparedStatement
         */
        public PreparedStatement getPreparedMergeStatement() {
            log.trace("Entry {}.getPreparedMergeStatement,", this.getClass().getName());

            log.trace("Exit {}.getPreparedMergeStatement,", this.getClass().getName());
            return this.preparedMergeStatement;
        }

        /**
         * Creates message producer for topic.
         * 
         * @return The MessageProducer created.
         * @throws JMSException
         */
        private MessageProducer createMessageProducer() throws JMSException {
            log.trace("Entry {}.createMessageProducer,", this.getClass().getName());
            Topic dest = ((AQjmsSession) (this.sess)).getTopic(this.userName,
                    this.config.getString(TxEventQSinkConfig.TXEVENTQ_QUEUE_NAME).toUpperCase());
            log.trace("Exit {}.createMessageProducer,", this.getClass().getName());
            return this.sess.createProducer(dest);
        }

        public TopicConnection getTopicConnection() {
            return this.topicConn;
        }

        public TopicSession getSession() {
            return this.sess;
        }

        public OracleConnection getOracleConnection() {
            return this.oracleConn;
        }

        public MessageProducer getMsgProducer() {
            return this.msgProducer;
        }

        public void close() {
            log.trace("Entry {}.close,", this.getClass().getName());

            try {
                if (this.preparedMergeStatement != null) {
                    log.debug("preparedMergeStatement will be closed.");
                    this.preparedMergeStatement.close();
                }
            } catch (SQLException e) {
                log.error("{}: {}", e.getClass().getName(), e);
            }

            try {

                if (this.sess != null) {
                    log.debug("Session rollback attempted.");
                    this.sess.rollback();
                    log.debug("Session will be closed.");
                    this.sess.close();
                }

                if (this.oracleConn != null) {
                    log.debug("Connection will be closed.");
                    this.oracleConn.close();
                }

                if (this.topicConn != null) {
                    log.debug("Topic Connection will be closed.");
                    this.topicConn.close();
                }

                if (this.msgProducer != null) {
                    log.debug("MessageProducer will be closed.");
                    this.msgProducer.close();
                }

            } catch (SQLException | JMSException ex) {
                log.error("{}: {}", ex.getClass().getName(), ex);

            } finally {
                this.topicConn = null;
                this.sess = null;
                this.oracleConn = null;
                this.msgProducer = null;
                log.debug("Connection to TxEventQ closed.");
            }

            log.trace("Exit {}.close,", this.getClass().getName());
        }

        @Override
        public String toString() {
            return "MessageProducerForInstance [mergeSqlStatement=" + mergeSqlStatement
                    + ", topicConn=" + topicConn + ", sess=" + sess + ", topic=" + topic
                    + ", msgProducer=" + msgProducer + ", userName=" + userName + ", instanceId="
                    + instanceId + ", config=" + config + ", preparedMergeStatement="
                    + preparedMergeStatement + ", oracleConn=" + oracleConn + "]";
        }
    }
}
