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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collection;
import java.util.Collections;
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


public class TxEventQProducer implements Closeable  {
	protected static final Logger log = LoggerFactory.getLogger(TxEventQProducer.class);

	private OracleConnection conn;
	private TxEventQSinkConfig config = null;
	
	public TxEventQProducer(TxEventQSinkConfig config) {
        this.config = config;
    }

	/**
	 * Uses the Oracle wallet to connect to the database.
	 */
    public void connect()  {
        if (this.isConnOpen()) {
            log.info("[{}] Connection is already open.", Thread.currentThread().getId());
            return;
        }
            
        try {
            System.setProperty("oracle.net.wallet_location",this.config.getString(TxEventQSinkConfig.DATABASE_WALLET_CONFIG));
            System.setProperty("oracle.net.tns_admin", this.config.getString(TxEventQSinkConfig.DATABASE_TNSNAMES_CONFIG));
            DriverManager.registerDriver(new oracle.jdbc.OracleDriver());
            String url = "jdbc:oracle:thin:@" + this.config.getString(TxEventQSinkConfig.DATABASE_TNS_ALIAS_CONFIG);
            this.conn = (OracleConnection) DriverManager.getConnection(url);
            this.conn.setAutoCommit(false);
            log.info("[{}:{}] Oracle TxEventQ connection opened!", Thread.currentThread().getId(), this.conn);
        }
        catch (SQLException sqlex) {
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
     * @param queueName The name of the queue to check existence for.
     * @return True if the queue exists otherwise false.
     * @throws SQLException
     */
    public boolean txEventQueueExists(String queueName) throws SQLException {
        if (!isConnOpen()) {
            connect();
        }

        DatabaseMetaData meta = this.conn.getMetaData();
        ResultSet resultSet = meta.getTables(null, null, queueName, new String[] { "TABLE" });

        return resultSet.next();
    }
    
    /**
     * Gets the number of shards for the specified queue.
     * 
     * @param queue The queue to get the number of shards for.
     * @return The number of shards.
     * @throws java.sql.SQLException
     */
    public int getNumOfShardsForQueue(String queue) throws java.sql.SQLException {
        if (!isConnOpen()) {
            connect();
        }
        
        CallableStatement getnumshrdStmt = null;
        int numshard;
        getnumshrdStmt = this.conn.prepareCall("{call dbms_aqadm.get_queue_parameter(?,?,?)}");
        getnumshrdStmt.setString(1, queue.toUpperCase());
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
     * @param conn The Oracle database connection.
     * @param queueName The name of the TxEventQ to enqueue message to.
     * @param sinkRecord The message to be enqueued.
     * @throws SQLException
     */
    public void enqueueMessage(OracleConnection conn, String queueName, SinkRecord sinkRecord) throws SQLException {
        JMSMessageProperties jmsMesgProp = JMSFactory.createJMSMessageProperties();
        jmsMesgProp.setHeaderProperties("1,15,JMSDeliveryMode,27,10,PERSISTENT");
        if (sinkRecord.kafkaPartition() != null) {
            String id = "" + 2 * sinkRecord.kafkaPartition();
            jmsMesgProp.setUserProperties("1,20,AQINTERNAL_PARTITION,24," + id.length() + "," + 2 * sinkRecord.kafkaPartition());
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
     * Enqueues the Kafka records into TEQ.
     * 
     * @param records The records to enqueue into TEQ.
     */
    public void put(Collection<SinkRecord> records) {
        if (!isConnOpen()) {
            connect();
        }

        try {

            for (SinkRecord sinkRecord : records) {
                log.info("[{}:{}] Enqueuing record: {}", Thread.currentThread().getId(), this.conn, sinkRecord.value());
                enqueueMessage(this.conn, this.config.getString(TxEventQSinkConfig.TXEVENTQ_QUEUE_NAME), sinkRecord);
            }
            this.conn.commit();

        } catch (SQLException e) {
            throw new ConnectException("Error putting records into TxEventQ: " + e.toString());

        }
    }
    
    /**
     * Checks if the database connection is open and valid.
     * 
     * @return True if the database is open and valid, otherwise false.
     */
    public boolean isConnOpen() {
        boolean isConnValid = false;
        try {
            if (this.conn != null) {
                isConnValid = this.conn.isValid(5);
            }
        } catch (SQLException e) {
            throw new ConnectException("Error checking if the database connection is valid: " + e.toString());
        }
        return isConnValid;
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
