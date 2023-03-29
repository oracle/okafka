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

package oracle.jdbc.txeventq.kafka.connect.source.utils;

import oracle.jdbc.OracleConnection;
import oracle.jdbc.aq.*;
import oracle.jdbc.aq.AQMessage;
import oracle.jdbc.internal.JMSDequeueOptions;
import oracle.jdbc.internal.JMSMessage;
import oracle.jdbc.internal.JMSMessageProperties;
import oracle.sql.RAW;
import oracle.sql.json.OracleJsonDatum;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class TxEventQConsumer implements Closeable {
	protected static final Logger log = LoggerFactory.getLogger(TxEventQConsumer.class);

	private TxEventQConnectorConfig config = null;
	private OracleConnection conn;

	public TxEventQConsumer(TxEventQConnectorConfig config) {
		this.config = config;
	}

	/**
	 * Uses the Oracle wallet to connect to the database.
	 */
	public OracleConnection connect() {
		try {
			log.info("[{}] Attempting to open database connection.", Thread.currentThread().getId());
			System.setProperty("oracle.net.wallet_location",
					this.config.getString(TxEventQConnectorConfig.DATABASE_WALLET_CONFIG));
			System.setProperty("oracle.net.tns_admin",
					this.config.getString(TxEventQConnectorConfig.DATABASE_TNSNAMES_CONFIG));
			DriverManager.registerDriver(new oracle.jdbc.OracleDriver());
			String url = "jdbc:oracle:thin:@"
					+ this.config.getString(TxEventQConnectorConfig.DATABASE_TNS_ALIAS_CONFIG);
			this.conn = (OracleConnection) DriverManager.getConnection(url);
			this.conn.setAutoCommit(false);
			log.info("[{}] Oracle TxEventQ connection [{}] opened.", Thread.currentThread().getId(), this.conn);
			return this.conn;
		} catch (SQLException sqlex) {
			throw new ConnectException("Couldn't establish a connection to the database: " + sqlex.toString());
		}
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

	/**
	 * Gets the RAW payload from the message and creates a new TxEventQSourceRecord
	 * object.
	 * 
	 * @param msgId The byte ID value of the the message.
	 * @param msg   The message that has been dequeued.
	 * @return A TxEventQSourceRecord object with the RAW payload that has been
	 *         dequeued.
	 * @throws SQLException
	 */
	private TxEventQSourceRecord processRawPayload(byte[] msgId, AQMessage msg) throws SQLException {
		RAW rawPayload = msg.getRAWPayload();
		String topic = this.config.getString(TxEventQConnectorConfig.KAFKA_TOPIC);
		String msgIdStr = byteArrayToHex(msgId);
		int shardNum = getShardId(msgIdStr);
		log.debug("[{}] Processing RAW Type message:[msgId: {}, shardNum: {}, bytes: {}]",
				Thread.currentThread().getId(), msgIdStr, shardNum, rawPayload.getBytes());
		return new TxEventQSourceRecord(null, null, topic, shardNum / 2, null, rawPayload.getBytes(),
				TxEventQSourceRecord.PayloadType.RAW, msgId);
	}

	/**
	 * Gets the JSON payload from the message and creates a new TxEventQSourceRecord
	 * object.
	 * 
	 * @param msgId The byte ID value of the the message.
	 * @param msg   The message that has been dequeued.
	 * @return A TxEventQSourceRecord object with the JSON payload that has been
	 *         dequeued.
	 * @throws SQLException
	 */
	private TxEventQSourceRecord processJsonPayload(byte[] msgId, AQMessage msg) throws SQLException {

		OracleJsonDatum jsonPayload = msg.getJSONPayload();
		String msgIdStr = byteArrayToHex(msgId);
		int shardNum = getShardId(msgIdStr);
		String topic = this.config.getString(TxEventQConnectorConfig.KAFKA_TOPIC);
		log.debug("[{}] Processing JSON Type message:[msgId: {}, shardNum: {}, bytes: {}]",
				Thread.currentThread().getId(), msgIdStr, shardNum, jsonPayload.getBytes());
		return new TxEventQSourceRecord(null, null, topic, shardNum / 2, null, jsonPayload.getBytes(),
				TxEventQSourceRecord.PayloadType.JSON, msgId);
	}

	/**
	 * Checks the type of TxEventQ that the dequeue will be performed on and calls
	 * the appropriate method to perform the dequeue.
	 * 
	 * @return A SourceRecord object containing the message that has been dequeued.
	 */
	public SourceRecord receive(OracleConnection conn) {
		String getQueueType = getQueueTableType(conn,
				this.config.getString(TxEventQConnectorConfig.TXEVENTQ_QUEUE_NAME).toUpperCase());
		log.debug("[{}]:[{}] Queue table {} is a {} type table.", Thread.currentThread().getId(), conn,
				this.config.getString(TxEventQConnectorConfig.TXEVENTQ_QUEUE_NAME), getQueueType);
		if (getQueueType.equalsIgnoreCase("JMS_BYTES")) {
			return receiveJmsMessage(conn);
		} else if (getQueueType.equalsIgnoreCase("RAW") || getQueueType.equalsIgnoreCase("JSON")) {
			return receiveAQMessage(conn, getQueueType);
		} else {
			log.error("Supported queue types are: JMS_BYTES, RAW, and JSON");
			return null;
		}
	}

	/**
	 * Dequeues either a JSON or RAW message from the TxEventQ.
	 * 
	 * @param queueType A String indicating the type of queue that the dequeue will
	 *                  be performed on.
	 * 
	 * @return A SourceRecord object containing the message that has been dequeued.
	 */
	public SourceRecord receiveAQMessage(OracleConnection conn, String queueType) {
		log.info("[{}] Waiting for RAW messages....", Thread.currentThread().getId());
		AQDequeueOptions deqopt = new AQDequeueOptions();

		deqopt.setRetrieveMessageId(true);
		try {
			String subscriber = this.config.getString(TxEventQConnectorConfig.TXEVENTQ_SUBSCRIBER_CONFIG);
			deqopt.setConsumerName(subscriber);
			deqopt.setDequeueMode(AQDequeueOptions.DequeueMode.REMOVE);
			deqopt.setVisibility(AQDequeueOptions.VisibilityOption.ON_COMMIT);
			deqopt.setNavigation(AQDequeueOptions.NavigationOption.NEXT_MESSAGE);
			deqopt.setDeliveryFilter(AQDequeueOptions.DeliveryFilter.PERSISTENT);
		} catch (SQLException e) {
			log.error("Error setting AQDequeueOptions: {}", e.toString());
			return null;
		}

		AQMessage msg = null;
		byte[] msgId = new byte[0];
		try {
			String txEventQTopic = this.config.getString(TxEventQConnectorConfig.TXEVENTQ_QUEUE_NAME);
			msg = conn.dequeue(txEventQTopic, deqopt, queueType);
			msgId = msg.getMessageId();
			if (msgId == null) {
				log.error("[{}] Message ID is null.", Thread.currentThread().getId());
				return null;
			}

			if (queueType.equalsIgnoreCase("RAW")) {
				return processRawPayload(msgId, msg);
			}

			if (queueType.equalsIgnoreCase("JSON")) {
				return processJsonPayload(msgId, msg);
			}

		} catch (SQLException e) {
			log.error("Error occurred while attempting to dequeue message.");
			return null;
		}

		return null;
	}

	/**
	 * Processes the JMS bytes message that was dequeued and creates a new
	 * TxEventQSourceRecord.
	 * 
	 * @param msgId The message Id of the message that was dequeued.
	 * @param msg   The message that was dequeued.
	 * @return A TxEventQSourceRecord containing the message information, Kafka
	 *         topic, and partitions location for Kafka to store the message at.
	 * @throws SQLException
	 */
	private TxEventQSourceRecord processJmsBytes(byte[] msgId, JMSMessage msg) {
		byte[] bytePayload = msg.getPayload();
		String topic = this.config.getString(TxEventQConnectorConfig.KAFKA_TOPIC);
		String msgIdStr = byteArrayToHex(msgId);
		int shardNum = getShardId(msgIdStr);
		log.debug("[{}] Processing JMS_BYTES message:[msgId: {}, shardNum: {}, bytes: {}]",
				Thread.currentThread().getId(), msgIdStr, shardNum, bytePayload);
		return new TxEventQSourceRecord(null, null, topic, shardNum / 2, null, bytePayload,
				TxEventQSourceRecord.PayloadType.JMS, msgId);
	}

	/**
	 * Gets the shard number that the message is located in the TxEventQ by looking
	 * at the message Id string.
	 * 
	 * @param messageId The message Id string to obtain the shard number information
	 *                  from.
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
	 * @throws java.sql.SQLException
	 */
	public int getNumOfShardsForQueue(OracleConnection conn, String queue) throws java.sql.SQLException {
		log.debug("[{}]:[{}]: Called getNumOfShardsForQueue", Thread.currentThread().getId(), conn);
		CallableStatement getnumshrdStmt = null;
		int numShard;
		getnumshrdStmt = conn.prepareCall("{call dbms_aqadm.get_queue_parameter(?,?,?)}");
		getnumshrdStmt.setString(1, queue);
		getnumshrdStmt.setString(2, "SHARD_NUM");
		getnumshrdStmt.registerOutParameter(3, Types.INTEGER);
		getnumshrdStmt.execute();
		numShard = getnumshrdStmt.getInt(3);
		getnumshrdStmt.close();
		log.debug("Number of shards for {}: {}", queue, numShard);
		return numShard;
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
				this.config.getList(TxEventQConnectorConfig.BOOTSTRAP_SERVERS_CONFIG));
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
	 * Dequeues a JMS type message from the TxEventQ and creates a SourceRecord that
	 * will be used by Kafka to populate the specified Kafaka topic.
	 * 
	 * @return A SourceRecord to be used by Kafka.
	 */
	public SourceRecord receiveJmsMessage(OracleConnection conn) {
		JMSDequeueOptions jmsDeqOpt = new JMSDequeueOptions();
		JMSMessage jmsMesg = null;

		/* set consumer name */
		jmsDeqOpt.setConsumerName(this.config.getString(TxEventQConnectorConfig.TXEVENTQ_SUBSCRIBER_CONFIG));
		/* set dequeue mode */
		jmsDeqOpt.setDequeueMode(oracle.jdbc.internal.JMSDequeueOptions.DequeueMode.REMOVE);
		/* set the wait time */
		jmsDeqOpt.setWait(10);
		/* Set visibility */
		jmsDeqOpt.setVisibility(oracle.jdbc.internal.JMSDequeueOptions.VisibilityOption.ON_COMMIT);
		/* set retrieve msgid */
		jmsDeqOpt.setRetrieveMessageId(true);
		jmsDeqOpt.setDeliveryMode(oracle.jdbc.internal.JMSDequeueOptions.DeliveryFilter.PERSISTENT);

		/* Dequeue */
		try {
			jmsMesg = ((oracle.jdbc.internal.OracleConnection) conn)
					.jmsDequeue(this.config.getString(TxEventQConnectorConfig.TXEVENTQ_QUEUE_NAME), jmsDeqOpt);
			JMSMessageProperties jmsMsgProps = jmsMesg.getJMSMessageProperties();
			JMSMessageProperties.JMSMessageType msgType = jmsMsgProps.getJMSMessageType();
			log.debug(
					"[{}] JMSMessageProperties values: [Message Type = {}, Header Properties = {}, User Properties = {}]",
					Thread.currentThread().getId(), msgType.getCode(), jmsMsgProps.getHeaderProperties(),
					jmsMsgProps.getUserProperties());

			byte[] msgId = jmsMesg.getMessageId();
			if (msgId == null) {
				log.error("[{}] Message ID is null.", Thread.currentThread().getId());
				return null;
			}

			return processJmsBytes(msgId, jmsMesg);

		} catch (SQLException ex) {
			log.error("Error SQLException: {}", ex.toString());
		}
		return null;
	}

	/**
	 * Gets the type of queue table.
	 * 
	 * @param conn      Connection to the database.
	 * @param queueName The name of the queue table to check the type for.
	 * @return A string value indicating queue tables type.
	 */
	public String getQueueTableType(OracleConnection conn, String queueName) {

		try (PreparedStatement statement = conn
				.prepareStatement("SELECT type, queue_table from user_queue_tables where queue_table = ?")) {
			statement.setString(1, queueName);
			try (ResultSet rs = statement.executeQuery()) {
				if (rs.next()) {
					return rs.getString("type");
				}
			}
		} catch (Exception e) {
			throw new ConnectException("Error unable to get the queue table type: " + e.toString());
		}
		return null;
	}

	/**
	 * Closes this stream and releases any system resources associated with it. If
	 * the stream is already closed then invoking this method has no effect.
	 *
	 * <p>
	 * As noted in {@link AutoCloseable#close()}, cases where the close may fail
	 * require careful attention. It is strongly advised to relinquish the
	 * underlying resources and to internally <em>mark</em> the {@code Closeable} as
	 * closed, prior to throwing the {@code IOException}.
	 *
	 * @throws IOException if an I/O error occurs
	 */
	@Override
	public void close() throws IOException {
		log.info("[{}]:[{}] Close Oracle TxEventQ Connections.", Thread.currentThread().getId(), this.conn);
		try {
			this.conn.close();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
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