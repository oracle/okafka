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

package oracle.jdbc.txeventq.kafka.connect.source.task;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.errors.ConnectException;

import oracle.jdbc.OracleConnection;
import oracle.jdbc.txeventq.kafka.connect.common.utils.AppInfoParser;
import oracle.jdbc.txeventq.kafka.connect.source.utils.TxEventQConnectorConfig;
import oracle.jdbc.txeventq.kafka.connect.source.utils.TxEventQConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TxEventQSourceTask extends SourceTask {
	static final Logger log = LoggerFactory.getLogger(TxEventQSourceTask.class);
	private String connectorName;

	private int batchSize;

	private TxEventQConnectorConfig config;
	private TxEventQConsumer consumer = null;
	private OracleConnection conn = null;

	@Override
	public String version() {
		return AppInfoParser.getVersion();
	}

	/**
	 * Start the Task. This should handle any configuration parsing and one-time
	 * setup of the task.
	 * 
	 * @param properties initial configuration
	 */
	@Override
	public void start(Map<String, String> properties) {
		log.info("[{}] Starting Kafka Connect for Oracle TxEventQ - Source Task", Thread.currentThread().getId());

		// Loading Task Configuration
		try {
			config = new TxEventQConnectorConfig(properties);
			consumer = new TxEventQConsumer(config);
		} catch (ConfigException ce) {
			log.error("[{}] Couldn't start TxEventQSourceTask due to configuration error",
					Thread.currentThread().getId());
			throw new ConnectException("Couldn't start TxEventQSourceTask due to configuration error", ce);
		}

		this.connectorName = this.config.name();
		
		// For version 1 batch size will be set to 1.
		this.batchSize = 1;
		
		try {
			if (!this.consumer.isConnOpen(this.conn)) {
				this.conn = this.consumer.connect();
			}else {
				log.debug("[{}] Connection [{}] is already open.", Thread.currentThread().getId(), this.conn);
			}
		} catch (SQLException e) {
			log.error("Database connection error occurred attempting to start task.", e);
			throw new ConnectException(e);
		}

		try {
			int kafkaPartitionNum = this.consumer
					.getKafkaTopicPartitionSize(this.config.getString(TxEventQConnectorConfig.KAFKA_TOPIC));
			int txEventQShardNum = this.consumer
					.getNumOfShardsForQueue(this.conn, this.config.getString(TxEventQConnectorConfig.TXEVENTQ_QUEUE_NAME));
			if (kafkaPartitionNum < txEventQShardNum / 2) {
				throw new ConnectException("The number of Kafka partitions " + kafkaPartitionNum
						+ " must be greater than or equal to " + txEventQShardNum / 2);
			}

		} catch (SQLException e) {
			throw new ConnectException(
					"Error attempting to validate the Kafka partition size is valid compared to the TxEventQ event stream: "
							+ e.toString());
		}

		log.info("[{}]:[{}] Connector [{}] Source Task started!", Thread.currentThread().getId(), this.conn, this.connectorName);
	}
	

	/**
	 * Poll this source task for new records. If no data is currently available,
	 * this method should block but return control to the caller regularly (by
	 * returning {@code null}) in order for the task to transition to the
	 * {@code PAUSED} state if requested to do so.
	 * <p>
	 * The task will be {@link #stop() stopped} on a separate thread, and when that
	 * happens this method is expected to unblock, quickly finish up any remaining
	 * processing, and return.
	 *
	 * @return a list of source records
	 */
	@Override
	public List<SourceRecord> poll() throws InterruptedException {
		log.info("[{}]:[{}] Entry TxEventQ SourceTask poll.", Thread.currentThread().getId(), this.conn);
		List<SourceRecord> records = null;

		for (int i = 0; i < this.batchSize; i++) {
			log.info("[{}]:[{}] receiving TxEventQ Messages.", Thread.currentThread().getId(), this.conn);

			// Check if TxEventQ Consumer is open to consume, if not open it.
			try {
				if (!this.consumer.isConnOpen(this.conn)) {
					this.conn = this.consumer.connect();
				}else {
					log.debug("[{}] Connection [{}] is already open.", Thread.currentThread().getId(), this.conn);
				}
			} catch (SQLException e) {
				log.error("Database connection error occurred attempting to poll records.", e);
				throw new ConnectException(e);
			}
			
			SourceRecord txEventQRecord = this.consumer.receive(this.conn);
			log.info("[{}]:[{}] The source record: {}", Thread.currentThread().getId(), this.conn, txEventQRecord);

			if (txEventQRecord == null) {
				return records;
			}

			if (records == null) {
				records = new ArrayList<>();
			}

			records.add(txEventQRecord);
		}

		log.info("[{}]:[{}] Returning {} records", Thread.currentThread().getId(), this.conn, recordCount(records));

		return records;
	}

	/**
	 * Returns the SourceRecord count in the list.
	 * 
	 * @param records The list of SourceRecords.
	 * @return An integer value indicating the number of SourceRecords in the list.
	 */
	private int recordCount(List<SourceRecord> records) {
		return (records == null) ? 0 : records.size();
	}

	/**
	 * <p>
	 * Commit an individual {@link SourceRecord} when the callback from the producer
	 * client is received. This method is also called when a record is filtered by a
	 * transformation or when "errors.tolerance" is set to "all" and thus will never
	 * be ACK'd by a broker. In both cases {@code metadata} will be null.
	 * <p>
	 *
	 * @param record   {@link SourceRecord} that was successfully sent via the
	 *                 producer, filtered by a transformation, or dropped on
	 *                 producer exception
	 * @param metadata {@link RecordMetadata} record metadata returned from the
	 *                 broker, or null if the record was filtered or if producer
	 *                 exceptions are ignored
	 * @throws InterruptedException
	 */
	@Override
	public void commitRecord(SourceRecord record, RecordMetadata metadata) throws InterruptedException {

		try {
			if (this.consumer.isConnOpen(this.conn)) {
				log.info("[{}]:[{}] Committing record: {} .", Thread.currentThread().getId(), this.conn, record);
				this.conn.commit();
			}
		} catch (SQLException e) {
			log.error("Error occurred attempting to commit record.", e);
			throw new ConnectException(e);
		}
	}

	/**
	 * Signal this SourceTask to stop. In SourceTasks, this method only needs to
	 * signal to the task that it should stop trying to poll for new data and
	 * interrupt any outstanding poll() requests. It is not required that the task
	 * has fully stopped. Note that this method necessarily may be invoked from a
	 * different thread than {@link #poll()} and {@link #commit()}.
	 */
	@Override
	public void stop() {
		log.info("[{}]:[{}] Stopping Kafka Connect for Oracle TxEventQ - Source Task", Thread.currentThread().getId(),
				this.conn);

		try {
			if (this.consumer.isConnOpen(this.conn)) {
				closeDatabaseConnection();
			}
		} catch (SQLException e) {
			log.error("Error occurred attempting to stop SourceTask.", e);
			throw new ConnectException(e);
		}
	}
	
	/**
	 * Calls the consumer's close method to close the database connection.
	 */
	private void closeDatabaseConnection() {
		try {
			this.consumer.close();
		} catch (IOException e) {
			log.error("Exception thrown while closing connection.", e);
		}
	}

}
