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

package oracle.jdbc.txeventq.kafka.connect.sink.task;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import oracle.jdbc.txeventq.kafka.connect.common.utils.AppInfoParser;
import oracle.jdbc.txeventq.kafka.connect.sink.utils.TxEventQProducer;
import oracle.jdbc.txeventq.kafka.connect.sink.utils.TxEventQSinkConfig;

public class TxEventQSinkTask extends SinkTask{
	private static final Logger log = LoggerFactory.getLogger(TxEventQSinkTask.class);
	private TxEventQSinkConfig config;
	private TxEventQProducer producer; 

	@Override
	public String version() {
		return AppInfoParser.getVersion();
	}

	@Override
	public void start(Map<String, String> properties) {
		log.info("[{}] Starting Kafka Connect for Oracle TxEventQ - Sink Task", Thread.currentThread().getId());

        // Loading Task Configuration
        try {
            config = new TxEventQSinkConfig(properties);
        } catch (ConfigException ce) {
            log.error("[{}] Couldn't start TxEventQSinkTask due to configuration error", Thread.currentThread().getId());
            throw new ConnectException("Couldn't start TxEventQSinkTask due to configuration error", ce);
        }
        
        producer = new TxEventQProducer(config);
        producer.connect();
        
        if (!producer.kafkaTopicExists(this.config.getString(TxEventQSinkConfig.KAFKA_TOPIC))){
            throw new ConnectException("The Kafka topic " + this.config.getString(TxEventQSinkConfig.KAFKA_TOPIC) + " does not exist.");
        }
        
        try {
            if (!producer.txEventQueueExists(this.config.getString(TxEventQSinkConfig.TXEVENTQ_QUEUE_NAME))){
                throw new ConnectException("The TxEventQ queue name " + this.config.getString(TxEventQSinkConfig.TXEVENTQ_QUEUE_NAME) + " does not exist.");
            }
        } catch (SQLException e1) {
            throw new ConnectException("Error attempting to validate the existence of the TxEventQ queue name: " + e1.toString());
        }
       
        try {
            int kafkaPartitionNum = producer.getKafkaTopicPartitionSize(this.config.getString(TxEventQSinkConfig.KAFKA_TOPIC));
            int txEventQShardNum = producer.getNumOfShardsForQueue(this.config.getString(TxEventQSinkConfig.TXEVENTQ_QUEUE_NAME));
            if (kafkaPartitionNum > txEventQShardNum) {
                throw new ConnectException("The number of Kafka partitions " + kafkaPartitionNum + " must be less than or equal the number TxEventQ event stream " + txEventQShardNum);
            }    
        } catch (SQLException e) {
           throw new ConnectException("Error attempting to validate the Kafka partition size is valid compared to the TxEventQ event stream: " + e.toString());
        }
	}

	@Override
	public void put(Collection<SinkRecord> records) {
		if (records.isEmpty()) {
			return;
		}
		// Check if TxEventQ producer is open to produce, if not open it.
        if (!this.producer.isConnOpen()) {
            log.info("[{}] Connection is closed. Connecting...", Thread.currentThread().getId());
            this.producer.connect();
        }
        producer.put(records);
	}

	@Override
	public void stop() {
	    log.info("[{}] Stopping Kafka Connect for Oracle TxEventQ - Sink Task", Thread.currentThread().getId());
        if (this.producer.isConnOpen()) {
            try {
                this.producer.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
		
	}

}
