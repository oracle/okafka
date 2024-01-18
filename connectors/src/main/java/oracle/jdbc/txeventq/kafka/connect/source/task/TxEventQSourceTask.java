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

package oracle.jdbc.txeventq.kafka.connect.source.task;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import oracle.jdbc.txeventq.kafka.connect.common.utils.AppInfoParser;
import oracle.jdbc.txeventq.kafka.connect.source.utils.TxEventQConnectorConfig;
import oracle.jdbc.txeventq.kafka.connect.source.utils.TxEventQConsumer;

public class TxEventQSourceTask extends SourceTask {

    static final Logger log = LoggerFactory.getLogger(TxEventQSourceTask.class);
    private String connectorName;

    private int batchSize;

    // Used to indicate when a batch has completed.
    private CountDownLatch batchCompleteIndicator = null;

    // This will be incremented each time poll() is called
    private AtomicInteger pollRotation = new AtomicInteger(1);

    // The value of the pollRotation the last time commit() was called
    private int lastCommitPollRotation = 0;

    // Indicates whether stop has been requested.
    private AtomicBoolean stopNow = new AtomicBoolean();

    private TxEventQConnectorConfig config;
    private TxEventQConsumer consumer = null;

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    /**
     * Start the Task. This should handle any configuration parsing and one-time setup of the task.
     * 
     * @param properties initial configuration
     */
    @Override
    public void start(Map<String, String> properties) {
        log.trace("[{}] Entry {}.start, props={}", Thread.currentThread().getId(),
                this.getClass().getName(), properties);

        // Loading Task Configuration
        this.config = new TxEventQConnectorConfig(properties);
        this.consumer = new TxEventQConsumer(config);

        this.connectorName = this.config.name();

        this.batchSize = this.config.getInt(TxEventQConnectorConfig.TASK_BATCH_SIZE_CONFIG);

        log.debug("The batch size is: {}", this.batchSize);

        this.consumer.connect();

        int kafkaPartitionNum = this.consumer.getKafkaTopicPartitionSize(
                this.config.getString(TxEventQConnectorConfig.KAFKA_TOPIC));
        int txEventQShardNum = this.consumer.getNumOfShardsForQueue(
                this.config.getString(TxEventQConnectorConfig.TXEVENTQ_QUEUE_NAME));
        if (kafkaPartitionNum < txEventQShardNum) {
            throw new ConnectException("The number of Kafka partitions " + kafkaPartitionNum
                    + " must be greater than or equal to " + txEventQShardNum);
        }

        log.trace("[{}]:[{}] Exit {}.start", Thread.currentThread().getId(),
                this.consumer.getDatabaseConnection(), this.getClass().getName());
    }

    /**
     * Poll this source task for new records. If no data is currently available, this method should
     * block but return control to the caller regularly (by returning {@code null}) in order for the
     * task to transition to the {@code PAUSED} state if requested to do so.
     * <p>
     * The task will be {@link #stop() stopped} on a separate thread, and when that happens this
     * method is expected to unblock, quickly finish up any remaining processing, and return.
     *
     * @return a list of source records
     */
    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        log.trace("[{}]:[{}] Entry {}.poll", Thread.currentThread().getId(),
                this.consumer.getDatabaseConnection(), this.getClass().getName());

        List<SourceRecord> records = new ArrayList<>();

        int messageCount = 0;

        /**
         * Committing unless there are errors between receiving the message TxEventQ and converting
         * it.
         */
        if (batchCompleteIndicator != null) {
            log.debug("[{}][{}] Awaiting batch completion signal", Thread.currentThread().getId(),
                    this.consumer.getDatabaseConnection());
            batchCompleteIndicator.await();

            log.debug("[{}]:[{}] Committing records", Thread.currentThread().getId(),
                    this.consumer.getDatabaseConnection());
            this.consumer.commit();
        }

        /**
         * This is a counter to keep track of how many times poll is called in order for us to know
         * if we are stuck waiting for the commitRecord callbacks to indicate whether the batch has
         * completed.
         */

        final int currentPollRotation = pollRotation.incrementAndGet();
        log.debug("[{}]:[{}] Starting poll rotation {}", Thread.currentThread().getId(),
                this.consumer.getDatabaseConnection(), currentPollRotation);

        try {
            if (!stopNow.get()) {
                log.debug("[{}]:[{}]:[{}] Polling for TxEventQ messages.",
                        Thread.currentThread().getId(), this,
                        this.consumer.getDatabaseConnection());

                records = this.consumer.receive(this.batchSize);

                if (records != null && !records.isEmpty()) {
                    messageCount = messageCount + records.size();
                }

            } else {
                log.debug("[{}]:[{}] Stopping polling for records", Thread.currentThread().getId(),
                        this.consumer.getDatabaseConnection());
            }
        } catch (final ConnectException exc) {
            log.error("{}:", exc.getClass().getName(), exc);
            messageCount = 0;
            records.clear();
        }

        synchronized (this) {
            if (messageCount > 0) {
                if (!stopNow.get()) {
                    batchCompleteIndicator = new CountDownLatch(messageCount);
                } else {
                    log.debug("Task is stopping, a batch of {} records is being removed.",
                            messageCount);
                    records.clear();
                    batchCompleteIndicator = null;
                }
            } else {
                batchCompleteIndicator = null;
            }
        }

        log.trace("[{}]:[{}]  Exit {}.poll retvalSize={} messageCount={}",
                Thread.currentThread().getId(), this.consumer.getDatabaseConnection(),
                this.getClass().getName(), recordCount(records), messageCount);

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

    @Override
    public void commit() throws InterruptedException {
        log.trace("[{}]:[{}] Entry {}.commit.", Thread.currentThread().getId(),
                this.consumer.getDatabaseConnection(), this.getClass().getName());

        /*
         * Checks that the all the messages in a batch are complete and not stuck. If this method is
         * called it means that Kafka Connect thinks that all messages have been completed. If this
         * is the case then commitRecord has been called for all messages. In the event that not all
         * the messages have been called by commitRecord, the connector may continue to wait for a
         * long time. In order to ensure this does not happen if the commit callback is called twice
         * without the poll rotation increasing then the batch complete indicator will be triggered
         * directly.
         */
        final int currentPollRotation = pollRotation.get();
        log.debug("Commit starting in poll rotation {}", currentPollRotation);

        if (lastCommitPollRotation == currentPollRotation) {
            synchronized (this) {
                if (batchCompleteIndicator != null) {
                    log.debug("Increase batch complete indicator by {}",
                            batchCompleteIndicator.getCount());

                    // This indicates that we are stuck because the connector is waiting for the
                    // signal in the poll() method and it's been waiting for at least two calls
                    // to this commit callback.
                    while (batchCompleteIndicator.getCount() > 0) {
                        batchCompleteIndicator.countDown();
                    }
                }
            }
        } else {
            lastCommitPollRotation = currentPollRotation;
        }

        log.trace("[{}]:[{}]  Exit {}.commit", Thread.currentThread().getId(),
                this.consumer.getDatabaseConnection(), this.getClass().getName());
    }

    /**
     * <p>
     * Commit an individual {@link SourceRecord} when the callback from the producer client is
     * received. This method is also called when a record is filtered by a transformation or when
     * "errors.tolerance" is set to "all" and thus will never be ACK'd by a broker. In both cases
     * {@code metadata} will be null.
     * <p>
     *
     * @param record   {@link SourceRecord} that was successfully sent via the producer, filtered by
     *                 a transformation, or dropped on producer exception
     * @param metadata {@link RecordMetadata} record metadata returned from the broker, or null if
     *                 the record was filtered or if producer exceptions are ignored
     * @throws InterruptedException
     */
    @Override
    public void commitRecord(SourceRecord record, RecordMetadata metadata)
            throws InterruptedException {

        log.trace("[{}]:[{}] Entry {}.commitRecord, record={}", Thread.currentThread().getId(),
                this.consumer.getDatabaseConnection(), this.getClass().getName(), record);

        batchCompleteIndicator.countDown();

        log.trace("[{}]:[{}]  Exit {}.commitRecord", Thread.currentThread().getId(),
                this.consumer.getDatabaseConnection(), this.getClass().getName());
    }

    /**
     * Signal this SourceTask to stop. In SourceTasks, this method only needs to signal to the task
     * that it should stop trying to poll for new data and interrupt any outstanding poll()
     * requests. It is not required that the task has fully stopped. Note that this method
     * necessarily may be invoked from a different thread than {@link #poll()} and
     * {@link #commit()}.
     */
    @Override
    public void stop() {
        log.trace("[{}]:[{}] Entry {}.stop", Thread.currentThread().getId(),
                this.consumer.getDatabaseConnection(), this.getClass().getName());

        stopNow.set(true);

        synchronized (this) {
            if (this.consumer != null) {
                try {
                    this.consumer.close();
                } catch (IOException e) {
                    throw new ConnectException(e.getMessage());
                }
            }
        }

        log.trace("[{}] Exit {}.stop", Thread.currentThread().getId(), this.getClass().getName());

    }
}
