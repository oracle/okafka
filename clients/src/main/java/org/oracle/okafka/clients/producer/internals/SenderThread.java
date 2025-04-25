/*
 ** OKafka Java Client version 23.4.
 **
 ** Copyright (c) 2019, 2024 Oracle and/or its affiliates.
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

/*
 * 04/20/2020: This file is modified to support Kafka Java Client compatability to Oracle Transactional Event Queues.
 *
 */

package org.oracle.okafka.clients.producer.internals;

import java.net.ConnectException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.oracle.okafka.clients.KafkaClient;
import org.oracle.okafka.clients.Metadata;
import org.apache.kafka.clients.RequestCompletionHandler;
import org.oracle.okafka.common.requests.ProduceRequest;
import org.oracle.okafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.internals.SenderMetricsRegistry;
//import org.oracle.okafka.clients.producer.internals.SenderMetricsRegistry;
import org.oracle.okafka.common.requests.ProduceResponse;
import org.oracle.okafka.common.utils.MessageIdConverter;
import org.oracle.okafka.common.utils.MessageIdConverter.OKafkaOffset;
import org.oracle.okafka.common.Node;
import org.oracle.okafka.common.errors.ConnectionException;
import org.oracle.okafka.common.errors.InvalidLoginCredentialsException;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.errors.InvalidMetadataException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.NotLeaderForPartitionException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Meter;
import org.apache.kafka.common.requests.ProduceResponse.RecordError;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;

import static org.apache.kafka.common.record.RecordBatch.NO_TIMESTAMP;

/**
 * The background thread that handles the sending of produce requests to the
 * Kafka cluster. This thread makes metadata requests to renew its view of the
 * cluster and then sends produce requests to the appropriate nodes.
 */
public class SenderThread implements Runnable {

	private final Logger log;
	/* the record accumulator that batches records */
	private final RecordAccumulator accumulator;

	private final KafkaClient client;

	/* the metadata for the client */
	private final Metadata metadata;

	/*
	 * the flag indicating whether the producer should guarantee the message order
	 * on the broker or not.
	 */
	private final boolean guaranteeMessageOrder;

	/* the client id used to identify this client in requests to the server */
	private final String clientId;

	/* the current correlation id to use when sending requests to servers */
	private int correlation;

	private final int maxRequestSize;

	/* the number of times to retry a failed request before giving up */
	private final int retries;

	/* the number of acknowledgements to request from the server */
	private final short acks;

	/* the clock instance used for getting the time */
	private final Time time;

	/* the max time to wait for the server to respond to the request */
	private final int requestTimeoutMs;

	/* The max time to wait before retrying a request which has failed */
	private final long retryBackoffMs;

	/* true while the sender thread is still running */
	private volatile boolean running;

	/*
	 * true when the caller wants to ignore all unsent/inflight messages and force
	 * close.
	 */
	private volatile boolean forceClose;

	/* metrics */
	private final SenderMetrics sensors;

	private Object syncObject = new Object();
	private final ProducerConfig config;

	public SenderThread(LogContext logContext, String clientId, KafkaClient client, Metadata metadata,
			RecordAccumulator accumulator, boolean guaranteeMessageOrder, ProducerConfig pConfig, short acks,
			int retries, SenderMetricsRegistry metricsRegistry, Time time) {
		this.log = logContext.logger(SenderThread.class);
		this.clientId = clientId;
		this.accumulator = accumulator;
		this.client = client;
		this.metadata = metadata;
		this.guaranteeMessageOrder = guaranteeMessageOrder;
		this.config = pConfig;
		this.maxRequestSize = config.getInt(ProducerConfig.MAX_REQUEST_SIZE_CONFIG);
		this.correlation = 0;
		this.running = true;
		this.acks = acks;
		this.time = time;
		this.sensors = new SenderMetrics(metricsRegistry, metadata, client, time);
		this.retries = retries;
		this.requestTimeoutMs = config.getInt(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG);
		this.retryBackoffMs = config.getLong(ProducerConfig.RETRY_BACKOFF_MS_CONFIG);
	}

	/**
	 * The main run loop for the sender thread
	 */
	public void run() {
		log.debug("Starting Kafka producer I/O thread.");
		// main loop, runs until close is called
		while (running) {
			try {
				run(time.milliseconds());
			} catch (ConnectionException ce) {
				log.error("Connection error in Kafka Producer I/O Thread: ", ce);
				Throwable e = ce.getCause();
				if (e instanceof ConnectException) {
					running = false;
				}
			} catch (Exception e) {
				log.error("Uncaught error in kafka producer I/O thread: ", e);
				if (e instanceof InvalidLoginCredentialsException)
					running = false;
			}
		}
		log.debug("Beginning shutdown of Kafka producer I/O thread, sending remaining records.");
		// okay we stopped accepting requests but there may still be
		// requests in the accumulator or waiting for acknowledgment,
		// wait until these are completed.
		while (!forceClose && this.accumulator.hasUndrained()) {
			try {
				run(time.milliseconds());
			} catch (Exception e) {
				log.error("Uncaught error in kafka producer I/O thread: ", e);
			}
		}
		if (forceClose) {
			// We need to fail all the incomplete batches and wake up the threads waiting on
			// the futures.
			log.debug("Aborting incomplete batches due to forced shutdown");
			this.accumulator.abortIncompleteBatches();
		}
		try {
			this.client.close();
		} catch (Exception ex) {
			log.error("failed to close AQ producer", ex);
		}

		log.debug("Shutdown of Kafka producer I/O thread has completed.");

	}

	/**
	 * Run a single iteration of sending
	 *
	 * @param now The current POSIX time in milliseconds
	 */
	void run(long now) {
		long pollTimeOut = sendProducerData(now);
		client.maybeUpdateMetadata(now);
		try {
			long sleepTime = pollTimeOut;
			if (sleepTime == Long.MAX_VALUE) {
				Long lingerConfig = config.getLong(ProducerConfig.LINGER_MS_CONFIG);

				if (lingerConfig.longValue() > 0)
					sleepTime = (int) Math.min(config.getLong(ProducerConfig.LINGER_MS_CONFIG), 500);
				else
					sleepTime = 500;
			}
			if (sleepTime > 0) {
				log.debug("Sender waiting for " + sleepTime);
				try {
					synchronized (syncObject) {
						syncObject.wait(sleepTime);
					}
				} catch (Exception sleepE) {
				}
				// Thread.sleep(sleepTime);
			}
		} catch (Exception e) {
		}
	}

	private long sendProducerData(long now) {
		// get the list of partitions with data ready to send
		RecordAccumulator.ReadyCheckResult result = this.accumulator.ready(metadata.fetch(), now);

		if (!result.unknownLeaderTopics.isEmpty()) {
			// The set of topics with unknown leader contains topics with leader election
			// pending as well as
			// topics which may have expired. Add the topic again to metadata to ensure it
			// is included
			// and request metadata update, since there are messages to send to the topic.
			for (String topic : result.unknownLeaderTopics)
				this.metadata.add(topic);

			log.debug("Requesting metadata update due to unknown leader topics from the batched records: {}",
					result.unknownLeaderTopics);

			this.metadata.requestUpdate();
		}
		// log.debug("ReadyNodes Size " + result.readyNodes.size());
		// remove any nodes we aren't ready to send to

		Iterator<org.apache.kafka.common.Node> iter = result.readyNodes.iterator();
		long notReadyTimeout = Long.MAX_VALUE;
		while (iter.hasNext()) {
			Node node = (org.oracle.okafka.common.Node) iter.next();
			if (!this.client.ready(node, now)) {
				iter.remove();
				log.debug("Node " + node + " is not ready and is removed for now");
				notReadyTimeout = Math.min(notReadyTimeout, this.client.pollDelayMs(node, now));
			}
		}

		// create produce requests
		Map<Integer, List<ProducerBatch>> batches = this.accumulator.drain(metadata.fetch(), result.readyNodes,
				maxRequestSize, now);

		if (guaranteeMessageOrder) {
			// Mute all the partitions drained
			for (List<ProducerBatch> batchList : batches.values()) {
				for (ProducerBatch batch : batchList)
					this.accumulator.mutePartition(batch.topicPartition);
			}
		}

		// List<ProducerBatch> expiredBatches =
		// this.accumulator.expiredBatches(this.requestTimeoutMs, now);

		List<ProducerBatch> expiredBatches = this.accumulator.expiredBatches(this.requestTimeoutMs);

		// Reset the producer id if an expired batch has previously been sent to the
		// broker. Also update the metrics
		// for expired batches. see the documentation of
		// @TransactionState.resetProducerId to understand why
		// we need to reset the producer id here.
		if (!expiredBatches.isEmpty())
			log.trace("Expired {} batches in accumulator", expiredBatches.size());
		for (ProducerBatch expiredBatch : expiredBatches) {
			String errorMessage = "Expiring " + expiredBatch.recordCount + " record(s) for "
					+ expiredBatch.topicPartition + ":" + (now - expiredBatch.createdMs)
					+ " ms has passed since batch creation";
			// failBatch(expiredBatch, NO_TIMESTAMP, new ArrayList<String>() { {
			// add("ID:00000000000000000000000000000000"); }}, new
			// TimeoutException(errorMessage));
			List<OKafkaOffset> msgIdList = new ArrayList<OKafkaOffset>();
			failBatch(expiredBatch, -1l, NO_TIMESTAMP, msgIdList,
					(RuntimeException) (new TimeoutException(errorMessage)));
		}
		sensors.updateProduceRequestMetrics(batches);
		// If we have any nodes that are ready to send + have sendable data, poll with 0
		// timeout so this can immediately
		// loop and try sending more data. Otherwise, the timeout is determined by nodes
		// that have partitions with data
		// that isn't yet sendable (e.g. lingering, backing off). Note that this
		// specifically does not include nodes
		// with sendable data that aren't ready to send since they would cause busy
		// looping.
		long pollTimeout = Math.min(result.nextReadyCheckDelayMs, notReadyTimeout);
		if (!result.readyNodes.isEmpty()) {
			log.trace("Instances with data ready to send: {}", result.readyNodes);
			// if some partitions are already ready to be sent, the select time would be 0;
			// otherwise if some partition already has some data accumulated but not ready
			// yet,
			// the select time will be the time difference between now and its linger expiry
			// time;
			// otherwise the select time will be the time difference between now and the
			// metadata expiry time;
			pollTimeout = 0;
		}
		sendProduceRequests(batches, pollTimeout);
		return pollTimeout;
	}

	/**
	 * Start closing the sender (won't actually complete until all data is sent out)
	 */
	public void initiateClose() {
		// Ensure accumulator is closed first to guarantee that no more appends are
		// accepted after
		// breaking from the sender loop. Otherwise, we may miss some callbacks when
		// shutting down.
		this.accumulator.close();
		this.running = false;
	}

	/**
	 * Closes the sender without sending out any pending messages.
	 */
	public void forceClose() {
		this.forceClose = true;
		initiateClose();
	}

	/**
	 * Transfer the record batches into a list of produce requests on a per-node
	 * basis
	 */
	private void sendProduceRequests(Map<Integer, List<ProducerBatch>> collated, long pollTimeout) {
		for (Map.Entry<Integer, List<ProducerBatch>> entry : collated.entrySet()) {
			sendProduceRequest(metadata.getNodeById(entry.getKey()), entry.getValue());
		}
	}

	/**
	 * Create a produce request from the given record batch
	 */
	private void sendProduceRequest(Node node, List<ProducerBatch> batches) {
		if (batches.isEmpty())
			return;
		// Map<String, Map<TopicPartition, MemoryRecords>> produceRecordsByTopic = new
		// HashMap<>();
		// Map<TopicPartition, ProducerBatch> batchesByPartition = new HashMap<>();
		for (ProducerBatch batch : batches) {
			boolean checkForDuplicate = false;
			List<OKafkaOffset> retryMsgIdList = null;
			/*
			 * TopicPartition tp = batch.topicPartition; MemoryRecords records =
			 * batch.records(); if(!produceRecordsByTopic.containsKey(tp.topic())) {
			 * produceRecordsByTopic.put(tp.topic(), new HashMap<TopicPartition,
			 * MemoryRecords>()); } produceRecordsByTopic.get(tp.topic()).put(tp, records);
			 * batchesByPartition.put(new TopicPartition(tp.topic(), tp.partition()),
			 * batch);
			 */

			RequestCompletionHandler callback = new RequestCompletionHandler() {
				@Override
				public void onComplete(ClientResponse response) {
					handleProduceResponse(response, batch, time.milliseconds());
				}
			};

			if (batch.inRetry() && batch.retryMsgIdList() != null) {
				checkForDuplicate = true;
				retryMsgIdList = batch.retryMsgIdList();
			}
			ProduceRequest.Builder builderRequest = new ProduceRequest.Builder(batch.topicPartition, batch.records(),
					(short) 1, -1, checkForDuplicate, retryMsgIdList);
			ClientRequest request = client.newClientRequest(node, builderRequest, time.milliseconds(), true, -1,
					callback);
			send(request, batch);
		}
	}

	/**
	 * Send produce request to destination Handle response generated from send.
	 */
	public void send(ClientRequest request, ProducerBatch batch) {
		ClientResponse response = null;
		try {
			response = client.send(request, time.milliseconds());
		} catch (Exception e) {
			log.error("Exception while sending the produce request for batch " + batch.topicPartition + " " + e, e);
			accumulator.reenqueue(batch, System.currentTimeMillis());
			return;
		}

		log.info("Batch Send complete, evaluating response " + batch.topicPartition);
		ProduceResponse pResponse = (ProduceResponse) response.responseBody();
		ProduceResponse.PartitionResponse partitionResponse = pResponse.getPartitionResponse();
		if (response.wasDisconnected()) {
			log.info("Connection to oracle database node " + response.destination() + " was broken. Retry again");

			if (partitionResponse.getCheckDuplicate()) {
				// During retry : check for the first message id.
				// If first record is published successfully then entire batch would be
				// published successfully.
				log.debug("Exception while sending publish request. Check storage before retry.");
				if (partitionResponse.msgIds != null && partitionResponse.msgIds.size() > 0) {
					log.debug("Check for message id " + partitionResponse.msgIds.get(0).getMsgId());
					batch.setRetryMsgId(partitionResponse.msgIds);
				}
			}
			accumulator.reenqueue(batch, System.currentTimeMillis());
			// Request for MetaData update since the Database instance has went down.
			int cuVNo = this.metadata.requestUpdate();
			log.debug("Requested for update of metadata from " + cuVNo);
		} else if (partitionResponse.exception != null) {
			RuntimeException producerException = partitionResponse.exception;
			if (producerException instanceof NotLeaderForPartitionException) {

				log.info("No Owner for Topic Partition " + batch.topicPartition + " retrying.");
				this.metadata.requestUpdate();
			}
			if (producerException instanceof InvalidTopicException) {
				log.info(producerException.getMessage());
				completeResponse(response);
			} else {
				log.info("Exception while sending batch for partiton " + batch.topicPartition + ". "
						+ producerException);
			}
			if (partitionResponse.getCheckDuplicate()) {
				// During retry : check for the first message id.
				// If first record is published successfully then entire batch would be
				// published successfully.
				log.debug("Exception while sending publish request. Check storage before retry.");
				if (partitionResponse.msgIds != null && partitionResponse.msgIds.size() > 0) {
					log.debug("Check for message id " + partitionResponse.msgIds.get(0).getMsgId());
					batch.setRetryMsgId(partitionResponse.msgIds);
				}
			}
			accumulator.reenqueue(batch, System.currentTimeMillis());
		} else {
			log.trace("No Exception from send. Completing the batch");
			completeResponse(response);
		}
	}

	/**
	 * Handle response using callback in a request
	 */
	private void completeResponse(ClientResponse response) {
		response.onComplete();
	}

	/**
	 * Handle a produce response
	 */
	private void handleProduceResponse(ClientResponse response, ProducerBatch batch, long now) {
		if (response.wasDisconnected()) {
			client.disconnected(metadata.getNodeById(Integer.parseInt(response.destination())), now);
			metadata.requestUpdate();
		}

		long receivedTimeMs = response.receivedTimeMs();
		int correlationId = response.requestHeader().correlationId();

		/*
		 * if (response.wasDisconnected()) { log.
		 * trace("Cancelled request with header {} due to node {} being disconnected",
		 * requestHeader, response.destination()); for (ProducerBatch batch :
		 * batches.values()) completeBatch(batch, new
		 * ProduceResponse.PartitionResponse(Errors.NETWORK_EXCEPTION), correlationId,
		 * now, 0L); } else {
		 * log.trace("Received produce response from node {} with correlation id {}",
		 * response.destination(), correlationId); if we have a response, parse it for
		 * (Map.Entry<TopicPartition, ProduceResponse.PartitionResponse> entry :
		 * response.responses().entrySet()) { TopicPartition tp = entry.getKey();
		 * ProduceResponse.PartitionResponse partResp = entry.getValue(); ProducerBatch
		 * batch = batches.get(tp); completeBatch(batch, partResp, correlationId, now,
		 * receivedTimeMs + response.throttleTimeMs()); }
		 * this.sensors.recordLatency(response.destination(),
		 * response.requestLatencyMs()); } else { this is the acks = 0 case, just
		 * complete all requests for (ProducerBatch batch : batches.values()) {
		 * completeBatch(batch, new ProduceResponse.PartitionResponse(Errors.NONE),
		 * correlationId, now, 0L); } }
		 */
		this.sensors.recordLatency(response.destination(), response.requestLatencyMs());
		ProduceResponse produceResponse = (ProduceResponse) response.responseBody();
		ProduceResponse.PartitionResponse partResp = produceResponse.getPartitionResponse();
		completeBatch(batch, partResp, correlationId, now, receivedTimeMs + produceResponse.throttleTimeMs());
	}

	/**
	 * Complete or retry the given batch of records.
	 *
	 * @param batch         The record batch
	 * @param response      The produce response
	 * @param correlationId The correlation id for the request
	 * @param now           The current POSIX timestamp in milliseconds
	 */
	private void completeBatch(ProducerBatch batch, ProduceResponse.PartitionResponse response, long correlationId,
			long now, long throttleUntilTimeMs) {
		RuntimeException exception = response.exception();

		if (exception != null) {
			if (canRetry(batch, response)) {
				reenqueueBatch(batch, now);
			} else
				failBatch(batch, response, exception);

			if (exception instanceof InvalidMetadataException) {
				metadata.requestUpdate();
			}
		} else
			completeBatch(batch, response);

		// Unmute the completed partition.
		if (guaranteeMessageOrder) {
			// this.accumulator.unmutePartition(batch.topicPartition, throttleUntilTimeMs);
			this.accumulator.unmutePartition(batch.topicPartition);
		}

	}

	private void completeBatch(ProducerBatch batch, ProduceResponse.PartitionResponse response) {

		if (batch.done(response.subPartitionId * MessageIdConverter.DEFAULT_SUBPARTITION_SIZE, response.logAppendTime,
				response.msgIds, null, null))
			this.accumulator.deallocate(batch);
	}

	private void failBatch(ProducerBatch batch, ProduceResponse.PartitionResponse response,
			RuntimeException topLevelException) {
		if (response.recordErrors == null || response.recordErrors.isEmpty()) {

			failBatch(batch, response.subPartitionId, response.logAppendTime, response.msgIds, topLevelException);

		} else {
			Map<Integer, RuntimeException> recordErrorMap = new HashMap<>(response.recordErrors.size());
			for (RecordError recordError : response.recordErrors) {
				// The API leaves us with some awkwardness interpreting the errors in the
				// response.
				// We cannot differentiate between different error cases (such as
				// INVALID_TIMESTAMP)
				// from the single error code at the partition level, so instead we use
				// INVALID_RECORD
				// for all failed records and rely on the message to distinguish the cases.
				final String errorMessage;
				if (recordError.message != null) {
					errorMessage = recordError.message;
				} else if (response.errorMessage != null) {
					errorMessage = response.errorMessage;
				} else {
					errorMessage = response.error.message();
				}

				// If the batch contained only a single record error, then we can unambiguously
				// use the exception type corresponding to the partition-level error code.
				if (response.recordErrors.size() == 1) {
					recordErrorMap.put(recordError.batchIndex, response.error.exception(errorMessage));
				} else {
					recordErrorMap.put(recordError.batchIndex, new InvalidRecordException(errorMessage));
				}
			}

			Function<Integer, RuntimeException> recordExceptions = batchIndex -> {
				RuntimeException exception = recordErrorMap.get(batchIndex);
				if (exception != null) {
					return exception;
				} else {
					// If the response contains record errors, then the records which failed
					// validation
					// will be present in the response. To avoid confusion for the remaining
					// records, we
					// return a generic exception.
					return new KafkaException("Failed to append record because it was part of a batch "
							+ "which had one more more invalid records");
				}
			};

			failBatch(batch, response.subPartitionId, response.logAppendTime, response.msgIds, topLevelException,
					recordExceptions);
		}
	}

	private void failBatch(ProducerBatch batch, long baseOffSet, long logAppendTime, List<OKafkaOffset> msgIds,
			RuntimeException exception) {
		this.sensors.recordErrors(batch.topicPartition.topic(), batch.recordCount);
		if (batch.done(baseOffSet, logAppendTime, msgIds, exception, batchIndex -> exception))
			this.accumulator.deallocate(batch);
	}

	private void failBatch(ProducerBatch batch, long baseOffSet, long logAppendTime, List<OKafkaOffset> msgIds,
			RuntimeException exception, Function<Integer, RuntimeException> recordExceptions) {
		this.sensors.recordErrors(batch.topicPartition.topic(), batch.recordCount);
		if (batch.done(baseOffSet, logAppendTime, msgIds, exception, recordExceptions))
			this.accumulator.deallocate(batch);
	}

	private void reenqueueBatch(ProducerBatch batch, long currentTimeMs) {
		this.accumulator.reenqueue(batch, currentTimeMs);
		this.sensors.recordRetries(batch.topicPartition.topic(), batch.recordCount);
	}

	/**
	 * We can retry a send if the error is transient and the number of attempts
	 * taken is fewer than the maximum allowed.
	 */
	private boolean canRetry(ProducerBatch batch, ProduceResponse.PartitionResponse response) {
		return batch.attempts() < this.retries && ((response.exception instanceof RetriableException));
	}

	public void wakeup() {
		try {
			synchronized (syncObject) {
				syncObject.notifyAll();
			}
		} catch (Exception e) {
		}
	}

	public boolean isRunning() {
		return running;
	}

	/**
	 * A collection of sensors for the sender
	 */
	private static class SenderMetrics {
		public final Sensor retrySensor;
		public final Sensor errorSensor;
		public final Sensor queueTimeSensor;
		public final Sensor requestTimeSensor;
		public final Sensor recordsPerRequestSensor;
		public final Sensor batchSizeSensor;
		public final Sensor compressionRateSensor;
		public final Sensor maxRecordSizeSensor;
		public final Sensor batchSplitSensor;
		private final SenderMetricsRegistry metrics;

		private final Time time;

		public SenderMetrics(SenderMetricsRegistry metrics, Metadata metadata, KafkaClient client, Time time) {
			this.metrics = metrics;
			this.time = time;

			this.batchSizeSensor = metrics.sensor("batch-size");
			this.batchSizeSensor.add(metrics.batchSizeAvg, new Avg());
			this.batchSizeSensor.add(metrics.batchSizeMax, new Max());

			this.compressionRateSensor = metrics.sensor("compression-rate");
			this.compressionRateSensor.add(metrics.compressionRateAvg, new Avg());

			this.queueTimeSensor = metrics.sensor("queue-time");
			this.queueTimeSensor.add(metrics.recordQueueTimeAvg, new Avg());
			this.queueTimeSensor.add(metrics.recordQueueTimeMax, new Max());

			this.requestTimeSensor = metrics.sensor("request-time");
			this.requestTimeSensor.add(metrics.requestLatencyAvg, new Avg());
			this.requestTimeSensor.add(metrics.requestLatencyMax, new Max());

			this.recordsPerRequestSensor = metrics.sensor("records-per-request");
			this.recordsPerRequestSensor.add(new Meter(metrics.recordSendRate, metrics.recordSendTotal));
			this.recordsPerRequestSensor.add(metrics.recordsPerRequestAvg, new Avg());

			this.retrySensor = metrics.sensor("record-retries");
			this.retrySensor.add(new Meter(metrics.recordRetryRate, metrics.recordRetryTotal));

			this.errorSensor = metrics.sensor("errors");
			this.errorSensor.add(new Meter(metrics.recordErrorRate, metrics.recordErrorTotal));

			this.maxRecordSizeSensor = metrics.sensor("record-size");
			this.maxRecordSizeSensor.add(metrics.recordSizeMax, new Max());
			this.maxRecordSizeSensor.add(metrics.recordSizeAvg, new Avg());

			// method inFlightRequestCount() is unavailabe in Okafka KafkaClient file

//				this.metrics.addMetric(metrics.requestsInFlight, (config, now) -> client.inFlightRequestCount());
			this.metrics.addMetric(metrics.metadataAge,
					(config, now) -> (now - metadata.lastSuccessfulUpdate()) / 1000.0);

			this.batchSplitSensor = metrics.sensor("batch-split-rate");
			this.batchSplitSensor.add(new Meter(metrics.batchSplitRate, metrics.batchSplitTotal));
		}

		private void maybeRegisterTopicMetrics(String topic) {
			// if one sensor of the metrics has been registered for the topic,
			// then all other sensors should have been registered; and vice versa
			String topicRecordsCountName = "topic." + topic + ".records-per-batch";
			Sensor topicRecordCount = this.metrics.getSensor(topicRecordsCountName);
			if (topicRecordCount == null) {
				Map<String, String> metricTags = Collections.singletonMap("topic", topic);

				topicRecordCount = this.metrics.sensor(topicRecordsCountName);
				MetricName rateMetricName = this.metrics.topicRecordSendRate(metricTags);
				MetricName totalMetricName = this.metrics.topicRecordSendTotal(metricTags);
				topicRecordCount.add(new Meter(rateMetricName, totalMetricName));

				String topicByteRateName = "topic." + topic + ".bytes";
				Sensor topicByteRate = this.metrics.sensor(topicByteRateName);
				rateMetricName = this.metrics.topicByteRate(metricTags);
				totalMetricName = this.metrics.topicByteTotal(metricTags);
				topicByteRate.add(new Meter(rateMetricName, totalMetricName));

				String topicCompressionRateName = "topic." + topic + ".compression-rate";
				Sensor topicCompressionRate = this.metrics.sensor(topicCompressionRateName);
				MetricName m = this.metrics.topicCompressionRate(metricTags);
				topicCompressionRate.add(m, new Avg());

				String topicRetryName = "topic." + topic + ".record-retries";
				Sensor topicRetrySensor = this.metrics.sensor(topicRetryName);
				rateMetricName = this.metrics.topicRecordRetryRate(metricTags);
				totalMetricName = this.metrics.topicRecordRetryTotal(metricTags);
				topicRetrySensor.add(new Meter(rateMetricName, totalMetricName));

				String topicErrorName = "topic." + topic + ".record-errors";
				Sensor topicErrorSensor = this.metrics.sensor(topicErrorName);
				rateMetricName = this.metrics.topicRecordErrorRate(metricTags);
				totalMetricName = this.metrics.topicRecordErrorTotal(metricTags);
				topicErrorSensor.add(new Meter(rateMetricName, totalMetricName));
			}
		}

		public void updateProduceRequestMetrics(Map<Integer, List<ProducerBatch>> batches) {
			long now = time.milliseconds();
			for (List<ProducerBatch> nodeBatch : batches.values()) {
				int records = 0;
				for (ProducerBatch batch : nodeBatch) {
					// register all per-topic metrics at once
					String topic = batch.topicPartition.topic();
					maybeRegisterTopicMetrics(topic);

					// per-topic record send rate
					String topicRecordsCountName = "topic." + topic + ".records-per-batch";
					Sensor topicRecordCount = Objects.requireNonNull(this.metrics.getSensor(topicRecordsCountName));
					topicRecordCount.record(batch.recordCount);

					// per-topic bytes send rate
					String topicByteRateName = "topic." + topic + ".bytes";
					Sensor topicByteRate = Objects.requireNonNull(this.metrics.getSensor(topicByteRateName));
					topicByteRate.record(batch.estimatedSizeInBytes());

					// per-topic compression rate
					String topicCompressionRateName = "topic." + topic + ".compression-rate";
					Sensor topicCompressionRate = Objects
							.requireNonNull(this.metrics.getSensor(topicCompressionRateName));
					topicCompressionRate.record(batch.compressionRatio());

					// global metrics
					this.batchSizeSensor.record(batch.estimatedSizeInBytes(), now);
					this.queueTimeSensor.record(batch.queueTimeMs(), now);
					this.compressionRateSensor.record(batch.compressionRatio());
					this.maxRecordSizeSensor.record(batch.maxRecordSize, now);
					records += batch.recordCount;
				}
				this.recordsPerRequestSensor.record(records, now);
			}

//				this.recordsPerRequestSensor.record(totalRecords, now);

		}

		public void recordRetries(String topic, int count) {
			long now = time.milliseconds();
			this.retrySensor.record(count, now);
			String topicRetryName = "topic." + topic + ".record-retries";
			Sensor topicRetrySensor = this.metrics.getSensor(topicRetryName);
			if (topicRetrySensor != null)
				topicRetrySensor.record(count, now);
		}

		public void recordErrors(String topic, int count) {
			long now = time.milliseconds();
			this.errorSensor.record(count, now);
			String topicErrorName = "topic." + topic + ".record-errors";
			Sensor topicErrorSensor = this.metrics.getSensor(topicErrorName);
			if (topicErrorSensor != null)
				topicErrorSensor.record(count, now);
		}

		public void recordLatency(String node, long latency) {
			long now = time.milliseconds();
			this.requestTimeSensor.record(latency, now);
			if (!node.isEmpty()) {
				String nodeTimeName = "node-" + node + ".latency";
				Sensor nodeRequestTime = this.metrics.getSensor(nodeTimeName);
				if (nodeRequestTime != null)
					nodeRequestTime.record(latency, now);
			}
		}

		void recordBatchSplit() {
			this.batchSplitSensor.record();
		}
	}
}
