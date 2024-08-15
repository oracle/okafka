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

package org.oracle.okafka.clients.producer;

import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.Range.between;
import static org.apache.kafka.common.config.ConfigDef.ValidString.in;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.oracle.okafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.ClientDnsLookup;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.oracle.okafka.common.config.SslConfigs;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.serialization.Serializer;

public class ProducerConfig extends AbstractConfig {


	private static final ConfigDef CONFIG;

	/** <code>oracle.instance.name</code>*/
	public static final String ORACLE_INSTANCE_NAME = CommonClientConfigs.ORACLE_INSTANCE_NAME;
	/** <code>oracle.service.name</code>*/
	public static final String ORACLE_SERVICE_NAME  = CommonClientConfigs.ORACLE_SERVICE_NAME;
	/** <code>oracle.net.tns_admin</code> */
	public static final String ORACLE_NET_TNS_ADMIN = CommonClientConfigs.ORACLE_NET_TNS_ADMIN;
	
	public static final String ORACLE_TRANSACTIONAL_PRODUCER = "oracle.transactional.producer";
	public static final String ORACLE_TRANSACTIONAL_PRODUCER_DOC = "Create a transactional producer which can be used to "
			+ "retrieve the database connection being used to publish the ProducerRecord."
			+ "Application can use getDBConnection() api to fetch the database connection."
			+ "A transactional producer will not accumulate the records in the batch "
			+ "but will immediately send it to Oracle Transactional Event Queue(TxEQ) server to publish into the topic.";

	/** <code>bootstrap.servers</code>*/
	public static final String BOOTSTRAP_SERVERS_CONFIG = CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;

	/** <code>client.dns.lookup</code> */
	public static final String CLIENT_DNS_LOOKUP_CONFIG = CommonClientConfigs.CLIENT_DNS_LOOKUP_CONFIG;

	/** <code>socket.connection.setup.timeout.ms</code> */
	public static final String SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG = CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG;

	/** <code>socket.connection.setup.timeout.max.ms</code> */
	public static final String SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG = CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG;


	/** <code>metadata.max.age.ms</code>*/
	public static final String METADATA_MAX_AGE_CONFIG = CommonClientConfigs.METADATA_MAX_AGE_CONFIG;
	private static final String METADATA_MAX_AGE_DOC = CommonClientConfigs.METADATA_MAX_AGE_DOC;

	/** <code>batch.size</code> */
	public static final String BATCH_SIZE_CONFIG = "batch.size";
	private static final String BATCH_SIZE_DOC = "The producer will attempt to batch records together into fewer requests whenever multiple records are being sent"
			+ " to the same partition. This helps performance on both the client and the server. This configuration controls the "
			+ "default batch size in bytes. "
			+ "<p>"
			+ "No attempt will be made to batch records larger than this size. "
			+ "<p>"
			+ "Each request sent to TEQ will contain only one batch, each message in batch corresponds to same partition. "
			+ "<p>"
			+ "A small batch size will make batching less common and may reduce throughput (a batch size of zero will disable "
			+ "batching entirely). A very large batch size may use memory a bit more wastefully as we will always allocate a "
			+ "buffer of the specified batch size in anticipation of additional records.";

	/** <code>acks</code> */
	public static final String ACKS_CONFIG = "acks";
	private static final String ACKS_DOC = "TEQ supports only <code>acks=all</code> since all instances of a oracle databse run on same disk. Hence there is no concept of replicating messages.";

	/** <code>linger.ms</code> */
	public static final String LINGER_MS_CONFIG = "linger.ms";
	private static final String LINGER_MS_DOC = "The producer groups together any records that arrive in between request transmissions into a single batched request. "
			+ "Normally this occurs only under load when records arrive faster than they can be sent out. However in some circumstances the client may want to "
			+ "reduce the number of requests even under moderate load. This setting accomplishes this by adding a small amount "
			+ "of artificial delay&mdash;that is, rather than immediately sending out a record the producer will wait for up to "
			+ "the given delay to allow other records to be sent so that the sends can be batched together. This can be thought "
			+ "of as analogous to Nagle's algorithm in TCP. This setting gives the upper bound on the delay for batching: once "
			+ "we get <code>" + BATCH_SIZE_CONFIG + "</code> worth of records for a partition it will be sent immediately regardless of this "
			+ "setting, however if we have fewer than this many bytes accumulated for this partition we will 'linger' for the "
			+ "specified time waiting for more records to show up. This setting defaults to 0 (i.e. no delay). Setting <code>" + LINGER_MS_CONFIG + "=5</code>, "
			+ "would have the effect of reducing the number of requests sent but would add up to 5ms of latency to records sent in the absence of load.";

	/** <code>client.id</code> */
	public static final String CLIENT_ID_CONFIG = CommonClientConfigs.CLIENT_ID_CONFIG;

	/** <code>send.buffer.bytes</code> (This property is not yet supported)*/
	public static final String SEND_BUFFER_CONFIG = CommonClientConfigs.SEND_BUFFER_CONFIG;

	/** <code>receive.buffer.bytes</code> (This property is not yet supported)*/
	public static final String RECEIVE_BUFFER_CONFIG = CommonClientConfigs.RECEIVE_BUFFER_CONFIG;

	/** <code>max.request.size</code> */
	public static final String MAX_REQUEST_SIZE_CONFIG = "max.request.size";
	private static final String MAX_REQUEST_SIZE_DOC = "The maximum size of a request in bytes. Each request to TEQ contains a batch of records from same and only one partition."
			+ "For this reason, this property doesn't have any  effect and is effectively equivalent to <code>batch.size</code>.";

	/** <code>reconnect.backoff.ms</code> */
	public static final String RECONNECT_BACKOFF_MS_CONFIG = CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG;

	/** <code>reconnect.backoff.max.ms</code> */
	public static final String RECONNECT_BACKOFF_MAX_MS_CONFIG = CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_CONFIG;

	/** <code>max.block.ms</code> */
	public static final String MAX_BLOCK_MS_CONFIG = "max.block.ms";
	private static final String MAX_BLOCK_MS_DOC = "The configuration controls how long <code>KafkaProducer.send()</code> and <code>KafkaProducer.partitionsFor()</code> will block."
			+ "These methods can be blocked either because the buffer is full or metadata unavailable."
			+ "Blocking in the user-supplied serializers or partitioner will not be counted against this timeout.";

	/** <code>metadata.max.idle.ms</code> */
	public static final String METADATA_MAX_IDLE_CONFIG = "metadata.max.idle.ms";
	private static final String METADATA_MAX_IDLE_DOC =
			"Controls how long the producer will cache metadata for a topic that's idle. If the elapsed " +
					"time since a topic was last produced to exceeds the metadata idle duration, then the topic's " +
					"metadata is forgotten and the next access to it will force a metadata fetch request.";


	/** <code>buffer.memory</code> */
	public static final String BUFFER_MEMORY_CONFIG = "buffer.memory";
	private static final String BUFFER_MEMORY_DOC = "The total bytes of memory the producer can use to buffer records waiting to be sent to the server. If records are "
			+ "sent faster than they can be delivered to the server the producer will block for <code>" + MAX_BLOCK_MS_CONFIG + "</code> after which it will throw an exception."
			+ "<p>"
			+ "This setting should correspond roughly to the total memory the producer will use, but is not a hard bound since "
			+ "not all memory the producer uses is used for buffering.";

	/** <code>retry.backoff.ms</code> */
	public static final String RETRY_BACKOFF_MS_CONFIG = CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG;

	/** <code>compression.type</code> (This property is not yet supported)*/
	public static final String COMPRESSION_TYPE_CONFIG = "compression.type";
	private static final String COMPRESSION_TYPE_DOC = "The compression type for all data generated by the producer. The default is none (i.e. no compression). Valid "
			+ " values are <code>none</code>, <code>gzip</code>, <code>snappy</code>, or <code>lz4</code>. "
			+ "Compression is of full batches of data, so the efficacy of batching will also impact the compression ratio (more batching means better compression). This property is ot yet supported.";

	/** <code>metrics.sample.window.ms</code> (This property is not yet supported)*/
	public static final String METRICS_SAMPLE_WINDOW_MS_CONFIG = CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_CONFIG;

	/** <code>metrics.num.samples</code> (This property is not yet supported)*/
	public static final String METRICS_NUM_SAMPLES_CONFIG = CommonClientConfigs.METRICS_NUM_SAMPLES_CONFIG;

	/**
	 * <code>metrics.log.level</code> (This property is not yet supported)
	 */
	public static final String METRICS_RECORDING_LEVEL_CONFIG = CommonClientConfigs.METRICS_RECORDING_LEVEL_CONFIG;

	/** <code>metric.reporters</code> (This property is not yet supported)*/
	public static final String METRIC_REPORTER_CLASSES_CONFIG = CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG;

	/** <code>max.in.flight.requests.per.connection</code> (This property is not yet supported)*/
	public static final String MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION = "max.in.flight.requests.per.connection";
	private static final String MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION_DOC = "The maximum number of unacknowledged requests the client will send on a single connection before blocking."
			+ "This property is not supportable beacuse client sends a request and waits for response.";

	/** <code>retries</code> */
	public static final String RETRIES_CONFIG = CommonClientConfigs.RETRIES_CONFIG;
	private static final String RETRIES_DOC = "Setting a value greater than zero will cause the client to resend any record whose send fails with a potentially transient error."
			+ " Note that this retry is no different than if the client resent the record upon receiving the error.";


	/** <code>key.serializer</code> */
	public static final String KEY_SERIALIZER_CLASS_CONFIG = "key.serializer";
	public static final String KEY_SERIALIZER_CLASS_DOC = "Serializer class for key that implements the <code>org.oracle.okafka.common.serialization.Serializer</code> interface.";

	/** <code>value.serializer</code> */
	public static final String VALUE_SERIALIZER_CLASS_CONFIG = "value.serializer";
	public static final String VALUE_SERIALIZER_CLASS_DOC = "Serializer class for value that implements the <code>org.oracle.okafka.common.serialization.Serializer</code> interface.";

	/** <code>connections.max.idle.ms</code> (This property is not yet supported)*/
	public static final String CONNECTIONS_MAX_IDLE_MS_CONFIG = CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG;

	/** <code>partitioner.class</code> */
	public static final String PARTITIONER_CLASS_CONFIG = "partitioner.class";
	private static final String PARTITIONER_CLASS_DOC = "Partitioner class that implements the <code>org.oracle.okafka.clients.producer.Partitioner</code> interface.";

	/** <code>request.timeout.ms</code> (This property is not yet supported)*/
	public static final String REQUEST_TIMEOUT_MS_CONFIG = CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG;
	private static final String REQUEST_TIMEOUT_MS_DOC = CommonClientConfigs.REQUEST_TIMEOUT_MS_DOC;


	/** <code>interceptor.classes</code> */
	public static final String INTERCEPTOR_CLASSES_CONFIG = "interceptor.classes";
	public static final String INTERCEPTOR_CLASSES_DOC = "A list of classes to use as interceptors. "
			+ "Implementing the <code>org.oracle.okafka.clients.producer.ProducerInterceptor</code> interface allows you to intercept (and possibly mutate) the records "
			+ "received by the producer before they are published to the Kafka cluster. By default, there are no interceptors.";

	/** <code>enable.idempotence</code> (This property is not yet supported)*/
	public static final String ENABLE_IDEMPOTENCE_CONFIG = "enable.idempotence";
	public static final String ENABLE_IDEMPOTENCE_DOC = "When set to 'true', the producer will ensure that exactly one copy of each message is written in the stream. If 'false', producer "
			+ "retries due to broker failures, etc., may write duplicates of the retried message in the stream. "
			+ "Note that enabling idempotence requires <code>" + MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION + "</code> to be less than or equal to 5, "
			+ "<code>" + RETRIES_CONFIG + "</code> to be greater than 0 and " + ACKS_CONFIG + " must be 'all'. If these values "
			+ "are not explicitly set by the user, suitable values will be chosen. If incompatible values are set, "
			+ "a ConfigException will be thrown. This property is not yet supported.";

	/** <code> transaction.timeout.ms (This property is not yet supported)</code> */
	public static final String TRANSACTION_TIMEOUT_CONFIG = "transaction.timeout.ms";
	public static final String TRANSACTION_TIMEOUT_DOC = "The maximum amount of time in ms that the transaction coordinator will wait for a transaction status update from the producer before proactively aborting the ongoing transaction." +
			"If this value is larger than the transaction.max.timeout.ms setting in the broker, the request will fail with a `InvalidTransactionTimeout` error. This property is not yet supported.";

	/** <code> transactional.id (This property is not yet supported)</code> */
	public static final String TRANSACTIONAL_ID_CONFIG = "transactional.id";
	public static final String TRANSACTIONAL_ID_DOC = "The TransactionalId to use for transactional delivery. This enables reliability semantics which span multiple producer sessions since it allows the client to guarantee that transactions using the same TransactionalId have been completed prior to starting any new transactions. If no TransactionalId is provided, then the producer is limited to idempotent delivery. " +
			"Note that enable.idempotence must be enabled if a TransactionalId is configured. " +
			"The default is <code>null</code>, which means transactions cannot be used. " +
			"Note that transactions requires a cluster of at least three brokers by default what is the recommended setting for production; for development you can change this, by adjusting broker setting `transaction.state.log.replication.factor`. This property is not yet supported.";

	/** <code>delivery.timeout.ms</code> */
	public static final String DELIVERY_TIMEOUT_MS_CONFIG = "delivery.timeout.ms";
	private static final String DELIVERY_TIMEOUT_MS_DOC = "An upper bound on the time to report success or failure "
			+ "after a call to <code>send()</code> returns. This limits the total time that a record will be delayed "
			+ "prior to sending, the time to await acknowledgement from the broker (if expected), and the time allowed "
			+ "for retriable send failures. The producer may report failure to send a record earlier than this config if "
			+ "either an unrecoverable error is encountered, the retries have been exhausted, "
			+ "or the record is added to a batch which reached an earlier delivery expiration deadline. "
			+ "The value of this config should be greater than or equal to the sum of <code>" + REQUEST_TIMEOUT_MS_CONFIG + "</code> "
			+ "and <code>" + LINGER_MS_CONFIG + "</code>.";

	/**
	 * <code>internal.auto.downgrade.txn.commit</code>
	 * Whether or not the producer should automatically downgrade the transactional commit request when the new group metadata
	 * feature is not supported by the broker.
	 * <p>
	 * The purpose of this flag is to make Kafka Streams being capable of working with old brokers when applying this new API.
	 * Non Kafka Streams users who are building their own EOS applications should be careful playing around
	 * with config as there is a risk of violating EOS semantics when turning on this flag.
	 *
	 * <p>
	 * Note: this is an internal configuration and could be changed in the future in a backward incompatible way
	 *
	 */
	static final String AUTO_DOWNGRADE_TXN_COMMIT = "internal.auto.downgrade.txn.commit";


	static {
		CONFIG = new ConfigDef().define(BOOTSTRAP_SERVERS_CONFIG, Type.LIST, Collections.emptyList(), new ConfigDef.NonNullValidator(), Importance.HIGH, CommonClientConfigs.BOOTSTRAP_SERVERS_DOC)
				.define(CLIENT_DNS_LOOKUP_CONFIG,
						Type.STRING,
						ClientDnsLookup.USE_ALL_DNS_IPS.toString(),
						in(ClientDnsLookup.USE_ALL_DNS_IPS.toString(),
								ClientDnsLookup.USE_ALL_DNS_IPS.toString(),
								ClientDnsLookup.RESOLVE_CANONICAL_BOOTSTRAP_SERVERS_ONLY.toString()),
						Importance.MEDIUM,
						CommonClientConfigs.CLIENT_DNS_LOOKUP_DOC)
				.define(BUFFER_MEMORY_CONFIG, Type.LONG, 32 * 1024 * 1024L, atLeast(0L), Importance.HIGH, BUFFER_MEMORY_DOC)
				.define(RETRIES_CONFIG, Type.INT, 0, between(0, Integer.MAX_VALUE), Importance.HIGH, RETRIES_DOC)
				.define(ACKS_CONFIG,
						Type.STRING,
						"1",
						in("all", "-1", "0", "1"),
						Importance.HIGH,
						ACKS_DOC)
				.define(COMPRESSION_TYPE_CONFIG, Type.STRING, "none", Importance.HIGH, COMPRESSION_TYPE_DOC)
				.define(BATCH_SIZE_CONFIG, Type.INT, 16384, atLeast(0), Importance.MEDIUM, BATCH_SIZE_DOC)
				.define(LINGER_MS_CONFIG, Type.LONG, 0, atLeast(0L), Importance.MEDIUM, LINGER_MS_DOC)
				.define(CLIENT_ID_CONFIG, Type.STRING, "", Importance.MEDIUM, CommonClientConfigs.CLIENT_ID_DOC)
				.define(SEND_BUFFER_CONFIG, Type.INT, 128 * 1024, atLeast(-1), Importance.MEDIUM, CommonClientConfigs.SEND_BUFFER_DOC)
				.define(RECEIVE_BUFFER_CONFIG, Type.INT, 32 * 1024, atLeast(-1), Importance.MEDIUM, CommonClientConfigs.RECEIVE_BUFFER_DOC)
				.define(MAX_REQUEST_SIZE_CONFIG,
						Type.INT,
						1 * 1024 * 1024,
						atLeast(0),
						Importance.MEDIUM,
						MAX_REQUEST_SIZE_DOC)
				.define(RECONNECT_BACKOFF_MS_CONFIG, Type.LONG, 50L, atLeast(0L), Importance.LOW, CommonClientConfigs.RECONNECT_BACKOFF_MS_DOC)
				.define(RECONNECT_BACKOFF_MAX_MS_CONFIG, Type.LONG, 1000L, atLeast(0L), Importance.LOW, CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_DOC)
				.define(RETRY_BACKOFF_MS_CONFIG, Type.LONG, 100L, atLeast(0L), Importance.LOW, CommonClientConfigs.RETRY_BACKOFF_MS_DOC)
				.define(MAX_BLOCK_MS_CONFIG,
						Type.LONG,
						60 * 1000,
						atLeast(0),
						Importance.MEDIUM,
						MAX_BLOCK_MS_DOC)
				.define(REQUEST_TIMEOUT_MS_CONFIG,
						Type.INT,
						30 * 1000,
						atLeast(0),
						Importance.MEDIUM,
						REQUEST_TIMEOUT_MS_DOC)
				.define(METADATA_MAX_AGE_CONFIG, Type.LONG, 5 * 60 * 1000, atLeast(0), Importance.LOW, METADATA_MAX_AGE_DOC)
				.define(METRICS_SAMPLE_WINDOW_MS_CONFIG,
						Type.LONG,
						30000,
						atLeast(0),
						Importance.LOW,
						CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_DOC)
				.define(METADATA_MAX_IDLE_CONFIG,
						Type.LONG,
						5 * 60 * 1000,
						atLeast(5000),
						Importance.LOW,
						METADATA_MAX_IDLE_DOC)
				.define(METRICS_NUM_SAMPLES_CONFIG, Type.INT, 2, atLeast(1), Importance.LOW, CommonClientConfigs.METRICS_NUM_SAMPLES_DOC)
				.define(METRICS_RECORDING_LEVEL_CONFIG,
						Type.STRING,
						Sensor.RecordingLevel.INFO.toString(),
						in(Sensor.RecordingLevel.INFO.toString(), Sensor.RecordingLevel.DEBUG.toString()),
						Importance.LOW,
						CommonClientConfigs.METRICS_RECORDING_LEVEL_DOC)
				.define(METRIC_REPORTER_CLASSES_CONFIG,
						Type.LIST,
						Collections.emptyList(),
						new ConfigDef.NonNullValidator(),
						Importance.LOW,
						CommonClientConfigs.METRIC_REPORTER_CLASSES_DOC)
				.define(MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
						Type.INT,
						5,
						atLeast(1),
						Importance.LOW,
						MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION_DOC)
				.define(KEY_SERIALIZER_CLASS_CONFIG,
						Type.CLASS,
						Importance.HIGH,
						KEY_SERIALIZER_CLASS_DOC)
				.define(VALUE_SERIALIZER_CLASS_CONFIG,
						Type.CLASS,
						Importance.HIGH,
						VALUE_SERIALIZER_CLASS_DOC)
				.define(SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG,
						Type.LONG,
						CommonClientConfigs.DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MS,
						Importance.MEDIUM,
						CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_DOC)
				.define(SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG,
						Type.LONG,
						CommonClientConfigs.DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS,
						Importance.MEDIUM,
						CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_DOC)
				/* default is set to be a bit lower than the server default (10 min), to avoid both client and server closing connection at same time */
				.define(CONNECTIONS_MAX_IDLE_MS_CONFIG,
						Type.LONG,
						9 * 60 * 1000,
						Importance.MEDIUM,
						CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_DOC)
				.define(DELIVERY_TIMEOUT_MS_CONFIG, Type.INT, 120 * 1000, atLeast(0), Importance.MEDIUM, DELIVERY_TIMEOUT_MS_DOC)
				.define(INTERCEPTOR_CLASSES_CONFIG,
						Type.LIST,
						Collections.emptyList(),
						new ConfigDef.NonNullValidator(),
						Importance.LOW,
						INTERCEPTOR_CLASSES_DOC)
				.define(PARTITIONER_CLASS_CONFIG,
						Type.CLASS,
						org.apache.kafka.clients.producer.internals.DefaultPartitioner.class,
						Importance.MEDIUM, PARTITIONER_CLASS_DOC)
				.define(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
						Type.STRING,
						CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL,
						Importance.MEDIUM,
						CommonClientConfigs.SECURITY_PROTOCOL_DOC)
				.withClientSslSupport()
				//.withClientSaslSupport()
				.define(SslConfigs.TNS_ALIAS, ConfigDef.Type.STRING, null, Importance.MEDIUM, SslConfigs.TNS_ALIAS_DOC)
				.define(ENABLE_IDEMPOTENCE_CONFIG,
						Type.BOOLEAN,
						false,
						Importance.LOW,
						ENABLE_IDEMPOTENCE_DOC)
				.define(TRANSACTION_TIMEOUT_CONFIG,
						Type.INT,
						60000,
						Importance.LOW,
						TRANSACTION_TIMEOUT_DOC)
				.define(TRANSACTIONAL_ID_CONFIG,
						Type.STRING,
						null,
						new ConfigDef.NonEmptyString(),
						Importance.LOW,
						TRANSACTIONAL_ID_DOC)
				.define(ORACLE_SERVICE_NAME,
						Type.STRING,
						null,
						Importance.HIGH,
						CommonClientConfigs.ORACLE_SERVICE_NAME_DOC)
				.define(ORACLE_INSTANCE_NAME,
						Type.STRING,
						null,
						Importance.HIGH,
						CommonClientConfigs.ORACLE_INSTANCE_NAME_DOC)
				.define(CommonClientConfigs.ORACLE_NET_TNS_ADMIN, 
						ConfigDef.Type.STRING, 
						Importance.MEDIUM, 
						CommonClientConfigs.ORACLE_NET_TNS_ADMIN_DOC)
				.define(CommonClientConfigs.ORACLE_TRANSACTIONAL_PRODUCER, 
						ConfigDef.Type.BOOLEAN,
						false,
						Importance.LOW, 
						CommonClientConfigs.ORACLE_SERVICE_NAME_DOC);
	}


	public static Map<String, Object> addSerializerToConfig(Map<String, Object> configs,
			Serializer<?> keySerializer, Serializer<?> valueSerializer) {
		Map<String, Object> newConfigs = new HashMap<>(configs);
		if (keySerializer != null)
			newConfigs.put(KEY_SERIALIZER_CLASS_CONFIG, keySerializer.getClass());
		if (valueSerializer != null)
			newConfigs.put(VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer.getClass());
		return newConfigs;
	}

	public static Properties addSerializerToConfig(Properties properties,
			Serializer<?> keySerializer,
			Serializer<?> valueSerializer) {
		Properties newProperties = new Properties();
		newProperties.putAll(properties);
		if (keySerializer != null)
			newProperties.put(KEY_SERIALIZER_CLASS_CONFIG, keySerializer.getClass().getName());
		if (valueSerializer != null)
			newProperties.put(VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer.getClass().getName());
		return newProperties;
	}

	public ProducerConfig(Properties props) {
		super(CONFIG, props);
	}

	public ProducerConfig(Map<String, Object> props) {
		super(CONFIG, props);
	}

	ProducerConfig(Map<?, ?> props, boolean doLog) {
		super(CONFIG, props, doLog);
	}

	public static Set<String> configNames() {
		return CONFIG.names();
	}

	public static void main(String[] args) {
		System.out.println(CONFIG.toHtmlTable());
	}

	static Map<String, Object> appendSerializerToConfig(Map<String, Object> configs,
			Serializer<?> keySerializer,
			Serializer<?> valueSerializer) {
		Map<String, Object> newConfigs = new HashMap<>(configs);
		if (keySerializer != null)
			newConfigs.put(KEY_SERIALIZER_CLASS_CONFIG, keySerializer.getClass());
		if (valueSerializer != null)
			newConfigs.put(VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer.getClass());
		return newConfigs;
	}


	boolean idempotenceEnabled() {
		boolean userConfiguredIdempotence = this.originals().containsKey(ENABLE_IDEMPOTENCE_CONFIG);
		boolean userConfiguredTransactions = this.originals().containsKey(TRANSACTIONAL_ID_CONFIG);
		boolean idempotenceEnabled = userConfiguredIdempotence && this.getBoolean(ENABLE_IDEMPOTENCE_CONFIG);

		if (!idempotenceEnabled && userConfiguredIdempotence && userConfiguredTransactions)
			throw new ConfigException("Cannot set a " + ProducerConfig.TRANSACTIONAL_ID_CONFIG + " without also enabling idempotence.");
		return userConfiguredTransactions || idempotenceEnabled;
	}
}
