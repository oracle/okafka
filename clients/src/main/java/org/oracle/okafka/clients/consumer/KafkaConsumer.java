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

package org.oracle.okafka.clients.consumer;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import javax.jms.JMSException;

import oracle.jms.AQjmsBytesMessage;
import oracle.jms.AQjmsDestination;

//import org.oracle.okafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.ClientDnsLookup;
import org.apache.kafka.clients.ClientUtils;
//import org.oracle.okafka.clients.consumer.internals.PartitionAssignor;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.oracle.okafka.clients.CommonClientConfigs;
import org.oracle.okafka.clients.Metadata;
import org.oracle.okafka.clients.NetworkClient;
import org.oracle.okafka.clients.TopicTeqParameters;
import org.apache.kafka.clients.consumer.internals.ConsumerInterceptors;
import org.oracle.okafka.clients.consumer.internals.ConsumerNetworkClient;
import org.oracle.okafka.clients.consumer.internals.FetchMetricsRegistry;
import org.oracle.okafka.clients.consumer.internals.OkafkaConsumerMetrics;
import org.oracle.okafka.clients.consumer.internals.NoOpConsumerRebalanceListener;

import org.oracle.okafka.clients.consumer.internals.SubscriptionState;
import org.oracle.okafka.clients.consumer.internals.SubscriptionState.FetchPosition;
import org.oracle.okafka.clients.consumer.internals.TopicMetadataFetcher;
import org.apache.kafka.clients.Metadata.LeaderAndEpoch;
import org.oracle.okafka.clients.consumer.internals.AQKafkaConsumer;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.oracle.okafka.common.config.SslConfigs;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.DisconnectException;
import org.oracle.okafka.common.errors.ConnectionException;
import org.oracle.okafka.common.errors.FeatureNotSupportedException;
import org.oracle.okafka.common.errors.InvalidLoginCredentialsException;
import org.oracle.okafka.common.network.AQClient;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Meter;
import org.apache.kafka.common.metrics.stats.WindowedCount;
import org.apache.kafka.common.record.TimestampType;
import org.oracle.okafka.common.requests.IsolationLevel;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.common.utils.LogContext;
import org.oracle.okafka.common.utils.ConnectionUtils;
import org.oracle.okafka.common.utils.MessageIdConverter;
import org.oracle.okafka.common.utils.MessageIdConverter.OKafkaOffset;
import org.oracle.okafka.common.utils.TNSParser;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

/**
 * A Java client that consumes records from a Transactional event
 * queues(TxEventQ).
 * 
 * The consumer maintains a single JDBC Connection to any one of the available
 * instances of Oracle database to fetch consumer records. Failure to close the
 * consumer after use will leak this connection. The client transparently
 * handles the failure of Oracle database instances, and transparently adapts as
 * topic partitions it fetches migrate within the Oracle database cluster. THis
 * client also allow groups of consumer to load balance consumption using
 * <a href="#consumergroups">consumer groups</a>.
 * 
 * <h3><a name="consumergroups">Consumer Groups and Topic Subscriptions</a></h3>
 *
 * TxEventQ supports Apache Kafka's concept of <i>consumer groups</i> which
 * allow a pool of processes to divide the work of consuming and processing
 * records and thus facilitating load balancing. These processes can either be
 * running on the same machine or they can be distributed over many machines to
 * provide scalability and fault tolerance for processing. All consumer
 * instances sharing the same {@code group.id} will be part of the same consumer
 * group.
 * 
 * A Consumer can subscribe to single topic using {@link #subscribe(Collection)
 * subscribe} or {@link #subscribe(Collection, ConsumerRebalanceListener)}.
 * Consumer throws FeatureNotSupportedException exception if topic subscription
 * collection size is greater than one. Also consumers can't subscribe using
 * {@link #subscribe(Pattern) subscribe(Pattern)},
 * {@link #subscribe(Pattern, ConsumerRebalanceListener) subscribe(Pattern,
 * ConsumerRebalanceListener)}, as pattern based subscription is not supported
 * for this release. Note: Topic Name must be passed in upper case.
 * 
 * Membership in a consumer group is maintained dynamically: if a process fails,
 * the partitions assigned to it will be reassigned to other consumers in the
 * same group. Similarly, if a new consumer joins the group, partitions will be
 * moved from existing consumers to the new one. This is known as
 * <i>rebalancing</i> the group.
 * 
 * In addition, when group reassignment happens automatically, consumers can be
 * notified through a {@link ConsumerRebalanceListener}, which allows them to
 * finish necessary application-level logic such as state cleanup, manual offset
 * commits, etc.
 * 
 * For this release of OKafka, manually assignment of partition is not
 * supported. Application will get <i>FeatureNotSupportedException</i> if
 * {@link #assign(Collection)} method is invoked.
 * 
 * <h3><a name="failuredetection">Detecting Consumer Failures</a></h3>
 * 
 * After subscribing to a set of topics, the consumer will automatically join
 * the group when {@link #poll(Duration)} is invoked. The poll API is designed
 * to ensure consumer liveness. As long as you continue to call poll, the
 * consumer will stay in the group and continue to receive messages from the
 * partitions it was assigned. Underneath the covers, OKafka consumer maintains
 * a JDBC connection to the Oracle database. If consumer crashes, its connection
 * to the Oracle Database gets severed and then the consumer will be considered
 * dead and its partitions will be reassigned.
 * 
 * OKafka consumer does not send heartbeat to the Oracle database. Also for this
 * release of Okafka,
 * <li><code>max.poll.interval.ms</code> is also not supported.
 * 
 * <h3>Offsets and Consumer Position</h3> TxEventQ maintains an offset for each
 * record of a partition in an internal format.This is equivalent to Apache
 * Kafka's Consumer Record offset. This offset or acts as a unique identifier of
 * a record within that partition, and also denotes the position of the consumer
 * in the partition. The position of consumer depends on {@link #commitSync()
 * committed position}. This is the last offset that has been stored securely.
 * Should the process starts or fail and restart , this is the offset that the
 * consumer will recover to. The consumer can either automatically commit
 * offsets periodically; or it can choose to control this committed position
 * manually by calling one of the commit APIs (e.g. {@link #commitSync()
 * commitSync} and {@link #commitAsync(OffsetCommitCallback) commitAsync}).
 * Below examples show how to use periodic or manual offset commit.
 * 
 * <h4>Automatic Offset Committing</h4> This example demonstrates a simple usage
 * of oKafka's consumer api that relies on automatic offset committing.
 * <p>
 * 
 * <pre>
 * {@code
 *     Properties props = new Properties();
 *     props.put("bootstrap.servers", "localhost:1521");
 *     props.put("oracle.service.name", "freepdb1");
 *     props.put("oracle.net.tns_admin","."); 
 *     props.put("group.id", "CG1");
 *     props.put("enable.auto.commit", "true");
 *     props.put("auto.commit.interval.ms", "10000");
 *     props.put("key.deserializer",  "org.apache.kafka.common.serialization.StringDeserializer");	      
 *     props.put("value.deserializer",    "org.apache.kafka.common.serialization.StringDeserializer");
 *     props.put("max.poll.records", 100);
 *     KafkaConsumer<String, String> consumer = null;
 *	   consumer = new KafkaConsumer<String, String>(props);
 *     consumer.subscribe(Arrays.asList("TXEQ"));
 *     ConsumerRecords<String, String> records = null; 
 *     try {
 *       records = consumer.poll(Duration.ofMillis(1000));
 *       for (ConsumerRecord<String, String> record : records) {
 *         System.out.println("topic = , partition=  ,key= , value = \n"+ 		 	  	             
 *           record.topic()+ "  "+record.partition()+ "  "+record.key()+"  "+ record.value());                  
 *       }
 *       consumer.commitSync();		 	  	    	 
 *     }catch(Exception ex) {
 *        ex.printStackTrace(); 
 *     } finally {
 *        consumer.close();
 *     }
 *}
 * </pre>
 *
 * The connection to the Oracle Database cluster is bootstrapped by specifying a
 * one Oracle Cluster node to contact using the configuration
 * {@code bootstrap.servers}.
 * <p>
 * Setting {@code enable.auto.commit} means that offsets are committed
 * automatically with a frequency controlled by the config
 * {@code auto.commit.interval.ms}.
 * <p>
 * In this example the consumer is subscribing to the topic <i>TXEQ</i> as part
 * of a group of consumers called <i>CG1</i> as configured with
 * {@code group.id}.
 * <p>
 * The deserializer settings specify how to turn bytes into objects. For
 * example, by specifying string deserializers, we are saying that our record's
 * key and value will just be simple strings.
 * <p>
 * <h4>Manual Offset Control</h4>
 * <p>
 * Instead of relying on the consumer to periodically commit consumed offsets,
 * users can also control when records should be considered as consumed and
 * hence commit their offsets. This is useful when the consumption of the
 * messages is coupled with some processing logic and hence a message should not
 * be considered as consumed until it is completed processing.
 * <p>
 * 
 * <pre>
 * {@code
 * Properties props = new Properties();
 * props.put("bootstrap.servers", "localhost:1521");
 * props.put("oracle.service.name", "freepdb1");
 * props.put("oracle.net.tns_admin", ".");
 * props.put("group.id", "CG1");
 * props.put("enable.auto.commit", "true");
 * props.put("auto.commit.interval.ms", "10000");
 * props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
 * props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
 * props.put("max.poll.records", 100);
 * KafkaConsumer<String, String> consumer = null;
 * consumer = new KafkaConsumer<String, String>(props);
 * consumer.subscribe(Arrays.asList("TXEQ"));
 * ConsumerRecords<String, String> records = null;
 * try {
 * 	final int minBatchSize = 200;
 * 	List<ConsumerRecord<String, String>> buffer = new ArrayList<>();
 * 	while (true) {
 * 		ConsumerRecords<String, String> records = consumer.poll(100);
 * 		for (ConsumerRecord<String, String> record : records) {
 * 			buffer.add(record);
 * 		}
 * 		if (buffer.size() <= minBatchSize) {
 * 			insertIntoDb(buffer);
 * 			consumer.commitSync();
 * 			buffer.clear();
 * 		}
 * 	}
 * } catch (Exception ex) {
 * 	ex.printStackTrace();
 * } finally {
 * 	consumer.close();
 * }
 * }
 * </pre>
 *
 * In this example we will consume a batch of records and batch them up in
 * memory. When we have enough records batched, we will insert them into a
 * database. If our process fails before commitSync() then all consumed messages
 * after previous commit are rolled back and considered not consumed. If process
 * restarted it starts consuming from next of previous committed offset. In this
 * way, OKafka provides "at-least-once" delivery guarantees, as each record will
 * likely be delivered one time but in failure case could be duplicated. With
 * OKafka, "exactly-once" delivery guarantees is possible with the use of
 * {@link #getDBConnection()} method. Using {@link #getDBConnection()},
 * application can retrieve the Oracle database connection which was used to
 * consume the records by the OKafka consumer. Application can use this database
 * connection to store the processed records in database. After that when
 * {@link #commitSync()} is invoked, the consumption and storage of record into
 * database is committed atomically. Below example depicts that.
 * 
 * *
 * 
 * <pre>
 * {@code
 * Properties props = new Properties();
 * props.put("bootstrap.servers", "localhost:1521");
 * props.put("oracle.service.name", "freepdb1");
 * props.put("oracle.net.tns_admin", ".");
 * props.put("group.id", "CG1");
 * props.put("enable.auto.commit", "true");
 * props.put("auto.commit.interval.ms", "10000");
 * props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
 * props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
 * props.put("max.poll.records", 100);
 * KafkaConsumer<String, String> consumer = null;
 * consumer = new KafkaConsumer<String, String>(props);
 * consumer.subscribe(Arrays.asList("TXEQ"));
 * ConsumerRecords<String, String> records = null;
 * try {
 * 	List<ConsumerRecord<String, String>> buffer = new ArrayList<>();
 * 	while (true) {
 * 		ConsumerRecords<String, String> records = consumer.poll(100);
 * 		Connection conn = ((KafkaConsumer<String, String>) consumer).getDBConnection();
 * 		for (ConsumerRecord<String, String> record : records) {
 * 			buffer.add(record);
 * 		}
 * 		insertIntoDb(buffer, conn);
 * 		consumer.commitSync();
 * 		buffer.clear();
 * 	}
 * } catch (Exception ex) {
 * 	ex.printStackTrace();
 * } finally {
 * 	consumer.close();
 * }
 * } 
 * </pre>
 * 
 * For this release of OKafka, {@link #commitSync(Map) commitSync(offsets)}
 * methods to manually commit the offset is not supported.
 * 
 * <p>
 *
 * <h3><a name="multithreaded">Multi-threaded Processing</a></h3>
 *
 * <p>
 * The okafka consumer is NOT thread-safe. All network I/O happens in the thread
 * of the application making the call. It is the responsibility of the user to
 * ensure that multi-threaded access is properly synchronized. Un-synchronized
 * access will result in {@link ConcurrentModificationException}. For this
 * release of OKafka, {@link #wakeup()} is not supported. Invoking the api would
 * not throw FeatureNotSupportedException.
 * <p>
 */

public class KafkaConsumer<K, V> implements Consumer<K, V> {

	private static final long NO_CURRENT_THREAD = -1L;
	private static final String CLIENT_ID_METRIC_TAG = "client-id";
	private static final AtomicInteger CONSUMER_CLIENT_ID_SEQUENCE = new AtomicInteger(1);
	private static final String JMX_PREFIX = "kafka.consumer";
	static final long DEFAULT_CLOSE_TIMEOUT_MS = 30 * 1000;

	final Metrics metrics;
	final OkafkaConsumerMetrics okcMetrics;
	private final FetchManagerMetrics fetchManagerMetrics;

	private final Logger log;
	private final String clientId;
	private final Deserializer<K> keyDeserializer;
	private final Deserializer<V> valueDeserializer;
	private final ConsumerInterceptors<K, V> interceptors;
    private final TopicMetadataFetcher topicMetadataFetcher;


	private final Time time;
	private final SubscriptionState subscriptions;
	private final Metadata metadata;
	private final ConsumerNetworkClient client;
	private final long retryBackoffMs;
	private final int requestTimeoutMs;
	private final int defaultApiTimeoutMs;
	private volatile boolean closed = false;
	// private List<PartitionAssignor> assignors;
	private List<ConsumerPartitionAssignor> assignors;

	// currentThread holds the threadId of the current thread accessing
	// KafkaConsumer
	// and is used to prevent multi-threaded access
	private final AtomicLong currentThread = new AtomicLong(NO_CURRENT_THREAD);
	// refcount is used to allow reentrant access by the thread who has acquired
	// currentThread
	private final AtomicInteger refcount = new AtomicInteger(0);
	private final int DLENGTH_SIZE = 4;
	private AQKafkaConsumer aqConsumer = null;

	private ConsumerGroupMetadata cgMetadata = null;

	/**
	 * A consumer is instantiated by providing a set of key-value pairs as
	 * configuration. Values can be either strings or objects of the appropriate
	 * type (for example a numeric configuration would accept either the string "42"
	 * or the integer 42).
	 * <p>
	 * Valid configuration strings are documented at {@link ConsumerConfig}.
	 * <p>
	 * Note: after creating a {@code KafkaConsumer} you must always {@link #close()}
	 * it to avoid resource leaks.
	 *
	 * @param configs The consumer configs
	 */
	public KafkaConsumer(Map<String, Object> configs) {
		this(configs, null, null);
	}

	/**
	 * A consumer is instantiated by providing a set of key-value pairs as
	 * configuration, and a key and a value {@link Deserializer}.
	 * <p>
	 * Valid configuration strings are documented at {@link ConsumerConfig}.
	 * <p>
	 * Note: after creating a {@code KafkaConsumer} you must always {@link #close()}
	 * it to avoid resource leaks.
	 *
	 * @param configs           The consumer configs
	 * @param keyDeserializer   The deserializer for key that implements
	 *                          {@link Deserializer}. The configure() method won't
	 *                          be called in the consumer when the deserializer is
	 *                          passed in directly.
	 * @param valueDeserializer The deserializer for value that implements
	 *                          {@link Deserializer}. The configure() method won't
	 *                          be called in the consumer when the deserializer is
	 *                          passed in directly.
	 */
	public KafkaConsumer(Map<String, Object> configs, Deserializer<K> keyDeserializer,
			Deserializer<V> valueDeserializer) {
		this(new ConsumerConfig(ConsumerConfig.addDeserializerToConfig(configs, keyDeserializer, valueDeserializer)),
				keyDeserializer, valueDeserializer);
	}

	/**
	 * A consumer is instantiated by providing a {@link java.util.Properties} object
	 * as configuration.
	 * <p>
	 * Valid configuration strings are documented at {@link ConsumerConfig}.
	 * <p>
	 * Note: after creating a {@code KafkaConsumer} you must always {@link #close()}
	 * it to avoid resource leaks.
	 *
	 * @param properties The consumer configuration properties
	 */
	public KafkaConsumer(Properties properties) {
		this(properties, null, null);
	}

	/**
	 * A consumer is instantiated by providing a {@link java.util.Properties} object
	 * as configuration, and a key and a value {@link Deserializer}.
	 * <p>
	 * Valid configuration strings are documented at {@link ConsumerConfig}.
	 * <p>
	 * Note: after creating a {@code KafkaConsumer} you must always {@link #close()}
	 * it to avoid resource leaks.
	 *
	 * @param properties        The consumer configuration properties
	 * @param keyDeserializer   The deserializer for key that implements
	 *                          {@link Deserializer}. The configure() method won't
	 *                          be called in the consumer when the deserializer is
	 *                          passed in directly.
	 * @param valueDeserializer The deserializer for value that implements
	 *                          {@link Deserializer}. The configure() method won't
	 *                          be called in the consumer when the deserializer is
	 *                          passed in directly.
	 */
	public KafkaConsumer(Properties properties, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
		this(new ConsumerConfig(ConsumerConfig.addDeserializerToConfig(properties, keyDeserializer, valueDeserializer)),
				keyDeserializer, valueDeserializer);
	}

	@SuppressWarnings("unchecked")
	private KafkaConsumer(ConsumerConfig config, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
		try {

			// System.setProperty("oracle.jms.conservativeNavigation","1");

			String clientId = config.getString(ConsumerConfig.CLIENT_ID_CONFIG);
			if (clientId.isEmpty())
				clientId = "consumer-" + CONSUMER_CLIENT_ID_SEQUENCE.getAndIncrement();
			this.clientId = clientId;

			String groupId = config.getString(ConsumerConfig.GROUP_ID_CONFIG);

			LogContext logContext = new LogContext("[Consumer clientId=" + clientId + ", groupId=" + groupId + "] ");
			this.log = logContext.logger(getClass());

			log.debug("Initializing Kafka Consumer");

			this.requestTimeoutMs = config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG);
			this.defaultApiTimeoutMs = config.getInt(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG);
			this.time = Time.SYSTEM;

			Map<String, String> metricsTags = Collections.singletonMap("client-id", clientId);

			MetricConfig metricConfig = new MetricConfig()
					.samples(config.getInt(ConsumerConfig.METRICS_NUM_SAMPLES_CONFIG))
					.timeWindow(config.getLong(ConsumerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG), TimeUnit.MILLISECONDS)
					.recordLevel(Sensor.RecordingLevel
							.forName(config.getString(ConsumerConfig.METRICS_RECORDING_LEVEL_CONFIG)))
					.tags(metricsTags);

			List<MetricsReporter> reporters = config
					.getConfiguredInstances(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG, MetricsReporter.class);

			reporters.add(new JmxReporter(JMX_PREFIX));

			this.metrics = new Metrics(metricConfig, reporters, time);
			this.retryBackoffMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG);

			// load interceptors and make sure they get clientId
			Map<String, Object> userProvidedConfigs = config.originals();
			userProvidedConfigs.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
			List<ConsumerInterceptor<K, V>> interceptorList = (List) (new ConsumerConfig(userProvidedConfigs, false))
					.getConfiguredInstances(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, ConsumerInterceptor.class);
			this.interceptors = new ConsumerInterceptors<>(interceptorList);

			if (keyDeserializer == null) {
				this.keyDeserializer = config.getConfiguredInstance(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
						Deserializer.class);
				this.keyDeserializer.configure(config.originals(), true);
			} else {
				config.ignore(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);
				this.keyDeserializer = keyDeserializer;
			}

			if (valueDeserializer == null) {
				this.valueDeserializer = config.getConfiguredInstance(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
						Deserializer.class);
				this.valueDeserializer.configure(config.originals(), false);
			} else {
				config.ignore(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);
				this.valueDeserializer = valueDeserializer;
			}

			ClusterResourceListeners clusterResourceListeners = configureClusterResourceListeners(keyDeserializer,
					valueDeserializer, reporters, interceptorList);
			this.metadata = new Metadata(retryBackoffMs, config.getLong(ConsumerConfig.METADATA_MAX_AGE_CONFIG), true,
					false, clusterResourceListeners, config);

			List<InetSocketAddress> addresses = null;
			String serviceName = config.getString(ConsumerConfig.ORACLE_SERVICE_NAME);
			String instanceName = null;

			if (config.getString(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG).equalsIgnoreCase("PLAINTEXT")) {
				// Changes for 2.8.1
				// addresses =
				// ClientUtils.parseAndValidateAddresses(config.getList(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
				addresses = ClientUtils.parseAndValidateAddresses(
						config.getList(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG),
						ClientDnsLookup.RESOLVE_CANONICAL_BOOTSTRAP_SERVERS_ONLY);
				serviceName = config.getString(ConsumerConfig.ORACLE_SERVICE_NAME);
				instanceName = config.getString(ConsumerConfig.ORACLE_INSTANCE_NAME);
			} else {
				if (config.getString(SslConfigs.TNS_ALIAS) == null)
					throw new InvalidLoginCredentialsException("Please provide valid connection string");
				TNSParser parser = new TNSParser(config);
				parser.readFile();
				String connStr = parser.getConnectionString(config.getString(SslConfigs.TNS_ALIAS).toUpperCase());
				if (connStr == null)
					throw new InvalidLoginCredentialsException("Please provide valid connection string");
				String host = parser.getProperty(connStr, "HOST");
				String portStr = parser.getProperty(connStr, "PORT");
				serviceName = parser.getProperty(connStr, "SERVICE_NAME");
				int port;
				if (host == null || portStr == null || serviceName == null)
					throw new InvalidLoginCredentialsException("Please provide valid connection string");
				try {
					port = Integer.parseInt(portStr);
				} catch (NumberFormatException nfe) {
					throw new InvalidLoginCredentialsException("Please provide valid connection string");
				}
				instanceName = parser.getProperty(connStr, "INSTANCE_NAME");
				addresses = new ArrayList<>();
				addresses.add(new InetSocketAddress(host, port));
			}
			// this.metadata.update(Cluster.bootstrap(addresses, config, serviceName,
			// instanceName), Collections.<String>emptySet(), time.milliseconds());

			{ // Changes for 2.8.1 :: Create Bootstrap Cluster and pass it to metadata.update
				// We must have OKafka Node with Service Name and Instance Name placed in the
				// bootstrap cluster.
				// For cluster created here, metadata.update has isBootstrapConfigured passed as
				// TRUE because the field is not public

				ArrayList<Node> bootStrapNodeList = new ArrayList<Node>(addresses.size());
				int id = -1;
				ConnectionUtils.remDuplicateEntries(addresses);
				for (InetSocketAddress inetAddr : addresses) {
					org.oracle.okafka.common.Node bootStrapNode = new org.oracle.okafka.common.Node(id--,
							inetAddr.getHostName(), inetAddr.getPort(), serviceName, instanceName);
					bootStrapNode.setBootstrapFlag(true);
					bootStrapNodeList.add((Node) bootStrapNode);
				}
				Cluster bootStrapCluster = new Cluster(null, bootStrapNodeList, new ArrayList<>(0),
						Collections.emptySet(), Collections.emptySet());
				log.debug("Setting up bootstrap cluster");
				this.metadata.update(bootStrapCluster, Collections.<String>emptySet(), time.milliseconds(), true);
			}

			String metricGrpPrefix = "consumer";
			FetchMetricsRegistry metricsRegistry = new FetchMetricsRegistry(Collections.singleton(CLIENT_ID_METRIC_TAG),
					metricGrpPrefix);
			this.fetchManagerMetrics = new FetchManagerMetrics(metrics, metricsRegistry);

			IsolationLevel isolationLevel = IsolationLevel
					.valueOf(config.getString(ConsumerConfig.ISOLATION_LEVEL_CONFIG).toUpperCase(Locale.ROOT));
			OffsetResetStrategy offsetResetStrategy = OffsetResetStrategy
					.valueOf(config.getString(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG).toUpperCase(Locale.ROOT));
			// Changes for 2.8.1
			// this.subscriptions = new SubscriptionState(offsetResetStrategy);
			this.subscriptions = new SubscriptionState(logContext, offsetResetStrategy);

			int maxPollIntervalMs = config.getInt(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG);
			int sessionTimeoutMs = config.getInt(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG);

			this.assignors = config.getConfiguredInstances(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
					ConsumerPartitionAssignor.class);

			/*
			 * this.assignors = config.getConfiguredInstances(ConsumerConfig.
			 * PARTITION_ASSIGNMENT_STRATEGY_CONFIG, ConsumerPartitionAssignor.class);
			 */

			// AQKafkaConsumer to talk to Oracle Database
			AQKafkaConsumer aqConsumer = new AQKafkaConsumer(logContext, config, time, this.metadata, this.metrics);
			aqConsumer.setAssignors(assignors);

			// Network Client to wrap aqConsumer. Maintains metadata
			NetworkClient networkClient = new NetworkClient(aqConsumer, this.metadata, clientId,
					config.getLong(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG),
					config.getLong(ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG),
					config.getInt(ConsumerConfig.SEND_BUFFER_CONFIG),
					config.getInt(ConsumerConfig.RECEIVE_BUFFER_CONFIG), (int) TimeUnit.HOURS.toMillis(1), time,
					logContext);

			// ConsumerNetworkClient uses network client to perform all consumer operations
			// i.e. poll/subscribe/joingroup/sync/commit/seek
			this.client = new ConsumerNetworkClient(groupId, logContext, networkClient, metadata, subscriptions,
					this.assignors, config.getBoolean(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG),
					config.getInt(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG), time, retryBackoffMs,
					maxPollIntervalMs, this.requestTimeoutMs, sessionTimeoutMs, defaultApiTimeoutMs, aqConsumer,
					metrics);
			
			this.topicMetadataFetcher = new TopicMetadataFetcher(logContext,this.client);

			this.okcMetrics = new OkafkaConsumerMetrics(metrics, metricGrpPrefix);

			cgMetadata = new ConsumerGroupMetadata(groupId);

			config.logUnused();
			AppInfoParser.registerAppInfo(JMX_PREFIX, clientId, metrics, time.milliseconds());

			log.debug("Kafka consumer initialized");
		} catch (Throwable t) {
			// call close methods if internal objects are already constructed
			// this is to prevent resource leak.
			close(0, true);
			// now propagate the exception
			throw new KafkaException("Failed to construct kafka consumer", t);
		}
	}

	/**
	 * Get the Oracle database connection used to consume records from Oracle
	 * Transactional Event Queue.
	 */
	public Connection getDBConnection() throws KafkaException {
		return ((ConsumerNetworkClient) client).getDBConnection();
	}

	/**
	 * Get the set of partitions currently assigned to this consumer using topic
	 * subscription. (which may be none if the assignment hasn't happened yet, or
	 * the partitions are in the process of getting reassigned).
	 * 
	 */
	public Set<TopicPartition> assignment() {
		return subscriptions.assignedPartitions();
	}

	/**
	 * Get the current subscription. Will return the same topics used in the most
	 * recent call to {@link #subscribe(Collection, ConsumerRebalanceListener)} , or
	 * an empty set if no such call has been made.
	 */
	public Set<String> subscription() {
		return subscriptions.subscription();
	}

	/**
	 * Subscribe to the given list of topics to get partitions assigned dynamically.
	 * However OKafka 23.4.0.0 supports subscription to only a single topic.
	 * Partitions are assigned dynamically to consumer based on <i>partition.assignment.strategy</i>. 
	 * <p>
	 * This method takes an object of {@link org.apache.kafka.clients.consumer.ConsumerRebalanceListener ConsumerRebalanceListener}.
	 * Its {@link ConsumerRebalanceListener#onPartitionsAssigned(Collection) onPartitionsAssigned} method will be invoked when
	 * partitions are assigned to this consumer. Similarly {@link ConsumerRebalanceListener#onPartitionsRevoked(Collection) onPartitionsRevoked}
	 * will be invoked when partitions are revoked. 
	 * <p>
	 * <b> Topic subscriptions are not incremental. This list will replace the
	 * current assignment (if there is one). </b> .
	 *
	 * If the given list of topics is empty, it is treated the same as
	 * {@link #unsubscribe()}. This call has effect only when poll is invoked.
	 * <p>
	 *
	 * @param topics   The list of topics to subscribe to
	 * @param listener null if not null is ignored
	 * @throws IllegalArgumentException If topics is null or contains null or empty
	 *                                  elements or size of topics is greater than
	 *                                  one.
	 */
	@Override
	public void subscribe(Collection<String> topics, ConsumerRebalanceListener listener) {
		acquireAndEnsureOpen();
		try {
			if (topics == null) {
				throw new IllegalArgumentException("Topic collection to subscribe to cannot be null");
			} else if (topics.isEmpty()) {
				// treat subscribing to empty topic list as the same as unsubscribing
				this.unsubscribe();
			} else {
				if (topics.size() > 1)
					throw new IllegalArgumentException("Only one topic can be subscribed");
				
				Collection<String> topicsUp = new ArrayList<String>(topics.size());
				
				for (String topic : topics) {
					if (topic == null || topic.trim().isEmpty())
						throw new IllegalArgumentException(
								"Topic collection to subscribe to cannot contain null or empty topic");
					
					topicsUp.add(topic.toUpperCase());
				}
				
				topics = topicsUp;
				
				// Only one topic can be subscribed, unsubcribe to previous topics before
				// subscribing to new topic
				Set<String> Alltopics = subscriptions.metadataTopics();
				if (Alltopics.size() > 0) {
					this.unsubscribe();
				}

				log.debug("Subscribed to topic(s): {}", Utils.join(topics, ", "));
				Set<String> subscribedTopicSet = new HashSet<>(topics);
				this.subscriptions.subscribe(subscribedTopicSet, listener);
				// metadata.setTopics(subscriptions.groupSubscription());
				// Change for 2.8.1 groupSubscription() is not present any more
				metadata.setTopics(subscribedTopicSet);
			}
		} finally {
			release();
		}
	}

	/**
	 * Subscribe to the given list of topics to get partitions assigned dynamically.
	 * However OKafka 23.4.0.0 supports subscription to only a single topic.
	 * Partitions are assigned dynamically to consumer based on <i>partition.assignment.strategy</i>. 
	 * 
	 * <b> Topic subscriptions are not incremental. This list will replace the
	 * current assignment (if there is one). </b> .
	 *
	 * If the given list of topics is empty, it is treated the same as
	 * {@link #unsubscribe()}. This call has effect only when poll is invoked.
	 * <p>
	 * This is a short-hand for
	 * {@link #subscribe(Collection, ConsumerRebalanceListener)}, which uses a no-op
	 * listener be default. 
	 *
	 * @param topics The list of topics to subscribe to
	 * @throws IllegalArgumentException If topics is null or contains null or empty
	 *                                  elements or size of topics is greater than
	 *                                  one.
	 */
	@Override
	public void subscribe(Collection<String> topics) {
		subscribe(topics, new NoOpConsumerRebalanceListener());
	}

	/**
	 * This method is not yet supported.
	 */
	@Override
	public void subscribe(Pattern pattern, ConsumerRebalanceListener listener) {
		throw new FeatureNotSupportedException("This feature is not suported for this release.");
	}

	/**
	 * This method is not yet supported.
	 */
	@Override
	public void subscribe(Pattern pattern) {
		subscribe(pattern, new NoOpConsumerRebalanceListener());
	}

	/**
	 * Unsubscribe from topic currently subscribed with
	 * {@link #subscribe(Collection)}.
	 */
	public void unsubscribe() {
		acquireAndEnsureOpen();
		try {
			log.debug("Unsubscribed to all topics ");
			this.subscriptions.unsubscribe();
			this.client.clearSubscription();
			this.client.unsubscribe();
			this.metadata.needMetadataForAllTopics(false);
		} finally {
			release();
		}
	}

	/**
	 * This method is not yet supported.
	 */
	@Override
	public void assign(Collection<TopicPartition> partitions) {
		throw new FeatureNotSupportedException("This feature is not suported for this release.");
	}

	/**
	 * Fetch data for the topic specified using {@link #subscribe(Collection)} APIs.
	 * It is an error to not have subscribed to any topic before polling for data
	 * Consumer maintains a single connection/session to any one of the oracle
	 * database instances. Each consumer in a consumer group is dynamically assigned partition(s) 
	 * of subscribed topic. 
	 * <p>
	 * On each poll, consumer consumes messages from the last fetch position(offset).
	 * If consumer is closed or application crashes without committing the consumed records then
	 * all uncommitted consumed messages are made available again. Next consumer who consumed from these partitions will start
	 * consuming records from the last committed offset.
	 * 
	 * This method returns immediately if there are records available. Otherwise, it will await the passed timeout.
	 * If the timeout expires, an empty record set will be returned. 
	 * Note that this method may block beyond the timeout in order to complete partition <i>rebalancing</i>.
	 * @see "Section 'Consumer Groups and Topic Subscriptions' in org.oracle.okafka.clients.consumer.KafkaConsumer documentation to understand 'rebalancing'"
	 * 
	 * @param timeout The time, in milliseconds, spent waiting in poll.
	 * @return map of topic to records since the last fetch for the subscribed list
	 *         of topic.
	 *
	 * @throws org.oracle.okafka.common.KafkaException for any other unrecoverable
	 *                                                 errors (e.g. errors
	 *                                                 deserializing key/value
	 *                                                 pairs, or any new error cases
	 *                                                 in future versions)
	 * @throws java.lang.IllegalArgumentException      if the timeout value is
	 *                                                 negative
	 * @throws java.lang.IllegalStateException         if the consumer is not
	 *                                                 subscribed to any topic.
	 *
	 */
	@Deprecated
	@Override
	public ConsumerRecords<K, V> poll(final long timeout) {
		if (timeout < 0)
			throw new IllegalArgumentException("Timeout must not be negative");

		return poll(time.timer(timeout), false);
	}

	/**
	 * Fetch data for the topic specified using {@link #subscribe(Collection)} APIs.
	 * It is an error to not have subscribed to any topic before polling for data.
	 * Each consumer(session )in a group is assigned a single unique partition of
	 * subscribed topic. Hence, Poll fetches data from its assigned partition till
	 * connection/session exists. If existing connection lost and connected to any
	 * instance of database then consumer(session) might be assigned with new
	 * partition of subscribed topic.
	 * <p>
	 * On each poll, consumer will try to use the last consumed offset as the
	 * starting offset and fetch sequentially.
	 * 
	 * @param timeout The time, in milliseconds, spent waiting in poll.
	 * @return map of topic to records since the last fetch for the subscribed list
	 *         of topic.
	 *
	 * @throws org.oracle.okafka.common.KafkaException for any other unrecoverable
	 *                                                 errors (e.g. errors
	 *                                                 deserializing key/value
	 *                                                 pairs)
	 * @throws java.lang.IllegalArgumentException      if the timeout value is
	 *                                                 negative
	 * @throws java.lang.IllegalStateException         if the consumer is not
	 *                                                 subscribed to any topic.
	 * @throws java.lang.ArithmeticException           if the timeout is greater
	 *                                                 than {@link Long#MAX_VALUE}
	 *                                                 milliseconds.
	 *
	 */
	@Override
	public ConsumerRecords<K, V> poll(final Duration timeout) {
		if (timeout.toMillis() < 0)
			throw new IllegalArgumentException("Timeout must not be negative");

		return poll(time.timer(timeout), true);
	}

	private ConsumerRecords<K, V> poll(final Timer timer, final boolean includeMetadataInTimeout) {
		acquireAndEnsureOpen();

		try {
			this.okcMetrics.recordPollStart(timer.currentTimeMs());
			// if (this.subscriptions.hasNoSubscription()) {
			// Changes for 2.8.1 use hasNoSubscriptionOrUserAssignment instead
			// hsNoSubscription
			if (this.subscriptions.hasNoSubscriptionOrUserAssignment()) {
				throw new IllegalStateException("Consumer is not subscribed to any topics or assigned any partitions");
			}
			do {
				// poll for new data until the timeout expires
				long elapsedTime = 0L;
				final long metadataEnd;
				if (includeMetadataInTimeout) {
					final long metadataStart = time.milliseconds();
					if (!updateMetadataAndSubscribeIfNeeded(timer.remainingMs())) {
						timer.update(time.milliseconds());
						continue;
					}
					metadataEnd = time.milliseconds();
					elapsedTime += metadataEnd - metadataStart;

				} else {
					while (!updateMetadataAndSubscribeIfNeeded(Long.MAX_VALUE)) {
						log.warn("Still waiting for metadata");
					}
					metadataEnd = time.milliseconds();
				}
				timer.update(time.milliseconds());
				final long syncStart = time.milliseconds();
				client.maybeAutoCommitOffsetsSync(time.milliseconds());
				final long syncEnd = time.milliseconds();
				elapsedTime += syncStart - syncEnd;
				// final long fetchStart = time.milliseconds();
				final Map<TopicPartition, List<ConsumerRecord<K, V>>> records = pollForFetches(timer.remainingMs());

				if (!records.isEmpty()) {

					return this.interceptors.onConsume(new ConsumerRecords<>(records));
				}
				
				timer.update(time.milliseconds());
			} while (timer.notExpired());
			// final long fetchEnd = time.milliseconds();
			// elapsedTime += fetchEnd - fetchStart;
			return ConsumerRecords.empty();
		} catch (ConnectionException | InvalidLoginCredentialsException exception) {
			log.error("Exception from poll: " + exception.getMessage(), exception);
			log.info("Closing the consumer due to exception : " + exception.getMessage());
			close();
			if(exception instanceof InvalidLoginCredentialsException)
				throw new AuthenticationException(exception.getMessage());
			throw exception;
		} finally {
			release();
			this.okcMetrics.recordPollEnd(timer.currentTimeMs());

		}
	}

	private boolean updateMetadataAndSubscribeIfNeeded(long timeout) {
		long elapsed = 0L;
		long subscriptionStart = time.milliseconds();
		client.maybeUpdateMetadata(timeout);
		elapsed = time.milliseconds() - subscriptionStart;
		if(elapsed > timeout) {
			log.debug("Timeout Exceeded in updateMetadataAndSubscribeIfNeeded() after maybeUpdateMetadata()");
			return false;
		}
		if (!client.mayBeTriggerSubcription(timeout - elapsed)) {
			return false;
		}
		elapsed = time.milliseconds() - subscriptionStart;
		if (elapsed <= timeout) {
			Set<TopicPartition> partitions = subscriptions.partitionsNeedingReset(time.milliseconds());
			if (partitions.isEmpty())
				return true;

			final Map<TopicPartition, Long> offsetResetTimestamps = new HashMap<>();
			for (final TopicPartition partition : partitions) {
				Long seekPos = offsetResetStrategyTimestamp(partition);
				if (seekPos != null)
					offsetResetTimestamps.put(partition, seekPos);
			}

			return client.resetOffsetsSync(offsetResetTimestamps, timeout - elapsed);

		} 
		else {
			log.debug("Timeout Exceeded in updateMetadataAndSubscribeIfNeeded() after mayBeTriggerSubscription()");
			return false;
		}
	}

	private Map<TopicPartition, List<ConsumerRecord<K, V>>> pollForFetches(final long timeoutMs) {

		if (timeoutMs <= 0)
			return Collections.<TopicPartition, List<ConsumerRecord<K, V>>>emptyMap();

		return createConsumerRecordsMap(client.poll(timeoutMs, fetchManagerMetrics));
	}
	/*
	 * private Map<TopicPartition, List<ConsumerRecord<K, V>>>
	 * createConsumerRecordsMap(List<AQjmsBytesMessage> messages, boolean obsolete)
	 * {
	 * 
	 * if(messages.size() == 0 ) { return Collections.<TopicPartition,
	 * List<ConsumerRecord<K, V>>>emptyMap(); }
	 * 
	 * Map<TopicPartition, List<ConsumerRecord<K, V>>> consumerRecords = new
	 * HashMap<>(); ConsumerRecord<K, V> record; String topic = null; int partition
	 * = -1; for(AQjmsBytesMessage message : messages) { try {
	 * 
	 * byte[] valueByteArray = message.getBytesData(); byte[] keyByteArray =
	 * message.getJMSCorrelationIDAsBytes();
	 * 
	 * //topic = message.getStringProperty("topic"); topic =
	 * ((AQjmsDestination)message.getJMSDestination()).getTopicName(); try {
	 * partition = message.getIntProperty(AQClient.PARTITION_PROPERTY)/2; }
	 * catch(Exception e) { try { partition =
	 * (int)message.getLongProperty(AQClient.PARTITION_PROPERTY)/2; }catch(Exception
	 * e1) {
	 * 
	 * } } K key = this.keyDeserializer.deserialize(topic, keyByteArray); V value =
	 * this.valueDeserializer.deserialize(topic, valueByteArray); OKafkaOffset
	 * okOffset = MessageIdConverter.getOKafkaOffset(message.getJMSMessageID(),
	 * true, true); record = new ConsumerRecord<>(topic, partition,
	 * okOffset.getOffset(), message.getJMSTimestamp(),
	 * TimestampType.LOG_APPEND_TIME, null, valueByteArray.length == 0 ?
	 * ConsumerRecord.NULL_SIZE : valueByteArray.length, valueByteArray.length == 0
	 * ? ConsumerRecord.NULL_SIZE : valueByteArray.length, key, value, new
	 * RecordHeaders()); } catch(JMSException exception) {
	 * log.error("JMS Exception while writing response  " + exception, exception);
	 * record = new ConsumerRecord<>("", -1, -1, -1,
	 * TimestampType.NO_TIMESTAMP_TYPE, null, ConsumerRecord.NULL_SIZE,
	 * ConsumerRecord.NULL_SIZE, null, null, new RecordHeaders()); } catch(Exception
	 * e) { record = new ConsumerRecord<>("", -1, -1, -1,
	 * TimestampType.NO_TIMESTAMP_TYPE, null, ConsumerRecord.NULL_SIZE,
	 * ConsumerRecord.NULL_SIZE, null, null, new RecordHeaders());
	 * log.error("Exception while writing response  " + e,e); } TopicPartition tp =
	 * new TopicPartition(topic, partition); if(tp != null && partition != -1) {
	 * //Changes for 2.8.1 try { subscriptions.position(tp, new
	 * FetchPosition(record.offset(), Optional.empty(), new
	 * LeaderAndEpoch(Optional.empty(), Optional.empty()))); }
	 * catch(IllegalStateException isE) { if(metadata.getDBMajorVersion() < 23) { //
	 * Partition assigned by TEQ Server not through JoinGroup/Sync
	 * subscriptions.assignFromSubscribed( Collections.singleton(tp));
	 * subscriptions.seek(tp,0); subscriptions.completeValidation(tp);
	 * subscriptions.position(tp, new FetchPosition(record.offset(),
	 * Optional.empty(), new LeaderAndEpoch(Optional.empty(), Optional.empty()))); }
	 * subscriptions.position(tp, new FetchPosition(record.offset(),
	 * Optional.empty(), new LeaderAndEpoch(Optional.empty(), Optional.empty()))); }
	 * catch(Exception e) { log.error("Exception while setting fetch position " + e
	 * , e); e.printStackTrace(); } / * OffsetAndMetadata offset =
	 * subscriptions.allConsumed().get(tp); if(offset == null)
	 * subscriptions.allConsumed().put(tp , new OffsetAndMetadata(record.offset()));
	 * else { if(offset.offset() < record.offset())
	 * subscriptions.allConsumed().put(tp , new OffsetAndMetadata(record.offset()));
	 * } / }
	 * 
	 * if(!consumerRecords.containsKey(tp)) consumerRecords.put(tp, new
	 * ArrayList<ConsumerRecord<K,V>>()); consumerRecords.get(tp).add(record);
	 * 
	 * } return consumerRecords; }
	 */

	private Map<TopicPartition, List<ConsumerRecord<K, V>>> createConsumerRecordsMap(List<AQjmsBytesMessage> messages) {

		if (messages.size() == 0) {
			return Collections.<TopicPartition, List<ConsumerRecord<K, V>>>emptyMap();
		}

		Map<TopicPartition, List<ConsumerRecord<K, V>>> consumerRecords = new HashMap<>();
		Map<TopicPartition, Integer> headersSize = new HashMap();
		ConsumerRecord<K, V> record;
		String topic = null;
		int partition = -1;
		int messageVersion = 1;
		byte[] keyArray = null;
		byte[] valueArray = null;
		int keyLen = 0;
		int valueLen = 0;
		int hSize = 0;

		for (AQjmsBytesMessage message : messages) {
			keyArray = null;
			valueArray = null;
			keyLen = 0;
			valueLen = 0;

			try {
				RecordHeaders rcH = new RecordHeaders();
				try {
					messageVersion = message.getIntProperty(AQClient.MESSAGE_VERSION);
				} catch (Exception e) {
					messageVersion = 1;
				}

				/*
				 * Received Byte Payload in below format: | KEY LENGTH (4 Bytes Fixed) | KEY | |
				 * VALUE LENGTH (4 BYTES FIXED) | VALUE | | HEADER NAME LENGTH(4 BYTES FIXED) |
				 * HEADER NAME | | HEADER VALUE LENGTH (4 BYTES FIXED) | HEADER VALUE | | HEADER
				 * NAME LENGTH(4 BYTES FIXED) | HEADER NAME | | HEADER VALUE LENGTH (4 BYTES
				 * FIXED) | HEADER VALUE |
				 * 
				 * For records with null key , KEY LENGTH is set to 0. For records with null
				 * value, VALUE LENGTH is set to 0. Number of headers are set in property
				 * "AQINTERNAL_HEADERCOUNT"
				 * 
				 */
				if (messageVersion == 2) {
					byte[] payloadArray = message.getBytesData();
					byte[] bLength = new byte[DLENGTH_SIZE];

					// Read Key First
					ByteBuffer pBuffer = ByteBuffer.wrap(payloadArray);
					pBuffer.get(bLength, 0, DLENGTH_SIZE);
					keyLen = ConnectionUtils.convertToInt(bLength);
					keyArray = new byte[keyLen];
					pBuffer.get(keyArray, 0, keyLen);

					// Get Actual Payload
					pBuffer.get(bLength, 0, DLENGTH_SIZE);
					valueLen = ConnectionUtils.convertToInt(bLength);

					valueArray = new byte[valueLen];
					pBuffer.get(valueArray, 0, valueLen);

					int hCount = 0;
					try {
						hCount = message.getIntProperty(AQClient.HEADERCOUNT_PROPERTY);
					} catch (Exception e) {
						hCount = 0;
					}
					int hKeyLen = 0;
					int hValueLen = 0;

					for (int i = 0; i < hCount; i++) {
						pBuffer.get(bLength, 0, DLENGTH_SIZE);
						hKeyLen = ConnectionUtils.convertToInt(bLength);
						if (hKeyLen > 0) {
							byte[] hKeyArray = new byte[hKeyLen];
							hSize += hKeyArray.length;
							pBuffer.get(hKeyArray, 0, hKeyLen);
							String hKey = new String(hKeyArray);
							pBuffer.get(bLength, 0, DLENGTH_SIZE);
							hValueLen = ConnectionUtils.convertToInt(bLength);
							byte[] hValueArray = new byte[hValueLen];
							hSize += hValueArray.length;
							pBuffer.get(hValueArray, 0, hValueLen);
							rcH.add(hKey, hValueArray);
						}

					}
				} else {
					keyArray = message.getJMSCorrelationIDAsBytes();
					valueArray = message.getBytesData();
				}

				topic = ((AQjmsDestination) message.getJMSDestination()).getTopicName();
				
				try {
					partition = (int) message.getLongProperty(AQClient.PARTITION_PROPERTY) / 2;
				} catch(Exception e1) {
					try {
						partition = message.getIntProperty(AQClient.PARTITION_PROPERTY) / 2;
					} catch(Exception e2) {
						try {
							partition = Integer.parseInt(message.getStringProperty(AQClient.PARTITION_PROPERTY)) / 2;
						} catch(Exception e3) {
							// do nothing
						}
					}
				}
				
				K key = this.keyDeserializer.deserialize(topic, keyArray);
				V value = this.valueDeserializer.deserialize(topic, valueArray);
				OKafkaOffset okOffset = MessageIdConverter.getOKafkaOffset(message.getJMSMessageID(), true, true);

				record = new ConsumerRecord<>(topic, partition, okOffset.getOffset(), message.getJMSTimestamp(),
						TimestampType.LOG_APPEND_TIME, null, keyLen == 0 ? ConsumerRecord.NULL_SIZE : keyLen,
						valueLen == 0 ? ConsumerRecord.NULL_SIZE : valueLen, key, value, rcH);

			} catch (JMSException exception) {
				log.error("JMS Exception while creting ConsumerRecord  " + exception, exception);
				record = new ConsumerRecord<>("", -1, -1, -1, TimestampType.NO_TIMESTAMP_TYPE, null,
						ConsumerRecord.NULL_SIZE, ConsumerRecord.NULL_SIZE, null, null, new RecordHeaders());
			} catch (Exception e) {
				record = new ConsumerRecord<>("", -1, -1, -1, TimestampType.NO_TIMESTAMP_TYPE, null,
						ConsumerRecord.NULL_SIZE, ConsumerRecord.NULL_SIZE, null, null, new RecordHeaders());

				log.error("Exception while creting ConsumerRecord  " + e, e);
			}

			TopicPartition tp = new TopicPartition(topic, partition);
			if (tp != null && partition != -1) {
				// Changes for 2.8.1
				try {
					subscriptions.position(tp, new FetchPosition(record.offset(), Optional.empty(),
							new LeaderAndEpoch(Optional.empty(), Optional.empty())));
				} catch (IllegalStateException isE) {
					TopicTeqParameters teqParam = metadata.topicParaMap.get(topic);
					int stickyDeqParam = teqParam != null ? teqParam.getStickyDeq(): 2;
					if (metadata.getDBMajorVersion() < 23 || stickyDeqParam == 1) {
						// Partition assigned by TEQ Server not through JoinGroup/Sync
						subscriptions.assignFromSubscribed(Collections.singleton(tp));
						subscriptions.seek(tp, 0);
						subscriptions.completeValidation(tp);
					}
					subscriptions.position(tp, new FetchPosition(record.offset(), Optional.empty(),
							new LeaderAndEpoch(Optional.empty(), Optional.empty())));
				} catch (Exception e) {
					log.error("Exception while setting fetch position " + e, e);
				}
				/*
				 * OffsetAndMetadata offset = subscriptions.allConsumed().get(tp); if(offset ==
				 * null) subscriptions.allConsumed().put(tp , new
				 * OffsetAndMetadata(record.offset())); else { if(offset.offset() <
				 * record.offset()) subscriptions.allConsumed().put(tp , new
				 * OffsetAndMetadata(record.offset())); }
				 */
			}

			if (!consumerRecords.containsKey(tp))
				consumerRecords.put(tp, new ArrayList<ConsumerRecord<K, V>>());
			consumerRecords.get(tp).add(record);

			if (!headersSize.containsKey(tp))
				headersSize.put(tp, hSize);
			headersSize.put(tp, headersSize.get(tp) + hSize);

		}

		Set<TopicPartition> topicPartitions = new HashSet<TopicPartition>(consumerRecords.keySet());
		FetchResponseMetricAggregator fetchResponseMetricAggregator = new FetchResponseMetricAggregator(
				fetchManagerMetrics, topicPartitions);

		for (Map.Entry<TopicPartition, List<ConsumerRecord<K, V>>> entry : consumerRecords.entrySet()) {
			int bytes = 0;
			List<ConsumerRecord<K, V>> recordList = entry.getValue();
			int recordCount = recordList.size();

			for (int i = 0; i < recordCount; i++) {
				bytes += recordList.get(i).serializedKeySize();
				bytes += recordList.get(i).serializedValueSize();
				bytes += hSize;
			}
			fetchResponseMetricAggregator.record(entry.getKey(), bytes, recordCount);
		}
		return consumerRecords;
	}

	private long remainingTimeAtLeastZero(final long timeoutMs, final long elapsedTime) {
		return Math.max(0, timeoutMs - elapsedTime);
	}

	/**
	 * Commits messages(offsets) consumed using {@link #poll(Duration) poll()} since
	 * last commit in this session.
	 * 
	 * Commit on session is either successfull or rollback. Commit fails only in
	 * rare cases like shutdown. Commit failure results in rollback. If rollback
	 * occurs then consumed messages since last commit are considered not consumed.
	 * If process restarts after failure then it starts consuming from this
	 * position.
	 * <p>
	 * This is a synchronous commit and will block until either the commit succeeds
	 * or rollback happens. Commit does not take any timeout into account for
	 * completion of call.
	 * <p>
	 *
	 * @throws org.oracle.okafka.common.KafkaException for any other unrecoverable
	 *                                                 errors (i.e topic doesn't
	 *                                                 exist, session rolled back as
	 *                                                 db shutdown).
	 */
	@Override
	public void commitSync() {
		commitSync(Duration.ofMillis(defaultApiTimeoutMs));
	}

	/**
	 * Commits messages(offsets) consumed using {@link #poll(Duration) poll()} since
	 * last commit in this session.
	 * 
	 * Commit on session is either successfull or rollback. Commit fails only in
	 * rare cases like shutdown. Commit failure results in rollback. If rollback
	 * occurs then consumed messages since last commit are considered not consumed.
	 * If process restarts after failure then it starts consuming from this
	 * position.
	 * <p>
	 * This is a synchronous commit and will block until either the commit succeeds
	 * or rollback happens. Commit does not take any timeout into account for
	 * completion of call. This call is equivalent to commitSync().
	 * <p>
	 *
	 * @throws org.oracle.okafka.common.KafkaException for any other unrecoverable
	 *                                                 errors .
	 */
	@Override
	public void commitSync(Duration timeout) {
		acquireAndEnsureOpen();
		long commitStart = time.nanoseconds();

		try {
			client.commitOffsetsSync(subscriptions.allConsumed(), timeout.toMillis());
			interceptors.onCommit(this.subscriptions.allConsumed());
		} catch (Exception exception) {
			if(exception instanceof DisconnectException)
				throw (DisconnectException)exception;
			throw new KafkaException("failed to commit messages", exception);
		} finally {
			release();
		}
	}

	/**
	 * This method not yet supported.
	 */
	@Override
	public void commitSync(final Map<TopicPartition, OffsetAndMetadata> offsets) {
		throw new FeatureNotSupportedException("This feature is not suported for this release.");
	}

	/**
	 * This method not yet supported.
	 */
	@Override
	public void commitSync(final Map<TopicPartition, OffsetAndMetadata> offsets, final Duration timeout) {
		throw new FeatureNotSupportedException("This feature is not suported for this release.");
	}

	/**
	 * Commit mesages consumed using {@link #poll(Duration)} for the subscribed
	 * topic and assigned partition in this session since last commit. This call is
	 * equivalent to {@link #commitAsync(OffsetCommitCallback)} with null callback.
	 */
	@Override
	public void commitAsync() {
		commitAsync(null);
	}

	/**
	 * Commits messages(offsets) consumed using {@link #poll(Duration) )} since last
	 * commit in this session.
	 * 
	 * Commit on session is either successfull or rollback. Commit fails only in
	 * rare cases like shutdown. Commit failure results in rollback. If rollback
	 * occurs then consumed messages since last commit are considered not consumed.
	 * If process restarts after failure then it starts consuming from this rollback
	 * position.
	 * <p>
	 * Internally this is an synchronous call and blocks until either commit is
	 * successful or rolled back. Any errors encountered are either passed to the
	 * callback (if provided) or discarded.
	 * <p>
	 * 
	 * @param callback Callback to invoke when the commit completes
	 */
	@Override
	public void commitAsync(OffsetCommitCallback callback) {
		try {
			client.commitOffsetsSync(subscriptions.allConsumed(), defaultApiTimeoutMs);
			if (callback != null)
				callback.onComplete(this.subscriptions.allConsumed(), null);
		} catch (Exception exception) {
			if (callback != null)
				callback.onComplete(this.subscriptions.allConsumed(),
						new KafkaException("failed to commit the current consumed offsets", exception));
		}
	}

	/**
	 * This method is not yet supported.
	 */
	@Override
	public void commitAsync(final Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
		throw new FeatureNotSupportedException("This feature is not suported for this release.");
	}

	/**
	 * Overrides the fetch offset that the consumer will use on the next
	 * {@link #poll(Duration) poll(timeout)}. If this API is invoked for the same
	 * partition more than once, the latest offset will be used on the next poll().
	 * Seeking to already consumed offset/message , in current or previous sessions,
	 * doesn't reconsume the message.
	 *
	 * @throws IllegalArgumentException if the provided offset is negative
	 */
	// @throws IllegalStateException if the provided TopicPartition is not
	// assigned to this consumer
	@Override
	public void seek(TopicPartition partition, long offset) {
		if (offset < 0)
			throw new IllegalArgumentException("seek offset must not be a negative number");

		acquireAndEnsureOpen();
		try {
			log.debug("Seeking to offset {} for partition {}", offset, partition);
			Map<TopicPartition, Long> offsetResetTimestamps = new HashMap<>();
			offsetResetTimestamps.put(partition, offset);
			client.resetOffsetsSync(offsetResetTimestamps, offset);

		} finally {
			release();
		}
	}

	/**
	 * Seek to the first available offset for each of the given partitions. This
	 * function evaluates lazily, seeking to the first offset in all partitions only
	 * when {@link #poll(Duration)} is called. Seeking to already consumed
	 * offset/message , in current or previous sessions, doesn't reconsume the
	 * message.
	 *
	 * @throws IllegalArgumentException if {@code partitions} is {@code null}
	 */
	// @throws IllegalStateException if the provided TopicPartition is not
	// assigned to this consumer

	@Override
	public void seekToBeginning(Collection<TopicPartition> partitions) {
		if (partitions == null)
			throw new IllegalArgumentException("Partitions collection cannot be null");

		acquireAndEnsureOpen();
		try {
			Collection<TopicPartition> parts = partitions.size() == 0 ? this.subscriptions.assignedPartitions()
					: partitions;
			for (TopicPartition tp : parts) {
				log.debug("Seeking to beginning of partition {}", tp);
				subscriptions.requestOffsetReset(tp, OffsetResetStrategy.EARLIEST);
			}
		} finally {
			release();
		}
	}

	/**
	 * Seek to the last offset for each of the given partitions. This function
	 * evaluates lazily, seeking to the final offset in all partitions only when
	 * {@link #poll(Duration)} is called. Seeking to already consumed offset/message
	 * , in current or previous sessions, doesn't reconsume the message.
	 *
	 * @throws IllegalArgumentException if {@code partitions} is {@code null}
	 */
	// @throws IllegalStateException if the provided TopicPartition is not
	// assigned to this consumer

	@Override
	public void seekToEnd(Collection<TopicPartition> partitions) {
		if (partitions == null)
			throw new IllegalArgumentException("Partitions collection cannot be null");

		acquireAndEnsureOpen();
		try {
			Collection<TopicPartition> parts = partitions.size() == 0 ? this.subscriptions.assignedPartitions()
					: partitions;
			for (TopicPartition tp : parts) {
				log.debug("Seeking to end of partition {}", tp);
				subscriptions.requestOffsetReset(tp, OffsetResetStrategy.LATEST);
			}
		} finally {
			release();
		}
	}
	
	/**
     * Get metadata about partitions for all topics that the user is authorized to view. This method will issue a
     * remote call to the server.

     * @return The map of topics and its partitions
     *
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors
     * @throws org.apache.kafka.common.errors.TimeoutException if the offset metadata could not be fetched before
     *         the amount of time allocated by {@code default.api.timeout.ms} expires.
     */
	@Override
	public Map<String, List<PartitionInfo>> listTopics() {
		return listTopics(Duration.ofMillis(defaultApiTimeoutMs));
	}
	
	/**
     * Get metadata about partitions for all topics that the user is authorized to view. This method will issue a
     * remote call to the server.
     *
     * @param timeout The maximum time this operation will block to fetch topic metadata
     *
     * @return The map of topics and its partitions
     * 
     * @throws org.apache.kafka.common.errors.TimeoutException if the topic metadata could not be fetched before
     *             expiration of the passed timeout
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors
     */
	@Override
	public Map<String, List<PartitionInfo>> listTopics(Duration timeout) {
		acquireAndEnsureOpen();
		try {
			return topicMetadataFetcher.getAllTopicMetadata(time.timer(timeout));
		} finally {
			release();
		}
	}
	
	@Override
	public Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition> partitions) {
		return committed(partitions, Duration.ofMillis(defaultApiTimeoutMs));
	}
	
	@Override
	public Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition> partitions, Duration timeout) {
        acquireAndEnsureOpen();
        try {
        	return client.fetchCommittedOffsets(partitions, time.timer(timeout));
        }finally {
        	release();
        }
	}

	/**
	 * This method is not yet supported.
	 */
	@Override
	public long position(TopicPartition partition) {
		throw new FeatureNotSupportedException("This feature is not suported for this release.");
	}

	/**
	 * This method is not yet supported.
	 */
	@Override
	public long position(TopicPartition partition, final Duration timeout) {
		throw new FeatureNotSupportedException("This feature is not suported for this release.");
	}

	/**
	 * This method is not yet supported.
	 */
	@Override
	public OffsetAndMetadata committed(TopicPartition partition) {
		throw new FeatureNotSupportedException("This feature is not suported for this release.");
	}

	/**
	 * This method is not yet supported.
	 */
	@Override
	public OffsetAndMetadata committed(TopicPartition partition, final Duration timeout) {
		throw new FeatureNotSupportedException("This feature is not suported for this release.");
	}

	/**
     * Get the metrics kept by the consumer
     */
	@Override
	public Map<MetricName, ? extends Metric> metrics() {
		return Collections.unmodifiableMap(this.metrics.metrics());
	}

	/**
	 * This method is not yet supported.
	 */
	@Override
	public List<PartitionInfo> partitionsFor(String topic) {
		throw new FeatureNotSupportedException("This feature is not suported for this release.");
	}

	/**
	 * This method is not yet supported.
	 */
	@Override
	public List<PartitionInfo> partitionsFor(String topic, Duration timeout) {
		throw new FeatureNotSupportedException("This feature is not suported for this release.");
	}

	/**
	 * This method is not yet supported.
	 */
	@Override
	public void pause(Collection<TopicPartition> partitions) {
		throw new FeatureNotSupportedException("This feature is not suported for this release.");
	}

	/**
	 * This method is not yet supported.
	 */
	@Override
	public void resume(Collection<TopicPartition> partitions) {
		throw new FeatureNotSupportedException("This feature is not suported for this release.");
	}

	/**
	 * This method is not yet supported.
	 */
	@Override
	public Set<TopicPartition> paused() {
		throw new FeatureNotSupportedException("This feature is not suported for this release.");
	}

	/**
	 * This method is not yet supported.
	 */
	@Override
	public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch) {
		throw new FeatureNotSupportedException("This feature is not suported for this release.");
	}

	/**
	 * This method is not yet supported.
	 */
	@Override
	public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch,
			Duration timeout) {
		throw new FeatureNotSupportedException("This feature is not suported for this release.");
	}

	/**
	 * This method is not yet supported.
	 */
	@Override
	public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
		throw new FeatureNotSupportedException("This feature is not suported for this release.");
	}

	/**
	 * This method is not yet supported.
	 */
	@Override
	public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions, Duration timeout) {
		throw new FeatureNotSupportedException("This feature is not suported for this release.");
	}

	/**
	 * This method is not yet supported.
	 */
	@Override
	public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
		throw new FeatureNotSupportedException("This feature is not suported for this release.");
	}

	/**
	 * This method is not yet supported.
	 */
	@Override
	public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions, Duration timeout) {
		throw new FeatureNotSupportedException("This feature is not suported for this release.");
	}

	/**
	 * Tries to close the consumer cleanly. If auto-commit is enabled, this will
	 * commit the current offsets . Close doen't take timeout into consideration.
	 *
	 * @throws org.apache.kafka.common.KafkaException for any other error during close
	 */
	@Override
	public void close() {
		close(Duration.ofMillis(DEFAULT_CLOSE_TIMEOUT_MS));
	}

	/**
	 * Tries to close the consumer cleanly. If auto-commit is enabled, this will
	 * commit the current offsets . Close doen't take timeout into consideration.
	 * 
	 * @param timeout  Not used
	 * @param timeUnit Not used
	 * @throws IllegalArgumentException                If the {@code timeout} is
	 *                                                 negative.
	 * @throws org.oracle.okafka.common.KafkaException for any other error during
	 *                                                 close
	 *
	 */
	@Deprecated
	public void close(long timeout, TimeUnit timeUnit) {
		close(Duration.ofMillis(timeUnit.toMillis(timeout)));
	}

	/**
	 * Tries to close the consumer cleanly. If auto-commit is enabled, this will
	 * commit the current offsets . Close doen't take timeout into consideration.
	 *
	 * @param timeout not used
	 *
	 * @throws IllegalArgumentException                If the {@code timeout} is
	 *                                                 negative.
	 * @throws org.oracle.okafka.common.KafkaException for any other error during
	 *                                                 close
	 */
	@Override
	public void close(Duration timeout) {
		if (timeout.toMillis() < 0)
			throw new IllegalArgumentException("The timeout cannot be negative.");
		acquire();
		try {
			if (!closed) {
				closed = true;
				close(timeout.toMillis(), false);
			}
		} finally {
			release();
		}
	}

	/**
	 * This method is not yet supported.
	 */
	@Override
	public void wakeup() {
		throw new FeatureNotSupportedException("This feature is not suported for this release.");
	}

	private ClusterResourceListeners configureClusterResourceListeners(Deserializer<K> keyDeserializer,
			Deserializer<V> valueDeserializer, List<?>... candidateLists) {
		ClusterResourceListeners clusterResourceListeners = new ClusterResourceListeners();
		for (List<?> candidateList : candidateLists)
			clusterResourceListeners.maybeAddAll(candidateList);

		clusterResourceListeners.maybeAdd(keyDeserializer);
		clusterResourceListeners.maybeAdd(valueDeserializer);
		return clusterResourceListeners;
	}

	private void close(long timeoutMs, boolean swallowException) {
		log.trace("Closing the Kafka consumer");
		AtomicReference<Throwable> firstException = new AtomicReference<>();
		try {
			if (client != null)
				client.close(Math.min(timeoutMs, requestTimeoutMs));
		} catch (Throwable t) {
			firstException.compareAndSet(null, t);
			log.error("Failed to close OKafka consumer ", t);
		}
		/*
		 * ClientUtils.closeQuietly(interceptors, "consumer interceptors",
		 * firstException); ClientUtils.closeQuietly(metrics, "consumer metrics",
		 * firstException); ClientUtils.closeQuietly(keyDeserializer,
		 * "consumer key deserializer", firstException);
		 * ClientUtils.closeQuietly(valueDeserializer, "consumer value deserializer",
		 * firstException);
		 */
		// Change for 2.8.1 :: closeQuietly moved to Utils instead of CLientUtils
		Utils.closeQuietly(interceptors, "consumer interceptors", firstException);
		Utils.closeQuietly(metrics, "consumer metrics", firstException);
		Utils.closeQuietly(keyDeserializer, "consumer key deserializer", firstException);
		Utils.closeQuietly(valueDeserializer, "consumer value deserializer", firstException);

		AppInfoParser.unregisterAppInfo(JMX_PREFIX, clientId, metrics);
		log.debug("Kafka consumer has been closed");
		Throwable exception = firstException.get();
		if (exception != null && !swallowException) {
			throw new KafkaException("Failed to close kafka consumer", exception);
		}
	}

	private Long offsetResetStrategyTimestamp(final TopicPartition partition) {
		OffsetResetStrategy strategy = subscriptions.resetStrategy(partition);
		if (strategy == OffsetResetStrategy.EARLIEST)
			return -2L;
		else if (strategy == OffsetResetStrategy.LATEST)
			return -1L;
		/*
		 * Changes for 2.8.1 : Not sure from where TO_OFFSET was introduced. else if
		 * (strategy == OffsetResetStrategy.TO_OFFSET) return
		 * subscriptions.position(partition);
		 */
		else
			return null;
	}

	/**
	 * Acquire the light lock and ensure that the consumer hasn't been closed.
	 * 
	 * @throws IllegalStateException If the consumer has been closed
	 */
	private void acquireAndEnsureOpen() {
		acquire();
		if (this.closed) {
			release();
			throw new IllegalStateException("This consumer has already been closed.");
		}
	}

	/**
	 * Acquire the light lock protecting this consumer from multi-threaded access.
	 * Instead of blocking when the lock is not available, however, we just throw an
	 * exception (since multi-threaded usage is not supported).
	 * 
	 * @throws ConcurrentModificationException if another thread already has the
	 *                                         lock
	 */
	private void acquire() {
		long threadId = Thread.currentThread().getId();
		if (threadId != currentThread.get() && !currentThread.compareAndSet(NO_CURRENT_THREAD, threadId))
			throw new ConcurrentModificationException("KafkaConsumer is not safe for multi-threaded access");
		refcount.incrementAndGet();
	}

	/**
	 * Release the light lock protecting the consumer from multi-threaded access.
	 */
	private void release() {
		if (refcount.decrementAndGet() == 0)
			currentThread.set(NO_CURRENT_THREAD);
	}

	/**
	 * This method is not yet supported.
	 */
	@Override
	public void seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata) {
		throw new FeatureNotSupportedException("This feature is not suported for this release.");
	}

	/**
     * Return the current group metadata associated with this consumer.
     *
     * @return consumer group metadata
     */
	@Override
	public ConsumerGroupMetadata groupMetadata() {
		return cgMetadata;
	}

	/**
	 * This method is not yet supported.
	 */
	@Override
	public void enforceRebalance() {
		throw new FeatureNotSupportedException("This feature is not suported for this release.");
	}

	/**
	 * This method is not yet supported.
	 */
	@Override
	public void enforceRebalance(String reason) {
		throw new FeatureNotSupportedException("This feature is not suported for this release.");
	}

	/**
	 * This method is not yet supported.
	 */
	@Override
	public Uuid clientInstanceId(Duration timeout) {
		throw new FeatureNotSupportedException("This feature is not suported for this release.");
	}

	/**
	 * This method is not yet supported.
	 */
	@Override
	public OptionalLong currentLag(TopicPartition topicPartition) {
		throw new FeatureNotSupportedException("This feature is not suported for this release.");
	}
	
	/**
     * Since we parse the message data for each partition from each fetch response lazily, fetch-level
     * metrics need to be aggregated as the messages from each partition are parsed. This class is used
     * to facilitate this incremental aggregation.
     */
	private static class FetchResponseMetricAggregator {
		private final FetchManagerMetrics sensors;
		private final Set<TopicPartition> unrecordedPartitions;

		private final FetchMetrics fetchMetrics = new FetchMetrics();
		private final Map<String, FetchMetrics> topicFetchMetrics = new HashMap<>();

		private FetchResponseMetricAggregator(FetchManagerMetrics sensors, Set<TopicPartition> partitions) {
			this.sensors = sensors;
			this.unrecordedPartitions = partitions;
		}

		/**
		 * After each partition is parsed, we update the current metric totals with the
		 * total bytes and number of records parsed. After all partitions have reported,
		 * we write the metric.
		 */
		public void record(TopicPartition partition, int bytes, int records) {
			this.unrecordedPartitions.remove(partition);
			this.fetchMetrics.increment(bytes, records);

			// collect and aggregate per-topic metrics
			String topic = partition.topic();
			FetchMetrics topicFetchMetric = this.topicFetchMetrics.get(topic);
			if (topicFetchMetric == null) {
				topicFetchMetric = new FetchMetrics();
				this.topicFetchMetrics.put(topic, topicFetchMetric);
			}
			topicFetchMetric.increment(bytes, records);

			if (this.unrecordedPartitions.isEmpty()) {
				// once all expected partitions from the fetch have reported in, record the
				// metrics
				this.sensors.bytesFetched.record(this.fetchMetrics.fetchBytes);
				this.sensors.recordsFetched.record(this.fetchMetrics.fetchRecords);

				// also record per-topic metrics
				for (Map.Entry<String, FetchMetrics> entry : this.topicFetchMetrics.entrySet()) {
					FetchMetrics metric = entry.getValue();
					this.sensors.recordTopicFetchMetrics(entry.getKey(), metric.fetchBytes, metric.fetchRecords);
				}
			}
		}

		private static class FetchMetrics {
			private int fetchBytes;
			private int fetchRecords;

			protected void increment(int bytes, int records) {
				this.fetchBytes += bytes;
				this.fetchRecords += records;
			}
		}
	}
	
	/**
	 * @hidden
	 * The {@link FetchManagerMetrics} class provides wrapper methods to record lag, lead, latency, and fetch metrics.
	 * It keeps an internal ID of the assigned set of partitions which is updated to ensure the set of metrics it
	 * records matches up with the topic-partitions in use.
	 */
	public static class FetchManagerMetrics {
		private final Metrics metrics;
		private FetchMetricsRegistry metricsRegistry;
		private final Sensor bytesFetched;
		private final Sensor recordsFetched;
		private final Sensor fetchLatency;

		private int assignmentId = 0;
		private Set<TopicPartition> assignedPartitions = Collections.emptySet();

		private FetchManagerMetrics(Metrics metrics, FetchMetricsRegistry metricsRegistry) {
			this.metrics = metrics;
			this.metricsRegistry = metricsRegistry;

			this.bytesFetched = metrics.sensor("bytes-fetched");
			this.bytesFetched.add(metrics.metricInstance(metricsRegistry.fetchSizeAvg), new Avg());
			this.bytesFetched.add(metrics.metricInstance(metricsRegistry.fetchSizeMax), new Max());
			this.bytesFetched.add(new Meter(metrics.metricInstance(metricsRegistry.bytesConsumedRate),
					metrics.metricInstance(metricsRegistry.bytesConsumedTotal)));

			this.recordsFetched = metrics.sensor("records-fetched");
			this.recordsFetched.add(metrics.metricInstance(metricsRegistry.recordsPerRequestAvg), new Avg());
			this.recordsFetched.add(new Meter(metrics.metricInstance(metricsRegistry.recordsConsumedRate),
					metrics.metricInstance(metricsRegistry.recordsConsumedTotal)));

			this.fetchLatency = metrics.sensor("fetch-latency");
			this.fetchLatency.add(metrics.metricInstance(metricsRegistry.fetchLatencyAvg), new Avg());
			this.fetchLatency.add(metrics.metricInstance(metricsRegistry.fetchLatencyMax), new Max());
			this.fetchLatency
					.add(new Meter(new WindowedCount(), metrics.metricInstance(metricsRegistry.fetchRequestRate),
							metrics.metricInstance(metricsRegistry.fetchRequestTotal)));
			/*
			 * this.recordsFetchLag = metrics.sensor("records-lag");
			 * this.recordsFetchLag.add(metrics.metricInstance(metricsRegistry.recordsLagMax
			 * ), new Max());
			 * 
			 * this.recordsFetchLead = metrics.sensor("records-lead");
			 * this.recordsFetchLead.add(metrics.metricInstance(metricsRegistry.
			 * recordsLeadMin), new Min());
			 */
		}

		private void recordTopicFetchMetrics(String topic, int bytes, int records) {
			// record bytes fetched
			String name = "topic." + topic + ".bytes-fetched";
			Sensor bytesFetched = this.metrics.getSensor(name);
			if (bytesFetched == null) {
				Map<String, String> metricTags = Collections.singletonMap("topic", topic.replace('.', '_'));

				bytesFetched = this.metrics.sensor(name);
				bytesFetched.add(this.metrics.metricInstance(metricsRegistry.topicFetchSizeAvg, metricTags), new Avg());
				bytesFetched.add(this.metrics.metricInstance(metricsRegistry.topicFetchSizeMax, metricTags), new Max());
				bytesFetched
						.add(new Meter(this.metrics.metricInstance(metricsRegistry.topicBytesConsumedRate, metricTags),
								this.metrics.metricInstance(metricsRegistry.topicBytesConsumedTotal, metricTags)));
			}
			bytesFetched.record(bytes);

			// record records fetched
			name = "topic." + topic + ".records-fetched";
			Sensor recordsFetched = this.metrics.getSensor(name);
			if (recordsFetched == null) {
				Map<String, String> metricTags = new HashMap<>(1);
				metricTags.put("topic", topic.replace('.', '_'));

				recordsFetched = this.metrics.sensor(name);
				recordsFetched.add(this.metrics.metricInstance(metricsRegistry.topicRecordsPerRequestAvg, metricTags),
						new Avg());
				recordsFetched.add(
						new Meter(this.metrics.metricInstance(metricsRegistry.topicRecordsConsumedRate, metricTags),
								this.metrics.metricInstance(metricsRegistry.topicRecordsConsumedTotal, metricTags)));
			}
			recordsFetched.record(records);
		}

		public void recordFetchLatency(double value) {
			fetchLatency.record(value);
		}
		/*
		 * private void maybeUpdateAssignment(SubscriptionState subscription) { int
		 * newAssignmentId = subscription.assignmentId(); if (this.assignmentId !=
		 * newAssignmentId) { Set<TopicPartition> newAssignedPartitions =
		 * subscription.assignedPartitions(); for (TopicPartition tp :
		 * this.assignedPartitions) { if (!newAssignedPartitions.contains(tp)) {
		 * metrics.removeSensor(partitionLagMetricName(tp));
		 * metrics.removeSensor(partitionLeadMetricName(tp));
		 * metrics.removeMetric(partitionPreferredReadReplicaMetricName(tp)); } }
		 * 
		 * for (TopicPartition tp : newAssignedPartitions) { if
		 * (!this.assignedPartitions.contains(tp)) { MetricName metricName =
		 * partitionPreferredReadReplicaMetricName(tp); if (metrics.metric(metricName)
		 * == null) { metrics.addMetric( metricName, (Gauge<Integer>) (config, now) ->
		 * subscription.preferredReadReplica(tp, 0L).orElse(-1) ); } } }
		 * 
		 * this.assignedPartitions = newAssignedPartitions; this.assignmentId =
		 * newAssignmentId; } }
		 * 
		 * private void recordPartitionLead(TopicPartition tp, long lead) {
		 * this.recordsFetchLead.record(lead);
		 * 
		 * String name = partitionLeadMetricName(tp); Sensor recordsLead =
		 * this.metrics.getSensor(name); if (recordsLead == null) { Map<String, String>
		 * metricTags = topicPartitionTags(tp);
		 * 
		 * recordsLead = this.metrics.sensor(name);
		 * 
		 * recordsLead.add(this.metrics.metricInstance(metricsRegistry.
		 * partitionRecordsLead, metricTags), new Value());
		 * recordsLead.add(this.metrics.metricInstance(metricsRegistry.
		 * partitionRecordsLeadMin, metricTags), new Min());
		 * recordsLead.add(this.metrics.metricInstance(metricsRegistry.
		 * partitionRecordsLeadAvg, metricTags), new Avg()); } recordsLead.record(lead);
		 * }
		 * 
		 * private void recordPartitionLag(TopicPartition tp, long lag) {
		 * this.recordsFetchLag.record(lag);
		 * 
		 * String name = partitionLagMetricName(tp); Sensor recordsLag =
		 * this.metrics.getSensor(name); if (recordsLag == null) { Map<String, String>
		 * metricTags = topicPartitionTags(tp); recordsLag = this.metrics.sensor(name);
		 * 
		 * recordsLag.add(this.metrics.metricInstance(metricsRegistry.
		 * partitionRecordsLag, metricTags), new Value());
		 * recordsLag.add(this.metrics.metricInstance(metricsRegistry.
		 * partitionRecordsLagMax, metricTags), new Max());
		 * recordsLag.add(this.metrics.metricInstance(metricsRegistry.
		 * partitionRecordsLagAvg, metricTags), new Avg()); } recordsLag.record(lag); }
		 * 
		 * private static String partitionLagMetricName(TopicPartition tp) { return tp +
		 * ".records-lag"; }
		 * 
		 * private static String partitionLeadMetricName(TopicPartition tp) { return tp
		 * + ".records-lead"; }
		 * 
		 * private MetricName partitionPreferredReadReplicaMetricName(TopicPartition tp)
		 * { Map<String, String> metricTags = topicPartitionTags(tp); return
		 * this.metrics.metricInstance(metricsRegistry.partitionPreferredReadReplica,
		 * metricTags); }
		 * 
		 * 
		 * private Map<String, String> topicPartitionTags(TopicPartition tp) {
		 * Map<String, String> metricTags = new HashMap<>(2); metricTags.put("topic",
		 * tp.topic().replace('.', '_')); metricTags.put("partition",
		 * String.valueOf(tp.partition())); return metricTags; }
		 */
	}
}
