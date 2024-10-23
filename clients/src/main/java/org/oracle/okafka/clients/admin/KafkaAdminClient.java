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

package org.oracle.okafka.clients.admin;

import org.apache.kafka.clients.ClientDnsLookup;
import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.ClientUtils;
import org.oracle.okafka.clients.admin.AdminClientConfig;
import org.oracle.okafka.clients.admin.KafkaAdminClient.Call;
import org.apache.kafka.clients.admin.AbortTransactionOptions;
import org.apache.kafka.clients.admin.AbortTransactionResult;
import org.apache.kafka.clients.admin.AbortTransactionSpec;
import org.apache.kafka.clients.admin.AlterClientQuotasOptions;
import org.apache.kafka.clients.admin.AlterClientQuotasResult;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsOptions;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.AlterConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.AlterConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.AlterPartitionReassignmentsOptions;
import org.apache.kafka.clients.admin.AlterPartitionReassignmentsResult;
import org.apache.kafka.clients.admin.AlterReplicaLogDirsOptions;
import org.apache.kafka.clients.admin.AlterReplicaLogDirsResult;
import org.apache.kafka.clients.admin.AlterUserScramCredentialsOptions;
import org.apache.kafka.clients.admin.AlterUserScramCredentialsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.CreateAclsOptions;
import org.apache.kafka.clients.admin.CreateAclsResult;
import org.apache.kafka.clients.admin.CreateDelegationTokenOptions;
import org.apache.kafka.clients.admin.CreateDelegationTokenResult;
import org.apache.kafka.clients.admin.CreatePartitionsOptions;
import org.apache.kafka.clients.admin.CreatePartitionsResult;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteAclsOptions;
import org.apache.kafka.clients.admin.DeleteAclsResult;
import org.apache.kafka.clients.admin.DeleteConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.DeleteConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.DeleteConsumerGroupsOptions;
import org.apache.kafka.clients.admin.DeleteConsumerGroupsResult;
import org.apache.kafka.clients.admin.DeleteRecordsOptions;
import org.apache.kafka.clients.admin.DeleteRecordsResult;
import org.apache.kafka.clients.admin.DeleteTopicsOptions;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeAclsOptions;
import org.apache.kafka.clients.admin.DescribeAclsResult;
import org.apache.kafka.clients.admin.DescribeClientQuotasOptions;
import org.apache.kafka.clients.admin.DescribeClientQuotasResult;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeConfigsOptions;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsOptions;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.admin.DescribeDelegationTokenOptions;
import org.apache.kafka.clients.admin.DescribeDelegationTokenResult;
import org.apache.kafka.clients.admin.DescribeFeaturesOptions;
import org.apache.kafka.clients.admin.DescribeFeaturesResult;
import org.apache.kafka.clients.admin.DescribeLogDirsOptions;
import org.apache.kafka.clients.admin.DescribeLogDirsResult;
import org.apache.kafka.clients.admin.DescribeMetadataQuorumOptions;
import org.apache.kafka.clients.admin.DescribeMetadataQuorumResult;
import org.apache.kafka.clients.admin.DescribeProducersOptions;
import org.apache.kafka.clients.admin.DescribeProducersResult;
import org.apache.kafka.clients.admin.DescribeReplicaLogDirsOptions;
import org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.DescribeTransactionsOptions;
import org.apache.kafka.clients.admin.DescribeTransactionsResult;
import org.apache.kafka.clients.admin.DescribeUserScramCredentialsOptions;
import org.apache.kafka.clients.admin.DescribeUserScramCredentialsResult;
import org.apache.kafka.clients.admin.ElectLeadersOptions;
import org.apache.kafka.clients.admin.ElectLeadersResult;
import org.apache.kafka.clients.admin.ExpireDelegationTokenOptions;
import org.apache.kafka.clients.admin.ExpireDelegationTokenResult;
import org.apache.kafka.clients.admin.FeatureUpdate;
import org.apache.kafka.clients.admin.FenceProducersOptions;
import org.apache.kafka.clients.admin.FenceProducersResult;
import org.apache.kafka.clients.admin.ListClientMetricsResourcesOptions;
import org.apache.kafka.clients.admin.ListClientMetricsResourcesResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec;
import org.apache.kafka.clients.admin.ListConsumerGroupsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListPartitionReassignmentsOptions;
import org.apache.kafka.clients.admin.ListPartitionReassignmentsResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.ListTransactionsOptions;
import org.apache.kafka.clients.admin.ListTransactionsResult;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
//import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.admin.RemoveMembersFromConsumerGroupOptions;
import org.apache.kafka.clients.admin.RemoveMembersFromConsumerGroupResult;
import org.apache.kafka.clients.admin.RenewDelegationTokenOptions;
import org.apache.kafka.clients.admin.RenewDelegationTokenResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.clients.admin.UnregisterBrokerOptions;
import org.apache.kafka.clients.admin.UnregisterBrokerResult;
import org.apache.kafka.clients.admin.UpdateFeaturesOptions;
import org.apache.kafka.clients.admin.UpdateFeaturesResult;
import org.apache.kafka.clients.admin.UserScramCredentialAlteration;
import org.apache.kafka.clients.admin.CreateTopicsResult.TopicMetadataAndConfig;
//import org.apache.kafka.clients.admin.KafkaAdminClient.LeastLoadedNodeProvider;
//import org.apache.kafka.clients.admin.KafkaAdminClient.LeastLoadedNodeProvider;
/*
import org.apache.kafka.clients.admin.KafkaAdminClient.Call;
import org.apache.kafka.clients.admin.KafkaAdminClient.ControllerNodeProvider;
import org.apache.kafka.clients.admin.KafkaAdminClient.MetadataUpdateNodeIdProvider;
import org.apache.kafka.clients.admin.KafkaAdminClient.NodeProvider;
import org.apache.kafka.clients.admin.KafkaAdminClient.TimeoutProcessor;
 */
import org.oracle.okafka.clients.CommonClientConfigs;
import org.oracle.okafka.clients.KafkaClient;
import org.oracle.okafka.clients.NetworkClient;
import org.oracle.okafka.clients.TopicTeqParameters;
import org.apache.kafka.clients.admin.internals.AdminMetadataManager;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.oracle.okafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicCollection;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.TopicCollection.TopicIdCollection;
import org.apache.kafka.common.TopicCollection.TopicNameCollection;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.config.ConfigResource;
import org.oracle.okafka.common.config.SslConfigs;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.DisconnectException;
import org.oracle.okafka.common.errors.FeatureNotSupportedException;
import org.oracle.okafka.common.errors.InvalidLoginCredentialsException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnknownTopicIdException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaFilter;
import org.apache.kafka.common.requests.AbstractResponse;
import org.oracle.okafka.common.requests.AbstractRequest;
import org.oracle.okafka.common.requests.CreateTopicsRequest;
import org.oracle.okafka.common.requests.CreateTopicsResponse;
import org.oracle.okafka.common.requests.DeleteTopicsRequest;
import org.oracle.okafka.common.requests.DeleteTopicsResponse;
import org.oracle.okafka.common.requests.MetadataRequest;
import org.oracle.okafka.common.requests.MetadataResponse;
import org.oracle.okafka.common.requests.CreateTopicsRequest.TopicDetails;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.LogContext;
import org.oracle.okafka.common.utils.TNSParser;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.KafkaFuture;
import org.oracle.okafka.clients.admin.internals.AQKafkaAdmin;
import org.oracle.okafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;

import static org.apache.kafka.common.requests.MetadataRequest.convertTopicIdsToMetadataRequestTopic;
import static org.apache.kafka.common.utils.Utils.closeQuietly;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.InetSocketAddress;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

/**
 * The default implementation of {@link AdminClient}. An instance of this class
 * is created by invoking one of the {@code create()} methods in
 * {@code AdminClient}. Users should not refer to this class directly.
 *
 * The API of this class is evolving, see {@link AdminClient} for details. Note:
 * Topic name has to be in uppercase wherever used.
 */
@InterfaceStability.Evolving
public class KafkaAdminClient extends AdminClient {

	/**
	 * The next integer to use to name a KafkaAdminClient which the user hasn't
	 * specified an explicit name for.
	 */
	private static final AtomicInteger ADMIN_CLIENT_ID_SEQUENCE = new AtomicInteger(1);

	/**
	 * The prefix to use for the JMX metrics for this class
	 */
	private static final String JMX_PREFIX = "kafka.admin.client";

	/**
	 * An invalid shutdown time which indicates that a shutdown has not yet been
	 * performed.
	 */
	private static final long INVALID_SHUTDOWN_TIME = -1;

	/**
	 * Thread name prefix for admin client network thread
	 */
	static final String NETWORK_THREAD_PREFIX = "kafka-admin-client-thread";

	private final Logger log;

	/**
	 * The default timeout to use for an operation.
	 */
	private final int defaultTimeoutMs;

	/**
	 * The name of this AdminClient instance.
	 */
	private final String clientId;

	/**
	 * Provides the time.
	 */
	private final Time time;

	/**
	 * The timeout to use for a single request.
	 */
	private final int requestTimeoutMs;

	/**
	 * The cluster metadata manager used by the KafkaClient.
	 */
	private final AdminMetadataManager metadataManager;

	/**
	 * The metrics for this KafkaAdminClient.
	 */
	private final Metrics metrics;

	/**
	 * The network client to use.
	 */
	private final KafkaClient client;

	/**
	 * The runnable used in the service thread for this admin client.
	 */
	private final AdminClientRunnable runnable;

	/**
	 * The network service thread for this admin client.
	 */
	private final Thread thread;

	/**
	 * During a close operation, this is the time at which we will time out all
	 * pending operations and force the RPC thread to exit. If the admin client is
	 * not closing, this will be 0.
	 */
	private final AtomicLong hardShutdownTimeMs = new AtomicLong(INVALID_SHUTDOWN_TIME);

	/**
	 * A factory which creates TimeoutProcessors for the RPC thread.
	 */
	private final TimeoutProcessorFactory timeoutProcessorFactory;

	private final int maxRetries;

	private final long retryBackoffMs;

	/**
	 * Get or create a list value from a map.
	 *
	 * @param map The map to get or create the element from.
	 * @param key The key.
	 * @param <K> The key type.
	 * @param <V> The value type.
	 * @return The list value.
	 */
	static <K, V> List<V> getOrCreateListValue(Map<K, List<V>> map, K key) {
		List<V> list = map.get(key);
		if (list != null)
			return list;
		list = new LinkedList<>();
		map.put(key, list);
		return list;
	}

	/**
	 * Send an exception to every element in a collection of KafkaFutureImpls.
	 *
	 * @param futures The collection of KafkaFutureImpl objects.
	 * @param exc     The exception
	 * @param <T>     The KafkaFutureImpl result type.
	 */
	private static <T> void completeAllExceptionally(Collection<KafkaFutureImpl<T>> futures, Throwable exc) {
		for (KafkaFutureImpl<?> future : futures) {
			future.completeExceptionally(exc);
		}
	}

	/**
	 * Get the current time remaining before a deadline as an integer.
	 *
	 * @param now        The current time in milliseconds.
	 * @param deadlineMs The deadline time in milliseconds.
	 * @return The time delta in milliseconds.
	 */
	static int calcTimeoutMsRemainingAsInt(long now, long deadlineMs) {
		long deltaMs = deadlineMs - now;
		if (deltaMs > Integer.MAX_VALUE)
			deltaMs = Integer.MAX_VALUE;
		else if (deltaMs < Integer.MIN_VALUE)
			deltaMs = Integer.MIN_VALUE;
		return (int) deltaMs;
	}

	/**
	 * Generate the client id based on the configuration.
	 *
	 * @param config The configuration
	 *
	 * @return The client id
	 */
	static String generateClientId(AdminClientConfig config) {
		String clientId = config.getString(AdminClientConfig.CLIENT_ID_CONFIG);
		if (!clientId.isEmpty())
			return clientId;
		return "adminclient-" + ADMIN_CLIENT_ID_SEQUENCE.getAndIncrement();
	}

	/**
	 * Get the deadline for a particular call.
	 *
	 * @param now             The current time in milliseconds.
	 * @param optionTimeoutMs The timeout option given by the user.
	 *
	 * @return The deadline in milliseconds.
	 */
	private long calcDeadlineMs(long now, Integer optionTimeoutMs) {
		if (optionTimeoutMs != null)
			return now + Math.max(0, optionTimeoutMs);
		return now + defaultTimeoutMs;
	}

	/**
	 * Pretty-print an exception.
	 *
	 * @param throwable The exception.
	 *
	 * @return A compact human-readable string.
	 */
	static String prettyPrintException(Throwable throwable) {
		if (throwable == null)
			return "Null exception.";
		if (throwable.getMessage() != null) {
			return throwable.getClass().getSimpleName() + ": " + throwable.getMessage();
		}
		return throwable.getClass().getSimpleName();
	}

	static KafkaAdminClient createInternal(AdminClientConfig config, TimeoutProcessorFactory timeoutProcessorFactory) {
		Metrics metrics = null;
		Time time = Time.SYSTEM;
		String clientId = generateClientId(config);
		LogContext logContext = createLogContext(clientId);
		KafkaClient client = null;
		try {
			AdminMetadataManager metadataManager = new AdminMetadataManager(logContext,
					config.getLong(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG),
					config.getLong(AdminClientConfig.METADATA_MAX_AGE_CONFIG), false);
			List<MetricsReporter> reporters = config
					.getConfiguredInstances(AdminClientConfig.METRIC_REPORTER_CLASSES_CONFIG, MetricsReporter.class);
			Map<String, String> metricTags = Collections.singletonMap("client-id", clientId);
			MetricConfig metricConfig = new MetricConfig()
					.samples(config.getInt(AdminClientConfig.METRICS_NUM_SAMPLES_CONFIG))
					.timeWindow(config.getLong(AdminClientConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG),
							TimeUnit.MILLISECONDS)
					.recordLevel(Sensor.RecordingLevel
							.forName(config.getString(AdminClientConfig.METRICS_RECORDING_LEVEL_CONFIG)))
					.tags(metricTags);
			reporters.add(new JmxReporter(JMX_PREFIX));
			metrics = new Metrics(metricConfig, reporters, time);

			AQKafkaAdmin admin = new AQKafkaAdmin(logContext, config, metadataManager, time);

			client = new NetworkClient(admin, metadataManager, clientId,
					config.getLong(AdminClientConfig.RECONNECT_BACKOFF_MS_CONFIG),
					config.getLong(AdminClientConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG),
					config.getInt(AdminClientConfig.SEND_BUFFER_CONFIG),
					config.getInt(AdminClientConfig.RECEIVE_BUFFER_CONFIG), (int) TimeUnit.HOURS.toMillis(1), time,
					logContext);
			return new KafkaAdminClient(config, clientId, time, metadataManager, metrics, client,
					timeoutProcessorFactory, logContext);
		} catch (Throwable exc) {
			closeQuietly(metrics, "Metrics");
			closeQuietly(client, "NetworkClient");
			throw new KafkaException("Failed create new KafkaAdminClient", exc);
		}
	}

	static KafkaAdminClient createInternal(AdminClientConfig config, KafkaClient client, Time time) {
		Metrics metrics = null;
		String clientId = generateClientId(config);

		try {
			metrics = new Metrics(new MetricConfig(), new LinkedList<MetricsReporter>(), time);
			LogContext logContext = createLogContext(clientId);
			AdminMetadataManager metadataManager = new AdminMetadataManager(logContext,
					config.getLong(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG),
					config.getLong(AdminClientConfig.METADATA_MAX_AGE_CONFIG), false);
			return new KafkaAdminClient(config, clientId, time, metadataManager, metrics, client, null, logContext);
		} catch (Throwable exc) {
			closeQuietly(metrics, "Metrics");
			throw new KafkaException("Failed create new KafkaAdminClient", exc);
		}
	}

	static LogContext createLogContext(String clientId) {
		return new LogContext("[AdminClient clientId=" + clientId + "] ");
	}

	private KafkaAdminClient(AdminClientConfig config, String clientId, Time time, AdminMetadataManager metadataManager,
			Metrics metrics, KafkaClient client, TimeoutProcessorFactory timeoutProcessorFactory, LogContext logContext)
			throws Exception {
		this.defaultTimeoutMs = config.getInt(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG);
		this.clientId = clientId;
		this.log = logContext.logger(KafkaAdminClient.class);
		this.time = time;
		this.metadataManager = metadataManager;
		this.requestTimeoutMs = config.getInt(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG);

		List<InetSocketAddress> addresses = null;
		String serviceName = null;
		String instanceName = null;
		System.setProperty("oracle.net.tns_admin", config.getString(ProducerConfig.ORACLE_NET_TNS_ADMIN));

		if (config.getString(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG).trim().equalsIgnoreCase("PLAINTEXT")) {

			addresses = ClientUtils.parseAndValidateAddresses(config.getList(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG),
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

		{ // Changes for 2.8.1 :: Create Bootstrap Cluster and pass it to metadata.update
			// We must have OKafka Node with Service Name and Instance Name placed in the
			// bootstrap cluster.
			// For cluster created here, isBootstrapConfigured is not set to TRUE because it
			// is not public

			ArrayList<Node> bootStrapNodeList = new ArrayList<Node>(addresses.size());
			int id = -1;
			for (InetSocketAddress inetAddr : addresses) {
				org.oracle.okafka.common.Node bootStrapNode = new org.oracle.okafka.common.Node(id--,
						inetAddr.getHostName(), inetAddr.getPort(), serviceName, instanceName);
				bootStrapNodeList.add((Node) bootStrapNode);
			}
			Cluster bootStrapCluster = new Cluster(null, bootStrapNodeList, new ArrayList<>(0), Collections.emptySet(),
					Collections.emptySet());

			this.metadataManager.update(bootStrapCluster, time.milliseconds());
		}

		// metadataManager.update(Cluster.bootstrap(addresses, config, serviceName,
		// instanceName), time.milliseconds());

		this.metrics = metrics;
		this.client = client;
		this.runnable = new AdminClientRunnable();
		String threadName = NETWORK_THREAD_PREFIX + " | " + clientId;
		this.thread = new KafkaThread(threadName, runnable, true);
		this.timeoutProcessorFactory = (timeoutProcessorFactory == null) ? new TimeoutProcessorFactory()
				: timeoutProcessorFactory;
		this.maxRetries = config.getInt(AdminClientConfig.RETRIES_CONFIG);
		this.retryBackoffMs = config.getLong(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG);
		config.logUnused();
		AppInfoParser.registerAppInfo(JMX_PREFIX, clientId, metrics, time.milliseconds());
		log.debug("Kafka admin client initialized");
		thread.start();
	}

	Time time() {
		return time;
	}

	@Override
	public void close(long duration, TimeUnit unit) {
		long waitTimeMs = unit.toMillis(duration);
		waitTimeMs = Math.min(TimeUnit.DAYS.toMillis(365), waitTimeMs); // Limit the timeout to a year.
		long now = time.milliseconds();
		long newHardShutdownTimeMs = now + waitTimeMs;
		long prev = INVALID_SHUTDOWN_TIME;
		while (true) {
			if (hardShutdownTimeMs.compareAndSet(prev, newHardShutdownTimeMs)) {
				if (prev == INVALID_SHUTDOWN_TIME) {
					log.info("Initiating close operation.");
				} else {
					log.info("Moving hard shutdown time forward.");
				}
				break;
			}
			prev = hardShutdownTimeMs.get();
			if (prev < newHardShutdownTimeMs) {
				log.info("Hard shutdown time is already earlier than requested.");
				newHardShutdownTimeMs = prev;
				break;
			}
		}
		if (log.isDebugEnabled()) {
			long deltaMs = Math.max(0, newHardShutdownTimeMs - time.milliseconds());
			log.info("Waiting for the I/O thread to exit. Hard shutdown in {} ms.", deltaMs);
		}
		try {
			// Wait for the thread to be joined.
			thread.join();

			AppInfoParser.unregisterAppInfo(JMX_PREFIX, clientId, metrics);

			log.debug("Kafka admin client closed.");
		} catch (InterruptedException e) {
			log.debug("Interrupted while joining I/O thread", e);
			Thread.currentThread().interrupt();
		}
	}

	/**
	 * An interface for providing a node for a call.
	 */
	private interface NodeProvider {
		Node provide();
	}

	private class MetadataUpdateNodeIdProvider implements NodeProvider {
		@Override
		public Node provide() {
			return client.leastLoadedNode(time.milliseconds());
		}
	}

	private class ConstantNodeIdProvider implements NodeProvider {
		private final int nodeId;

		ConstantNodeIdProvider(int nodeId) {
			this.nodeId = nodeId;
		}

		@Override
		public Node provide() {
			if (metadataManager.isReady() && (metadataManager.nodeById(nodeId) != null)) {
				return metadataManager.nodeById(nodeId);
			}
			// If we can't find the node with the given constant ID, we schedule a
			// metadata update and hope it appears. This behavior is useful for avoiding
			// flaky behavior in tests when the cluster is starting up and not all nodes
			// have appeared.
			metadataManager.requestUpdate();
			return null;
		}
	}

	/**
	 * Provides the controller node.
	 */
	private class ControllerNodeProvider implements NodeProvider {
		@Override
		public Node provide() {
			if (metadataManager.isReady() && (metadataManager.controller() != null)) {
				return metadataManager.controller();
			}
			metadataManager.requestUpdate();
			return null;
		}
	}

	/**
	 * Provides the least loaded node.
	 */
	private class LeastLoadedNodeProvider implements NodeProvider {
		@Override
		public Node provide() {
			if (metadataManager.isReady()) {
				// This may return null if all nodes are busy.
				// In that case, we will postpone node assignment.
				return client.leastLoadedNode(time.milliseconds());
			}
			metadataManager.requestUpdate();
			return null;
		}
	}

	abstract class Call {
		private final boolean internal;
		private final String callName;
		private final long deadlineMs;
		private final NodeProvider nodeProvider;
		private int tries = 0;
		private boolean aborted = false;
		private Node curNode = null;
		private long nextAllowedTryMs = 0;

		Call(boolean internal, String callName, long deadlineMs, NodeProvider nodeProvider) {
			this.internal = internal;
			this.callName = callName;
			this.deadlineMs = deadlineMs;
			this.nodeProvider = nodeProvider;
		}

		Call(String callName, long deadlineMs, NodeProvider nodeProvider) {
			this(false, callName, deadlineMs, nodeProvider);
		}

		protected Node curNode() {
			return curNode;
		}

		/**
		 * Handle a failure.
		 *
		 * Depending on what the exception is and how many times we have already tried,
		 * we may choose to fail the Call, or retry it. It is important to print the
		 * stack traces here in some cases, since they are not necessarily preserved in
		 * ApiVersionException objects.
		 *
		 * @param now       The current time in milliseconds.
		 * @param throwable The failure exception.
		 */
		final void fail(long now, Throwable throwable) {
			if (aborted) {
				// If the call was aborted while in flight due to a timeout, deliver a
				// TimeoutException. In this case, we do not get any more retries - the call has
				// failed. We increment tries anyway in order to display an accurate log
				// message.
				tries++;
				failWithTimeout(now, throwable);
				return;
			}
			// If this is an UnsupportedVersionException that we can retry, do so. Note that
			// a
			// protocol downgrade will not count against the total number of retries we get
			// for
			// this RPC. That is why 'tries' is not incremented.
			if ((throwable instanceof UnsupportedVersionException)
					&& handleUnsupportedVersionException((UnsupportedVersionException) throwable)) {
				log.debug("{} attempting protocol downgrade and then retry.", this);
				runnable.enqueue(this, now);
				return;
			}
			tries++;
			nextAllowedTryMs = now + retryBackoffMs;

			// If the call has timed out, fail.
			if (calcTimeoutMsRemainingAsInt(now, deadlineMs) < 0) {
				failWithTimeout(now, throwable);
				return;
			}
			// If the exception is not retriable, fail.
			if (!(throwable instanceof RetriableException)) {
				if (log.isDebugEnabled()) {
					log.debug("{} failed with non-retriable exception after {} attempt(s)", this, tries,
							new Exception(prettyPrintException(throwable)));
				}
				handleFailure(throwable);
				return;
			}
			// If we are out of retries, fail.
			if (tries > maxRetries) {
				failWithTimeout(now, throwable);
				return;
			}
			if (log.isDebugEnabled()) {
				log.debug("{} failed: {}. Beginning retry #{}", this, prettyPrintException(throwable), tries);
			}
			runnable.enqueue(this, now);
		}

		private void failWithTimeout(long now, Throwable cause) {
			if (log.isDebugEnabled()) {
				log.debug("{} timed out at {} after {} attempt(s)", this, now, tries,
						new Exception(prettyPrintException(cause)));
			}
			handleFailure(
					new TimeoutException(this + " timed out at " + now + " after " + tries + " attempt(s)", cause));
		}

		/**
		 * Create an AbstractRequest.Builder for this Call.
		 *
		 * @param timeoutMs The timeout in milliseconds.
		 *
		 * @return The AbstractRequest builder.
		 */
		@SuppressWarnings("rawtypes")
		abstract AbstractRequest.Builder createRequest(int timeoutMs);

		/**
		 * Process the call response.
		 *
		 * @param abstractResponse The AbstractResponse.
		 *
		 */
		abstract void handleResponse(org.apache.kafka.common.requests.AbstractResponse abstractResponse);

		/**
		 * Handle a failure. This will only be called if the failure exception was not
		 * retriable, or if we hit a timeout.
		 *
		 * @param throwable The exception.
		 */
		abstract void handleFailure(Throwable throwable);

		/**
		 * Handle an UnsupportedVersionException.
		 *
		 * @param exception The exception.
		 *
		 * @return True if the exception can be handled; false otherwise.
		 */
		boolean handleUnsupportedVersionException(UnsupportedVersionException exception) {
			return false;
		}

		@Override
		public String toString() {
			return "Call(callName=" + callName + ", deadlineMs=" + deadlineMs + ", tries=" + tries
					+ ", nextAllowedTryMs=" + nextAllowedTryMs + ")";
		}

		public boolean isInternal() {
			return internal;
		}
	}

	static class TimeoutProcessorFactory {
		TimeoutProcessor create(long now) {
			return new TimeoutProcessor(now);
		}
	}

	static class TimeoutProcessor {
		/**
		 * The current time in milliseconds.
		 */
		private final long now;

		/**
		 * The number of milliseconds until the next timeout.
		 */
		private int nextTimeoutMs;

		/**
		 * Create a new timeout processor.
		 *
		 * @param now The current time in milliseconds since the epoch.
		 */
		TimeoutProcessor(long now) {
			this.now = now;
			this.nextTimeoutMs = Integer.MAX_VALUE;
		}

		/**
		 * Check for calls which have timed out. Timed out calls will be removed and
		 * failed. The remaining milliseconds until the next timeout will be updated.
		 *
		 * @param calls The collection of calls.
		 *
		 * @return The number of calls which were timed out.
		 */
		int handleTimeouts(Collection<Call> calls, String msg) {
			int numTimedOut = 0;
			for (Iterator<Call> iter = calls.iterator(); iter.hasNext();) {
				Call call = iter.next();
				int remainingMs = calcTimeoutMsRemainingAsInt(now, call.deadlineMs);
				if (remainingMs < 0) {
					call.fail(now, new TimeoutException(msg + " Call: " + call.callName));
					iter.remove();
					numTimedOut++;
				} else {
					nextTimeoutMs = Math.min(nextTimeoutMs, remainingMs);
				}
			}
			return numTimedOut;
		}

		/**
		 * Check whether a call should be timed out. The remaining milliseconds until
		 * the next timeout will be updated.
		 *
		 * @param call The call.
		 *
		 * @return True if the call should be timed out.
		 */
		boolean callHasExpired(Call call) {
			int remainingMs = calcTimeoutMsRemainingAsInt(now, call.deadlineMs);
			if (remainingMs < 0)
				return true;
			nextTimeoutMs = Math.min(nextTimeoutMs, remainingMs);
			return false;
		}

		int nextTimeoutMs() {
			return nextTimeoutMs;
		}
	}

	private final class AdminClientRunnable implements Runnable {
		/**
		 * Calls which have not yet been assigned to a node. Only accessed from this
		 * thread.
		 */
		private final ArrayList<Call> pendingCalls = new ArrayList<>();

		/**
		 * Maps nodes to calls that we want to send. Only accessed from this thread.
		 */
		private final Map<Node, List<Call>> callsToSend = new HashMap<>();

		/**
		 * Maps node ID strings to calls that have been sent. Only accessed from this
		 * thread.
		 */
		private final Map<String, List<Call>> callsInFlight = new HashMap<>();

		/**
		 * Maps correlation IDs to calls that have been sent. Only accessed from this
		 * thread.
		 */
		private final Map<Integer, Call> correlationIdToCalls = new HashMap<>();

		/**
		 * Pending calls. Protected by the object monitor. This will be null only if the
		 * thread has shut down.
		 */
		private List<Call> newCalls = new LinkedList<>();

		/**
		 * Time out the elements in the pendingCalls list which are expired.
		 *
		 * @param processor The timeout processor.
		 */
		private void timeoutPendingCalls(TimeoutProcessor processor) {
			int numTimedOut = processor.handleTimeouts(pendingCalls, "Timed out waiting for a node assignment.");
			if (numTimedOut > 0)
				log.debug("Timed out {} pending calls.", numTimedOut);
		}

		/**
		 * Time out calls which have been assigned to nodes.
		 *
		 * @param processor The timeout processor.
		 */
		private int timeoutCallsToSend(TimeoutProcessor processor) {
			int numTimedOut = 0;
			for (List<Call> callList : callsToSend.values()) {
				numTimedOut += processor.handleTimeouts(callList, "Timed out waiting to send the call.");
			}
			if (numTimedOut > 0)
				log.debug("Timed out {} call(s) with assigned nodes.", numTimedOut);
			return numTimedOut;
		}

		/**
		 * Drain all the calls from newCalls into pendingCalls.
		 *
		 * This function holds the lock for the minimum amount of time, to avoid
		 * blocking users of AdminClient who will also take the lock to add new calls.
		 */
		private synchronized void drainNewCalls() {
			if (!newCalls.isEmpty()) {
				pendingCalls.addAll(newCalls);
				newCalls.clear();
			}
		}

		/**
		 * Choose nodes for the calls in the pendingCalls list.
		 *
		 * @param now The current time in milliseconds.
		 * @return The minimum time until a call is ready to be retried if any of the
		 *         pending calls are backing off after a failure
		 */
		private long maybeDrainPendingCalls(long now) {
			long pollTimeout = Long.MAX_VALUE;
			log.trace("Trying to choose nodes for {} at {}", pendingCalls, now);

			Iterator<Call> pendingIter = pendingCalls.iterator();
			while (pendingIter.hasNext()) {
				Call call = pendingIter.next();

				// If the call is being retried, await the proper backoff before finding the
				// node
				if (now < call.nextAllowedTryMs) {
					pollTimeout = Math.min(pollTimeout, call.nextAllowedTryMs - now);
				} else if (maybeDrainPendingCall(call, now)) {
					pendingIter.remove();
				}
			}
			return pollTimeout;
		}

		/**
		 * Check whether a pending call can be assigned a node. Return true if the
		 * pending call was either transferred to the callsToSend collection or if the
		 * call was failed. Return false if it should remain pending.
		 */
		private boolean maybeDrainPendingCall(Call call, long now) {
			try {
				Node node = call.nodeProvider.provide();
				if (node != null) {
					log.trace("Assigned {} to node {}", call, node);
					call.curNode = node;
					getOrCreateListValue(callsToSend, node).add(call);
					return true;
				} else {
					log.trace("Unable to assign {} to a node.", call);
					return false;
				}
			} catch (Throwable t) {
				// Handle authentication errors while choosing nodes.
				log.debug("Unable to choose node for {}", call, t);
				call.fail(now, t);
				return true;
			}
		}

		/**
		 * Send the calls which are ready.
		 *
		 * @param now The current time in milliseconds.
		 * @return The minimum timeout we need for poll().
		 */
		private long sendEligibleCalls(long now) {
			
			long pollTimeout = Long.MAX_VALUE;
			for (Iterator<Map.Entry<Node, List<Call>>> iter = callsToSend.entrySet().iterator(); iter.hasNext();) {
				Map.Entry<Node, List<Call>> entry = iter.next();
				List<Call> calls = entry.getValue();
				if (calls.isEmpty()) {
					iter.remove();
					continue;
				}
				Node node = entry.getKey();
				if (!client.ready((org.oracle.okafka.common.Node) node, now)) {
					long nodeTimeout = client.pollDelayMs((org.oracle.okafka.common.Node) node, now);
					pollTimeout = Math.min(pollTimeout, nodeTimeout);
					log.trace("Client is not ready to send to {}. Must delay {} ms", node, nodeTimeout);
					continue;
				}
				Call call = calls.remove(0);
				int requestTimeoutMs = Math.min(KafkaAdminClient.this.requestTimeoutMs,
						calcTimeoutMsRemainingAsInt(now, call.deadlineMs));
				AbstractRequest.Builder<?> requestBuilder;
				try {
					requestBuilder = call.createRequest(requestTimeoutMs);
				} catch (Throwable throwable) {
					call.fail(now,
							new KafkaException(String.format("Internal error sending %s to %s.", call.callName, node)));
					continue;
				}

				ClientRequest clientRequest = client.newClientRequest((org.oracle.okafka.common.Node) node,
						requestBuilder, now, true, requestTimeoutMs, null);
				log.info("Sending {} to {}. correlationId={}", requestBuilder, node, clientRequest.correlationId());
				ClientResponse response = client.send(clientRequest, now);

				getOrCreateListValue(callsInFlight, node.idString()).add(call);
				correlationIdToCalls.put(clientRequest.correlationId(), call);

				log.trace("Received response for {} from {}. correlationId={}", requestBuilder, node,
						response.requestHeader().correlationId());
				handleResponse(time.milliseconds(), call, response);
				correlationIdToCalls.remove(clientRequest.correlationId());

			}
			
			return pollTimeout;
		}

		/**
		 * Handle responses from the server.
		 *
		 * @param now       The current time in milliseconds.
		 * @param responses The latest responses from KafkaClient.
		 **/
		private void handleResponse(long now, Call call, ClientResponse response) {

			try {
				if (response.wasDisconnected()) {
					client.disconnected((org.oracle.okafka.common.Node) (metadataManager
							.nodeById(Integer.parseInt(response.destination()))), now);
					metadataManager.requestUpdate();

				}
				call.handleResponse(response.responseBody());
			} catch (Throwable t) {
				if (log.isTraceEnabled())
					log.trace("{} handleResponse failed with {}", call, prettyPrintException(t));
				call.fail(now, t);
			}
		}

		/**
		 * Time out expired calls that are in flight.
		 *
		 * Calls that are in flight may have been partially or completely sent over the
		 * wire. They may even be in the process of being processed by the remote
		 * server. At the moment, our only option to time them out is to close the
		 * entire connection.
		 *
		 * @param processor The timeout processor.
		 */
		private void timeoutCallsInFlight(TimeoutProcessor processor) {
			int numTimedOut = 0;
			for (Map.Entry<String, List<Call>> entry : callsInFlight.entrySet()) {
				List<Call> contexts = entry.getValue();
				if (contexts.isEmpty())
					continue;
				String nodeId = entry.getKey();
				// We assume that the first element in the list is the earliest. So it should be
				// the
				// only one we need to check the timeout for.
				Call call = contexts.get(0);
				if (processor.callHasExpired(call)) {
					if (call.aborted) {
						log.warn("Aborted call {} is still in callsInFlight.", call);
					} else {
						log.debug("Closing connection to {} to time out {}", nodeId, call);
						call.aborted = true;
						client.disconnect(
								(org.oracle.okafka.common.Node) metadataManager.nodeById(Integer.parseInt(nodeId)));
						numTimedOut++;
						// We don't remove anything from the callsInFlight data structure. Because the
						// connection
						// has been closed, the calls should be returned by the next client#poll(),
						// and handled at that point.
					}
				}
			}
			if (numTimedOut > 0)
				log.debug("Timed out {} call(s) in flight.", numTimedOut);
		}

		/**
		 * Handle responses from the server.
		 *
		 * @param now       The current time in milliseconds.
		 * @param responses The latest responses from KafkaClient.
		 **/
		private void handleResponses(long now, List<ClientResponse> responses) {
			for (ClientResponse response : responses) {
				int correlationId = response.requestHeader().correlationId();

				Call call = correlationIdToCalls.get(correlationId);
				if (call == null) {
					// If the server returns information about a correlation ID we didn't use yet,
					// an internal server error has occurred. Close the connection and log an error
					// message.
					log.error(
							"Internal server error on {}: server returned information about unknown "
									+ "correlation ID {}, requestHeader = {}",
							response.destination(), correlationId, response.requestHeader());
					client.disconnect((org.oracle.okafka.common.Node) metadataManager
							.nodeById(Integer.parseInt(response.destination())));
					continue;
				}

				// Stop tracking this call.
				correlationIdToCalls.remove(correlationId);
				List<Call> calls = callsInFlight.get(response.destination());
				if ((calls == null) || (!calls.remove(call))) {
					log.error("Internal server error on {}: ignoring call {} in correlationIdToCall "
							+ "that did not exist in callsInFlight", response.destination(), call);
					continue;
				}

				// Handle the result of the call. This may involve retrying the call, if we got
				// a
				// retriable exception.
				if (response.versionMismatch() != null) {
					call.fail(now, response.versionMismatch());
				} else if (response.wasDisconnected()) {
					AuthenticationException authException = client
							.authenticationException((org.oracle.okafka.common.Node) call.curNode());
					if (authException != null) {
						call.fail(now, authException);
					} else {
						call.fail(now,
								new DisconnectException(String.format(
										"Cancelled %s request with correlation id %s due to node %s being disconnected",
										call.callName, correlationId, response.destination())));
					}
					metadataManager.requestUpdate();
				} else {
					try {
						call.handleResponse(response.responseBody());
						if (log.isTraceEnabled())
							log.trace("{} got response {}", call, response.responseBody());
					} catch (Throwable t) {
						if (log.isTraceEnabled())
							log.trace("{} handleResponse failed with {}", call, prettyPrintException(t));
						call.fail(now, t);
					}
				}
			}
		}

		/**
		 * Unassign calls that have not yet been sent based on some predicate. For
		 * example, this is used to reassign the calls that have been assigned to a
		 * disconnected node.
		 *
		 * @param shouldUnassign Condition for reassignment. If the predicate is true,
		 *                       then the calls will be put back in the pendingCalls
		 *                       collection and they will be reassigned
		 */
		private void unassignUnsentCalls(Predicate<org.oracle.okafka.common.Node> shouldUnassign) {
			for (Iterator<Map.Entry<Node, List<Call>>> iter = callsToSend.entrySet().iterator(); iter.hasNext();) {
				Map.Entry<Node, List<Call>> entry = iter.next();
				org.oracle.okafka.common.Node node = (org.oracle.okafka.common.Node) entry.getKey();
				List<Call> awaitingCalls = entry.getValue();

				if (awaitingCalls.isEmpty()) {
					iter.remove();
				} else if (shouldUnassign.test(node)) {
					pendingCalls.addAll(awaitingCalls);
					iter.remove();
				}
			}
		}

		private boolean hasActiveExternalCalls(Collection<Call> calls) {
			for (Call call : calls) {
				if (!call.isInternal()) {
					return true;
				}
			}
			return false;
		}

		/**
		 * Return true if there are currently active external calls.
		 */
		private boolean hasActiveExternalCalls() {
			if (hasActiveExternalCalls(pendingCalls)) {
				return true;
			}
			for (List<Call> callList : callsToSend.values()) {
				if (hasActiveExternalCalls(callList)) {
					return true;
				}
			}
			return hasActiveExternalCalls(correlationIdToCalls.values());
		}

		private boolean threadShouldExit(long now, long curHardShutdownTimeMs) {
			boolean activeExtrnalCalls = hasActiveExternalCalls();
			if (!activeExtrnalCalls) {
				log.debug("All work has been completed, and the I/O thread is now exiting.");
				return true;
			}
			if (now >= curHardShutdownTimeMs) {
				log.debug("Forcing a hard I/O thread shutdown. Requests in progress will be aborted.");
				return true;
			}
			log.debug("Hard shutdown in {} ms.", curHardShutdownTimeMs - now);
			return false;
		}

		@Override
		public void run() {
			log.trace("Thread starting");
			try {
				processRequests();
			} finally {
				AppInfoParser.unregisterAppInfo(JMX_PREFIX, clientId, metrics);

				int numTimedOut = 0;
				TimeoutProcessor timeoutProcessor = new TimeoutProcessor(Long.MAX_VALUE);
				synchronized (this) {
					numTimedOut += timeoutProcessor.handleTimeouts(newCalls, "The AdminClient thread has exited.");
					newCalls = null;
				}
				numTimedOut += timeoutProcessor.handleTimeouts(pendingCalls, "The AdminClient thread has exited.");
				numTimedOut += timeoutCallsToSend(timeoutProcessor);
				numTimedOut += timeoutProcessor.handleTimeouts(correlationIdToCalls.values(),
						"The AdminClient thread has exited.");
				if (numTimedOut > 0) {
					log.debug("Timed out {} remaining operation(s).", numTimedOut);
				}
				closeQuietly(client, "KafkaClient");
				closeQuietly(metrics, "Metrics");
				log.debug("Exiting AdminClientRunnable thread.");
			}
		}

		private void processRequests() {
			long now = time.milliseconds();
			while (true) {
				// Copy newCalls into pendingCalls.
				drainNewCalls();

				// Check if the AdminClient thread should shut down.
				long curHardShutdownTimeMs = hardShutdownTimeMs.get();
				if (curHardShutdownTimeMs != INVALID_SHUTDOWN_TIME) {
					if (threadShouldExit(now, curHardShutdownTimeMs)) {
						break;
					}
				}

				// Handle timeouts.
				TimeoutProcessor timeoutProcessor = timeoutProcessorFactory.create(now);
				timeoutPendingCalls(timeoutProcessor);
				timeoutCallsToSend(timeoutProcessor);
				timeoutCallsInFlight(timeoutProcessor);

				long pollTimeout = Math.min(1200000, timeoutProcessor.nextTimeoutMs());
				if (curHardShutdownTimeMs != INVALID_SHUTDOWN_TIME) {
					pollTimeout = Math.min(pollTimeout, curHardShutdownTimeMs - now);
				}

				// Choose nodes for our pending calls.
				pollTimeout = Math.min(pollTimeout, maybeDrainPendingCalls(now));
				long metadataFetchDelayMs = metadataManager.metadataFetchDelayMs(now);
				if (metadataFetchDelayMs == 0) {
					metadataManager.transitionToUpdatePending(now);
					Call metadataCall = makeMetadataCall(now);
					// Create a new metadata fetch call and add it to the end of pendingCalls.
					// Assign a node for just the new call (we handled the other pending nodes
					// above).

					if (!maybeDrainPendingCall(metadataCall, now))
						pendingCalls.add(metadataCall);
				}
				
				pollTimeout = Math.min(pollTimeout, sendEligibleCalls(now));

				if (metadataFetchDelayMs > 0) {
					pollTimeout = Math.min(pollTimeout, metadataFetchDelayMs);
				}
				// Ensure that we use a small poll timeout if there are pending calls which need
				// to be sent
				if (!pendingCalls.isEmpty())
					pollTimeout = Math.min(pollTimeout, retryBackoffMs);

				/*
				 * OKafka uses synchronous network I/O. No need to wait // Wait for network
				 * responses. log.trace("Entering KafkaClient#poll(timeout={})", pollTimeout);
				 * List<ClientResponse> responses = client.poll(pollTimeout, now);
				 * log.trace("KafkaClient#poll retrieved {} response(s)", responses.size());
				 */
				// unassign calls to disconnected nodes
				unassignUnsentCalls(client::connectionFailed);

				/*
				 * For OKafka Response is handled in sendEligibleCalls itself // Update the
				 * current time and handle the latest responses. now = time.milliseconds();
				 * handleResponses(now, responses);
				 */
			}
		}

		/**
		 * Queue a call for sending.
		 *
		 * If the AdminClient thread has exited, this will fail. Otherwise, it will
		 * succeed (even if the AdminClient is shutting down). This function should
		 * called when retrying an existing call.
		 *
		 * @param call The new call object.
		 * @param now  The current time in milliseconds.
		 */
		void enqueue(Call call, long now) {
			if (call.tries > maxRetries) {
				log.debug("Max retries {} for {} reached", maxRetries, call);
				call.fail(time.milliseconds(), new TimeoutException());
				return;
			}
			if (log.isDebugEnabled()) {
				log.debug("Queueing {} with a timeout {} ms from now.", call, call.deadlineMs - now);
			}
			boolean accepted = false;
			synchronized (this) {
				if (newCalls != null) {
					newCalls.add(call);
					accepted = true;
				}
			}
			if (accepted) {
				client.wakeup(); // wake the thread if it is in poll()
			} else {
				log.debug("The AdminClient thread has exited. Timing out {}.", call);
				call.fail(Long.MAX_VALUE, new TimeoutException("The AdminClient thread has exited."));
			}
		}

		/**
		 * Initiate a new call.
		 *
		 * This will fail if the AdminClient is scheduled to shut down.
		 *
		 * @param call The new call object.
		 * @param now  The current time in milliseconds.
		 */
		void call(Call call, long now) {
			if (hardShutdownTimeMs.get() != INVALID_SHUTDOWN_TIME) {
				log.debug("The AdminClient is not accepting new calls. Timing out {}.", call);
				call.fail(Long.MAX_VALUE, new TimeoutException("The AdminClient thread is not accepting new calls."));
			} else {
				enqueue(call, now);
			}
		}

		/**
		 * Create a new metadata call.
		 */
		private Call makeMetadataCall(long now) {
			return new Call(true, "fetchMetadata", calcDeadlineMs(now, requestTimeoutMs),
					new MetadataUpdateNodeIdProvider()) {
				@Override
				public MetadataRequest.Builder createRequest(int timeoutMs) {
					// Since this only requests node information, it's safe to pass true
					// for allowAutoTopicCreation (and it simplifies communication with
					// older brokers)
					/*
					 * return new MetadataRequest.Builder(new MetadataRequestData()
					 * .setTopics(Collections.emptyList()) .setAllowAutoTopicCreation(true));
					 */
					return new MetadataRequest.Builder(Collections.emptyList(), true, Collections.emptyList());
				}

				@Override
				public void handleResponse(org.apache.kafka.common.requests.AbstractResponse abstractResponse) {
					MetadataResponse response = (MetadataResponse) abstractResponse;
					long now = time.milliseconds();
					metadataManager.update(response.cluster(), now);

					// Unassign all unsent requests after a metadata refresh to allow for a new
					// destination to be selected from the new metadata
					unassignUnsentCalls(node -> true);
				}

				@Override
				public void handleFailure(Throwable e) {
					metadataManager.updateFailed(e);
				}
			};
		}
	}

	private static boolean topicNameIsUnrepresentable(String topicName) {
		return topicName == null || topicName.isEmpty();
	}

	private static boolean groupIdIsUnrepresentable(String groupId) {
		return groupId == null;
	}
	/*
	 * @Override public CreateTopicsResult createTopics(final Collection<NewTopic>
	 * newTopics, final CreateTopicsOptions options) { final Map<String,
	 * KafkaFutureImpl<TopicMetadataAndConfig>> topicFutures = new
	 * HashMap<>(newTopics.size()); final CreatableTopicCollection topics = new
	 * CreatableTopicCollection(); for (NewTopic newTopic : newTopics) { if
	 * (topicNameIsUnrepresentable(newTopic.name())) {
	 * KafkaFutureImpl<TopicMetadataAndConfig> future = new KafkaFutureImpl<>();
	 * future.completeExceptionally(new
	 * InvalidTopicException("The given topic name '" + newTopic.name() +
	 * "' cannot be represented in a request.")); topicFutures.put(newTopic.name(),
	 * future); } else if (!topicFutures.containsKey(newTopic.name())) {
	 * topicFutures.put(newTopic.name(), new KafkaFutureImpl<>());
	 * 
	 * // Copied logic from convertToCreatableTopic of NewTopic CreatableTopic
	 * creatableTopic = new CreatableTopic(). setName(newTopic.name()).
	 * setNumPartitions(newTopic.numPartitions()).
	 * setReplicationFactor(newTopic.replicationFactor());
	 * 
	 * if (newTopic.configs() != null) { for (Entry<String, String> entry :
	 * newTopic.configs().entrySet()) { creatableTopic.configs().add( new
	 * CreateableTopicConfig(). setName(entry.getKey()).
	 * setValue(entry.getValue())); } }
	 * 
	 * topics.add(creatableTopic); } } if (!topics.isEmpty()) { final long now =
	 * time.milliseconds(); final long deadline = calcDeadlineMs(now,
	 * options.timeoutMs()); final Call call = getCreateTopicsCall(options,
	 * topicFutures, topics, Collections.emptyMap(), now, deadline);
	 * runnable.call(call, now); } return new
	 * org.oracle.okafka.clients.admin.CreateTopicsResult(new
	 * HashMap<>(topicFutures)); }
	 * 
	 * private Call getCreateTopicsCall(final CreateTopicsOptions options, final
	 * Map<String, KafkaFutureImpl<TopicMetadataAndConfig>> futures, final
	 * CreatableTopicCollection topics, final Map<String,
	 * ThrottlingQuotaExceededException> quotaExceededExceptions, final long now,
	 * final long deadline) { return new Call("createTopics", deadline, new
	 * ControllerNodeProvider()) {
	 * 
	 * @Override public CreateTopicsRequest.Builder createRequest(int timeoutMs) {
	 * return new CreateTopicsRequest.Builder( new CreateTopicsRequestData()
	 * .setTopics(topics) .setTimeoutMs(timeoutMs)
	 * .setValidateOnly(options.shouldValidateOnly())); }
	 * 
	 * @Override public void
	 * handleResponse(org.apache.kafka.common.requests.AbstractResponse
	 * abstractResponse) { // Check for controller change
	 * handleNotControllerError(abstractResponse); // Handle server responses for
	 * particular topics. final CreateTopicsResponse response =
	 * (CreateTopicsResponse) abstractResponse; final CreatableTopicCollection
	 * retryTopics = new CreatableTopicCollection(); final Map<String,
	 * ThrottlingQuotaExceededException> retryTopicQuotaExceededExceptions = new
	 * HashMap<>(); for (CreatableTopicResult result : response.data().topics()) {
	 * KafkaFutureImpl<TopicMetadataAndConfig> future = futures.get(result.name());
	 * if (future == null) { log.warn("Server response mentioned unknown topic {}",
	 * result.name()); } else { ApiError error = new ApiError(result.errorCode(),
	 * result.errorMessage()); if (error.isFailure()) { if
	 * (error.is(Errors.THROTTLING_QUOTA_EXCEEDED)) {
	 * ThrottlingQuotaExceededException quotaExceededException = new
	 * ThrottlingQuotaExceededException( response.throttleTimeMs(),
	 * error.messageWithFallback()); if (options.shouldRetryOnQuotaViolation()) {
	 * retryTopics.add(topics.find(result.name()).duplicate());
	 * retryTopicQuotaExceededExceptions.put(result.name(), quotaExceededException);
	 * } else { future.completeExceptionally(quotaExceededException); } } else {
	 * future.completeExceptionally(error.exception()); } } else {
	 * TopicMetadataAndConfig topicMetadataAndConfig; if
	 * (result.topicConfigErrorCode() != Errors.NONE.code()) {
	 * topicMetadataAndConfig = new TopicMetadataAndConfig(
	 * Errors.forCode(result.topicConfigErrorCode()).exception()); } else if
	 * (result.numPartitions() == CreateTopicsResult.UNKNOWN) {
	 * topicMetadataAndConfig = new TopicMetadataAndConfig(new
	 * UnsupportedVersionException(
	 * "Topic metadata and configs in CreateTopics response not supported")); } else
	 * { List<CreatableTopicConfigs> configs = result.configs(); Config topicConfig
	 * = new Config(configs.stream() .map(this::configEntry)
	 * .collect(Collectors.toSet())); topicMetadataAndConfig = new
	 * TopicMetadataAndConfig(result.topicId(), result.numPartitions(),
	 * result.replicationFactor(), topicConfig); }
	 * future.complete(topicMetadataAndConfig); } } } // If there are topics to
	 * retry, retry them; complete unrealized futures otherwise. if
	 * (retryTopics.isEmpty()) { // The server should send back a response for every
	 * topic. But do a sanity check anyway.
	 * completeUnrealizedFutures(futures.entrySet().stream(), topic ->
	 * "The controller response did not contain a result for topic " + topic); }
	 * else { final long now = time.milliseconds(); final Call call =
	 * getCreateTopicsCall(options, futures, retryTopics,
	 * retryTopicQuotaExceededExceptions, now, deadline); runnable.call(call, now);
	 * } }
	 * 
	 * private ConfigEntry configEntry(CreatableTopicConfigs config) { return new
	 * ConfigEntry( config.name(), config.value(),
	 * configSource(DescribeConfigsResponse.ConfigSource.forId(config.configSource()
	 * )), config.isSensitive(), config.readOnly(), Collections.emptyList(), null,
	 * null); }
	 * 
	 * @Override void handleFailure(Throwable throwable) { // If there were any
	 * topics retries due to a quota exceeded exception, we propagate // the initial
	 * error back to the caller if the request timed out.
	 * maybeCompleteQuotaExceededException(options.shouldRetryOnQuotaViolation(),
	 * throwable, futures, quotaExceededExceptions, (int) (time.milliseconds() -
	 * now)); // Fail all the other remaining futures
	 * completeAllExceptionally(futures.values(), throwable); } }; }
	 * 
	 * @Override public DeleteTopicsResult deleteTopics(final Collection<String>
	 * topicNames, final DeleteTopicsOptions options) { final Map<String,
	 * KafkaFutureImpl<Void>> topicFutures = new HashMap<>(topicNames.size()); final
	 * List<String> validTopicNames = new ArrayList<>(topicNames.size()); for
	 * (String topicName : topicNames) { if (topicNameIsUnrepresentable(topicName))
	 * { KafkaFutureImpl<Void> future = new KafkaFutureImpl<>();
	 * future.completeExceptionally(new
	 * InvalidTopicException("The given topic name '" + topicName +
	 * "' cannot be represented in a request.")); topicFutures.put(topicName,
	 * future); } else if (!topicFutures.containsKey(topicName)) {
	 * topicFutures.put(topicName, new KafkaFutureImpl<>());
	 * validTopicNames.add(topicName); } } if (!validTopicNames.isEmpty()) { final
	 * long now = time.milliseconds(); final long deadline = calcDeadlineMs(now,
	 * options.timeoutMs()); final Call call = getDeleteTopicsCall(options,
	 * topicFutures, validTopicNames, Collections.emptyMap(), now, deadline);
	 * runnable.call(call, now); } return new DeleteTopicsResult(new
	 * HashMap<>(topicFutures)); }
	 * 
	 * private Call getDeleteTopicsCall(final DeleteTopicsOptions options, final
	 * Map<String, KafkaFutureImpl<Void>> futures, final List<String> topics, final
	 * Map<String, ThrottlingQuotaExceededException> quotaExceededExceptions, final
	 * long now, final long deadline) { return new Call("deleteTopics", deadline,
	 * new ControllerNodeProvider()) {
	 * 
	 * @Override DeleteTopicsRequest.Builder createRequest(int timeoutMs) { return
	 * new DeleteTopicsRequest.Builder( new DeleteTopicsRequestData()
	 * .setTopicNames(topics) .setTimeoutMs(timeoutMs)); }
	 * 
	 * @Override void
	 * handleResponse(org.apache.kafka.common.requests.AbstractResponse
	 * abstractResponse) { // Check for controller change
	 * handleNotControllerError(abstractResponse); // Handle server responses for
	 * particular topics. final DeleteTopicsResponse response =
	 * (DeleteTopicsResponse) abstractResponse; final List<String> retryTopics = new
	 * ArrayList<>(); final Map<String, ThrottlingQuotaExceededException>
	 * retryTopicQuotaExceededExceptions = new HashMap<>(); for
	 * (DeletableTopicResult result : response.data().responses()) {
	 * KafkaFutureImpl<Void> future = futures.get(result.name()); if (future ==
	 * null) { log.warn("Server response mentioned unknown topic {}",
	 * result.name()); } else { ApiError error = new ApiError(result.errorCode(),
	 * result.errorMessage()); if (error.isFailure()) { if
	 * (error.is(Errors.THROTTLING_QUOTA_EXCEEDED)) {
	 * ThrottlingQuotaExceededException quotaExceededException = new
	 * ThrottlingQuotaExceededException( response.throttleTimeMs(),
	 * error.messageWithFallback()); if (options.shouldRetryOnQuotaViolation()) {
	 * retryTopics.add(result.name());
	 * retryTopicQuotaExceededExceptions.put(result.name(), quotaExceededException);
	 * } else { future.completeExceptionally(quotaExceededException); } } else {
	 * future.completeExceptionally(error.exception()); } } else {
	 * future.complete(null); } } } // If there are topics to retry, retry them;
	 * complete unrealized futures otherwise. if (retryTopics.isEmpty()) { // The
	 * server should send back a response for every topic. But do a sanity check
	 * anyway. completeUnrealizedFutures(futures.entrySet().stream(), topic ->
	 * "The controller response did not contain a result for topic " + topic); }
	 * else { final long now = time.milliseconds(); final Call call =
	 * getDeleteTopicsCall(options, futures, retryTopics,
	 * retryTopicQuotaExceededExceptions, now, deadline); runnable.call(call, now);
	 * } }
	 * 
	 * @Override void handleFailure(Throwable throwable) { // If there were any
	 * topics retries due to a quota exceeded exception, we propagate // the initial
	 * error back to the caller if the request timed out.
	 * maybeCompleteQuotaExceededException(options.shouldRetryOnQuotaViolation(),
	 * throwable, futures, quotaExceededExceptions, (int) (time.milliseconds() -
	 * now)); // Fail all the other remaining futures
	 * completeAllExceptionally(futures.values(), throwable); } }; }
	 * 
	 */

	@Override
	public CreateTopicsResult createTopics(final Collection<NewTopic> newTopics, final CreateTopicsOptions options) {
		final Map<String, KafkaFutureImpl<TopicMetadataAndConfig>> topicFutures = new HashMap<>(newTopics.size());
		final Map<String, CreateTopicsRequest.TopicDetails> topicsMap = new HashMap<>(newTopics.size());
		
		for (NewTopic newTopic : newTopics) {
			if (topicNameIsUnrepresentable(newTopic.name())) {
				KafkaFutureImpl<TopicMetadataAndConfig> future = new KafkaFutureImpl<>();
				future.completeExceptionally(new InvalidTopicException(
						"The given topic name '" + newTopic.name() + "' cannot be represented in a request."));
				topicFutures.put(newTopic.name().toUpperCase(), future);
			} else if (!topicFutures.containsKey(newTopic.name().toUpperCase())) {
				topicFutures.put(newTopic.name().toUpperCase(), new KafkaFutureImpl<TopicMetadataAndConfig>());
				TopicDetails topicDetails = null;

				if (newTopic.replicasAssignments() != null) {
					if (newTopic.configs() != null) {
						topicDetails = new TopicDetails(newTopic.replicasAssignments(), newTopic.configs());
					} else {
						topicDetails = new TopicDetails(newTopic.replicasAssignments());
					}
				} else {
					if (newTopic.configs() != null) {
						topicDetails = new TopicDetails(newTopic.numPartitions(), newTopic.replicationFactor(),
								newTopic.configs());
					} else {
						topicDetails = new TopicDetails(newTopic.numPartitions(), newTopic.replicationFactor());
					}
				}
				topicsMap.put(newTopic.name().toUpperCase(), topicDetails);
			}
		}
		final long now = time.milliseconds();
		Call call = new Call("createTopics", calcDeadlineMs(now, options.timeoutMs()), new ControllerNodeProvider()) {

			@Override
			public AbstractRequest.Builder createRequest(int timeoutMs) {
				return new CreateTopicsRequest.Builder(topicsMap, timeoutMs, options.shouldValidateOnly());
			}

			@Override
			public void handleResponse(org.apache.kafka.common.requests.AbstractResponse abstractResponse) {
				CreateTopicsResponse response = (CreateTopicsResponse) abstractResponse;

				// Handle server responses for particular topics.
				for (Map.Entry<String, Exception> entry : response.errors().entrySet()) {
					KafkaFutureImpl<TopicMetadataAndConfig> future = topicFutures.get(entry.getKey());
					if (future == null) {
						log.warn("Server response mentioned unknown topic {}", entry.getKey());
					} else {
						Exception exception = entry.getValue();
						if (exception != null) {
							future.completeExceptionally(exception);
						} else {
							future.complete(null);
						}
					}
				}
				// The server should send back a response for every topic. But do a sanity check
				// anyway.
				for (Map.Entry<String, KafkaFutureImpl<TopicMetadataAndConfig>> entry : topicFutures.entrySet()) {
					KafkaFutureImpl<TopicMetadataAndConfig> future = entry.getValue();
					if (!future.isDone()) {
						if (response.getResult() != null) {
							future.completeExceptionally(response.getResult());
						} else
							future.completeExceptionally(new ApiException(
									"The server response did not " + "contain a reference to node " + entry.getKey()));
					}
				}
			}

			@Override
			void handleFailure(Throwable throwable) {
				completeAllExceptionally(topicFutures.values(), throwable);
			}
		};
		if (!topicsMap.isEmpty()) {
			runnable.call(call, now);
		}
		org.apache.kafka.clients.admin.CreateTopicsResult createTopicResults = new org.oracle.okafka.clients.admin.CreateTopicsResult(
				new HashMap<String, KafkaFuture<TopicMetadataAndConfig>>(topicFutures));
		return createTopicResults;
	}

	@Override
	public DeleteTopicsResult deleteTopics(final TopicCollection topics, DeleteTopicsOptions options) {

		if (topics instanceof TopicIdCollection) {
			throw new FeatureNotSupportedException("Topic Ids are not supported for this release of OKafka.");
		}

		final Collection<String> topicNames = ((TopicCollection.TopicNameCollection) topics).topicNames();
		final Map<String, KafkaFutureImpl<Void>> topicFutures = new HashMap<>(topicNames.size());
		final List<String> validTopicNames = new ArrayList<>(topicNames.size());

		for (String topicName : topicNames) {
			if (topicNameIsUnrepresentable(topicName)) {

				KafkaFutureImpl<Void> future = new KafkaFutureImpl<>();
				future.completeExceptionally(new InvalidTopicException(
						"The given topic name '" + topicName + "' cannot be represented in a request."));
				topicFutures.put(topicName.toUpperCase(), future);

			} else if (!topicFutures.containsKey(topicName.toUpperCase())) {
				topicFutures.put(topicName.toUpperCase(), new KafkaFutureImpl<Void>());
				validTopicNames.add(topicName.toUpperCase());
			}
		}
		final long now = time.milliseconds();
		Call call = new Call("deleteTopics", calcDeadlineMs(now, options.timeoutMs()), new ControllerNodeProvider()) {

			@Override
			AbstractRequest.Builder createRequest(int timeoutMs) {
				return new DeleteTopicsRequest.Builder(new HashSet<>(validTopicNames), timeoutMs);
			}

			@Override
			void handleResponse(org.apache.kafka.common.requests.AbstractResponse abstractResponse) {
				DeleteTopicsResponse response = (DeleteTopicsResponse) abstractResponse;
				// Handle server responses for particular topics.
				for (Map.Entry<String, SQLException> entry : response.errors().entrySet()) {
					KafkaFutureImpl<Void> future = topicFutures.get(entry.getKey());
					if (future == null) {
						log.warn("Server response mentioned unknown topic {}", entry.getKey());
					} else {
						SQLException exception = entry.getValue();
						if (exception != null) {
							future.completeExceptionally(new KafkaException(exception.getMessage()));
						} else {
							future.complete(null);
						}
					}
				}
				// The server should send back a response for every topic. But do a sanity check
				// anyway.
				for (Map.Entry<String, KafkaFutureImpl<Void>> entry : topicFutures.entrySet()) {
					KafkaFutureImpl<Void> future = entry.getValue();
					if (!future.isDone()) {
						if (response.getResult() != null) {
							future.completeExceptionally(response.getResult());
						} else
							future.completeExceptionally(new ApiException(
									"The server response did not " + "contain a reference to node " + entry.getKey()));
					}
				}
			}

			@Override
			void handleFailure(Throwable throwable) {
				completeAllExceptionally(topicFutures.values(), throwable);
			}
		};
		if (!validTopicNames.isEmpty()) {
			runnable.call(call, now);
		}
		return new org.oracle.okafka.clients.admin.DeleteTopicsResult(
				new HashMap<String, KafkaFuture<Void>>(topicFutures));

	}

	private void handleNotControllerError(org.apache.kafka.common.requests.AbstractResponse response)
			throws ApiException {
		if (response.errorCounts().containsKey(Errors.NOT_CONTROLLER)) {
			handleNotControllerError(Errors.NOT_CONTROLLER);
		}
	}

	private void handleNotControllerError(Errors error) throws ApiException {
		metadataManager.clearController();
		metadataManager.requestUpdate();
		throw error.exception();
	}
	
	@Override
	public ListTopicsResult listTopics(final ListTopicsOptions options) {
		final KafkaFutureImpl<Map<String, TopicListing>> topicListingFuture = new KafkaFutureImpl<>();
		final long now = time.milliseconds();
		runnable.call(new Call("listTopics", calcDeadlineMs(now, options.timeoutMs()), new LeastLoadedNodeProvider()) {

			@Override
			MetadataRequest.Builder createRequest(int timeoutMs) {
				return MetadataRequest.Builder.listAllTopics();
			}

			@Override
			void handleResponse(AbstractResponse abstractResponse) {
				MetadataResponse response = (MetadataResponse) abstractResponse;
				if(response.getException()==null) {
				Map<String, TopicListing> topicListing = new HashMap<>();
				Map<String, TopicTeqParameters> topicTeqParameters = response.teqParameters();

				for (Map.Entry<String, TopicTeqParameters> entry : topicTeqParameters.entrySet()) {
					String topicName = entry.getKey();
					if (entry.getValue().getStickyDeq() == 2)
						topicListing.put(topicName, new TopicListing(topicName, null, false));
				}

				topicListingFuture.complete(topicListing);
				}else {
					handleFailure(response.getException());
				}
			}

			@Override
			void handleFailure(Throwable throwable) {
				topicListingFuture.completeExceptionally(throwable);
			}
		}, now);

		Constructor<ListTopicsResult> constructor;
		ListTopicsResult ltr;
		try {
			constructor = ListTopicsResult.class.getDeclaredConstructor(KafkaFuture.class);

			constructor.setAccessible(true);
			try {
				ltr = constructor.newInstance(topicListingFuture);
				return ltr;
			} catch (InstantiationException | IllegalAccessException | IllegalArgumentException
					| InvocationTargetException e) {
				ltr = null;
				log.error("Exception caught: ",e);
			}
		} catch (NoSuchMethodException | SecurityException e) {
			log.error("Exception caught: ",e);
			constructor = null;
			ltr = null;
		}
		return ltr;
	}
	
	
	@Override
	public DescribeTopicsResult describeTopics(final TopicCollection topics, DescribeTopicsOptions options) {

		if (topics instanceof TopicIdCollection) {
			throw new FeatureNotSupportedException("Topic Ids are not supported for this release of OKafka.");
		}

		final Collection<String> topicNames = ((TopicCollection.TopicNameCollection) topics).topicNames();
		final Map<String, KafkaFutureImpl<org.apache.kafka.clients.admin.TopicDescription>> topicFutures = new HashMap<>(
				topicNames.size());
		final List<String> validTopicNames = new ArrayList<>(topicNames.size());

		for (String topicName : topicNames) {
			if (topicNameIsUnrepresentable(topicName)) {

				KafkaFutureImpl<org.apache.kafka.clients.admin.TopicDescription> future = new KafkaFutureImpl<>();
				future.completeExceptionally(new InvalidTopicException(
						"The given topic name '" + topicName + "' cannot be represented in a request."));
				topicFutures.put(topicName.toUpperCase(), future);

			} else if (!topicFutures.containsKey(topicName.toUpperCase())) {
				topicFutures.put(topicName.toUpperCase(),
						new KafkaFutureImpl<org.apache.kafka.clients.admin.TopicDescription>());
				validTopicNames.add(topicName.toUpperCase());
			}
		}
		if (validTopicNames.isEmpty()) {
			org.apache.kafka.clients.admin.DescribeTopicsResult describeTopicsResult = new org.oracle.okafka.clients.admin.DescribeTopicsResult(
					new HashMap<String, KafkaFuture<org.apache.kafka.clients.admin.TopicDescription>>(topicFutures));
			return describeTopicsResult;
		}
		final long now = time.milliseconds();

		runnable.call(
				new Call("describeTopics", calcDeadlineMs(now, options.timeoutMs()), new LeastLoadedNodeProvider()) {

					@Override
					MetadataRequest.Builder createRequest(int timeoutMs) {
						return new MetadataRequest.Builder(validTopicNames, false, validTopicNames);
					}

					@Override
					void handleResponse(AbstractResponse abstractResponse) {
						MetadataResponse response = (MetadataResponse) abstractResponse;
						List<PartitionInfo> partitionInfo = response.partitions();
						Map<String, Exception> errorsPerTopic = response.topicErrors();
						Map<String, TopicTeqParameters> topicTeqParameters = response.teqParameters();

						Map<String, List<TopicPartitionInfo>> topicPartitions = new HashMap<>();

						for (int i = 0; i < partitionInfo.size(); i++) {

							String name = partitionInfo.get(i).topic();
							int partition = partitionInfo.get(i).partition();
							Node leader = partitionInfo.get(i).leader();
							Node[] replicas = partitionInfo.get(i).replicas();
							Node[] inSyncReplicas = partitionInfo.get(i).inSyncReplicas();

							TopicPartitionInfo topicPartitionInfo = new TopicPartitionInfo(partition, leader,
									replicas != null ? new ArrayList<>(Arrays.asList(replicas)) : new ArrayList<>(),
									inSyncReplicas != null ? new ArrayList<>(Arrays.asList(inSyncReplicas))
											: new ArrayList<>());
							if (topicPartitions.containsKey(name)) {
								topicPartitions.get(name).add(topicPartitionInfo);
							} else {
								topicPartitions.put(name, new ArrayList<>(Arrays.asList(topicPartitionInfo)));
							}
						}

						for (Map.Entry<String, KafkaFutureImpl<org.apache.kafka.clients.admin.TopicDescription>> entry : topicFutures
								.entrySet()) {

							KafkaFutureImpl<TopicDescription> future = entry.getValue();

							if (errorsPerTopic.get(entry.getKey()) != null) {
								future.completeExceptionally(errorsPerTopic.get(entry.getKey()));
							}

							TopicDescription topicDescription = new org.oracle.okafka.clients.admin.TopicDescription(
									entry.getKey(), false, topicPartitions.get(entry.getKey()),
									topicTeqParameters.get(entry.getKey()));

							future.complete((org.apache.kafka.clients.admin.TopicDescription) topicDescription);
						}
					}

					@Override
					void handleFailure(Throwable throwable) {
						completeAllExceptionally(topicFutures.values(), throwable);
					}
				}, now);
		DescribeTopicsResult describeTopicsResult = new org.oracle.okafka.clients.admin.DescribeTopicsResult(
				new HashMap<String, KafkaFuture<TopicDescription>>(topicFutures));
		return describeTopicsResult;
	}

	@Override
	public DescribeClusterResult describeCluster(DescribeClusterOptions options) {
		throw new FeatureNotSupportedException("This feature is not suported for this release.");
	}

	@Override
	public DescribeAclsResult describeAcls(final AclBindingFilter filter, DescribeAclsOptions options) {
		throw new FeatureNotSupportedException("This feature is not suported for this release.");
	}

	@Override
	public CreateAclsResult createAcls(Collection<AclBinding> acls, CreateAclsOptions options) {
		throw new FeatureNotSupportedException("This feature is not suported for this release.");
	}

	@Override
	public DeleteAclsResult deleteAcls(Collection<AclBindingFilter> filters, DeleteAclsOptions options) {
		throw new FeatureNotSupportedException("This feature is not suported for this release.");
	}

	@Override
	public DescribeConfigsResult describeConfigs(Collection<ConfigResource> configResources,
			final DescribeConfigsOptions options) {
		throw new FeatureNotSupportedException("This feature is not suported for this release.");
	}

	@Override
	public AlterConfigsResult alterConfigs(Map<ConfigResource, Config> configs, final AlterConfigsOptions options) {
		throw new FeatureNotSupportedException("This feature is not suported for this release.");
	}

	@Override
	public AlterReplicaLogDirsResult alterReplicaLogDirs(Map<TopicPartitionReplica, String> replicaAssignment,
			final AlterReplicaLogDirsOptions options) {
		throw new FeatureNotSupportedException("This feature is not suported for this release.");
	}

	@Override
	public DescribeLogDirsResult describeLogDirs(Collection<Integer> brokers, DescribeLogDirsOptions options) {
		throw new FeatureNotSupportedException("This feature is not suported for this release.");
	}

	@Override
	public DescribeReplicaLogDirsResult describeReplicaLogDirs(Collection<TopicPartitionReplica> replicas,
			DescribeReplicaLogDirsOptions options) {
		throw new FeatureNotSupportedException("This feature is not suported for this release.");
	}

	@Override
	public CreatePartitionsResult createPartitions(Map<String, NewPartitions> newPartitions,
			final CreatePartitionsOptions options) {
		throw new FeatureNotSupportedException("This feature is not suported for this release.");
	}

	@Override
	public DeleteRecordsResult deleteRecords(final Map<TopicPartition, RecordsToDelete> recordsToDelete,
			final DeleteRecordsOptions options) {

		throw new FeatureNotSupportedException("This feature is not suported for this release.");
	}

	@Override
	public CreateDelegationTokenResult createDelegationToken(final CreateDelegationTokenOptions options) {
		throw new FeatureNotSupportedException("This feature is not suported for this release.");
	}

	@Override
	public RenewDelegationTokenResult renewDelegationToken(final byte[] hmac,
			final RenewDelegationTokenOptions options) {
		throw new FeatureNotSupportedException("This feature is not suported for this release.");
	}

	@Override
	public ExpireDelegationTokenResult expireDelegationToken(final byte[] hmac,
			final ExpireDelegationTokenOptions options) {
		throw new FeatureNotSupportedException("This feature is not suported for this release.");
	}

	@Override
	public DescribeDelegationTokenResult describeDelegationToken(final DescribeDelegationTokenOptions options) {
		throw new FeatureNotSupportedException("This feature is not suported for this release.");
	}

	@Override
	public DescribeConsumerGroupsResult describeConsumerGroups(final Collection<String> groupIds,
			final DescribeConsumerGroupsOptions options) {

		throw new FeatureNotSupportedException("This feature is not suported for this release.");
	}

	@Override
	public ListConsumerGroupsResult listConsumerGroups(ListConsumerGroupsOptions options) {
		throw new FeatureNotSupportedException("This feature is not suported for this release.");
	}

	@Override
	public ListConsumerGroupOffsetsResult listConsumerGroupOffsets(final String groupId,
			final ListConsumerGroupOffsetsOptions options) {
		throw new FeatureNotSupportedException("This feature is not suported for this release.");
	}

	@Override
	public DeleteConsumerGroupsResult deleteConsumerGroups(Collection<String> groupIds,
			DeleteConsumerGroupsOptions options) {

		throw new FeatureNotSupportedException("This feature is not suported for this release.");
	}

	@Override
	public void close(Duration timeout) {
		// TODO Auto-generated method stub

	}

	@Override
	public AlterConfigsResult incrementalAlterConfigs(Map<ConfigResource, Collection<AlterConfigOp>> configs,
			AlterConfigsOptions options) {
		throw new FeatureNotSupportedException("This feature is not suported for this release.");
	}

	@Override
	public DeleteConsumerGroupOffsetsResult deleteConsumerGroupOffsets(String groupId, Set<TopicPartition> partitions,
			DeleteConsumerGroupOffsetsOptions options) {
		throw new FeatureNotSupportedException("This feature is not suported for this release.");
	}

	@Override
	public ElectLeadersResult electLeaders(ElectionType electionType, Set<TopicPartition> partitions,
			ElectLeadersOptions options) {
		throw new FeatureNotSupportedException("This feature is not suported for this release.");
	}

	@Override
	public AlterPartitionReassignmentsResult alterPartitionReassignments(
			Map<TopicPartition, Optional<NewPartitionReassignment>> reassignments,
			AlterPartitionReassignmentsOptions options) {
		throw new FeatureNotSupportedException("This feature is not suported for this release.");
	}

	@Override
	public ListPartitionReassignmentsResult listPartitionReassignments(Optional<Set<TopicPartition>> partitions,
			ListPartitionReassignmentsOptions options) {
		throw new FeatureNotSupportedException("This feature is not suported for this release.");
	}

	@Override
	public RemoveMembersFromConsumerGroupResult removeMembersFromConsumerGroup(String groupId,
			RemoveMembersFromConsumerGroupOptions options) {
		throw new FeatureNotSupportedException("This feature is not suported for this release.");
	}

	@Override
	public AlterConsumerGroupOffsetsResult alterConsumerGroupOffsets(String groupId,
			Map<TopicPartition, OffsetAndMetadata> offsets, AlterConsumerGroupOffsetsOptions options) {
		throw new FeatureNotSupportedException("This feature is not suported for this release.");
	}

	@Override
	public ListOffsetsResult listOffsets(Map<TopicPartition, OffsetSpec> topicPartitionOffsets,
			ListOffsetsOptions options) {
		throw new FeatureNotSupportedException("This feature is not suported for this release.");
	}

	@Override
	public DescribeClientQuotasResult describeClientQuotas(ClientQuotaFilter filter,
			DescribeClientQuotasOptions options) {
		throw new FeatureNotSupportedException("This feature is not suported for this release.");
	}

	@Override
	public AlterClientQuotasResult alterClientQuotas(Collection<ClientQuotaAlteration> entries,
			AlterClientQuotasOptions options) {
		throw new FeatureNotSupportedException("This feature is not suported for this release.");
	}

	@Override
	public DescribeUserScramCredentialsResult describeUserScramCredentials(List<String> users,
			DescribeUserScramCredentialsOptions options) {
		throw new FeatureNotSupportedException("This feature is not suported for this release.");
	}

	@Override
	public AlterUserScramCredentialsResult alterUserScramCredentials(List<UserScramCredentialAlteration> alterations,
			AlterUserScramCredentialsOptions options) {
		throw new FeatureNotSupportedException("This feature is not suported for this release.");
	}

	@Override
	public DescribeFeaturesResult describeFeatures(DescribeFeaturesOptions options) {
		throw new FeatureNotSupportedException("This feature is not suported for this release.");
	}

	@Override
	public UpdateFeaturesResult updateFeatures(Map<String, FeatureUpdate> featureUpdates,
			UpdateFeaturesOptions options) {
		throw new FeatureNotSupportedException("This feature is not suported for this release.");
	}

	@Override
	public UnregisterBrokerResult unregisterBroker(int brokerId, UnregisterBrokerOptions options) {
		throw new FeatureNotSupportedException("This feature is not suported for this release.");
	}

	@Override
	public Map<MetricName, ? extends Metric> metrics() {
		throw new FeatureNotSupportedException("This feature is not suported for this release.");
	}

	@Override
	public ListConsumerGroupOffsetsResult listConsumerGroupOffsets(Map<String, ListConsumerGroupOffsetsSpec> groupSpecs,
			ListConsumerGroupOffsetsOptions options) {
		throw new FeatureNotSupportedException("This feature is not suported for this release.");
	}

	@Override
	public DescribeMetadataQuorumResult describeMetadataQuorum(DescribeMetadataQuorumOptions options) {
		throw new FeatureNotSupportedException("This feature is not suported for this release.");
	}

	@Override
	public DescribeProducersResult describeProducers(Collection<TopicPartition> partitions,
			DescribeProducersOptions options) {
		throw new FeatureNotSupportedException("This feature is not suported for this release.");
	}

	@Override
	public DescribeTransactionsResult describeTransactions(Collection<String> transactionalIds,
			DescribeTransactionsOptions options) {
		throw new FeatureNotSupportedException("This feature is not suported for this release.");
	}

	@Override
	public AbortTransactionResult abortTransaction(AbortTransactionSpec spec, AbortTransactionOptions options) {
		throw new FeatureNotSupportedException("This feature is not suported for this release.");
	}

	@Override
	public ListTransactionsResult listTransactions(ListTransactionsOptions options) {
		throw new FeatureNotSupportedException("This feature is not suported for this release.");
	}

	@Override
	public FenceProducersResult fenceProducers(Collection<String> transactionalIds, FenceProducersOptions options) {
		throw new FeatureNotSupportedException("This feature is not suported for this release.");
	}

	@Override
	public Uuid clientInstanceId(Duration timeout) {
		throw new FeatureNotSupportedException("This feature is not suported for this release.");
	}

	@Override
	public ListClientMetricsResourcesResult listClientMetricsResources(ListClientMetricsResourcesOptions arg0) {
		throw new FeatureNotSupportedException("This feature is not suported for this release.");
	}

}
