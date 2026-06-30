package org.oracle.okafka.tests;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.junit.Assert;
import org.oracle.okafka.clients.admin.AdminClient;
import org.oracle.okafka.clients.consumer.KafkaConsumer;
import org.oracle.okafka.clients.producer.KafkaProducer;

public final class OkafkaTestSupport {

	private static final String CONFIG_RESOURCE = "test.config";
	private static final String CONFIG_RESOURCE_PATH = "src/test/resources/test.config";
	private static final String ROOT_CONFIG_RESOURCE_PATH = "clients/src/test/resources/test.config";
	private static final String TNS_ADMIN_CONFIG = "oracle.net.tns_admin";
	private static final String DEFAULT_TNS_ADMIN_PATH = "./src/test/resources";
	private static final long FUTURE_TIMEOUT_SECONDS = 60L;
	private static final Duration POLL_TIMEOUT = Duration.ofMillis(1000);
	private static final Duration CONSUME_TIMEOUT = Duration.ofSeconds(60);
	private static final AtomicInteger COUNTER = new AtomicInteger();

	private OkafkaTestSupport() {
	}

	public static Properties baseProperties() {
		Properties properties = new Properties();
		try {
			ConfigSource configSource = openConfig();
			try (InputStream input = configSource.input) {
				properties.load(input);
			}
			useResourceTnsAdmin(properties, configSource.directory);
			Assert.assertFalse("test.config must contain at least one property", properties.isEmpty());
			return properties;
		} catch (Exception e) {
			throw new AssertionError("Unable to load OKafka test configuration from "
					+ CONFIG_RESOURCE_PATH + ", " + ROOT_CONFIG_RESOURCE_PATH + ", or classpath resource "
					+ CONFIG_RESOURCE, e);
		}
	}

	private static ConfigSource openConfig() throws Exception {
		File resourceConfig = new File(CONFIG_RESOURCE_PATH);
		if (resourceConfig.isFile()) {
			return new ConfigSource(new FileInputStream(resourceConfig), resourceConfig.getAbsoluteFile().getParentFile());
		}

		File rootResourceConfig = new File(ROOT_CONFIG_RESOURCE_PATH);
		if (rootResourceConfig.isFile()) {
			return new ConfigSource(new FileInputStream(rootResourceConfig), rootResourceConfig.getAbsoluteFile().getParentFile());
		}

		URL configResource = OkafkaTestSupport.class.getClassLoader().getResource(CONFIG_RESOURCE);
		if (configResource != null) {
			File configDirectory = null;
			if ("file".equals(configResource.getProtocol())) {
				configDirectory = Paths.get(configResource.toURI()).getParent().toFile();
			}
			return new ConfigSource(configResource.openStream(), configDirectory);
		}

		throw new FileNotFoundException(CONFIG_RESOURCE);
	}

	private static void useResourceTnsAdmin(Properties properties, File configDirectory) throws Exception {
		String configuredTnsAdmin = properties.getProperty(TNS_ADMIN_CONFIG);
		if (!DEFAULT_TNS_ADMIN_PATH.equals(configuredTnsAdmin)) {
			return;
		}

		if (configDirectory != null && new File(configDirectory, "ojdbc.properties").isFile()) {
			properties.setProperty(TNS_ADMIN_CONFIG, configDirectory.getAbsolutePath());
			return;
		}

		URL ojdbcProperties = OkafkaTestSupport.class.getClassLoader().getResource("ojdbc.properties");
		if (ojdbcProperties != null && "file".equals(ojdbcProperties.getProtocol())) {
			properties.setProperty(TNS_ADMIN_CONFIG, Paths.get(ojdbcProperties.toURI()).getParent().toString());
		}
	}

	private static final class ConfigSource {
		private final InputStream input;
		private final File directory;

		private ConfigSource(InputStream input, File directory) {
			this.input = input;
			this.directory = directory;
		}
	}

	public static Properties producerProperties() {
		Properties properties = baseProperties();
		properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		return properties;
	}

	public static Properties consumerProperties(String groupId) {
		Properties properties = baseProperties();
		properties.put("group.id", groupId);
		properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		return properties;
	}

	public static Admin admin() {
		return AdminClient.create(baseProperties());
	}

	public static Producer<String, String> producer() {
		return new KafkaProducer<String, String>(producerProperties());
	}

	public static KafkaConsumer<String, String> consumer(Properties properties) {
		return new KafkaConsumer<String, String>(properties);
	}

	public static String uniqueTopic(String baseName) {
		return uniqueName(baseName);
	}

	public static String uniqueGroup(String baseName) {
		return uniqueName(baseName);
	}

	private static String uniqueName(String baseName) {
		return (baseName + "_" + System.currentTimeMillis() + "_" + COUNTER.incrementAndGet()).toUpperCase();
	}

	public static <T> T get(KafkaFuture<T> future, String operation) throws Exception {
		return future.get(FUTURE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
	}

	public static <T> T get(Future<T> future, String operation) throws Exception {
		return future.get(FUTURE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
	}

	public static void createTopic(Admin admin, String topic, int partitions) throws Exception {
		get(admin.createTopics(Arrays.asList(new NewTopic(topic, partitions, (short) 1))).all(),
				"create topic " + topic);
		Assert.assertTrue("Created topic should be listed: " + topic, get(admin.listTopics().names(), "list topics")
				.contains(topic));
	}

	public static void deleteTopicIfExists(Admin admin, String topic) {
		try {
			if (get(admin.listTopics().names(), "list topics before delete").contains(topic)) {
				get(admin.deleteTopics(Arrays.asList(topic)).all(), "delete topic " + topic);
			}
		} catch (Exception e) {
			throw new AssertionError("Unable to delete topic during test cleanup: " + topic, e);
		}
	}

	public static void produceRecords(Producer<String, String> producer, String topic, int count) throws Exception {
		produceRecords(producer, topic, 0, count);
	}

	public static void produceRecords(Producer<String, String> producer, String topic, int startIndex, int count)
			throws Exception {
		Future<RecordMetadata> lastFuture = null;
		for (int i = 0; i < count; i++) {
			int sequence = startIndex + i;
			lastFuture = producer.send(new ProducerRecord<String, String>(topic, "K" + sequence, "V" + sequence));
		}
		if (lastFuture != null) {
			get(lastFuture, "produce records to " + topic);
		}
		producer.flush();
	}

	public static void produceRecordsToPartitions(Producer<String, String> producer, String topic, int partitions)
			throws Exception {
		for (int partition = 0; partition < partitions; partition++) {
			get(producer.send(new ProducerRecord<String, String>(topic, partition, "K" + partition, "V" + partition)),
					"produce record to partition " + partition);
		}
		producer.flush();
	}

	public static int consumeExactly(Consumer<String, String> consumer, int expectedCount) {
		return consumeExactly(consumer, expectedCount, true);
	}

	public static int consumeExactly(Consumer<String, String> consumer, int expectedCount, boolean commit) {
		int consumed = consumeUpTo(consumer, expectedCount, CONSUME_TIMEOUT);
		Assert.assertEquals("Did not consume the expected number of records", expectedCount, consumed);
		if (commit && consumed > 0) {
			consumer.commitSync();
		}
		return consumed;
	}

	public static int consumeAtLeastOne(Consumer<String, String> consumer) {
		int consumed = consumeUpTo(consumer, 1, CONSUME_TIMEOUT);
		Assert.assertTrue("Expected to consume at least one record", consumed > 0);
		consumer.commitSync();
		return consumed;
	}

	public static int consumeUpTo(Consumer<String, String> consumer, int maxCount, Duration timeout) {
		int consumed = 0;
		long deadline = System.currentTimeMillis() + timeout.toMillis();
		while (consumed < maxCount && System.currentTimeMillis() < deadline) {
			ConsumerRecords<String, String> records = consumer.poll(POLL_TIMEOUT);
			consumed += records.count();
			printRecords(records);
		}
		return consumed;
	}

	public static void waitForAssignment(Consumer<String, String> consumer, Collection<?> expectedPartitions) {
		long deadline = System.currentTimeMillis() + CONSUME_TIMEOUT.toMillis();
		while (System.currentTimeMillis() < deadline) {
			consumer.poll(POLL_TIMEOUT);
			if (consumer.assignment().containsAll(expectedPartitions)) {
				return;
			}
		}
		Assert.fail("Timed out waiting for assignment. Expected=" + expectedPartitions + " actual="
				+ consumer.assignment());
	}

	public static void eventually(String description, Duration timeout, CheckedBooleanSupplier condition) {
		long deadline = System.currentTimeMillis() + timeout.toMillis();
		Throwable lastFailure = null;
		while (System.currentTimeMillis() < deadline) {
			try {
				if (condition.getAsBoolean()) {
					return;
				}
			} catch (Throwable e) {
				lastFailure = e;
			}
			try {
				Thread.sleep(POLL_TIMEOUT.toMillis());
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new AssertionError("Interrupted while waiting for " + description, e);
			}
		}
		if (lastFailure != null) {
			throw new AssertionError("Timed out waiting for " + description, lastFailure);
		}
		Assert.fail("Timed out waiting for " + description);
	}

	@FunctionalInterface
	public interface CheckedBooleanSupplier {
		boolean getAsBoolean() throws Exception;
	}

	private static void printRecords(ConsumerRecords<String, String> records) {
		for (ConsumerRecord<String, String> record : records) {
			System.out.printf("partition=%d offset=%d key=%s value=%s%n", record.partition(), record.offset(),
					record.key(), record.value());
		}
	}
}
