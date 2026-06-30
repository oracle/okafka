package org.oracle.okafka.tests;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.Assert;
import org.junit.Test;
import org.oracle.okafka.clients.consumer.KafkaConsumer;
import org.oracle.okafka.clients.producer.KafkaProducer;

public class OkafkaTransactionalProducerConsumer {

	private static final int PARTITION_COUNT = 1;
	private static final Duration POLL_TIMEOUT = Duration.ofMillis(1000);
	private static final Duration SOURCE_CONSUME_TIMEOUT = Duration.ofSeconds(60);
	private static final Duration TARGET_EMPTY_TIMEOUT = Duration.ofSeconds(5);

	@Test(timeout = 180000)
	public void okafkaTransactionalProducerConsumerTest() throws Exception {
		String sourceTopic = OkafkaTestSupport.uniqueTopic("TEQ_TXN_SOURCE");
		String targetTopic = OkafkaTestSupport.uniqueTopic("TEQ_TXN_TARGET");
		String sourceGroupId = OkafkaTestSupport.uniqueGroup("G_TXN_SOURCE");
		String targetGroupId = OkafkaTestSupport.uniqueGroup("G_TXN_TARGET");
		String auditTable = uniqueTableName();

		try (Admin admin = OkafkaTestSupport.admin()) {
			try {
				OkafkaTestSupport.createTopic(admin, sourceTopic, PARTITION_COUNT);
				OkafkaTestSupport.createTopic(admin, targetTopic, PARTITION_COUNT);
				try (Producer<String, String> seedProducer = OkafkaTestSupport.producer()) {
					OkafkaTestSupport.produceRecords(seedProducer, sourceTopic, 2);
				}

				runConsumeTransformProduceTransactions(sourceTopic, targetTopic, sourceGroupId, targetGroupId,
						auditTable);
			} finally {
				OkafkaTestSupport.deleteTopicIfExists(admin, sourceTopic);
				OkafkaTestSupport.deleteTopicIfExists(admin, targetTopic);
			}
		}
	}

	private static void runConsumeTransformProduceTransactions(String sourceTopic, String targetTopic,
			String sourceGroupId, String targetGroupId, String auditTable) throws Exception {
		Properties consumerProperties = OkafkaTestSupport.consumerProperties(sourceGroupId);
		consumerProperties.put("auto.offset.reset", "earliest");
		consumerProperties.put("enable.auto.commit", "false");
		consumerProperties.put("max.poll.records", "1");

		try (KafkaConsumer<String, String> sourceConsumer = OkafkaTestSupport.consumer(consumerProperties)) {
			createAuditTable(auditTable);
			try {
				sourceConsumer.subscribe(Arrays.asList(sourceTopic));
				ConsumerRecord<String, String> recordToAbort = pollOneRecord(sourceConsumer);
				Connection transactionConnection = sourceConsumer.getDBConnection();

				try (KafkaProducer<String, String> producer = transactionalProducer(transactionConnection)) {
					producer.initTransactions();
					Assert.assertNotNull("Transactional producer should expose the consumer DB connection",
							producer.getDBConnection());

					processRecordAndAbort(recordToAbort, producer, targetTopic, auditTable);
					Assert.assertEquals("Audit row inserted during aborted transaction should be rolled back", 0,
							auditRowCount(transactionConnection, auditTable));
					assertTargetRecordCount(targetTopic, targetGroupId + "_ABORT", 0);

					ConsumerRecord<String, String> recordToCommit = pollOneRecord(sourceConsumer);
					processRecordAndCommit(recordToCommit, producer, targetTopic, auditTable);
					Assert.assertEquals("Audit row inserted during committed transaction should be visible", 1,
							auditRowCount(transactionConnection, auditTable));
					assertTargetRecordCount(targetTopic, targetGroupId + "_COMMIT", 1);
				}
			} finally {
				dropAuditTable(auditTable);
			}
		}
	}

	private static void processRecordAndAbort(ConsumerRecord<String, String> sourceRecord,
			KafkaProducer<String, String> producer, String targetTopic, String auditTable) throws Exception {
		producer.beginTransaction();
		insertAuditRow(producer.getDBConnection(), auditTable, "ABORT", sourceRecord);
		producer.send(new ProducerRecord<String, String>(targetTopic, sourceRecord.key(),
				"aborted-" + sourceRecord.value()));
		producer.abortTransaction();
	}

	private static void processRecordAndCommit(ConsumerRecord<String, String> sourceRecord,
			KafkaProducer<String, String> producer, String targetTopic, String auditTable) throws Exception {
		producer.beginTransaction();
		insertAuditRow(producer.getDBConnection(), auditTable, "COMMIT", sourceRecord);
		Future<RecordMetadata> sent = producer.send(new ProducerRecord<String, String>(targetTopic,
				sourceRecord.key(), "committed-" + sourceRecord.value()));
		producer.commitTransaction();
		OkafkaTestSupport.get(sent, "produce committed transactional output record");
	}

	private static ConsumerRecord<String, String> pollOneRecord(Consumer<String, String> consumer) {
		long deadline = System.currentTimeMillis() + SOURCE_CONSUME_TIMEOUT.toMillis();
		while (System.currentTimeMillis() < deadline) {
			ConsumerRecords<String, String> records = consumer.poll(POLL_TIMEOUT);
			if (records.count() > 0) {
				return records.iterator().next();
			}
		}
		Assert.fail("Timed out waiting for source record");
		return null;
	}

	private static void assertTargetRecordCount(String targetTopic, String groupId, int expectedCount) {
		Properties consumerProperties = OkafkaTestSupport.consumerProperties(groupId);
		consumerProperties.put("auto.offset.reset", "earliest");
		consumerProperties.put("max.poll.records", "10");

		try (Consumer<String, String> targetConsumer = OkafkaTestSupport.consumer(consumerProperties)) {
			targetConsumer.subscribe(Arrays.asList(targetTopic));
			int consumed = expectedCount == 0
					? OkafkaTestSupport.consumeUpTo(targetConsumer, 1, TARGET_EMPTY_TIMEOUT)
					: OkafkaTestSupport.consumeExactly(targetConsumer, expectedCount, false);
			Assert.assertEquals("Unexpected number of target records", expectedCount, consumed);
			if (consumed > 0) {
				targetConsumer.commitSync();
			}
		}
	}

	private static void createAuditTable(String auditTable) throws Exception {
		try (KafkaProducer<String, String> producer = standaloneTransactionalProducer();
				Statement statement = producer.getDBConnection().createStatement()) {
			statement.executeUpdate("CREATE TABLE " + auditTable
					+ " (event_status VARCHAR2(20), source_topic VARCHAR2(128), event_key VARCHAR2(512), event_value VARCHAR2(4000))");
		}
	}

	private static void dropAuditTable(String auditTable) {
		try (KafkaProducer<String, String> producer = standaloneTransactionalProducer();
				Statement statement = producer.getDBConnection().createStatement()) {
			statement.executeUpdate("DROP TABLE " + auditTable + " PURGE");
		} catch (Exception e) {
			System.out.println("Unable to drop transactional audit table " + auditTable + ": " + e.getMessage());
		}
	}

	private static void insertAuditRow(Connection connection, String auditTable, String eventStatus,
			ConsumerRecord<String, String> sourceRecord) throws Exception {
		String sql = "INSERT INTO " + auditTable
				+ " (event_status, source_topic, event_key, event_value) VALUES (?, ?, ?, ?)";
		try (PreparedStatement statement = connection.prepareStatement(sql)) {
			statement.setString(1, eventStatus);
			statement.setString(2, sourceRecord.topic());
			statement.setString(3, sourceRecord.key());
			statement.setString(4, sourceRecord.value());
			statement.executeUpdate();
		}
	}

	private static int auditRowCount(Connection connection, String auditTable) throws Exception {
		try (PreparedStatement statement = connection.prepareStatement("SELECT COUNT(*) FROM " + auditTable);
				ResultSet resultSet = statement.executeQuery()) {
			Assert.assertTrue("Audit row count query should return a row", resultSet.next());
			return resultSet.getInt(1);
		}
	}

	private static KafkaProducer<String, String> transactionalProducer(Connection connection) {
		Properties producerProperties = OkafkaTestSupport.producerProperties();
		producerProperties.put("oracle.transactional.producer", "true");
		return new KafkaProducer<String, String>(producerProperties, connection);
	}

	private static KafkaProducer<String, String> standaloneTransactionalProducer() {
		Properties producerProperties = OkafkaTestSupport.producerProperties();
		producerProperties.put("oracle.transactional.producer", "true");
		return new KafkaProducer<String, String>(producerProperties);
	}

	private static String uniqueTableName() {
		return OkafkaTestSupport.uniqueTopic("TXN_AUDIT").replaceAll("[^A-Z0-9_]", "_");
	}
}
