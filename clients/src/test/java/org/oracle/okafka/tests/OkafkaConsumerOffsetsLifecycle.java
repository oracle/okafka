package org.oracle.okafka.tests;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Future;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.Test;
import org.oracle.okafka.clients.admin.AdminClient;
import org.oracle.okafka.clients.consumer.KafkaConsumer;
import org.oracle.okafka.clients.producer.KafkaProducer;

public class OkafkaConsumerOffsetsLifecycle {

	private static final Duration POLL_TIMEOUT = Duration.ofMillis(1000);
	private static final long ASSIGNMENT_TIMEOUT_MS = 30000L;
	private static final long CONSUME_TIMEOUT_MS = 300000L;
	private static final int LARGE_MAX_POLL_RECORDS = 1000;
	private static final int MEDIUM_MAX_POLL_RECORDS = 100;
	private static final int SMALL_MAX_POLL_RECORDS = 1;

	@Test
	public void ConsumerOffsetsLifecycleTest() {
		String topic = "TEQ_OFFSETS";
		String groupId = "G_OFFSETS";
		TopicPartition topicPartition = new TopicPartition(topic, 0);
		Set<TopicPartition> topicPartitions = Collections.singleton(topicPartition);

		Properties adminProps = OkafkaSetup.setup();
		Properties producerProps = OkafkaSetup.setup();
		Properties consumerProps = OkafkaSetup.setup();

		producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		consumerProps.put("group.id", groupId);
		consumerProps.put("max.poll.records", LARGE_MAX_POLL_RECORDS);
		consumerProps.put("enable.auto.commit", "false");
		consumerProps.put("auto.offset.reset", "earliest");
		consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		Admin admin = null;
		Producer<String, String> producer = null;
		ConsumerHandle consumerHandle = null;

		long producedCount = 0L;
		Long committedCount = null;

		try {
			admin = AdminClient.create(adminProps);
			deleteTopicIfExists(admin, topic);
			admin.createTopics(Arrays.asList(new NewTopic(topic, 1, (short) 1))).all().get();

			producer = new KafkaProducer<String, String>(producerProps);
			consumerHandle = new ConsumerHandle(new KafkaConsumer<String, String>(consumerProps), LARGE_MAX_POLL_RECORDS);

			assertSnapshot("step1-after-create-topic", consumerHandle.consumer, topicPartition, topicPartitions, 0L, 0L, null, false);

			producedCount += produceMessages(producer, topic, producedCount, 1);
			assertSnapshot("step2-after-produce-1", consumerHandle.consumer, topicPartition, topicPartitions, 0L, producedCount, committedCount, false);

			consumerHandle.consumer.subscribe(Arrays.asList(topic));
			ConsumeStepResult consumeResult = consumeAndCommitMessages(consumerHandle, consumerProps, topic, 1, committedCount);
			consumerHandle = consumeResult.consumerHandle;
			committedCount = consumeResult.committedCount;
			waitForAssignment(consumerHandle.consumer, topicPartition);
			assertSnapshot("step3-after-consume-1-and-assignment", consumerHandle.consumer, topicPartition, topicPartitions, 0L, producedCount, committedCount, true);

			producedCount += produceMessages(producer, topic, producedCount, 20000);
			assertSnapshot("step4-after-produce-20000", consumerHandle.consumer, topicPartition, topicPartitions, 0L, producedCount, committedCount, true);

			consumeResult = consumeAndCommitMessages(consumerHandle, consumerProps, topic, 1, committedCount);
			consumerHandle = consumeResult.consumerHandle;
			committedCount = consumeResult.committedCount;
			assertSnapshot("step5-after-consume-1", consumerHandle.consumer, topicPartition, topicPartitions, 0L, producedCount, committedCount, true);

			consumeResult = consumeAndCommitMessages(consumerHandle, consumerProps, topic, 10000, committedCount);
			consumerHandle = consumeResult.consumerHandle;
			committedCount = consumeResult.committedCount;
			assertSnapshot("step6-after-consume-10000", consumerHandle.consumer, topicPartition, topicPartitions, 0L, producedCount, committedCount, true);

			consumeResult = consumeAndCommitMessages(consumerHandle, consumerProps, topic, 9999, committedCount);
			consumerHandle = consumeResult.consumerHandle;
			committedCount = consumeResult.committedCount;
			assertSnapshot("step7-after-consume-remaining", consumerHandle.consumer, topicPartition, topicPartitions, 0L, producedCount, committedCount, true);

			producedCount += produceMessages(producer, topic, producedCount, 22000);
			assertSnapshot("step8-after-produce-22000", consumerHandle.consumer, topicPartition, topicPartitions, 0L, producedCount, committedCount, true);

			consumeResult = consumeAndCommitMessages(consumerHandle, consumerProps, topic, 21000, committedCount);
			consumerHandle = consumeResult.consumerHandle;
			committedCount = consumeResult.committedCount;
			assertSnapshot("step9-after-consume-21000", consumerHandle.consumer, topicPartition, topicPartitions, 0L, producedCount, committedCount, true);

			consumeResult = consumeAndCommitMessages(consumerHandle, consumerProps, topic, 1000, committedCount);
			consumerHandle = consumeResult.consumerHandle;
			committedCount = consumeResult.committedCount;
			assertSnapshot("step10-after-consume-remaining-1000", consumerHandle.consumer, topicPartition, topicPartitions, 0L, producedCount, committedCount, true);

			producedCount += produceMessages(producer, topic, producedCount, 40000);
			assertSnapshot("step11-after-produce-40000", consumerHandle.consumer, topicPartition, topicPartitions, 0L, producedCount, committedCount, true);

			consumeResult = consumeAndCommitMessages(consumerHandle, consumerProps, topic, 22000, committedCount);
			consumerHandle = consumeResult.consumerHandle;
			committedCount = consumeResult.committedCount;
			assertSnapshot("step12-after-consume-22000", consumerHandle.consumer, topicPartition, topicPartitions, 0L, producedCount, committedCount, true);

			consumeResult = consumeAndCommitMessages(consumerHandle, consumerProps, topic, 18000, committedCount);
			consumerHandle = consumeResult.consumerHandle;
			committedCount = consumeResult.committedCount;
			assertSnapshot("step13-after-consume-remaining", consumerHandle.consumer, topicPartition, topicPartitions, 0L, producedCount, committedCount, true);

			producedCount += produceMessages(producer, topic, producedCount, 44000);
			assertSnapshot("step14-after-produce-44000", consumerHandle.consumer, topicPartition, topicPartitions, 0L, producedCount, committedCount, true);

			consumeResult = consumeAndCommitMessages(consumerHandle, consumerProps, topic, 39999, committedCount);
			consumerHandle = consumeResult.consumerHandle;
			committedCount = consumeResult.committedCount;
			assertSnapshot("step15-after-consume-40000", consumerHandle.consumer, topicPartition, topicPartitions, 0L, producedCount, committedCount, true);

			consumeResult = consumeAndCommitMessages(consumerHandle, consumerProps, topic, 4001, committedCount);
			consumerHandle = consumeResult.consumerHandle;
			committedCount = consumeResult.committedCount;
			assertSnapshot("step16-after-consume-remaining", consumerHandle.consumer, topicPartition, topicPartitions, 0L, producedCount, committedCount, true);

			producedCount += produceMessages(producer, topic, producedCount, 100000);
			assertSnapshot("step17-after-produce-100000", consumerHandle.consumer, topicPartition, topicPartitions, 0L, producedCount, committedCount, true);

			consumeResult = consumeAndCommitMessages(consumerHandle, consumerProps, topic, 40000, committedCount);
			consumerHandle = consumeResult.consumerHandle;
			committedCount = consumeResult.committedCount;
			assertSnapshot("step18-after-consume-40000", consumerHandle.consumer, topicPartition, topicPartitions, 0L, producedCount, committedCount, true);

			consumeResult = consumeAndCommitMessages(consumerHandle, consumerProps, topic, 60000, committedCount);
			consumerHandle = consumeResult.consumerHandle;
			committedCount = consumeResult.committedCount;
			assertSnapshot("step19-after-consume-remaining", consumerHandle.consumer, topicPartition, topicPartitions, 0L, producedCount, committedCount, true);

			closeConsumerHandle(consumerHandle);
			consumerHandle = null;

			producedCount += produceMessages(producer, topic, producedCount, 22000);

			consumerHandle = new ConsumerHandle(createConsumer(consumerProps, topic, LARGE_MAX_POLL_RECORDS), LARGE_MAX_POLL_RECORDS);
			consumeWithoutCommitMessages(consumerHandle, consumerProps, topic, 22000);
			closeConsumerHandle(consumerHandle);
			consumerHandle = null;

			consumerHandle = new ConsumerHandle(createConsumer(consumerProps, topic, LARGE_MAX_POLL_RECORDS), LARGE_MAX_POLL_RECORDS);
			assertSnapshot("step20-after-restart-without-commit", consumerHandle.consumer, topicPartition, topicPartitions, 0L,
					producedCount, committedCount, false);
		} catch (Exception e) {
			System.out.println("Exception while testing consumer offsets lifecycle " + e);
			e.printStackTrace();
			Assert.fail("ConsumerOffsetsLifecycleTest failed with exception: " + e.getMessage());
		} finally {
			if (consumerHandle != null && consumerHandle.consumer != null) {
				System.out.println("Closing Consumer");
				consumerHandle.consumer.close();
			}
			if (producer != null) {
				System.out.println("Closing Producer");
				producer.close();
			}
			if (admin != null) {
				try {
					admin.deleteTopics(Arrays.asList(topic)).all().get();
				} catch (Exception cleanupEx) {
					System.out.println("Exception while deleting topic " + topic + " " + cleanupEx);
					cleanupEx.printStackTrace();
				}
				admin.close();
			}
		}

		System.out.println("Test: OkafkaConsumerOffsetsLifecycle completed");
	}

	private void deleteTopicIfExists(Admin admin, String topic) {
		try {
			admin.deleteTopics(Arrays.asList(topic)).all().get();
			System.out.println("Deleted existing topic before test: " + topic);
		} catch (Exception e) {
			System.out.println("Topic did not exist or could not be deleted before create: " + topic + " " + e.getMessage());
		}
	}

	private void waitForAssignment(Consumer<String, String> consumer, TopicPartition topicPartition) {
		long deadline = System.currentTimeMillis() + ASSIGNMENT_TIMEOUT_MS;
		while (System.currentTimeMillis() < deadline) {
			ConsumerRecords<String, String> records = consumer.poll(POLL_TIMEOUT);
			System.out.println("Waiting for assignment. Polled " + records.count() + " records. Assignment="
					+ consumer.assignment());
			if (consumer.assignment().contains(topicPartition))
				return;
		}
		Assert.fail("Timed out waiting for assignment of " + topicPartition);
	}

	private long produceMessages(Producer<String, String> producer, String topic, long startIndex, int count) throws Exception {
		Future<RecordMetadata> lastFuture = null;
		for (int i = 0; i < count; i++) {
			long sequence = startIndex + i;
			lastFuture = producer.send(new ProducerRecord<String, String>(topic, 0, "K" + sequence, "V" + sequence));
			if ((i + 1) % 2000 == 0 && lastFuture != null)
				lastFuture.get();
		}
		if (lastFuture != null)
			lastFuture.get();
		producer.flush();
		System.out.println("Produced " + count + " messages to " + topic);
		return count;
	}

	private ConsumeStepResult consumeAndCommitMessages(ConsumerHandle consumerHandle, Properties consumerProps, String topic,
			int count, Long currentCommittedCount) {
		ConsumerHandle activeConsumerHandle = ensureConsumerWithMaxPollRecords(consumerHandle, consumerProps, topic,
				LARGE_MAX_POLL_RECORDS);
		Long committedCount = currentCommittedCount;
		int remaining = count;

		int largeChunkCount = (remaining / LARGE_MAX_POLL_RECORDS) * LARGE_MAX_POLL_RECORDS;
		if (largeChunkCount > 0) {
			committedCount = consumeAndCommitOnCurrentConsumer(activeConsumerHandle.consumer, largeChunkCount, committedCount);
			remaining -= largeChunkCount;
		}

		int mediumChunkCount = (remaining / MEDIUM_MAX_POLL_RECORDS) * MEDIUM_MAX_POLL_RECORDS;
		if (mediumChunkCount > 0) {
			activeConsumerHandle = switchConsumer(activeConsumerHandle, consumerProps, topic, MEDIUM_MAX_POLL_RECORDS);
			committedCount = consumeAndCommitOnCurrentConsumer(activeConsumerHandle.consumer, mediumChunkCount, committedCount);
			remaining -= mediumChunkCount;
		}

		if (remaining > 0) {
			activeConsumerHandle = switchConsumer(activeConsumerHandle, consumerProps, topic, SMALL_MAX_POLL_RECORDS);
			committedCount = consumeAndCommitOnCurrentConsumer(activeConsumerHandle.consumer, remaining, committedCount);
		}

		return new ConsumeStepResult(activeConsumerHandle, committedCount);
	}

	private ConsumerHandle consumeWithoutCommitMessages(ConsumerHandle consumerHandle, Properties consumerProps, String topic,
			int count) {
		ConsumerHandle activeConsumerHandle = ensureConsumerWithMaxPollRecords(consumerHandle, consumerProps, topic,
				LARGE_MAX_POLL_RECORDS);
		int remaining = count;

		int largeChunkCount = (remaining / LARGE_MAX_POLL_RECORDS) * LARGE_MAX_POLL_RECORDS;
		if (largeChunkCount > 0) {
			consumeOnCurrentConsumer(activeConsumerHandle.consumer, largeChunkCount);
			remaining -= largeChunkCount;
		}

		int mediumChunkCount = (remaining / MEDIUM_MAX_POLL_RECORDS) * MEDIUM_MAX_POLL_RECORDS;
		if (mediumChunkCount > 0) {
			activeConsumerHandle = switchConsumer(activeConsumerHandle, consumerProps, topic, MEDIUM_MAX_POLL_RECORDS);
			consumeOnCurrentConsumer(activeConsumerHandle.consumer, mediumChunkCount);
			remaining -= mediumChunkCount;
		}

		if (remaining > 0) {
			activeConsumerHandle = switchConsumer(activeConsumerHandle, consumerProps, topic, SMALL_MAX_POLL_RECORDS);
			consumeOnCurrentConsumer(activeConsumerHandle.consumer, remaining);
		}

		return activeConsumerHandle;
	}

	private Long consumeAndCommitOnCurrentConsumer(Consumer<String, String> consumer, int count, Long currentCommittedCount) {
		long consumed = consumeOnCurrentConsumer(consumer, count);
		consumer.commitSync();
		long previousCommittedOffset = currentCommittedCount == null ? 0L : currentCommittedCount.longValue();
		long newCommittedCount = previousCommittedOffset + consumed;
		System.out.println("Committed offset after consuming " + count + " messages = " + newCommittedCount);
		return newCommittedCount;
	}

	private long consumeOnCurrentConsumer(Consumer<String, String> consumer, int count) {
		long consumed = 0L;
		long deadline = System.currentTimeMillis() + CONSUME_TIMEOUT_MS;
		while (consumed < count && System.currentTimeMillis() < deadline) {
			ConsumerRecords<String, String> records = consumer.poll(POLL_TIMEOUT);
			if (records != null && records.count() > 0) {
				consumed += records.count();
				System.out.println("Consumed " + records.count() + " records in this poll. Total for step=" + consumed);
			}
		}
		Assert.assertEquals("Did not consume the expected number of records in this step", count, consumed);
		return consumed;
	}

	private ConsumerHandle ensureConsumerWithMaxPollRecords(ConsumerHandle consumerHandle, Properties consumerProps, String topic,
			int maxPollRecords) {
		if (consumerHandle.maxPollRecords == maxPollRecords)
			return consumerHandle;
		return switchConsumer(consumerHandle, consumerProps, topic, maxPollRecords);
	}

	private ConsumerHandle switchConsumer(ConsumerHandle consumerHandle, Properties consumerProps, String topic, int maxPollRecords) {
		if (consumerHandle != null && consumerHandle.consumer != null) {
			System.out.println("Closing consumer with max.poll.records=" + consumerHandle.maxPollRecords);
			consumerHandle.consumer.close();
		}
		Consumer<String, String> consumer = createConsumer(consumerProps, topic, maxPollRecords);
		System.out.println("Created consumer with max.poll.records=" + maxPollRecords);
		return new ConsumerHandle(consumer, maxPollRecords);
	}

	private Consumer<String, String> createConsumer(Properties consumerProps, String topic, int maxPollRecords) {
		Properties updatedConsumerProps = new Properties();
		updatedConsumerProps.putAll(consumerProps);
		updatedConsumerProps.put("max.poll.records", Integer.toString(maxPollRecords));
		Consumer<String, String> consumer = new KafkaConsumer<String, String>(updatedConsumerProps);
		consumer.subscribe(Arrays.asList(topic));
		return consumer;
	}

	private void closeConsumerHandle(ConsumerHandle consumerHandle) {
		if (consumerHandle != null && consumerHandle.consumer != null) {
			System.out.println("Closing consumer with max.poll.records=" + consumerHandle.maxPollRecords);
			consumerHandle.consumer.close();
		}
	}

	private static class ConsumerHandle {
		private final Consumer<String, String> consumer;
		private final int maxPollRecords;

		private ConsumerHandle(Consumer<String, String> consumer, int maxPollRecords) {
			this.consumer = consumer;
			this.maxPollRecords = maxPollRecords;
		}
	}

	private static class ConsumeStepResult {
		private final ConsumerHandle consumerHandle;
		private final Long committedCount;

		private ConsumeStepResult(ConsumerHandle consumerHandle, Long committedCount) {
			this.consumerHandle = consumerHandle;
			this.committedCount = committedCount;
		}
	}

	private void assertSnapshot(String label, Consumer<String, String> consumer, TopicPartition topicPartition,
			Set<TopicPartition> topicPartitions, long expectedBeginningOffset, long expectedEndOffset,
			Long expectedCommittedOffset, boolean checkCurrentLag) {
		Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(topicPartitions);
		Map<TopicPartition, Long> endOffsets = consumer.endOffsets(topicPartitions);
		Map<TopicPartition, OffsetAndMetadata> committedOffsets = consumer.committed(topicPartitions);

		Long actualBeginningOffset = beginningOffsets.get(topicPartition);
		Long actualEndOffset = endOffsets.get(topicPartition);
		OffsetAndMetadata actualCommittedOffset = committedOffsets.get(topicPartition);

		System.out.println(label + " beginningOffsets=" + beginningOffsets);
		System.out.println(label + " endOffsets=" + endOffsets);
		System.out.println(label + " committedOffsets=" + committedOffsets);

		Assert.assertNotNull(label + " beginning offset should not be null", actualBeginningOffset);
		Assert.assertEquals(label + " beginning offset mismatch", expectedBeginningOffset, actualBeginningOffset.longValue());

		Assert.assertNotNull(label + " end offset should not be null", actualEndOffset);
		Assert.assertEquals(label + " end offset mismatch", expectedEndOffset, actualEndOffset.longValue());

		if (expectedCommittedOffset == null) {
			Assert.assertNull(label + " committed offset should be null", actualCommittedOffset);
		} else {
			Assert.assertNotNull(label + " committed offset should not be null", actualCommittedOffset);
			Assert.assertEquals(label + " committed offset mismatch", expectedCommittedOffset.longValue(),
					actualCommittedOffset.offset());
		}

		if (checkCurrentLag) {
			OptionalLong currentLag = consumer.currentLag(topicPartition);
			System.out.println(label + " currentLag=" + currentLag);
			if (expectedCommittedOffset == null) {
				Assert.assertFalse(label + " currentLag should be empty", currentLag.isPresent());
			} else {
				Assert.assertTrue(label + " currentLag should be present", currentLag.isPresent());
				Assert.assertEquals(label + " currentLag mismatch",
						expectedEndOffset - expectedCommittedOffset.longValue(), currentLag.getAsLong());
			}
		}
	}
}
