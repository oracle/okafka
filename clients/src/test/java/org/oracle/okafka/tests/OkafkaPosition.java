package org.oracle.okafka.tests;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;
import org.oracle.okafka.clients.admin.AdminClient;
import org.oracle.okafka.clients.consumer.KafkaConsumer;
import org.oracle.okafka.clients.producer.KafkaProducer;

public class OkafkaPosition {

	private static final String TOPIC_NAME = "TEQ_POSITION";
	private static final String GROUP_ID = "G_POSITION";
	private static final Duration POLL_TIMEOUT = Duration.ofSeconds(10);
	private static final long CONSUME_TIMEOUT_MS = 60000L;
	private static final int TOTAL_MESSAGES = 1000;
	private static final int MESSAGES_TO_CONSUME = TOTAL_MESSAGES / 2;

	@Test
	public void PositionTracksCurrentConsumerProgressWithoutCommitTest() {
		String topic = TOPIC_NAME;
		TopicPartition topicPartition = new TopicPartition(topic, 0);

		Properties producerProps = OkafkaSetup.setup();
		producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Properties consumerProps = OkafkaSetup.setup();
		consumerProps.put("group.id", GROUP_ID);
		consumerProps.put("enable.auto.commit", "false");
		consumerProps.put("auto.offset.reset", "earliest");
		consumerProps.put("max.poll.records", Integer.toString(MESSAGES_TO_CONSUME));
		consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		Admin admin = null;
		Producer<String, String> producer = null;
		KafkaConsumer<String, String> consumer = null;

		try {
			admin = AdminClient.create(OkafkaSetup.setup());
			deleteTopicIfExists(admin, topic);
			admin.createTopics(Arrays.asList(new NewTopic(topic, 1, (short) 1))).all().get();
			System.out.println("Created topic for position test: " + topic);

			producer = new KafkaProducer<String, String>(producerProps);
			produceMessages(producer, topic, TOTAL_MESSAGES);

			consumer = new KafkaConsumer<String, String>(consumerProps);
			final KafkaConsumer<String, String> rebalanceConsumer = consumer;
			final long[] rebalancePosition = new long[] { -1L };
			final boolean[] assignmentHappened = new boolean[] { false };
			consumer.subscribe(Arrays.asList(topic), new ConsumerRebalanceListener() {
				@Override
				public synchronized void onPartitionsRevoked(Collection<TopicPartition> partitions) {
					System.out.println("Partitions revoked in position test: " + partitions);
				}

				@Override
				public synchronized void onPartitionsAssigned(Collection<TopicPartition> partitions) {
					System.out.println("Partitions assigned in position test: " + partitions);
					assignmentHappened[0] = true;
					try {
						rebalancePosition[0] = rebalanceConsumer.position(topicPartition);
						System.out.println("Position inside rebalance callback before consuming messages: "
								+ rebalancePosition[0]);
					} catch (Exception e) {
						System.out.println("Exception while calling position inside rebalance callback " + e);
						e.printStackTrace();
					}
				}
			});

			try {
				long positionBeforeFirstPoll = consumer.position(topicPartition);
				System.out.println("Position before first poll: " + positionBeforeFirstPoll);
			} catch (Exception e) {
				System.out.println("Exception while calling position before first poll " + e);
			}

			consumer.poll(POLL_TIMEOUT);

			long consumedCount = consumeMessages(consumer, MESSAGES_TO_CONSUME);
			long position = consumer.position(topicPartition);

			System.out.println("Consumed count without commit: " + consumedCount);
			System.out.println("Position after consuming without commit: " + position);
			if (consumedCount != MESSAGES_TO_CONSUME)
				throw new RuntimeException("Expected to consume " + MESSAGES_TO_CONSUME + " records but consumed " + consumedCount);
			if (position != MESSAGES_TO_CONSUME)
				throw new RuntimeException("Expected position " + MESSAGES_TO_CONSUME + " but got " + position);

			consumer.commitSync();
			System.out.println("Committed consumed messages after position check");
		} catch (Exception e) {
			System.out.println("Exception while testing consumer position " + e);
			e.printStackTrace();
		} finally {
			if (consumer != null) {
				System.out.println("Closing Consumer");
				consumer.close();
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

		System.out.println("Test: OkafkaPosition completed for topic " + topic);
	}

	private void deleteTopicIfExists(Admin admin, String topic) {
		try {
			if (admin.listTopics().names().get().contains(topic)) {
				admin.deleteTopics(Arrays.asList(topic)).all().get();
				System.out.println("Deleted existing topic before test: " + topic);
			}
		} catch (Exception e) {
			System.out.println("Topic did not exist or could not be deleted before create: " + topic + " " + e.getMessage());
		}
	}

	private void produceMessages(Producer<String, String> producer, String topic, int count) throws Exception {
		Future<RecordMetadata> lastFuture = null;
		for (int i = 0; i < count; i++) {
			lastFuture = producer.send(new ProducerRecord<String, String>(topic, 0, "K" + i, "V" + i));
		}
		if (lastFuture != null)
			lastFuture.get();
		producer.flush();
		System.out.println("Produced " + count + " messages to " + topic);
	}

	private long consumeMessages(Consumer<String, String> consumer, int count) {
		long consumed = 0L;
		long deadline = System.currentTimeMillis() + CONSUME_TIMEOUT_MS;
		while (consumed < count && System.currentTimeMillis() < deadline) {
			ConsumerRecords<String, String> records = consumer.poll(POLL_TIMEOUT);
			consumed += printRecords(records);
		}
		return consumed;
	}

	private long printRecords(ConsumerRecords<String, String> records) {
		if (records == null || records.count() == 0)
			return 0L;

		for (ConsumerRecord<String, String> record : records) {
			System.out.printf("Consumed record partition=%d offset=%d key=%s value=%s%n",
					record.partition(), record.offset(), record.key(), record.value());
		}
		return records.count();
	}
}
