package org.oracle.okafka.tests;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;
import org.oracle.okafka.clients.admin.AdminClient;
import org.oracle.okafka.clients.consumer.KafkaConsumer;
import org.oracle.okafka.clients.producer.KafkaProducer;

public class OkafkaOffsetsForTimes {

	private static final String TOPIC_NAME = "TEQ_OFFSETS_FOR_TIMES";
	private static final String GROUP_ID = "G_OFFSETS_FOR_TIMES";

	@Test
	public void OffsetsForTimesTest() {
		TopicPartition topicPartition = new TopicPartition(TOPIC_NAME, 0);

		try {
			createTopic(TOPIC_NAME);
			runOffsetsForTimesScenario(topicPartition);
		} catch (Exception e) {
			System.out.println("Exception while running offsetsForTimes test " + e);
			e.printStackTrace();
		}

		System.out.println("Test: OkafkaOffsetsForTimes completed for topic " + TOPIC_NAME);
	}

	private void createTopic(String topicName) {
		try (Admin admin = AdminClient.create(OkafkaSetup.setup())) {
			if (admin.listTopics().names().get().contains(topicName)) {
				admin.deleteTopics(Arrays.asList(topicName)).all().get();
				System.out.println("Deleted existing topic for offsetsForTimes test: " + topicName);
			}
			admin.createTopics(Arrays.asList(new NewTopic(topicName, 1, (short) 1))).all().get();
			System.out.println("Created topic for offsetsForTimes test: " + topicName);
		} catch (Exception e) {
			System.out.println("Exception while creating topic " + topicName + " " + e);
			e.printStackTrace();
		}
	}

	private void runOffsetsForTimesScenario(TopicPartition topicPartition) {
		Properties producerProperties = OkafkaSetup.setup();
		producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		long firstSendTime = -1L;
		long secondSendTime = -1L;
		long thirdSendTime = -1L;

		try (Producer<String, String> producer = new KafkaProducer<String, String>(producerProperties)) {
			Future<RecordMetadata> future;

			firstSendTime = System.currentTimeMillis();
			future = producer.send(new ProducerRecord<String, String>(topicPartition.topic(), "k1", "message-1"));
			future.get();
			Thread.sleep(5000L);

			secondSendTime = System.currentTimeMillis();
			future = producer.send(new ProducerRecord<String, String>(topicPartition.topic(), "k2", "message-2"));
			future.get();
			Thread.sleep(5000L);

			thirdSendTime = System.currentTimeMillis();
			future = producer.send(new ProducerRecord<String, String>(topicPartition.topic(), "k3", "message-3"));
			future.get();

			producer.flush();
			System.out.println("Produced 3 messages to topic " + topicPartition.topic());
			System.out.println("firstSendTime(ms)=" + firstSendTime + " -> " + Instant.ofEpochMilli(firstSendTime));
			System.out.println("secondSendTime(ms)=" + secondSendTime + " -> " + Instant.ofEpochMilli(secondSendTime));
			System.out.println("thirdSendTime(ms)=" + thirdSendTime + " -> " + Instant.ofEpochMilli(thirdSendTime));
		} catch (Exception e) {
			System.out.println("Exception while producing messages for offsetsForTimes " + e);
			e.printStackTrace();
			return;
		}

		Properties consumerProperties = OkafkaSetup.setup();
		consumerProperties.put("group.id", GROUP_ID);
		consumerProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		consumerProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		Consumer<String, String> consumer = new KafkaConsumer<String, String>(consumerProperties);
		try {
			long betweenTimestamp = secondSendTime;

			Map<TopicPartition, OffsetAndTimestamp> betweenResult = consumer
					.offsetsForTimes(Collections.singletonMap(topicPartition, betweenTimestamp));

			System.out.println("Between timestamp offsetsForTimes");
			System.out.println("Requested timestamp(ms): " + betweenTimestamp + " -> "
					+ Instant.ofEpochMilli(betweenTimestamp));
			System.out.println("Returned map: " + betweenResult);
			System.out.println("Returned value for " + topicPartition + ": " + betweenResult.get(topicPartition));
		} catch (Exception e) {
			System.out.println("Exception while querying offsetsForTimes after producing messages " + e);
			e.printStackTrace();
		} finally {
			consumer.close();
		}
	}
}
