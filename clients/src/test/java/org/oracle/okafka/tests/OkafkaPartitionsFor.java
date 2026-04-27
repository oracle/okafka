package org.oracle.okafka.tests;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.junit.Test;
import org.oracle.okafka.clients.admin.AdminClient;
import org.oracle.okafka.clients.consumer.KafkaConsumer;
import org.oracle.okafka.clients.producer.KafkaProducer;

public class OkafkaPartitionsFor {

	private static final String TOPIC_NAME = "TEQ_PARTITIONS_FOR";

	@Test
	public void PartitionsForTest() {
		String topic = TOPIC_NAME;

		Properties prop = OkafkaSetup.setup();
		prop.put("group.id", "S1");
		prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		Properties producerProps = OkafkaSetup.setup();
		producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Admin admin = null;
		Consumer<String, String> consumer = null;
		Producer<String, String> producer = null;
		try {
			admin = AdminClient.create(OkafkaSetup.setup());
			if (admin.listTopics().names().get().contains(topic)) {
				admin.deleteTopics(Arrays.asList(topic)).all().get();
				System.out.println("Deleted existing topic before partitionsFor test: " + topic);
			}
			admin.createTopics(Arrays.asList(new NewTopic(topic, 3, (short) 1))).all().get();
			System.out.println("Created topic with 3 partitions for partitionsFor test: " + topic);

			producer = new KafkaProducer<String, String>(producerProps);
			producer.send(new ProducerRecord<String, String>(topic, 0, "K0", "V0")).get();
			producer.send(new ProducerRecord<String, String>(topic, 1, "K1", "V1")).get();
			producer.send(new ProducerRecord<String, String>(topic, 2, "K2", "V2")).get();
			producer.flush();
			System.out.println("Produced seed messages to partitions 0, 1 and 2 for topic: " + topic);

			consumer = new KafkaConsumer<String, String>(prop);
			List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);

			System.out.println("Consumer partition info for topic " + topic + ": " + partitionInfos);

		} catch (Exception e) {
			System.out.println("Exception while fetching partitions for topic " + topic + " " + e);
			e.printStackTrace();
		} finally {
			if (producer != null) {
				System.out.println("Closing Producer");
				producer.close();
			}
			if (consumer != null) {
				System.out.println("Closing Consumer");
				consumer.close();
			}
			if (admin != null) {
				try {
					admin.deleteTopics(Arrays.asList(topic)).all().get();
					System.out.println("Deleted topic after partitionsFor test: " + topic);
				} catch (Exception cleanupEx) {
					System.out.println("Exception while deleting topic " + topic + " " + cleanupEx);
					cleanupEx.printStackTrace();
				}
				admin.close();
			}
		}

		System.out.println("Test: OkafkaPartitionsFor completed");
	}
}
