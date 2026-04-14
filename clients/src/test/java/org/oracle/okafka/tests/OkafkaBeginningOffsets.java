package org.oracle.okafka.tests;

import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.Test;
import org.oracle.okafka.clients.consumer.KafkaConsumer;

public class OkafkaBeginningOffsets {

	@Test
	public void BeginningOffsetsTest() {
		Properties prop = OkafkaSetup.setup();
		prop.put("group.id", "S1");
		prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		Consumer<String, String> consumer = new KafkaConsumer<String, String>(prop);
		try {
			Set<TopicPartition> topicPartitions = new HashSet<>();
			topicPartitions.add(new TopicPartition("TEQ", 0));
			topicPartitions.add(new TopicPartition("TEQ", 1));
			topicPartitions.add(new TopicPartition("TEQ", 2));

			Map<TopicPartition, Long> consumerOffsets = consumer.beginningOffsets(topicPartitions);
			System.out.println("Consumer beginning offsets: " + consumerOffsets);
			for (TopicPartition tp : topicPartitions) {
				Long consumerOffset = consumerOffsets.get(tp);
				System.out.println("TopicPartition " + tp + " consumerOffset=" + consumerOffset);
			}
		} catch (Exception e) {
			System.out.println("Exception while fetching beginning offsets " + e);
			e.printStackTrace();
		} finally {
			System.out.println("Closing Consumer");
			consumer.close();
		}

		System.out.println("Test: OkafkaBeginningOffsets completed");
	}
}
