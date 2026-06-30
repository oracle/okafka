package org.oracle.okafka.tests;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.Test;

public class OkafkaEndOffsets {

	@Test(timeout = 120000)
	public void okafkaEndOffsetsTest() throws Exception {
		String topic = OkafkaTestSupport.uniqueTopic("TEQ_END");
		String groupId = OkafkaTestSupport.uniqueGroup("G_END");
		TopicPartition topicPartition = new TopicPartition(topic, 0);
		Set<TopicPartition> topicPartitions = Collections.singleton(topicPartition);

		try (Admin admin = OkafkaTestSupport.admin()) {
			try {
				OkafkaTestSupport.createTopic(admin, topic, 1);
				try (Producer<String, String> producer = OkafkaTestSupport.producer()) {
					OkafkaTestSupport.produceRecords(producer, topic, 5);
				}
				try (Consumer<String, String> consumer = OkafkaTestSupport
						.consumer(OkafkaTestSupport.consumerProperties(groupId))) {
					Map<TopicPartition, Long> consumerOffsets = consumer.endOffsets(topicPartitions);
					Assert.assertEquals("End offset should be available for the test partition", 1,
							consumerOffsets.size());
					Assert.assertEquals("End offset should equal produced record count", 5L,
							consumerOffsets.get(topicPartition).longValue());
				}
			} finally {
				OkafkaTestSupport.deleteTopicIfExists(admin, topic);
			}
		}
	}
}
