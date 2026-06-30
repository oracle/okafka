package org.oracle.okafka.tests;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.Test;

public class OkafkaBeginningOffsets {

	@Test(timeout = 120000)
	public void okafkaBeginningOffsetsTest() throws Exception {
		String topic = OkafkaTestSupport.uniqueTopic("TEQ_BEGINNING");
		String groupId = OkafkaTestSupport.uniqueGroup("G_BEGINNING");
		TopicPartition topicPartition = new TopicPartition(topic, 0);
		Set<TopicPartition> topicPartitions = Collections.singleton(topicPartition);

		try (Admin admin = OkafkaTestSupport.admin()) {
			try {
				OkafkaTestSupport.createTopic(admin, topic, 1);
				try (Consumer<String, String> consumer = OkafkaTestSupport
						.consumer(OkafkaTestSupport.consumerProperties(groupId))) {
					Map<TopicPartition, Long> consumerOffsets = consumer.beginningOffsets(topicPartitions);
					Assert.assertEquals("Beginning offset should be available for the test partition", 1,
							consumerOffsets.size());
					Assert.assertEquals("New topic should begin at offset zero", 0L,
							consumerOffsets.get(topicPartition).longValue());
				}
			} finally {
				OkafkaTestSupport.deleteTopicIfExists(admin, topic);
			}
		}
	}
}
