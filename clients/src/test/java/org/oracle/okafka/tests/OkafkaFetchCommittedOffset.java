package org.oracle.okafka.tests;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Collections;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.Test;

public class OkafkaFetchCommittedOffset {

	@Test(timeout = 120000)
	public void okafkaFetchCommittedOffsetTest() throws Exception {
		String topic = OkafkaTestSupport.uniqueTopic("TEQ_COMMITTED");
		String groupId = OkafkaTestSupport.uniqueGroup("G_COMMITTED");
		TopicPartition topicPartition = new TopicPartition(topic, 0);
		Set<TopicPartition> topicPartitions = Collections.singleton(topicPartition);
		try (Admin admin = OkafkaTestSupport.admin()) {
			try {
				OkafkaTestSupport.createTopic(admin, topic, 1);
				try (Producer<String, String> producer = OkafkaTestSupport.producer()) {
					OkafkaTestSupport.produceRecords(producer, topic, 3);
				}
				Properties prop = OkafkaTestSupport.consumerProperties(groupId);
				prop.put("auto.offset.reset", "earliest");
				prop.put("max.poll.records", "3");
				try (Consumer<String, String> consumer = OkafkaTestSupport.consumer(prop)) {
					consumer.subscribe(Arrays.asList(topic));
					OkafkaTestSupport.consumeExactly(consumer, 3);
					Map<TopicPartition, OffsetAndMetadata> committedMap = consumer.committed(topicPartitions);
					Assert.assertNotNull("Committed offset should be present after commit",
							committedMap.get(topicPartition));
					Assert.assertEquals("Committed offset should equal consumed record count", 3L,
							committedMap.get(topicPartition).offset());
				}
			} finally {
				OkafkaTestSupport.deleteTopicIfExists(admin, topic);
			}
		}
	}
}
