package org.oracle.okafka.tests;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.Test;

public class ListConsumerGroupOffsets {

	@Test(timeout = 120000)
	public void listConsumerGroupOffsetsTest() throws Exception {
		String topic = OkafkaTestSupport.uniqueTopic("TEQ_GROUP_OFFSETS");
		String groupId = OkafkaTestSupport.uniqueGroup("G_GROUP_OFFSETS");
		TopicPartition topicPartition = new TopicPartition(topic, 0);
		try (Admin admin = OkafkaTestSupport.admin()) {
			try {
				OkafkaTestSupport.createTopic(admin, topic, 1);
				try (Producer<String, String> producer = OkafkaTestSupport.producer()) {
					OkafkaTestSupport.produceRecords(producer, topic, 2);
				}
				Properties props = OkafkaTestSupport.consumerProperties(groupId);
				props.put("auto.offset.reset", "earliest");
				props.put("max.poll.records", "2");
				try (Consumer<String, String> consumer = OkafkaTestSupport.consumer(props)) {
					consumer.subscribe(Arrays.asList(topic));
					OkafkaTestSupport.consumeExactly(consumer, 2);
				}

				ListConsumerGroupOffsetsSpec spec = new ListConsumerGroupOffsetsSpec()
						.topicPartitions(Collections.singletonList(topicPartition));
				ListConsumerGroupOffsetsResult result = admin
						.listConsumerGroupOffsets(Collections.singletonMap(groupId, spec));
				Map<String, Map<TopicPartition, OffsetAndMetadata>> offsets = OkafkaTestSupport.get(result.all(),
						"list consumer group offsets");
				Assert.assertTrue("Result should include requested consumer group", offsets.containsKey(groupId));
				Assert.assertEquals("Committed group offset should equal consumed count", 2L,
						offsets.get(groupId).get(topicPartition).offset());
			} finally {
				OkafkaTestSupport.deleteTopicIfExists(admin, topic);
			}
		}
	}
}
