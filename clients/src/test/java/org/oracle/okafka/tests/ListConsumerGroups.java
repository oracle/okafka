package org.oracle.okafka.tests;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.junit.Assert;
import org.junit.Test;

public class ListConsumerGroups {
	
	@Test(timeout = 120000)
	public void listConsumerGroupsTest() throws Exception {
		String topic = OkafkaTestSupport.uniqueTopic("TEQ_LIST_GROUPS");
		String groupId = OkafkaTestSupport.uniqueGroup("G_LIST_GROUPS");
		try (Admin admin = OkafkaTestSupport.admin()) {
			try {
				OkafkaTestSupport.createTopic(admin, topic, 1);
				try (Producer<String, String> producer = OkafkaTestSupport.producer()) {
					OkafkaTestSupport.produceRecords(producer, topic, 1);
				}
				Properties props = OkafkaTestSupport.consumerProperties(groupId);
				props.put("auto.offset.reset", "earliest");
				try (Consumer<String, String> consumer = OkafkaTestSupport.consumer(props)) {
					consumer.subscribe(Arrays.asList(topic));
					OkafkaTestSupport.consumeExactly(consumer, 1);
				}
				OkafkaTestSupport.eventually("consumer group to be listed: " + groupId, Duration.ofSeconds(30),
						() -> OkafkaTestSupport.get(admin.listConsumerGroups().all(), "list consumer groups")
								.stream().anyMatch(group -> group.groupId().equals(groupId)));
				Assert.assertTrue("Committed consumer group should be listed: " + groupId,
						OkafkaTestSupport.get(admin.listConsumerGroups().all(), "list consumer groups")
								.stream().anyMatch(group -> group.groupId().equals(groupId)));
			} finally {
				OkafkaTestSupport.deleteTopicIfExists(admin, topic);
			}
		}
	}
}
