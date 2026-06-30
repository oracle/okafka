package org.oracle.okafka.tests;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.junit.Assert;
import org.junit.Test;

public class DeleteConsumerGroups {

	@Test(timeout = 120000)
	public void deleteConsumerGroupsTest() throws Exception {
		String topic = OkafkaTestSupport.uniqueTopic("TEQ_DELETE_GROUP");
		String groupId = OkafkaTestSupport.uniqueGroup("G_DELETE_GROUP");
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
				OkafkaTestSupport.get(admin.deleteConsumerGroups(Arrays.asList(groupId)).all(),
						"delete consumer group");
				OkafkaTestSupport.eventually("consumer group to be deleted: " + groupId, Duration.ofSeconds(30),
						() -> OkafkaTestSupport.get(admin.listConsumerGroups().all(), "list consumer groups")
								.stream().noneMatch(group -> group.groupId().equals(groupId)));
				boolean stillListed = OkafkaTestSupport.get(admin.listConsumerGroups().all(), "list consumer groups")
						.stream().anyMatch(group -> group.groupId().equals(groupId));
				Assert.assertFalse("Deleted consumer group should not be listed: " + groupId, stillListed);
			} finally {
				OkafkaTestSupport.deleteTopicIfExists(admin, topic);
			}
		}
	}
}
