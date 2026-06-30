package org.oracle.okafka.tests;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.junit.Assert;
import org.junit.Test;

public class OkafkaUnsubscribe {

	@Test(timeout = 120000)
	public void okafkaUnsubscribeTest() throws Exception {
		String topic = OkafkaTestSupport.uniqueTopic("TEQ_UNSUBSCRIBE");
		String groupId = OkafkaTestSupport.uniqueGroup("G_UNSUBSCRIBE");
		try (Admin admin = OkafkaTestSupport.admin()) {
			try {
				OkafkaTestSupport.createTopic(admin, topic, 1);
				try (Producer<String, String> producer = OkafkaTestSupport.producer()) {
					OkafkaTestSupport.produceRecords(producer, topic, 3);
				}

				Properties prop = OkafkaTestSupport.consumerProperties(groupId);
				prop.put("max.poll.records", "3");
				prop.put("auto.offset.reset", "earliest");
				try (Consumer<String, String> consumer = OkafkaTestSupport.consumer(prop)) {
					consumer.subscribe(Arrays.asList(topic));
					OkafkaTestSupport.consumeExactly(consumer, 3);

					consumer.unsubscribe();
					Assert.assertTrue("Subscription should be empty after unsubscribe", consumer.subscription().isEmpty());
					Assert.assertTrue("Assignment should be empty after unsubscribe", consumer.assignment().isEmpty());
				}
			} finally {
				OkafkaTestSupport.deleteTopicIfExists(admin, topic);
			}
		}
	}
}
