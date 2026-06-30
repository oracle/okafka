package org.oracle.okafka.tests;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.junit.Assert;
import org.junit.Test;

public class SimpleOkafkaConsumer {

	@Test(timeout = 120000)
	public void simpleOkafkaConsumerTest() throws Exception {
		String topic = OkafkaTestSupport.uniqueTopic("TEQ_CONSUMER");
		String groupId = OkafkaTestSupport.uniqueGroup("G_CONSUMER");
		try (Admin admin = OkafkaTestSupport.admin()) {
			try {
				OkafkaTestSupport.createTopic(admin, topic, 1);
				try (Producer<String, String> producer = OkafkaTestSupport.producer()) {
					OkafkaTestSupport.produceRecords(producer, topic, 1000);
				}

				Properties prop = OkafkaTestSupport.consumerProperties(groupId);
				prop.put("max.poll.records", "1000");
				prop.put("auto.offset.reset", "earliest");
				try (Consumer<String, String> consumer = OkafkaTestSupport.consumer(prop)) {
					consumer.subscribe(Arrays.asList(topic));
					int consumed = OkafkaTestSupport.consumeExactly(consumer, 1000);
					Assert.assertEquals("Consumer should read all produced records", 1000, consumed);
				}
			} finally {
				OkafkaTestSupport.deleteTopicIfExists(admin, topic);
			}
		}
	}
}
