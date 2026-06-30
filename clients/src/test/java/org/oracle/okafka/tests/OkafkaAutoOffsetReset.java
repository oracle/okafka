package org.oracle.okafka.tests;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.junit.Assert;
import org.junit.Test;

public class OkafkaAutoOffsetReset {

	@Test(timeout = 120000)
	public void okafkaAutoOffsetResetTest() throws Exception {
		String topic = OkafkaTestSupport.uniqueTopic("TEQ_AUTO_OFFSET");
		String groupId = OkafkaTestSupport.uniqueGroup("G_AUTO_OFFSET");
		try (Admin admin = OkafkaTestSupport.admin()) {
			try {
				OkafkaTestSupport.createTopic(admin, topic, 1);
				try (Producer<String, String> producer = OkafkaTestSupport.producer()) {
					OkafkaTestSupport.produceRecords(producer, topic, 10);
				}

				Properties prop = OkafkaTestSupport.consumerProperties(groupId);
				prop.put("max.poll.records", "10");
				prop.put("auto.offset.reset", "earliest");
				try (Consumer<String, String> consumer = OkafkaTestSupport.consumer(prop)) {
					consumer.subscribe(Arrays.asList(topic));
					int consumed = OkafkaTestSupport.consumeExactly(consumer, 10);
					Assert.assertEquals("Earliest reset should consume all existing records for a new group", 10,
							consumed);
				}
			} finally {
				OkafkaTestSupport.deleteTopicIfExists(admin, topic);
			}
		}
	}
}
