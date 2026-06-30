package org.oracle.okafka.tests;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.Test;

public class OkafkaSeekToEnd {

	@Test(timeout = 120000)
	public void okafkaSeekToEndTest() throws Exception {
		String topic = OkafkaTestSupport.uniqueTopic("TEQ_SEEK_END");
		String groupId = OkafkaTestSupport.uniqueGroup("G_SEEK_END");
		TopicPartition topicPartition = new TopicPartition(topic, 0);
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
					OkafkaTestSupport.waitForAssignment(consumer, Collections.singleton(topicPartition));
					consumer.seekToEnd(Collections.singleton(topicPartition));
					int consumedBeforeNewRecords = OkafkaTestSupport.consumeUpTo(consumer, 1, Duration.ofSeconds(5));
					Assert.assertEquals("seekToEnd should skip existing records", 0, consumedBeforeNewRecords);

					try (Producer<String, String> producer = OkafkaTestSupport.producer()) {
						OkafkaTestSupport.produceRecords(producer, topic, 10, 1);
					}
					int consumedAfterNewRecords = OkafkaTestSupport.consumeUpTo(consumer, 1, Duration.ofSeconds(60));
					Assert.assertEquals("After seekToEnd, consumer should read newly produced records", 1,
							consumedAfterNewRecords);
				}
			} finally {
				OkafkaTestSupport.deleteTopicIfExists(admin, topic);
			}
		}
	}
}
