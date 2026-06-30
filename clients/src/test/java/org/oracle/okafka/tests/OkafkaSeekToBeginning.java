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

public class OkafkaSeekToBeginning {

	@Test(timeout = 120000)
	public void okafkaSeekToBeginningTest() throws Exception {
		String topic = OkafkaTestSupport.uniqueTopic("TEQ_SEEK_BEGIN");
		String groupId = OkafkaTestSupport.uniqueGroup("G_SEEK_BEGIN");
		TopicPartition topicPartition = new TopicPartition(topic, 0);
		try (Admin admin = OkafkaTestSupport.admin()) {
			try {
				OkafkaTestSupport.createTopic(admin, topic, 1);
				try (Producer<String, String> producer = OkafkaTestSupport.producer()) {
					OkafkaTestSupport.produceRecords(producer, topic, 10);
				}

				Properties prop = OkafkaTestSupport.consumerProperties(groupId);
				prop.put("max.poll.records", "10");
				prop.put("auto.offset.reset", "latest");
				try (Consumer<String, String> consumer = OkafkaTestSupport.consumer(prop)) {
					consumer.subscribe(Arrays.asList(topic));
					OkafkaTestSupport.waitForAssignment(consumer, Collections.singleton(topicPartition));
					consumer.seekToBeginning(Collections.singleton(topicPartition));
					int consumed = OkafkaTestSupport.consumeUpTo(consumer, 10, Duration.ofSeconds(60));
					Assert.assertEquals("seekToBeginning should allow reading all existing records", 10, consumed);
				}
			} finally {
				OkafkaTestSupport.deleteTopicIfExists(admin, topic);
			}
		}
	}
}
