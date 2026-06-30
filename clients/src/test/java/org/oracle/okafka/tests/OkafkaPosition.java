package org.oracle.okafka.tests;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.Test;

public class OkafkaPosition {

	private static final int TOTAL_MESSAGES = 1000;
	private static final int MESSAGES_TO_CONSUME = TOTAL_MESSAGES / 2;

	@Test(timeout = 120000)
	public void PositionTracksCurrentConsumerProgressWithoutCommitTest() throws Exception {
		String topic = OkafkaTestSupport.uniqueTopic("TEQ_POSITION");
		String groupId = OkafkaTestSupport.uniqueGroup("G_POSITION");
		TopicPartition topicPartition = new TopicPartition(topic, 0);

		try (Admin admin = OkafkaTestSupport.admin()) {
			try {
				OkafkaTestSupport.createTopic(admin, topic, 1);
				try (Producer<String, String> producer = OkafkaTestSupport.producer()) {
					OkafkaTestSupport.produceRecords(producer, topic, TOTAL_MESSAGES);
				}

				Properties consumerProps = OkafkaTestSupport.consumerProperties(groupId);
				consumerProps.put("enable.auto.commit", "false");
				consumerProps.put("auto.offset.reset", "earliest");
				consumerProps.put("max.poll.records", Integer.toString(MESSAGES_TO_CONSUME));

				try (Consumer<String, String> consumer = OkafkaTestSupport.consumer(consumerProps)) {
					consumer.subscribe(Arrays.asList(topic));
					int consumed = OkafkaTestSupport.consumeExactly(consumer, MESSAGES_TO_CONSUME, false);
					long position = consumer.position(topicPartition);

					Assert.assertEquals("Consumer should read half the produced records", MESSAGES_TO_CONSUME,
							consumed);
					Assert.assertEquals("Position should advance for consumed records even without commit",
							MESSAGES_TO_CONSUME, position);
					consumer.commitSync();
				}
			} finally {
				OkafkaTestSupport.deleteTopicIfExists(admin, topic);
			}
		}
	}
}
