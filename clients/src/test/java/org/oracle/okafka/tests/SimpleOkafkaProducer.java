package org.oracle.okafka.tests;

import java.util.Collections;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.Test;

public class SimpleOkafkaProducer {

	@Test(timeout = 120000)
	public void simpleOkafkaProducerTest() throws Exception {
		String topic = OkafkaTestSupport.uniqueTopic("TEQ_PRODUCER");
		TopicPartition topicPartition = new TopicPartition(topic, 0);
		try (Admin admin = OkafkaTestSupport.admin()) {
			try {
				OkafkaTestSupport.createTopic(admin, topic, 1);
				try (Producer<String, String> producer = OkafkaTestSupport.producer()) {
					OkafkaTestSupport.produceRecords(producer, topic, 1000);
				}
				Long endOffset = OkafkaTestSupport.get(
						admin.listOffsets(Collections.singletonMap(topicPartition,
								org.apache.kafka.clients.admin.OffsetSpec.latest()))
								.partitionResult(topicPartition),
						"fetch latest offset").offset();
				Assert.assertEquals("Produced message count should match latest offset", 1000L, endOffset.longValue());
			} finally {
				OkafkaTestSupport.deleteTopicIfExists(admin, topic);
			}
		}
	}
}
