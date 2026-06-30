package org.oracle.okafka.tests;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.Test;

public class OkafkaOffsetsForTimes {

	@Test(timeout = 120000)
	public void okafkaOffsetsForTimesTest() throws Exception {
		String topic = OkafkaTestSupport.uniqueTopic("TEQ_OFFSETS_FOR_TIMES");
		String groupId = OkafkaTestSupport.uniqueGroup("G_OFFSETS_FOR_TIMES");
		TopicPartition topicPartition = new TopicPartition(topic, 0);

		try (Admin admin = OkafkaTestSupport.admin()) {
			try {
				OkafkaTestSupport.createTopic(admin, topic, 1);
				long secondSendTime;
				try (Producer<String, String> producer = OkafkaTestSupport.producer()) {
					OkafkaTestSupport.get(producer.send(new ProducerRecord<String, String>(topic, "k1", "message-1")),
							"produce first timestamped record");
					Thread.sleep(1000L);
					secondSendTime = System.currentTimeMillis();
					OkafkaTestSupport.get(producer.send(new ProducerRecord<String, String>(topic, "k2", "message-2")),
							"produce second timestamped record");
					Thread.sleep(1000L);
					OkafkaTestSupport.get(producer.send(new ProducerRecord<String, String>(topic, "k3", "message-3")),
							"produce third timestamped record");
					producer.flush();
				}

				Properties consumerProperties = OkafkaTestSupport.consumerProperties(groupId);
				try (Consumer<String, String> consumer = OkafkaTestSupport.consumer(consumerProperties)) {
					Map<TopicPartition, OffsetAndTimestamp> result = consumer
							.offsetsForTimes(Collections.singletonMap(topicPartition, secondSendTime));
					Assert.assertTrue("offsetsForTimes result should include requested partition",
							result.containsKey(topicPartition));
					Assert.assertNotNull("offsetsForTimes should return a timestamped offset",
							result.get(topicPartition));
					Assert.assertEquals("Timestamp lookup should resolve to the second produced record", 1L,
							result.get(topicPartition).offset());
				}
			} finally {
				OkafkaTestSupport.deleteTopicIfExists(admin, topic);
			}
		}
	}
}
