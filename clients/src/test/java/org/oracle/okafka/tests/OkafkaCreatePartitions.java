package org.oracle.okafka.tests;

import java.util.Map;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.TopicDescription;
import org.junit.Assert;
import org.junit.Test;

public class OkafkaCreatePartitions {
	@Test(timeout = 120000)
	public void okafkaCreatePartitionsTest() throws Exception {
		String topic = OkafkaTestSupport.uniqueTopic("TEQ_CREATE_PARTITIONS");
		try (Admin admin = OkafkaTestSupport.admin()) {
			try {
				OkafkaTestSupport.createTopic(admin, topic, 3);
				OkafkaTestSupport.get(admin.createPartitions(Map.of(topic, NewPartitions.increaseTo(6))).all(),
						"create partitions");
				TopicDescription description = OkafkaTestSupport.get(admin.describeTopics(java.util.Arrays.asList(topic))
						.topicNameValues().get(topic), "describe topic");
				Assert.assertEquals("Topic should have been expanded to six partitions", 6,
						description.partitions().size());
			} finally {
				OkafkaTestSupport.deleteTopicIfExists(admin, topic);
			}
		}
	}
}
