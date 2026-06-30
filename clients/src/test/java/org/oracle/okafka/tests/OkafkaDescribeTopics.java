package org.oracle.okafka.tests;

import java.util.Arrays;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.TopicDescription;
import org.junit.Assert;
import org.junit.Test;

public class OkafkaDescribeTopics {

	@Test(timeout = 120000)
	public void okafkaDescribeTopicsTest() throws Exception {
		String topic = OkafkaTestSupport.uniqueTopic("TEQ_DESCRIBE");
		try (Admin admin = OkafkaTestSupport.admin()) {
			try {
				OkafkaTestSupport.createTopic(admin, topic, 2);
				TopicDescription description = OkafkaTestSupport
						.get(admin.describeTopics(Arrays.asList(topic)).topicNameValues().get(topic), "describe topic");
				Assert.assertEquals("Topic description should return requested topic", topic, description.name());
				Assert.assertEquals("Topic should have two partitions", 2, description.partitions().size());
			} finally {
				OkafkaTestSupport.deleteTopicIfExists(admin, topic);
			}
		}
	}
}
