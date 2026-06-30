package org.oracle.okafka.tests;

import java.util.Arrays;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicCollection;
import org.apache.kafka.common.Uuid;
import org.junit.Assert;
import org.junit.Test;

public class OkafkaDescribeTopicsById {
	@Test(timeout = 120000)
	public void okafkaDescribeTopicsByIdTest() throws Exception {
		String topic = OkafkaTestSupport.uniqueTopic("TEQ_DESCRIBE_ID");
		try (Admin admin = OkafkaTestSupport.admin()) {
			try {
				OkafkaTestSupport.createTopic(admin, topic, 1);
				TopicDescription byName = OkafkaTestSupport
						.get(admin.describeTopics(Arrays.asList(topic)).topicNameValues().get(topic), "describe topic");
				Uuid topicId = byName.topicId();
				TopicDescription byId = OkafkaTestSupport.get(admin
						.describeTopics(TopicCollection.TopicIdCollection.ofTopicIds(Arrays.asList(topicId)))
						.topicIdValues().get(topicId), "describe topic by id");

				Assert.assertEquals("Description by ID should return original topic name", topic, byId.name());
				Assert.assertEquals("Description by ID should return original topic ID", topicId, byId.topicId());
			} finally {
				OkafkaTestSupport.deleteTopicIfExists(admin, topic);
			}
		}
	}
}
