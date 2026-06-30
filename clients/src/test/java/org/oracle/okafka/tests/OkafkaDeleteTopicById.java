package org.oracle.okafka.tests;

import java.util.Arrays;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicCollection;
import org.apache.kafka.common.Uuid;
import org.junit.Assert;
import org.junit.Test;

public class OkafkaDeleteTopicById {

	@Test(timeout = 120000)
	public void okafkaDeleteTopicByIdTest() throws Exception {
		String topic = OkafkaTestSupport.uniqueTopic("TEQ_DELETE_TOPIC_ID");
		try (Admin admin = OkafkaTestSupport.admin()) {
			OkafkaTestSupport.createTopic(admin, topic, 1);
			TopicDescription description = OkafkaTestSupport
					.get(admin.describeTopics(Arrays.asList(topic)).topicNameValues().get(topic), "describe topic");
			Uuid topicId = description.topicId();

			OkafkaTestSupport.get(admin.deleteTopics(TopicCollection.TopicIdCollection.ofTopicIds(Arrays.asList(topicId)))
					.all(), "delete topic by id");
			Assert.assertFalse("Deleted topic should no longer be listed",
					OkafkaTestSupport.get(admin.listTopics().names(), "list topics").contains(topic));
		}
	}
}
