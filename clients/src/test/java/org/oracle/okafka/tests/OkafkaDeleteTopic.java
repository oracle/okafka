package org.oracle.okafka.tests;

import java.util.Arrays;

import org.apache.kafka.clients.admin.Admin;
import org.junit.Assert;
import org.junit.Test;

public class OkafkaDeleteTopic {

	@Test(timeout = 120000)
	public void okafkaDeleteTopicTest() throws Exception {
		String topic = OkafkaTestSupport.uniqueTopic("TEQ_DELETE_TOPIC");
		try (Admin admin = OkafkaTestSupport.admin()) {
			OkafkaTestSupport.createTopic(admin, topic, 1);
			OkafkaTestSupport.get(admin.deleteTopics(Arrays.asList(topic)).all(), "delete topic");
			Assert.assertFalse("Deleted topic should no longer be listed",
					OkafkaTestSupport.get(admin.listTopics().names(), "list topics").contains(topic));
		}
	}
}
