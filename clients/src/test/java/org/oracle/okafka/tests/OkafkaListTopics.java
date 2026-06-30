package org.oracle.okafka.tests;

import java.util.Set;

import org.apache.kafka.clients.admin.Admin;
import org.junit.Assert;
import org.junit.Test;

public class OkafkaListTopics {

	@Test(timeout = 120000)
	public void okafkaListTopicsTest() throws Exception {
		String topic = OkafkaTestSupport.uniqueTopic("TEQ_LIST_TOPICS");
		try (Admin admin = OkafkaTestSupport.admin()) {
			try {
				OkafkaTestSupport.createTopic(admin, topic, 1);
				Set<String> topics = OkafkaTestSupport.get(admin.listTopics().names(), "list topics");
				Assert.assertTrue("Created topic should be returned by listTopics", topics.contains(topic));
			} finally {
				OkafkaTestSupport.deleteTopicIfExists(admin, topic);
			}
		}
	}
}
