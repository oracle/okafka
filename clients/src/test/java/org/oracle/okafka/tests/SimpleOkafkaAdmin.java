package org.oracle.okafka.tests;

import org.apache.kafka.clients.admin.Admin;
import org.junit.Assert;
import org.junit.Test;

public class SimpleOkafkaAdmin {

	@Test(timeout = 120000)
	public void simpleOkafkaAdminTest() throws Exception {
		String topic = OkafkaTestSupport.uniqueTopic("TEQ_ADMIN");
		try (Admin admin = OkafkaTestSupport.admin()) {
			try {
				OkafkaTestSupport.createTopic(admin, topic, 5);
				Assert.assertTrue("Created topic should be returned by listTopics",
						OkafkaTestSupport.get(admin.listTopics().names(), "list topics").contains(topic));
			} finally {
				OkafkaTestSupport.deleteTopicIfExists(admin, topic);
			}
		}
	}
}
