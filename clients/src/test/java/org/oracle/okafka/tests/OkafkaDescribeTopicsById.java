package org.oracle.okafka.tests;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicCollection;
import org.apache.kafka.common.Uuid;
import org.junit.Test;
import org.oracle.okafka.clients.admin.AdminClient;

public class OkafkaDescribeTopicsById {
	@Test
	public void AdminTest() {
		try (Admin admin = AdminClient.create(OkafkaSetup.setup())) {

			DescribeTopicsResult res1 = admin.describeTopics(
					TopicCollection.TopicNameCollection.ofTopicNames(new ArrayList<String>(Arrays.asList("TEQ"))));

			Map<String, KafkaFuture<TopicDescription>> description1 = res1.topicNameValues();

			Uuid topicId = description1.get("TEQ").get().topicId();

			DescribeTopicsResult res2 = admin.describeTopics(
					TopicCollection.TopicIdCollection.ofTopicIds(new ArrayList<Uuid>(Arrays.asList(topicId))));

			Map<Uuid, KafkaFuture<TopicDescription>> descriptionById = res2.topicIdValues();

			for (Map.Entry<Uuid, KafkaFuture<TopicDescription>> entry : descriptionById.entrySet()) {
				System.out.println("Description - " + entry.getValue().get());
			}

		} catch (Exception e) {
			System.out.println("Exception while Describing topic " + e);
			e.printStackTrace();
		}

		System.out.println("Test: OkafkaDescribeTopicsById Complete");

	}
}
