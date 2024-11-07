package org.oracle.okafka.tests;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
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
        	CreateTopicsResult result = admin.createTopics(Arrays.asList(
					new NewTopic("TOPIC",5, (short)1)));
        	Uuid createdTopicId = result.topicId("TOPIC").get();
        	admin.close();
        	
        	Admin admin1 = AdminClient.create(OkafkaSetup.setup());
        	
			DescribeTopicsResult res = admin1.describeTopics(
					TopicCollection.TopicIdCollection.ofTopicIds(new ArrayList<Uuid>(Arrays.asList(createdTopicId))));

			Map<Uuid,KafkaFuture<TopicDescription>> description=res.topicIdValues();
        	
        	for(Map.Entry<Uuid,KafkaFuture<TopicDescription>> entry : description.entrySet()) {
        		System.out.println("Description - "+entry.getValue().get());
        	}
		}
		catch(Exception e)
		{
			System.out.println("Exception while Describing topic " + e);
			e.printStackTrace();
		}
		
		System.out.println("Test: OkafkaDescribeTopicsById Complete");

	}
}
