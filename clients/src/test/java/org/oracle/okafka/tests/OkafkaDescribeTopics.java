package org.oracle.okafka.tests;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicCollection;
import org.apache.kafka.common.Uuid;
import org.junit.Test;
import org.oracle.okafka.clients.admin.AdminClient;
import org.oracle.okafka.clients.admin.KafkaAdminClient;

public class OkafkaDescribeTopics {

	@Test
	public void AdminTest() {
        try (Admin admin = AdminClient.create(OkafkaSetup.setup())) {
        	DescribeTopicsResult res=admin.describeTopics
        			(TopicCollection.TopicNameCollection.ofTopicNames(new ArrayList<String> (Arrays.asList("TE1"))));
        
        	Map<String,KafkaFuture<TopicDescription>> description=res.topicNameValues();
        	
        	for(Map.Entry<String,KafkaFuture<TopicDescription>> entry : description.entrySet()) {
        		System.out.println("Description - "+entry.getValue().get());
        	}
        	
		}
		catch(Exception e)
		{
			System.out.println("Exception while Describing topic " + e);
			e.printStackTrace();
		}
		
		System.out.println("Test: OkafkaDescribeTopic Complete");

	}
}