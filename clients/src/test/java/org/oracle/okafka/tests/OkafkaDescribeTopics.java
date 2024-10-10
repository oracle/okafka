package org.oracle.okafka.tests;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.common.TopicCollection;
import org.junit.Test;
import org.oracle.okafka.clients.admin.AdminClient;
import org.oracle.okafka.clients.admin.KafkaAdminClient;

public class OkafkaDescribeTopics {

	@Test
	public void AdminTest() {
        try (Admin admin = AdminClient.create(OkafkaSetup.setup())) {
        	


        	KafkaAdminClient kAdminClient = (((org.oracle.okafka.clients.admin.KafkaAdminClient)admin));
        	DescribeTopicsResult res=kAdminClient.describeTopics
        			(TopicCollection.TopicNameCollection.ofTopicNames(new ArrayList<String> (Arrays.asList("KTOPIC1"))));
        	Thread.sleep(20000);
//        	TopicDescription td= res.topicNameValues().get("TEQ").get();
//        	List<TopicPartitionInfo> ls=td.partitions();
//        	for(int i=0;i<ls.size();i++) {
//        		System.out.println(ls.get(i).partition()+"---");
//        	}
        	System.out.println(res.topicNameValues());
        	
		}
		catch(Exception e)
		{
			System.out.println("Exception while creating topic " + e);
			e.printStackTrace();
		}
		
		System.out.println("Main thread complete ");

	}
}