package org.oracle.okafka.tests;


import org.apache.kafka.clients.admin.Admin;
import org.junit.Test;
import org.oracle.okafka.clients.admin.AdminClient;
import org.oracle.okafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;

public class OkafkaListTopics {

	@Test
	public void AdminTest() {
        try (Admin admin = AdminClient.create(OkafkaSetup.setup())) {
        	
        	KafkaAdminClient kAdminClient = (((org.oracle.okafka.clients.admin.KafkaAdminClient)admin));
        	
        	ListTopicsResult res=kAdminClient.listTopics(new ListTopicsOptions());
        	Thread.sleep(20000);

        	System.out.println(res.namesToListings());
		}
		catch(Exception e)
		{
			System.out.println("Exception while creating topic " + e);
			e.printStackTrace();
		}
		
		System.out.println("Main thread complete ");

	}
}