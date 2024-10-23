package org.oracle.okafka.tests;


import org.apache.kafka.clients.admin.Admin;
import org.junit.Test;
import org.oracle.okafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;

public class OkafkaListTopics {

	@Test
	public void AdminTest() {
        try (Admin admin = AdminClient.create(OkafkaSetup.setup())) {
        	
        	ListTopicsResult res=admin.listTopics(new ListTopicsOptions());
        	
        	System.out.println(res.names().get());

		}
		catch(Exception e)
		{
			System.out.println("Exception while Listing Topics " + e);
			e.printStackTrace();
		}
		
		System.out.println("Test: OkafkaListTopics complete");

	}
}