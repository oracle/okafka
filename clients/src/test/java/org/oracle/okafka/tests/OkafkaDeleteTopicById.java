package org.oracle.okafka.tests;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicCollection;
import org.apache.kafka.common.Uuid;
import org.junit.Test;
import org.oracle.okafka.clients.admin.AdminClient;

public class OkafkaDeleteTopicById {
	@Test
	public void DeleteTopicByIdTest() {
		 try (Admin admin = AdminClient.create(OkafkaSetup.setup())) {
			 
			CreateTopicsResult result = admin.createTopics(Arrays.asList(
						new NewTopic("TOPIC",5, (short)1)));
			Uuid createdTopicId = result.topicId("TOPIC").get();
			DeleteTopicsResult delResult = admin.deleteTopics(TopicCollection.TopicNameCollection.ofTopicIds(new ArrayList<Uuid> (Arrays.asList(createdTopicId))));
			try {
				KafkaFuture<Void> ftr =  delResult.all();
				ftr.get();
				System.out.println("Main Thread Out of wait now");
			} catch ( InterruptedException | ExecutionException e ) {

				throw new IllegalStateException(e);
			}
			System.out.println("Auto Closing admin now");
		}
		catch(Exception e)
		{
			System.out.println("Exception while deleting topic " + e);
			e.printStackTrace();
		}
		System.out.println("Test: OkfakaDeleteTopicById completed");
	}
}
