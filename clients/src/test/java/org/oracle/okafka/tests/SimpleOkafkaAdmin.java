package org.oracle.okafka.tests;

import java.util.Arrays;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.junit.Test;
import org.oracle.okafka.clients.admin.AdminClient;

public class SimpleOkafkaAdmin {

	@Test
	public void AdminTest() {

		try (Admin admin = AdminClient.create(OkafkaSetup.setup())) {
			CreateTopicsResult result = admin.createTopics(Arrays.asList(new NewTopic("TEQ", 5, (short) 1)));
			try {
				KafkaFuture<Void> ftr = result.all();
				ftr.get();
				System.out.println("Main Thread Out of wait now");
			} catch (InterruptedException | ExecutionException e) {

				throw new IllegalStateException(e);
			}
			System.out.println("Auto Closing admin now");

		} catch (Exception e) {
			System.out.println("Exception while creating topic " + e);
			e.printStackTrace();
		}

		System.out.println("Main thread complete ");

	}
}