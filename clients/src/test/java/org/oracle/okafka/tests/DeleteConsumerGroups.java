package org.oracle.okafka.tests;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DeleteConsumerGroupsResult;
import org.apache.kafka.common.KafkaFuture;
import org.junit.Test;
import org.oracle.okafka.clients.admin.AdminClient;

public class DeleteConsumerGroups {

	@Test
	public void DeleteGroupsTest() {

		try (Admin admin = AdminClient.create(OkafkaSetup.setup())) {
			
			DeleteConsumerGroupsResult delResult = admin.deleteConsumerGroups(new ArrayList<>(Arrays.asList("S1")));
			try {
				KafkaFuture<Void> groupFutures = delResult.all();
				groupFutures.get();
				System.out.println("Main Thread Out of wait now");
			} catch (InterruptedException | ExecutionException e) {

				throw new IllegalStateException(e);
			}
			System.out.println("Auto Closing admin now");
		} catch (Exception e) {
			System.out.println("Exception while deleting groups " + e);
			e.printStackTrace();
		}
		System.out.println("Test: DeleteConsumerGroups completed");
	}

}
