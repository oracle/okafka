package org.oracle.okafka.tests;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreatePartitionsResult;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.common.KafkaFuture;
import org.oracle.okafka.clients.admin.AdminClient;

import org.junit.Test;

public class OkafkaCreatePartitions {
	@Test
	public void CreatePartitionsTest() {

		try (Admin admin = AdminClient.create(OkafkaSetup.setup())) {
			CreatePartitionsResult result = admin.createPartitions(Map.of("TEQ", NewPartitions.increaseTo(6)));
			try {
				KafkaFuture<Void> ftr = result.values().get("TEQ");
				ftr.get();
				System.out.println("Partitions created successfully. Main Thread Out of wait now");
			} catch (InterruptedException | ExecutionException e) {

				throw new IllegalStateException(e);
			}
			System.out.println("Auto Closing admin now");

		} catch (Exception e) {
			System.out.println("Exception while creating Partitions " + e);
			e.printStackTrace();
		}

		System.out.println("Main thread complete ");

	}
}
