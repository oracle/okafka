package org.oracle.okafka.tests;

import java.util.Collection;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.oracle.okafka.clients.admin.AdminClient;

import org.junit.Test;

public class ListConsumerGroups {
	
	@Test
	public void ListConsumerGroupsTest() {

		try (Admin admin = AdminClient.create(OkafkaSetup.setup())) {

			ListConsumerGroupsResult result = admin.listConsumerGroups();
			try {
				Collection<ConsumerGroupListing> consumerGroups = result.all().get();
				String groupNames = consumerGroups.stream().map(ConsumerGroupListing::groupId)
						.collect(Collectors.joining(", "));
				System.out.println("Consumer Groups: " + groupNames);
				System.out.println("Main Thread Out of wait now");
			} catch (Exception e) {
				System.out.println(e);
			}
			System.out.println("Auto Closing admin now");
		} catch (Exception e) {
			System.out.println("Exception while listing Consumer Groups " + e);
			e.printStackTrace();
		}
		System.out.println("Test: ListConsumerGroups completed");
	}
}
