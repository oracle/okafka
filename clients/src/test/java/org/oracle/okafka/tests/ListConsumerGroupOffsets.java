package org.oracle.okafka.tests;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;
import org.oracle.okafka.clients.admin.AdminClient;

public class ListConsumerGroupOffsets {

	@Test
	public void ListConsumerGroupOffsetsTest() {

		try (Admin admin = AdminClient.create(OkafkaSetup.setup())) {
			String groupId = "S1";
			List<TopicPartition> topicPartitions = new ArrayList<>();
			topicPartitions.add(new TopicPartition("TEQ", 0));
			topicPartitions.add(new TopicPartition("TEQ", 1));
			topicPartitions.add(new TopicPartition("TEQ", 2));
			topicPartitions.add(new TopicPartition("TEQ", 3));
			topicPartitions.add(new TopicPartition("TEQ", 4));

			ListConsumerGroupOffsetsSpec spec = new ListConsumerGroupOffsetsSpec().topicPartitions(topicPartitions);
			ListConsumerGroupOffsetsResult result = admin
					.listConsumerGroupOffsets(Collections.singletonMap(groupId, spec));
			try {
				KafkaFuture<Map<String, Map<TopicPartition, OffsetAndMetadata>>> ftr = result.all();
				System.out.println(ftr.get());
				System.out.println("Main Thread Out of wait now");
			} catch (Exception e) {
				System.out.println(e);
			}
			System.out.println("Auto Closing admin now");
		} catch (Exception e) {
			System.out.println("Exception while listing Consumer Group Offsets " + e);
			e.printStackTrace();
		}
		System.out.println("Test: ListConsumerGroupOffsets completed");
	}

}
