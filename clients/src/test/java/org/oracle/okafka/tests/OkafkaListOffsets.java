package org.oracle.okafka.tests;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;
import org.oracle.okafka.clients.admin.AdminClient;

public class OkafkaListOffsets {
	@Test
	public void ListOffsetTest() {

		try (Admin admin = AdminClient.create(OkafkaSetup.setup())) {
			TopicPartition tp1 = new TopicPartition("TEQ", 0);
			TopicPartition tp2 = new TopicPartition("TEQ", 1);
			TopicPartition tp3 = new TopicPartition("TEQ", 2);

			Map<TopicPartition, OffsetSpec> topicOffsetSpecMap = new HashMap<>();
			topicOffsetSpecMap.put(tp1, OffsetSpec.earliest());
			topicOffsetSpecMap.put(tp2, OffsetSpec.latest());
			topicOffsetSpecMap.put(tp3, OffsetSpec.maxTimestamp());

			ListOffsetsResult result = admin.listOffsets(topicOffsetSpecMap);

			for (TopicPartition tp : topicOffsetSpecMap.keySet()) {
				try {
					ListOffsetsResultInfo resInfo = result.partitionResult(tp).get();
					System.out.print(resInfo);
				} catch (Exception e) {
					System.out.println(e);
				}
			}

			System.out.println("Auto Closing admin now");
		} catch (Exception e) {
			System.out.println("Exception while listing offsets " + e);
			e.printStackTrace();
		}
		System.out.println("Test: OkafkaListOffsets completed");
	}
}
