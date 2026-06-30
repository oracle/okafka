package org.oracle.okafka.tests;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.Test;

public class OkafkaListOffsets {
	@Test(timeout = 120000)
	public void okafkaListOffsetsTest() throws Exception {
		String topic = OkafkaTestSupport.uniqueTopic("TEQ_LIST_OFFSETS");
		try (Admin admin = OkafkaTestSupport.admin()) {
			try {
				OkafkaTestSupport.createTopic(admin, topic, 3);
				try (Producer<String, String> producer = OkafkaTestSupport.producer()) {
					OkafkaTestSupport.produceRecordsToPartitions(producer, topic, 3);
				}
				TopicPartition tp0 = new TopicPartition(topic, 0);
				TopicPartition tp1 = new TopicPartition(topic, 1);
				TopicPartition tp2 = new TopicPartition(topic, 2);

				Map<TopicPartition, OffsetSpec> topicOffsetSpecMap = new HashMap<>();
				topicOffsetSpecMap.put(tp0, OffsetSpec.earliest());
				topicOffsetSpecMap.put(tp1, OffsetSpec.latest());
				topicOffsetSpecMap.put(tp2, OffsetSpec.maxTimestamp());

				ListOffsetsResult result = admin.listOffsets(topicOffsetSpecMap);
				ListOffsetsResultInfo earliest = OkafkaTestSupport.get(result.partitionResult(tp0),
						"list earliest offset");
				ListOffsetsResultInfo latest = OkafkaTestSupport.get(result.partitionResult(tp1),
						"list latest offset");
				ListOffsetsResultInfo maxTimestamp = OkafkaTestSupport.get(result.partitionResult(tp2),
						"list max timestamp offset");

				Assert.assertEquals("Earliest offset should be zero", 0L, earliest.offset());
				Assert.assertEquals("Latest offset should reflect one produced record", 1L, latest.offset());
				Assert.assertTrue("Max timestamp offset should be non-negative", maxTimestamp.offset() >= 0L);
				Assert.assertFalse("Topic should remain listed after listOffsets",
						Collections.disjoint(OkafkaTestSupport.get(admin.listTopics().names(), "list topics"),
								Collections.singleton(topic)));
			} finally {
				OkafkaTestSupport.deleteTopicIfExists(admin, topic);
			}
		}
	}
}
