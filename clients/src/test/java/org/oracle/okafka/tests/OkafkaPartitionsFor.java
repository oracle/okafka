package org.oracle.okafka.tests;

import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.PartitionInfo;
import org.junit.Assert;
import org.junit.Test;

public class OkafkaPartitionsFor {

	@Test(timeout = 120000)
	public void okafkaPartitionsForTest() throws Exception {
		String topic = OkafkaTestSupport.uniqueTopic("TEQ_PARTITIONS_FOR");
		String groupId = OkafkaTestSupport.uniqueGroup("G_PARTITIONS_FOR");

		try (Admin admin = OkafkaTestSupport.admin()) {
			try {
				OkafkaTestSupport.createTopic(admin, topic, 3);
				try (Producer<String, String> producer = OkafkaTestSupport.producer()) {
					OkafkaTestSupport.produceRecordsToPartitions(producer, topic, 3);
				}

				Properties prop = OkafkaTestSupport.consumerProperties(groupId);
				try (Consumer<String, String> consumer = OkafkaTestSupport.consumer(prop)) {
					List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
					Assert.assertNotNull("partitionsFor should return metadata for an existing topic", partitionInfos);
					Assert.assertEquals("partitionsFor should return all topic partitions", 3, partitionInfos.size());
					Assert.assertTrue("Partition metadata should match requested topic",
							partitionInfos.stream().allMatch(info -> topic.equals(info.topic())));
				}
			} finally {
				OkafkaTestSupport.deleteTopicIfExists(admin, topic);
			}
		}
	}
}
