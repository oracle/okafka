package org.oracle.okafka.tests;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.junit.Assert;
import org.junit.Test;

public class ConsumerMetricsTest {

	@Test(timeout = 120000)
	public void consumerMetricsTest() throws Exception {
		String topic = OkafkaTestSupport.uniqueTopic("TEQ_CONSUMER_METRICS");
		String groupId = OkafkaTestSupport.uniqueGroup("G_CONSUMER_METRICS");
		try (Admin admin = OkafkaTestSupport.admin()) {
			try {
				OkafkaTestSupport.createTopic(admin, topic, 1);
				try (Producer<String, String> producer = OkafkaTestSupport.producer()) {
					OkafkaTestSupport.produceRecords(producer, topic, 5);
				}

				Properties prop = OkafkaTestSupport.consumerProperties(groupId);
				prop.put("max.poll.records", "5");
				prop.put("auto.offset.reset", "earliest");
				try (Consumer<String, String> consumer = OkafkaTestSupport.consumer(prop)) {
					consumer.subscribe(Arrays.asList(topic));
					OkafkaTestSupport.consumeExactly(consumer, 5);
					Map<MetricName, ? extends Metric> metricData = consumer.metrics();
					Assert.assertFalse("Consumer metrics should not be empty after consuming records",
							metricData.isEmpty());
					Assert.assertTrue("Consumer metrics should contain at least one metric value",
							metricData.values().stream().anyMatch(metric -> metric.metricValue() != null));
				}
			} finally {
				OkafkaTestSupport.deleteTopicIfExists(admin, topic);
			}
		}
	}
}
