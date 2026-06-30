package org.oracle.okafka.tests;

import java.util.Map;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.junit.Assert;
import org.junit.Test;

public class ProducerMetricsTest {

	@Test(timeout = 120000)
	public void producerMetricsTest() throws Exception {
		String topic = OkafkaTestSupport.uniqueTopic("TEQ_PRODUCER_METRICS");
		try (Admin admin = OkafkaTestSupport.admin()) {
			try {
				OkafkaTestSupport.createTopic(admin, topic, 1);
				try (Producer<String, String> producer = OkafkaTestSupport.producer()) {
					OkafkaTestSupport.produceRecords(producer, topic, 100);
					Map<MetricName, ? extends Metric> metricData = producer.metrics();
					Assert.assertFalse("Producer metrics should not be empty after producing records",
							metricData.isEmpty());
					Assert.assertTrue("Producer metrics should contain at least one metric value",
							metricData.values().stream().anyMatch(metric -> metric.metricValue() != null));
				}
			} finally {
				OkafkaTestSupport.deleteTopicIfExists(admin, topic);
			}
		}
	}
}
