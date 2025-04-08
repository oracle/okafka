package org.oracle.okafka.tests;

import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

class TestRunner {
	public static void main(String[] args) {

		Result result = new Result();

		result = JUnitCore.runClasses(SimpleOkafkaAdmin.class, SimpleOkafkaProducer.class, OkafkaListOffsets.class,
				OkafkaAutoOffsetReset.class, OkafkaFetchCommittedOffset.class, ListConsumerGroups.class,
				ListConsumerGroupOffsets.class, SimpleOkafkaProducer.class, OkafkaSeekToEnd.class,
				OkafkaSeekToBeginning.class, SimpleOkafkaProducer.class, OkafkaUnsubscribe.class,
				ProducerMetricsTest.class, ConsumerMetricsTest.class, DeleteConsumerGroups.class,
				OkafkaDescribeTopics.class, OkafkaListTopics.class, OkafkaDescribeTopicsById.class,
				OkafkaDeleteTopic.class, OkafkaDeleteTopicById.class);

		for (Failure failure : result.getFailures()) {
			System.out.println("Test failure : " + failure.toString());
		}
		System.out.println("Tests ran succesfully: " + result.wasSuccessful());
	}
}