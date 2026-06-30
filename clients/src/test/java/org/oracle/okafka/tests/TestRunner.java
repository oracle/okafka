package org.oracle.okafka.tests;

import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

public class TestRunner {

	private static final Class<?>[] TEST_CLASSES = new Class<?>[] { SimpleOkafkaAdmin.class,
			SimpleOkafkaProducer.class, SimpleOkafkaConsumer.class, OkafkaListTopics.class,
			OkafkaDescribeTopics.class, OkafkaDescribeTopicsById.class, OkafkaCreatePartitions.class,
			OkafkaDeleteTopic.class, OkafkaDeleteTopicById.class, OkafkaListOffsets.class,
			OkafkaBeginningOffsets.class, OkafkaEndOffsets.class, OkafkaFetchCommittedOffset.class,
			OkafkaAutoOffsetReset.class, OkafkaSeekToBeginning.class, OkafkaSeekToEnd.class,
			OkafkaUnsubscribe.class, OkafkaOffsetsForTimes.class,
			OkafkaPartitionsFor.class, OkafkaPosition.class, OkafkaConsumerOffsetsLifecycle.class,
			OkafkaTransactionalProducerConsumer.class, ListConsumerGroups.class, ListConsumerGroupOffsets.class,
			DeleteConsumerGroups.class, ProducerMetricsTest.class, ConsumerMetricsTest.class };

	public static void main(String[] args) {
		int totalRun = 0;
		int totalFailed = 0;
		int totalIgnored = 0;
		long totalDurationMs = 0L;

		System.out.println("Running OKafka integration test suite");
		System.out.println("Test classes: " + TEST_CLASSES.length);
		System.out.println();

		for (Class<?> testClass : TEST_CLASSES) {
			long startedAt = System.currentTimeMillis();
			Result result = JUnitCore.runClasses(testClass);
			long durationMs = System.currentTimeMillis() - startedAt;

			totalRun += result.getRunCount();
			totalFailed += result.getFailureCount();
			totalIgnored += result.getIgnoreCount();
			totalDurationMs += durationMs;

			String status = result.wasSuccessful() ? "PASS" : "FAIL";
			System.out.printf("[%s] %s (tests=%d, failed=%d, ignored=%d, duration=%d ms)%n", status,
					testClass.getSimpleName(), result.getRunCount(), result.getFailureCount(),
					result.getIgnoreCount(), durationMs);

			for (Failure failure : result.getFailures()) {
				System.err.println("  Failed: " + failure.getDescription());
				System.err.println(failure.getTrace());
			}
		}

		System.out.println();
		System.out.println("OKafka integration test suite summary");
		System.out.println("Classes run: " + TEST_CLASSES.length);
		System.out.println("Tests run: " + totalRun);
		System.out.println("Tests failed: " + totalFailed);
		System.out.println("Tests ignored: " + totalIgnored);
		System.out.println("Duration: " + totalDurationMs + " ms");
		System.out.println("Successful: " + (totalFailed == 0));

		if (totalFailed > 0) {
			System.exit(1);
		}
	}
}
