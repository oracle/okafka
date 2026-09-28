package org.oracle.okafka.tests;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.Test;
import org.oracle.okafka.clients.consumer.KafkaConsumer;

/** Database integration test for the consumer interceptor lifecycle. */
public class OkafkaConsumerInterceptorTest {
    private static final String MARKER_VALUE = "configured-by-okafka-test";

    @Test(timeout = 120000)
    public void interceptorIsConfiguredAroundConsumeAndCommit() throws Exception {
        String topic = OkafkaTestSupport.uniqueTopic("TEQ_CONSUMER_INTERCEPTOR");
        String groupId = OkafkaTestSupport.uniqueGroup("OKAFKA_CONSUMER_INTERCEPTOR");
        RecordingConsumerInterceptor.reset();

        try (Admin admin = OkafkaTestSupport.admin()) {
            try {
                OkafkaTestSupport.createTopic(admin, topic, 1);
                try (Producer<String, String> producer = OkafkaTestSupport.producer()) {
                    OkafkaTestSupport.produceRecords(producer, topic, 1);
                }

                System.out.println("APPLICATION: creating consumer with RecordingConsumerInterceptor");
                try (KafkaConsumer<String, String> consumer = OkafkaTestSupport.consumer(consumerProperties(groupId, false))) {
                    Assert.assertEquals("Interceptor configure() did not receive the custom property", MARKER_VALUE,
                            RecordingConsumerInterceptor.configuredMarker());

                    consumer.subscribe(Collections.singletonList(topic));
                    ConsumerRecords<String, String> records = pollForRecord(consumer);
                    Assert.assertEquals("Expected the produced record", 1, records.count());
                    Assert.assertTrue("onConsume() must run before poll() returns records",
                            RecordingConsumerInterceptor.onConsumeCalled());
                    System.out.println("APPLICATION: poll() returned " + records.count() + " record after onConsume");

                    consumer.commitSync();
                    Map<TopicPartition, OffsetAndMetadata> committed = RecordingConsumerInterceptor.committedOffsets();
                    Assert.assertNotNull("onCommit() was not invoked", committed);
                    Assert.assertEquals("Expected one committed partition", 1, committed.size());
                    OffsetAndMetadata committedOffset = committed.get(new TopicPartition(topic, 0));
                    Assert.assertNotNull("onCommit() did not receive the consumed topic-partition", committedOffset);
                    Assert.assertTrue("Committed offset should not be negative", committedOffset.offset() >= 0);
                    System.out.println("APPLICATION: commitSync() returned after onCommit(offset="
                            + committedOffset.offset() + ")");
                }
            } finally {
                OkafkaTestSupport.deleteTopicIfExists(admin, topic);
            }
        }
    }

    @Test(timeout = 120000)
    public void interceptorIsCalledForAutoCommit() throws Exception {
        String topic = OkafkaTestSupport.uniqueTopic("TEQ_CONSUMER_AUTO_COMMIT_INTERCEPTOR");
        String groupId = OkafkaTestSupport.uniqueGroup("OKAFKA_CONSUMER_AUTO_COMMIT_INTERCEPTOR");
        RecordingConsumerInterceptor.reset();

        try (Admin admin = OkafkaTestSupport.admin()) {
            try {
                OkafkaTestSupport.createTopic(admin, topic, 1);
                try (Producer<String, String> producer = OkafkaTestSupport.producer()) {
                    OkafkaTestSupport.produceRecords(producer, topic, 1);
                }

                Properties properties = consumerProperties(groupId, true);
                try (KafkaConsumer<String, String> consumer = OkafkaTestSupport.consumer(properties)) {
                    consumer.subscribe(Collections.singletonList(topic));
                    Assert.assertEquals("Expected the produced record", 1, pollForRecord(consumer).count());
                    Assert.assertTrue("onConsume() must run before poll() returns records",
                            RecordingConsumerInterceptor.onConsumeCalled());
                    System.out.println("APPLICATION: first poll returned the record; waiting for auto-commit");
                    Thread.sleep(1200L);

                    TopicPartition partition = new TopicPartition(topic, 0);
                    long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
                    while (System.nanoTime() < deadline) {
                        consumer.poll(Duration.ofMillis(250));
                        Map<TopicPartition, OffsetAndMetadata> committed = RecordingConsumerInterceptor.committedOffsets();
                        if (committed != null && committed.containsKey(partition)) {
                            Assert.assertTrue("Auto-commit onCommit() must follow onConsume()",
                                    RecordingConsumerInterceptor.onConsumeCalled());
                            System.out.println("APPLICATION: auto-commit invoked onCommit(offset="
                                    + committed.get(partition).offset() + ")");
                            return;
                        }
                    }
                    Assert.fail("Timed out waiting for auto-commit to invoke onCommit()");
                }
            } finally {
                OkafkaTestSupport.deleteTopicIfExists(admin, topic);
            }
        }
    }

    @Test(timeout = 120000)
    public void interceptorIsCalledForAsyncCommit() throws Exception {
        String topic = OkafkaTestSupport.uniqueTopic("TEQ_CONSUMER_ASYNC_COMMIT_INTERCEPTOR");
        String groupId = OkafkaTestSupport.uniqueGroup("OKAFKA_CONSUMER_ASYNC_COMMIT_INTERCEPTOR");
        RecordingConsumerInterceptor.reset();

        try (Admin admin = OkafkaTestSupport.admin()) {
            try {
                OkafkaTestSupport.createTopic(admin, topic, 1);
                try (Producer<String, String> producer = OkafkaTestSupport.producer()) {
                    OkafkaTestSupport.produceRecords(producer, topic, 1);
                }

                try (KafkaConsumer<String, String> consumer = OkafkaTestSupport.consumer(consumerProperties(groupId, false))) {
                    consumer.subscribe(Collections.singletonList(topic));
                    Assert.assertEquals("Expected the produced record", 1, pollForRecord(consumer).count());
                    Assert.assertTrue("onConsume() must run before poll() returns records",
                            RecordingConsumerInterceptor.onConsumeCalled());

                    TopicPartition partition = new TopicPartition(topic, 0);
                    AtomicReference<Map<TopicPartition, OffsetAndMetadata>> callbackOffsets = new AtomicReference<>();
                    AtomicReference<Exception> callbackException = new AtomicReference<>();
                    AtomicBoolean callbackSawOnCommit = new AtomicBoolean(false);
                    consumer.commitAsync((offsets, exception) -> {
                        callbackOffsets.set(offsets);
                        callbackException.set(exception);
                        Map<TopicPartition, OffsetAndMetadata> committed = RecordingConsumerInterceptor.committedOffsets();
                        callbackSawOnCommit.set(committed != null && committed.containsKey(partition));
                        System.out.println("APPLICATION: commitAsync callback invoked after onCommit");
                    });

                    Assert.assertNull("commitAsync callback reported an error", callbackException.get());
                    Assert.assertNotNull("commitAsync callback was not invoked", callbackOffsets.get());
                    Map<TopicPartition, OffsetAndMetadata> committed = RecordingConsumerInterceptor.committedOffsets();
                    Assert.assertNotNull("onCommit() was not invoked for commitAsync", committed);
                    Assert.assertNotNull("onCommit() did not receive the consumed topic-partition",
                            committed.get(partition));
                    Assert.assertTrue("onCommit() must run before the commitAsync callback", callbackSawOnCommit.get());
                    System.out.println("APPLICATION: commitAsync() returned after onCommit");
                }
            } finally {
                OkafkaTestSupport.deleteTopicIfExists(admin, topic);
            }
        }
    }

    private static Properties consumerProperties(String groupId, boolean autoCommit) {
        Properties properties = OkafkaTestSupport.consumerProperties(groupId);
        properties.put("enable.auto.commit", Boolean.toString(autoCommit));
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("auto.offset.reset", "earliest");
        properties.put("interceptor.classes", RecordingConsumerInterceptor.class.getName());
        properties.put(RecordingConsumerInterceptor.MARKER_CONFIG, MARKER_VALUE);
        return properties;
    }

    private static ConsumerRecords<String, String> pollForRecord(KafkaConsumer<String, String> consumer) {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (System.nanoTime() < deadline) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            if (!records.isEmpty()) {
                return records;
            }
        }
        Assert.fail("Timed out waiting for the produced record");
        return ConsumerRecords.empty();
    }
}
