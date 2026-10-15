package org.oracle.okafka.tests;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Assert;
import org.junit.Test;
import org.oracle.okafka.clients.admin.AdminClient;
import org.oracle.okafka.clients.consumer.KafkaConsumer;
import org.oracle.okafka.clients.producer.KafkaProducer;
import org.oracle.okafka.clients.producer.OKafkaHeaders;

/** Database integration test; run with clients as the working directory. */
public class OkafkaExpiration {
    private static final String TOPIC_PREFIX = "TEQEXP_";
    private static final int MESSAGE_COUNT = 5;
    private static final Duration TTL = Duration.ofMinutes(1);

    public static void main(String[] args) throws Exception {
        new OkafkaExpiration().messagesExpireAndTopicIsDeleted();
    }

    @Test
    public void messagesExpireAndTopicIsDeleted() throws Exception {
        String runId = "ttl-test-" + UUID.randomUUID();
        String groupSuffix = UUID.randomUUID().toString().replace("-", "").substring(0, 12);
        String topicName = TOPIC_PREFIX + System.currentTimeMillis();
        try (Admin admin = AdminClient.create(OkafkaTestSupport.baseProperties())) {
            NewTopic topic = new NewTopic(topicName, 1, (short) 1);
            topic.configs(Collections.singletonMap("retention.ms", "3600000"));
            admin.createTopics(Collections.singletonList(topic)).all().get(60, TimeUnit.SECONDS);
            System.out.println("Created " + topicName + " with one-hour retention; run ID: " + runId);
            Throwable failure = null;
            try {
                try (KafkaConsumer<String, String> before = consumer("EXP_BEFORE_" + groupSuffix);
                     KafkaConsumer<String, String> after = consumer("EXP_AFTER_" + groupSuffix)) {
                    // Register both subscribers before enqueue; keep the second group unread.
                    before.subscribe(Collections.singletonList(topicName));
                    after.subscribe(Collections.singletonList(topicName));
                    before.poll(Duration.ofSeconds(1));
                    after.poll(Duration.ofSeconds(1));

                    Set<String> expected = sendMessages(topicName, runId);
                    Map<String, Long> initial = receive(before, expected.size(), Duration.ofSeconds(20));
                    Assert.assertEquals("Unexpected records before expiry", expected, initial.keySet());
                    assertTimeToLives(initial, runId);
                    before.commitSync();
                    System.out.println("Verified all " + expected.size() + " messages before expiration.");
                    System.out.println("Waiting " + TTL.plusSeconds(15) + " before checking the second consumer group...");
                    Thread.sleep(TTL.plusSeconds(15).toMillis());

                    // Poll for the full window so an early control message cannot hide expired records.
                    Map<String, Long> remaining = receive(after, Integer.MAX_VALUE, Duration.ofSeconds(20));
                    Set<String> controls = new HashSet<>();
                    controls.add(runId + "-control");
                    controls.add(runId + "-no-ttl");
                    Assert.assertEquals("Unexpected records after expiry", controls, remaining.keySet());
                    after.commitSync();
                    System.out.println("PASS: all five TTL messages expired; both non-expiring controls were delivered.");
                }
            } catch (Exception | AssertionError e) {
                failure = e;
                throw e;
            } finally {
                try {
                    admin.deleteTopics(Collections.singletonList(topicName)).all().get(60, TimeUnit.SECONDS);
                    System.out.println("Deleted test topic " + topicName);
                } catch (Exception cleanupFailure) {
                    if (failure != null) {
                        failure.addSuppressed(cleanupFailure);
                    } else {
                        throw cleanupFailure;
                    }
                }
            }
        }
    }

    private static KafkaConsumer<String, String> consumer(String groupId) {
        Properties properties = OkafkaTestSupport.baseProperties();
        properties.put("group.id", groupId);
        properties.put("enable.auto.commit", "false");
        properties.put("auto.offset.reset", "earliest");
        properties.put("max.poll.interval.ms", "300000");
        properties.put("max.poll.records", "100");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return new KafkaConsumer<>(properties);
    }

    private static Map<String, Long> receive(KafkaConsumer<String, String> consumer, int expectedCount,
                                             Duration timeout) {
        Map<String, Long> timeToLives = new HashMap<>();
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline && timeToLives.size() < expectedCount) {
            for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofSeconds(1))) {
                long ttl = OKafkaHeaders.timeToLive(record.headers());
                System.out.printf("Received key=%s partition=%d offset=%d ttlMillis=%d%n",
                        record.key(), record.partition(), record.offset(), ttl);
                timeToLives.put(record.key(), ttl);
            }
        }
        return timeToLives;
    }

    private static void assertTimeToLives(Map<String, Long> timeToLives, String runId) {
        for (int i = 1; i <= MESSAGE_COUNT; i++) {
            assertTimeToLive(timeToLives, runId + "-expires-" + i, TTL.toMillis());
        }
        assertTimeToLive(timeToLives, runId + "-control", 0L);
        assertTimeToLive(timeToLives, runId + "-no-ttl", -1L);
    }

    private static void assertTimeToLive(Map<String, Long> timeToLives, String key, long expected) {
        Long actual = timeToLives.get(key);
        Assert.assertNotNull("No consumed record found for " + key, actual);
        Assert.assertEquals("Unexpected TTL for " + key, expected, actual.longValue());
    }

    private static Set<String> sendMessages(String topicName, String runId) throws Exception {
        Properties properties = OkafkaTestSupport.baseProperties();
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("oracle.transactional.producer", "false");

        List<String> keys = new ArrayList<>();
        List<Future<RecordMetadata>> results = new ArrayList<>();
        System.out.println("Enqueue started at: " + Instant.now());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            for (int i = 1; i <= MESSAGE_COUNT; i++) {
                String key = runId + "-expires-" + i;
                ProducerRecord<String, String> record = new ProducerRecord<>(
                        topicName, 0, key, "Expiration test " + key + "; TTL=" + TTL);
                record.headers().add(OKafkaHeaders.timeToLive(TTL));
                keys.add(key);
                results.add(producer.send(record));
            }

            String controlKey = runId + "-control";
            ProducerRecord<String, String> control = new ProducerRecord<>(
                    topicName, 0, controlKey, "Expiration test " + controlKey + "; no message expiration");
            control.headers().add(OKafkaHeaders.timeToLive(Duration.ZERO));
            keys.add(controlKey);
            results.add(producer.send(control));

            String noTtlKey = runId + "-no-ttl";
            ProducerRecord<String, String> noTtl = new ProducerRecord<>(
                    topicName, 0, noTtlKey, "Expiration test " + noTtlKey + "; no TTL header");
            keys.add(noTtlKey);
            results.add(producer.send(noTtl));

            for (int i = 0; i < results.size(); i++) {
                RecordMetadata metadata = results.get(i).get(60, TimeUnit.SECONDS);
                System.out.printf("Acknowledged %s: topic=%s partition=%d offset=%d%n",
                        keys.get(i), metadata.topic(), metadata.partition(), metadata.offset());
            }

            System.out.println("All sends acknowledged at: " + Instant.now());
        }
        return new HashSet<>(keys);
    }
}
