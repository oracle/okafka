package org.oracle.okafka.tests;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

public class RecordingConsumerInterceptor implements ConsumerInterceptor<String, String> {
    public static final String MARKER_CONFIG = "okafka.interceptor.test.marker";

    private static volatile Object configuredMarker;
    private static volatile boolean onConsumeCalled;
    private static volatile Map<TopicPartition, OffsetAndMetadata> committedOffsets;

    public static void reset() {
        configuredMarker = null;
        onConsumeCalled = false;
        committedOffsets = null;
    }

    public static Object configuredMarker() {
        return configuredMarker;
    }

    public static boolean onConsumeCalled() {
        return onConsumeCalled;
    }

    public static Map<TopicPartition, OffsetAndMetadata> committedOffsets() {
        return committedOffsets == null ? null : new HashMap<TopicPartition, OffsetAndMetadata>(committedOffsets);
    }

    @Override
    public void configure(Map<String, ?> configs) {
        configuredMarker = configs.get(MARKER_CONFIG);
        System.out.println("INTERCEPTOR: configure(marker=" + configuredMarker + ")");
    }

    @Override
    public ConsumerRecords<String, String> onConsume(ConsumerRecords<String, String> records) {
        onConsumeCalled = true;
        System.out.println("INTERCEPTOR: onConsume(records=" + records.count() + ")");
        return records;
    }

    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        committedOffsets = new HashMap<TopicPartition, OffsetAndMetadata>(offsets);
        System.out.println("INTERCEPTOR: onCommit(offsets=" + offsets + ")");
    }

    @Override
    public void close() {
        System.out.println("INTERCEPTOR: close");
    }
}
