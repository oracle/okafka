/*
** OKafka Java Client version 23.4.
**
** Copyright (c) 2019, 2024 Oracle and/or its affiliates.
** Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.
*/

package org.oracle.okafka.clients.consumer.internals;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.CumulativeSum;
import org.apache.kafka.common.metrics.stats.Max;

import java.util.concurrent.TimeUnit;

public class OkafkaConsumerMetrics implements AutoCloseable {
    private final MetricName lastPollMetricName;
    private final Sensor timeBetweenPollSensor;
    private final Sensor pollIdleSensor;
    private final Sensor commitSyncSensor;
    private final Metrics metrics;
    private long lastPollMs;
    private long pollStartMs;
    private long timeSinceLastPollMs;

    public OkafkaConsumerMetrics(Metrics metrics, String metricGrpPrefix) {
        this.metrics = metrics;
        String metricGroupName = metricGrpPrefix + "-metrics";
        Measurable lastPoll = (mConfig, now) -> {
            if (lastPollMs == 0L)
                return -1d;
            else
                return TimeUnit.SECONDS.convert(now - lastPollMs, TimeUnit.MILLISECONDS);
        };
        this.lastPollMetricName = metrics.metricName("last-poll-seconds-ago",
                metricGroupName, "The number of seconds since the last poll() invocation.");
        metrics.addMetric(lastPollMetricName, lastPoll);

        this.timeBetweenPollSensor = metrics.sensor("time-between-poll");
        this.timeBetweenPollSensor.add(metrics.metricName("time-between-poll-avg",
                metricGroupName,
                "The average delay between invocations of poll() in milliseconds."),
                new Avg());
        this.timeBetweenPollSensor.add(metrics.metricName("time-between-poll-max",
                metricGroupName,
                "The max delay between invocations of poll() in milliseconds."),
                new Max());

        this.pollIdleSensor = metrics.sensor("poll-idle-ratio-avg");
        this.pollIdleSensor.add(metrics.metricName("poll-idle-ratio-avg",
                metricGroupName,
                "The average fraction of time the consumer's poll() is idle as opposed to waiting for the user code to process records."),
                new Avg());

        this.commitSyncSensor = metrics.sensor("commit-sync-time-ns-total");
        this.commitSyncSensor.add(
            metrics.metricName(
                "commit-sync-time-ns-total",
                metricGroupName,
                "The total time the consumer has spent in commitSync in nanoseconds"
            ),
            new CumulativeSum()
        );

    }

    public void recordPollStart(long pollStartMs) {
        this.pollStartMs = pollStartMs;
        this.timeSinceLastPollMs = lastPollMs != 0L ? pollStartMs - lastPollMs : 0;
        this.timeBetweenPollSensor.record(timeSinceLastPollMs);
        this.lastPollMs = pollStartMs;
    }

    public void recordPollEnd(long pollEndMs) {
        long pollTimeMs = pollEndMs - pollStartMs;
        double pollIdleRatio = pollTimeMs * 1.0 / (pollTimeMs + timeSinceLastPollMs);
        this.pollIdleSensor.record(pollIdleRatio);
    }

    public void recordCommitSync(long duration) {
        this.commitSyncSensor.record(duration);
    }


    @Override
    public void close() {
        metrics.removeMetric(lastPollMetricName);
        metrics.removeSensor(timeBetweenPollSensor.name());
        metrics.removeSensor(pollIdleSensor.name());
        metrics.removeSensor(commitSyncSensor.name());
    }
}
