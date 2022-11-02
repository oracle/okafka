/*
** Kafka Connect for TxEventQ version 1.0.
**
** Copyright (c) 2019, 2022 Oracle and/or its affiliates.
** Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.
*/

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package oracle.jdbc.txeventq.kafka.connect.common.utils;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.Sanitizer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.util.Properties;

public class AppInfoParser {
    private static final Logger log = LoggerFactory.getLogger(AppInfoParser.class);

    private static final String NAME;
    private static final String VERSION;
    private static final String COMMIT_ID;

    static {
        Properties props = new Properties();
        try (InputStream resourceStream = AppInfoParser.class
                .getResourceAsStream("/kafka-connect-oracle-version.properties")) {
            props.load(resourceStream);
        } catch (IOException e) {
            log.warn("Error while loading kafka-connect-oracle-version.properties.");
        }

        NAME = props.getProperty("name", "unknown").trim();
        VERSION = props.getProperty("version", "unknown").trim();
        COMMIT_ID = props.getProperty("commitId", "unknown").trim();
        log.trace("[{}] app-name={} version={}", Thread.currentThread().getId(), NAME, VERSION);
    }

    public static String getName() {
        return NAME;
    }

    public static String getVersion() {
        return VERSION;
    }

    public static String getCommitId() {
        return COMMIT_ID;
    }

    public static synchronized void registerAppInfo(String prefix, String id, Metrics metrics) {
        try {
            ObjectName name = new ObjectName(prefix + ":type=app-info,id=" + Sanitizer.jmxSanitize(id));
            AppInfoParser.AppInfo mBean = new AppInfoParser.AppInfo();
            ManagementFactory.getPlatformMBeanServer().registerMBean(mBean, name);
            registerMetrics(metrics); // prefix will be added later by JmxReporter
        } catch (InstanceAlreadyExistsException e) {
            log.warn("The MBean is already under the control of the MBean server.");
        } catch (MBeanRegistrationException e) {
            log.warn("Unable to register the MBean.");
        } catch (NotCompliantMBeanException e) {
            log.warn("This object is not a JMXcompliant MBean");
        } catch (MalformedObjectNameException e) {
            log.warn("The string passed as aparameter does not have the right format.");
        }
    }

    public static synchronized void unregisterAppInfo(String prefix, String id, Metrics metrics) {
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        try {
            ObjectName name = new ObjectName(prefix + ":type=app-info,id=" + Sanitizer.jmxSanitize(id));
            if (server.isRegistered(name))
                server.unregisterMBean(name);

            unregisterMetrics(metrics);
        } catch (MBeanRegistrationException e) {
            log.warn("Unable to unregister the MBean.");
        } catch (InstanceNotFoundException e) {
            log.warn("The MBean specified is notregistered in the MBean server.");
        } catch (MalformedObjectNameException e) {
            log.warn("The string passed as aparameter does not have the right format.");
        }
    }

    private static MetricName metricName(Metrics metrics, String name) {
        return metrics.metricName(name, "app-info", "Metric indicating " + name);
    }

    private static void registerMetrics(Metrics metrics) {
        if (metrics != null) {
            metrics.addMetric(metricName(metrics, "version"), new AppInfoParser.ImmutableValue<>(VERSION));
            metrics.addMetric(metricName(metrics, "commit-id"), new AppInfoParser.ImmutableValue<>(COMMIT_ID));
        }
    }

    private static void unregisterMetrics(Metrics metrics) {
        if (metrics != null) {
            metrics.removeMetric(metricName(metrics, "version"));
            metrics.removeMetric(metricName(metrics, "commit-id"));
        }
    }

    public interface AppInfoMBean {
        public String getVersion();

        public String getCommitId();
    }

    public static class AppInfo implements AppInfoParser.AppInfoMBean {

        public AppInfo() {
        }

        @Override
        public String getVersion() {
            return AppInfoParser.getVersion();
        }

        @Override
        public String getCommitId() {
            return AppInfoParser.getCommitId();
        }

    }

    static class ImmutableValue<T> implements Gauge<T> {
        private final T gaugeValue;

        public ImmutableValue(T gaugeVal) {
            this.gaugeValue = gaugeVal;
        }

        @Override
        public T value(MetricConfig config, long now) {
            return gaugeValue;
        }
    }
}
