/*
** Kafka Connect for TxEventQ.
**
** Copyright (c) 2023, 2024 Oracle and/or its affiliates.
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

package oracle.jdbc.txeventq.kafka.connect.source;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import oracle.jdbc.txeventq.kafka.connect.common.utils.AppInfoParser;
import oracle.jdbc.txeventq.kafka.connect.source.task.TxEventQSourceTask;
import oracle.jdbc.txeventq.kafka.connect.source.utils.TxEventQConnectorConfig;

/**
 * TxEventQSourceConnector is a connector interface that will pull data from an ORACLE TxEventQ and
 * send it to Kafka.
 */
public class TxEventQSourceConnector extends SourceConnector {
    private static final Logger log = LoggerFactory.getLogger(TxEventQSourceConnector.class);

    private Map<String, String> configProperties;

    /**
     * Get the version of this task. Usually this should be the same as the corresponding
     * {@link Connector} class's version.
     *
     * @return the version, formatted as a String
     */
    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    /**
     * Returns the Task implementation for this Connector.
     */
    @Override
    public Class<? extends Task> taskClass() {
        return (Class) TxEventQSourceTask.class;
    }

    /**
     * Returns a set of configurations for Tasks based on the current configuration, producing at
     * most count configurations.
     *
     * @param maxTasks maximum number of configurations to generate
     * @return configurations for Tasks
     */
    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        log.debug("[{}] Setting task configurations for {} workers.",
                Thread.currentThread().getId(), maxTasks);
        final List<Map<String, String>> configs = new ArrayList<>(maxTasks);
        for (int i = 0; i < maxTasks; ++i) {
            configs.add(configProperties);
        }
        return configs;
    }

    @Override
    public void start(Map<String, String> originalProps) {
        log.debug("[{}] Starting Oracle TxEventQ Source Connector", Thread.currentThread().getId());
        this.configProperties = originalProps;
    }

    @Override
    public void stop() {
        log.debug("[{}] Stopping Oracle TEQ Source Connector", Thread.currentThread().getId());
    }

    /**
     * Define the configuration for the connector.
     *
     * @return The ConfigDef for this connector.
     */
    @Override
    public ConfigDef config() {
        return TxEventQConnectorConfig.getConfig();
    }
}
