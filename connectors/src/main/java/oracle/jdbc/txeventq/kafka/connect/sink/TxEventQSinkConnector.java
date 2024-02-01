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

package oracle.jdbc.txeventq.kafka.connect.sink;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import oracle.jdbc.txeventq.kafka.connect.common.utils.AppInfoParser;
import oracle.jdbc.txeventq.kafka.connect.sink.task.TxEventQSinkTask;
import oracle.jdbc.txeventq.kafka.connect.sink.utils.TxEventQSinkConfig;

public class TxEventQSinkConnector extends SinkConnector {
    private static final Logger log = LoggerFactory.getLogger(TxEventQSinkConnector.class);

    private Map<String, String> configProperties;

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void start(Map<String, String> originalProps) {
        log.debug("[{}] Starting Oracle TxEventQ Sink Connector", Thread.currentThread().getId());
        this.configProperties = originalProps;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return (Class) TxEventQSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        log.debug("Setting task configurations for {} workers.", maxTasks);
        final List<Map<String, String>> configs = new ArrayList<>(maxTasks);
        for (int i = 0; i < maxTasks; ++i) {
            configs.add(configProperties);
        }
        return configs;
    }

    @Override
    public void stop() {
        log.debug("[{}] Stopping Oracle TxEventQ Sink Connector", Thread.currentThread().getId());

    }

    @Override
    public ConfigDef config() {
        return TxEventQSinkConfig.getConfig();
    }

}
