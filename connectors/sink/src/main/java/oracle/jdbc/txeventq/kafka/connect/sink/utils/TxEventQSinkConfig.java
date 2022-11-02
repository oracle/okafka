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

package oracle.jdbc.txeventq.kafka.connect.sink.utils;

import java.util.Map;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

public class TxEventQSinkConfig extends AbstractConfig {

    // TEQ Configuration
    public static final String DATABASE_TNS_ALIAS_CONFIG = "db_tns_alias";
    private static final String DATABASE_TNS_ALIAS_DOC = "The TNS alias name placed in the tnsnames.ora for the database to connect to.";
    private static final String DATABASE_TNS_ALIAS_DISPLAY = "TNS alias used for JDBC connection.";

    public static final String DATABASE_WALLET_CONFIG = "wallet.path";
    private static final String DATABASE_WALLET_DOC = "The directory to the wallet information.";
    private static final String DATABASE_WALLET_DISPLAY = "wallet.path";

    public static final String DATABASE_TNSNAMES_CONFIG = "tnsnames.path";
    private static final String DATABASE_TNSNAMES_DOC = "The directory to where the tnsnames.ora file is located.";
    private static final String DATABASE_TNSNAMES_DISPLAY = "tnsnames.path";

    public static final String TXEVENTQ_QUEUE_NAME = "txeventq.queue.name";
    public static final String TXEVENTQ_QUEUE_NAME_DOC = "The name of the TxEventQ queue where the connector writes all records that are read from the Kafka broker.";
    public static final String TXEVENTQ_QUEUE_NAME_DISPLAY = "txeventq.queue.name";

    public static final String TXEVENTQ_QUEUE_TYPE_CONFIG = "txeventq.queue.type";
    public static final String TXEVENTQ_QUEUE_TYPE_DOC = "txeventq.queue.type";
    public static final String TXEVENTQ_QUEUE_TYPE_DISPLAY = "txeventq.queue.type";

    // Kafka Configuration
    public static final String KAFKA_TOPIC = "topics";
    public static final String KAFKA_TOPIC_DOC = "The name of the Kafka topics where the connector reads all records from.";
    public static final String KAFKA_TOPIC_DISPLAY = "Kafka topic";

    public static final String KAFKA_CONNECT_NAME = "name";
    private static final String KAFKA_CONNECT_NAME_DOC = "";
    private static final String KAFKA_CONNECT_NAME_DISPLAY = "";

    public static final String BOOTSTRAP_SERVERS_CONFIG = CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
    private static final String BOOTSTRAP_SERVERS_DOC = CommonClientConfigs.BOOTSTRAP_SERVERS_DOC;
    private static final String BOOTSTRAP_SERVERS_DISPLAY = "";

    public TxEventQSinkConfig(Map<String, String> originals) {
        super(getConfig(), originals);
    }

    public static ConfigDef getConfig() {
        ConfigDef configDef = new ConfigDef();

        int orderInGroup = 0;

        // Database Group Configurations
        String groupName = "Database";

        configDef.define(DATABASE_TNS_ALIAS_CONFIG, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH,
                DATABASE_TNS_ALIAS_DOC, groupName, ++orderInGroup, ConfigDef.Width.LONG, DATABASE_TNS_ALIAS_DISPLAY);

        configDef.define(DATABASE_WALLET_CONFIG, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH,
                DATABASE_WALLET_DOC, groupName, ++orderInGroup, ConfigDef.Width.MEDIUM, DATABASE_WALLET_DISPLAY);

        configDef.define(DATABASE_TNSNAMES_CONFIG, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH,
                DATABASE_TNSNAMES_DOC, groupName, ++orderInGroup, ConfigDef.Width.MEDIUM, DATABASE_TNSNAMES_DISPLAY);

        // TxEventQ Group Configurations
        groupName = "TxEventQ";
        orderInGroup = 0;

        configDef.define(TXEVENTQ_QUEUE_NAME, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH,
                TXEVENTQ_QUEUE_NAME_DOC, groupName, ++orderInGroup, ConfigDef.Width.MEDIUM,
                TXEVENTQ_QUEUE_NAME_DISPLAY);

        configDef.define(TXEVENTQ_QUEUE_TYPE_CONFIG, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH,
                TXEVENTQ_QUEUE_TYPE_DOC, groupName, ++orderInGroup, ConfigDef.Width.MEDIUM,
                TXEVENTQ_QUEUE_TYPE_DISPLAY);

        // KAFKA Group Configurations
        groupName = "kafka";
        orderInGroup = 0;

        configDef.define(KAFKA_TOPIC, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, KAFKA_TOPIC_DOC, groupName,
                ++orderInGroup, ConfigDef.Width.LONG, KAFKA_TOPIC_DISPLAY);
        configDef.define(KAFKA_CONNECT_NAME, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH,
                KAFKA_CONNECT_NAME_DOC, groupName, ++orderInGroup, ConfigDef.Width.MEDIUM, KAFKA_CONNECT_NAME_DISPLAY);
        configDef.define(BOOTSTRAP_SERVERS_CONFIG, ConfigDef.Type.LIST, ConfigDef.Importance.HIGH,
                BOOTSTRAP_SERVERS_DOC, groupName, ++orderInGroup, ConfigDef.Width.MEDIUM, BOOTSTRAP_SERVERS_DISPLAY);

        return configDef;
    }

    public String name() {
        return getString(KAFKA_CONNECT_NAME);
    }

}
