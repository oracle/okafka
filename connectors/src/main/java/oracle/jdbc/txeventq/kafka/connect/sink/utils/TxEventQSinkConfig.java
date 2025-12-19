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
    private static final String TXEVENTQ_QUEUE_NAME_DOC = "The name of the TxEventQ queue where the connector writes all records that are read from the Kafka broker.";
    private static final String TXEVENTQ_QUEUE_NAME_DISPLAY = "txeventq.queue.name";

    public static final String TXEVENTQ_QUEUE_SCHEMA = "txeventq.queue.schema";
    private static final String TXEVENTQ_QUEUE_SCHEMA_DOC = "The name of the schema for the TxEventQ queue.";
    private static final String TXEVENTQ_QUEUE_SCHEMA_DISPLAY = "txeventq.queue.schema";

    public static final String TXEVENTQ_QUEUE_JMS_BYTES_INCLUDE_KAFKA_HEADERS = "txeventq.jms.bytes.include.kafka.headers";
    private static final String TXEVENTQ_QUEUE_JMS_BYTES_INCLUDE_KAFKA_HEADERS_DOC = "Indicates if the JMS Bytes message should process headers from Kafka. The default value will be false and headers will not be processed.";
    private static final String TXEVENTQ_QUEUE_JMS_BYTES_INCLUDE_KAFKA_HEADERS_DISPLAY = "txeventq.jms.bytes.include.kafka.headers";
    public static final boolean TXEVENTQ_QUEUE_JMS_BYTES_INCLUDE_KAFKA_HEADERS_DEFAULT = false;

    public static final String TXEVENTQ_QUEUE_JMS_BYTES_INCLUDE_KAFKA_METADATA = "txeventq.jms.bytes.include.kafka.metadata";
    private static final String TXEVENTQ_QUEUE_JMS_BYTES_INCLUDE_KAFKA_METADATA_DOC = "Indicates if the JMS Bytes message should process Kafka metadata Kafka topic name, Kafka partition, Kafka offset, and Kafka timestamp. The default value will be false.";
    private static final String TXEVENTQ_QUEUE_JMS_BYTES_INCLUDE_KAFKA_METADATA_DISPLAY = "txeventq.jms.bytes.include.kafka.metadata";
    public static final boolean TXEVENTQ_QUEUE_JMS_BYTES_INCLUDE_KAFKA_METADATA_DEFAULT = false;

    // Kafka Configuration
    public static final String KAFKA_TOPIC = "topics";
    public static final String KAFKA_TOPIC_DOC = "The name of the Kafka topic where the connector reads all records from.";
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

        configDef.define(DATABASE_TNS_ALIAS_CONFIG, ConfigDef.Type.STRING, "",
                ConfigDef.Importance.HIGH, DATABASE_TNS_ALIAS_DOC, groupName, ++orderInGroup,
                ConfigDef.Width.LONG, DATABASE_TNS_ALIAS_DISPLAY);

        configDef.define(DATABASE_WALLET_CONFIG, ConfigDef.Type.STRING, "",
                ConfigDef.Importance.HIGH, DATABASE_WALLET_DOC, groupName, ++orderInGroup,
                ConfigDef.Width.MEDIUM, DATABASE_WALLET_DISPLAY);

        configDef.define(DATABASE_TNSNAMES_CONFIG, ConfigDef.Type.STRING, "",
                ConfigDef.Importance.HIGH, DATABASE_TNSNAMES_DOC, groupName, ++orderInGroup,
                ConfigDef.Width.MEDIUM, DATABASE_TNSNAMES_DISPLAY);

        // TxEventQ Group Configurations
        groupName = "TxEventQ";
        orderInGroup = 0;

        configDef.define(TXEVENTQ_QUEUE_SCHEMA, ConfigDef.Type.STRING, "",
                ConfigDef.Importance.HIGH, TXEVENTQ_QUEUE_SCHEMA_DOC, groupName, ++orderInGroup,
                ConfigDef.Width.MEDIUM, TXEVENTQ_QUEUE_SCHEMA_DISPLAY);

        configDef.define(TXEVENTQ_QUEUE_NAME, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH,
                TXEVENTQ_QUEUE_NAME_DOC, groupName, ++orderInGroup, ConfigDef.Width.MEDIUM,
                TXEVENTQ_QUEUE_NAME_DISPLAY);

        configDef.define(TXEVENTQ_QUEUE_JMS_BYTES_INCLUDE_KAFKA_HEADERS, ConfigDef.Type.BOOLEAN,
                TXEVENTQ_QUEUE_JMS_BYTES_INCLUDE_KAFKA_HEADERS_DEFAULT, ConfigDef.Importance.LOW,
                TXEVENTQ_QUEUE_JMS_BYTES_INCLUDE_KAFKA_HEADERS_DOC, groupName, ++orderInGroup,
                ConfigDef.Width.MEDIUM, TXEVENTQ_QUEUE_JMS_BYTES_INCLUDE_KAFKA_HEADERS_DISPLAY);

        configDef.define(TXEVENTQ_QUEUE_JMS_BYTES_INCLUDE_KAFKA_METADATA, ConfigDef.Type.BOOLEAN,
                TXEVENTQ_QUEUE_JMS_BYTES_INCLUDE_KAFKA_METADATA_DEFAULT, ConfigDef.Importance.LOW,
                TXEVENTQ_QUEUE_JMS_BYTES_INCLUDE_KAFKA_METADATA_DOC, groupName, ++orderInGroup,
                ConfigDef.Width.MEDIUM, TXEVENTQ_QUEUE_JMS_BYTES_INCLUDE_KAFKA_METADATA_DISPLAY);

        // KAFKA Group Configurations
        groupName = "kafka";
        orderInGroup = 0;

        configDef.define(KAFKA_TOPIC, ConfigDef.Type.LIST, "", ConfigDef.Importance.HIGH,
                KAFKA_TOPIC_DOC, groupName, ++orderInGroup, ConfigDef.Width.LONG,
                KAFKA_TOPIC_DISPLAY);
        configDef.define(KAFKA_CONNECT_NAME, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH,
                KAFKA_CONNECT_NAME_DOC, groupName, ++orderInGroup, ConfigDef.Width.MEDIUM,
                KAFKA_CONNECT_NAME_DISPLAY);
        configDef.define(BOOTSTRAP_SERVERS_CONFIG, ConfigDef.Type.LIST, ConfigDef.Importance.HIGH,
                BOOTSTRAP_SERVERS_DOC, groupName, ++orderInGroup, ConfigDef.Width.MEDIUM,
                BOOTSTRAP_SERVERS_DISPLAY);

        return configDef;
    }

    public String name() {
        return getString(KAFKA_CONNECT_NAME);
    }

}
