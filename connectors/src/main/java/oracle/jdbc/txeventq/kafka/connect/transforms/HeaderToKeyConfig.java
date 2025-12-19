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

package oracle.jdbc.txeventq.kafka.connect.transforms;

import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HeaderToKeyConfig extends AbstractConfig {
    private static final Logger log = LoggerFactory.getLogger(HeaderToKeyConfig.class);

    public static final String KAFKA_HEADER_TO_KEY_CONFIG = "set.header.to.key";
    private static final String KAFKA_HEADER_TO_KEY_DISPLAY = "Set Header value as new key value";
    private static final String KAFKA_HEADER_TO_KEY_DOC = "Header key to retreive value for and use as new key value.";

    public HeaderToKeyConfig(Map<String, ?> originals) {
        super(getConfig(), originals);

    }

    public HeaderToKeyConfig(ConfigDef definition, Map<String, String> originals) {
        super(definition, originals);
    }

    public HeaderToKeyConfig(ConfigDef definition, Map<String, String> originals, boolean doLog) {
        super(definition, originals, doLog);
    }

    public static ConfigDef getConfig() {
        ConfigDef configDef = new ConfigDef();

        int orderInGroup = 0;

        // KAFKA Group Configurations
        String groupName = "Kafka";

        configDef.define(KAFKA_HEADER_TO_KEY_CONFIG, ConfigDef.Type.STRING, "",
                ConfigDef.Importance.HIGH, KAFKA_HEADER_TO_KEY_DOC, groupName, ++orderInGroup,
                ConfigDef.Width.LONG, KAFKA_HEADER_TO_KEY_DISPLAY);

        return configDef;
    }
}
