/*
** Kafka Connect for TxEventQ.
**
** Copyright (c) 2024, 2025 Oracle and/or its affiliates.
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

import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import oracle.jdbc.txeventq.kafka.connect.schema.Key;

public class HeaderToKey<R extends ConnectRecord<R>> implements Transformation<R> {
    protected static final Logger log = LoggerFactory.getLogger(HeaderToKey.class);

    private String headerKey;

    @Override
    public void configure(Map<String, ?> props) {
        // Parse the configuration properties
        HeaderToKeyConfig config = new HeaderToKeyConfig(props);
        this.headerKey = config.getString(HeaderToKeyConfig.KAFKA_HEADER_TO_KEY_CONFIG);
    }

    @Override
    public R apply(R record) {
        // Find the header with the specified key
        Header header = record.headers().lastWithName(headerKey);

        if (header != null) {
            // Get the value from the header
            Schema newKeySchema = header.schema();
            Object newKeyValue = header.value();

            // Create and return the new record with the updated key
            return record.newRecord(record.topic(), record.kafkaPartition(),
                    record.keySchema() != null && record.keySchema().equals(Key.SCHEMA_KEY_V1)
                            ? Key.SCHEMA_KEY_V1
                            : newKeySchema,
                    record.keySchema() != null && record.keySchema().equals(Key.SCHEMA_KEY_V1)
                            ? new Key(new String((byte[]) newKeyValue, StandardCharsets.UTF_8))
                                    .toKeyStructV1()
                            : newKeyValue,
                    record.valueSchema(), record.value(), record.timestamp(), record.headers());
        }
        // If header not found, return the original record unchanged
        return record;
    }

    @Override
    public ConfigDef config() {
        return HeaderToKeyConfig.getConfig();
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub

    }

}
