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

package oracle.jdbc.txeventq.kafka.connect.schema;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This schema is used to store the incoming correlation value on the message interface. If the
 * correlation is specified it will be used as the key for the Kafka topic.
 *
 */
public class Key {

    protected static final Logger log = LoggerFactory.getLogger(Key.class);
    public static final Schema SCHEMA_KEY_V1 = SchemaBuilder.struct().name("Key").version(1)
            .field("correlation", Schema.STRING_SCHEMA).optional().build();

    private final String correlation;

    /**
     * Creates a Key with the specified key value.
     * 
     * @param keyValue The key value that will be used by Kafka which will be the correlation id of
     *                 the JMSMessage.
     */
    public Key(String keyValue) {
        log.trace("[{}] Entry {}.Key", Thread.currentThread().getId(), this.getClass().getName());
        this.correlation = keyValue;
        log.trace("[{}] Exit {}.Key", Thread.currentThread().getId(), this.getClass().getName());
    }

    /**
     * Creates a structured record for Key.
     * 
     * @return A structured record containing a set of named fields with values, each field using an
     *         independent Schema.
     */
    public Struct toKeyStructV1() {
        log.trace("[{}] Entry {}.toKeyStructV1", Thread.currentThread().getId(),
                this.getClass().getName());
        log.trace("[{}] Exit {}.toKeyStructV1", Thread.currentThread().getId(),
                this.getClass().getName());
        return new Struct(SCHEMA_KEY_V1).put("correlation", this.correlation);
    }
}
