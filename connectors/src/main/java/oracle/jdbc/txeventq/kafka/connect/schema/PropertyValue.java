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

public class PropertyValue {
    protected static final Logger log = LoggerFactory.getLogger(PropertyValue.class);
    public static final Schema SCHEMA_PROPERTYVALUE_V1 = SchemaBuilder.struct()
            .name("PropertyValue").version(1).field("propertyType", Schema.STRING_SCHEMA)
            .field("boolean", Schema.OPTIONAL_BOOLEAN_SCHEMA)// Populated only for boolean value.
            .field("byte", Schema.OPTIONAL_INT8_SCHEMA) // Populated only for byte value.
            .field("short", Schema.OPTIONAL_INT16_SCHEMA) // Populated only for short value.
            .field("integer", Schema.OPTIONAL_INT32_SCHEMA) // Populated only for integer value.
            .field("long", Schema.OPTIONAL_INT64_SCHEMA) // Populated only for long value.
            .field("float", Schema.OPTIONAL_FLOAT32_SCHEMA) // Populated only for float value.
            .field("double", Schema.OPTIONAL_FLOAT64_SCHEMA) // Populated only for double value.
            .field("string", Schema.OPTIONAL_STRING_SCHEMA) // Populated only for string value.
            .build();

    private final String propertyType;
    private final Object value;

    /**
     * Creates a PropertyValue with the specified value.
     * 
     */
    public PropertyValue(Object value) {
        log.trace("Entry {}.PropertyValue", this.getClass().getName());
        this.value = value;
        if (value instanceof Boolean) {
            propertyType = "boolean";
        } else if (value instanceof Byte) {
            propertyType = "byte";
        } else if (value instanceof Short) {
            propertyType = "short";
        } else if (value instanceof Integer) {
            propertyType = "integer";
        } else if (value instanceof Long) {
            propertyType = "long";
        } else if (value instanceof Float) {
            propertyType = "float";
        } else if (value instanceof Double) {
            propertyType = "double";
        } else if (value instanceof String) {
            propertyType = "string";
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported value propertyType: " + value.getClass().getName() + ".");
        }

        log.trace("Exit {}.PropertyValue", this.getClass().getName());
    }

    /**
     * Creates a structured record for PropertyValue.
     * 
     * @return A structured record containing a set of named fields with values, each field using an
     *         independent Schema.
     */
    public Struct toPropertyValueStructV1() {
        log.trace("[{}] Entry {}.toPropertyValueStructV1", Thread.currentThread().getId(),
                this.getClass().getName());

        log.trace("[{}] Exit {}.toPropertyValueStructV1", Thread.currentThread().getId(),
                this.getClass().getName());
        return new Struct(SCHEMA_PROPERTYVALUE_V1).put("propertyType", propertyType)
                .put(propertyType, value);
    }
}
