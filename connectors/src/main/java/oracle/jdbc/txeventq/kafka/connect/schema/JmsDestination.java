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

import java.sql.SQLException;

import javax.jms.Destination;
import javax.jms.JMSException;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import oracle.jdbc.txeventq.kafka.connect.common.utils.JmsUtils;

public class JmsDestination {
    protected static final Logger log = LoggerFactory.getLogger(JmsDestination.class);

    public static final Schema SCHEMA_JMSDESTINATION_V1 = SchemaBuilder.struct()
            .name("JMSDestination").version(1).field("type", Schema.STRING_SCHEMA)
            .field("name", Schema.STRING_SCHEMA).field("owner", Schema.OPTIONAL_STRING_SCHEMA)
            .field("completeName", Schema.OPTIONAL_STRING_SCHEMA)
            .field("completeTableName", Schema.OPTIONAL_STRING_SCHEMA)
            .field("address", Schema.OPTIONAL_STRING_SCHEMA)
            .field("protocol", Schema.OPTIONAL_INT32_SCHEMA).optional().build();

    private final String type;
    private final String name;
    private final String owner;
    private final String completeName;
    private final String completeTableName;
    private final String address;
    private final int protocol;

    /**
     * Constructs a JmsDestination object.
     * 
     * @param destination The Destination object to get information from.
     * @throws JMSException
     * @throws SQLException
     */
    public JmsDestination(Destination destination) throws JMSException, SQLException {
        log.trace("[{}] Entry {}.JmsDestination", Thread.currentThread().getId(),
                this.getClass().getName());
        this.type = JmsUtils.destinationType(destination);
        this.name = JmsUtils.destinationName(destination);
        this.owner = JmsUtils.destinationOwner(destination);
        this.completeName = JmsUtils.destinationCompleteName(destination);
        this.completeTableName = JmsUtils.destinationCompleteTableName(destination);
        this.address = JmsUtils.destinationAgentAddress(destination);
        this.protocol = JmsUtils.destinationAgentProtocol(destination);
        log.trace("[{}] Exit {}.JmsDestination", Thread.currentThread().getId(),
                this.getClass().getName());
    }

    /**
     * Creates a structured record for a JmsDestination.
     * 
     * @return A structured record containing a set of named fields with values, each field using an
     *         independent Schema.
     */
    public Struct toJmsDestinationStructV1() {
        log.trace("[{}] Entry {}.toJmsDestinationStructV1", Thread.currentThread().getId(),
                this.getClass().getName());
        log.trace("[{}] Exit {}.toJmsDestinationStructV1", Thread.currentThread().getId(),
                this.getClass().getName());
        return new Struct(SCHEMA_JMSDESTINATION_V1).put("type", this.type).put("name", this.name)
                .put("owner", this.owner).put("completeName", this.completeName)
                .put("completeTableName", this.completeTableName).put("address", this.address)
                .put("protocol", this.protocol);
    }

}
