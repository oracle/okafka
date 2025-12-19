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
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.TextMessage;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import oracle.jms.AQjmsBytesMessage;
import oracle.jms.AQjmsMessage;

public class JmsMessage {
    protected static final Logger log = LoggerFactory.getLogger(JmsMessage.class);
    public static final Schema SCHEMA_JMSMESSAGE_V1 = SchemaBuilder.struct().name("JMSMessage")
            .version(1).field("jmsMessageType", Schema.STRING_SCHEMA)
            .field("jmsMessageId", Schema.STRING_SCHEMA).field("jmsTimestamp", Schema.INT64_SCHEMA)
            .field("jmsDeliveryMode", Schema.INT32_SCHEMA)
            .field("jmsCorrelationId", Schema.OPTIONAL_STRING_SCHEMA)
            .field("jmsReplyTo", JmsDestination.SCHEMA_JMSDESTINATION_V1)
            .field("jmsDestination", JmsDestination.SCHEMA_JMSDESTINATION_V1)
            .field("jmsRedelivered", Schema.BOOLEAN_SCHEMA)
            .field("jmsPriority", Schema.OPTIONAL_INT32_SCHEMA)
            .field("jmsExpiration", Schema.OPTIONAL_INT64_SCHEMA)
            .field("jmsType", Schema.OPTIONAL_STRING_SCHEMA)
            .field("jmsRetry_count", Schema.INT32_SCHEMA)
            .field("jmsProperties",
                    SchemaBuilder.map(Schema.STRING_SCHEMA, PropertyValue.SCHEMA_PROPERTYVALUE_V1))
            .required().field("payloadText", Schema.OPTIONAL_STRING_SCHEMA)
            .field("payloadMap",
                    SchemaBuilder.map(Schema.STRING_SCHEMA, PropertyValue.SCHEMA_PROPERTYVALUE_V1)
                            .optional()) // Populated only for JMS map message.
            .field("payloadBytes", Schema.OPTIONAL_BYTES_SCHEMA) // Populated only for JMS bytes
                                                                 // message.
            .build();

    private final String messageType;
    private final String messageId;
    private final String correlationId;
    private final JmsDestination destination;
    private final JmsDestination replyTo;
    private final int priority;
    private final long expiration;
    private final String type;
    private final long timestamp;
    private final int deliveryMode;
    private final boolean redelivered;
    private final int retryCount;
    private final Map<String, Struct> properties;
    private final byte[] payloadBytes;
    private final String payloadText;
    private final Map<String, Struct> payloadMap;

    /**
     * Creates a JmsMessage with the specified Message properties.
     * 
     * @param jms            The Message object
     * @param messageVersion Indicates if the AQINTERNAL_MESSAGEVERSION property is a value of 1 or
     *                       2.
     * @param jmsBytesValue  If the AQINTERNAL_MESSAGEVERSION is a value of 2 then this will contain
     *                       the message value that is stored in the payload. Since the payload of a
     *                       message with AQINTERNAL_MESSAGEVERSION value of 2 will contain more
     *                       information than just the message value. The message value has to be
     *                       parsed from the payload.
     * @throws JMSException
     * @throws SQLException
     */
    public JmsMessage(Message jms, int messageVersion, byte[] jmsBytesValue)
            throws JMSException, SQLException {
        log.trace("Entry {}.JmsMessage", this.getClass().getName());

        this.messageId = jms.getJMSMessageID();
        this.correlationId = jms.getJMSCorrelationID();

        this.destination = jms.getJMSDestination() != null
                ? new JmsDestination(jms.getJMSDestination())
                : null;

        this.replyTo = jms.getJMSReplyTo() != null ? new JmsDestination(jms.getJMSReplyTo()) : null;
        this.priority = jms.getJMSPriority();
        this.expiration = jms.getJMSExpiration();
        this.timestamp = jms.getJMSTimestamp();
        this.redelivered = jms.getJMSRedelivered();
        this.properties = propertiesMap(jms);
        this.deliveryMode = jms.getJMSDeliveryMode();
        this.type = jms.getJMSType();
        this.retryCount = ((AQjmsMessage) jms).getAttempts();

        if (jms instanceof BytesMessage) {

            this.messageType = "bytes";
            final AQjmsBytesMessage bytesMessage = (AQjmsBytesMessage) jms;

            /*
             * If the message type is a Jms Bytes Message type and the AQINTERNAL_MESSAGEVERSION
             * property value is 2 then use the jmsBytesValue passed into the method as the payload
             * value. This is required because the received byte payload is being sent in the format
             * shown below and we only need the value for the message that is stored into Kafka: |
             * KEY LENGTH (4 Bytes Fixed) | KEY | | VALUE LENGTH (4 BYTES FIXED) | VALUE | | HEADER
             * NAME LENGTH(4 BYTES FIXED) | HEADER NAME | | HEADER VALUE LENGTH (4 BYTES FIXED) |
             * HEADER VALUE | | HEADER NAME LENGTH(4 BYTES FIXED) | HEADER NAME | | HEADER VALUE
             * LENGTH (4 BYTES FIXED) | HEADER VALUE |
             * 
             * For records with null key , KEY LENGTH is set to 0. For records with null value,
             * VALUE LENGTH is set to 0. Number of headers are set in property
             * "AQINTERNAL_HEADERCOUNT"
             * 
             */
            if (messageVersion == 2) {
                this.payloadBytes = jmsBytesValue;
            } else {
                final byte[] bytes = new byte[(int) bytesMessage.getBodyLength()];

                bytesMessage.reset();
                bytesMessage.readBytes(bytes);
                this.payloadBytes = bytes;
            }

            this.payloadText = null;
            this.payloadMap = null;

        } else if (jms instanceof TextMessage) {
            this.messageType = "text";
            final TextMessage textMessage = (TextMessage) jms;
            this.payloadText = textMessage.getText();
            this.payloadMap = null;
            this.payloadBytes = null;
        } else if (jms instanceof MapMessage) {
            this.messageType = "map";
            final MapMessage mapMessage = (MapMessage) jms;
            final Map<String, Struct> map = new HashMap<>();
            final Enumeration<?> names = mapMessage.getMapNames();
            while (names.hasMoreElements()) {
                final String name = names.nextElement().toString();
                map.put(name,
                        new PropertyValue(mapMessage.getObject(name)).toPropertyValueStructV1());
            }

            this.payloadText = null;
            this.payloadMap = map;
            this.payloadBytes = null;

        } else {
            throw new UnsupportedOperationException(
                    "JMS message type '" + jms.getClass() + "' is not supported.");
        }
        log.trace("Exit {}.JmsMessage", this.getClass().getName());
    }

    /**
     * Creates a Map containing all the properties in the Message.
     *
     * @param jms The Message.
     * @return A Map for the properties in the specified Message.
     * @throws JMSException
     */
    public static Map<String, Struct> propertiesMap(Message jms) throws JMSException {
        log.trace("Entry {}.propertiesMap", JmsMessage.class.getName());
        final Map<String, Struct> result = new HashMap<>();
        final Enumeration<?> names = jms.getPropertyNames();
        while (names.hasMoreElements()) {
            final String name = names.nextElement().toString();
            log.debug("The property name is: {}", name);
            if (jms.getObjectProperty(name) != null) {
                result.put(name,
                        new PropertyValue(jms.getObjectProperty(name)).toPropertyValueStructV1());
            }
        }

        log.trace("Exit {}.propertiesMap", JmsMessage.class.getName());
        return result;
    }

    /**
     * Creates a structured record for a JmsMessage.
     * 
     * @return A structured record containing a set of named fields with values, each field using an
     *         independent Schema.
     */
    public Struct toJmsMessageStructV1() {
        log.trace("Entry {}.toJmsMessageStructV1", this.getClass().getName());
        final Struct result = new Struct(SCHEMA_JMSMESSAGE_V1)
                .put("jmsMessageType", this.messageType).put("jmsMessageId", this.messageId)
                .put("jmsCorrelationId", this.correlationId).put("jmsPriority", this.priority)
                .put("jmsExpiration", this.expiration).put("jmsTimestamp", this.timestamp)
                .put("jmsRedelivered", this.redelivered).put("jmsProperties", this.properties)
                .put("jmsDeliveryMode", this.deliveryMode).put("jmsType", this.type)
                .put("jmsRetry_count", this.retryCount);
        if (payloadText != null) {
            result.put("payloadText", payloadText);
        }
        if (payloadMap != null) {
            result.put("payloadMap", payloadMap);
        }
        if (payloadBytes != null) {
            result.put("payloadBytes", payloadBytes);
        }
        if (destination != null) {
            result.put("jmsDestination", destination.toJmsDestinationStructV1());
        }
        if (replyTo != null) {
            result.put("jmsReplyTo", replyTo.toJmsDestinationStructV1());
        }
        log.trace("[{}] Exit {}.toJmsMessageStructV1", Thread.currentThread().getId(),
                this.getClass().getName());
        return result;
    }
}
