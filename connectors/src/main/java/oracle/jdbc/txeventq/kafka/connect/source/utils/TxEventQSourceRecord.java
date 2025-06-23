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

package oracle.jdbc.txeventq.kafka.connect.source.utils;

import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;

/**
 * A message is the unit that is enqueued or dequeued. An TEQ Message object holds both its content,
 * or payload, and its properties. This class provides methods to get and set message properties and
 * the payload.
 *
 * @param <T>
 */

public class TxEventQSourceRecord extends SourceRecord {

    // the id (16 bytes) of this message.
    private byte[] messageId = new byte[0];

    public enum PayloadType {
        RAW, JSON, JMS_BYTES, JMS_TEXT, JMS_MAP
    }

    private final PayloadType payloadType;

    public TxEventQSourceRecord(Map<String, ?> sourcePartition, Map<String, ?> sourceOffset,
            String topic, Integer partition, Schema valueSchema, Object value, PayloadType type,
            byte[] msgId) {
        super(sourcePartition, sourceOffset, topic, partition, valueSchema, value);
        this.payloadType = type;
        this.messageId = msgId;
    }

    public TxEventQSourceRecord(Map<String, ?> sourcePartition, Map<String, ?> sourceOffset,
            String topic, Integer partition, Schema keySchema, java.lang.Object key,
            Schema valueSchema, Object value, PayloadType type, byte[] msgId) {
        super(sourcePartition, sourceOffset, topic, partition, keySchema, key, valueSchema, value);
        this.payloadType = type;
        this.messageId = msgId;
    }

    public TxEventQSourceRecord(Map<String, ?> sourcePartition, Map<String, ?> sourceOffset,
            String topic, Schema valueSchema, Object value, PayloadType type, byte[] msgId) {
        super(sourcePartition, sourceOffset, topic, valueSchema, value);
        this.payloadType = type;
        this.messageId = msgId;
    }

    public TxEventQSourceRecord(Map<String, ?> sourcePartition, Map<String, ?> sourceOffset,
            String topic, Schema keySchema, java.lang.Object key, Schema valueSchema, Object value,
            PayloadType type, byte[] msgId) {
        super(sourcePartition, sourceOffset, topic, keySchema, key, valueSchema, value);
        this.payloadType = type;
        this.messageId = msgId;
    }

    public PayloadType getPayloadType() {
        return payloadType;
    }

    public String getMessageId() {
        return byteArrayToHex(messageId);
    }

    private static String byteArrayToHex(byte[] a) {
        StringBuilder sb = new StringBuilder(a.length * 2);
        for (byte b : a)
            sb.append(String.format("%02x", b));
        return sb.toString();
    }
}
