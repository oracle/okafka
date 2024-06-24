/*
** OKafka Java Client version 23.4.
**
** Copyright (c) 2019, 2020 Oracle and/or its affiliates.
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

/*
 * 04/20/2020: This file is modified to support Kafka Java Client compatability to Oracle Transactional Event Queues.
 *
 */

package org.oracle.okafka.clients.producer.internals;

import java.util.List;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.oracle.okafka.common.utils.MessageIdConverter.OKafkaOffset;

/**
 * A class that models the future completion of a produce request for a single partition. There is one of these per
 * partition in a produce request and it is shared by all the {@link RecordMetadata} instances that are batched together
 * for the same partition in the request.
 */
public class ProduceRequestResult extends org.apache.kafka.clients.producer.internals.ProduceRequestResult {

    private volatile List<OKafkaOffset> msgIds = null;

    /**
     * Create an instance of this class.
     *
     * @param topicPartition The topic and partition to which this record set was sent was sent
     */
    public ProduceRequestResult(TopicPartition topicPartition) {
        super(topicPartition);
    }

    /**
     * Set the result of the produce request.
     *
     * @param baseOffset The base offset assigned to the record
     * @param logAppendTime The log append time or -1 if CreateTime is being used
     * @param error The error that occurred if there was one, or null
     */
    public void set(long baseOffset, long logAppendTime, List<OKafkaOffset> msgIds, RuntimeException error) {
        set(baseOffset, logAppendTime, error);
        this.msgIds = msgIds;
    }

    /**
     * The base offset for the request (the first offset in the record set)
     */
    public List<OKafkaOffset> msgIds() {
        return msgIds;
    }

}
