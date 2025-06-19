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

package oracle.jdbc.txeventq.kafka.connect.common.utils;

import java.sql.SQLException;

import javax.jms.Destination;
import javax.jms.JMSException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import oracle.jms.AQjmsDestination;

public abstract class JmsUtils {

    protected static final Logger log = LoggerFactory.getLogger(JmsUtils.class);

    /**
     * Determines if the Destination is an instance of AQjmsDestination. If an instance of
     * AQjmsDestination gets either the queue name or the topic name.
     * 
     * @param destination The Destination object
     *
     * @return The queue name or topic name depending on the instance type.
     * @throws JMSException
     * @throws SQLException
     */
    public static String destinationName(Destination destination)
            throws JMSException, SQLException {
        if (destination instanceof AQjmsDestination) {
            log.debug("Processing Destination of type AQjmsDestination");
            return ((AQjmsDestination) destination).getQueueName() != null
                    ? ((AQjmsDestination) destination).getQueueName()
                    : ((AQjmsDestination) destination).getTopicName();
        }
        return null;
    }

    /**
     * If the Destination is an instance of AQjmsDestination gets the owner of the queue or topic.
     * 
     * @param destination The Destination object.
     * @return The schema of the queue or topic. Null if not an instance of AQjmsDestination.
     * @throws JMSException
     */
    public static String destinationOwner(Destination destination) throws JMSException {
        if (destination instanceof AQjmsDestination) {
            return ((AQjmsDestination) destination).getQueueName() != null
                    ? ((AQjmsDestination) destination).getQueueOwner()
                    : ((AQjmsDestination) destination).getTopicOwner();
        }
        return null;
    }

    /**
     * If the Destination is an instance of AQjmsDestination gets the complete name of the queue or
     * topic in the form "[schema.]name".
     * 
     * @param destination The Destination object.
     * @return The complete name of the queue or topic. Null if not an instance of AQjmsDestination.
     */
    public static String destinationCompleteName(Destination destination) {
        if (destination instanceof AQjmsDestination) {
            return ((AQjmsDestination) destination).getCompleteName();
        }
        return null;
    }

    /**
     * If the Destination is an instance of AQjmsDestination gets the complete name of the queue
     * table of the queue or topic in the form "[schema.]name".
     * 
     * @param destination The Destination object.
     * @return The complete name of the queue's or topic's queue table. Null if not an instance of
     *         AQjmsDestination.
     */
    public static String destinationCompleteTableName(Destination destination) {
        if (destination instanceof AQjmsDestination) {
            return ((AQjmsDestination) destination).getCompleteTableName();
        }
        return null;
    }

    /**
     * Determines if the destination object is a queue or topic.
     * 
     * @param destination The Destination object.
     * @return A string of "queue" if the object is a queue and string of "topic" if the object is a
     *         topic. Null if none of the above apply.
     * @throws JMSException
     */
    public static String destinationType(Destination destination) throws JMSException {
        if (destination instanceof AQjmsDestination) {
            return ((AQjmsDestination) destination).getQueueName() != null ? "queue" : "topic";
        }
        return null;
    }
}
