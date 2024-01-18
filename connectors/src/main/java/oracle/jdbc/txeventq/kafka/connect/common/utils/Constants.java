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

package oracle.jdbc.txeventq.kafka.connect.common.utils;

/**
 * Contains a collection of defined constants.
 */
public final class Constants {

    private Constants() {
    }

    /**
     * Retriable Ora Errors
     */
    // Timeout or End-of-Fetch Error When Dequeuing Messages
    public static final int ORA_25228 = 25228;

    // IO Error: Connection reset
    public static final int ORA_17002 = 17002;

    // Closed Connection.
    public static final int ORA_17008 = 17008;

    // TNS no listener.
    public static final int ORA_12541 = 12541;

    // Unknown host specified
    public static final int ORA_17868 = 17868;

    // No more data to read from socket
    public static final int ORA_17410 = 17410;

    // ORACLE initialization or shutdown in progress
    public static final int ORA_01033 = 1033;

    // The Oracle instance is not available for use. Start the instance.
    public static final int ORA_01034 = 1034;

    // Immediate shutdown or close in progress
    public static final int ORA_01089 = 1089;

    // Cannot connect to event stream owner instance {} of database {}
    public static final int ORA_24221 = 24221;

    // Cannot connect to shard owner instance {} of database {}
    public static final int ORA_25348 = 25348;

    // Session is closed.
    public static final int JMS_131 = 131;

    // Database not open
    public static final int ORA_01109 = 1109;

}
