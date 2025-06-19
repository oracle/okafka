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

public class Node {

    private final int id;
    private final String instanceName;

    public Node(int id, String instanceName) {
        if (id <= 0)
            id = 0;
        this.id = id;
        this.instanceName = instanceName;
    }

    public int getId() {
        return id;
    }

    public String getInstanceName() {
        return instanceName;
    }

    @Override
    public String toString() {
        return "Node [id=" + id + ", instanceName=" + instanceName;
    }

}
