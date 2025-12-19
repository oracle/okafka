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

public class MessageUtils {
    public static byte[] convertTo4Byte(int len) {
        byte[] bArray = new byte[4];

        bArray[0] = (byte) (len >>> 24);
        bArray[1] = (byte) (len >>> 16);
        bArray[2] = (byte) (len >>> 8);
        bArray[3] = (byte) (len);

        return bArray;
    }

    public static int convertToInt(byte[] bInt) {
        return (((bInt[0] & 0xff) << 24) | ((bInt[1] & 0xff) << 16) | ((bInt[2] & 0xff) << 8)
                | (bInt[3] & 0xff));
    }

}
