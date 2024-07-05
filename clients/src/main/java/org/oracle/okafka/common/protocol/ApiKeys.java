/*
** OKafka Java Client version 23.4.
**
** Copyright (c) 2019, 2024 Oracle and/or its affiliates.
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
** OKafka Java Client version 23.4.
**
** Copyright (c) 2019, 2020 Oracle and/or its affiliates.
** Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.
*/

package 
org.oracle.okafka.common.protocol;

/**
 * Identifiers for all the Kafka APIs
 */
public enum ApiKeys {
    CREATE_TOPICS(0, "CreateTopics"),
    DELETE_TOPICS(1, "DeleteTopics"),
	METADATA(2, "Metadata"),
	PRODUCE(3, "Produce"),
	FETCH(4, "Consume"),
	COMMIT(5, "Commit"),
	SUBSCRIBE(6, "Subscribe"),
	OFFSETRESET(7, "OffsetReset"),
	UNSUBSCRIBE(8, "Unsubscribe"),
	JOIN_GROUP(9, "JoinGroup"),
	SYNC_GROUP(10, "SyncGroup"),
	CONNECT_ME(11,"ConnectMe");
    private static final ApiKeys[] ID_TO_TYPE;
    private static final int MIN_API_KEY = 0;
    public static final int MAX_API_KEY;

    static {
        int maxKey = -1;
        for (ApiKeys key : ApiKeys.values())
            maxKey = Math.max(maxKey, key.id);
        ApiKeys[] idToType = new ApiKeys[maxKey + 1];
        for (ApiKeys key : ApiKeys.values())
            idToType[key.id] = key;
        ID_TO_TYPE = idToType;
        MAX_API_KEY = maxKey;
    }

    /** the permanent and immutable id of an API--this can't change ever */
    public final short id;

    /** an english description of the api--this is for debugging and can change */
    public final String name;

    ApiKeys(int id, String name) {
        if (id < 0)
            throw new IllegalArgumentException("id must not be negative, id: " + id);
        this.id = (short) id;
        this.name = name;
    }

    public static ApiKeys forId(int id) {
        if (!hasId(id))
            throw new IllegalArgumentException(String.format("Unexpected ApiKeys id `%s`, it should be between `%s` " +
                    "and `%s` (inclusive)", id, MIN_API_KEY, MAX_API_KEY));
        return ID_TO_TYPE[id];
    }

    public static boolean hasId(int id) {
        return id >= MIN_API_KEY && id <= MAX_API_KEY;
    }

    private static String toHtml() {
        final StringBuilder b = new StringBuilder();
        b.append("<table class=\"data-table\"><tbody>\n");
        b.append("<tr>");
        b.append("<th>Name</th>\n");
        b.append("<th>Key</th>\n");
        b.append("</tr>");
        for (ApiKeys key : ApiKeys.values()) {
            b.append("<tr>\n");
            b.append("<td>");
            b.append("<a href=\"#The_Messages_" + key.name + "\">" + key.name + "</a>");
            b.append("</td>");
            b.append("<td>");
            b.append(key.id);
            b.append("</td>");
            b.append("</tr>\n");
        }
        b.append("</table>\n");
        return b.toString();
    }

    public static void main(String[] args) {
        System.out.println(toHtml());
    }
    
    public static org.apache.kafka.common.protocol.ApiKeys convertToApacheKafkaKey(ApiKeys apiKey)
    {
    	switch(apiKey)
    	{
    	case CREATE_TOPICS:
    		return org.apache.kafka.common.protocol.ApiKeys.CREATE_TOPICS;
    	case DELETE_TOPICS:
    		return org.apache.kafka.common.protocol.ApiKeys.DELETE_TOPICS;
    	case METADATA:
    		return org.apache.kafka.common.protocol.ApiKeys.METADATA;
    	case PRODUCE:
    		return org.apache.kafka.common.protocol.ApiKeys.PRODUCE;
    	case FETCH:
    		return org.apache.kafka.common.protocol.ApiKeys.FETCH;
    	case COMMIT:
    		return org.apache.kafka.common.protocol.ApiKeys.OFFSET_COMMIT;
    	case SUBSCRIBE:
    		//Not present in Apache Kafka. Dummy set to DESCRIBE_GROUPS
    		return org.apache.kafka.common.protocol.ApiKeys.DESCRIBE_GROUPS;
    	case OFFSETRESET:
    		//Seek operation. 
    		return org.apache.kafka.common.protocol.ApiKeys.OFFSET_FETCH;
    	case UNSUBSCRIBE:
    		return org.apache.kafka.common.protocol.ApiKeys.DELETE_GROUPS;
    	case JOIN_GROUP:
    		return org.apache.kafka.common.protocol.ApiKeys.JOIN_GROUP;
    	case SYNC_GROUP:
    		return org.apache.kafka.common.protocol.ApiKeys.SYNC_GROUP;
    	case CONNECT_ME:
    		//Operation to find Oracle RAC Node to connect to. Not exactly a FIND_CORRDINATOR call.
    		return org.apache.kafka.common.protocol.ApiKeys.FIND_COORDINATOR;
    	default: 
    		// Default to HEARTBEAT. No SUpport for HEARTBEAT for oKafka.
    		return org.apache.kafka.common.protocol.ApiKeys.HEARTBEAT;
    	}
    }
    
    public static ApiKeys convertToOracleApiKey(org.apache.kafka.common.protocol.ApiKeys apiKey)
    {
    	switch(apiKey)
    	{
    	case CREATE_TOPICS:
    		return ApiKeys.CREATE_TOPICS;
    	case DELETE_TOPICS:
    		return ApiKeys.DELETE_TOPICS;
    	case METADATA:
    		return ApiKeys.METADATA;
    	case PRODUCE:
    		return ApiKeys.PRODUCE;
    	case FETCH:
    		return ApiKeys.FETCH;
    	case OFFSET_COMMIT:
    		return COMMIT;
    	case DESCRIBE_GROUPS:
    		//Not present in Apache Kafka. Dummy set to DESCRIBE_GROUPS
    		return SUBSCRIBE;
    	case OFFSET_FETCH:
    		//Seek operation. 
    		return OFFSETRESET;
    	case DELETE_GROUPS:
    		return UNSUBSCRIBE;
    	case JOIN_GROUP:
    		return JOIN_GROUP;
    	case SYNC_GROUP:
    		return SYNC_GROUP;
    	case FIND_COORDINATOR:
    		//Operation to find Oracle RAC Node to connect to. Not exactly a FIND_CORRDINATOR call.
    		return CONNECT_ME;
    	default: 
    		// Default to FETCH.
    		return FETCH;
    	}
    }
}
