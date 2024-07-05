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
 * 04/20/2020: This file is modified to support Kafka Java Client compatability to Oracle Transactional Event Queues.
 *
 */

package org.oracle.okafka.common.requests;

import java.util.List;
import java.util.Map;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.oracle.okafka.common.protocol.ApiKeys;
import org.oracle.okafka.common.requests.AbstractResponse;
import org.oracle.okafka.common.utils.MessageIdConverter.OKafkaOffset;

public class ProduceResponse extends AbstractResponse {

    public static final long INVALID_OFFSET = -1L;

    //private final Map<TopicPartition, PartitionResponse> responses;
    private final TopicPartition topicPartition;
    private final PartitionResponse partitionResponse;
    private final int throttleTimeMs;
    public static final int DEFAULT_THROTTLE_TIME = 0;
    private String msgIdToCheck;
    private boolean checkDuplicate;
    /**
     * Constructor for Version 0
     */
    public ProduceResponse(TopicPartition topicPartition, PartitionResponse partitionResponse) {
        this(topicPartition, partitionResponse, DEFAULT_THROTTLE_TIME);
    }

    /**
     * Constructor for the latest version
     */
    public ProduceResponse(TopicPartition topicPartition, PartitionResponse partitionResponse, int throttleTimeMs) {
    	super(ApiKeys.PRODUCE);
        this.topicPartition = topicPartition;
        this.partitionResponse = partitionResponse;
        this.throttleTimeMs = throttleTimeMs;
    }

    /*public Map<TopicPartition, PartitionResponse> responses() {
        return this.responses;
    }*/
    
    public PartitionResponse getPartitionResponse() {
    	return this.partitionResponse;
    }
    
    public TopicPartition getPartition() {
    	return this.topicPartition;
    }

    public int throttleTimeMs() {
        return this.throttleTimeMs;
    }
    
    /*public void add(Map<TopicPartition, PartitionResponse> partitionResponses) {
    	responses.putAll(partitionResponses);
    }*/
  

    public static final class PartitionResponse {
        public RuntimeException exception;
        public List<OKafkaOffset> msgIds;
        //public List<Long> logAppendTime;
        public long logAppendTime;
        public long subPartitionId;
        private boolean checkDuplicate;

        public PartitionResponse(RuntimeException exception) {
            this.exception = exception;
            this.logAppendTime = 0l;
            this.msgIds = null;
        }

        @Override
        public String toString() {
            StringBuilder b = new StringBuilder();
            b.append('{');
            b.append("error: ");
            b.append(exception.toString());
            b.append(",logAppendTime: ");
            b.append(logAppendTime);
            b.append('}');
            return b.toString();
        }
        
        public RuntimeException exception() {
        	return exception;
        }
        
    	public boolean getCheckDuplicate()
    	{
    		return this.checkDuplicate;
    	}
    	public void setCheckDuplicate(boolean checkDups)
    	{
    		this.checkDuplicate = checkDups; 
    	}
    	
    	public List<OKafkaOffset> getOffsets()
    	{
    		return msgIds;
    	}
    	public void setOffsets (List<OKafkaOffset> offsetList) {
    		this.msgIds = offsetList;
    	}
    }

    public boolean shouldClientThrottle(short version) {
        return version >= 6;
    }

	@Override
	public ApiMessage data() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<Errors, Integer> errorCounts() {
		// TODO Auto-generated method stub
		return null;
	}
	

}
