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

package org.oracle.okafka.common.requests;

import java.util.List;

import org.oracle.okafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.utils.Utils;

public class MetadataRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<MetadataRequest> {
    	private static final List<String> ALL_TOPICS = null;
		private final List<String> topics;
		private final boolean allowAutoTopicCreation;
		private final List<String> teqParaTopic;
		
		public Builder(List<String> topics, boolean allowAutoTopicCreation, List<String> paraTopic) {
			super(ApiKeys.METADATA);
			this.topics = topics;
			this.allowAutoTopicCreation = allowAutoTopicCreation;
			this.teqParaTopic = paraTopic;
		}
		
		 public static Builder allTopics() {
	            return new Builder(ALL_TOPICS, false, ALL_TOPICS);
	     }
		 
		 public List<String> topics() {
	            return this.topics;
	     }

	     public boolean isAllTopics() {
	            return this.topics == ALL_TOPICS;
	     }
	     
	   
		@Override
        public MetadataRequest build() {
            return new MetadataRequest(topics, allowAutoTopicCreation, teqParaTopic);
        }
		
		@Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(type=metadataRequest").
            append(", topics=(").append(Utils.join(topics, ", "))
            .append(")");
            return bld.toString();
        }

		@Override
		public MetadataRequest build(short version) {
			return build();
		}
	}
	private final List<String> teqParaTopic;
    private final List<String> topics;
    private final boolean allowAutoTopicCreation;
	private MetadataRequest(List<String> topics, boolean allowAutoTopicCreation, List<String> teqParaTopic) {
		super(ApiKeys.METADATA, (short)1);
		this.topics = topics;
		this.allowAutoTopicCreation = allowAutoTopicCreation;
		this.teqParaTopic = teqParaTopic;
	}
	
	public List<String> topics() {
		return this.topics;
	}
	
	public boolean allowAutoTopicCreation() {
		return this.allowAutoTopicCreation;
	}
	
	  public List<String> teqParaTopics() {
	    	 return this.teqParaTopic;
	     }


	@Override
	public ApiMessage data() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
		// TODO Auto-generated method stub
		return null;
	}

}
