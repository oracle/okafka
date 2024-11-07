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

import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.Cluster;
import org.oracle.okafka.clients.NetworkClient;
import org.oracle.okafka.clients.TopicTeqParameters;
import org.oracle.okafka.common.Node;
import org.oracle.okafka.common.errors.FeatureNotSupportedException;
import org.oracle.okafka.common.protocol.ApiKeys;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;

public class MetadataResponse extends AbstractResponse {
	private final String clusterId;
	private final List<Node> nodes;
	private final List<PartitionInfo> partitionInfo;
	private final Map<String, Exception> errorsPerTopic;
	private final Map<Uuid, Exception> errorsPerTopicId;
	private Exception exception;
	private final Map<String, TopicTeqParameters> teqParams;
	private final Map<String,Uuid> topicsIdMap;
	
	
	public MetadataResponse(String clusterId, List<Node> nodes, List<PartitionInfo> partitionInfo,
			Map<String, Exception> errorsPerTopic, Map<Uuid,Exception> errorsPerTopicId, Map<String, TopicTeqParameters> _teqParams, Map<String,Uuid> topicsIdMap) {
		super(ApiKeys.METADATA);
		this.clusterId = clusterId;
		this.nodes = nodes;
		this.partitionInfo = partitionInfo;
		this.errorsPerTopic = errorsPerTopic;
		this.errorsPerTopicId = errorsPerTopicId;
		this.exception = null;
		this.teqParams = _teqParams;
		this.topicsIdMap = topicsIdMap;

	}
	
	public List<Node> nodes() {
		return nodes;
	}
	
	public List<PartitionInfo> partitions() {
		return partitionInfo;
	}
	
	public Map<String, TopicTeqParameters> teqParameters(){
		return teqParams;
	}
	
	public void setException(Exception exception) {
		this.exception = exception;
	}
	
	public Exception getException() {
		return this.exception;
	}
	
	public Map<String,Uuid> getTopicsIdMap(){
		return this.topicsIdMap;
		
	}
	/**
     * Get a snapshot of the cluster metadata from this response
     * @return the cluster snapshot
     */
   /* public Cluster cluster(AbstractConfig configs) {
    	return new Cluster(clusterId, NetworkClient.convertToKafkaNodes(nodes), partitionInfo,new HashSet<>(), new HashSet<>(), nodes.size() > 0 ?nodes.get(0) : null);//, configs);
    }*/
    
    /**
     * Get a snapshot of the cluster metadata from this response
     * @return the cluster snapshot
     */
    public Cluster cluster() {
    	return new Cluster(clusterId, NetworkClient.convertToKafkaNodes(nodes), partitionInfo,new HashSet<>(), new HashSet<>(), nodes.size() > 0 ?nodes.get(0) : null);//, configs);
    }
    
    public Map<String, Exception> topicErrors() {
    	return  this.errorsPerTopic;
    }
    
    public Map<Uuid, Exception> topicIdErrors(){
    	return this.errorsPerTopicId;
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

	@Override
	public int throttleTimeMs() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void maybeSetThrottleTimeMs(int arg0) {
		throw new FeatureNotSupportedException("This feature is not suported for this release.");		
	}
	
}

