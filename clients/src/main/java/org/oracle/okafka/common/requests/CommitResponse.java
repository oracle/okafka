/*
** OKafka Java Client version 23.4.
**
** Copyright (c) 2019, 2024 Oracle and/or its affiliates.
** Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.
*/

package org.oracle.okafka.common.requests;

import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.oracle.okafka.common.Node;
import org.oracle.okafka.common.protocol.ApiKeys;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;

public class CommitResponse extends AbstractResponse {
	
	private final boolean error;
	private final Map<Node, Exception> result;
	private final Map<Node, List<TopicPartition>> nodes;
	private final Map<TopicPartition, OffsetAndMetadata> offsets;
	
	public CommitResponse(Map<Node, Exception> result, Map<Node, List<TopicPartition>> nodes,
			              Map<TopicPartition, OffsetAndMetadata> offsets, boolean error) {
		super(ApiKeys.COMMIT);
		this.result = result;
		this.nodes = nodes;
		this.offsets = offsets;
		this.error = error;
		
	}
	
	public Map<Node, Exception> getResult() {
		return result;
	}
	
	public Map<Node, List<TopicPartition>> getNodes() {
		return nodes;
	}
	
	public Map<TopicPartition, OffsetAndMetadata> offsets() {
		return offsets;
	}
	
	public boolean error() {
		return error;
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
		// TODO Auto-generated method stub
		
	}
	

}
