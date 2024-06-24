/*
** OKafka Java Client version 23.4.
**
** Copyright (c) 2019, 2020 Oracle and/or its affiliates.
** Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.
*/

package org.oracle.okafka.common.requests;

import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.oracle.okafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.requests.AbstractResponse;
import org.oracle.okafka.common.protocol.ApiKeys;

public class CommitRequest extends AbstractRequest {
public static class Builder extends AbstractRequest.Builder<CommitRequest>  {
		
		private final Map<Node, List<TopicPartition>> nodeTPMap;
		private final Map<TopicPartition, OffsetAndMetadata> offsetAndMetadata;
		
		public Builder(Map<Node, List<TopicPartition>> _nodeTPMap, Map<TopicPartition, OffsetAndMetadata> offsetAndMetadata) {
			super(ApiKeys.COMMIT);
			this.nodeTPMap = _nodeTPMap;
			this.offsetAndMetadata = offsetAndMetadata;
		}
		
		@Override
        public CommitRequest build() {
            return new CommitRequest(nodeTPMap, offsetAndMetadata);
        }
		
		@Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(type=commitRequest").
                append(")");
            return bld.toString();
        }

		@Override
		public CommitRequest build(short version) {
			return new CommitRequest(nodeTPMap, offsetAndMetadata);
		}
	}
	
private final Map<Node, List<TopicPartition>> nodeTPMap;
private final Map<TopicPartition, OffsetAndMetadata> offsetAndMetadata;
	private CommitRequest(Map<Node, List<TopicPartition>> _nodeTPMap, Map<TopicPartition, OffsetAndMetadata> offsetAndMetadata) {
		super(ApiKeys.COMMIT,(short)1);
		this.nodeTPMap = _nodeTPMap;
		this.offsetAndMetadata = offsetAndMetadata;
	}
	
	public Map<Node, List<TopicPartition>> nodes() {
		return this.nodeTPMap;
	}
	
	public Map<TopicPartition, OffsetAndMetadata> offsets() {
		return this.offsetAndMetadata;
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
