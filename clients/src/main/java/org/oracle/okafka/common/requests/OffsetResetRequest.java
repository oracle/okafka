/*
** OKafka Java Client version 23.4.
**
** Copyright (c) 2019, 2024 Oracle and/or its affiliates.
** Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.
*/

package org.oracle.okafka.common.requests;

import java.util.Map;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.requests.AbstractResponse;
import org.oracle.okafka.common.protocol.ApiKeys;

public class OffsetResetRequest extends AbstractRequest {
	
	public static class Builder extends AbstractRequest.Builder<OffsetResetRequest> {
		
		private final Map<TopicPartition, Long> offsetResetTimestamps;
		private final long pollTimeoutMs;
		
		public Builder(Map<TopicPartition, Long> offsetResetTimestamps, long pollTimeoutMs) {
			super(ApiKeys.OFFSETRESET);
			this.offsetResetTimestamps = offsetResetTimestamps;
			this.pollTimeoutMs = pollTimeoutMs;
		}
		
		@Override
		public OffsetResetRequest build(short version) {
			return new OffsetResetRequest(offsetResetTimestamps, pollTimeoutMs);
		}
		
		@Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(type=OffsetResetRequest").
                append(", offsetResetTimestampss=").append(offsetResetTimestamps).
                append(")");
            return bld.toString();
        }

	}
	
	private final Map<TopicPartition, Long> offsetResetTimestamps;
	private final long pollTimeoutMs;
	private OffsetResetRequest(Map<TopicPartition, Long> offsetResetTimestamps, long pollTimeoutMs) {
		super(ApiKeys.OFFSETRESET, (short)0);
		this.offsetResetTimestamps = offsetResetTimestamps;
		this.pollTimeoutMs = pollTimeoutMs;
	}
	
	public Map<TopicPartition, Long> offsetResetTimestamps() {
		return this.offsetResetTimestamps;
	}
	
	public long pollTimeout() {
		return this.pollTimeoutMs;
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
