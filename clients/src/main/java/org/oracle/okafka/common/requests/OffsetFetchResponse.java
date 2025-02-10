package org.oracle.okafka.common.requests;

import java.util.Map;
import java.util.Objects;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.oracle.okafka.common.protocol.ApiKeys;

public class OffsetFetchResponse extends AbstractResponse {
	
	private final Map<TopicPartition, Long> offsetFetchResponseMap;
	private Exception exception;
	
	public OffsetFetchResponse(Map<TopicPartition, Long> offsetFetchResponseMap) {
		super(ApiKeys.OFFSET_FETCH);
		this.offsetFetchResponseMap = offsetFetchResponseMap;
		exception = null;
	}
	
	public void setException(Exception ex) {
		this.exception=ex;
	}
	
	public Map<TopicPartition, Long> getOffsetFetchResponseMap(){
		return offsetFetchResponseMap;
	}
	
	public Exception getException() {
		return exception;
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
	public void maybeSetThrottleTimeMs(int throttleTimeMs) {
		// TODO Auto-generated method stub
		
	}

}
