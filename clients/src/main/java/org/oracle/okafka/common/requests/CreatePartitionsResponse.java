package org.oracle.okafka.common.requests;

import java.util.Map;

import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.oracle.okafka.common.errors.FeatureNotSupportedException;
import org.oracle.okafka.common.protocol.ApiKeys;

public class CreatePartitionsResponse  extends AbstractResponse  {
	private final Map<String, Exception> createPartitionTopicResult;
	private Exception exception;
	
	public CreatePartitionsResponse(Map<String, Exception> createPartitionTopicResult) {
		super(ApiKeys.CREATE_PARTITIONS);
		this.createPartitionTopicResult = createPartitionTopicResult;
	}
	
	public Map<String, Exception> getErrorsMap(){
		return this.createPartitionTopicResult;
	}
	
	public void setException(Exception exception) {
		this.exception = exception;
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
		throw new FeatureNotSupportedException("This feature is not suported for this release.");
	}

}
