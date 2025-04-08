package org.oracle.okafka.common.requests;

import java.util.Map;

import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.oracle.okafka.common.protocol.ApiKeys;

public class DeleteGroupsResponse extends AbstractResponse {

	private final Map<String, Exception> errorMap;
	private Exception exception;

    public DeleteGroupsResponse(Map<String, Exception> errors) {
        super(ApiKeys.DELETE_GROUPS);
        this.errorMap = errors;
    }

    public Map<String, Exception> errors(){
    	return errorMap;
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
		// TODO Auto-generated method stub
		
	}
}
