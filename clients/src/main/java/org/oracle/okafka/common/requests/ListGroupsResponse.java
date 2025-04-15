package org.oracle.okafka.common.requests;

import java.util.List;
import java.util.Map;

import org.oracle.okafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;

public class ListGroupsResponse extends AbstractResponse{
	
	private final List<String> groupNames;
	private Exception exception;
	
	public ListGroupsResponse(List<String> groups ) {
        super(ApiKeys.LIST_GROUPS);
		this.groupNames = groups;
    }
	
	public List<String> groups(){
		return groupNames;
	}
	
	public void setException(Exception ex) {
			this.exception = ex;
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
