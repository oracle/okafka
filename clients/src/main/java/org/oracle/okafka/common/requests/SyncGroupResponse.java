package org.oracle.okafka.common.requests;

import java.util.Map;

import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.oracle.okafka.common.internals.SessionData;
import org.oracle.okafka.common.protocol.ApiKeys;

public class SyncGroupResponse extends AbstractResponse {
	private SessionData sessionData;
	private int version;
	private Exception exception;
   
	public SyncGroupResponse(SessionData sessionData, int version, Exception exception) {
		super(ApiKeys.SYNC_GROUP);
		this.sessionData = sessionData;
		this.version = version;
		this.exception = exception;
	}
	
	public SessionData getSessionData() {
		return this.sessionData;
	}
	
	public int getVersion() {
		return this.version;
	}
	
	public Exception getException() {
		return this.exception;
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
