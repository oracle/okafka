package org.oracle.okafka.common.requests;

import java.util.List;

import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.requests.AbstractResponse;
import org.oracle.okafka.common.internals.SessionData;
import org.oracle.okafka.common.protocol.ApiKeys;

public class SyncGroupRequest extends AbstractRequest {
	public static class Builder extends AbstractRequest.Builder<SyncGroupRequest> {
    	private List<SessionData> sessionData;
    	private final int version;
		
		public Builder(List<SessionData> sessionData, int version) {
			super(ApiKeys.SYNC_GROUP);
			this.sessionData = sessionData;
			this.version = version;
		}
		
		@Override
		public SyncGroupRequest build(short apiVersion) {
			return new SyncGroupRequest(sessionData, version, apiVersion);
		}
		
		@Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(type=SyncGroupRequest")
            .append(")");
            return bld.toString();
        }

	}
	
	private List<SessionData> sessionData;
	private int version;
	public SyncGroupRequest(List<SessionData> sessionData, int version, short apiVersion) {
		super(ApiKeys.SYNC_GROUP, apiVersion);
		this.sessionData = sessionData;
		this.version = version;
	}
	
	public List<SessionData> getSessionData() {
		return this.sessionData;
	}
	
	public int getVersion() {
		return this.version;
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