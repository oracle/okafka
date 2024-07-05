package org.oracle.okafka.common.requests;

import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.requests.AbstractResponse;
import org.oracle.okafka.common.internals.SessionData;
import org.oracle.okafka.common.protocol.ApiKeys;

public class JoinGroupRequest extends AbstractRequest {
	
	public static class Builder extends AbstractRequest.Builder<JoinGroupRequest> {
    	private SessionData sessionData;
		
		public Builder(SessionData sessionData) {
			super(ApiKeys.JOIN_GROUP);
			this.sessionData = sessionData;
		}
		
		@Override
        public JoinGroupRequest build() {
            return new JoinGroupRequest(sessionData);
        }
		
		@Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(type=joinGroupRequest")
            .append(")");
            return bld.toString();
        }

		@Override
		public JoinGroupRequest build(short version) {
			return build();
		}
		
	}
	
	private SessionData sessionData;
	public JoinGroupRequest(SessionData sessionData ) {
		super(ApiKeys.JOIN_GROUP, (short)1);
		this.sessionData = sessionData;
		
	}

	public SessionData getSessionData() {
		return this.sessionData;
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
