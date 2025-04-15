package org.oracle.okafka.common.requests;

import org.oracle.okafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.requests.AbstractResponse;

public class ListGroupsRequest extends AbstractRequest {
	
	public static class Builder extends AbstractRequest.Builder<ListGroupsRequest> {
        public Builder() {
            super(ApiKeys.LIST_GROUPS);
        }

        @Override
        public ListGroupsRequest build(short version) {
            return new ListGroupsRequest(version);
        }

        @Override
        public String toString() {
        	return "(type = ListGroupsRequest)";
        }
    }
	
	public ListGroupsRequest(short version) {
        super(ApiKeys.LIST_GROUPS, version);
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
