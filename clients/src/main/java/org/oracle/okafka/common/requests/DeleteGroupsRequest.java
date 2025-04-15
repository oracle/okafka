package org.oracle.okafka.common.requests;

import java.util.List;

import org.apache.kafka.common.protocol.ApiMessage;
import org.oracle.okafka.common.protocol.ApiKeys;

public class DeleteGroupsRequest extends AbstractRequest {
	private final List<String> groups;
	
	public static class Builder extends AbstractRequest.Builder<DeleteGroupsRequest> {
        private final List<String> groups;

        public Builder(List<String> groups) {
            super(ApiKeys.DELETE_GROUPS);
            this.groups = groups;
        }

        @Override
        public DeleteGroupsRequest build(short version) {
            return new DeleteGroupsRequest(groups);
        }

        @Override
        public String toString() {
            return groups.toString();
        }
    }
	
	public DeleteGroupsRequest(List<String> groups) {
        super(ApiKeys.DELETE_GROUPS, (short)1);
        this.groups = groups;
    }
	
	public List<String> groups() {
        return this.groups;
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
