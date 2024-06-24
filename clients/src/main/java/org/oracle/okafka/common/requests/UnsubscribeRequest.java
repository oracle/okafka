/*
** OKafka Java Client version 23.4.
**
** Copyright (c) 2019, 2020 Oracle and/or its affiliates.
** Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.
*/

package org.oracle.okafka.common.requests;

import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.requests.AbstractResponse;
import org.oracle.okafka.common.protocol.ApiKeys;

public class UnsubscribeRequest extends AbstractRequest {
public static class Builder extends AbstractRequest.Builder<UnsubscribeRequest> {
		
		public Builder() {
			super(ApiKeys.UNSUBSCRIBE);
		}
		
		@Override
        public UnsubscribeRequest build() {
            return new UnsubscribeRequest();
        }
		
		@Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(type=unsubscribeRequest").
                append(")");
            return bld.toString();
        }

		@Override
		public UnsubscribeRequest build(short version) {
			return build();
		}
	}
    public UnsubscribeRequest() {
    	super(ApiKeys.UNSUBSCRIBE,(short)1);
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
