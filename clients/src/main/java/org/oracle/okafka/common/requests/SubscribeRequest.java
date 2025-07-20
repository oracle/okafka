/*
** OKafka Java Client version 23.4.
**
** Copyright (c) 2019, 2024 Oracle and/or its affiliates.
** Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.
*/

package org.oracle.okafka.common.requests;

import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.requests.AbstractResponse;
import org.oracle.okafka.common.protocol.ApiKeys;

public class SubscribeRequest extends AbstractRequest {
public static class Builder extends AbstractRequest.Builder<SubscribeRequest> {
		private final String topic;
		public Builder(String topic) {
			super(ApiKeys.SUBSCRIBE);
			this.topic = topic;
		}

		@Override
		public SubscribeRequest build(short version) {
			return new SubscribeRequest(topic, version);
		}
		
		@Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(type=subscribeRequest").
            append(", topics=").append(topic).
                append(")");
            return bld.toString();
        }
	}

    private final String topic;
    
    public SubscribeRequest(String topic, short version) {
    	super(ApiKeys.SUBSCRIBE, version);
	    this.topic = topic;
    }

    public String getTopic() {
    	return this.topic;
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

