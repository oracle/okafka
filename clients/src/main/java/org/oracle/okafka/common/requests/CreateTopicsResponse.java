/*
** OKafka Java Client version 23.4.
**
** Copyright (c) 2019, 2024 Oracle and/or its affiliates.
** Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.
*/

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * 04/20/2020: This file is modified to support Kafka Java Client compatability to Oracle Transactional Event Queues.
 *
 */

package org.oracle.okafka.common.requests;

import java.util.Map;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.oracle.okafka.common.errors.FeatureNotSupportedException;
import org.oracle.okafka.common.protocol.ApiKeys;

public class CreateTopicsResponse extends AbstractResponse {
    final Map<String, Exception> errors;
    final Map<String, Uuid> topicIdMap;
    private Exception requestResult;

    public CreateTopicsResponse(Map<String, Exception> errors, Map<String,Uuid> topicIdMap) {
    	super(ApiKeys.CREATE_TOPICS);
        this.errors = errors;
        this.topicIdMap=topicIdMap;
        this.requestResult = null;
    }

    public Map<String, Exception> errors() {
        return errors;
    }
    
    public void setResult(Exception ex) {
    	if(requestResult != null) {
    		requestResult = ex;
    	}
    }
    
    public Exception getResult() {
    	return requestResult;
    }
    
    public Map<String, Uuid> topicIdMap(){
    	return topicIdMap;
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
		throw new FeatureNotSupportedException("This feature is not suported for this release.");		
	}

}
