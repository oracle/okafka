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

import org.oracle.okafka.common.protocol.ApiKeys;

public abstract class AbstractRequest extends org.apache.kafka.common.requests.AbstractRequest{

	public AbstractRequest(ApiKeys apiKey, short version)
	{
		super(ApiKeys.convertToApacheKafkaKey(apiKey), version);
	}
	
    public static abstract class Builder<T extends AbstractRequest> extends org.apache.kafka.common.requests.AbstractRequest.Builder<T> 
    {
        private final ApiKeys apiKey;

        
        public Builder(ApiKeys apiKey) { 
        	super(ApiKeys.convertToApacheKafkaKey(apiKey), (short)1);
            this.apiKey = apiKey;
        }

        public ApiKeys apiKeyOKafka() {
            return apiKey;
        }

    }
}
