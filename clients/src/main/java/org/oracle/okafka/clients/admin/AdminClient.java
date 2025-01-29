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

package org.oracle.okafka.clients.admin;

import java.util.Map;
import java.util.Properties;

import org.apache.kafka.common.annotation.InterfaceStability;

/**
  * The administrative client for Transactional Event Queues(TXEQ), which supports managing and inspecting topics.
 * For this release only creation of topic(s) and deletion of topic(s) is supported.
 * A topic can be created by invoking {@code #createTopics(Collection)} and deleted by invoking {@code #deleteTopics(Collection)} method.
 * <p>
 * Topic can be created with following configuration. 
 * <p>
 * retention.ms: Amount of time in milliseconds for which records stay in topic and are available for consumption. Internally, <i>retention.ms</i> value is rounded to the second. Default value for this parameter is 7 days.
 * </p>
 */
@InterfaceStability.Evolving
public abstract class AdminClient implements Admin {

    /**
     * Create a new AdminClient with the given configuration.
     *
     * @param props The configuration.
     * @return The new KafkaAdminClient.
     */
	final static String DUMMY_BOOTSTRAP ="localhost:1521";
    public static AdminClient create(Properties props) {
    	String bootStrap = (String)props.get(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG);
    	if(bootStrap== null)
    	{
    		String secProtocol = props.getProperty(AdminClientConfig.SECURITY_PROTOCOL_CONFIG);
    		if(secProtocol != null && secProtocol.equalsIgnoreCase("SSL")) {
    			// Connect using Oracle Wallet and tnsnames.ora. 
    			// User does not need to know the database host ip and port.
    			props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, DUMMY_BOOTSTRAP);
    		}
    	}
        return KafkaAdminClient.createInternal(new org.oracle.okafka.clients.admin.AdminClientConfig(props), new KafkaAdminClient.TimeoutProcessorFactory());
    }

    /**
     * Create a new AdminClient with the given configuration.
     *
     * @param conf The configuration.
     * @return The new KafkaAdminClient.
     */
    public static AdminClient create(Map<String, Object> conf) {
    	
    	String bootStrap = (String)conf.get(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG);
    	if(bootStrap == null)
    	{
    		String setSecProtocol = (String)conf.get(AdminClientConfig.SECURITY_PROTOCOL_CONFIG);
    		if(setSecProtocol != null && setSecProtocol.equalsIgnoreCase("SSL")) 
    		{
    			// Connect using Wallet and TNSNAMES.ora. 
    			// User does not need to know the database host ip and port.
    			conf.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, DUMMY_BOOTSTRAP);
    		}
    	}
        return KafkaAdminClient.createInternal(new AdminClientConfig(conf), null);
    }
    
}
