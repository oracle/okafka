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

package org.oracle.okafka.common;

public class Node  extends org.apache.kafka.common.Node{
	private static final Node NO_NODE = new Node(-1, "", -1, "", "");

    private int id;
    private String idString;
    private String instanceName;
    private String host;
    private int port;
    private String serviceName;
    private String user;
    private String protocol;
    
    // Cache hashCode as it is called in performance sensitive parts of the code (e.g. RecordAccumulator.ready)
    private Integer hash;
    
    public Node(String host, int port, String serviceName) {
    	this(0, host, port, serviceName, "");
    }
    
    public Node(int id, String host, int port, String serviceName) {
    	this(id, host, port, "" , serviceName);
    }

    public Node(int id, String host, int port, String serviceName, String instanceName) {
    	super(id, host, port);
    	
    	if(id<=0)
    		id = 0;
        this.id = id;
        this.idString = "INSTANCE_"+id;
        if(host != null)
        	this.host = host.toUpperCase();
        this.port = port;
        if(serviceName != null)
        	this.serviceName = serviceName.toUpperCase();
        if(instanceName != null)
        	this.instanceName = instanceName.toUpperCase();
    }

    public static Node noNode() {
        return NO_NODE;
    }
    
    public Node(Node node) {
    	this(node.id(), node.host(), node.port(),node.serviceName(), node.instanceName());    	
    }

    public boolean isEmpty() {
        return host == null || host.isEmpty() || port < 0 || serviceName.isEmpty() ;
    }

    /**
     * The Node id of this Node
     */
    public int id() {
        return id;
    }
    
    public void setHost(String host) {
    	this.host = host.toUpperCase();
    }
    
    public void setId(int newId)
    {
    	if(id <=0)
    	{
    		id = newId;
    	}
    }
    
    public void setService(String _service)
    {
    	if(serviceName==null)
    		serviceName = _service.toUpperCase();
    }
    
    public void setInstanceName(String _instanceName)
    {
    	instanceName = _instanceName.toUpperCase();
    }
    
    public void setUser(String _user) {
    	if(user == null || user.length() == 0 )
    		this.user = _user.toUpperCase();
    }
    
    public void setProtocol(String _protocol)
    {
    	if(protocol == null)
    		protocol = _protocol.toUpperCase();
    }

    /**
     * The host name for this Node
     */
    public String host() {
        return host;
    }

    /**
     * The port for this Node
     */
    public int port() {
        return port;
    }
    
    /**
     * Name of the service running on this node/instance
     */
    public String serviceName() {
    	return serviceName;
    }
    
    /**
     * Name of this instance
     * @return name of the insatnce
     */
    public String instanceName() {
    	return instanceName;
    }
    
    public String idString() {
    	return idString;
    }
    
    public String user()
    {
    	return user;
    }
    
    public String protocol()
    {
    	return protocol;
    }
    

    @Override
    public int hashCode() {
    	if (this.hash == null) {
    		this.hash = getHashCode();
    	} 
    	return this.hash;
    }
    
    public int updateHashCode()
    {
        this.hash = getHashCode();
        return this.hash;
    }
    
    private int getHashCode()
    {
    	int result = 31 + (((host == null) || host.isEmpty()) ? 0 : host.hashCode());
        result = 31 * result + id;
        result = 31 * result + port;
        result = 31 * result +(((serviceName == null) || serviceName.isEmpty()) ? 0 : serviceName.hashCode());
        result = 31 * result +(((instanceName == null) || instanceName.isEmpty()) ? 0 : instanceName.hashCode());
        result = 31 * result +(((user == null) || user.isEmpty()) ? 0 : user.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        
        if (obj == null || getClass() != obj.getClass())
            return false;
        
        Node other = (Node) obj;
        return (host== null ? other.host() == null : host.equals(other.host())) &&
            id == other.id() &&
            port == other.port() &&          
            (serviceName == null ? other.serviceName() == null : serviceName.equals(other.serviceName())) &&
            (instanceName == null ? other.instanceName() == null : instanceName.equals(other.instanceName())); 
    }

    @Override
    public String toString() {
    	String str = ((serviceName != null) && !serviceName.equals("")) ? serviceName : "";
    	String str2 = ((instanceName != null) && !instanceName.equals("")) ? instanceName : "";
    	String str3 = ((user != null) && !user.equals("")) ? user : "";
        return id + ":" + host + ":" + port + ":" +  str + ":" + str2+ ":" + str3;
    }
}
