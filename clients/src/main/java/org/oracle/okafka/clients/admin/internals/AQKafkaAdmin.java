/*
** OKafka Java Client version 23.4.
**
** Copyright (c) 2019, 2024 Oracle and/or its affiliates.
** Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.
*/

package org.oracle.okafka.clients.admin.internals;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.admin.internals.AdminMetadataManager;
import org.oracle.okafka.clients.admin.AdminClientConfig;
import org.oracle.okafka.common.Node;
import org.oracle.okafka.common.errors.ConnectionException;
import org.oracle.okafka.common.errors.InvalidLoginCredentialsException;
import org.oracle.okafka.common.errors.RecordNotFoundSQLException;
import org.oracle.okafka.common.network.AQClient;
import org.oracle.okafka.common.protocol.ApiKeys;
import org.oracle.okafka.common.requests.CreateTopicsRequest;
import org.oracle.okafka.common.requests.CreateTopicsResponse;
import org.oracle.okafka.common.requests.DeleteTopicsRequest;
import org.oracle.okafka.common.requests.DeleteTopicsResponse;
import org.oracle.okafka.common.requests.MetadataRequest;
import org.oracle.okafka.common.requests.CreateTopicsRequest.TopicDetails;
import org.oracle.okafka.common.utils.ConnectionUtils;
import org.oracle.okafka.common.utils.CreateTopics;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;

/**
 * AQ client for publishing requests to AQ and generating reponses.
 *
 */
public class AQKafkaAdmin extends AQClient{
	
	//private final Logger log;
	private final AdminClientConfig configs;
	private final Time time;
	private final Map<Node, Connection> connections;
	private final AdminMetadataManager metadataManager;
	private boolean forceMetadata = false;
	
	public AQKafkaAdmin(LogContext logContext, AdminClientConfig configs, AdminMetadataManager _metadataManager, Time time) {
		super(logContext.logger(AQKafkaAdmin.class), configs);
		this.configs = configs;
		this.time = time;
		this.connections = new HashMap<Node, Connection>();	
		this.metadataManager = _metadataManager;
	}
	
	/**
	 * Send request to aq.
	 */
	@Override
	public ClientResponse send(ClientRequest request) {
		return parseRequest(request, ApiKeys.convertToOracleApiKey(request.apiKey()));
		
	}
	
	/**
	 * Determines the type of request and calls appropriate method for handling request
	 * @param request request to be sent
	 * @param key uniquely identifies type of request.
	 * @return response for given request
	 */
	private ClientResponse parseRequest( ClientRequest request, ApiKeys key) {
		if(key == ApiKeys.CREATE_TOPICS) 
			return createTopics(request);
		if(key == ApiKeys.DELETE_TOPICS)
			return deleteTopics(request);
		if(key == ApiKeys.METADATA) 
			return getMetadata(request);
		return null;
		
	}
	
	/**
	 * Executes a series of queries on each entry in list of topics for creating a corresponding topic .
	 * @param request request for creating list of topics
	 * @return response for create topics request.
	 */
	private ClientResponse createTopics(ClientRequest request) {
		
		 Node node = (org.oracle.okafka.common.Node) metadataManager.nodeById(Integer.parseInt(request.destination()));
		 CreateTopicsRequest.Builder builder= (CreateTopicsRequest.Builder)request.requestBuilder();
		 Map<String, TopicDetails> topics = builder.build().topics();
		 Connection jdbcConn = connections.get(node);		 
		 Map<String, Exception> result = new HashMap<String, Exception>();
		 Map<String, Uuid> topicIdMap = new HashMap<>();
		 SQLException exception = null;
		 try {			 			 
			 result = CreateTopics.createTopics(jdbcConn, topics, topicIdMap) ;
		 } catch(SQLException sql) {
			 sql.printStackTrace();
			 exception = sql;
			 log.trace("Unexcepted error occured with connection to node {}, closing the connection", request.destination());
			 log.trace("Failed to create topics {}", topics.keySet());
			 try {
				 jdbcConn.close(); 
				 connections.remove(node);				 
				 log.trace("Connection with node {} is closed", request.destination());
			 } catch(SQLException sqlException) {
				 log.trace("Failed to close connection with node {}", request.destination());
			 }
		 } 
		 return createTopicsResponse(result, exception, exception != null, request, topicIdMap);	 
	}
	
	@Override
	public boolean isChannelReady(Node node) {
		return connections.containsKey(node);
	}
	
	/**
	 * Generates a response for create topics request.
	 */
	private ClientResponse createTopicsResponse(Map<String, Exception> errors, Exception result, boolean disconnected,
			ClientRequest request, Map<String, Uuid> topicIdMap) {
				
		CreateTopicsResponse topicResponse = new CreateTopicsResponse(errors,topicIdMap);
		if(result != null)
			topicResponse.setResult(result);
		ClientResponse response = new ClientResponse(request.makeHeader((short)1),
				request.callback(), request.destination(), request.createdTimeMs(),
				time.milliseconds(), disconnected, null, null, topicResponse);

		return response;
	}
	
	/**
	 * Executes a series of queries on each entry in list of topics for deleting a corresponding topic .
	 * @param request request for deleting list of topics
	 * @return response for delete topics request.
	 */
	private ClientResponse deleteTopics(ClientRequest request) {
		 Node node =(org.oracle.okafka.common.Node) metadataManager.nodeById(Integer.parseInt(request.destination()));
		 DeleteTopicsRequest.Builder builder= (DeleteTopicsRequest.Builder)request.requestBuilder();
		 DeleteTopicsRequest deleteTopicRequest = builder.build();
		 if(deleteTopicRequest.topicIds()!=null) {
			 return deleteTopicsById(request);
		 }
		 Set<String> topics =deleteTopicRequest.topics();
		 Connection jdbcConn = connections.get(node);
		 String query = "begin dbms_aqadm.drop_sharded_queue(queue_name=>?, force =>(case ? when 1 then true else false end)); end;";
		 CallableStatement cStmt = null;
		 Map<String, SQLException> result = new HashMap<>();
		 Set<String> topicSet = new HashSet<>(topics);
		 try {			 
			 cStmt = jdbcConn.prepareCall(query);
			 for(String topic: topics) {
				 try {
					 cStmt.setString(1, topic);
					 cStmt.setInt(2, 1);
					 cStmt.execute();  
				 } catch(SQLException sql) {
					 log.trace("Failed to delete topic : {}", topic);
					 result.put(topic, sql);
				 }
				 if(result.get(topic) == null) {
					 topicSet.remove(topic);
					 log.trace("Deleted a topic : {}", topic);
					 result.put(topic, null);
				 }
				 
			 } 
		 } catch(SQLException sql) {
			 log.trace("Unexcepted error occured with connection to node {}, closing the connection", node);
			 log.trace("Failed to delete topics : {}", topicSet);
			 try {
				 connections.remove(node);
				 jdbcConn.close(); 
				 log.trace("Connection with node {} is closed", request.destination());
			 } catch(SQLException sqlException) {
				 log.trace("Failed to close connection with node {}", node);
			 }
			 return deleteTopicsResponse(result,Collections.emptyMap(), sql,true, request);
		 }
		 try {
			 cStmt.close();
		 } catch(SQLException sql) {
			 //do nothing
		 }
		 return deleteTopicsResponse(result,Collections.emptyMap(), null, false, request);
	}
	
	private ClientResponse deleteTopicsById(ClientRequest request) {
		 Node node =(org.oracle.okafka.common.Node) metadataManager.nodeById(Integer.parseInt(request.destination()));
		 DeleteTopicsRequest.Builder builder= (DeleteTopicsRequest.Builder)request.requestBuilder();
		 DeleteTopicsRequest deleteTopicRequest = builder.build();
		 Set<Uuid> topicIds =deleteTopicRequest.topicIds();
		 Connection jdbcConn = connections.get(node);
		 Map<Uuid, SQLException> result = new HashMap<>();
		 Set<Uuid> topicIdSet = new HashSet<>(topicIds);
		 
		 String query = "DECLARE\r\n"
		 		+ "   id NUMBER := ?;\r\n"
		 		+ "   queue_name VARCHAR2(100);\r\n"
		 		+ "BEGIN\r\n"
		 		+ "   SELECT name INTO queue_name\r\n"
		 		+ "   FROM user_queues\r\n"
		 		+ "   WHERE qid = id;\r\n"
		 		+ "\r\n"
		 		+ "   DBMS_AQADM.DROP_SHARDED_QUEUE(queue_name => queue_name, force => (CASE ? WHEN 1 THEN TRUE ELSE FALSE END));\r\n"
		 		+ "END;";
		 CallableStatement cStmt = null;
		 try {			 
			 cStmt = jdbcConn.prepareCall(query);
			 for(Uuid id: topicIds) {
				 String topic;
				 try {
					 topic = getTopicById(jdbcConn,id);
					 cStmt.setInt(1, (int)id.getLeastSignificantBits());
					 cStmt.setInt(2, 1);
					 cStmt.execute();  
				 } catch(SQLException sql) {
					 log.trace("Failed to delete topic with id : {}", id);
					 result.put(id, sql);
				 }
				 if(result.get(id) == null) {
					 topicIdSet.remove(id);
					 log.trace("Deleted a topic with id : {}", id);
					 result.put(id, null);
				 }
				 
			 } 
		 } catch(SQLException sql) {
			 log.trace("Unexcepted error occured with connection to node {}, closing the connection", node);
			 log.trace("Failed to delete topics with Ids : {}", topicIdSet);
			 try {
				 connections.remove(node);
				 jdbcConn.close(); 
				 log.trace("Connection with node {} is closed", request.destination());
			 } catch(SQLException sqlException) {
				 log.trace("Failed to close connection with node {}", node);
			 }
			 return deleteTopicsResponse(Collections.emptyMap(), result, sql,true, request);
		 }
		 try {
			 cStmt.close();
		 } catch(SQLException sql) {
			 //do nothing
		 }
		 return deleteTopicsResponse(Collections.emptyMap(),result, null, false, request);
		 
	}
	
	/**
	 * Generates a response for delete topics request.
	 */
	private ClientResponse deleteTopicsResponse(Map<String, SQLException> topicErrorMap, Map<Uuid, SQLException> topicIdErrorMap,  Exception result, boolean disconnected, ClientRequest request) {
		DeleteTopicsResponse topicResponse = new DeleteTopicsResponse(topicErrorMap, topicIdErrorMap);
		if(result != null)
			topicResponse.setResult(result);
		ClientResponse response = new ClientResponse(request.makeHeader((short)1),
				request.callback(), request.destination(), request.createdTimeMs(),
				time.milliseconds(), disconnected, null, null, topicResponse);
		return response;
	}
	
	private ClientResponse getMetadata(ClientRequest request) {
		Node node =(org.oracle.okafka.common.Node) metadataManager.nodeById(Integer.parseInt(request.destination()));
		if (node == null)
		{
			List<org.apache.kafka.common.Node> nodeList = metadataManager.updater().fetchNodes();
			for(org.apache.kafka.common.Node nodeNow : nodeList)
			{
				if(nodeNow.id() == Integer.parseInt(request.destination()))
				{
					node = (org.oracle.okafka.common.Node)nodeNow;
				}
			}
			
		}
		ClientResponse response = getMetadataNow(request, connections.get(node), node, forceMetadata);
		if(response.wasDisconnected()) { 

			connections.remove(node);
			forceMetadata = true;
		}
		
		return response;
	}
	
	/**
	 * Creates a connection to given node if already doesn't exist
	 * @param node node to be connected 
	 * @return returns connection established
	 * @throws SQLException
	 */
	private Connection getConnection(Node node) {
		try {
			Connection newConn = ConnectionUtils.createJDBCConnection(node, configs);
			connections.put(node, newConn);
		} catch(SQLException excp) {
			log.error("Exception while connecting to Oracle Database " + excp, excp);
			
			excp.printStackTrace();
			if(excp.getErrorCode()== 1017)
				throw new InvalidLoginCredentialsException(excp);
			
			throw new ConnectionException(excp.getMessage());
		}
		return connections.get(node);
	}
	
	@Override
	public void connect(Node node) {
		getConnection(node);
	}
	
	/**
	 * Closes all existing connections to a cluster.
	 */
	@Override
	public void close() {
		List<Node> closeNodes = new ArrayList<Node>();
		closeNodes.addAll(connections.keySet());
		closeNodes.forEach(n->close(n));
	}
	
	/**
	 * Close connection to a given node
	 */
	@Override
	public void close(Node node) {
		if(connections.containsKey(node)) {
			Connection conn = connections.get(node);
			try {
				conn.close();
				connections.remove(node);
				log.trace("Connection to node {} closed", node);
			} catch(SQLException sql) {
				
			}
			
		}
	}
}
