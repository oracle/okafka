/*
** OKafka Java Client version 23.4.
**
** Copyright (c) 2019, 2024 Oracle and/or its affiliates.
** Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.
*/

package org.oracle.okafka.clients.admin.internals;

import java.net.ConnectException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.admin.internals.AdminMetadataManager;
import org.oracle.okafka.clients.NetworkClient;
import org.oracle.okafka.clients.admin.AdminClientConfig;
import org.oracle.okafka.common.Node;
import org.oracle.okafka.common.errors.ConnectionException;
import org.oracle.okafka.common.errors.InvalidLoginCredentialsException;
import org.oracle.okafka.common.network.AQClient;
import org.oracle.okafka.common.protocol.ApiKeys;
import org.oracle.okafka.common.requests.CreateTopicsRequest;
import org.oracle.okafka.common.requests.CreateTopicsResponse;
import org.oracle.okafka.common.requests.DeleteGroupsRequest;
import org.oracle.okafka.common.requests.DeleteGroupsResponse;
import org.oracle.okafka.common.requests.DeleteTopicsRequest;
import org.oracle.okafka.common.requests.DeleteTopicsResponse;
import org.oracle.okafka.common.requests.ListGroupsResponse;
import org.oracle.okafka.common.requests.OffsetFetchRequest;
import org.oracle.okafka.common.requests.OffsetFetchResponse;
import org.oracle.okafka.common.requests.CreateTopicsRequest.TopicDetails;
import org.oracle.okafka.common.requests.OffsetFetchResponse.PartitionOffsetData;
import org.oracle.okafka.common.utils.ConnectionUtils;
import org.oracle.okafka.common.utils.CreateTopics;
import org.oracle.okafka.common.utils.FetchOffsets;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
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
		if(key == ApiKeys.LIST_OFFSETS)
			return listOffsets(request);
		if(key == ApiKeys.METADATA) 
			return getMetadata(request);
		if(key == ApiKeys.OFFSET_FETCH)
			return getCommittedOffsets(request);
		if(key == ApiKeys.LIST_GROUPS)
			return getConsumerGroups(request);
		if(key == ApiKeys.DELETE_GROUPS)
			return deleteConsumerGroups(request);
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
					 topic = ConnectionUtils.getTopicById(jdbcConn,id);
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
	
	private ClientResponse listOffsets(ClientRequest request) {
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
		
		ClientResponse response = getOffsetsResponse(request,connections.get(node));
		if(response.wasDisconnected()) { 
			connections.remove(node);
			forceMetadata = true;
		}
		
		return response;
	}
	
	private ClientResponse getConsumerGroups(ClientRequest request) {
		Node node = (org.oracle.okafka.common.Node) metadataManager.nodeById(Integer.parseInt(request.destination()));
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
		
		Connection jdbcConn = connections.get(node);
		
		List<String> consumerGroups = new ArrayList<>();
		Exception exception = null;
		boolean disconnected = false;

		String query = "SELECT DISTINCT(CONSUMER_NAME) FROM USER_QUEUE_SUBSCRIBERS";
		CallableStatement cStmt = null;
		try {
			cStmt = jdbcConn.prepareCall(query);
			ResultSet rs = cStmt.executeQuery();
			while (rs.next()) {
				consumerGroups.add(rs.getString(1));
			}

			try {
				if (cStmt != null)
					cStmt.close();
			} catch (SQLException sql) {
				// do nothing
			}

		} catch (SQLException sqlE) {
			exception = sqlE;
			
			int errorCode = sqlE.getErrorCode();
			log.error("ListGroup: SQL Error:ORA-" + errorCode,sqlE);
			if (errorCode == 28 || errorCode == 17410) {
				disconnected = true;
				exception = new DisconnectException(sqlE.getMessage(),sqlE);
			} 
			
			log.trace("Unexcepted error occured with connection to node {}, closing the connection", node);
			log.trace("Failed to fetch Consumer Groups");
			try {
				connections.remove(node);
				jdbcConn.close();
				log.trace("Connection with node {} is closed", request.destination());
			} catch (SQLException sqlException) {
				log.trace("Failed to close connection with node {}", node);
			}
		}
		ListGroupsResponse listGroupsResponse = new ListGroupsResponse(consumerGroups);
		listGroupsResponse.setException(exception);

		return new ClientResponse(request.makeHeader((short) 1), request.callback(), request.destination(),
				request.createdTimeMs(), System.currentTimeMillis(), disconnected , null, null, listGroupsResponse);
	}
	
	private ClientResponse getCommittedOffsets(ClientRequest request) {

		OffsetFetchRequest.Builder builder = (OffsetFetchRequest.Builder) request.requestBuilder();
		OffsetFetchRequest offsetFetchRequest = builder.build();
		Map<String, List<TopicPartition>> perGroupTopicPartitions = offsetFetchRequest.perGroupTopicpartitions();
		Map<String, Map<TopicPartition, PartitionOffsetData>> responseMap = new HashMap<>();

		Node node = (org.oracle.okafka.common.Node) metadataManager.nodeById(Integer.parseInt(request.destination()));
		if (node == null) {
			List<org.apache.kafka.common.Node> nodeList = metadataManager.updater().fetchNodes();
			for (org.apache.kafka.common.Node nodeNow : nodeList) {
				if (nodeNow.id() == Integer.parseInt(request.destination())) {
					node = (org.oracle.okafka.common.Node) nodeNow;
				}
			}
		}

		Connection jdbcConn = connections.get(node);
		Exception exception = null;
		boolean disconnected = false;
		try {
			for (Map.Entry<String, List<TopicPartition>> groupEntry : perGroupTopicPartitions.entrySet()) {
				String groupId = groupEntry.getKey();
				List<TopicPartition> topicPartitions = groupEntry.getValue();
				Map<TopicPartition, PartitionOffsetData> offsetFetchResponseMap = null;

				if (topicPartitions == OffsetFetchRequest.ALL_TOPIC_PARTITIONS) {
					List<String> topicNames = fetchSubscribedQueues(groupId, jdbcConn);
					if (topicNames == null || topicNames.isEmpty()) {
						responseMap.put(groupId, offsetFetchResponseMap);
						continue;
					} else {
						topicPartitions = new ArrayList<>();
						for (String topic : topicNames) {
							int partNum = getQueueParameter(SHARDNUM_PARAM, topic, jdbcConn);
							for (int i = 0; i < partNum; i++) {
								topicPartitions.add(new TopicPartition(topic, i));
							}
						}
					}
				} else {
					if (fetchSubscribedQueues(groupId, jdbcConn).size() == 0) {
						responseMap.put(groupId, offsetFetchResponseMap);
						continue;
					}
				}
				offsetFetchResponseMap = new HashMap<>();
				Map<String, Integer> validInvalidTopicPartitionMap = new HashMap<>();
				for (TopicPartition tp : topicPartitions) {
					String topic = tp.topic();
					if (!validInvalidTopicPartitionMap.containsKey(topic)) {
						try {
							int totalPartition = getQueueParameter(SHARDNUM_PARAM, topic, jdbcConn);
							validInvalidTopicPartitionMap.put(topic, totalPartition);

						} catch (SQLException sqlE) {
							int errorNo = sqlE.getErrorCode();
							if (errorNo == 24010) {
								validInvalidTopicPartitionMap.put(topic, -1);
							}
						}
					}
					if (validInvalidTopicPartitionMap.get(topic) == -1 || validInvalidTopicPartitionMap.get(topic) <= tp.partition()) {
						offsetFetchResponseMap.put(tp, null);
						continue;
					}
					try {
						long offset = FetchOffsets.fetchCommittedOffset(tp.topic(), tp.partition(), groupId, jdbcConn);
						if (offset != -1)
							offsetFetchResponseMap.put(tp, new PartitionOffsetData(offset, null));
						else
							offsetFetchResponseMap.put(tp, null);
					} catch (SQLException sqlE) {
						int errorCode = sqlE.getErrorCode();
						log.error("ListConsumerGroupOffset: SQL Error:ORA-" + errorCode, sqlE);
						if (errorCode == 28 || errorCode == 17410) {
							disconnected = true;
							throw new DisconnectException(sqlE.getMessage(),sqlE);
						} else
							offsetFetchResponseMap.put(tp, new PartitionOffsetData(-1L, sqlE));

					}
				}
				responseMap.put(groupId, offsetFetchResponseMap);
			}
		} catch (Exception e) {
			try {
				exception = e;
				if(e instanceof SQLException) {
					int errorCode = ((SQLException)e).getErrorCode();
					log.error("SQL Error:ORA-" + errorCode);
					if (errorCode == 28 || errorCode == 17410) {
						disconnected = true;
						exception = new DisconnectException(e.getMessage(), e);
					} 
				}
				log.debug("Unexcepted error occured with connection to node {}, closing the connection",
						request.destination());
				if (jdbcConn != null)
					jdbcConn.close();

				log.trace("Connection with node {} is closed", request.destination());
			} catch (SQLException sqlEx) {
				log.trace("Failed to close connection with node {}", request.destination());
			}
		}

		if (disconnected) {
			connections.remove(node);
			forceMetadata = true;
		}

		OffsetFetchResponse offsetResponse = new OffsetFetchResponse(responseMap);
		offsetResponse.setException(exception);

		return new ClientResponse(request.makeHeader((short) 1), request.callback(), request.destination(),
				request.createdTimeMs(), System.currentTimeMillis(), disconnected, null, null, offsetResponse);

	}
	
	private ClientResponse deleteConsumerGroups(ClientRequest request) {
		DeleteGroupsRequest.Builder builder = (DeleteGroupsRequest.Builder) request.requestBuilder();
		DeleteGroupsRequest deleteGroupsRequest = builder.build();
		List<String> groups = deleteGroupsRequest.groups();
		Map<String, Exception> errors = new HashMap<>();

		Node node = (org.oracle.okafka.common.Node) metadataManager.nodeById(Integer.parseInt(request.destination()));
		if (node == null) {
			List<org.apache.kafka.common.Node> nodeList = metadataManager.updater().fetchNodes();
			for (org.apache.kafka.common.Node nodeNow : nodeList) {
				if (nodeNow.id() == Integer.parseInt(request.destination())) {
					node = (org.oracle.okafka.common.Node) nodeNow;
				}
			}
		}

		Connection jdbcConn = connections.get(node);
		Exception exception = null;
		boolean disconnected = false;
		
		String plsql = 
			    "BEGIN " +
			    "  DBMS_AQADM.REMOVE_SUBSCRIBER( " +
			    "    queue_name => ?, " +
			    "    subscriber => sys.aq$_agent(?, NULL, NULL) " +
			    "  ); " +
			    "END;";
		CallableStatement cStmt = null;
		try {
			for(String group : groups) {
					List<String> subscribedTopics = fetchSubscribedQueues(group, jdbcConn);

					if(subscribedTopics.isEmpty())
						errors.put(group, new GroupIdNotFoundException("The group doesn't exist"));
					else {
						cStmt = jdbcConn.prepareCall(plsql);

						for(String topic: subscribedTopics) {
							cStmt.setString(1, topic);
							cStmt.setString(2, group);
							cStmt.execute();  
						}
						errors.put(group, null);
					}
			}
		}catch (SQLException sqlE) {
			exception = sqlE;

			if (sqlE.getErrorCode() == 6550) {
				log.error("Not all privileges granted to the database user.",
						sqlE.getMessage());
				log.info("Please grant all the documented privileges to database user.");
				if (sqlE instanceof SQLSyntaxErrorException) {
					log.trace("Please grant all the documented privileges to database user.");
				}
			}

			int errorCode = sqlE.getErrorCode();
			log.error("DeleteConsumerGroups: SQL Error:ORA-" + errorCode, sqlE);
			if (errorCode == 28 || errorCode == 17410) {
				disconnected = true;
				exception = new DisconnectException(sqlE.getMessage(), sqlE);
			}
			try {
				log.debug("Unexcepted error occured with connection to node {}, closing the connection",
						request.destination());
				if (jdbcConn != null)
					jdbcConn.close();

				log.trace("Connection with node {} is closed", request.destination());
			} catch (SQLException sqlEx) {
				log.trace("Failed to close connection with node {}", request.destination());
			}
		} 
		DeleteGroupsResponse deleteGroupsResponse = new DeleteGroupsResponse(errors);
		deleteGroupsResponse.setException(exception);
		return new ClientResponse(request.makeHeader((short) 1), request.callback(), request.destination(),
				request.createdTimeMs(), System.currentTimeMillis(), disconnected, null, null, deleteGroupsResponse);
		
	}
	
	/**
	 * Creates a connection to given node if already doesn't exist
	 * @param node node to be connected 
	 * @return returns connection established
	 * @throws SQLException
	 */
	private Connection getConnection(Node node) {
		try {
			Connection newConn = ConnectionUtils.createJDBCConnection(node, configs, this.log);
			connections.put(node, newConn);
			List<Node> nodes = new ArrayList<>();
			String clusterId = ((oracle.jdbc.internal.OracleConnection) newConn).getServerSessionInfo()
					.getProperty("DATABASE_NAME");
			this.getNodes(nodes, newConn, node, forceMetadata);
			Cluster newCluster = new Cluster(clusterId, NetworkClient.convertToKafkaNodes(nodes),
					Collections.emptySet(), Collections.emptySet(), Collections.emptySet(),
					nodes.size() > 0 ? nodes.get(0) : null);// , configs);
			this.metadataManager.update(newCluster, System.currentTimeMillis());
		} catch (SQLException excp) {
			log.error("Exception while connecting to Oracle Database " + excp, excp);
			int errorCode = excp.getErrorCode();
			if (errorCode == 1017)
				throw new InvalidLoginCredentialsException(excp);
			if(errorCode==12514 || errorCode==12541)
				throw new ConnectionException(new ConnectException(excp.getMessage()));
			
			throw new ConnectionException(excp);
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
