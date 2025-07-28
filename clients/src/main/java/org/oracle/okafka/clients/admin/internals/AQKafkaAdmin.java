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
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.internals.AdminMetadataManager;
import org.oracle.okafka.clients.NetworkClient;
import org.oracle.okafka.clients.admin.AdminClientConfig;
import org.oracle.okafka.common.Node;
import org.oracle.okafka.common.errors.ConnectionException;
import org.oracle.okafka.common.errors.InvalidLoginCredentialsException;
import org.oracle.okafka.common.network.AQClient;
import org.oracle.okafka.common.protocol.ApiKeys;
import org.oracle.okafka.common.requests.CreatePartitionsRequest;
import org.oracle.okafka.common.requests.CreatePartitionsResponse;
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
import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
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
		if(key == ApiKeys.CREATE_PARTITIONS)
			return createPartitions(request);
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
		 Exception exception = null;
		 boolean disconnected = false;
		 try {			 			 
			 result = CreateTopics.createTopics(jdbcConn, topics, topicIdMap, log);
		 } catch(Exception e) {
			 log.error("Unexcepted error occured while creating topics",e);
			 if(e instanceof DisconnectException)
				 disconnected = true;
			 exception = e; 
		 } 
		 return createTopicsResponse(result, exception, disconnected, request, topicIdMap);	 
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
		 boolean disconnected = false;
		 try {
			 cStmt = jdbcConn.prepareCall(query);
			 for(String topic: topics) {
				 try {
					 log.debug("Deleting Topic: {}",topic);
					 cStmt.setString(1, topic);
					 cStmt.setInt(2, 1);
					 cStmt.execute();  
					 log.debug("Topic Deleted: {}",topic);
				 } catch(SQLException sql) {
					 if(ConnectionUtils.isConnectionClosed(jdbcConn))
						 throw new DisconnectException("Database connection got severed while deleting topics",sql);
					 log.trace("Failed to delete topic : {}", topic);
					 result.put(topic, sql);
				 }
				 if(result.get(topic) == null) {
					 topicSet.remove(topic);
					 log.trace("Deleted a topic : {}", topic);
					 result.put(topic, null);
				 }
			 }
			 return deleteTopicsResponse(result, Collections.emptyMap(), null, false, request);
		 } catch(Exception e) {
			 log.error("Unexcepted error occured while deleting topics",e);
			 log.trace("Failed to delete topics : {}", topicSet);
			 if(e instanceof DisconnectException)
				 disconnected = true;
			 return deleteTopicsResponse(result, Collections.emptyMap(), e, disconnected, request);
		 } finally {
				try {
					if(cStmt != null)
						cStmt.close();
				} catch (SQLException e) {
					// do nothing
				}
		 } 
	}
	
	private ClientResponse deleteTopicsById(ClientRequest request) {
		 Node node =(org.oracle.okafka.common.Node) metadataManager.nodeById(Integer.parseInt(request.destination()));
		 DeleteTopicsRequest.Builder builder= (DeleteTopicsRequest.Builder)request.requestBuilder();
		 DeleteTopicsRequest deleteTopicRequest = builder.build();
		 Set<Uuid> topicIds =deleteTopicRequest.topicIds();
		 Connection jdbcConn = connections.get(node);
		 Map<Uuid, SQLException> result = new HashMap<>();
		 Set<Uuid> topicIdSet = new HashSet<>(topicIds);
		 boolean disconnected = false;
		 
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
					 log.debug("Deleting Topic with id: {}",id);
					 topic = ConnectionUtils.getTopicById(jdbcConn,id);
					 cStmt.setInt(1, (int)id.getLeastSignificantBits());
					 cStmt.setInt(2, 1);
					 cStmt.execute();
					 log.debug("Deleted Topic with id: {}",id);
				 } catch(SQLException sql) {
					 if(ConnectionUtils.isConnectionClosed(jdbcConn))
						 throw new DisconnectException("Database connection got severed while deleting topics", sql);
					 log.trace("Failed to delete topic with id : {}", id);
					 result.put(id, sql);
				 }
				 if(result.get(id) == null) {
					 topicIdSet.remove(id);
					 log.trace("Deleted a topic with id : {}", id);
					 result.put(id, null);
				 }
				 
			 }
			 return deleteTopicsResponse(Collections.emptyMap(),result, null, false, request); 
		 } catch(Exception e) {
			 log.error("Unexcepted error occured while deleting topics",e);
			 log.trace("Failed to delete topics with Ids : {}", topicIdSet);
			 if(e instanceof DisconnectException)
				 disconnected = true;
			 return deleteTopicsResponse(Collections.emptyMap(), result, e, disconnected, request);
			} finally {
				try {
					if (cStmt != null)
						cStmt.close();
				} catch (SQLException e) {
					// do nothing
				}
			} 
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
		ClientResponse response = getMetadataNow(request, connections.get(node), node, forceMetadata);
		if(response.wasDisconnected()) { 
			forceMetadata = true;
		}
		return response;
	}
	
	private ClientResponse listOffsets(ClientRequest request) {
		Node node =(org.oracle.okafka.common.Node) metadataManager.nodeById(Integer.parseInt(request.destination()));
		ClientResponse response = getOffsetsResponse(request,connections.get(node));		
		return response;
	}
	
	private ClientResponse getConsumerGroups(ClientRequest request) {
		Node node = (org.oracle.okafka.common.Node) metadataManager.nodeById(Integer.parseInt(request.destination()));
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
		} catch (SQLException sqlE) {
			exception = sqlE;
			log.error("Exception caught while fetching Consumer Groups", sqlE);
			if (ConnectionUtils.isConnectionClosed(jdbcConn)) {
				disconnected = true;
				exception = new DisconnectException(sqlE.getMessage(), sqlE);
			}
			log.trace("Failed to fetch Consumer Groups");
		} finally {
			try {
				if (cStmt != null)
					cStmt.close();
			} catch (SQLException e) {
				// do nothing
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
							else throw sqlE;
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
						log.error("Exception caught while fetching committed offsets for topic: {}, partition: {}", tp.topic(), tp.partition() , sqlE);
						if (ConnectionUtils.isConnectionClosed(jdbcConn)) {
							disconnected = true;
							throw new DisconnectException(sqlE.getMessage(),sqlE);
						} else
							offsetFetchResponseMap.put(tp, new PartitionOffsetData(-1L, sqlE));

					}
				}
				responseMap.put(groupId, offsetFetchResponseMap);
			}
		} catch (Exception e) {
				exception = e;
				if(e instanceof SQLException) {
					log.error("Exception caught while fetching committed offsets", e);
					if (ConnectionUtils.isConnectionClosed(jdbcConn)) {
						disconnected = true;
						exception = new DisconnectException(e.getMessage(), e);
					} 
				}
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
				log.error("Not all privileges granted to the database user.", sqlE.getMessage());
				log.info("Please grant all the documented privileges to database user.");
				if (sqlE instanceof SQLSyntaxErrorException) {
					log.trace("Please grant all the documented privileges to database user.");
				}
			}

			log.error("Exception occured while deleting consumer groups", sqlE);
			if (ConnectionUtils.isConnectionClosed(jdbcConn)) {
				disconnected = true;
				exception = new DisconnectException(sqlE.getMessage(), sqlE);
			}
		} 
		DeleteGroupsResponse deleteGroupsResponse = new DeleteGroupsResponse(errors);
		deleteGroupsResponse.setException(exception);
		return new ClientResponse(request.makeHeader((short) 1), request.callback(), request.destination(),
				request.createdTimeMs(), System.currentTimeMillis(), disconnected, null, null, deleteGroupsResponse);
		
	}
	
	private ClientResponse createPartitions(ClientRequest request) {
		CreatePartitionsRequest.Builder builder = (CreatePartitionsRequest.Builder) request.requestBuilder();
		CreatePartitionsRequest createPartitionsRequest = builder.build();
		Map<String, NewPartitions> partitionsNewCount = createPartitionsRequest.partitionNewCounts();
		Map<String, Exception> errors = new HashMap<>();

		Node node = (org.oracle.okafka.common.Node) metadataManager.nodeById(Integer.parseInt(request.destination()));
		
		Connection jdbcConn = connections.get(node);
		Exception exception = null;
		boolean disconnected = false;
		
		String plsql = 
			    "BEGIN " +
			    "  DBMS_AQADM.ADD_EVENTSTREAM( ?, ? ); " +
			    "END;";
		CallableStatement cStmt = null;
		try {
			for (Map.Entry<String, NewPartitions> entry : partitionsNewCount.entrySet()) {
				String topic = entry.getKey();
				int newPartitionCount = entry.getValue().totalCount();
				int currentPartitionCount;
				try {
					currentPartitionCount = getQueueParameter(SHARDNUM_PARAM, topic, jdbcConn);

					if (currentPartitionCount < newPartitionCount) {
						cStmt = jdbcConn.prepareCall(plsql);
						cStmt.setString(1, topic);
						cStmt.setInt(2, newPartitionCount - currentPartitionCount);
						log.debug("Adding {} more partitions to the topic {}.", newPartitionCount - currentPartitionCount, topic);
						cStmt.execute();
						log.debug("Successfully added {} partitions to the topic {}.", newPartitionCount - currentPartitionCount, topic);
						errors.put(topic, null);
					} else if (currentPartitionCount > newPartitionCount)
						errors.put(topic, new InvalidPartitionsException(String.format("Topic currently has %d partitions, which is higher than the requested %d.",
										currentPartitionCount, newPartitionCount)));
					else
						errors.put(topic, new InvalidPartitionsException(String.format("Topic already has %d partitions.", currentPartitionCount)));
				} catch (SQLException sqlE) {
					int errorNo = sqlE.getErrorCode();
					if (errorNo == 24010) 
						errors.put(topic, new UnknownTopicOrPartitionException("Topic doesn't exist",sqlE));
					else
						throw sqlE;
				}
			}

		} catch (SQLException sqlE) {
			log.error("Unexpected error occured while creating Partitions", sqlE);
			exception = sqlE;
			int errorNo = sqlE.getErrorCode();
			if (errorNo == 6550)
				log.error("Please grant all the documented privileges to the database user.", sqlE.getMessage());
			if (ConnectionUtils.isConnectionClosed(jdbcConn)) {
				disconnected = true;
				exception = new DisconnectException("Database Connection got severed while creating Partitions.", sqlE);
			}
		} finally {
			try {
				if (cStmt != null)
					cStmt.close();
			} catch (SQLException sqlE) {
				// do nothing
			}
		}
		CreatePartitionsResponse createPartitionsResponse = new CreatePartitionsResponse(errors);
		createPartitionsResponse.setException(exception);
		return new ClientResponse(request.makeHeader((short) 1), request.callback(), request.destination(),
				request.createdTimeMs(), System.currentTimeMillis(), disconnected, null, null, createPartitionsResponse); 
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

			ConnectionUtils.updateNodeInfo(node, newConn);

			/*
			 * Fetching the nodes and updating the metadataManager to ensure that Cluster
			 * have the correct mapping in the nodesById Map even when the bootstrap node
			 * have been updated after the initial connection
			 */
			if(node.isBootstrap()){
				List<Node> nodes = new ArrayList<>();
				String clusterId = ((oracle.jdbc.internal.OracleConnection) newConn).getServerSessionInfo()
						.getProperty("DATABASE_NAME");
				this.getNodes(nodes, newConn, node, forceMetadata);
				for(Node n: nodes) {
					n.setBootstrapFlag(false);
				}
				
				Cluster newCluster = new Cluster(clusterId, NetworkClient.convertToKafkaNodes(nodes),
						Collections.emptySet(), Collections.emptySet(), Collections.emptySet(),
						nodes.size() > 0 ? nodes.get(0) : null);// , configs);
				this.metadataManager.update(newCluster, System.currentTimeMillis());
			}
			connections.put(node, newConn);

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
				log.trace("Failed to close connection with node {}", node);
			}
			
		}
	}
}
