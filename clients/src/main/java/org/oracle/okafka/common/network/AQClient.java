/*
** OKafka Java Client version 23.4.
**
** Copyright (c) 2019, 2024 Oracle and/or its affiliates.
** Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.
*/

package org.oracle.okafka.common.network;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Statement;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.oracle.okafka.clients.CommonClientConfigs;
import org.oracle.okafka.clients.TopicTeqParameters;
import org.oracle.okafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.config.AbstractConfig;
import org.oracle.okafka.common.requests.MetadataRequest;
import org.oracle.okafka.common.requests.MetadataRequest.Builder;
import org.oracle.okafka.common.requests.MetadataResponse;
import org.oracle.okafka.common.requests.CreateTopicsRequest.TopicDetails;
import org.oracle.okafka.common.utils.ConnectionUtils;
import org.oracle.okafka.common.utils.CreateTopics;
import org.slf4j.Logger;
import java.sql.Timestamp;
import java.sql.Date;

import javax.jms.JMSException;
import oracle.jdbc.OracleTypes;
import oracle.jdbc.driver.OracleConnection;
import oracle.jms.AQjmsSession;

/*
 *  Abstract class to communicate with Oracle Database. 
 *  This is extended by AQKafkaProducer, AQKafkaConsumer and AQKafkaAdmin classes.
 *  getMetadataNow is the only implemented method which remains common for all the implemented class.
 *  
 *  All class extending this abstract class must implement 
 *  1. ClientResponse send(ClientRequest request) method.
 *     Each implementing class has its own meaningful operations to perform against Oracle Database.
 *     AQKafkaProducer has to produce the records and fetch metadata.
 *     AQKafkaConsumer has to subscribe to topics, Get involved in Rebalancing activity, Consume the records and commit the offsets.
 *     AQKafkaAdmin has to create or drop the topic.
 *     
 *  2. void connect(Node node)     
 *     Connects to the database node. 
 *     For AQKafkaProducer and AQKafkaConsumer it also creates a JMS Session internally as well.
 *     For AQKafkaAdmin it only creates database connection to this node and maintains in a hashtable.
 *     
 *  3. void close(Node node)
 *     Closes Database connection to this node. 
 *     If a JMS Session is also created then that will also be closed.
 *      
 *  4. void close();
 *     Closes all connection to all the database nodes      
 *     
 *  5. boolean isChannelReady(Node node)   
 *     Checks if a connection is already created for this node or not 
 */
public abstract class AQClient {
	
	protected final Logger log ;
	private final AbstractConfig configs;
	
	
	private Map<Integer, Timestamp> instancesTostarttime;
	public List<Node> all_nodes = new ArrayList<>();
	public List<PartitionInfo> partitionInfoList = new ArrayList<>();
	private  int userQueueShardsQueryIndex = 0;
	
	public static final String PARTITION_PROPERTY = "AQINTERNAL_PARTITION";
	public static final String HEADERCOUNT_PROPERTY = "AQINTERNAL_HEADERCOUNT";
	public static final String MESSAGE_VERSION = "AQINTERNAL_MESSAGEVERSION";
	public static final String STICKYDEQ_PARAM = "STICKY_DEQUEUE";
	public static final String KEYBASEDENQ_PARAM = "KEY_BASED_ENQUEUE";
	public static final String SHARDNUM_PARAM = "SHARD_NUM";
	
	public AQClient(Logger log, AbstractConfig configs) {
		this.log = log;
		this.configs = configs;
	}
	/*
	 *  Each implementing class has its own meaningful operations to perform against Oracle Database.
	 *     AQKafkaProducer has to produce the records and fetch metadata.
	 *     AQKafkaConsumer has to subscribe to topics, Get involved in Rebalancing activity, Consume the records and commit the offsets.
	 *     AQKafkaAdmin has to create or drop the topic.
	 */
	public abstract ClientResponse send(ClientRequest request);
	
	/*
	 *  Connects to the database node. 
	 *  For AQKafkaProducer and AQKafkaConsumer it also creates a JMS Session internally as well.
 	 *  For AQKafkaAdmin it only creates database connection to this node and maintains in a hashtable.
	 */
	public abstract void connect(Node node) throws JMSException;
	
	/*  Checks if a connection is already created for this node or not */
	public abstract boolean isChannelReady(Node node);
	
	/* Checks if a connection is already created for this node or not */
	public abstract void close(Node node);
	
	/* Closes all connection to all the database nodes */
	public abstract void close();
	
	
	/* Get Metadata from Oracle Database.
	 * This involves fetching information for all the available database instances.
	 * Fetching partition count for the interested topics. 
	 * Fetching information as to which topic-partition is owned at what database instance.
	 * */
	public ClientResponse getMetadataNow(ClientRequest request, Connection con, Node currentNode, boolean metadataRequested) {
		
		log.debug("AQClient: Getting Metadata now");
		
		MetadataRequest.Builder builder= (MetadataRequest.Builder)request.requestBuilder();
		MetadataRequest metadataRequest = builder.build();	
		List<Node> nodes = new ArrayList<>();
		List<PartitionInfo> partitionInfo = new ArrayList<>();
		Map<String, Exception> errorsPerTopic = new HashMap<>();
		List<String> metadataTopics = new ArrayList<String>(metadataRequest.topics());
		boolean disconnected = false;
		String clusterId = "";
		boolean getPartitioninfo = false;
		Map<String , TopicTeqParameters> topiParameterMap = null;
		try {
			if(con == null)
			{
				disconnected = true;
				throw new NullPointerException("Database connection to fetch metadata is null");
			}
			//Database Name to be set as Cluster ID
			clusterId = ((oracle.jdbc.internal.OracleConnection)con).getServerSessionInfo().getProperty("DATABASE_NAME");
		
			getPartitioninfo = getNodes(nodes, con, currentNode, metadataRequested); 
			
            if(getPartitioninfo || metadataRequested) 
            {		 			
				getPartitionInfo(metadataRequest.topics(), metadataTopics, con,
						nodes, metadataRequest.allowAutoTopicCreation(), partitionInfo, errorsPerTopic);
            }
            
            List<String> teqParaList = metadataRequest.teqParaTopics();
            topiParameterMap = new HashMap<String, TopicTeqParameters>(teqParaList.size());
            for(String teqTopic : teqParaList)
            {
            	TopicTeqParameters teqPara =  fetchQueueParameters(teqTopic, con);
            	topiParameterMap.put(teqTopic, teqPara);
            }
            
    		            
		} catch(Exception exception) {
			log.error("Exception while getting metadata "+ exception.getMessage(), exception );
			//exception.printStackTrace();

			if(exception instanceof SQLException) 
				if(((SQLException)exception).getErrorCode() == 6550) {
					log.error("Not all privileges granted to the database user.", ((SQLException)exception).getMessage());
					log.info("Please grant all the documented privileges to database user.");
				}
			if(exception instanceof SQLSyntaxErrorException)
				log.trace("Please grant all the documented privileges to database user.");
			for(String topic : metadataTopics) {
				errorsPerTopic.put(topic, exception);
			}
			disconnected = true;			
			try {
				log.debug("Unexcepted error occured with connection to node {}, closing the connection", request.destination());
				if(con != null)
					con.close();
				
				log.trace("Connection with node {} is closed", request.destination());
			} catch(SQLException sqlEx) {
				log.trace("Failed to close connection with node {}", request.destination());
			}
       }
		return  new ClientResponse(request.makeHeader((short)1),
				request.callback(), request.destination(), request.createdTimeMs(),
				System.currentTimeMillis(), disconnected, null,null, new MetadataResponse(clusterId, all_nodes, partitionInfoList, errorsPerTopic, topiParameterMap));
	}

	// Fetches existing cluster nodes 
	// Returns TRUE if new node is added, existing node went down, or if the startup time changed for the nodes
	// otherwise return false
	private boolean getNodes(List<Node> nodes, Connection con, Node connectedNode, boolean metadataRequested) throws SQLException {
		Statement stmt = null;
		ResultSet result = null;
		String user = "";
		boolean furtherMetadata = false;
		boolean onlyOneNode = false;

		try {
			user = con.getMetaData().getUserName();
			stmt = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			String query = "select inst_id, instance_name, startup_time  from gv$instance";
			result = stmt.executeQuery(query);
			Map<Integer, String> instance_names = new HashMap<>();
			Map<Integer, Timestamp> instance_startTimes = new HashMap<>();

			while(result.next()) {
				int instId = result.getInt(1);
				String instName = result.getString(2);
				instance_names.put(instId, instName);
				Date startup_time = result.getDate(3);
				Timestamp ts=new Timestamp(startup_time.getTime());
				instance_startTimes.put(instId, ts);
			}
			result.close();
			result = null;

			if (instance_names.size()==1)
			{
				// Connected Node is :
				// Node connectedNode = getNodeToThisConnection(con);
				// Only one RAC node is up and we are connected to it.
				if(connectedNode == null)
				{
					if(all_nodes == null || all_nodes.size() == 0)
					{
						furtherMetadata = true;
						onlyOneNode = false;
					}
				}
				else {
					nodes.add(connectedNode);
					all_nodes = nodes;
					onlyOneNode = true;
				}
			}

			if(!instance_startTimes.equals(instancesTostarttime)) {
				instancesTostarttime = instance_startTimes;
				furtherMetadata = true;
			}

			if(onlyOneNode) {
				return furtherMetadata;
			}

			if (furtherMetadata || metadataRequested) {

				query = "select inst_id, TYPE, value from gv$listener_network order by inst_id";
				result = stmt.executeQuery(query);
				Map<Integer, ArrayList<String>> services = new HashMap<>();
				Map<Integer,ArrayList<String>> localListenersMap = new HashMap<>();

				String security = configs.getString(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG);
				String preferredService = configs.getString(CommonClientConfigs.ORACLE_SERVICE_NAME);
				if(preferredService == null)
				{
					if(con != null) 
					{
						preferredService = ConnectionUtils.getConnectedService(con);
					}
				}

				boolean plainText = security.equalsIgnoreCase("PLAINTEXT")?true:false;

				while(result.next()) {
					int instId = result.getInt(1);
					String type = result.getString(2);
					String value = result.getString(3);

					if(type.equalsIgnoreCase("SERVICE NAME")) {
						ArrayList<String> servicesList = services.get(instId);
						if(servicesList == null)
						{
							servicesList = new ArrayList<String>();
							services.put(instId,servicesList);
						}
						if(preferredService != null && value.equalsIgnoreCase(preferredService))
						{
							log.debug("Found Preferred Services " + value);
							servicesList.add(0, value);
						}
						else {
							servicesList.add(value);
						}
					}
					else if(type.equalsIgnoreCase("LOCAL LISTENER"))
					{
						ArrayList<String> localListenerList =  localListenersMap.get(instId);
						if(localListenerList == null)
						{
							localListenerList = new ArrayList<String>();
							localListenersMap.put(instId, localListenerList);
						}
						localListenerList.add(value);
					}
				} //Result set Parsed
				result.close();
				result = null;

				for(Integer instIdNow : instance_names.keySet())
				{
					/*if( instIdNow.intValue() == connectedInst)
					continue; */

					log.debug("Processing metadata for instance: " + instIdNow);

					ArrayList<String> localListenerList = localListenersMap.get(instIdNow);
					
					if(localListenerList == null)
					{
						if(con != null)
						{
							//String hostNPort = ConnectionUtils.getConnectedHost(con);
							String hostNPort = ConnectionUtils.getConnectedHostnPort(con);
							localListenerList = new ArrayList<String>();
							localListenerList.add(hostNPort);
						}
					}
					if(localListenerList != null)
					{
						for(String localListenerNow : localListenerList)
						{
							log.debug("Processing Local Listener " + localListenerNow);
							String str = localListenerNow;
							//AdHoc processing of LISTENER STRING 
							StringBuilder sb = new StringBuilder();

							for(int ind = 0;ind < str.length(); ind++)
								if(str.charAt(ind) != ' ')
									sb.append(str.charAt(ind));

							str = sb.toString();
							String protocolNow = getProperty(str,"PROTOCOL");
							log.debug("Protocol used by this local listener " + protocolNow);

							if( (plainText && protocolNow.equalsIgnoreCase("TCP")) || 
									(!plainText && protocolNow.equalsIgnoreCase("TCPS")))
							{
								String host = getProperty(str, "HOST");;
								Integer port = Integer.parseInt(getProperty(str, "PORT"));
								log.debug("Hot:PORT " + host +":"+port);

								// ToDo: Assign Service List instead of a single Service
								Node newNode =new Node(instIdNow, host, port, services.get(instIdNow).get(0), instance_names.get(instIdNow));
								newNode.setUser(user);
								log.debug("New Node created: " + newNode);
								newNode.updateHashCode();
								nodes.add(newNode);
								all_nodes = nodes;
							}
						}
						log.debug("Exploring hosts of the cluster. #Nodes " + nodes.size());
						for(Node nodeNow : nodes)
						{	
							log.debug("DB Instance: " + nodeNow);
						}
					}
					else {
						if(connectedNode != null) {
							nodes.add(connectedNode);
							all_nodes = nodes;
							onlyOneNode = true;
						}
					}
				}
			}
		}
		catch(Exception e)
		{
			log.error("Exception while updating metadata " ,e);
		} finally {
			try {
				if(result != null)
					result.close();

				if(stmt != null)
				stmt.close();
			} catch(SQLException sqlEx) {
				//do nothing
			}
		}

		return furtherMetadata;
	}
	
	private Node getNodeToThisConnection(Connection con)
	{
		Node node = null;
		try {
			String url = con.getMetaData().getURL();
			oracle.jdbc.internal.OracleConnection oracleInternalConn = (oracle.jdbc.internal.OracleConnection)con;
			String instanceName = oracleInternalConn.getServerSessionInfo().getProperty("INSTANCE_NAME");
			int instanceNum = Integer.parseInt(oracleInternalConn.getServerSessionInfo().getProperty("AUTH_INSTANCE_NO"));
			String dbServiceName = oracleInternalConn.getServerSessionInfo().getProperty("SERVICE_NAME");
			String userName = con.getMetaData().getUserName();


			String dbHost = null;

			try
			{
				final String hostStr = "(HOST=";
				int sIndex = url.indexOf(hostStr);
				int eIndex = url.indexOf(")", sIndex);
				dbHost = (url.substring(sIndex+(hostStr.length()), eIndex));
			}catch(Exception e)
			{
				dbHost = oracleInternalConn.getServerSessionInfo().getProperty("AUTH_SC_SERVER_HOST");
				String dbDomain = oracleInternalConn.getServerSessionInfo().getProperty("AUTH_SC_DB_DOMAIN");
				dbHost = dbHost +"."+dbDomain;
			}

			log.debug("DB HOST To This Connection " + dbHost);
			String dbPort = null;
			try {
				final String portStr = "(PORT=";
				int sIndex = url.indexOf(portStr);
				int eIndex = url.indexOf(")", sIndex);
				dbPort = (url.substring(sIndex+(portStr.length()), eIndex));

			}catch(Exception ignoreE)
			{}

			if(dbPort == null)
			{
				List<String> bootStrapServers = this.configs.getList(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
				for(String bootServer : bootStrapServers)
				{
					StringTokenizer stn = new StringTokenizer(bootServer,":");

					String dbHostOrigin = stn.nextToken();
					// This DB Port may be of different database host. 
					dbPort = stn.nextToken();

					//Exit if DB Host Name matches, continue otherwise. If DB Host is not in bootstrap, 
					//then we will assume that all DB RAC nodes have same listener port
					if(dbHostOrigin.equalsIgnoreCase(dbHost))
					{
						break;
					}
				}
			}

			node = new Node(instanceNum, dbHost,Integer.parseInt(dbPort), dbServiceName, instanceName);
			node.setUser(userName);
			node.updateHashCode();
			log.info("Connection was setup to node " + node);
		}
		catch(Exception e)
		{
			log.error("Exception while creating node from JDBC Connection", e );
		}
		return node;
	}


	private void getPartitionInfo(List<String> topics, List<String> topicsRem, Connection con,
			List<Node> nodes, boolean allowAutoTopicCreation, 
			List<PartitionInfo> partitionInfo, Map<String, Exception> errorsPerTopic) throws Exception {
		
		if(nodes.size() <= 0 || topics == null || topics.isEmpty())
			return;

		String queryQShard[] = {"select SHARD_ID, OWNER_INSTANCE from user_queue_shards where  QUEUE_ID = (select qid from user_queues where name = upper(?)) ",
		"select SHARD_ID, ENQUEUE_INSTANCE from user_queue_shards where  QUEUE_ID = (select qid from user_queues where name = upper(?)) "};
		
		PreparedStatement stmt1 = null;
		int qryIndex=userQueueShardsQueryIndex;
		do {
			try {
				stmt1 = con.prepareStatement(queryQShard[qryIndex]);
				int nodeIndex = 0 ;
				int nodesSize = nodes.size();
				ResultSet result1 = null;
				Node[] nodesArray = null;
				if(nodesSize > 1) {
					int max = -1;
					for(Node nodeNew : nodes)  {
						if(nodeNew.id() > max)
							max = nodeNew.id();
					}

					nodesArray = new Node[max];
					for(Node nodeNew : nodes) 
						nodesArray[nodeNew.id()-1] = nodeNew;
				}

				for(String topic : topics) {
					boolean topicDone = false;
					int partCnt = 0;
					try {
						//Get number of partitions
						partCnt = getQueueParameter(SHARDNUM_PARAM, ConnectionUtils.enquote(topic), con);	
					} catch(SQLException sqlE) {
						int errorNo = sqlE.getErrorCode();
						if(errorNo == 24010)  {
							//Topic does not exist, it will be created
							continue;
						}
					}catch(Exception excp) {
						// Topic May or may not exists. We will not attempt to create it again
						topicsRem.remove(topic);
						continue;
					}

					boolean partArr[] =  new boolean[partCnt];
					for(int i =0; i < partCnt ;i++)
						partArr[i] = false;

					// If more than one RAC node then check who is owner Node for which partition
					if(nodes.size()  > 1) {

						stmt1.clearParameters();
						stmt1.setString(1, topic);
						result1 = stmt1.executeQuery(); 
						// If any row exist 
						if(result1.isBeforeFirst()) {
							while(result1.next() ) {
								int partNum = result1.getInt(1)/2;
								int nodeNum = result1.getInt(2);
								partitionInfo.add(new PartitionInfo(topic, partNum , nodesArray[nodeNum-1], new Node[0], new Node[0]));	
								partArr[partNum] = true;
							}

							result1.close();
							// For the partitions not yet mapped to an instance 
							for(int i = 0; i < partCnt ; i++) {
								if( partArr[i] == false ) {
									partitionInfo.add(new PartitionInfo(topic, i , nodes.get(nodeIndex++%nodesSize), null, null));	
								}
							}
							topicDone = true;
						} // Entry Existed in USER_QUEUE_SHARD
					}// Nodes > 1

					// No Record in USER_QUEUE_SHARD or Node =1 check if topic exist		   	
					if(!topicDone){
						for(int i = 0; i < partCnt ; i++) {
							//When nodeSize > 1 but the partition is not yet created, then we distribute this partition across 
							// available nodes by assigning the partition to node in round robin manner.
							partitionInfo.add(new PartitionInfo(topic, i , nodes.get(nodeIndex++%nodesSize), null, null));
						}
						topicDone =true;
					}
					if(topicDone)
						topicsRem.remove(topic);
				} // For all Topics

				if(allowAutoTopicCreation && topicsRem.size() > 0) {
					Map<String, TopicDetails> topicDetails = new HashMap<String, TopicDetails>();
					for(String topicRem : topicsRem) {
						topicDetails.put(topicRem, new TopicDetails(1, (short)0 , Collections.<String, String>emptyMap()));
					}
					Map<String, Exception> errors= CreateTopics.createTopics(con, topicDetails);
					for(String topicRem : topicsRem) {
						if(errors.get(topicRem) == null) {
							partitionInfo.add(new PartitionInfo(topicRem, 0, nodes.get(nodeIndex++%nodesSize), null, null));
						} else {
							errorsPerTopic.put(topicRem, errors.get(topicRem));
						}
					}
				}
				partitionInfoList = partitionInfo;
				break;
			} 
			catch(SQLException sqe){
				if(sqe.getErrorCode() == 904) {
					qryIndex++;
					userQueueShardsQueryIndex = qryIndex;
					continue;
				}
			}
			finally {
			
				try {
					if(stmt1 != null) 
						stmt1.close();		
				} catch(Exception ex) {
					//do nothing
				}
				System.out.println(userQueueShardsQueryIndex);
			}
		} 
		while(qryIndex<2);
	}
	
	// returns the value for a queue Parameter
    public int getQueueParameter(String queueParamName, String topic, Connection con) throws SQLException {
		if(topic == null) return 0;
		String query = "begin dbms_aqadm.get_queue_parameter(?,?,?); end;";
		CallableStatement cStmt = null;
		int para= 1;

		try {
			cStmt = con.prepareCall(query);
			cStmt.setString(1, topic);
			cStmt.setString(2, queueParamName);
			cStmt.registerOutParameter(3, OracleTypes.NUMBER);
			cStmt.execute();
			para = cStmt.getInt(3);
		} 
		finally {
			if(cStmt != null)
				cStmt.close();
		}		   
		return para;
	}  
    
    // Fetches all the queue parameters for a topic from the TEQ server,
    // and maintains metadata(all queue parameter values) for that topic.
	public void fetchQueueParameters(String topic, Connection conn, HashMap<String,TopicTeqParameters> topicParaMap) throws SQLException {
		if(topic == null) return ;
		if(!topicParaMap.containsKey(topic)) {
			TopicTeqParameters topicTeqParam = fetchQueueParameters(topic, conn);
	        topicParaMap.put(topic, topicTeqParam);
		}
	} 
	
	public TopicTeqParameters fetchQueueParameters(String topic, Connection conn) throws SQLException {
		if(topic == null) return  null ;
		
		TopicTeqParameters topicTeqParam = new TopicTeqParameters();
		topicTeqParam.setKeyBased(getQueueParameter(KEYBASEDENQ_PARAM, topic, conn));
		topicTeqParam.setStickyDeq(getQueueParameter(STICKYDEQ_PARAM, topic, conn));
		topicTeqParam.setShardNum(getQueueParameter(SHARDNUM_PARAM, topic, conn));
		
		return topicTeqParam;
	} 

	public static String getProperty(String str, String property) {
		String tmp = str.toUpperCase();
		int index = tmp.indexOf(property.toUpperCase());
		if(index == -1)
			return null;
		int index1 = tmp.indexOf("=", index);
		if(index1 == -1)
			return null;
		int index2 = tmp.indexOf(")", index1);
		if(index2 == -1)
			return null;
		return str.substring(index1 + 1, index2).trim();
	}
	
}
