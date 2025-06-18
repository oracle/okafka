/*
 ** OKafka Java Client version 23.4.
 **
 ** Copyright (c) 2019, 2024 Oracle and/or its affiliates.
 ** Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.
 */

package org.oracle.okafka.clients.consumer.internals;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;

import oracle.jdbc.OracleData;
import oracle.jdbc.OracleTypes;
import oracle.jdbc.OracleArray;
import oracle.jms.AQjmsBytesMessage;
import oracle.jms.AQjmsConnection;
import oracle.jms.AQjmsConsumer;
import oracle.jms.AQjmsSession;

import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.oracle.okafka.clients.CommonClientConfigs;
import org.oracle.okafka.clients.Metadata;
import org.oracle.okafka.clients.NetworkClient;
import org.oracle.okafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.oracle.okafka.clients.consumer.TxEQAssignor;
import org.oracle.okafka.common.Node;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;

import org.oracle.okafka.common.internals.PartitionData;
import org.oracle.okafka.common.internals.QPATInfo;
import org.oracle.okafka.common.internals.QPATInfoList;
import org.oracle.okafka.common.internals.QPIMInfo;
import org.oracle.okafka.common.internals.QPIMInfoList;
import org.oracle.okafka.common.internals.SessionData;
import org.oracle.okafka.common.network.AQClient;
import org.oracle.okafka.common.network.SelectorMetrics;
import org.oracle.okafka.common.protocol.ApiKeys;
import org.oracle.okafka.common.requests.CommitRequest;
import org.oracle.okafka.common.requests.CommitResponse;
import org.oracle.okafka.common.requests.ConnectMeRequest;
import org.oracle.okafka.common.requests.ConnectMeResponse;
import org.oracle.okafka.common.requests.FetchRequest;
import org.oracle.okafka.common.requests.FetchResponse;
import org.oracle.okafka.common.requests.JoinGroupRequest;
import org.oracle.okafka.common.requests.JoinGroupResponse;
import org.oracle.okafka.common.requests.MetadataResponse;
import org.oracle.okafka.common.requests.OffsetFetchRequest;
import org.oracle.okafka.common.requests.OffsetFetchResponse;
import org.oracle.okafka.common.requests.OffsetFetchResponse.PartitionOffsetData;
import org.oracle.okafka.common.requests.OffsetResetRequest;
import org.oracle.okafka.common.requests.OffsetResetResponse;
import org.oracle.okafka.common.requests.SubscribeRequest;
import org.oracle.okafka.common.requests.SubscribeResponse;
import org.oracle.okafka.common.requests.SyncGroupRequest;
import org.oracle.okafka.common.requests.SyncGroupResponse;
import org.oracle.okafka.common.requests.UnsubscribeResponse;
import org.oracle.okafka.common.utils.ConnectionUtils;
import org.oracle.okafka.common.utils.FetchOffsets;
import org.apache.kafka.common.utils.LogContext;
import org.oracle.okafka.common.utils.MessageIdConverter;
import org.oracle.okafka.common.utils.MessageIdConverter.OKafkaOffset;
import org.apache.kafka.common.utils.Time;
import oracle.jdbc.OracleConnection;

/**
 * This class consumes messages from AQ 
 */
public final class AQKafkaConsumer extends AQClient{
	//private final Logger log ;
	//Holds TopicPublishers of each node. Each TopicPublisher can contain a connection to corresponding node, session associated with that connection and topic publishers associated with that session
	private final Map<Node, TopicConsumers> topicConsumersMap;
	private final ConsumerConfig configs;
	private final Time time;
	private  String msgIdFormat = "00";
	private List<ConsumerPartitionAssignor> assignors;
	private final SelectorMetrics selectorMetrics;
	private Metadata metadata;

	private boolean skipConnectMe = false;
	private boolean externalConn = false;
	
	private static final String LTWT_COMMIT_SYNC = "{call dbms_teqk.AQ$_COMMITSYNC(?, ?, ?, ?, ?, ?, ?)}";
	private static final String LTWT_COMMIT_SYNC_ALL = "{call dbms_teqk.AQ$_COMMITSYNC_ALL(?, ?, ?, ?, ?, ?, ?)}";
	private static final String LTWT_SEEK = "{call dbms_teqk.AQ$_SEEK(?, ?, ?, ?, ?, ?, ?)}";
	private static final String LTWT_SEEK_TO_BEGINNING = "{call dbms_teqk.AQ$_SEEKTOBEGINNING(?, ?, ?, ?, ?)}";
	private static final String LTWT_SEEK_TO_END = "{call dbms_teqk.AQ$_SEEKTOEND(?, ?, ?, ?, ?)}";
	private static final String LTWT_SUB = "{call sys.dbms_aqadm.add_ltwt_subscriber(?, sys.aq$_agent(?,null,null))}";
	
	private final Map<Node, Map<String, CallableStatement>> callableCacheMap = new ConcurrentHashMap<>();

	public AQKafkaConsumer(LogContext logContext, ConsumerConfig configs, Time time, Metadata metadata,Metrics metrics)

	{   
		super(logContext.logger(AQKafkaConsumer.class), configs);
		System.setProperty("oracle.jms.conservativeNavigation","1");
		this.configs = configs;
		this.topicConsumersMap = new HashMap<Node, TopicConsumers>();
		this.time =time;
		this.metadata = metadata;
		this.selectorMetrics = new SelectorMetrics(metrics, "Selector", Collections.<String, String>emptyMap(),true);
		this.selectorMetrics.recordConnectionCount(topicConsumersMap);;

	}

	public void setAssignors(List<ConsumerPartitionAssignor> _assignores )
	{
		assignors = _assignores;
	}
	
	private CallableStatement getOrCreateCallable(Node node, String key, String sql) {
	    Map<String, CallableStatement> nodeMap = callableCacheMap.computeIfAbsent(node, n -> new ConcurrentHashMap<>());
	    
	    CallableStatement stmt = nodeMap.get(key);
	    try {
	        if (stmt == null || stmt.isClosed()) {
	            Connection con = getConnection(node);
	            stmt = con.prepareCall(sql);
	            nodeMap.put(key, stmt);
	        }
	        return stmt;
	    } catch (SQLException | JMSException e) {
	        throw new RuntimeException("Failed to prepare statement for " + key, e);
	    }
	}

	public void closeCallableStmt(Node node) {
		Map<String, CallableStatement> stmts = callableCacheMap.remove(node);
		if (stmts != null) {
			for (CallableStatement stmt : stmts.values()) {
				try { stmt.close(); } catch (Exception e) {}
			}
		}
	}

	private String getCurrentUser(Node node) throws SQLException, JMSException {
		Connection con = getConnection(node);
		return con.getMetaData().getUserName();
	}

	private Connection getConnection(Node node) throws SQLException, JMSException {
		return ((AQjmsSession) topicConsumersMap.get(node).getSession()).getDBConnection();
	}

	public ClientResponse send(ClientRequest request) {
		this.selectorMetrics.requestCompletedSend(request.destination());
		ClientResponse cr = parseRequest(request, ApiKeys.convertToOracleApiKey(request.apiKey()));
		if(cr!=null) {
			this.selectorMetrics.recordCompletedReceive(cr.destination(),cr.requestLatencyMs());
		}
		return cr;
	}
	
	/**
	 * Determines the type of request and calls appropriate method for handling request
	 * @param request request to be sent
	 * @param key uniquely identifies type of request.
	 * @return response for given request
	 */
	private ClientResponse parseRequest( ClientRequest request, ApiKeys key) {
		switch(key)
		{
		case FETCH:
			return receive(request);
		case COMMIT:
			return commit(request);
		case SUBSCRIBE:
			return subscribe(request);
		case UNSUBSCRIBE:
			return unsubscribe(request);
		case OFFSETRESET:
			return seek(request);
		case JOIN_GROUP:
			return joinGroup(request);
		case SYNC_GROUP:
			return syncGroup(request);
		case METADATA:
			return getMetadata(request);
		case CONNECT_ME:
			return connectMe(request);
		case OFFSET_FETCH:
			return fetchOffsets(request);
		}
		return null;
	}

	
	
	/**
	 * Consumes messages in bulk from a given topic . Consumes or wait till either timeout occurs or max.poll.records condition is met
	 * @param node consume by establishing connection to this instance. 
	 * @param topic topic from which messages to be consumed
	 * @param timeoutMs wait for messages until this time expires
	 * @return
	 */
	public ClientResponse receive(ClientRequest request) {
		Node node =metadata.getNodeById(Integer.parseInt(request.destination()));
		FetchRequest.Builder builder = (FetchRequest.Builder)request.requestBuilder();
		FetchRequest fetchRequest = builder.build();
		String topic = fetchRequest.topic();
		long timeoutMs = fetchRequest.pollTimeout();
		boolean disconnected = false;
		try {
			if(!topicConsumersMap.containsKey(node) ) {
				topicConsumersMap.put(node, new TopicConsumers(node));
			}
			Message[] messages = null;
			TopicConsumers consumers = topicConsumersMap.get(node);
			TopicSubscriber subscriber = consumers.getTopicSubscriber(topic);
			log.debug("Invoking bulkReceive");
			int maxRecords = configs.getInt(ConsumerConfig.MAX_POLL_RECORDS_CONFIG);
			if(maxRecords == 1)
			{
				Message msg = subscriber.receive(timeoutMs);
				if(msg != null)
				{
					messages = new Message[1];
					messages[0] = msg;
				}
			}
			else 
			{
				messages = ((AQjmsConsumer)subscriber).bulkReceive(configs.getInt(ConsumerConfig.MAX_POLL_RECORDS_CONFIG), timeoutMs);
			}
			log.debug("After bulkreceive. #Message = " + (messages!=null?messages.length:"0") );
			if(messages == null) 
				return createFetchResponse(request, topic, Collections.emptyList(), false, null);
			List<AQjmsBytesMessage> msgs = new ArrayList<>();
			for(int i=0; i < messages.length;i++) {
				if(messages[i] instanceof AQjmsBytesMessage)
				{
					msgs.add((AQjmsBytesMessage)messages[i]);
				}
				else {
					log.debug("Received other than AQjmsBytesMessage");
					try {
						int partition = messages[i].getIntProperty(AQClient.PARTITION_PROPERTY);
						OKafkaOffset  okOffset = 	MessageIdConverter.getOKafkaOffset(messages[i].getJMSMessageID(), true, true);
						long offset = okOffset.getOffset();
						log.error("Message is not an instance of AQjmsBytesMessage: Topic {} partition {} offset{}",topic, partition, offset );
					} catch(Exception exception) {
						//do nothing
					}
				}
			}

			return createFetchResponse(request, topic, msgs, false, null);
		} catch(JMSException exception) { 
			log.debug("Exception in bulkReceive " + exception.getMessage(),exception);
			int errorCode = 0;
			Throwable cause = exception;
			SQLException mainCause = ConnectionUtils.getSQLException(cause);
			if(mainCause!=null){
				errorCode = mainCause.getErrorCode();
			}
			else{
				errorCode = Integer.parseInt(exception.getErrorCode());
			}

			log.debug("Dequeue Error Code = " + errorCode);
			//If not Rebalancing Error and not Transient error then 
			if(!(errorCode == 24003 ||	errorCode == 120)) {
				log.warn("Exception from bulkReceive " + exception.getMessage(), exception);
				close(node);
				disconnected = true;
				log.error("failed to receive messages from topic: {}", topic);
			}
			return createFetchResponse(request, topic, Collections.emptyList(), disconnected, exception);
		}catch(Exception ex) {
			log.error("Exception from bulkReceive " + ex, ex );
			close(node);
			disconnected = true;
			return createFetchResponse(request, topic, Collections.emptyList(), true, ex);
		}
	}

	private ClientResponse createFetchResponse(ClientRequest request, String topic, List<AQjmsBytesMessage> messages, boolean disconnected, Exception exception) {
		return new ClientResponse(request.makeHeader((short)1), request.callback(), request.destination(), 
				request.createdTimeMs(), time.milliseconds(), disconnected, null,null,
				new FetchResponse(topic, messages, exception));
	}

	/**
	 * Commit messages consumed by all sessions.
	 */
	public ClientResponse commit(ClientRequest request) {
		CommitRequest.Builder builder = (CommitRequest.Builder)request.requestBuilder();
		CommitRequest commitRequest = builder.build();
		Map<Node, List<TopicPartition>> nodes =  commitRequest.nodes();
		Map<TopicPartition, OffsetAndMetadata> offsets = commitRequest.offsets();
		Map<Node, Exception> result = new HashMap<>();
		boolean error = false;
		log.debug("Commit Nodes. " + nodes.size());
		for(Map.Entry<Node, List<TopicPartition>> node : nodes.entrySet()) {
			if(node.getValue().size() > 0) {
				String topic = node.getValue().get(0).topic();
				TopicConsumers consumers = topicConsumersMap.get(node.getKey());
				try {
					Boolean ltwtSub = configs.getBoolean(ConsumerConfig.ORACLE_CONSUMER_LIGHTWEIGHT);

					if(!ltwtSub.equals(true)) {
						log.debug("Committing now for node " + node.toString());
						TopicSession jmsSession =consumers.getSession();
						if(jmsSession != null)
						{
							log.debug("Committing now for node " + node.toString());
							jmsSession.commit();
							log.debug("Commit done");
						}else {
							log.info("No valid session to commit for node " + node);
						}
					}
					else{
						log.debug("Performing lightweight commit for node " + node);
						commitOffsetsLightWeightSub(node.getKey(), topic, offsets);
					}
					result.put(node.getKey(), null);

				} catch(JMSException exception) {
					error = true;
					result.put(node.getKey(), exception);
				}
				catch(Exception e)
				{
					log.error("Exception from commit " + e, e);
				}
			}
			else {
				log.info("Not Committing on Node " + node);
			}

		}

		return createCommitResponse(request, nodes, offsets, result, error);
	}

	private void commitOffsetsLightWeightSub(Node node, String topic, Map<TopicPartition, OffsetAndMetadata> offsets) {
     	int size = offsets.size();
		int[] partitions = new int[size];
		int[] priorities = new int[size];
		long[] subshards = new long[size];
		long[] sequences = new long[size];

		int index = 0;
		for (Map.Entry<TopicPartition, OffsetAndMetadata> offsetEntry : offsets.entrySet()) {
			TopicPartition tp = offsetEntry.getKey();
			OffsetAndMetadata metadata = offsetEntry.getValue();
			partitions[index] = tp.partition() * 2;
			priorities[index] = 0;
			subshards[index] = metadata.offset() / MessageIdConverter.DEFAULT_SUBPARTITION_SIZE;
			sequences[index] = metadata.offset() % MessageIdConverter.DEFAULT_SUBPARTITION_SIZE;
			index++;
		}

		commitSyncAll(node, topic, partitions, priorities, subshards, sequences);
	}

	public void CommitSync(Node node, String topic, int partition_id, int priority, 
			long subshard_id, long seq_num ) {

		try {
			String user = getCurrentUser(node);
			CallableStatement cStmt = getOrCreateCallable(node, "COMMIT_SYNC", LTWT_COMMIT_SYNC);
			cStmt.setString(1, user);
			cStmt.setString(2, topic);
			cStmt.setString(3, configs.getString(ConsumerConfig.GROUP_ID_CONFIG));
			cStmt.setInt(4, partition_id);
			cStmt.setInt(5, priority);
			cStmt.setLong(6, subshard_id);
			cStmt.setLong(7, seq_num);
			cStmt.execute();
			log.debug("Light weight CommitSync executed successfully for topic: {}, partition: {}, subshard: {}, seq: {}",
					topic, partition_id, subshard_id, seq_num);
		} catch(Exception ex) {
			log.error("Error during light weight CommitSync for node: " + node + ", topic: " + topic, ex);
		}
	}

	public void commitSyncAll(Node node, String topic, int[] partition_id, int[] priority, 
			long[] subshard_id, long[] seq_num ) {

		try {
			OracleConnection oracleCon = (OracleConnection) getConnection(node);
			String user = getCurrentUser(node);

			Array partitionArray = oracleCon.createOracleArray("DBMS_TEQK.INPUT_ARRAY_T", partition_id);
			Array priorityArray = oracleCon.createOracleArray("DBMS_TEQK.INPUT_ARRAY_T", priority);
			Array subshardArray = oracleCon.createOracleArray("DBMS_TEQK.INPUT_ARRAY_T", subshard_id);
			Array sequenceArray = oracleCon.createOracleArray("DBMS_TEQK.INPUT_ARRAY_T", seq_num);

			CallableStatement cStmt = getOrCreateCallable(node, "COMMIT_SYNC_ALL", LTWT_COMMIT_SYNC_ALL);
			cStmt.setString(1, user);
			cStmt.setString(2, topic);
			cStmt.setString(3, configs.getString(ConsumerConfig.GROUP_ID_CONFIG));
			cStmt.setArray(4, partitionArray);
			cStmt.setArray(5, priorityArray);
			cStmt.setArray(6, subshardArray);
			cStmt.setArray(7, sequenceArray);
			cStmt.execute();
			log.debug("Light weight CommitSyncAll executed for topic: {}, partitions: {}", topic, partition_id.length);
		} catch(Exception ex) {
			log.error("Error in light weight commitSyncAll for topic: " + topic + ", node: " + node, ex);
		}
	}

	public void lightWeightSeek(Node node, String topic, long partition_id, long priority, 
			long subshard_id, long seq_num ) throws Exception {
		try {
			String user = getCurrentUser(node);
			CallableStatement cStmt = getOrCreateCallable(node, "SEEK", LTWT_SEEK);
			cStmt.setString(1, user);
			cStmt.setString(2, topic);
			cStmt.setString(3, configs.getString(ConsumerConfig.GROUP_ID_CONFIG));
			cStmt.setLong(4, partition_id);
			cStmt.setLong(5, priority);
			cStmt.setLong(6, subshard_id);
			cStmt.setLong(7, seq_num);
			cStmt.execute();
			log.debug("Light weight seek executed successfully for topic: {}, partition: {}, subshard: {}, seq: {}",
					topic, partition_id, subshard_id, seq_num);
		} catch(Exception ex) {
			log.error("Error in lightWeightseek for topic: " + topic + ", node: " + node, ex);
			throw ex;
		}
	}

	public void lightWeightSeektoBeginning(Node node, String topic, Long[] partition_id, Long[] priority) throws Exception {
		try {
			OracleConnection oracleCon = (OracleConnection) getConnection(node);
			String user = getCurrentUser(node);
			Array partitionArray = oracleCon.createOracleArray("DBMS_TEQK.SEEK_INPUT_ARRAY_T", partition_id);
			Array priorityArray = oracleCon.createOracleArray("DBMS_TEQK.SEEK_INPUT_ARRAY_T", priority);
			CallableStatement cStmt = getOrCreateCallable(node, "SEEK_TO_BEGINNING", LTWT_SEEK_TO_BEGINNING);
			cStmt.setString(1, user);
			cStmt.setString(2, topic);
			cStmt.setString(3, configs.getString(ConsumerConfig.GROUP_ID_CONFIG));
			cStmt.setArray(4, partitionArray);
			cStmt.setArray(5, priorityArray);
			log.debug("lightWeightSeektoBeginning: User: {}, Topic: {}, GroupId: {}, Partition IDs: {}, Priority: {}",
					user, topic, configs.getString(ConsumerConfig.GROUP_ID_CONFIG),
					Arrays.toString(partition_id), Arrays.toString(priority));

			cStmt.execute();
			log.debug("lightWeightSeektoBeginning executed for topic: {}, partitions: {}", topic, partition_id.length);
		} catch(Exception ex) {
			log.error("Error in lightWeightSeektoBeginning for topic: " + topic + ", node: " + node, ex);
			throw ex;
		}
	}


	public void lightWeightSeektoEnd(Node node, String topic, Long[] partition_id, Long[] priority) throws Exception {
		try {
			OracleConnection oracleCon = (OracleConnection) getConnection(node);
			String user = getCurrentUser(node);
			Array partitionArray = oracleCon.createOracleArray("DBMS_TEQK.SEEK_INPUT_ARRAY_T", partition_id);
			Array priorityArray = oracleCon.createOracleArray("DBMS_TEQK.SEEK_INPUT_ARRAY_T", priority);
			CallableStatement cStmt = getOrCreateCallable(node, "SEEK_TO_END", LTWT_SEEK_TO_END);
			cStmt.setString(1, user);
			cStmt.setString(2, topic);
			cStmt.setString(3, configs.getString(ConsumerConfig.GROUP_ID_CONFIG));
			cStmt.setArray(4, partitionArray);
			cStmt.setArray(5, priorityArray);
			log.debug("lightWeightSeektoEnd: User: {}, Topic: {}, GroupId: {}, Partition IDs: {}, Priority: {}",
					user, topic, configs.getString(ConsumerConfig.GROUP_ID_CONFIG),
					Arrays.toString(partition_id), Arrays.toString(priority));

			cStmt.execute();
			log.debug("lightWeightSeektoEnd executed for topic: {}, partitions: {}", topic, partition_id.length);
		} catch (Exception ex) {
			log.error("Error in lightWeightSeektoEnd for topic: " + topic + ", node: " + node, ex);
			throw ex;
		}
	}


	private void lightweightSubscriberSeek(Node node, String topic, Map<TopicPartition, Long> offsets, Map<TopicPartition, Exception> responses) {
		List<Long> seekbeginPartitions = new ArrayList<>();
		List<Long> seekEndPartitions = new ArrayList<>();
		List<Long> seekbeginPriorities = new ArrayList<>();
		List<Long> seekEndPriorities = new ArrayList<>();

		List<TopicPartition> seekBeginoffs = new ArrayList<>();
		List<TopicPartition> seekEndoffs = new ArrayList<>();

		for (Map.Entry<TopicPartition, Long> entry : offsets.entrySet()) {
			TopicPartition tp = entry.getKey();
			long offset = entry.getValue();
			long partition = tp.partition();
			long priority = 0;

			try {
				if (offset == -2L) {  // Seek to beginning
					seekbeginPartitions.add(2L * partition);
					seekbeginPriorities.add((long) priority);
					seekBeginoffs.add(tp);
					continue;
				}
				else if (offset == -1L) {  // Seek to end
					seekEndPartitions.add(2L * partition);
					seekEndPriorities.add((long) priority);
					seekEndoffs.add(tp);
					continue;
				}
				else {
					long subshard = offset / MessageIdConverter.DEFAULT_SUBPARTITION_SIZE;
					long sequence = offset % MessageIdConverter.DEFAULT_SUBPARTITION_SIZE;
					lightWeightSeek(node, topic, 2*partition, priority, subshard, sequence);
					responses.put(tp, null);
				}
			} catch (Exception ex) {
				responses.put(tp, ex);
			}
		}
		try {
			if (!seekbeginPartitions.isEmpty()) {
				lightWeightSeektoBeginning(node, topic,
						seekbeginPartitions.toArray(new Long[0]),
						seekbeginPriorities.toArray(new Long[0]));
				for (TopicPartition tp : seekBeginoffs) {
					responses.put(tp, null);
				}
			}

			if (!seekEndPartitions.isEmpty()) {
				lightWeightSeektoEnd(node, topic,
						seekEndPartitions.toArray(new Long[0]),
						seekEndPriorities.toArray(new Long[0]));
				for (TopicPartition tp : seekEndoffs) {
					responses.put(tp, null);
				}
			}
		}
		catch (Exception e) {
			log.error("Error in lightweightSubscriberSeek for topic: " + topic + ", node: " + node, e);
			for (TopicPartition tp : seekBeginoffs) {
				responses.put(tp, e);
			}
			for (TopicPartition tp : seekEndoffs) {
				responses.put(tp, e);
			}
		}
	}

	
    private ClientResponse createCommitResponse(ClientRequest request, Map<Node, List<TopicPartition>> nodes,
			Map<TopicPartition, OffsetAndMetadata> offsets, Map<Node, Exception> result, boolean error) {
		return new ClientResponse(request.makeHeader((short)1), request.callback(), request.destination(), 
				request.createdTimeMs(), time.milliseconds(), false, null,null,
				new CommitResponse(result, nodes, offsets, error));
	}

	private String getMsgIdFormat(Connection con, String topic )
	{
		String msgFormat = "66";

		PreparedStatement msgFrmtStmt = null;
		ResultSet rs = null;
		try{
			String enqoteTopic = ConnectionUtils.enquote(topic);
			String msgFrmtTxt = "select msgid from "+enqoteTopic+" where rownum = ?";
			msgFrmtStmt = con.prepareStatement(msgFrmtTxt);
			msgFrmtStmt.setInt(1,1);
			rs = msgFrmtStmt.executeQuery();
			if(rs.next() ) {
				msgFormat = rs.getString(1).substring(26, 28);
			}
		}catch(Exception e) {
			//Do Nothing
		}
		finally {
			try { 
				if(msgFrmtStmt != null) {msgFrmtStmt.close();}
			} catch(Exception e) {}
		}
		return msgFormat;

	}

	private class SeekInput {
		static final int SEEK_BEGIN = 1;
		static final int SEEK_END = 2;
		static final int SEEK_MSGID = 3;
		static final int NO_DISCARD_SKIPPED = 1;
		static final int DISCARD_SKIPPED = 2;
		int partition;
		int priority;
		int seekType;
		String seekMsgId;
		public SeekInput() {
			priority = -1;
		}
	}

	private static void validateMsgId(String msgId) throws IllegalArgumentException {

		if(msgId == null || msgId.length() !=32)
			throw new IllegalArgumentException("Invalid Message Id " + ((msgId==null)?"null":msgId));

		try {
			for(int i =0; i< 32 ; i+=4)
			{
				String msgIdPart = msgId.substring(i,i+4); 
				Long.parseLong(msgIdPart,16);
			}
		}catch(Exception e) {
			throw new IllegalArgumentException("Invalid Mesage Id " + msgId);
		}   
	}   
	/*private static int getpriority(String msgId) {
    	try {

    	}catch(Exception e) {
    		return 1;
    	}
    }*/

	public ClientResponse seek(ClientRequest request) {
		log.debug("Sending Seek Request");
		Map<TopicPartition, Exception> responses = new HashMap<>();
		CallableStatement seekStmt = null;
		try {
			OffsetResetRequest.Builder builder = (OffsetResetRequest.Builder)request.requestBuilder();
			OffsetResetRequest offsetResetRequest = builder.build();
			Node node = metadata.getNodeById(Integer.parseInt(request.destination()));
			log.debug("Destination Node: " + node);

			Map<TopicPartition, Long> offsetResetTimestamps = offsetResetRequest.offsetResetTimestamps();
			Map<String, Map<TopicPartition, Long>> offsetResetTimeStampByTopic = new HashMap<String, Map<TopicPartition, Long>>() ;
			for(Map.Entry<TopicPartition, Long> offsetResetTimestamp : offsetResetTimestamps.entrySet()) {
				String topic = offsetResetTimestamp.getKey().topic();
				if ( !offsetResetTimeStampByTopic.containsKey(topic) ) {
					offsetResetTimeStampByTopic.put(topic, new  HashMap<TopicPartition, Long>());
				}
				offsetResetTimeStampByTopic.get(topic).put(offsetResetTimestamp.getKey(), offsetResetTimestamp.getValue());
			}
			TopicConsumers consumers = topicConsumersMap.get(node);
			Connection con = ((AQjmsSession)consumers.getSession()).getDBConnection();

			SeekInput[] seekInputs = null;
			String[] inArgs = new String[5];
			int indx =0;
			for(Map.Entry<String, Map<TopicPartition, Long>> offsetResetTimestampOfTopic : offsetResetTimeStampByTopic.entrySet()) {
				String topic =  offsetResetTimestampOfTopic.getKey();
				inArgs[0] = "Topic: " + topic + " ";
				Map<TopicPartition,Long> partitionOffsets = offsetResetTimestampOfTopic.getValue();

				if(consumers.lightWeightSub) {
					lightweightSubscriberSeek(node, topic, partitionOffsets, responses);
					continue;
				}

				try {
					if(msgIdFormat.equals("00") ) {
						msgIdFormat = getMsgIdFormat(con, topic);
					}

					int inputSize = partitionOffsets.entrySet().size(); 
					seekInputs = new SeekInput[inputSize];

					for(Map.Entry<TopicPartition, Long> offsets : partitionOffsets.entrySet()) {
						seekInputs[indx] = new SeekInput(); 
						try {
							TopicPartition tp = offsets.getKey();
							seekInputs[indx].partition = tp.partition();
							inArgs[1] = "Partrition: " + seekInputs[indx].partition;
							if(offsets.getValue() == -2L) {
								seekInputs[indx].seekType = SeekInput.SEEK_BEGIN; // Seek to Beginning
								inArgs[2]= "Seek Type: " + seekInputs[indx].seekType;
							}
							else if( offsets.getValue() == -1L) {
								seekInputs[indx].seekType = SeekInput.SEEK_END; // Seek to End
								inArgs[2]= "Seek Type: " + seekInputs[indx].seekType;
							}
							else {
								seekInputs[indx].seekType = SeekInput.SEEK_MSGID; // Seek to MessageId
								inArgs[2]= "Seek Type: " + seekInputs[indx].seekType;
								inArgs[3] ="Seek to Offset: " +  offsets.getValue();
								seekInputs[indx].seekMsgId = MessageIdConverter.getMsgId(tp, offsets.getValue(), msgIdFormat, 0);
								inArgs[4] = "Seek To MsgId: "+seekInputs[indx].seekMsgId ;
								validateMsgId(seekInputs[indx].seekMsgId);
							}
							indx++;
						}catch(IllegalArgumentException e ) {
							String errorMsg = "";
							for(int i =0; i<inArgs.length; i++ ) {
								if(inArgs[i] == null)
									break;

								errorMsg += inArgs[i];
							}
							Exception newE  =  new IllegalArgumentException(errorMsg, e);
							throw newE;
						}
					}

					StringBuilder sb = new StringBuilder();
					sb.append("declare \n seek_output_array  dbms_aq.seek_output_array_t; \n ");
					sb.append("begin \n dbms_aq.seek(queue_name => ?,");	
					sb.append("consumer_name=>?,");
					sb.append("seek_input_array =>  dbms_aq.seek_input_array_t( ") ;
					for(int i = 0; i < seekInputs.length; i++) {
						sb.append("dbms_aq.seek_input_t( shard=> ?, priority => ?, seek_type => ?, seek_pos => dbms_aq.seek_pos_t( msgid => hextoraw(?))) ");
						if(i != seekInputs.length-1)
							sb.append(", ");
					}
					sb.append("), ");
					sb.append("skip_option => ?, seek_output_array => seek_output_array);\n");
					sb.append("end;\n" );
					seekStmt = con.prepareCall(sb.toString());
					int stmtIndx = 1;
					seekStmt.setString(stmtIndx++, ConnectionUtils.enquote(topic));
					seekStmt.setString(stmtIndx++, configs.getString(ConsumerConfig.GROUP_ID_CONFIG));

					for(int i=0; i < seekInputs.length; i++) {
						if(seekInputs[i].partition == -1) {
							seekStmt.setInt(stmtIndx++, seekInputs[i].partition);
						}else {
							seekStmt.setInt(stmtIndx++, 2*seekInputs[i].partition);
						}
						if(seekInputs[i].seekType == SeekInput.SEEK_MSGID) {
							seekStmt.setInt(stmtIndx++, 0);
						}else {
							seekStmt.setInt(stmtIndx++, seekInputs[i].priority);
						}

						seekStmt.setInt(stmtIndx++, seekInputs[i].seekType);
						if(seekInputs[i].seekType == SeekInput.SEEK_MSGID){
							seekStmt.setString(stmtIndx++, seekInputs[i].seekMsgId);
						}else {
							seekStmt.setNull(stmtIndx++,Types.CHAR);
						}
					}
					seekStmt.setInt(stmtIndx++, SeekInput.DISCARD_SKIPPED);
					log.debug("Invoking 'dbms_aq.seek_input_t' for the topic: "+ topic);
					seekStmt.execute();
					log.debug("'dbms_aq.seek_input_t' completed for the topic: "+ topic);
					for(Map.Entry<TopicPartition, Long> offsets : offsetResetTimestampOfTopic.getValue().entrySet()) {
						responses.put(offsets.getKey(), null);
					}

				} catch(Exception e) {
					for(Map.Entry<TopicPartition, Long> offsets : offsetResetTimestampOfTopic.getValue().entrySet()) {
						responses.put(offsets.getKey(), e);
					}
				}finally {
					if(seekStmt != null) {
						try { 
							seekStmt.close();
							seekStmt = null;
						}catch(Exception e) {}
					}
				}
			}// While Topics

		} catch(Exception e) {
			return new ClientResponse(request.makeHeader((short)1), request.callback(), request.destination(), 
					request.createdTimeMs(), time.milliseconds(), true, null,null, new OffsetResetResponse(responses, e));
		}
		return new ClientResponse(request.makeHeader((short)1), request.callback(), request.destination(), 
				request.createdTimeMs(), time.milliseconds(), false, null,null, new OffsetResetResponse(responses, null));
	}

	
    private ClientResponse unsubscribe(ClientRequest request) {
		HashMap<String, Exception> response = new HashMap<>();
		for(Map.Entry<Node, TopicConsumers> topicConsumersByNode: topicConsumersMap.entrySet())
		{
			for(Map.Entry<String, TopicSubscriber> topicSubscriber : topicConsumersByNode.getValue().getTopicSubscriberMap().entrySet()) {
				try {
					((AQjmsConsumer)topicSubscriber.getValue()).close();
					response.put(topicSubscriber.getKey(), null);
					topicConsumersByNode.getValue().remove(topicSubscriber.getKey());
				}catch(JMSException jms) {
					response.put(topicSubscriber.getKey(), jms);
				}
			}
			try {
				((AQjmsSession)topicConsumersByNode.getValue().getSession()).close();
				((AQjmsConnection)topicConsumersByNode.getValue().getConnection()).close();
				topicConsumersByNode.getValue().setSession(null);
				topicConsumersByNode.getValue().setConnection(null);
				// ToDo: Delete User_queue_partition_assignment_table entry for   this Consumer Session from Database
				// Execute DBMS_TEQK.AQ$_REMOVE_SESSION()

			} 

			catch(JMSException jms) {
				//log.error("Failed to close session: {} associated with connection: {} and node: {}  ", consumers.getSession(), topicConsumersMap.getConnection(), node );
			}
		}

		topicConsumersMap.clear();
		return new ClientResponse(request.makeHeader((short)1), request.callback(), request.destination(), 
				request.createdTimeMs(), time.milliseconds(), true,null,null, new UnsubscribeResponse(response));
	}

	private ClientResponse getMetadata(ClientRequest request) {
		Connection conn = null;
		Node node = null;
		Cluster cluster = null;
		//Cluster used for this metadata is still a bootstrap cluster and does not have all necessary information
		//Pick any random node from the bootstrap nodes and send metadata request.
		if(metadata.isBootstrap())
		{
			cluster = metadata.fetch();
			List<Node> clusterNodes = NetworkClient.convertToOracleNodes(cluster.nodes());
			// Check if we have a node where connection already exists
			Set<Node> nodesWithConn = topicConsumersMap.keySet();
			for(Node nodeNow: clusterNodes)
			{
				for(Node connectedNode : nodesWithConn)
				{
					if(connectedNode.equals(nodeNow))
					{
						//Found a node with a connection to database.
						node = nodeNow;
						break;
					}
				}
			}
			if(node == null)
			{
				//No node with connection yet. Pick the first bootstrap node.
				node = clusterNodes.get(0);
				log.debug("No Connected Node Found. Picked first of bootstrap nodes.: " + node);
			}
		}
		else
		{
			node = (org.oracle.okafka.common.Node)metadata.getNodeById(Integer.parseInt(request.destination()));
		}
		try {
			TopicConsumers  tConsumer = topicConsumersMap.get(node);
			if(tConsumer == null)
				throw new NullPointerException("TConsumer for Node "+ node);

			TopicSession tSession = tConsumer.getSession();
			if(tSession == null)
				throw new NullPointerException ("TSesion for TConsumer for node" + node);

			conn = ((AQjmsSession)topicConsumersMap.get(node).getSession()).getDBConnection();			
		} catch(JMSException jmsExcp) {			
			try {
				log.trace("Unexcepted error occured with connection to node {}, closing the connection", request.destination());
				topicConsumersMap.get(node).getConnection().close();
				log.trace("Connection with node {} is closed", request.destination());
			} catch(JMSException jmsEx) {
				log.trace("Failed to close connection with node {}", request.destination());
			}
		}

		ClientResponse response = getMetadataNow(request, conn, node, metadata.updateRequested());

		MetadataResponse metadataresponse = (MetadataResponse)response.responseBody();

		org.apache.kafka.common.Cluster updatedCluster = metadataresponse.cluster();

		for(String topic: updatedCluster.topics()) {
			try {
				super.fetchQueueParameters(topic, conn, metadata.topicParaMap);
			} catch (SQLException e) {
				log.error("Exception while fetching TEQ parameters and updating metadata " + e.getMessage());
			}
		}


		if(response.wasDisconnected()) {
			topicConsumersMap.remove(node);
			metadata.requestUpdate();
		}
		return response;
	}

	/** Sends a join_group request to TEQ. JOIN_GROUP call returns a list of sessions that are part of rebalancing and their previous assignment. 
	 * @param request join group request
	 * @return
	 */
	private ClientResponse joinGroup(ClientRequest request) {	
		log.debug("Sending  AQ Join Group Request");
		JoinGroupRequest.Builder builder = (JoinGroupRequest.Builder)request.requestBuilder();
		JoinGroupRequest  joinRequest= builder.build();
		SessionData sessionData = joinRequest.getSessionData();
		CallableStatement joinStmt = null;
		int sessionId = -1;
		int instId = -1;
		int joinGroupVersion = sessionData.getVersion();

		try {
			Node node = metadata.getNodeById(Integer.parseInt(request.destination()));
			log.debug("Destination Node : " + node.toString());
			TopicConsumers consumers = topicConsumersMap.get(node);
			Connection con = ((AQjmsSession)consumers.getSession()).getDBConnection();

			sessionId = getSessionId(con);
			instId = getInstId(con);

			// First Join Group Request from all consumer and every join group request from group leader must attempt to clean the USER_QUEUE_PARTITION_ASSIGNMENT_TABLE
			if(joinGroupVersion < 0 || sessionData.getLeader() == 1)
			{
				log.debug("Attempt to cleanup USER_QUEUE_PARTITION_ASSIGNMENT_TABLE by session " + sessionId +" at instance " + instId);
				removeStaleEntries(con);
			}

			final String qpimLstType = "SYS.AQ$_QPIM_INFO_LIST";
			final String qpatLstType = "SYS.AQ$_QPAT_INFO_LIST";
			log.debug("Assigned partition Size " + sessionData.getAssignedPartitions().size());
			QPATInfo[] a = new QPATInfo[sessionData.getAssignedPartitions().size()];
			int ind = 0;
			for(PartitionData pData: sessionData.getAssignedPartitions()) {
				QPATInfo qpat = new QPATInfo();
				//qpat.setSchema(sessionData.getSchema() != null ? ConnectionUtils.enquote(sessionData.getSchema().toUpperCase()) : null);
				qpat.setSchema(sessionData.getSchema() != null ? (sessionData.getSchema().toUpperCase()) : null);
				//qpat.setQueueName(ConnectionUtils.enquote(pData.getTopicPartition().topic().toUpperCase()));
				qpat.setQueueName((pData.getTopicPartition().topic().toUpperCase()));
				qpat.setQueueId(pData.getQueueId());
				String subscriberNameIn =pData.getSubName() == null ? configs.getString(ConsumerConfig.GROUP_ID_CONFIG).toUpperCase(): pData.getSubName().toUpperCase();
				/*if(subscriberNameIn != null) {
				subscriberNameIn = ConnectionUtils.enquote(subscriberNameIn);
			}*/
				qpat.setSubscriberName(subscriberNameIn);
				qpat.setSubscriberId(pData.getSubId());
				qpat.setGroupLeader(sessionData.getLeader());
				qpat.setPartitionId(pData.getTopicPartition().partition() == -1 ? -1 : pData.getTopicPartition().partition() *2);
				//System.out.println("Setting partition for this qpat to " + qpat.getPartitionId());
				qpat.setFlags(-1);
				qpat.setVersion(sessionData.getVersion());
				qpat.setInstId(sessionData.getInstanceId());
				qpat.setSessionId(sessionData.getSessionId());
				qpat.setAuditId(sessionData.getAuditId());
				qpat.setTimeStamp(new java.sql.Time(System.currentTimeMillis()));
				//qpat.setTimeStamp(new java.sql.Time(sessionData.createTime.getTime()));
				a[ind] = qpat;
				ind++;
			}

			QPATInfoList qpatl = new QPATInfoList();
			qpatl.setArray(a);
			joinStmt = con.prepareCall("{call DBMS_TEQK.AQ$_JOIN_GROUP(?, ?, ?, ? )}");
			joinStmt.setObject(1,  qpatl, OracleTypes.ARRAY);
			joinStmt.setInt(4, joinGroupVersion);
			joinStmt.registerOutParameter(1, OracleTypes.ARRAY, qpatLstType);
			joinStmt.registerOutParameter(2, OracleTypes.ARRAY, qpimLstType);
			joinStmt.registerOutParameter(3, Types.INTEGER);
			joinStmt.registerOutParameter(4, Types.INTEGER);
			log.debug("Executing DBMS_TEQK.AQ$_JOIN_GROUP");
			joinStmt.execute();

			QPATInfo[] qpatInfo = ((QPATInfoList)qpatl.create(joinStmt.getObject(1), 2002)).getArray();
			QPIMInfoList qpiml = new QPIMInfoList();
			OracleData odata = ((QPIMInfoList)qpiml.create(joinStmt.getObject(2), 2002));
			QPIMInfo[] qpimInfo = null;
			if(odata != null) {
				qpimInfo = ((QPIMInfoList)odata).getArray();
			}

			log.debug("Return from DBMS_TEQK.AQ$_JOIN_GROUP. QPATINFO Size " +qpatInfo.length );
			for(int i = 0; i < qpatInfo.length; i++)
			{

				log.debug("QPAT[" +i +"]:(Inst,Session,GroupLeader,Partition,Flag,Version#) = ("+
						qpatInfo[i].getInstId()+","+qpatInfo[i].getSessionId()+"," +
						qpatInfo[i].getGroupLeader()+","+qpatInfo[i].getPartitionId()+"," +
						qpatInfo[i].getFlags()+","+qpatInfo[i].getVersion());
			}


			return createJoinGroupResponse(request, sessionId, instId, qpatInfo, qpimInfo, joinStmt.getInt(4), null, false);
		} catch(Exception exception) {
			boolean disconnected = false;
			log.error("Exception while executing JoinGroup " + exception.getMessage() , exception);
			if(exception instanceof SQLException )
			{
				SQLException sqlExcp = (SQLException)exception;
				int errorCode = sqlExcp.getErrorCode();
				log.error("JoinGroup: SQL ERROR: ORA-"+ errorCode, exception);
				if(errorCode == 28 || errorCode == 17410 || errorCode == 1403) {
					disconnected = true;
				}
			}
			return createJoinGroupResponse(request, sessionId, instId, null, null, -1, exception, disconnected);
		}
		finally {
			try {
				if(joinStmt != null)
					joinStmt.close();
			} catch(Exception ex) {
				//do nothing
			}	
		}
	}

	private void removeStaleEntries(Connection con)
	{
		try {
			PreparedStatement prStmt = con.prepareStatement("DELETE FROM USER_QUEUE_PARTITION_ASSIGNMENT_TABLE where session_id = -1");
			prStmt.execute();
			con.commit();
			prStmt.close();
		}catch(Exception e)
		{
			log.warn("Exception while deleting stale entries from USER_QUEUE_PARTITION_ASSIGNMENT_TABLE",e);
		}
	}

	private int getSessionId(Connection con) throws SQLException {

		Statement st = null;
		ResultSet rs = null;

		try {
			String sessionIdStr =  ((oracle.jdbc.internal.OracleConnection)con).getServerSessionInfo().getProperty("AUTH_SESSION_ID");
			return Integer.parseInt(sessionIdStr);
		}catch(Exception e)
		{
			// Failed to get session id from  connection object. Execute query to find session id now
		}
		try {

			st = con.createStatement(); 
			rs = st.executeQuery("select sys_context('USERENV', 'SID') from dual");
			if(rs.next() ) {
				return rs.getInt(1);
			} 
		}catch(SQLException sqlException) {
			//do nothing
		} finally {
			try {
				if(rs != null)
					rs.close();
			}catch(SQLException exception) {

			}
			try {
				if(st != null)
					st.close();
			}catch(SQLException exception) {

			}  		
		}

		throw new SQLException("Error in fetching Session Id");

	}

	private int getInstId(Connection con) throws SQLException {
		Statement st = null;
		ResultSet rs = null;
		try {
			String instIdStr = ((oracle.jdbc.internal.OracleConnection)con).getServerSessionInfo().getProperty("AUTH_INSTANCE_NO");
			return Integer.parseInt(instIdStr);
		}catch(Exception e)
		{
			//Failed to get instance number from connection object. Do Query now
		}
		try {
			st = con.createStatement(); 
			rs = st.executeQuery("select sys_context('USERENV', 'INSTANCE') from dual");
			if(rs.next() ) {
				return rs.getInt(1);
			} 
		}catch(SQLException sqlException) {
			//do nothing
		} finally {
			try {
				if(rs != null)
					rs.close();
			}catch(SQLException exception) {

			}
			try {
				if(st != null)
					st.close();
			}catch(SQLException exception) {

			}

		}

		throw new SQLException("Error in fetching Instance Id");

	}


	public int getSubcriberCount(Node node, String topic) throws SQLException {
		int count =0;
		PreparedStatement Stmt = null;
		ResultSet rs = null;
		Connection con;
		try {
			con = ((AQjmsSession)topicConsumersMap.get(node).getSession()).getDBConnection();
			String query = "select count(*) from  user_durable_subs where name = :1 and queue_name = :2";
			Stmt = con.prepareStatement(query);
			Stmt.setString(1, configs.getString(ConsumerConfig.GROUP_ID_CONFIG));
			Stmt.setString(2, topic);
			rs = Stmt.executeQuery();

			if(rs.next()) {
				count = rs.getInt(1);
				return count;
			}
		}catch(SQLException sqlException) {
			//do nothing
		} catch (JMSException e) {
			//do nothing
		} finally {
			try {
				if(rs != null)
					rs.close();
			}catch(SQLException exception) {

			}
			try {
				if(Stmt != null)
					Stmt.close();
			}catch(SQLException exception) {

			}

		}

		throw new SQLException("Error in getting the subscriber count");
	}

	public String getoffsetStartegy() {
		return configs.getString(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG);
	}

	/* Returns a list of sessions that are part of rebalancing and their previous assignment */
	private ClientResponse createJoinGroupResponse(ClientRequest request, int sessionId, int instId, QPATInfo[] qpatInfo, QPIMInfo[] qpimInfo, int version, Exception exception, boolean disconnected) {

		Map<String, SessionData> memberPartitionMap = new HashMap<String, SessionData>();
		List<PartitionData> partitions = new ArrayList<>();
		int leader = 0;
		int length = 0;
		if(qpatInfo != null)
			length = qpatInfo.length;

		log.debug("Creating Join Group Response. QPAT Length: " +length);
		try {
			if(disconnected)
			{
				throw exception;
			}

			if(qpatInfo != null) {
				//Check if This session is Leader or not
				for(int ind = 0; ind < length; ind++) {
					if(qpatInfo[ind].getSessionId() == sessionId && qpatInfo[ind].getInstId() == instId ) {
						if(qpatInfo[ind].getGroupLeader() == 1)
							leader = 1;
						break;
					}
				}
				log.debug("Leader of the group? " + leader);
				if(leader == 1)
				{
					//Set Partition ownership map
					Map<Integer, ArrayList<Integer>> instPListMap = new HashMap<Integer, ArrayList<Integer>>();
					Map<Integer,Integer> partitionInstMap = new HashMap<Integer,Integer>();

					String topic = qpatInfo[0]!=null?qpatInfo[0].getQueueName():null;

					if(qpimInfo != null && qpimInfo.length > 0) 
					{
						log.debug("Partitions Created for topic " 
								+qpatInfo[0].getSchema()+"."+qpatInfo[0].getQueueName()+ " = " +qpimInfo.length);
						for(QPIMInfo qpimNow : qpimInfo)
						{
							if(topic == null)
								topic = qpimNow.getQueueName();

							int instNow = qpimNow.getOwnerInstId();
							ArrayList<Integer> pInstListNow = instPListMap.get(instNow);
							if(pInstListNow == null)
							{
								pInstListNow = new ArrayList<Integer>();
								instPListMap.put(instNow, pInstListNow);
							}
							pInstListNow.add(qpimNow.getPartitionId()/2);
							partitionInstMap.put(qpimNow.getPartitionId()/2, instNow);
						}
					} 
					else
					{
						log.info("No partition yet created for Topic "+ 
								qpatInfo[0].getSchema()+"."+qpatInfo[0].getQueueName());
					}
					ArrayList<SessionData> membersList = new ArrayList<SessionData>();
					for(QPATInfo qpatNow: qpatInfo)
					{

						if(qpatNow.name == null )
						{
							qpatNow.name = qpatNow.getInstId() +"_"+ qpatNow.getSessionId();
						}

						try {
							//System.out.println("TxEQAssignor:Printing QPat " + qpatNow.name);
							//System.out.println("TxEQAssignor:"+ qpatNow.toString());
						}catch(Exception e)
						{
							log.error("Exception while printing qpat " + qpatNow.name + " exception: " + e.getMessage());
						}

						String name = qpatNow.name;
						SessionData teqSession = memberPartitionMap.get(name);
						List<PartitionData> teqPList = null;
						if(teqSession == null)
						{
							teqSession = new SessionData( qpatNow.getSessionId(), qpatNow.getInstId(), 
									qpatNow.getSchema(),qpatNow.getQueueName(), qpatNow.getQueueId(), qpatNow.getSubscriberName(),
									qpatNow.getSubscriberId(), null, // qpatNow.getTimeStamp()== null?new java.util.Date():new java.util.Date(qpatNow.getTimeStamp().getTime()), 
									qpatNow.getGroupLeader(), qpatNow.getVersion(), qpatNow.getAuditId());
							//System.out.println("createJoinGroupResponse 1: qpat With queue Id and subscriber ID "  + qpatNow.getQueueId() + " " + qpatNow.getSubscriberId());
							log.debug("Member Added " + teqSession);
							membersList.add(teqSession);
							memberPartitionMap.put(name, teqSession);
						}
						if(qpatNow.getPartitionId() >= 0) 
						{
							teqPList = teqSession.getPreviousPartitions();
							int pIdNow = qpatNow.getPartitionId()/2;
							int ownerPid = partitionInstMap.get(pIdNow);
							//System.out.println("createJoinGroupResponse 2: Partition " + pIdNow  + "  Added to " +qpatNow.name); 
							teqPList.add(new PartitionData(qpatNow.getQueueName(), qpatNow.getQueueId(), pIdNow, qpatNow.getSubscriberName(), qpatNow.getSubscriberId(), ownerPid, ownerPid == qpatNow.getInstId()? true:false));
						}
					}
					log.debug("Invoking Assignors");
					for(ConsumerPartitionAssignor assignor : assignors)
					{
						if(assignor instanceof TxEQAssignor)
						{
							log.debug("Using TEQ Assignor. ");
							TxEQAssignor txEQAssignor = (TxEQAssignor) assignor;
							txEQAssignor.setInstPListMap(instPListMap);
							Map<String, ArrayList<SessionData>> topicMemberMap = new HashMap<String,ArrayList<SessionData>>();
							topicMemberMap.put(topic,membersList);
							log.debug("Setting topicMembership Map. Member List Size " + membersList.size() +" Map Size  " + topicMemberMap.size());
							txEQAssignor.setPartitionMemberMap(topicMemberMap);
						}
					}
				}
				if(qpimInfo != null)
				{
					for(int ind = 0; ind < qpimInfo.length; ind++) {
						partitions.add(new PartitionData(qpimInfo[ind].getQueueName(), qpatInfo[0].getQueueId(), qpimInfo[ind].getPartitionId()/2, qpatInfo[0].getSubscriberName(), qpatInfo[0].getSubscriberId(), qpimInfo[ind].getOwnerInstId() , false));
					}
				}
			}
		} catch(Exception excp) {
			if(excp instanceof SQLException)
			{
				SQLException sqlEx = (SQLException)excp;
				log.error("Exception in creating Join Group response " + sqlEx.getMessage(), sqlEx);
			}
			memberPartitionMap.clear();
			partitions.clear();
			leader = -1;
		}
		JoinGroupResponse jgResponse = new JoinGroupResponse(memberPartitionMap, partitions, leader, version, exception);
		log.debug("Join Group Response Created");
		return new ClientResponse(request.makeHeader((short)1), request.callback(), request.destination(), 
				request.createdTimeMs(), time.milliseconds(), disconnected, null,null, jgResponse);
	}

	/** 
	 * Sends a SYNC request to TEQ. Leader session performs assignment to all sessions which are part of consumer group(or participating in rebalancing) from subscribed topic using partition assignor and this assignment is sent to TEQ in sync group request.
	 * Follower sends an empty request.
	 * @param request
	 * @return
	 */
	private ClientResponse syncGroup(ClientRequest request) {
		SyncGroupRequest.Builder builder = (SyncGroupRequest.Builder)request.requestBuilder();
		SyncGroupRequest  syncRequest= builder.build();
		List<SessionData> sData = syncRequest.getSessionData();
		CallableStatement syncStmt = null;
		Connection con = null;
		//System.out.println("SyncGroup 1: Sending Sync Group Now");
		try {
			Node node = metadata.getNodeById(Integer.parseInt(request.destination()));
			TopicConsumers consumers = topicConsumersMap.get(node);
			con = ((AQjmsSession)consumers.getSession()).getDBConnection();

			final String typeList = "SYS.AQ$_QPAT_INFO_LIST";
			int size = 0;

			for(SessionData data : sData) {
				size += data.getAssignedPartitions().size();
			}
			log.debug("Before Sync, Assigned Partition List size "+ size);
			QPATInfo[] a = new QPATInfo[size];

			int ind = 0;
			for(SessionData sessionData : sData) {
				for(PartitionData pData: sessionData.getAssignedPartitions()) {

					QPATInfo qpat = new QPATInfo();
					//qpat.setSchema(sessionData.getSchema() != null ? ConnectionUtils.enquote(sessionData.getSchema().toUpperCase()) : null);
					qpat.setSchema(sessionData.getSchema() != null ? (sessionData.getSchema().toUpperCase()) : null);

					//qpat.setQueueName(ConnectionUtils.enquote(pData.getTopicPartition().topic().toUpperCase()));
					qpat.setQueueName((pData.getTopicPartition().topic().toUpperCase()));
					qpat.setQueueId(sessionData.getQueueId());
					String subscriberNameIn =pData.getSubName() == null ? configs.getString(ConsumerConfig.GROUP_ID_CONFIG).toUpperCase(): pData.getSubName().toUpperCase();
					/*if(subscriberNameIn != null) {
						subscriberNameIn = ConnectionUtils.enquote(subscriberNameIn);
					}*/
					qpat.setSubscriberName(subscriberNameIn);
					qpat.setSubscriberId(sessionData.getSubscriberId());
					qpat.setGroupLeader(sessionData.getLeader());
					int pId = pData.getTopicPartition().partition();
					// If partitions assigned is -1 then keep it as it is, else multiply with 2 as for TEQ partitions are even numbered
					if(pId > 0 )
						pId *= 2;

					qpat.setPartitionId(pId);
					qpat.setFlags(2); // DBMS_TEQK.ASSIGNED
					qpat.setVersion(sessionData.getVersion());
					qpat.setInstId(sessionData.getInstanceId());
					qpat.setSessionId(sessionData.getSessionId());
					qpat.setAuditId(sessionData.getAuditId());

					qpat.setTimeStamp(new java.sql.Time(System.currentTimeMillis()));
					a[ind] = qpat;
					ind++;
				}
			}

			QPATInfoList qpatl = new QPATInfoList();
			if(a.length > 0)
				qpatl.setArray(a);
			syncStmt = con.prepareCall("{call DBMS_TEQK.AQ$_SYNC(?, ?)}");
			syncStmt.setObject(1, qpatl, OracleTypes.ARRAY);
			syncStmt.setInt(2, syncRequest.getVersion());
			syncStmt.registerOutParameter(1, OracleTypes.ARRAY, typeList);
			syncStmt.registerOutParameter(2, Types.INTEGER);
			//System.out.println("SyncGroup 8: Executing SYNC Procedure now");
			syncStmt.execute();
			//System.out.println("SyncGroup 9: Retrieved  Response. creating qpatInfo array now");
			QPATInfo[] qpatInfo = ((QPATInfoList)qpatl.create(syncStmt.getObject(1), 2002)).getArray();

			log.debug("Return from DBMS_TEQK.AQ$_SYNC. QPATINFO Size " +qpatInfo.length );
			for(int i = 0; i < qpatInfo.length; i++)
			{

				log.debug("QPAT[" +i +"]:(Inst,Session,GroupLeader,Partition,Flag,Version#) = ("+
						qpatInfo[i].getInstId()+","+qpatInfo[i].getSessionId()+"," +
						qpatInfo[i].getGroupLeader()+","+qpatInfo[i].getPartitionId()+"," +
						qpatInfo[i].getFlags()+","+qpatInfo[i].getVersion());
			}

			//System.out.println("SyncGroup 10 : Sync Response Received. Assigned Partition count " + qpatInfo.length);
			return createSyncResponse(request, qpatInfo, syncStmt.getInt(2), null, false);
		} catch(Exception exception) {
			boolean disconnected = false;
			log.error("Exception in syncGroup " + exception.getMessage(), exception);
			if(exception instanceof SQLException)
			{
				SQLException sqlExcp = (SQLException) exception;
				int sqlErrorCode = sqlExcp.getErrorCode();
				log.error("syncGroup: SQL ERROR: ORA-"+ sqlErrorCode, exception);
				if(sqlErrorCode == 28 || sqlErrorCode == 17410 || sqlErrorCode == 1403)
					disconnected = true;
			}
			return createSyncResponse(request, null, -1, exception, disconnected);
		} finally {
			try {
				if(syncStmt != null)
					syncStmt.close();
			}catch(SQLException sqlException) {
				//do nothing
			}
		}
	}

	/* Returns a list of partitions assigned to this session */
	private ClientResponse createSyncResponse(ClientRequest request, QPATInfo[] qpatInfo, int version, Exception exception, boolean disconnected) {
		SessionData data = null;
		try {
			if(exception == null) {
				//System.out.println("Processing Sync Response. Printing Assigned Paritions");
				if(qpatInfo.length > 0) {
					//System.out.println("Response for session : "+ qpatInfo[0].getInstId()+"_"+qpatInfo[0].getSessionId());

					data = new SessionData(qpatInfo[0].getSessionId(), qpatInfo[0].getInstId(), qpatInfo[0].getSchema(), qpatInfo[0].getQueueName(),
							qpatInfo[0].getQueueId(), qpatInfo[0].getSubscriberName(), qpatInfo[0].getSubscriberId(), new java.util.Date(),
							/*new java.sql.Date(qpatInfo[0].getTimeStamp().getTime()), */ qpatInfo[0].getGroupLeader(), qpatInfo[0].getVersion(), qpatInfo[0].getAuditId());
				}

				for(int ind = 0; ind < qpatInfo.length; ind++) {
					int pId = qpatInfo[ind].getPartitionId();
					if(pId >0)
						pId = pId/2;

					//System.out.println("TxEQAssignor:Assigned Partition :   " + pId);
					data.addAssignedPartitions(new PartitionData(qpatInfo[ind].getQueueName(), qpatInfo[ind].getQueueId(), pId,
							qpatInfo[ind].getSubscriberName(), qpatInfo[ind].getSubscriberId(), qpatInfo[ind].getInstId(), data.getInstanceId()==qpatInfo[ind].getInstId()?true:false ));
				}
			}
		} catch(Exception ex) {
			log.error("Exception from createSyncResponse " +ex, ex);
			ex.printStackTrace();
			exception = ex;
		}

		return new ClientResponse(request.makeHeader((short)1), request.callback(), request.destination(), 
				request.createdTimeMs(), time.milliseconds(), disconnected, null,null, new SyncGroupResponse(data, version, exception));
	}
	
	private ClientResponse fetchOffsets(ClientRequest request) {

		OffsetFetchRequest.Builder builder = (OffsetFetchRequest.Builder) request.requestBuilder();
		OffsetFetchRequest offsetFetchRequest = builder.build();
		List<TopicPartition> topicPartitions = offsetFetchRequest.perGroupTopicpartitions().values().iterator().next();
		String groupId = offsetFetchRequest.perGroupTopicpartitions().keySet().iterator().next();
		Map<TopicPartition, PartitionOffsetData> offsetFetchResponseMap = new HashMap<>();
		boolean disconnected = false;
		Exception exception = null;
		Connection jdbcConn = null;
		TopicConsumers topicConsumer = null;

		for (Node nodeNow : topicConsumersMap.keySet()) {
			if (request.destination().equals("" + nodeNow.id())) {
				topicConsumer = topicConsumersMap.get(nodeNow);
			}
		}
		try {
			jdbcConn = topicConsumer.getDBConnection();
			
			for (TopicPartition tp : topicPartitions) {
				try {
					long offset = FetchOffsets.fetchCommittedOffset(tp.topic(), tp.partition(), groupId, jdbcConn);
					if (offset != -1) 
						offsetFetchResponseMap.put(tp, new PartitionOffsetData(offset,null));
					else {
						log.warn("No Committed Offset found for Queue:" + tp.topic() + "Partition:" + tp.partition());
						offsetFetchResponseMap.put(tp, null);
					}
				} catch (SQLException sqlE) {
						int errorCode = sqlE.getErrorCode();
						log.error("SQL ERROR while fetching commit offset: ORA- " + errorCode, sqlE);
						if(errorCode == 28 || errorCode == 17410) {
							disconnected = true;
							throw sqlE;
						}
						else
							offsetFetchResponseMap.put(tp, new PartitionOffsetData(-1L,sqlE));
				}
			}


		} catch (Exception e) {
			log.error("Exception while fetching offsets " + e.getMessage(), e);
			try {
				exception = e;
				log.debug("Unexcepted error occured with connection to node {}, closing the connection",
						request.destination());
				if (jdbcConn != null)
					jdbcConn.close();

				log.trace("Connection with node {} is closed", request.destination());
			} catch (SQLException sqlEx) {
				log.trace("Failed to close connection with node {}", request.destination());
			}
		}

		OffsetFetchResponse offsetResponse = new OffsetFetchResponse(Collections.singletonMap(groupId, offsetFetchResponseMap));
		
		offsetResponse.setException(exception);

		return new ClientResponse(request.makeHeader((short) 1), request.callback(), request.destination(),
				request.createdTimeMs(), System.currentTimeMillis(), disconnected, null, null, offsetResponse);
	}

	public ClientResponse connectMe(ClientRequest request)
	{
		ConnectMeRequest.Builder builder = (ConnectMeRequest.Builder)request.requestBuilder();
		ConnectMeRequest  connectMeRequest= builder.build();
		Node nodeNow = metadata.getNodeById(Integer.parseInt(request.destination()));
		TopicConsumers consumers = topicConsumersMap.get(nodeNow);
		ConnectMeResponse connMeResponse = null;

		if(consumers != null)
		{
			try {
				Connection conn = ((AQjmsSession)consumers.getSession()).getDBConnection();
				connMeResponse = connectMe(connectMeRequest, conn);
			} catch(Exception e)
			{
				log.error("Exception while executing DBMS_TEQK.AQ$_connect_me " + e.getMessage(), e);
			}
		}

		if(connMeResponse == null)
		{
			connMeResponse = new ConnectMeResponse();
			connMeResponse.setInstId(0);
		}

		return new ClientResponse(request.makeHeader((short)1), request.callback(), request.destination(), 
				request.createdTimeMs(), time.milliseconds(), false, null,null, connMeResponse );
	}


	private ConnectMeResponse connectMe(ConnectMeRequest connMeRequest, Connection conn)
	{
		int instId = 0;
		String url = "";
		int flags = 0;
		ConnectMeResponse connMeResponse = new ConnectMeResponse();
		connMeResponse.setInstId(0);

		String connectProc = " call DBMS_TEQK.AQ$_CONNECT_ME( schema => :1 , queue_name => :2 , subscriber_name => :3 , inst_id => :4, url => :5, flags => :6, p_list => :7)  ";		
		try (CallableStatement connectMeStmt = conn.prepareCall(connectProc)) {

			String schemaName = connMeRequest.getSchemaName();
			if(schemaName == null)
			{
				try {
					schemaName = conn.getMetaData().getUserName();
				}catch(Exception e)
				{
					schemaName = ""; // Oracle DB Server will pick the current schema
				}
			}
			connectMeStmt.setString(1, schemaName);
			connectMeStmt.setString(2, connMeRequest.getToipcName());
			connectMeStmt.setString(3, connMeRequest.getGroupId());
			connectMeStmt.registerOutParameter(4, java.sql.Types.NUMERIC);
			connectMeStmt.registerOutParameter(5, java.sql.Types.VARCHAR);
			connectMeStmt.registerOutParameter(6, java.sql.Types.NUMERIC);
			connectMeStmt.registerOutParameter(7, OracleTypes.ARRAY, "DBMS_TEQK.PARTITION_LIST");
			connectMeStmt.execute();

			instId = connectMeStmt.getInt(4);
			url = connectMeStmt.getString(5);
			flags = connectMeStmt.getInt(6);
			Array pArray = connectMeStmt.getArray(7);
			BigDecimal[] partitionArr = (BigDecimal[])pArray.getArray();

			connMeResponse.setInstId(instId);
			connMeResponse.setUrl(url);
			connMeResponse.setFlags(flags);
			connMeResponse.setPartitionList(partitionArr);
			log.info("Preferred Broker: " + instId+ " URL " + url);

		} catch(Exception connMeEx)
		{
			log.error("Exception while executing DBMS_TEQK.AQ$_CONNECTME " + connMeEx, connMeEx);
		}

		ArrayList<Node> nodeList = connMeResponse.processUrl();
		String security = configs.getString(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG);
		boolean plainText = security.equalsIgnoreCase("PLAINTEXT")?true:false;
		if(nodeList != null)
		{
			for(Node nodeNow: nodeList)
			{
				if( (plainText && nodeNow.protocol().equalsIgnoreCase("TCP")) ||
						(!plainText && nodeNow.protocol().equalsIgnoreCase("TCPS")))
				{
					connMeResponse.setPreferredNode(nodeNow);
					break;
				}
			}
		}

		return connMeResponse;
	}

	public void connect(Node node) throws JMSException{
		if(!topicConsumersMap.containsKey(node)) {
			TopicConsumers nodeConsumers = null;
			try {
				nodeConsumers = new TopicConsumers(node);
				topicConsumersMap.put(node, nodeConsumers);
				this.selectorMetrics.maybeRegisterConnectionMetrics(node);
				this.selectorMetrics.connectionCreated.record();
			} catch(JMSException e) {
				log.error("Exception while creating Topic consumer " + e, e );
				close(node, nodeConsumers);
				throw e;
			}
		}
	}

	public boolean isChannelReady(Node node) {
		return topicConsumersMap.containsKey(node);
	}

	public void close(Node node) {
		if(topicConsumersMap.get(node) == null)
			return;
		close(node, topicConsumersMap.get(node));
		topicConsumersMap.remove(node);
	}

	/**
	 * Closes AQKafkaConsumer
	 */
	public void close() {
		log.trace("Closing AQ kafka consumer");
		for(Map.Entry<Node, TopicConsumers> nodeConsumers : topicConsumersMap.entrySet()) {
			close(nodeConsumers.getKey(), nodeConsumers.getValue());
		}
		log.trace("Closed AQ kafka consumer");
		topicConsumersMap.clear();
	}

	/**
	 * Closes connection, session associated with each connection  and all topic consumers associated with session.
	 */
	private void close(Node node, TopicConsumers consumers) {
		if(node == null || consumers == null)
			return;
		for(Map.Entry<String, TopicSubscriber> topicSubscriber : consumers.getTopicSubscriberMap().entrySet()) {
			try {
				((AQjmsConsumer)topicSubscriber.getValue()).close();
			}catch(JMSException jms) {
				log.error("Failed to close topic consumer for topic: {} ", topicSubscriber.getKey());
			}
		}
		try {
			if ((AQjmsSession)consumers.getSession() != null) {
				((AQjmsSession)consumers.getSession()).close();
			}
		} catch(JMSException jms) {
			log.error("Failed to close session: {} associated with connection: {} and node: {}  ", consumers.getSession(), consumers.getConnection(), node );
		}
		try {
			if (consumers.getConnection() != null) {
				((AQjmsConnection)consumers.getConnection()).close();
				closeCallableStmt(node);	
			}
			this.selectorMetrics.connectionClosed.record();	
		} catch(JMSException jms) {
			log.error("Failed to close connection: {} associated with node: {}  ", consumers.getConnection(), node );
		}
		//topicConsumersMap.remove(node);
	}
	/**
	 * Creates topic connection to given node if connection doesn't exist.
	 * @param node to which connection has to be created.
	 * @throws JMSException throws jmsexception if failed to establish comnnection to given node.
	 */
	private void createTopicConnection(Node node) throws JMSException {
		createTopicConnection(node, TopicSession.AUTO_ACKNOWLEDGE);

	}
	private void createTopicConnection(Node node, int mode) throws JMSException {

		if(!topicConsumersMap.containsKey(node)) {
			TopicConsumers consumers =null;
			consumers = new TopicConsumers(node, mode);
			topicConsumersMap.put(node, consumers);
		}
	}

	public ClientResponse subscribe(ClientRequest request) {

		for(Map.Entry<Node, TopicConsumers> topicConsumersByNode: topicConsumersMap.entrySet())
		{
			for(Map.Entry<String, TopicSubscriber> topicSubscriber : topicConsumersByNode.getValue().getTopicSubscriberMap().entrySet()) {
				try {
					((AQjmsConsumer)topicSubscriber.getValue()).close();
					topicConsumersByNode.getValue().remove(topicSubscriber.getKey());
				}catch(JMSException jms) {
					//do nothing
				}
			}
			// ToDo:Check if we need this or not. Ideally when consumer is closed, not committed messages should be rolled back.
			try {
				AQjmsSession sess = ((AQjmsSession)topicConsumersByNode.getValue().getSession());
				if(sess.children() > 0)
				{
					// ToDo: Maintain list of topic subscribed with this topicConsumerMap and remove only those which are not needed
					log.info("Remove possible old subscribers from this KafkaConsumer");
					sess.close();
					topicConsumersByNode.getValue().setSession(null);
				}

			} catch(JMSException jms) {
				//log.error("Failed to close session: {} associated with connection: {} and node: {}  ", consumers.getSession(), consumers.getConnection(), node );
			}
		}
		SubscribeRequest.Builder builder = (SubscribeRequest.Builder)request.requestBuilder();
		SubscribeRequest commitRequest = builder.build();
		String topic = commitRequest.getTopic();
		Node node = metadata.getNodeById(Integer.parseInt(request.destination()));
		try {
			if(!topicConsumersMap.containsKey(node) ) {
				topicConsumersMap.put(node, new TopicConsumers(node));
			}
			TopicConsumers consumers = topicConsumersMap.get(node);	
			metadata.setDBVersion(consumers.getDBVersion());	
			
			if(consumers.getlightWeightSub() && metadata.getDBMajorVersion() > 26) {
				consumers.createLightWeightSub(topic, node);
			}
			else {
				consumers.getTopicSubscriber(topic);
			}
				
		} catch(JMSException exception) { 
			log.error("Exception during Subscribe request " + exception, exception);
			log.info("Exception during Subscribe request. " + exception);
			log.info("Closing connection to node. " + node);
			close(node);
			return createSubscribeResponse(request, topic, exception, false);
		}
		return createSubscribeResponse(request, topic, null, false);
	}

	private ClientResponse createSubscribeResponse(ClientRequest request, String topic, JMSException exception, boolean disconnected) {
		return new ClientResponse(request.makeHeader((short)1), request.callback(), request.destination(), 
				request.createdTimeMs(), time.milliseconds(), disconnected, null,null,
				new SubscribeResponse(topic, exception));

	}

	public Connection getDBConnection(Node n) throws KafkaException
	{
		Connection dbConn = null;
		try {
			//Poll is not invoked yet
			if(topicConsumersMap == null || topicConsumersMap.isEmpty())
			{
				log.trace("Poll is not invoked yet. Creating database connection for the first time ");
				TopicConsumers consumerToNode = new TopicConsumers((org.oracle.okafka.common.Node)n, Session.SESSION_TRANSACTED );
				topicConsumersMap.put((org.oracle.okafka.common.Node)n, consumerToNode);
			}

			for(Node nodeNow: topicConsumersMap.keySet())
			{
				if(nodeNow.id() == n.id())
				{
					TopicConsumers tConsumer =  topicConsumersMap.get(nodeNow);
					dbConn = tConsumer.getDBConnection();
					log.debug("Returning Connection for node " + nodeNow.toString() );
					if(dbConn != null && !dbConn.isClosed())
					{
						return dbConn;
					}
					else
					{
						log.debug("Connection to Node " + nodeNow + " is " + ((dbConn==null)?"null.":"closed."));
					}
				}
			}
		}catch(Exception e)
		{
			throw new KafkaException("Failed to fetch Oracle Database Connection for this consumer", e);
		}

		return dbConn;
	}

	public Connection getDBConnection() throws KafkaException
	{
		Connection dbConn = null;
		try {
			//Poll is not invoked yet
			if(topicConsumersMap == null || topicConsumersMap.isEmpty())
			{
				log.trace("Poll is not invoked yet. Creating database connection for the first time ");
				List<org.apache.kafka.common.Node> bootStrapNodes = metadata.fetch().nodes();
				for(org.apache.kafka.common.Node bNode : bootStrapNodes)
				{
					TopicConsumers bootStrapConsumer = new TopicConsumers((org.oracle.okafka.common.Node)bNode, Session.SESSION_TRANSACTED );
					topicConsumersMap.put((org.oracle.okafka.common.Node)bNode, bootStrapConsumer);
					break;
				}
				// A connection was created before poll is invoked. This connection is passed on to the application and may be used for transactional activity.
				// OKafkaConsumer must use this connection going forward. Hence it should not look for optimal database instance to consume records. Hence avoid connectMe call.
				skipConnectMe = true;
			}

			for(Node n: topicConsumersMap.keySet())
			{
				TopicConsumers tConsumer =  topicConsumersMap.get(n);
				dbConn = tConsumer.getDBConnection();
				if(dbConn != null && !dbConn.isClosed())
					return dbConn;
			}
		}catch(Exception e)
		{
			throw new KafkaException("Failed to fetch Oracle Database Connection for this consumer", e);
		}

		return dbConn;
	}

	public boolean skipConnectMe()
	{
		return this.skipConnectMe;
	}

	public void setSkipConnectMe(boolean skipConnectMe)
	{
		this.skipConnectMe = skipConnectMe;
	}

	public boolean isExternalConn()
	{
		return this.externalConn;
	}
	/**This class is used to create and manage connection to database instance.
	 * Also creates, manages session associated with each connection and topic consumers associated with each session
	 */
	private final class TopicConsumers {
		private TopicConnection conn = null;
		private TopicSession sess = null;
		private Map<String, TopicSubscriber> topicSubscribers = null;
		private final Node node;
		private String dbVersion;
		private Boolean lightWeightSub;
		public TopicConsumers(Node node) throws JMSException {
			this(node, TopicSession.AUTO_ACKNOWLEDGE);
		}
		public TopicConsumers(Node node,int mode) throws JMSException {
			
			this.node = node;
			conn = createTopicConnection(node);

			sess = createTopicSession(mode);
			try {
				Connection oConn = ((AQjmsSession)sess).getDBConnection();
				int instId = Integer.parseInt(((oracle.jdbc.internal.OracleConnection)oConn).getServerSessionInfo().getProperty("AUTH_INSTANCE_NO"));
				String serviceName = ((oracle.jdbc.internal.OracleConnection)oConn).getServerSessionInfo().getProperty("SERVICE_NAME");
				String instanceName = ((oracle.jdbc.internal.OracleConnection)oConn).getServerSessionInfo().getProperty("INSTANCE_NAME");
				String user = oConn.getMetaData().getUserName();
				try {
					String sessionId = ((oracle.jdbc.internal.OracleConnection)oConn).getServerSessionInfo().getProperty("AUTH_SESSION_ID");
					String serialNum = ((oracle.jdbc.internal.OracleConnection)oConn).getServerSessionInfo().getProperty("AUTH_SERIAL_NUM");
					String serverPid = ((oracle.jdbc.internal.OracleConnection)oConn).getServerSessionInfo().getProperty("AUTH_SERVER_PID");

					log.info("Database Consumer Session Info: "+ sessionId +","+serialNum+". Process Id " + serverPid +" Instance Name "+ instanceName);

					try {
						this.dbVersion = ConnectionUtils.getDBVersion(oConn);
						this.lightWeightSub = configs.getBoolean(ConsumerConfig.ORACLE_CONSUMER_LIGHTWEIGHT);
							
					}catch(Exception e)
					{
						log.error("Exception whle fetching DB Version and lightweight consumer config" + e);
					}

				}catch(Exception e)
				{
					log.error("Exception wnile getting database session information " + e);
				}

				node.setId(instId);
				node.setService(serviceName);
				node.setInstanceName(instanceName);
				node.setUser(user);
				node.updateHashCode();
			}catch(Exception e)
			{
				log.error("Exception while getting instance id from conneciton " + e, e);
			}

			topicSubscribers = new HashMap<>();
		}
		/**
		 * Creates topic connection to node
		 * @param node destination to which connection is needed
		 * @return established topic connection
		 * @throws JMSException
		 */
		public TopicConnection createTopicConnection(Node node) throws JMSException {
			if(conn == null)
				conn = ConnectionUtils.createTopicConnection(node, configs, log);
			return conn;
		}

		public TopicSubscriber getTopicSubscriber(String topic) throws JMSException {
			TopicSubscriber subscriber = topicSubscribers.get(topic);
			if(subscriber == null)
				subscriber = createTopicSubscriber(topic);
			return subscriber;
		}

		/**
		 * Creates topic session from established connection
		 * @param mode mode of acknowledgement with which session has to be created
		 * @return created topic session
		 * @throws JMSException
		 */
		public TopicSession createTopicSession(int mode) throws JMSException {
			if(sess != null) 
				return sess;			
			sess= ConnectionUtils.createTopicSession(conn, mode, true);
			conn.start();
			return sess;

		}

		/**
		 * Creates topic consumer for given topic
		 */
		private TopicSubscriber createTopicSubscriber(String topic) throws JMSException {
			refresh(node);
			Topic dest = ((AQjmsSession)sess).getTopic((node!=null&&node.user()!=null)?node.user():ConnectionUtils.getUsername(configs), topic); 
			TopicSubscriber subscriber = sess.createDurableSubscriber(dest, configs.getString(ConsumerConfig.GROUP_ID_CONFIG));
			topicSubscribers.put(topic, subscriber);
			return subscriber;
		}
		
		private void createLightWeightSub(String topic, Node node)  { 
			try {		 
				CallableStatement cStmt = getOrCreateCallable(node, "CREATE_LTWT_SUB", LTWT_SUB);
				cStmt.setString(1, ConnectionUtils.enquote(topic));
				cStmt.setString(2, configs.getString(ConsumerConfig.GROUP_ID_CONFIG));
				cStmt.execute();
				log.debug("Lightweight subscriber created for topic: " + topic + ", node: " + node);
			} 
			catch(Exception ex) { 
				log.error("Error creating lightweight subscriber for topic: " + topic + ", node: " + node, ex);
			}
		}

		private void refresh(Node node) throws JMSException {
			conn = createTopicConnection(node);
			sess = createTopicSession(TopicSession.AUTO_ACKNOWLEDGE);
		}
		public TopicConnection getConnection() {
			return conn;
		}

		public TopicSession getSession() {
			return sess;
		}

		public Connection getDBConnection() throws JMSException
		{
			if(sess == null)
				return null;
			else
				return ((AQjmsSession)sess).getDBConnection();
		}

		public Map<String, TopicSubscriber> getTopicSubscriberMap() {
			return topicSubscribers;
		}

		public void setSession(TopicSession sess) {
			this.sess = sess;
		}

		public void setConnection(TopicConnection conn) {
			this.conn = conn;
		}

		public void remove(String topic) {
			topicSubscribers.remove(topic);
		}

		public String getDBVersion()
		{
			return dbVersion;
		}
		
		public boolean getlightWeightSub() {
			return lightWeightSub;
		}

	}

}
