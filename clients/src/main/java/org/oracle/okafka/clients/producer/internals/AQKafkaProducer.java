/*
 ** OKafka Java Client version 23.4.
 **
 ** Copyright (c) 2019, 2024 Oracle and/or its affiliates.
 ** Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.
 */

package org.oracle.okafka.clients.producer.internals;


import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

import javax.jms.BytesMessage;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicSession;
import javax.jms.TopicPublisher;

import oracle.jms.AQjmsBytesMessage;
import oracle.jms.AQjmsConnection;
import oracle.jms.AQjmsConstants;
import oracle.jms.AQjmsException;
import oracle.jms.AQjmsProducer;
import oracle.jms.AQjmsSession;

import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.oracle.okafka.clients.Metadata;
import org.oracle.okafka.clients.NetworkClient;
import org.oracle.okafka.clients.TopicTeqParameters;
import org.oracle.okafka.clients.producer.ProducerConfig;
import org.oracle.okafka.clients.producer.internals.OracleTransactionManager.TransactionState;
import org.oracle.okafka.common.Node;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.NotLeaderForPartitionException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.metrics.Metrics;
//import org.apache.kafka.common.network.Selector.SelectorMetrics;
import org.oracle.okafka.common.network.AQClient;
import org.oracle.okafka.common.network.SelectorMetrics;
import org.oracle.okafka.common.protocol.ApiKeys;
import org.oracle.okafka.common.requests.MetadataResponse;
import org.oracle.okafka.common.requests.ProduceRequest;
import org.oracle.okafka.common.requests.ProduceResponse;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Record;
import org.oracle.okafka.common.utils.ConnectionUtils;
import org.oracle.okafka.common.utils.MessageIdConverter;
import org.oracle.okafka.common.utils.MessageIdConverter.OKafkaOffset;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;

/**
 * This class sends messages to AQ
 */
public final class AQKafkaProducer extends AQClient {

	//Holds TopicPublishers of each node. Each TopicPublisher can contain a connection to corresponding node, session associated with that connection and topic publishers associated with that session
	private final Map<Node, TopicPublishers> topicPublishersMap;
	private final ProducerConfig configs;
	private final Time time;
	private Metadata metadata; 
	private final Metrics metrics;
	private final SelectorMetrics selectorMetrics;
	private final int DLENGTH_SIZE = 4;
	private boolean transactionalProducer = false;
	private boolean idempotentProducer = false;
	private int connectMode = AQjmsSession.AUTO_ACKNOWLEDGE;
	private Connection dbConn = null;
	private Connection externalDbConn = null;
	private AQKafkaProducerStatus status = AQKafkaProducerStatus.PRE_INIT;
	private OracleTransactionManager oTxm = null;

	// Flags for testing only. Do not set any of these to true
	static boolean forceRollback = false;
	static boolean forceRetry = false;
	static boolean forceDisconnect = false;
	static boolean stopReconnect = false;


	/* To Test
	 *   1. forceRollback to cause messages to be rolled back. Message Id won't exist in the table.
	 *   1.1 Retry here with same connection and confirm that message id is not found and on retry we are able to publish the messages in the same sendTOAQ call
	 *   1.2 Retry here after disconnecting the current connection. This emulates session crashing and failing to produce. Verify that we are able to connect on next attempt and reproduce
	 *   1.3 Disconnect the connection and do not re-attempt here. This emulates instance crashing for a while. Verify that the batch gets re-enqueued and we are able to publish it again.
	 *   
	 *   2. forceRollback is false hence messages are persistently stored in the table.
	 *   2.1 Retry here with same connection and confirm that message id is found and on retry we are not publishing messages again.
	 *   2.2 Retry here after disconnecting the current connection. This emulates session crashing after successful produce.
	 *   	 Verify that we are able to connect on next attempt and confirm that messages are reproduced
	 *   2.3 Disconnect the connection and do not re-attempt here. This emulates instance crashing for a while after successful enqueue. 
	 *   	 Verify that the batch gets re-enqueued and on retry we confirm that the message exists in the system and they are not produced again. 
	 */

	private enum AQKafkaProducerStatus
	{
		PRE_INIT,
		INIT,
		OPEN,
		CLOSE
	}

	private HashMap<TopicPartition, OKafkaOffset> currentOffsetMap = null;
	//	private final SelectorMetrics sensors;

	public AQKafkaProducer(LogContext logContext, ProducerConfig configs, Time time, Metadata _metadata, Metrics metrics, OracleTransactionManager txM)
	{   
		super(logContext.logger(AQKafkaProducer.class), configs);
		this.configs = configs;
		this.time = time;
		this.topicPublishersMap = new HashMap<Node, TopicPublishers>();
		this.metadata = _metadata;
		this.oTxm = txM;
		this.metrics=metrics;
		this.selectorMetrics = new SelectorMetrics(this.metrics, "Selector", Collections.<String, String>emptyMap(),true);
		this.selectorMetrics.recordConnectionCount(topicPublishersMap);;

		try
		{
			transactionalProducer = configs.getBoolean(ProducerConfig.ORACLE_TRANSACTIONAL_PRODUCER);
		}catch(Exception e) {
			transactionalProducer = false;
		}

		try {
			idempotentProducer = configs.getBoolean(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG);
			if(idempotentProducer)
			{
				connectMode = AQjmsSession.SESSION_TRANSACTED;
			}
		}catch(Exception e)
		{
			idempotentProducer = false;
		}

		if(transactionalProducer)
		{
			connectMode = AQjmsSession.SESSION_TRANSACTED;
			currentOffsetMap = new HashMap<TopicPartition, OKafkaOffset>();
		}
		status = AQKafkaProducerStatus.INIT;
	}

	synchronized public void setExternalDbConnection (Connection conn)
	{
		log.debug("Setting externally supplied db connection " + conn);
		if(oTxm.getTransactionState() == TransactionState.BEGIN)
		{
			throw new KafkaException("A transaction with another oracle connection already active.");
		}
		oTxm.setDBConnection(externalDbConn);
		this.externalDbConn = conn;
	}

	private void addToMap(Node node,TopicPublishers nodePublishers ) throws JMSException
	{
		topicPublishersMap.put(node, nodePublishers);
		log.info("Connected nodes: "+topicPublishersMap.keySet());
		status = AQKafkaProducerStatus.OPEN;
		selectorMetrics.maybeRegisterConnectionMetrics(node);
		selectorMetrics.connectionCreated.record();
		log.debug("CONNECTED NODES: "+topicPublishersMap.keySet());
	}
	
	private void connect(Node node, Connection con) throws JMSException {
		TopicPublishers nodePublishers = null;
		try {
			log.debug("Creating new Topic connection for node " + node);
			nodePublishers = new TopicPublishers(node, con);
			addToMap(node, nodePublishers);
		}catch(JMSException e) {
			close(node, nodePublishers);
			throw e;
		}	
	}


	public void connect(Node node) throws JMSException {
		TopicPublishers nodePublishers = null;
		try {
			log.debug("Creating new connection for node " + node);
			nodePublishers = new TopicPublishers(node, connectMode);
			addToMap(node, nodePublishers);
		}catch(JMSException e) {
			close(node, nodePublishers);
			throw e;
		}	          	
	}

	public boolean isChannelReady(Node node) {
		if(topicPublishersMap.containsKey(node)) {
			return true;
		}
		return false;
	}

	/* If Database Connection was externally created then pass it.
	 * If no external connection provided, then look for internally created database connection.
	 * If internal database connection is not created then create one and cache it. Use it for all publish calls.
	 */
	public Connection getDBConnection(boolean force) throws JMSException
	{
		if(externalDbConn != null)
		{
			log.debug("Returning externally supplied db connection. " + externalDbConn);
			return externalDbConn;
		}

		if(dbConn != null)
		{
			log.debug("Returning already created db connection. " + dbConn);
			return dbConn;
		}

		//Database connection is not established yet. If not forced to do so, return null 
		if(!force)
		{
			log.debug(" Database connection not established yet. Not forced to create one. Returning null.");
			return null;
		}

		try {
			if(topicPublishersMap == null || topicPublishersMap.isEmpty())
			{
				org.apache.kafka.common.Node bootStrapNode = metadata.fetch().nodes().get(0);
				log.debug("Transactional producer trying to connect to BootstrapNode. " + bootStrapNode );
				//ToDo: NetworkClient.initConnection() should be invoked ideally.
				this.connect((Node)bootStrapNode);
			}
			Cluster clusterNow = metadata.fetch();
			Node leaderNode = metadata.getLeader();
			log.debug("Leader node is " + leaderNode);

			Node controllerNode = (Node)clusterNow.controller();
			log.debug("Controller Node " + controllerNode);

			if(controllerNode == null)
			{
				boolean isBootStrap =  metadata.fetch().isBootstrapConfigured();
				if(isBootStrap)
					controllerNode = (Node)metadata.fetch().controller();
				else
					controllerNode = (Node)metadata.fetch().nodes().get(0);

				metadata.setLeader(controllerNode);
				log.debug("getDBConnection: Controller Node and LeaderNode set to " +  controllerNode);
			}
			TopicPublishers topicPbs =  topicPublishersMap.get(controllerNode);
			dbConn = ((AQjmsSession)topicPbs.sess).getDBConnection();
			return dbConn;
		}
		catch(Exception e)
		{
			log.info("Faiiled to create database connection for transactional producer. Exception: " + e );
			throw e;
		}
	}

	public Future<RecordMetadata> transactionalSend(TopicPartition tp, byte[] serializedKey, byte[] serializedValue, 
			Header[] headers, Callback interceptCallback) 
	{
		ProduceRequestResult produceResult = null;
		FutureRecordMetadata frm  = null;
		RuntimeException publishException = null;
		OKafkaOffset thisOffset = null;

		log.debug("Message for TopicPartition " + tp);
		try {
			if(topicPublishersMap == null || topicPublishersMap.isEmpty())
			{
				org.apache.kafka.common.Node bootStrapNode = null;
				if(externalDbConn != null)
				{
					log.debug("");
					int instId = ConnectionUtils.getInstanceId(externalDbConn);
					bootStrapNode =  metadata.fetch().nodeById(instId);
					if(bootStrapNode == null )
					{
						//if(metadata.isBootstrap())
						{
							bootStrapNode = metadata.fetch().nodes().get(0);
							((Node)bootStrapNode).setId(instId);
							log.debug("External DB + BOotStrap Node " + bootStrapNode);
						}
						log.warn("Connection setup to instance "+ instId + ". Which is not found in current cluster");
					}
					//else 
					{
						log.debug("Created publisher using externally supplied database connection to instance " + instId);
						this.connect((Node)bootStrapNode, externalDbConn);
					}
				}

				if(bootStrapNode == null) {
					bootStrapNode = metadata.fetch().nodes().get(0);
					log.debug("Creating using bootstrapnode " + bootStrapNode );
					this.connect((Node)bootStrapNode);
				}
				metadata.setLeader((Node)bootStrapNode);
			}

			Cluster clusterNow = metadata.fetch();
			Node controllerNode = (Node)clusterNow.controller();
			log.debug("Controller " + controllerNode);
			Node leaderNode = metadata.getLeader();
			log.debug("Leader Node " + leaderNode);

			TopicPublishers topicPbs = null;
			if(leaderNode != null)
				topicPbs =  topicPublishersMap.get(leaderNode);
			else
				topicPbs =  topicPublishersMap.get(controllerNode);

			log.debug("Available Topic Publishers " + topicPbs);
			if(topicPbs == null && topicPublishersMap.size() > 0) // Get first connected node
			{
				for(Map.Entry<Node, TopicPublishers> connectedPubEntry : topicPublishersMap.entrySet())
				{
					metadata.setLeader(connectedPubEntry.getKey());
					topicPbs = connectedPubEntry.getValue();
					break;
				}
			}
			TopicPublisher tps = topicPbs.getTopicPublisher(tp.topic());
			Connection conn = ((AQjmsSession)topicPbs.getSession()).getDBConnection();

			oTxm.setDBConnection(conn);

			TopicTeqParameters topicTeqParam = metadata.topicParaMap.get(tp.topic());
			if(topicTeqParam == null)
			{
				try {
					super.fetchQueueParameters(tp.topic(), conn, metadata.topicParaMap);
				} catch (SQLException e) {
					log.error("Exception while fetching TEQ parameters and updating metadata " + e.getMessage());
				}
			}
			int msgVersion = topicTeqParam.getMsgVersion();

			BytesMessage byteMessage = createBytesMessage(topicPbs.sess, tp, 

					serializedKey != null ? ByteBuffer.wrap(serializedKey) : null, 
							serializedValue != null ? ByteBuffer.wrap(serializedValue) : null , headers, msgVersion);

			try {
				tps.publish(byteMessage, DeliveryMode.PERSISTENT, 0, AQjmsConstants.EXPIRATION_NEVER);
			}catch(JMSException e)
			{
				log.error("Exception while producing transactionl message " + e.getMessage());
				publishException = new RuntimeException(e);
			}

			if(publishException == null)
			{
				OKafkaOffset prevOffset = currentOffsetMap.get(tp);
				thisOffset =  MessageIdConverter.computeOffset(prevOffset, byteMessage.getJMSMessageID());
				currentOffsetMap.remove(tp);
				currentOffsetMap.put(tp, thisOffset);
			}
			else { 
				//Create Empty or Invalid Offset
				thisOffset= MessageIdConverter.getOKafkaOffset("",false,false);
			}

			produceResult = new ProduceRequestResult(tp);

			produceResult.set(thisOffset.subPartitionId(), (publishException==null)?byteMessage.getJMSTimestamp():-1, 
					Collections.singletonList(thisOffset), publishException);

			frm = new FutureRecordMetadata(produceResult, 0, System.currentTimeMillis(),
					serializedKey != null ? serializedKey.length : 0, 
							serializedValue != null ? serializedValue.length : 0 ,time );


			produceResult.done();
			this.oTxm.addRecordToTransaction(frm);

		}catch(Exception e)
		{
			log.error("Error while publishing records within a transaction." + e.getMessage(), e);
			produceResult = new ProduceRequestResult(tp);
			produceResult.set(-1L, -1L, null, new RuntimeException(e));
			frm = new FutureRecordMetadata(produceResult, -1l, System.currentTimeMillis(),
					serializedKey != null ? serializedKey.length : 0,
							serializedValue != null ? serializedValue.length : 0 ,time );

			produceResult.done();
		}
		return frm;
	}

	public ClientResponse send(ClientRequest request) {
		ClientResponse cr = parseRequest(request, ApiKeys.convertToOracleApiKey(request.apiKey()));	
		selectorMetrics.recordCompletedReceive(cr.destination(), cr.requestLatencyMs());
		return cr;
	}

	/**
	 * Determines the type of request and calls appropriate method for handling request
	 * @param request request to be sent
	 * @param key uniquely identifies type of request.
	 * @return response for given request
	 */
	private ClientResponse parseRequest( ClientRequest request, ApiKeys key) {
		if(key == ApiKeys.PRODUCE) 
			return publish(request);
		if(key == ApiKeys.METADATA)
			return getMetadata(request);
		return null;

	}

	/**
	 *Unwraps memory records of a producer batch into records. 
	 *Then translates each record into AQjmsBytesMessage and sends them to database instance as AqjmsBytesMessage array.
	 *Returns response for all messages in a memory records.
	 */
	private ClientResponse publish(ClientRequest request) {
		ProduceRequest.Builder builder = (ProduceRequest.Builder)request.requestBuilder();
		ProduceRequest produceRequest = builder.build();
		Node node = metadata.getNodeById(Integer.parseInt(request.destination()));
		TopicPartition topicPartition = produceRequest.getTopicpartition();
		MemoryRecords memoryRecords = produceRequest.getMemoryRecords();
		TopicPublishers nodePublishers = null;
		AQjmsBytesMessage[] msgs =null;
		ProduceResponse.PartitionResponse partitionResponse = null;

		//TopicPublishers allPublishers = null;
		TopicPublisher publisher = null;
		int retryCnt = 2; 
		AQjmsBytesMessage byteMessage  = null;
		TopicTeqParameters topicTeqParam = metadata.topicParaMap.get(topicPartition.topic());
		long batchSize=memoryRecords.sizeInBytes();
		int msgVersion = topicTeqParam.getMsgVersion();

		boolean checkForCommit = false;
		boolean disconnected = false;
		boolean notALeader = false;
		Exception pException = null;

		log.debug("Publish request for node " + node);

		try {
			if(topicTeqParam.getKeyBased() != 2) {
				String errMsg = "Topic " + topicPartition.topic() + " is not an Oracle kafka topic, Please drop and re-create topic"
						+" using Admin.createTopics() or dbms_aqadm.create_database_kafka_topic procedure";
				throw new InvalidTopicException(errMsg);
			}
		}
		catch(InvalidTopicException e) {
			log.error("Cannot send messages to topic " + topicPartition.topic() + ". Not a kafka topic");
			partitionResponse =  createResponses(topicPartition, e, msgs);
			//			selectorMetrics.recordCompletedReceive(request.destination(), batchSize, System.currentTimeMillis());
			return createClientResponse(request, topicPartition, partitionResponse, disconnected);
		}

		do
		{
			disconnected = false;
			checkForCommit = false;
			notALeader = false;
			pException = null;
			retryCnt--;

			try  {
				nodePublishers = topicPublishersMap.get(node);
				if(nodePublishers == null)
				{
					throw new NullPointerException("No publishers created for node " + node);
				}
				log.debug("Found a publisher " + nodePublishers +" for node " + node);
				TopicSession session = nodePublishers.getSession();

				if(idempotentProducer)
				{
					String checkMsgId = null;
					try {
						if(produceRequest.checkForDups())
						{
							Connection dbConn = ((AQjmsSession)session).getDBConnection();
							List<OKafkaOffset> retryMsgIds = produceRequest.retryMsgList();

							if(retryMsgIds != null && retryMsgIds.size()  > 0)
							{
								checkMsgId = retryMsgIds.get(0).getMsgId().substring(3);
								log.debug("Duplicate Check for parition " + topicPartition + "for msgId  " + checkMsgId);
							}

							boolean msgIdExist = checkIfMsgIdExist(dbConn, topicPartition.topic(), checkMsgId);
							if(msgIdExist)
							{
								log.info("Message Id " +checkMsgId +" exists for topic partition "+ topicPartition+". Records were succesfully produced.");
								partitionResponse = createResponses(topicPartition, null, null);
								partitionResponse.setCheckDuplicate(false);
								partitionResponse.setOffsets(retryMsgIds);
								return createClientResponse(request, topicPartition, partitionResponse, false);
							}
							else
							{
								log.info("Message Id " + checkMsgId +" exists for topic partition "+ topicPartition +" does not exist. Retrying to publish");
							}
						}

					}catch(Exception e) 
					{
						log.error("Exception while checking for duplicates for topic partition " + topicPartition +" message id " + checkMsgId +" Exception : "+ e ,e);
						checkForCommit = produceRequest.checkForDups();
						throw e;
					}
				}

				final List<AQjmsBytesMessage> messages = new ArrayList<>();	
				Iterator<MutableRecordBatch> mutableRecordBatchIterator = memoryRecords.batchIterator();
				while(mutableRecordBatchIterator.hasNext()) {
					Iterator<Record>  recordIterator = mutableRecordBatchIterator.next().iterator();
					while(recordIterator.hasNext()) {
						Record record = recordIterator.next();
						byteMessage = createBytesMessage(session, topicPartition, record.key(), record.value(), record.headers(), msgVersion);
						messages.add(byteMessage);
					}
				}

				publisher = nodePublishers.getTopicPublisher(topicPartition.topic());
				msgs = messages.toArray(new AQjmsBytesMessage[0]);

				log.trace("sending messages to topic : {} with partition: {}, number of messages: {}", topicPartition.topic(), topicPartition.partition(), msgs.length);

				sendToAQ(msgs, publisher);
				if(idempotentProducer)
				{
					try {
						//Session must be a transacted session. 
						log.trace("Idempotent Producer. Committing with node " + node);

						if(forceRollback) {
							nodePublishers.sess.rollback();
							forceRetry = true;
							forceRollback = false;
						}
						else
						{
							nodePublishers.sess.commit();
						}

						if(forceDisconnect) {
							nodePublishers.sess.close();
							forceRetry  = true;
							forceDisconnect = false;
						}

						if(forceRetry)
						{
							forceRetry = false;
							throw new KafkaException("Dummy Exception");
						}
					}catch(Exception e)
					{
						log.error("Exception while committing records " + e.getMessage());
						checkForCommit = true;
						throw e;
					}
				}
				selectorMetrics.recordCompletedSend(request.destination(),batchSize, System.currentTimeMillis());
				log.trace("Messages sent successfully to topic : {} with partition: {}, number of messages: {}", topicPartition.topic(), topicPartition.partition(), msgs.length);
				retryCnt = 0;
			}
			catch(Exception e) {

				pException = e;

				if(!checkForCommit) {
					log.error("Exception while sending records for topic partition " + topicPartition + " no node " + node , e);
				}
				else {
					log.error("Exception while committing records for topic partition " + topicPartition + " no node " + node , e);
				}

				if ( e instanceof JMSException) {
					log.info(" Encountered JMS Exception:" + e.getMessage() );
					// This exception is thrown from sever when AQ tries to publish into a partition which is not owned by the connected node
					if( (e instanceof AQjmsException ) && ((AQjmsException)e).getErrorNumber() == 25348 )
					{
						notALeader = true;
						retryCnt = 0;
						break;
					}
				}
				if(nodePublishers != null)
				{
					boolean connected = nodePublishers.isConnected();
					log.info("KafkaProducer is connected to the broker? " + connected);
					// Database connection used to publish the records is terminated.
					if(!connected )
					{
						try {
							nodePublishers.close();
							if( !stopReconnect && retryCnt > 0)
							{
								log.info("Reconnecting to node " + node);
								boolean reCreate = nodePublishers.reCreate();
								if(!reCreate) {
									log.info("Failed to reconnect to  " + node +" . Failing this batch for " + topicPartition);
									disconnected = true;
								}
							}else {
								disconnected = true;
								log.info("Failed to reconnect to  " + node +" . Failing this batch for " + topicPartition);
							}
							stopReconnect = false;

						}catch(Exception reConnException)
						{
							log.error("Exception while reconnecting to node " + node , reConnException);
							disconnected = true;
							retryCnt = 0;
							try {
								// Close again just to be sure that we are not leaking connections.
								nodePublishers.close();  
							}catch(Exception ignoreExcp) {}
						}
					}
					if(checkForCommit)
					{
						if(!disconnected ) {
							//Re-connected to the same database instance after one retry
							try {
								log.debug("Connection to node is fine. Checking if previous publish was successfull or not.");
								nodePublishers = topicPublishersMap.get(node);
								java.sql.Connection conn = ((AQjmsSession)nodePublishers.sess).getDBConnection();
								String msgId = msgs[0].getJMSMessageID().substring(3);
								boolean msgIdExists = checkIfMsgIdExist(conn, topicPartition.topic(), msgId);
								checkForCommit = false;
								if(msgIdExists) {
									//successfully produced the message
									log.debug("Message Id " + msgId+" already present in for " + topicPartition + ". No need to retry.");
									retryCnt = 0;
									pException = null;
								}
								else {
									/* DO Nothing. 
									 * Producer is successfully connected to the database node.
									 * It will retry one more time to produce.
									 */
								}
							}
							catch(Exception msgIdExcp) 
							{
								log.info("Exception while checking if message id exists or not " + msgIdExcp);
								log.info("Batch will be processed again after checking for duplicates.");
								checkForCommit = true;
								retryCnt=0;
							}
						}
						else {
							log.info("Node " + node + " is not reachable. Batch will be reprocessed after checking for duplicates.");
							retryCnt=0;
						}
					}
				}
			}
		}while(retryCnt > 0);
		if(pException != null)
		{
			if(notALeader)
			{
				log.info("Node "+ node +" is not a Leader for partition " + topicPartition );
				partitionResponse =  createResponses(topicPartition, new NotLeaderForPartitionException(pException), msgs);  
				this.metadata.requestUpdate();
			}
			if(disconnected)
			{
				TopicPublishers tpRemoved = topicPublishersMap.remove(node);
				log.trace("Connection with node {} is closed", request.destination());
				String exceptionMsg = "Database instance not reachable: " + node;
				org.apache.kafka.common.errors.DisconnectException disconnExcp = new org.apache.kafka.common.errors.DisconnectException(exceptionMsg,pException);
				partitionResponse =  createResponses(topicPartition, disconnExcp, msgs);
			}
		}
		else 
		{
			partitionResponse = createResponses(topicPartition, null, msgs);
		}
		partitionResponse.setCheckDuplicate(checkForCommit);
		return createClientResponse(request, topicPartition, partitionResponse, disconnected);
	}

	private void dumpTopicPublishers()
	{
		if(this.topicPublishersMap == null)
			log.info("TopicPublisherMap is null");
		else
			log.info("TopicPublisherMap size " +topicPublishersMap.size() );

		for(Node n: topicPublishersMap.keySet()) {
			log.info("Publihsers for Node " + n);
			log.info(topicPublishersMap.get(n).toString());
		}

	}

	private boolean checkIfMsgIdExist(Connection con,String topicName, String msgId)
	{
		boolean msgIdExists = false;
		String qry =" Select count(*) from " +ConnectionUtils.enquote(topicName) + " where msgid = '" + msgId+"'";
		log.debug("Executing " + qry);
		ResultSet rs = null;
		try (Statement stmt = con.prepareCall(qry);) {
			stmt.execute(qry);
			rs = stmt.getResultSet();
			if(rs.next())
			{
				int msgCnt = rs.getInt(1);

				if(msgCnt == 0)
				{
					msgIdExists = false;
				}
				else
					msgIdExists = true;
			}
			else {
				msgIdExists = false;
			}
			rs.close();
			rs = null;

		}catch(Exception e)
		{
			log.info("Exception while checking if msgId Exists or not. " + e,e);
			if(rs!=null)
			{
				try { 
					rs.close();
				}catch(Exception ignoreE) {}
			}
		}
		log.debug("Message Id "+  msgId +" Exists?: " + msgIdExists);
		return msgIdExists;
	}


	private ClientResponse createClientResponse(ClientRequest request, TopicPartition topicPartition, ProduceResponse.PartitionResponse partitionResponse, boolean disconnected) {
		return  new ClientResponse(request.makeHeader((short)1), request.callback(), request.destination(), 
				request.createdTimeMs(), time.milliseconds(), disconnected, null,null,
				new ProduceResponse(topicPartition, partitionResponse));
	}

	/**
	 * Bulk send messages to AQ.
	 * @param messages array of AQjmsBytesmessage to be sent
	 * @param publisher topic publisher used for sending messages
	 * @throws JMSException throws JMSException
	 */
	private void sendToAQ(AQjmsBytesMessage[] messages, TopicPublisher publisher) throws JMSException {
		//Sends messages in bulk using topic publisher;
		log.info("In BulkSend: #messages = " + messages.length);
		((AQjmsProducer)publisher).bulkSend(publisher.getTopic(), messages);
	}

	/**
	 * Creates AQjmsBytesMessage from ByteBuffer's key, value and headers
	 */
	private AQjmsBytesMessage createBytesMessage(TopicSession session, TopicPartition topicPartition, 
			ByteBuffer key, ByteBuffer value, Header[] headers, int messageVersion) throws JMSException {

		AQjmsBytesMessage msg=null;
		if(messageVersion == 2) {
			msg = createBytesMessageV2(session,topicPartition,key, value, headers);
		}
		else {
			msg = createBytesMessageV1(session,topicPartition,key, value, headers);
		}
		return msg;
	}

	/**
	 * 
	 * Creates AQjmsBytesMessage from ByteBuffer's key, value and headers in V1 version
	 * In V1 version, Key is stored as correlation ID.
	 */
	private AQjmsBytesMessage createBytesMessageV1(TopicSession session, TopicPartition topicPartition, 
			ByteBuffer key, ByteBuffer value, Header[] headers) throws JMSException {

		AQjmsBytesMessage msg = (AQjmsBytesMessage)(session.createBytesMessage());

		if(key!=null) {
			byte[] keyByteArray  = new byte[key.limit()];
			key.get(keyByteArray);
			msg.setJMSCorrelationID(new String(keyByteArray));
		}

		byte[] payload = new byte[value.limit()];
		value.get(payload);
		msg.writeBytes(payload);
		payload = null;
		msg.setStringProperty("topic", topicPartition.topic());
		msg.setStringProperty(AQClient.PARTITION_PROPERTY, Integer.toString(topicPartition.partition()*2));
		msg.setIntProperty(MESSAGE_VERSION, 1);

		return msg;
	}

	/*
	 * Creates AQjmsBytesMessage from ByteBuffer's key, value and headers in V2 version
	 * In V2 version, Key is stored as part of the message payload as described below.
	 * 
	 * Construct Byte Payload in below format:
	 * | KEY LENGTH (4 Bytes Fixed)          | KEY   |
	 * | VALUE LENGTH (4 BYTES FIXED)        | VALUE |
	 * | HEADER NAME LENGTH(4 BYTES FIXED)   | HEADER NAME |
	 * | HEADER VALUE LENGTH (4 BYTES FIXED) | HEADER VALUE |
	 * | HEADER NAME LENGTH(4 BYTES FIXED)   | HEADER NAME |
	 * | HEADER VALUE LENGTH (4 BYTES FIXED) | HEADER VALUE |
	 * 
	 * For records with null key , KEY LENGTH is set to 0.
	 * For records with null value, VALUE LENGTH is set to 0.
	 * Number of headers are set in property "AQINTERNAL_HEADERCOUNT"
	 * 
	 * 	*/
	private AQjmsBytesMessage createBytesMessageV2(TopicSession session, TopicPartition topicPartition, 
			ByteBuffer key, ByteBuffer value, Header[] headers) throws JMSException {

		AQjmsBytesMessage msg=null;
		int keyLen = 0;
		int valueLen =0;

		int hKeysLen[] = null;
		int hValuesLen[] = null;

		byte[] keyByteArray  = null;
		byte[] valueByteArray = null;


		if(headers != null)
		{
			hKeysLen = new int[headers.length];
			hValuesLen = new int[headers.length];
		}

		msg = (AQjmsBytesMessage)(session.createBytesMessage());

		int totalSize = 0;
		if(key != null) {

			keyByteArray =  new byte[key.limit()];
			key.get(keyByteArray);
			keyLen = keyByteArray.length;
		}

		totalSize += (keyLen + DLENGTH_SIZE );

		if(value != null) {
			valueByteArray = new byte[value.limit()];
			value.get(valueByteArray);
			valueLen = valueByteArray.length;

		}
		totalSize += (valueLen + DLENGTH_SIZE);

		if(headers != null) {
			int hIndex = 0;
			for(Header h:headers)
			{
				int hKeyLen = h.key().getBytes().length;
				totalSize += (hKeyLen + DLENGTH_SIZE);
				hKeysLen[hIndex] = hKeyLen;
				int hValueLength = h.value().length;
				totalSize += (hValueLength +DLENGTH_SIZE);
				hValuesLen[hIndex++] = hValueLength;
			}
		}
		ByteBuffer pBuffer = ByteBuffer.allocate(totalSize);

		//If Key is null Put Length = 0
		pBuffer.put(ConnectionUtils.convertTo4Byte(keyLen));
		if(keyLen > 0) {
			pBuffer.put(keyByteArray);
			msg.setJMSCorrelationID(new String(keyByteArray));
		}
		//If Value is null then put length = 0
		pBuffer.put(ConnectionUtils.convertTo4Byte(valueLen));
		if(valueLen > 0)
		{
			pBuffer.put(valueByteArray);
		}

		if(headers != null)
		{
			int hIndex = 0;
			for(Header h : headers)
			{
				pBuffer.put(ConnectionUtils.convertTo4Byte(hKeysLen[hIndex]));
				pBuffer.put(h.key().getBytes());
				pBuffer.put(ConnectionUtils.convertTo4Byte(hValuesLen[hIndex++]));
				pBuffer.put(h.value());
			}
		}

		pBuffer.rewind();
		byte[] payload = new byte[pBuffer.limit()];
		pBuffer.get(payload);
		msg.writeBytes(payload);
		payload = null;
		msg.setStringProperty(PARTITION_PROPERTY, Integer.toString(topicPartition.partition()*2));
		if(headers !=null)
		{
			msg.setIntProperty(HEADERCOUNT_PROPERTY, headers.length);
		}

		msg.setIntProperty(MESSAGE_VERSION, 2);

		return msg;
	}


	/**
	 * Creates response for records in a producer batch from each corresponding AQjmsBytesMessage data updated after send is done.
	 */
	private ProduceResponse.PartitionResponse createResponses(TopicPartition tp, RuntimeException exception, AQjmsBytesMessage[] msgs) {
		int iter=0;
		//Map<TopicPartition, ProduceResponse.PartitionResponse> responses = new HashMap<>();
		ProduceResponse.PartitionResponse response =new ProduceResponse.PartitionResponse(exception);

		//if(exception == null)
		if(msgs!= null)
		{
			response.msgIds = new ArrayList<>();
			//response.logAppendTime = new ArrayList<>();
			String msgId = null;
			long timeStamp = -1;
			long subPartitionId = -1;
			OKafkaOffset prevOKafkaOffset = null;

			while(iter<msgs.length) {
				try {
					msgId = msgs[iter].getJMSMessageID(); 
					timeStamp = msgs[iter].getJMSTimestamp();
					prevOKafkaOffset = MessageIdConverter.computeOffset(prevOKafkaOffset, msgId);

				} catch(Exception excp) {
					msgId = null;
					timeStamp = -1;
				}
				response.msgIds.add(prevOKafkaOffset);
				//response.logAppendTime.add(timeStamp);
				iter++;
			}
			response.logAppendTime = timeStamp;
			response.subPartitionId = subPartitionId;
		}
		//responses.put(new TopicPartition(tp.topic(), tp.partition()), response);		
		return response;
	}

	private ClientResponse getMetadata(ClientRequest request) {
		Connection conn = null;
		Node node = null;
		//Cluster used for this metadata is still a bootstrap cluster and does not have all necessary information
		//Pick any random node from the bootstrap nodes and send metadata request.
		if(metadata.isBootstrap())
		{
			Cluster cluster = metadata.fetch();
			List<Node> clusterNodes = NetworkClient.convertToOracleNodes(cluster.nodes());
			// Check if we have a node where connection already exists
			Set<Node> nodesWithConn = topicPublishersMap.keySet();
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
				log.info("No Connected Node Found. Picked first of bootstrap nodes.: " + node);
			}
		}
		else
		{
			node = (org.oracle.okafka.common.Node)metadata.getNodeById(Integer.parseInt(request.destination()));
		}
		try {
			TopicPublishers tpNode  = topicPublishersMap.get(node);
			if(tpNode != null)
			{
				conn = ((AQjmsSession)topicPublishersMap.get(node).getSession()).getDBConnection();
			}else {
				// Designated node does not have a connection. Find alternative. 
				for(TopicPublishers tPublishers: topicPublishersMap.values())
				{
					if(tPublishers.isConnected())
					{
						conn = ((AQjmsSession)tPublishers.getSession()).getDBConnection();
					}
				}

				if(conn == null)
				{
					log.info("Sender not connected to any node. Re-connecting.");
					List<Node> clusterNodes = NetworkClient.convertToOracleNodes(metadata.fetch().nodes());
					for(Node n : clusterNodes)
					{
						try {
							this.connect(n);
							log.info("Attempting to connect to " + n);
							conn = ((AQjmsSession)topicPublishersMap.get(n).getSession()).getDBConnection();
							log.info("Connected to node " + n);
							node = n;
							break;
						}catch(Exception e)
						{
							log.info(" Node {} not rechable", n);
						}
					}
				}
				/*if(conn == null)
					metadata.requestUpdate(); */
			}
		} catch(JMSException jms) {			
			try {
				log.trace("Unexcepted error occured with connection to node {}, closing the connection", request.destination());
				topicPublishersMap.get(metadata.getNodeById(Integer.parseInt(request.destination()))).getConnection().close();
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
			topicPublishersMap.remove(metadata.getNodeById(Integer.parseInt(request.destination())));
			metadata.requestUpdate();
		}
		return response;
	}

	/**
	 * Closes AQKafkaProducer
	 */
	public void close() {
		for(Map.Entry<Node, TopicPublishers> nodePublishers : topicPublishersMap.entrySet()) {
			close(nodePublishers.getKey(), nodePublishers.getValue());
		}
		topicPublishersMap.clear();
		status = AQKafkaProducerStatus.CLOSE;
	}

	//Close publishers for this node only
	public void close(Node node) {

		TopicPublishers tpNode = topicPublishersMap.get(node);
		close(node, tpNode);
	}

	public boolean isClosed()
	{
		if(status == AQKafkaProducerStatus.CLOSE)
			return true;
		return false;
	}
	/**
	 * Closes all connections, session associated with each connection  and all topic publishers associated with session.
	 */
	private void close(Node node, TopicPublishers publishers) {
		if( node == null || publishers == null)
			return ;

		for(Map.Entry<String, TopicPublisher> topicPublisher : publishers.getTopicPublisherMap().entrySet()) {
			try {
				topicPublisher.getValue().close();
			}catch(JMSException jms) {
				log.error("failed to close topic publisher for topic {} ", topicPublisher.getKey());
			}
		}
		try {
			publishers.getSession().close();
		} catch(JMSException jms) {
			log.error("failed to close session {} associated with connection {} and node {}  ",publishers.getSession(), publishers.getConnection(), node );
		}
		try {
			publishers.getConnection().close();
			this.selectorMetrics.connectionClosed.record();
		} catch(JMSException jms) {
			log.error("failed to close connection {} associated with node {}  ",publishers.getConnection(), node );
		}
	}

	/**This class is used to create and manage connection to database instance.
	 * Also creates, manages session associated with each connection and topic publishers associated with each session
	 */
	private final class TopicPublishers {
		private Connection externalConn;
		private Node node;
		private TopicConnection conn;
		private TopicSession sess;
		private Map<String, TopicPublisher> topicPublishers = null;
		private int sessionAckMode =  javax.jms.Session.AUTO_ACKNOWLEDGE;

		private boolean isAlive = false;
		PreparedStatement pingStmt = null;
		private final String PING_QUERY = "SELECT banner FROM v$version where 1<>1";

		private String connInfo = "";

		public TopicPublishers(Node node, Connection externalConn) throws JMSException {
			this.node = node;
			this.externalConn = externalConn;
			sessionAckMode = javax.jms.Session.SESSION_TRANSACTED;
			createPublishers(false);
			topicPublishers = new HashMap<>();
			log.debug("ExternalConnection " + externalConn);
		}

		public TopicPublishers(Node node) throws JMSException {
			this(node, TopicSession.AUTO_ACKNOWLEDGE);
		}
		public TopicPublishers(Node _node,int mode) throws JMSException {
			this.node = _node;
			this.sessionAckMode = mode;

			try {
				createPublishers(false);
				topicPublishers = new HashMap<>();
				/*
				Connection oConn = ((AQjmsSession)sess).getDBConnection();
				int instId = Integer.parseInt(((oracle.jdbc.internal.OracleConnection)oConn).getServerSessionInfo().getProperty("AUTH_INSTANCE_NO"));
				String serviceName = ((oracle.jdbc.internal.OracleConnection)oConn).getServerSessionInfo().getProperty("SERVICE_NAME");
				String instanceName = ((oracle.jdbc.internal.OracleConnection)oConn).getServerSessionInfo().getProperty("INSTANCE_NAME");
				String user = oConn.getMetaData().getUserName();

				try {
					String sessionId = ((oracle.jdbc.internal.OracleConnection)oConn).getServerSessionInfo().getProperty("AUTH_SESSION_ID");
					String serialNum = ((oracle.jdbc.internal.OracleConnection)oConn).getServerSessionInfo().getProperty("AUTH_SERIAL_NUM");
					String serverPid = ((oracle.jdbc.internal.OracleConnection)oConn).getServerSessionInfo().getProperty("AUTH_SERVER_PID");
					connInfo = "Session_Info:"+ sessionId +","+serialNum+". Process Id:" + serverPid +". Instance Name:"+instanceName;
					log.info("Database Producer "+connInfo);
				}catch(Exception ignoreE)
				{
				}

				node.setId(instId);
				node.setService(serviceName);
				node.setInstanceName(instanceName);
				node.setUser(user);
				node.updateHashCode();
				pingStmt = oConn.prepareStatement(PING_QUERY);
				pingStmt.setQueryTimeout(1);
				isAlive = true;*/
			}catch(Exception e)
			{
				log.error("Exception while getting instance id from conneciton " + e, e);
				throw e;
			}

		}

		public String toString()
		{
			String tpDesc =connInfo +". Acknowledge_mode:"+ sessionAckMode +".";
			if(topicPublishers!= null && topicPublishers.size() > 0)
			{
				String topicInfo ="Topics:[";
				boolean first = true;
				for(String topic : topicPublishers.keySet())
				{
					if(!first)
						topicInfo+=",";

					topicInfo += topic;

					first = false;
				}
				topicInfo+="].";
				tpDesc += topicInfo;
			}
			return tpDesc;
		}

		private boolean createPublishers(boolean reCreate) throws JMSException {
			try {
				conn = createTopicConnection();
				sess = createTopicSession(sessionAckMode);
				Connection oConn = ((AQjmsSession)sess).getDBConnection();

				int instId = Integer.parseInt(((oracle.jdbc.internal.OracleConnection)oConn).getServerSessionInfo().getProperty("AUTH_INSTANCE_NO"));
				String serviceName = ((oracle.jdbc.internal.OracleConnection)oConn).getServerSessionInfo().getProperty("SERVICE_NAME");
				String instanceName = ((oracle.jdbc.internal.OracleConnection)oConn).getServerSessionInfo().getProperty("INSTANCE_NAME");
				String user = oConn.getMetaData().getUserName();
				String sessionId = ((oracle.jdbc.internal.OracleConnection)oConn).getServerSessionInfo().getProperty("AUTH_SESSION_ID");
				String serialNum = ((oracle.jdbc.internal.OracleConnection)oConn).getServerSessionInfo().getProperty("AUTH_SERIAL_NUM");
				String serverPid = ((oracle.jdbc.internal.OracleConnection)oConn).getServerSessionInfo().getProperty("AUTH_SERVER_PID");
				connInfo = "Session_Info:"+ sessionId +","+serialNum+". Process Id:" + serverPid +". Instance Name:"+instanceName;
				log.info("Database Producer "+connInfo);

				if(reCreate)
				{
					node.setId(instId);
					node.setService(serviceName);
					node.setInstanceName(instanceName);
					node.setUser(user);
					node.updateHashCode();
				}
				pingStmt = oConn.prepareStatement(PING_QUERY);
				pingStmt.setQueryTimeout(1);
				isAlive = true;
			}catch(Exception setupException)
			{
				JMSException crPublisherException = new JMSException(setupException.getMessage());
				crPublisherException.setLinkedException(setupException);
				throw crPublisherException;
			}
			return true;
		}
		/**
		 * Creates topic connection to node
		 * @param node destination to which connection is needed
		 * @return established topic connection
		 * @throws JMSException
		 */
		public TopicConnection createTopicConnection() throws Exception {
			if(externalConn != null  && !externalConn.isClosed())
			{
				log.debug("Using External Connection to setup TopicConnection");
				conn = ConnectionUtils.createTopicConnection(externalConn, configs, log);
			}
			else
			{	
				conn = ConnectionUtils.createTopicConnection(node, configs, log);
			}
			return conn;
		}

		public TopicPublisher getTopicPublisher(String topic) throws JMSException {
			TopicPublisher publisher = topicPublishers.get(topic);
			if(publisher == null) {
				publisher = createTopicPublisher(topic);
				topicPublishers.put(topic, publisher);
			}
			return publisher;
		}

		/**
		 * Creates topic session from established connection
		 * @param mode mode of acknowledgement with which session has to be created
		 * @return created topic session
		 * @throws JMSException
		 */
		public TopicSession createTopicSession(int mode) throws JMSException {		
			if(sess != null && ((AQjmsSession)sess).isOpen()) 
				return sess;			

			boolean transactedSession = false;
			if(mode == AQjmsSession.SESSION_TRANSACTED)
			{
				transactedSession = true;
				mode = AQjmsSession.AUTO_ACKNOWLEDGE;
			}

			sess= ConnectionUtils.createTopicSession(conn, mode, transactedSession);
			conn.start();
			return sess;

		}

		/**
		 * Creates topic publisher for given topic
		 */
		private TopicPublisher createTopicPublisher(String topic) throws JMSException {
			Topic dest = ((AQjmsSession)sess).getTopic((node!=null&&node.user()!=null)?node.user():ConnectionUtils.getUsername(configs), topic);
			TopicPublisher publisher = sess.createPublisher(dest);
			return publisher;

		}
		public TopicConnection getConnection() {
			return conn;
		}

		public TopicSession getSession() {
			return sess;
		}

		public Map<String, TopicPublisher> getTopicPublisherMap() {
			return topicPublishers;
		}

		public boolean isConnected()
		{
			if(isAlive)
			{
				try
				{			
					pingStmt.executeQuery();
				}catch(Exception e)
				{
					log.error("Publishers to node {} Failed to connect.", node.toString());
					isAlive = false;
				}
			}
			return isAlive;
		}

		public boolean reCreate() throws JMSException
		{
			log.debug("Recreating TopicPublisher " + this.toString());
			close();
			boolean reCreateSucc = createPublishers(true);
			if(!reCreateSucc) {
				log.debug("Recreation failed");
				return false;
			}else {
				log.debug("Successfully recreated " + this.toString() );
			}
			try {
				Map<String, TopicPublisher> topicPublishersNew = new HashMap<String, TopicPublisher>();
				for(String topic:  topicPublishers.keySet())
				{
					try {
						TopicPublisher tpNew = createTopicPublisher(topic);
						topicPublishersNew.put(topic, tpNew);
					} catch(Exception e) 
					{
						log.error("Exception "+ e +" while re-creating publishers for topic " +topic + " for node" + node );		
					}
				}
				topicPublishers.clear();
				topicPublishers = topicPublishersNew;
			}catch(Exception e)
			{
				log.error("Exception "+ e +" while re-creating publishers for topic for node" + node );
			}
			isAlive = true;

			return isAlive;
		}

		public void close()
		{
			try {
				if(pingStmt != null )
				{
					if( !pingStmt.isClosed())
						pingStmt.close();

					pingStmt = null;
				}
			}catch(Exception e) {
				log.error("Error while closing ping statement for " + node);
				pingStmt = null;
			}
			try {
				if(sess != null)
				{
					if(((AQjmsSession)sess).isOpen())
						sess.close();

					sess = null;
				}
			}catch(Exception e)
			{
				log.error("Error while closing session for " + node);
				sess = null;
			}
			try {
				if(conn != null)
				{
					if( ((AQjmsConnection)conn).isOpen())
						conn.close();

					conn = null;
				}

			}catch(Exception e)
			{
				log.error("Error while closing connection for " + node);
			}
			isAlive = false;
		}

	}
}
