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

package org.oracle.okafka.clients.consumer.internals;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.jms.JMSException;

import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.CumulativeCount;
import org.apache.kafka.common.metrics.stats.CumulativeSum;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Meter;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.metrics.stats.WindowedCount;
import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.oracle.okafka.clients.KafkaClient;
import org.oracle.okafka.clients.Metadata;
import org.oracle.okafka.clients.NetworkClient;
import org.oracle.okafka.clients.consumer.KafkaConsumer.FetchManagerMetrics;
import org.oracle.okafka.clients.consumer.internals.SubscriptionState.FetchPosition;
import org.apache.kafka.clients.RequestCompletionHandler;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Assignment;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.GroupAssignment;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Subscription;
//import org.apache.kafka.clients.consumer.internals.SubscriptionState;
//import org.oracle.okafka.common.Cluster;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.oracle.okafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.oracle.okafka.common.internals.PartitionData;
import org.oracle.okafka.common.internals.SessionData;
import org.oracle.okafka.common.requests.CommitRequest;
import org.oracle.okafka.common.requests.CommitResponse;
import org.oracle.okafka.common.requests.ConnectMeRequest;
import org.oracle.okafka.common.requests.ConnectMeResponse;
import org.oracle.okafka.common.requests.FetchRequest;
import org.oracle.okafka.common.requests.FetchResponse;
import org.oracle.okafka.common.requests.JoinGroupRequest;
import org.oracle.okafka.common.requests.JoinGroupResponse;
import org.oracle.okafka.common.requests.OffsetResetRequest;
import org.oracle.okafka.common.requests.OffsetResetResponse;
import org.oracle.okafka.common.requests.SubscribeRequest;
import org.oracle.okafka.common.requests.SubscribeResponse;
import org.oracle.okafka.common.requests.SyncGroupRequest;
import org.oracle.okafka.common.requests.SyncGroupResponse;
import org.oracle.okafka.common.requests.UnsubscribeRequest;
import org.oracle.okafka.common.requests.UnsubscribeResponse;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import oracle.jms.AQjmsBytesMessage;

public class ConsumerNetworkClient {
	private static final int MAX_POLL_TIMEOUT_MS = 5000;
	private final Logger log;
	private final KafkaClient client;
	private final Metadata metadata;
	private final Time time;
	private final boolean autoCommitEnabled;
	private final int autoCommitIntervalMs;
	private long nextAutoCommitDeadline;
	private final long retryBackoffMs;
	private final int maxPollTimeoutMs;
	private final int requestTimeoutMs;
	private final int sesssionTimeoutMs;
	private final long defaultApiTimeoutMs;
	private final SubscriptionState subscriptions;
	private Set<String> subscriptionSnapshot;
	private boolean rejoin = false;
	private boolean needsJoinPrepare = true;
	private SessionData sessionData = null;
	private final List<ConsumerPartitionAssignor> assignors;
	private final List<AQjmsBytesMessage> messages = new ArrayList<>();
	private Node currentSession = null;
	String consumerGroupId;
	private final AQKafkaConsumer aqConsumer;
	private final ConsumerCoordinatorMetrics sensors;
	private long lastRebalanceStartMs = -1L;
    private long lastRebalanceEndMs = -1L;
	

	public ConsumerNetworkClient(
			String groupId,
			org.apache.kafka.common.utils.LogContext logContext,
			KafkaClient client,
			Metadata metadata,
			SubscriptionState subscriptions,
			List<ConsumerPartitionAssignor> assignors,
			boolean autoCommitEnabled,
			int autoCommitIntervalMs,
			Time time,
			long retryBackoffMs,
			int requestTimeoutMs,
			int maxPollTimeoutMs,
			int sessionTimeoutMs,
			long defaultApiTimeoutMs,
			AQKafkaConsumer aqConsumer,
			Metrics metrics) {
		this.consumerGroupId = groupId;
		this.log = logContext.logger(ConsumerNetworkClient.class);
		this.client = client;
		this.metadata = metadata;
		this.subscriptions = subscriptions;
		this.assignors = assignors;
		this.autoCommitEnabled = autoCommitEnabled;
		this.autoCommitIntervalMs = autoCommitIntervalMs;
		this.time = time;
		this.retryBackoffMs = retryBackoffMs;
		this.maxPollTimeoutMs = Math.min(maxPollTimeoutMs, MAX_POLL_TIMEOUT_MS);
		this.requestTimeoutMs = requestTimeoutMs;
		this.sesssionTimeoutMs = sessionTimeoutMs;
		//Snapshot of subscription. Useful for ensuring if all topics are subscribed.
		this.subscriptionSnapshot = new HashSet<>();
		this.defaultApiTimeoutMs = defaultApiTimeoutMs;
        this.aqConsumer = aqConsumer;
        this.sensors = new ConsumerCoordinatorMetrics(metrics, "consumer");
		if (autoCommitEnabled)
			this.nextAutoCommitDeadline = time.milliseconds() + autoCommitIntervalMs;
	}
	
	public Connection getDBConnection()
	{
		if(currentSession != null)
		{
			log.debug("Poll has been invoked. Return connection used to fetch the records");
			Connection conn = aqConsumer.getDBConnection(currentSession);
			if(conn == null)
			{
				((NetworkClient)client).initiateConnect(currentSession,0);
			}
		}
		else if(this.metadata.isBootstrap())
		{
			log.debug("No connection setup yet. Establishing connection to bootstrap node");
			List<org.apache.kafka.common.Node> bootStrapNodeList  = metadata.fetch().nodes();

			//Always Connect to Node 0.
			Node bootStrapNode = (org.oracle.okafka.common.Node)bootStrapNodeList.get(0);
			log.debug("Setting up connection to bootstrap node" +bootStrapNode.toString());
			((NetworkClient)client).initiateConnect(bootStrapNode,0);
			log.debug("Bootstrap node updated after connection " +bootStrapNode.toString());
			
			//Assigning currentSession to BootStrapNode. All further operation will go through the same session
			currentSession = bootStrapNode;
			metadata.setLeader(currentSession);
			// A connection was created before poll is invoked. This connection is passed on to the application and may be used for transactionala activity.
			// OKafkaConsumer must use this connection going forward. Hence it should not look for optimal database instance to consume records. Hence avoid connectMe call.
			aqConsumer.setSkipConnectMe(true);
		}
		
		else if(metadata.fetch().controller() != null)
		{
			log.debug("Fail-safe. Metadata has been updated but poll and connectMe are not invoked.");
			currentSession = (Node)metadata.fetch().controller();
			
			Connection conn = aqConsumer.getDBConnection(currentSession);
			if(conn == null)
			{
				((NetworkClient)client).initiateConnect(currentSession,0);
			}
			if(metadata.getLeader() == null)
			{
				metadata.setLeader(currentSession);
			}
			aqConsumer.setSkipConnectMe(true);
		}		
		else {
			log.warn("Unexpected state. Failed to fetch database connection.");
			return null;
		}
		
		return  aqConsumer.getDBConnection(currentSession);
	}

	/**
	 * Poll from subscribed topics.
	 * Each node polls messages from a list of topic partitions for those it is a leader.
	 * @param timeoutMs poll messages for all subscribed topics.
	 * @param fetchManagerMetrics for recording fetch Requests
	 * @return messages consumed.
	 */
	public List<AQjmsBytesMessage> poll(final long timeoutMs,FetchManagerMetrics fetchManagerMetrics)  {
		boolean retry = false;
		long pollStartTime = System.currentTimeMillis();
		long timeSpent = 0;
		do {
			retry = false;
			this.messages.clear();
			Map<Node, String> pollMap = getPollableMap();
			long now = time.milliseconds();
			RequestCompletionHandler callback = new RequestCompletionHandler() {
				public void onComplete(ClientResponse response) {
					//do nothing;
				}
			};
			log.debug("Polling for topics #" + pollMap.entrySet().size());
			for(Map.Entry<Node, String> poll : pollMap.entrySet()) {	
				Node node = poll.getKey();
				log.debug("Fetch Records for topic " + poll.getValue() + " from host " + node );
				String topic =  poll.getValue();
				if(metadata.topicParaMap.get(topic).getStickyDeq() != 2) {
					String errMsg = "Topic " + topic + " is not an Oracle kafka topic, Please drop and re-create topic"
							+" using Admin.createTopics() or dbms_aqadm.create_database_kafka_topic procedure";
					throw new InvalidTopicException(errMsg);				
				}
				if(!this.client.ready(node, now)) {
					log.debug("Failed to consume messages from node: {}", node);
					//ToDo: Retry poll to get new connection to same or different node.
					if(currentSession  != null && currentSession == node)
					{
						currentSession = null;
					}
				} else {
					ClientRequest request  = createFetchRequest(node, poll.getValue(), callback, requestTimeoutMs < timeoutMs ? requestTimeoutMs : (int)timeoutMs);
					ClientResponse response = client.send(request, now);
					fetchManagerMetrics.recordFetchLatency(response.requestLatencyMs());
					handleFetchResponse(response, timeoutMs);
					if (response.wasDisconnected())
						retry = true;

					break;
				}
			}
			timeSpent = System.currentTimeMillis() - pollStartTime;
		}while(retry &&  timeSpent < timeoutMs );
		
		return this.messages;
	}
	
	/**
	 * 
	 * @return map of <node , topic> . Every node is leader for its corresponding topic.
	 */
	private Map<Node, String> getPollableMap() {
		try {
			if(currentSession == null) {
				List<Node> nodeList = NetworkClient.convertToOracleNodes(metadata.fetch().nodes());
				
				//Use only one node if skipConnectMe is set
				if(nodeList.size() == 1 || aqConsumer.skipConnectMe())
				{
					currentSession = nodeList.get(0);
					log.debug("Leader Node " + currentSession);
					metadata.setLeader(currentSession);
					return Collections.singletonMap(currentSession, this.subscriptionSnapshot.iterator().next());
				}
				
				//If more than 1 node available then, Pick a READY Node first. 
				//Use a READY Node, invoke DBMS_TEQK.AQ$_CONNECTME() and use the node returned from there
				for(Node nodeNow: nodeList)
				{
					// Passing now=0 so that this check do not fail just because Metadata update is due
					if(client.isReady(nodeNow,0))
					{
						Node preferedNode   = getPreferredNode(nodeNow, nodeNow.user(),  subscriptionSnapshot.iterator().next() , consumerGroupId);
						if(preferedNode == null)
						{
							currentSession = nodeNow;
						}
						else
						{
							int preferredInst = preferedNode.id();
							for(Node currentNode : nodeList)
							{
								if(currentNode.id() == preferredInst)
								{
									currentSession = currentNode;
									break;
								}
							}
							if(currentSession == null)
							{
								// This should not happen. MetaData.fetch.nodes() should get one node for all the instances. 
								currentSession = preferedNode; // No connection yet to this node. Connection will get setup eventually
								nodeList.add(preferedNode);
							}
							// Closing other connections
							for(Node closeNode : nodeList)
							{
								if(closeNode != currentSession)
								{
									//If DB connection exist for this node then close it
									if(client.isReady(closeNode,0))
									{
										log.debug("Closing exta node: " + closeNode);
										client.close(closeNode);
									}
								}
							}
						}
						break;
					}
				}
				// If there is no READY node, then pick a node randomly
				if(currentSession == null) {
					Cluster cluster = metadata.fetch();
					if(cluster.controller() != null)
						currentSession = (Node)cluster.controller();
					else
						currentSession = nodeList.get(0);
					
					log.debug("There is no ready node available :using " + currentSession);
				}
				else {
					//Node oldLeader = clusterLeaderMap.get(cluster.clusterResource().clusterId());
					log.debug("Leader for this metadata set to " + currentSession);
					metadata.setLeader(currentSession);
					//cluster.setLeader(currentSession);
				}
			}
			return Collections.singletonMap(currentSession, this.subscriptionSnapshot.iterator().next());
		} catch(java.util.NoSuchElementException exception) {
			//do nothing
		}
		return Collections.emptyMap();
	}
	
	

	private ClientRequest createFetchRequest(Node destination, String topic, RequestCompletionHandler callback, int requestTimeoutMs) {
		return this.client.newClientRequest(destination,  new FetchRequest.Builder(topic, requestTimeoutMs) , time.milliseconds(), true, requestTimeoutMs, callback);
	}

	private void handleFetchResponse(ClientResponse response, long timeoutMs) {
		FetchResponse fetchResponse = (FetchResponse)response.responseBody();
		messages.addAll(fetchResponse.getMessages());
		if(response.wasDisconnected()) {
			client.disconnected(metadata.getNodeById(Integer.parseInt(response.destination())), time.milliseconds()); 
			rejoin = true;
			currentSession = null;
			if(sessionData != null)
			{
				log.info("Invalidating database session " + sessionData.name +". New one will get created.");
				sessionData.invalidSessionData();
			}
			return;
		}
		joinGroupifNeeded(response, timeoutMs);
	}

	private void joinGroupifNeeded(ClientResponse response, long timeoutMs) {
		try {
			FetchResponse fResponse = (FetchResponse)response.responseBody();
			Exception exception = fResponse.getException();
			long elapsed = response.requestLatencyMs();
			long prevTime = time.milliseconds();
			long current;

			while(elapsed < timeoutMs && rejoinNeeded(exception)) {
				log.debug("JoinGroup Is Needed");
				if (needsJoinPrepare) {
					log.debug("Revoking");
					onJoinPrepare();
					needsJoinPrepare = false;
				}
				if (lastRebalanceStartMs == -1L)
	                lastRebalanceStartMs = time.milliseconds();
				log.debug("Sending Join Group Request to database via node " + response.destination());
				sendJoinGroupRequest(metadata.getNodeById(Integer.parseInt(response.destination())));
				log.debug("Join Group Response received");
				exception = null;
				current = time.milliseconds();
				elapsed = elapsed + (current - prevTime);
				prevTime = current;
			}
		} catch(Exception e)
		{
			log.error(e.getMessage(), e);
			throw e;
		}
	}

	private boolean rejoinNeeded(Exception exception ) {
		if (exception != null && exception instanceof JMSException) {
			if( ((JMSException)exception).getLinkedException().getMessage().startsWith("ORA-24003") ) {
				log.debug("Join Group is needed");
				return true;				
			}
		}

		return rejoin;
	}

	private void onJoinPrepare() {
		maybeAutoCommitOffsetsSync(time.milliseconds());

		// execute the user's callback before rebalance
		ConsumerRebalanceListener listener = subscriptions.rebalanceListener();
		log.debug("Revoking previously assigned partitions {}", subscriptions.assignedPartitions());
		try {
			Set<TopicPartition> revoked = new HashSet<>(subscriptions.assignedPartitions());
			listener.onPartitionsRevoked(revoked);
		} catch (InterruptException e) {
			throw e;
		} catch (Exception e) {
			log.error("User provided listener {} failed on partition revocation", listener.getClass().getName(), e);
		}
        // Changes for 2.8.1 : SubscriptionState.java copied from org.apache.kafka* to org.oracle.okafka*
		subscriptions.resetGroupSubscription();
	}
	private void sendJoinGroupRequest(Node node) {
		log.debug("Sending JoinGroup");
		SessionData sessionData = this.sessionData;
		if(sessionData == null || sessionData.isInvalid()) {
			// First join group request
			String topic = subscriptionSnapshot.iterator().next();
			sessionData = new SessionData(-1, -1, node.user(), topic,-1, null, -1,null, -1, -1, -1);
			sessionData.addAssignedPartitions(new PartitionData(topic, -1, -1,
					null, -1, -1, false));
		}
		long now = time.milliseconds();
		ClientRequest request = this.client.newClientRequest(node, new JoinGroupRequest.Builder(sessionData), now, true);
		log.debug("Sending JoinGroup Request");
		ClientResponse response = this.client.send(request, now);  // Invokes  AQKafkaConsumer.joinGroup
		log.debug("Got JoinGroup Response, Handling Join Group Response");
		handleJoinGroupResponse(response);
		log.debug("Handled JoinGroup Response");
	}

	private void handleJoinGroupResponse(ClientResponse response) {
		JoinGroupResponse jResponse = (JoinGroupResponse)response.responseBody();
		
		if(response.wasDisconnected()) {
			log.info("Join Group failed as connection to database was severed.");
			client.disconnected(metadata.getNodeById(Integer.parseInt(response.destination())), time.milliseconds()); 
			rejoin = true;
			currentSession = null;
			if(sessionData != null)
			{
				log.info("Invalidating database session " + sessionData.name +". New one will get created.");
				sessionData.invalidSessionData();
			}
			return;
		}
		sensors.joinSensor.record(response.requestLatencyMs());
		
		//Map<Integer, Map<Integer, SessionData>> sData = jResponse.getSessionData();
		int leader = jResponse.leader();
		if(leader == 1) {
			log.debug("Invoking onJoinLeader ");
			onJoinLeader(metadata.getNodeById(Integer.parseInt(response.destination())), jResponse);
		} else {
			log.debug("Invoking onJoinFollower ");
			onJoinFollower(metadata.getNodeById(Integer.parseInt(response.destination())), jResponse);
		}

	}

	private void onJoinFollower(Node node, JoinGroupResponse jResponse) {
		List<SessionData> sData = new ArrayList<>();
		String topic = subscriptionSnapshot.iterator().next();
		SessionData sessionData = new SessionData(-1, -1, node.user(),  topic, -1, null, -1, null, -1, -1, -1);
		sessionData.addAssignedPartitions(new PartitionData(topic, -1, -1,
				null, -1, -1, false));
		sData.add(sessionData);
		sendSyncGroupRequest(node, sData, jResponse.version());
	}

	private void onJoinLeader(Node node, JoinGroupResponse jResponse) {
		Map<String, SessionData> sData = jResponse.getSessionData();
		List<PartitionData> partitions = jResponse.partitions();
		ConsumerPartitionAssignor assignor = lookUpAssignor();
		if (assignor == null)
			throw new IllegalStateException("Coordinator selected invalid assignment protocol.");

		Set<String> allSubscribedTopics = new HashSet<>();
		Map<String, Subscription> subscriptions = new HashMap<>();

		String prevSession = null;
		for(Map.Entry<String, SessionData> sessionEntry: sData.entrySet()) {
			String sessionName = sessionEntry.getKey();
			if(prevSession == null || !prevSession.equals(sessionName))
			{

				List<String> subTopics = new ArrayList<>();
				subTopics.add(sessionEntry.getValue().getSubscribedTopics());
				subscriptions.put(sessionName, new Subscription(subTopics, null));
				allSubscribedTopics.addAll(subTopics);
			}
			prevSession = sessionName;
		}

		//Changes for 2.8.1 :; GroupSubscribe was changed to metadataTopics()
		this.subscriptions.groupSubscribe(allSubscribedTopics);
		metadata.setTopics(this.subscriptions.metadataTopics()); 

		ConsumerPartitionAssignor.GroupSubscription gSub = new ConsumerPartitionAssignor.GroupSubscription(subscriptions);
		
		GroupAssignment gAssignment = assignor.assign(metadata.fetch(),gSub);
		Map<String, Assignment> assignment = gAssignment.groupAssignment();
		
		log.debug("Invoking geAssignment");
		List<SessionData> fAssignment = getAssignment(assignment, sData, partitions, jResponse.version());
		sendSyncGroupRequest(node, fAssignment, jResponse.version());
	}

	private void sendSyncGroupRequest(Node node, List<SessionData> sessionData, int version) {
		long now = time.milliseconds();
		ClientRequest request = this.client.newClientRequest(node, new SyncGroupRequest.Builder(sessionData, version), now, true);
		ClientResponse response = this.client.send(request, now);
		handleSyncGroupResponse(response);
	}
	private void handleSyncGroupResponse(ClientResponse response) {
		SyncGroupResponse syncResponse = (SyncGroupResponse)response.responseBody();
		Exception exception = syncResponse.getException();
		
		if(response.wasDisconnected()) {
			log.info("Sync Group failed as connection to database was severed.");
			client.disconnected(metadata.getNodeById(Integer.parseInt(response.destination())), time.milliseconds()); 
			rejoin = true;
			currentSession = null;
			if(sessionData != null)
			{
				log.info("Invalidating database session " + sessionData.name +". New one will get created.");
				sessionData.invalidSessionData();
			}
			sensors.failedRebalanceSensor.record();
			return;
		}
		
		if(exception == null) {
			sensors.syncSensor.record(response.requestLatencyMs());
			onJoinComplete(syncResponse.getSessionData());
			rejoin = false;
			needsJoinPrepare = true;
			this.sessionData = syncResponse.getSessionData();    	  
		}

	}

	protected void onJoinComplete(SessionData sessionData) {
		log.debug("OnJoinComplete Invoked");
		List<TopicPartition> assignment = new ArrayList<>();
		for(PartitionData pData : sessionData.getAssignedPartitions()) {
			log.debug("Assigned PartitionData " + pData.toString());
			assignment.add(pData.getTopicPartition());
		}
		subscriptions.assignFromSubscribed(assignment);
		//Changes for 2.8.1
		// Seek to current offset per say
		assignment.stream().forEach(tp->
			{
				subscriptions.seek(tp,0);
				subscriptions.completeValidation(tp);
			});

		ConsumerPartitionAssignor assignor = lookUpAssignor();
		// give the assignor a chance to update internal state based on the received
		// assignment
		// Changes for 2.8.1: See if GroupInstanceId can be fetched from Local Config 
		ConsumerGroupMetadata cgMetaData = new ConsumerGroupMetadata(sessionData.getSubscriberName(), sessionData.getVersion(), sessionData.name, Optional.of(sessionData.name));
		assignor.onAssignment(new ConsumerPartitionAssignor.Assignment(assignment, null), cgMetaData);

		// reschedule the auto commit starting from now
		this.nextAutoCommitDeadline = time.milliseconds() + autoCommitIntervalMs;

		// execute the user's callback after rebalance
		ConsumerRebalanceListener listener = subscriptions.rebalanceListener();
		log.debug("Setting newly assigned partitions {}", subscriptions.assignedPartitions());
		try {
			Set<TopicPartition> assigned = new HashSet<>(subscriptions.assignedPartitions());
			listener.onPartitionsAssigned(assigned);
			lastRebalanceEndMs = time.milliseconds();
            sensors.successfulRebalanceSensor.record(lastRebalanceEndMs - lastRebalanceStartMs);
            lastRebalanceStartMs = -1L;
		} catch (InterruptException e) {
			sensors.failedRebalanceSensor.record();
			throw e;
		} catch (Exception e) {
			sensors.failedRebalanceSensor.record();
			log.error("User provided listener {} failed on partition assignment", listener.getClass().getName(), e);
		}
	}
	
	private List<SessionData> getAssignment(Map<String, Assignment> assignment, Map<String, SessionData> sData, List<PartitionData> partitions, int version) {
		log.debug("Getting new assignment"); 
		List<SessionData> fAssignment = new ArrayList<>();

		Map<TopicPartition, PartitionData> pDataBytp = new HashMap<>();
		Map<TopicPartition, PartitionData> pDataBytp2 = new HashMap<>();

		for(SessionData data : sData.values()) {
			for(PartitionData pData : data.getAssignedPartitions()) {
				pDataBytp.put(pData.getTopicPartition(), pData);
			}
		}

		for(PartitionData pData : partitions) {
			pDataBytp2.put(pData.getTopicPartition(), pData);
		}

		for(Map.Entry<String, Assignment> assignmentEntry : assignment.entrySet()) {
			String sessionName = assignmentEntry.getKey();
			SessionData prevData = sData.get(sessionName);

			SessionData data = new SessionData(prevData.getSessionId(), prevData.getInstanceId(), prevData.getSchema(),prevData.getSubscribedTopics(), prevData.getQueueId(),
					prevData.getSubscriberName(), prevData.getSubscriberId(), prevData.createTime, prevData.getLeader(), version, prevData.getAuditId());
			for(TopicPartition tp : assignmentEntry.getValue().partitions()) {

				if(pDataBytp.get(tp) == null)
					data.addAssignedPartitions(pDataBytp2.get(tp));
				else 
					data.addAssignedPartitions(pDataBytp.get(tp));				 
			}

			fAssignment.add(data);

		}
		return fAssignment;
	}
/*
 * DummyCluster no longer being used
	private Cluster getDummyCluster(List<PartitionData> partitions) {

		Cluster cluster = metadata.fetch();
		List<PartitionInfo> pInfo = new ArrayList<>();
		for(PartitionData pData : partitions) {
			pInfo.add(new PartitionInfo(pData.getTopicPartition().topic(), pData.getTopicPartition().partition(), cluster.nodes().get(0), null, null));
		}
		return new Cluster("dummy", cluster.nodes(), pInfo,
				cluster.unauthorizedTopics(), cluster.internalTopics(), cluster.getConfigs());

	}*/

	private ConsumerPartitionAssignor lookUpAssignor() {
		if(this.assignors.size() == 0) 
			return null;
		return this.assignors.get(0);
	} 

	/**
	 * Subscribe to topic if not done
	 * @return true if subscription is successsful else false.
	 * @throws Exception 
	 */
	
	 public boolean mayBeTriggerSubcription(long timeout) {
		
		if(!subscriptions.subscription().equals(subscriptionSnapshot)) {
			boolean noSubExist = false;
			rejoin = true;
			String topic = getSubscribableTopics();
			long now = time.milliseconds();
			Node node = client.leastLoadedNode(now);
			if( node == null || !client.ready(node, now) ) {
				log.error("Failed to subscribe to topic: {}", topic);
				return false;
			}
			try {
				if(aqConsumer.getSubcriberCount(node, topic) < 1) {
					noSubExist = true;
				}
				
				ClientRequest request = this.client.newClientRequest(node, new SubscribeRequest.Builder(topic), now, true, requestTimeoutMs < timeout ? requestTimeoutMs: (int)timeout, null);
				ClientResponse response = this.client.send(request, now);

				if(handleSubscribeResponse(response)) {
					
					if(noSubExist && aqConsumer.getoffsetStartegy().equalsIgnoreCase("earliest")) {
		                	TopicPartition tp = new TopicPartition(topic, -1);
		                	Map<TopicPartition, Long> offsetResetTimestamps = new HashMap<TopicPartition, Long>(); 		            		
		            		offsetResetTimestamps.put(tp, -2L);
		            			
		                return resetOffsetsSync(offsetResetTimestamps, timeout);
		            } 
		                	
		            else if(noSubExist && aqConsumer.getoffsetStartegy().equalsIgnoreCase("none")) {
		                throw new ConfigException("No previous offset found for the consumer group");
		            }
				}
				else {
					return false;
				}
				
			}
			catch(ConfigException exception) {
				log.error("Exception while subscribing to the topic" + exception.getMessage(),exception);
				log.info("Closing the consumer due to exception : " + exception.getMessage());
				 throw new ConfigException("No previous offset found for the consumer group");
			}
			catch(Exception e){
				log.error("Exception while subscribing to the topic" + e.getMessage(),e);
			}
		
		}
		return true;

	}

	public void maybeUpdateMetadata(long timeout) {
		Cluster cluster = metadata.fetch();
		long curr = time.milliseconds();
		if(cluster.isBootstrapConfigured() || metadata.timeToNextUpdate(curr) == 0 ? true : false) {
			int lastVersion = metadata.version();
			long elapsed = 0;
			long prev;
			
			while ((elapsed <= timeout) && (metadata.version() <= lastVersion) && !metadata.isClosed()) {
				prev = time.milliseconds();
				client.maybeUpdateMetadata(curr);
				curr = time.milliseconds();
				elapsed = elapsed + (prev - curr);
			}
			log.debug("Metadata updated:Current Metadata Version " + metadata.version());

		}
	}

	private boolean handleSubscribeResponse(ClientResponse response) {
		if(response.wasDisconnected()) {
			client.disconnected(metadata.getNodeById(Integer.parseInt(response.destination())), time.milliseconds());
		}
		SubscribeResponse subscribeResponse = (SubscribeResponse)response.responseBody();
		JMSException exception = subscribeResponse.getException();
		if(exception != null) { 
			log.error("failed to subscribe to topic {}", subscribeResponse.getTopic());		
			return false;
		}else {
			this.subscriptionSnapshot.add(subscribeResponse.getTopic());
		}
		return true;

	}

	/**
	 * Updates subscription snapshot and returns subscribed topic. 
	 * @return subscribed topic
	 */
	private String getSubscribableTopics() {
		//this.subcriptionSnapshot = new HashSet<>(subscriptions.subscription());
		return getSubscribedTopic();
	}

	/**
	 * return subscribed topic.
	 */
	private String getSubscribedTopic() {
		HashSet<String> subscribableTopics = new HashSet<>();
		for(String topic : subscriptions.subscription()) {
			if(!this.subscriptionSnapshot.contains(topic)) {
				subscribableTopics.add(topic);
				this.subscriptionSnapshot.clear();
			}	
		}
		return subscribableTopics.iterator().next();
	}

	public boolean commitOffsetsSync(Map<TopicPartition, OffsetAndMetadata> offsets, long timeout) throws Exception{
		try {
		log.debug("Sending synchronous commit of offsets: {} request", offsets);	
		//long elapsed = 0;
		ClientRequest request;
		ClientResponse response;
		//Changes for 2.8.1:: Send leader Node explicitly here
		//request = this.client.newClientRequest(null, new CommitRequest.Builder(getCommitableNodes(offsets), offsets), time.milliseconds(), true);
		Map<Node, List<TopicPartition>> commitableNodes = getCommitableNodes(offsets);
		if(commitableNodes == null || commitableNodes.size() == 0)
		{
			log.debug("No offsets to commit. Return");
			return true;
		}
		request = this.client.newClientRequest(metadata.getLeader(), new CommitRequest.Builder(commitableNodes, offsets), time.milliseconds(), true);
		response = this.client.send(request, time.milliseconds());
		handleCommitResponse(response); 

		if(((CommitResponse)response.responseBody()).error()) {

			throw ((CommitResponse)response.responseBody()).getResult()
			.entrySet().iterator().next().getValue();
		}
		}catch(Exception e)
		{
			log.error("Exception while committing messages " + e,e);
			throw e;
		}

		return true;
	}

	private void handleCommitResponse(ClientResponse response) {
		CommitResponse commitResponse = (CommitResponse)response.responseBody();
		Map<Node, List<TopicPartition>> nodes =  commitResponse.getNodes();
		Map<TopicPartition, OffsetAndMetadata> offsets = commitResponse.offsets();
		Map<Node, Exception> result = commitResponse.getResult();
		for(Map.Entry<Node, Exception> nodeResult : result.entrySet()) {
			if(nodeResult.getValue() == null) {
				this.sensors.commitSensor.record(response.requestLatencyMs());
				for(TopicPartition tp : nodes.get(nodeResult.getKey())) {
					log.debug("Commited to topic partiton: {} with  offset: {} ", tp, offsets.get(tp));
					offsets.remove(tp);
				}
				nodes.remove(nodeResult.getKey());	
			} else {
				for(TopicPartition tp : nodes.get(nodeResult.getKey())) {
					log.error("Failed to commit to topic partiton: {} with  offset: {} ", tp, offsets.get(tp));
				}

			}	
		}	
	}

	/**
	 * Returns nodes that have sessions ready for commit.
	 * @param offsets Recently consumed offset for each partition since last commit.
	 * @return map of node , list of partitions(node is leader for its corresponding partition list) that are ready for commit.
	 */
	private Map<Node, List<TopicPartition>> getCommitableNodes(Map<TopicPartition, OffsetAndMetadata> offsets) {
		Map<Node, List<TopicPartition>> nodeTPMap = new HashMap<>();
		Cluster cluster = metadata.fetch();
		
		Node leaderNode = metadata.getLeader();
		if(leaderNode== null)
		{
			//Find a ready node
			List<org.apache.kafka.common.Node> kafkaNodeList = cluster.nodes();
			for(org.apache.kafka.common.Node node :kafkaNodeList )
			{
				if(client.isReady((org.oracle.okafka.common.Node)node, 0))
				{
					leaderNode = (org.oracle.okafka.common.Node)node;
					log.info("Leader Node not present. Picked first ready node: " + leaderNode);
					break;
				}
			}
		}
		log.debug("Sending Commit request to leader Node " + leaderNode);
		
		for(Map.Entry<TopicPartition, OffsetAndMetadata> metadata : offsets.entrySet())	{
			if(!client.ready(leaderNode, time.milliseconds())) {
				log.info("Failed to send commit as Leader node is not ready to send commit: " + leaderNode);
				log.error("Failed to commit to topic partiton: {} with  offset: {} ", metadata.getKey(), metadata.getValue());
			} else {
				List<TopicPartition> nodeTPList= nodeTPMap.get(leaderNode);
				if(nodeTPList == null) {
					nodeTPList = new ArrayList<TopicPartition>();
					nodeTPMap.put(leaderNode, nodeTPList );
				}
				nodeTPList.add(metadata.getKey());
			}
		}
		return nodeTPMap;
	}
	public boolean resetOffsetsSync(Map<TopicPartition, Long>  offsetResetTimestamps, long timeout) {
		long now = time.milliseconds();
		Node node = client.leastLoadedNode(now);
		if( node == null || !client.ready(node, now) ) 
			return false;
		ClientResponse response = client.send(client.newClientRequest(node, new OffsetResetRequest.Builder(offsetResetTimestamps, 0), now, true, requestTimeoutMs < timeout ? requestTimeoutMs: (int)timeout, null), now);
		return handleOffsetResetResponse(response, offsetResetTimestamps);
	}

	public boolean handleOffsetResetResponse(ClientResponse response, Map<TopicPartition, Long>  offsetResetTimestamps) {
		OffsetResetResponse offsetResetResponse = (OffsetResetResponse)response.responseBody();
		Map<TopicPartition, Exception> result = offsetResetResponse.offsetResetResponse();
		Set<TopicPartition> failed = new HashSet<>();
		for(Map.Entry<TopicPartition, Exception> tpResult : result.entrySet()) {
			Long offsetLong = offsetResetTimestamps.get(tpResult.getKey());
			String offset ;

			if( offsetLong == -2L )
				offset = "TO_EARLIEST" ;
			else if (offsetLong == -1L) 
				offset = "TO_LATEST";
			else offset = Long.toString(offsetLong);
			if( tpResult.getValue() == null && tpResult.getKey().partition()!=-1) {
				subscriptions.requestOffsetReset(tpResult.getKey(), null);
				subscriptions.seekValidated(tpResult.getKey(), new FetchPosition(0));
			    subscriptions.completeValidation(tpResult.getKey());
				log.trace("seek to offset {} for topicpartition  {} is successful", offset, tpResult.getKey());
			}
			else if(tpResult.getValue()!=null){
				if(tpResult.getValue() instanceof java.sql.SQLException && ((java.sql.SQLException)tpResult.getValue()).getErrorCode() == 25323)
					subscriptions.requestOffsetReset(tpResult.getKey(), null);
				else failed.add(tpResult.getKey());
				log.warn("Failed to update seek for topicpartition {} to offset {}", tpResult.getKey(), offset);
			}

		}
		//Changes for 2.8.1. Copied SubscriptionState.java from org.apache.kafka.clients.consumer.internals to 
		// org.oracle.okafka.clients.consumer.internals for this requestFailed method.
		subscriptions.requestFailed(failed, time.milliseconds() + retryBackoffMs);
		return true;
	}
	
	/**
	 * Synchronously commit last consumed offsets if auto commit is enabled.
	 */
	public void maybeAutoCommitOffsetsSync(long now) {
		if (autoCommitEnabled && now >= nextAutoCommitDeadline) {
			this.nextAutoCommitDeadline = now + autoCommitIntervalMs;
			doCommitOffsetsSync();
		}
	}

	public void clearSubscription() {
		//doCommitOffsetsSync();
		this.subscriptionSnapshot.clear();			
	}

	/**
	 * Synchronously commit offsets.
	 */
	private void doCommitOffsetsSync() {
		Map<TopicPartition, OffsetAndMetadata> allConsumedOffsets = subscriptions.allConsumed();

		try {
			commitOffsetsSync(allConsumedOffsets, 0);

		} catch(Exception exception) {
			//nothing to do
		} finally {
			nextAutoCommitDeadline = time.milliseconds() + autoCommitIntervalMs;
		}
	} 

	public void unsubscribe() {
		
		if(currentSession!=null) {
		ClientRequest request = this.client.newClientRequest(currentSession, new UnsubscribeRequest.Builder(), time.milliseconds(), true);
		ClientResponse response = this.client.send(request, time.milliseconds());
		handleUnsubscribeResponse(response);
		}

	}
	private void handleUnsubscribeResponse(ClientResponse response) {

		if(response.wasDisconnected()) {
			log.debug("handleUnsubscribeResponse : node in disconnected state\n");
			client.disconnected(metadata.getNodeById(Integer.parseInt(response.destination())), time.milliseconds()); 
			rejoin = true;
			currentSession = null;
			if(sessionData != null)
			{
				log.debug("handleUnsubscribeResponse : Invalidating database session " + sessionData.name +". New one will get created.\n");
				sessionData.invalidSessionData();
			}
			return;
		}
		UnsubscribeResponse  unsubResponse = (UnsubscribeResponse)response.responseBody();
		for(Map.Entry<String, Exception> responseByTopic: unsubResponse.response().entrySet()) {
			if(responseByTopic.getValue() != null)
				log.info("Failed to unsubscribe from topic: with exception: ", responseByTopic.getKey(), responseByTopic.getValue());
			else
				log.info("Unsubscribed from topic: ", responseByTopic.getKey());
		}

	}

	/**
	 * Return the time to the next needed invocation of {@link #poll(long)}.
	 * @param now current time in milliseconds
	 * @return the maximum time in milliseconds the caller should wait before the next invocation of poll()
	 */
	public long timeToNextPoll(long now, long timeoutMs) {
		if (!autoCommitEnabled)
			return timeoutMs;

		if (now > nextAutoCommitDeadline)
			return 0;

		return Math.min(nextAutoCommitDeadline - now, timeoutMs);
	}

	/**
	 * Closes the AQKafkaConsumer.
	 * Commits last consumed offsets if auto commit enabled
	 * @param timeoutMs
	 */
	public void close(long timeoutMs) throws Exception {
		KafkaException autoCommitException = null;
		if(autoCommitEnabled) {
			Map<TopicPartition, OffsetAndMetadata> allConsumedOffsets = subscriptions.allConsumed();
			try {
				commitOffsetsSync(allConsumedOffsets, timeoutMs);
			} catch (Exception exception) {
				autoCommitException= new KafkaException("failed to commit consumed messages", exception);
			}
		}
		this.client.close();
		if(autoCommitException != null)
			throw autoCommitException;
	}


	private Node getPreferredNode(Node currentNode, String schema, String topic, String groupId)
	{
		long now = time.milliseconds();
		ClientRequest request = this.client.newClientRequest(currentNode, new ConnectMeRequest.Builder(schema,topic,groupId), now, true);
		log.debug("Sending ConnectMe Request");
		ClientResponse response = this.client.send(request, now);  // Invokes  DBMS_TEQK.AQ$_CONNECT_ME
		log.debug("Got ConnectMe response");
		ConnectMeResponse connMeResponse = (ConnectMeResponse)response.responseBody();
		Node preferredNode  = connMeResponse.getPreferredNode();
		log.debug("ConnectMe: PreferredNode " +preferredNode );
		return preferredNode;
	}
	
	private class ConsumerCoordinatorMetrics {
        public final String metricGrpName;
        
        private final Sensor commitSensor;
        public final Sensor joinSensor;
        public final Sensor syncSensor;
        public final Sensor successfulRebalanceSensor;
        public final Sensor failedRebalanceSensor;
        

        public ConsumerCoordinatorMetrics(Metrics metrics, String metricGrpPrefix) {
            this.metricGrpName = metricGrpPrefix + "-coordinator-metrics";

            this.commitSensor = metrics.sensor("commit-latency");
            this.commitSensor.add(metrics.metricName("commit-latency-avg",
                this.metricGrpName,
                "The average time taken for a commit request"), new Avg());
            this.commitSensor.add(metrics.metricName("commit-latency-max",
                this.metricGrpName,
                "The max time taken for a commit request"), new Max());
            this.commitSensor.add(createMeter(metrics, metricGrpName, "commit", "commit calls"));
            	
            this.joinSensor = metrics.sensor("join-latency");
            this.joinSensor.add(metrics.metricName("join-time-avg",
                this.metricGrpName,
                "The average time taken for a group rejoin"), new Avg());
            this.joinSensor.add(metrics.metricName("join-time-max",
                this.metricGrpName,
                "The max time taken for a group rejoin"), new Max());
            this.joinSensor.add(createMeter(metrics, metricGrpName, "join", "group joins"));

            this.syncSensor = metrics.sensor("sync-latency");
            this.syncSensor.add(metrics.metricName("sync-time-avg",
                this.metricGrpName,
                "The average time taken for a group sync"), new Avg());
            this.syncSensor.add(metrics.metricName("sync-time-max",
                this.metricGrpName,
                "The max time taken for a group sync"), new Max());
            this.syncSensor.add(createMeter(metrics, metricGrpName, "sync", "group syncs"));

            this.successfulRebalanceSensor = metrics.sensor("rebalance-latency");
            this.successfulRebalanceSensor.add(metrics.metricName("rebalance-latency-avg",
                this.metricGrpName,
                "The average time taken for a group to complete a successful rebalance, which may be composed of " +
                    "several failed re-trials until it succeeded"), new Avg());
            this.successfulRebalanceSensor.add(metrics.metricName("rebalance-latency-max",
                this.metricGrpName,
                "The max time taken for a group to complete a successful rebalance, which may be composed of " +
                    "several failed re-trials until it succeeded"), new Max());
            this.successfulRebalanceSensor.add(metrics.metricName("rebalance-latency-total",
                this.metricGrpName,
                "The total number of milliseconds this consumer has spent in successful rebalances since creation"),
                new CumulativeSum());
            this.successfulRebalanceSensor.add(
                metrics.metricName("rebalance-total",
                    this.metricGrpName,
                    "The total number of successful rebalance events, each event is composed of " +
                        "several failed re-trials until it succeeded"),
                new CumulativeCount()
            );
            this.successfulRebalanceSensor.add(
                metrics.metricName(
                    "rebalance-rate-per-hour",
                    this.metricGrpName,
                    "The number of successful rebalance events per hour, each event is composed of " +
                        "several failed re-trials until it succeeded"),
                new Rate(TimeUnit.HOURS, new WindowedCount())
            );

            this.failedRebalanceSensor = metrics.sensor("failed-rebalance");
            this.failedRebalanceSensor.add(
                metrics.metricName("failed-rebalance-total",
                    this.metricGrpName,
                    "The total number of failed rebalance events"),
                new CumulativeCount()
            );
            this.failedRebalanceSensor.add(
                metrics.metricName(
                    "failed-rebalance-rate-per-hour",
                    this.metricGrpName,
                    "The number of failed rebalance events per hour"),
                new Rate(TimeUnit.HOURS, new WindowedCount())
            );
            
            

            Measurable lastRebalance = (config, now) -> {
                if (lastRebalanceEndMs == -1L)
                    // if no rebalance is ever triggered, we just return -1.
                    return -1d;
                else
                    return TimeUnit.SECONDS.convert(now - lastRebalanceEndMs, TimeUnit.MILLISECONDS);
            };
            metrics.addMetric(metrics.metricName("last-rebalance-seconds-ago",
                this.metricGrpName,
                "The number of seconds since the last successful rebalance event"),
                lastRebalance);
            
            Measurable numParts = (config, now) -> subscriptions.numAssignedPartitions();
            metrics.addMetric(metrics.metricName("assigned-partitions",
                this.metricGrpName,
                "The number of partitions currently assigned to this consumer"), numParts);

        }
        
        public Meter createMeter(Metrics metrics, String groupName, String baseName, String descriptiveName) {
            return new Meter(new WindowedCount(),
                    metrics.metricName(baseName + "-rate", groupName,
                            String.format("The number of %s per second", descriptiveName)),
                    metrics.metricName(baseName + "-total", groupName,
                            String.format("The total number of %s", descriptiveName)));
        }
    }
	
}
