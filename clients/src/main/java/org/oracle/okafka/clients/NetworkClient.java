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

package org.oracle.okafka.clients;

//import org.apache.kafka.clients.unused.ClusterConnectionStates;
import org.apache.kafka.clients.admin.internals.AdminMetadataManager;
import org.apache.kafka.clients.admin.internals.AdminMetadataManager.AdminMetadataUpdater;
import org.oracle.okafka.common.Node;
import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.MetadataUpdater;
import org.apache.kafka.clients.RequestCompletionHandler;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthenticationException;
import org.oracle.okafka.common.errors.ConnectionException;
import org.oracle.okafka.common.errors.InvalidLoginCredentialsException;
import org.apache.kafka.common.metrics.Sensor;
import org.oracle.okafka.common.network.AQClient;
import org.oracle.okafka.common.protocol.ApiKeys;
import org.oracle.okafka.common.requests.AbstractRequest;
import org.oracle.okafka.common.requests.MetadataRequest;
import org.oracle.okafka.common.requests.MetadataResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;

import java.net.ConnectException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;

import javax.jms.JMSException;
import javax.jms.JMSSecurityException;

/**
 * A network client for synchronous request/response network i/o. This is an internal class used to implement the
 * user-facing producer and consumer clients.
 * <p>
 * This class is not thread-safe!
 */
public class NetworkClient implements KafkaClient {

	private final Logger log;

	/* the selector used to perform network i/o */
	private final AQClient aqClient;

	private Metadata metadata;
	private AdminMetadataManager metadataManager;
	private MetadataUpdater metadataUpdater;

	private final Random randOffset;

	/* the state of each node's connection */
	private final ClusterConnectionStates connectionStates;

	/* the socket send buffer size in bytes */
	private final int socketSendBuffer;

	/* the socket receive size buffer in bytes */
	private final int socketReceiveBuffer;

	/* the client id used to identify this client in requests to the server */
	private final String clientId;

	/* the current correlation id to use when sending requests to servers */
	private int correlation;

	/* default timeout for individual requests to await acknowledgement from servers */
	private final int defaultRequestTimeoutMs;

	/* time in ms to wait before retrying to create connection to a server */
	private final long reconnectBackoffMs;

	private final Time time;

	private final Sensor throttleTimeSensor;

	// Invoked from KafkaProducer and KafkaConsumer
	public NetworkClient(AQClient aqClient,
			Metadata metadata,
			String clientId,
			long reconnectBackoffMs,
			long reconnectBackoffMax,
			int socketSendBuffer,
			int socketReceiveBuffer,
			int defaultRequestTimeoutMs,
			Time time,
			LogContext logContext) {
		this(null,
				metadata,
				aqClient,
				clientId,
				reconnectBackoffMs,
				reconnectBackoffMax,
				socketSendBuffer,
				socketReceiveBuffer,
				defaultRequestTimeoutMs,
				time,
				null,
				logContext);
	}

/*	public NetworkClient(AQClient aqClient,
			Metadata metadata,
			String clientId,
			long reconnectBackoffMs,
			long reconnectBackoffMax,
			int socketSendBuffer,
			int socketReceiveBuffer,
			int defaultRequestTimeoutMs,
			Time time,
			Sensor throttleTimeSensor,
			LogContext logContext) {
		this(null,
				metadata,
				aqClient,
				clientId,
				reconnectBackoffMs,
				reconnectBackoffMax,
				socketSendBuffer,
				socketReceiveBuffer,
				defaultRequestTimeoutMs,
				time,
				throttleTimeSensor,
				logContext);
	}*/

	//Invoked from KafkaAdmin. Passing metadataManger instead of metadata
	//ToDo: Check if this is needed or not.
	
	public NetworkClient(AQClient aqClient,
			AdminMetadataManager metadataManger,
			String clientId,
			long reconnectBackoffMs,
			long reconnectBackoffMax,
			int socketSendBuffer,
			int socketReceiveBuffer,
			int defaultRequestTimeoutMs,
			Time time,
			LogContext logContext) {
		this(metadataManger,
				null,
				aqClient,
				clientId,
				reconnectBackoffMs,
				reconnectBackoffMax,
				socketSendBuffer,
				socketReceiveBuffer,
				defaultRequestTimeoutMs,
				time,
				null,
				logContext);
	}

	private NetworkClient(AdminMetadataManager metadataManager,
			Metadata metadata,
			AQClient aqClient,
			String clientId,
			long reconnectBackoffMs,
			long reconnectBackoffMax,
			int socketSendBuffer,
			int socketReceiveBuffer,
			int defaultRequestTimeoutMs,
			Time time,
			Sensor throttleTimeSensor,
			LogContext logContext) {
		/* It would be better if we could pass `DefaultMetadataUpdater` from the public constructor, but it's not
		 * possible because `DefaultMetadataUpdater` is an inner class and it can only be instantiated after the
		 * super constructor is invoked.
		 */
		this.metadata = metadata;
		this.metadataManager = metadataManager;

		if(metadataManager != null)
		{
			this.metadataUpdater = metadataManager.updater();
		}else 
		{
			this.metadataManager = null;
			this.metadataUpdater = null;
		}
		if (metadataUpdater == null) {
			if (this.metadata == null)
				throw new IllegalArgumentException("`metadata` must not be null");

			this.metadataUpdater = new DefaultMetadataUpdater(metadata);
		} 
		/*else {
			this.metadataUpdater = metadataUpdater;
		}*/
		this.aqClient = aqClient;
		this.clientId = clientId;
		this.connectionStates = new ClusterConnectionStates(reconnectBackoffMs, reconnectBackoffMax);
		this.socketSendBuffer = socketSendBuffer;
		this.socketReceiveBuffer = socketReceiveBuffer;
		this.correlation = 0;
		this.randOffset = new Random();
		this.defaultRequestTimeoutMs = defaultRequestTimeoutMs;
		this.reconnectBackoffMs = reconnectBackoffMs;
		this.time = time;
		this.throttleTimeSensor = throttleTimeSensor;
		this.log = logContext.logger(NetworkClient.class);
	}

	/**
	 * Begin connecting to the given node, return true if we are already connected and ready to send to that node.
	 *
	 * @param node The node to check
	 * @param now The current timestamp
	 * @return True if we are ready to send to the given node
	 */
	// @Override
	public boolean ready(Node node, long now) {
		if (node.isEmpty())
			throw new IllegalArgumentException("Cannot connect to empty node " + node);
		if (isReady(node, now))
			return true;
		if (connectionStates.canConnect(node, now)) {
			// if we are interested in sending to a node and we don't have a connection to it, initiate one
			return initiateConnect(node, now);
		}

		return false;
	}

	// Visible for testing
	boolean canConnect(Node node, long now) {
		return connectionStates.canConnect(node, now);
	}

	/**
	 * Disconnects the connection to a particular node, if there is one.
	 * Any pending ClientRequests for this connection will receive disconnections.
	 *
	 * @param nodeId The id of the node
	 */
	// @Override
	public void disconnect(Node node) {

	}

	public ClusterConnectionStates getConnectionStates() {
		return this.connectionStates;
	}

	/**
	 * Closes the connection to a particular node (if there is one).
	 *
	 * @param node the node
	 */
	//@Override
	public void close(Node node) {
		aqClient.close(node);
		connectionStates.remove(node);
	}

	/**
	 * Returns the number of milliseconds to wait, based on the connection state, before attempting to send data. When
	 * disconnected, this respects the reconnect backoff time. When connecting or connected, this handles slow/stalled
	 * connections.
	 *
	 * @param node The node to check
	 * @param now The current timestamp
	 * @return The number of milliseconds to wait.
	 */
	// @Override
	public long connectionDelay(Node node, long now) {
		return connectionStates.connectionDelay(node, now);
	}

	/**
	 * Return the poll delay in milliseconds based on both connection and throttle delay.
	 * @param node the connection to check
	 * @param now the current time in ms
	 */
	// @Override
	public long pollDelayMs(Node node, long now) {
		return connectionStates.pollDelayMs(node, now);
	}

	/**
	 * Check if the connection of the node has failed, based on the connection state. Such connection failure are
	 * usually transient and can be resumed in the next {@link #ready(org.oracle.okafka.common.Node, long)} }
	 * call, but there are cases where transient failures needs to be caught and re-acted upon.
	 *
	 * @param node the node to check
	 * @return true iff the connection has failed and the node is disconnected
	 */
	// @Override
	public boolean connectionFailed(Node node) {
		return connectionStates.isDisconnected(node);
	}

	/**
	 * Check if authentication to this node has failed, based on the connection state. Authentication failures are
	 * propagated without any retries.
	 *
	 * @param node the node to check
	 * @return an AuthenticationException iff authentication has failed, null otherwise
	 */
	//@Override
	public AuthenticationException authenticationException(Node node) {
		return connectionStates.authenticationException(node);
	}

	/**
	 * Check if the node  is ready to send more requests.
	 *
	 * @param node The node
	 * @param now The current time in ms
	 * @return true if the node is ready
	 */
	// @Override
	public boolean isReady(Node node, long now) {
		// if we need to update our metadata now declare all requests unready to make metadata requests first
		// priority
		// isReady will return false if metadata is due for update.  Alternative is to not check for this and handle on the caller partreturn canSendRequest(node, now);

		//return !metadataUpdater.isUpdateDue(now) && canSendRequest(node, now);
		//No Point in stopping the world if MetaData update is due. We periodically check and update it. 
		return canSendRequest(node, now);
	}

	/**
	 * Are we connected and ready and able to send more requests to the given connection?
	 *
	 * @param node The node
	 * @param now the current timestamp
	 */
	private boolean canSendRequest(Node node, long now) {
		boolean connState = this.connectionStates.isReady(node, now);
		return connState;

	}

	/**
	 * Send the given request. Requests can only be sent out to ready nodes.
	 * @param request The request
	 * @param now The current timestamp
	 */
	//@Override
	public ClientResponse send(ClientRequest request, long now) {
		return doSend(request, false, now);
	}

	private void sendInternalMetadataRequest(MetadataRequest.Builder builder, Node node, long now) {
		ClientRequest clientRequest = newClientRequest(node, builder, now, true);
		ClientResponse response = doSend(clientRequest, true, now);
		log.debug("Got response for metadata request {} from node {}", builder, node);
		((DefaultMetadataUpdater)metadataUpdater).handleCompletedMetadataResponse(
				response.requestHeader(), time.milliseconds(), (MetadataResponse)response.responseBody());
	}

	private ClientResponse doSend(ClientRequest clientRequest, boolean isInternalRequest, long now) {
		ClientResponse response = null;
		try {
			Node node = null;
			if (metadata != null)
			{
				node = (org.oracle.okafka.common.Node)metadata.getNodeById(Integer.parseInt(clientRequest.destination()));
				/*
				 * When Bootstrap cluster is created, it does not contain much information about
				 * the node. For Bootstrap node, the id remains 0. After connecting to the
				 * bootstrap node, its id and name and other details are populated. Metadata
				 * contains the HashMap of the Node. HashMap containing the node as key may not
				 * reflect the correct Hash value of the Key on some cases and the Node returned
				 * can be null. We handle such cases by manually traversing the HashMap to look
				 * for the correct node with id.
				 */
				if (node == null) {
					List<org.apache.kafka.common.Node> nodeList = metadata.fetch().nodes();
					for (org.apache.kafka.common.Node nodeNow : nodeList) {
						if (nodeNow.id() == Integer.parseInt(clientRequest.destination())) {
							node = (org.oracle.okafka.common.Node) nodeNow;
							break;
						}
					}
				}
			}
			else if(metadataManager != null)
			{
				node = (org.oracle.okafka.common.Node)metadataManager.nodeById(Integer.parseInt(clientRequest.destination()));
				
			}

			if (node !=null && !isInternalRequest) {
				// If this request came from outside the NetworkClient, validate
				// that we can send data.  If the request is internal, we trust
				// that internal code has done this validation.  Validation
				// will be slightly different for some internal requests (for
				// example, ApiVersionsRequests can be sent prior to being in
				// READY state.)
				if (!this.ready(node, now)) {
					log.info("Attempt to send a request to node " + node + " which is not ready.");
					throw new IllegalStateException("Attempt to send a request to node " + node + " which is not ready.");
				}
			}
			log.debug("Sending Request: " + ApiKeys.convertToOracleApiKey(clientRequest.apiKey()).name());
			response =  aqClient.send(clientRequest);
			log.debug("Response Received "  + ApiKeys.convertToOracleApiKey(clientRequest.apiKey()).name());
			handleDisconnection(node, response.wasDisconnected(), time.milliseconds());
		} catch(Exception e)
		{
			log.error("Exception from NetworkClient.doSend " + e,e);
			throw e;
		}
		return response;
	}

	// @Override
	public boolean hasReadyNodes(long now) {
		return connectionStates.hasReadyNodes(now);
	}

	//  @Override 
	public long maybeUpdateMetadata(long now) {
		return metadataUpdater.maybeUpdate(now);
	}

	/**
	 * Close the network client
	 */
	// @Override
	public void close() {
		aqClient.close();
		this.metadataUpdater.close();
	}

	/**
	 * Choose first ready node.
	 *
	 * @return The node ready.
	 */
	//  @Override
	public Node leastLoadedNode(long now) {

		List<Node> nodes = convertToOracleNodes(this.metadataUpdater.fetchNodes());
		log.info("Available Nodes " + nodes.size());
		for(Node n : nodes)
		{
			log.debug(n.toString());
		}
		Node found = null;
		int offset = this.randOffset.nextInt(nodes.size());
		for (int i = 0; i < nodes.size(); i++) {
			int idx = (offset + i) % nodes.size();
			Node node = nodes.get(idx);
			/* Removed isMetadataUpdate pending check */
			//if (isReady(node, now)) {
			if (canSendRequest(node, now)) {
				// if we find an established connection with no in-flight requests we can stop right away
				log.debug("Found connected node {}", node);
				return node;
			}/* else if (!this.connectionStates.isBlackedOut(node, now) ) {
				// otherwise if this is the best we have found so far, record that
				found = node;
			} else if (log.isTraceEnabled()) {
				log.debug("Removing node {} from least loaded node selection: is-blacked-out: {}",
						node, this.connectionStates.isBlackedOut(node, now));
			} */
		}

	/*	if (found != null)
			log.debug("Found least loaded node {}", found);
		else */
		{
			log.info("All Known nodes are disconnected. Try one time to connect.");
			//System.out.println("All known nodes are disconnected. Try to re-connect to each node one after the other");
			// If no node is reachable, try to connect one time
			boolean connected = false;
			for(Node node : nodes)
			{
				connected = initiateConnect(node,now);
				if(connected)
				{
					log.info("Reconnect successful to node " + node);
					found = node;
					break;
				}
				/*else {
					try 
					{
						//Cannot connect to Oracle Database. Retry after reconnectBackoffMs seconds
						Thread.sleep(reconnectBackoffMs);
					} 
					catch(Exception ignoreE) {} 
				} */
			}
			//If no known node is reachable, try without instnace_name. This is needed in case 
			//application is using SCAN-Listener and database service which gets migrated to available instance 
			/* if(!connected)
			{
				log.info("Not able to connect to any know instances.");
				Node oldNode = nodes.get(0);
				Node newNode = new Node(oldNode.host(), oldNode.port(), oldNode.serviceName());
				log.info("Trying to connect to: " + newNode);
				connected = initiateConnect(newNode,now);
				if(connected) {
					log.info("Connected to "+ newNode);
					found = newNode;
				}
				else {
					log.error("Not able to reach Oracle Database:" + newNode);
				}
			} */
			if(found == null)
				log.debug("Least loaded node selection failed to find an available node");
		}

		return found;
	}

	//  @Override
	public void disconnected(Node node, long now) {
		this.aqClient.close(node);
		this.connectionStates.disconnected(node, now);
	}


	/**
	 * Initiate a connection to the given node
	 */
	public boolean initiateConnect(Node node, long now) {
		try {
			log.info("Initiating connection to node {}", node);
			this.connectionStates.connecting(node, now);
			aqClient.connect(node);
			this.connectionStates.ready(node);
			log.debug("Connection is established to node {}", node);
		} catch (Exception e) {
			log.error("Failed to connect to node {} with error {}", node, e.getMessage());

			if (e instanceof JMSException) { // from Producer or Consumer
				JMSException jmsExcp = (JMSException) e;
				String jmsError = jmsExcp.getErrorCode();
				if (jmsError != null && (jmsError.equals("12514") || jmsError.equals("12541"))) {
					throw new ConnectionException(new ConnectException(e.getMessage()));
				}
				if (jmsError != null && jmsError.equals("1405")) {
					log.error("create session privilege is not granted", e.getMessage());
					log.info(
							"Check Oracle Documentation for Kafka Java Client Interface for Oracle Transactional"
							+ " Event Queues to get the complete list of privileges required for the database user.");
				}
			}

			if (e instanceof JMSSecurityException)
				throw new InvalidLoginCredentialsException("Invalid login details provided:" + e.getMessage());

			/* attempt failed, we'll try again after the backoff */
			connectionStates.disconnected(node, now);
			/* maybe the problem is our metadata, update it */
			if (!(metadataUpdater instanceof AdminMetadataUpdater))
				((DefaultMetadataUpdater) metadataUpdater).requestUpdate();
			else
				metadataManager.requestUpdate();

			if (e instanceof ConnectionException) {  // from Admin
				Throwable t = e.getCause();
				if(t instanceof ConnectException) {
					throw (ConnectionException)e;
				}
			}
				
			if (e instanceof InvalidLoginCredentialsException)
				throw (InvalidLoginCredentialsException) e;

			log.warn("Error connecting to node {}", node, e);

			return false;
		}
		return true;
	}

	private void handleDisconnection(Node node, boolean disconnected, long now) {
		if(disconnected) {
			disconnected(node, now);
			if(!(metadataUpdater instanceof AdminMetadataUpdater))
			((DefaultMetadataUpdater)metadataUpdater).requestUpdate();
		}	
	}

	public static List<org.apache.kafka.common.Node> convertToKafkaNodes(List<Node> okafkaNodeList)
	{
		ArrayList<org.apache.kafka.common.Node> kafkaNodeList = new ArrayList<org.apache.kafka.common.Node>();
		for(Node n : okafkaNodeList)
		{
			kafkaNodeList.add((org.apache.kafka.common.Node)n);
		}
		return kafkaNodeList;
	}

	public static List<Node> convertToOracleNodes(List<org.apache.kafka.common.Node> apacheNodeList)
	{
		ArrayList<Node> oracleNodeList = new ArrayList<Node>();
		for(org.apache.kafka.common.Node n : apacheNodeList)
		{
			oracleNodeList.add((org.oracle.okafka.common.Node)n);
		}
		return oracleNodeList;
	}


	class DefaultMetadataUpdater implements MetadataUpdater {

		/* the current cluster metadata */
		private final Metadata metadata;

		/* true iff there is a metadata request that has been sent and for which we have not yet received a response */
		private boolean metadataFetchInProgress;

		DefaultMetadataUpdater(Metadata metadata) {
			this.metadata = metadata;
			this.metadataFetchInProgress = false;
		}

		@Override
		public List<org.apache.kafka.common.Node> fetchNodes() {
			return (metadata.fetch().nodes());
		}


		@Override
		public boolean isUpdateDue(long now) {
			return !this.metadataFetchInProgress && this.metadata.timeToNextUpdate(now) == 0;
		}

		@Override
		public long maybeUpdate(long now) {
			// should we update our metadata?
			long timeToNextMetadataUpdate = metadata.timeToNextUpdate(now);
			long waitForMetadataFetch = this.metadataFetchInProgress ? defaultRequestTimeoutMs : 0;
			long metadataTimeout = Math.max(timeToNextMetadataUpdate, waitForMetadataFetch);
			if (metadataTimeout > 0) {
				return metadataTimeout;
			}
			Node node = leastLoadedNode(now);
			if (node == null)  
			{
				if(metadata != null && metadata.fetch() != null)
				{
					List<Node> nodes = convertToOracleNodes(metadata.fetch().nodes());
					if(nodes != null)
					{
						String oldClusterId = null;
						try {
							oldClusterId = metadata.fetch().clusterResource().clusterId();
						} catch(Exception ignoreExcp) {}
						
						Node oldNode = nodes.get(0);
						Node newNode = new Node(oldNode.host(), oldNode.port(), oldNode.serviceName());
						newNode.setBootstrapFlag(true);
						log.info("MetaData Updater : Trying to connect to: " + newNode);
						boolean connected = initiateConnect(newNode,now);
						if(connected) {
							log.info("Connection Successful. Using this node to fetch metadata");
							node = newNode;
							Cluster renewCluster = new Cluster(oldClusterId, Collections.singletonList(newNode), new ArrayList<>(0),
									Collections.emptySet(), Collections.emptySet());
							this.metadata.update(renewCluster, Collections.<String>emptySet(), time.milliseconds(), true);
						}
						else {
							log.info("Not able to connect to "+ newNode);
						}
					}
				}
			}
			// If connection is not setup yet then return
			if(node == null)
			{
				log.error("Give up sending metadata request since no node is available. Retry after " + reconnectBackoffMs);
				return reconnectBackoffMs;
			}
			else {
				log.debug("May Update matadata with node : " + node);
			}

			return maybeUpdate(now, node);
		}

		public void handleCompletedMetadataResponse(org.apache.kafka.common.requests.RequestHeader requestHeader, long now, MetadataResponse response) {
			this.metadataFetchInProgress = false;
			//org.apache.kafka.common.Cluster cluster = response.cluster(metadata.getConfigs());
			org.apache.kafka.common.Cluster cluster = response.cluster();

			// check if any topics metadata failed to get updated
			Map<String, Exception> errors = response.topicErrors();
			if (!errors.isEmpty())
				log.warn("Error while fetching metadata for topics : {}", errors);

			// don't update the cluster if there are no valid nodes...the topic we want may still be in the process of being
			// created which means we will get errors and no nodes until it exists
			if (response.getException() == null && errors.isEmpty()) {
				this.metadata.update(cluster, null, now, false);
			} else {
				log.debug("Ignoring empty metadata response with correlation id {}.", requestHeader.correlationId());
				this.metadata.failedUpdate(now, null);
			}
		}

		//@Override
		public void handleDisconnection(String destination) {
			//not used
		}

		// @Override
		public void handleAuthenticationFailure(AuthenticationException exception) {
			metadataFetchInProgress = false;
			if (metadata.updateRequested())
				metadata.failedUpdate(time.milliseconds(), exception);
		}

		// @Override
		public void requestUpdate() {
			this.metadata.requestUpdate();
		}

		@Override
		public void close() {
			aqClient.close();
			this.metadata.close();
		}

		/**
		 * Add a metadata request to the list of sends if we can make one
		 */
		private long maybeUpdate(long now, Node node) {
			if (!canSendRequest(node, now)) {
				log.debug("Cannot send Request. connect Now to node: " + node);
				if (connectionStates.canConnect(node, now)) {
					// we don't have a connection to this node right now, make one
					// log.info(this.toString() + " Initialize connection to node {} for sending
					// metadata request", node);

					if (!initiateConnect(node, now))
						return reconnectBackoffMs;
				} else
					return reconnectBackoffMs;
			}
			this.metadataFetchInProgress = true;
			MetadataRequest.Builder metadataRequest;
			if (metadata.needMetadataForAllTopics()) {
				metadataRequest = MetadataRequest.Builder.allTopics();
			} else {
				List<String> topicList = new ArrayList<>(metadata.topics());
				metadataRequest = new MetadataRequest.Builder(topicList, metadata.allowAutoTopicCreation(), topicList);
			}
			log.debug("Sending metadata request {} to node {}", metadataRequest, node);
			sendInternalMetadataRequest(metadataRequest, node, now);
			return defaultRequestTimeoutMs;
		}

		@Override
		public void handleServerDisconnect(long now, String nodeId,
				Optional<AuthenticationException> maybeAuthException) {
			// TODO Auto-generated method stub

		}

		@Override
		public void handleFailedRequest(long now, Optional<KafkaException> maybeFatalException) {
			// TODO Auto-generated method stub

		}

		@Override
		public void handleSuccessfulResponse(org.apache.kafka.common.requests.RequestHeader requestHeader, long now,
				org.apache.kafka.common.requests.MetadataResponse metadataResponse) {
			// TODO Auto-generated method stub

		}

	}

	@Override
	public ClientRequest newClientRequest(Node node,
			AbstractRequest.Builder<?> requestBuilder,
			long createdTimeMs,
			boolean expectResponse) {
		return newClientRequest(node, requestBuilder, createdTimeMs, expectResponse, defaultRequestTimeoutMs, null);
	}

	@Override
	public ClientRequest newClientRequest(Node node,
			AbstractRequest.Builder<?> requestBuilder,
			long createdTimeMs,
			boolean expectResponse,
			int requestTimeoutMs,
			RequestCompletionHandler callback) {
		return new ClientRequest(""+node.id(), requestBuilder, correlation++, clientId, createdTimeMs, expectResponse,
				defaultRequestTimeoutMs, callback);
	}

}
