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

//import org.oracle.okafka.common.Cluster;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.oracle.okafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.sql.SQLException;
import java.util.StringTokenizer;

/**
 * A class encapsulating some of the logic around metadata.
 * <p>
 * This class is shared by the client thread (for partitioning) and the background sender thread.
 *
 * Metadata is maintained for only a subset of topics, which can be added to over time. When we request metadata for a
 * topic we don't have any metadata for it will trigger a metadata update.
 * <p>
 * If topic expiry is enabled for the metadata, any topic that has not been used within the expiry interval
 * is removed from the metadata refresh set after an update. Consumers disable topic expiry since they explicitly
 * manage topics while producers rely on topic expiry to limit the refresh set.
 */
public final class Metadata implements Closeable {

	private static final Logger log = LoggerFactory.getLogger(Metadata.class);

	public static final long TOPIC_EXPIRY_MS = 5 * 60 * 1000;
	private static final long TOPIC_EXPIRY_NEEDS_UPDATE = -1L;

	private final long refreshBackoffMs;
	private final long metadataExpireMs;
	private int version;
	private long lastRefreshMs;
	private long lastSuccessfulRefreshMs;
	private AuthenticationException authenticationException;
	private Cluster cluster;
	private boolean isBootStrap;
	private boolean needUpdate;
	/* Topics with expiry time */
	private final Map<String, Long> topics;
	private final List<Listener> listeners;
	private final ClusterResourceListeners clusterResourceListeners;
	private boolean needMetadataForAllTopics;
	private final boolean allowAutoTopicCreation;
	private final boolean topicExpiryEnabled;
	private boolean isClosed;
	private final AbstractConfig configs;
	HashMap<String, Node> clusterLeaderMap = new HashMap<String, Node>();
	private KafkaException fatalException;
	int dbMajorVersion = 23;
	int dbMinorVersion = 1;
	public final HashMap<String, TopicTeqParameters> topicParaMap = new HashMap<>();
	
	public Metadata(long refreshBackoffMs, long metadataExpireMs, boolean allowAutoTopicCreation, AbstractConfig configs) {
		this(refreshBackoffMs, metadataExpireMs, allowAutoTopicCreation, false, new ClusterResourceListeners(), configs);
	}

	/**
	 * Create a new Metadata instance
	 * @param refreshBackoffMs The minimum amount of time that must expire between metadata refreshes to avoid busy
	 *        polling
	 * @param metadataExpireMs The maximum amount of time that metadata can be retained without refresh
	 * @param allowAutoTopicCreation If this and the broker config 'auto.create.topics.enable' are true, topics that
	 *                               don't exist will be created by the broker when a metadata request is sent
	 * @param topicExpiryEnabled If true, enable expiry of unused topics
	 * @param clusterResourceListeners List of ClusterResourceListeners which will receive metadata updates.
	 */
	public Metadata(long refreshBackoffMs, long metadataExpireMs, boolean allowAutoTopicCreation,
			boolean topicExpiryEnabled, ClusterResourceListeners clusterResourceListeners, AbstractConfig configs) {
		this.refreshBackoffMs = refreshBackoffMs;
		this.metadataExpireMs = metadataExpireMs;
		this.allowAutoTopicCreation = allowAutoTopicCreation;
		this.topicExpiryEnabled = topicExpiryEnabled;
		this.lastRefreshMs = 0L;
		this.lastSuccessfulRefreshMs = 0L;
		this.version = 0;
		this.cluster = Cluster.empty();
		this.needUpdate = false;
		this.topics = new HashMap<>();
		this.listeners = new ArrayList<>();
		this.clusterResourceListeners = clusterResourceListeners;
		this.needMetadataForAllTopics = false;
		this.isClosed = false;
		this.configs = configs;
		this.isBootStrap=true;
	}

	/**
	 * Get the current cluster info without blocking
	 */
	public synchronized Cluster fetch() {
		return this.cluster;
	}

	/**
	 * Add the topic to maintain in the metadata. If topic expiry is enabled, expiry time
	 * will be reset on the next update.
	 */
	public synchronized void add(String topic) {
		Objects.requireNonNull(topic, "topic cannot be null");
		if (topics.put(topic, TOPIC_EXPIRY_NEEDS_UPDATE) == null) {
			// requestUpdateForNewTopics();
		}
	}
	// Changes for 2.8.1  See if requestUpdateForNewTopics needs to be invoked
	/**
	 * Add the topic to maintain in the metadata. If topic expiry is enabled, expiry time
	 * will be reset on the next update.
	 */
	public synchronized void add(String topic, long timeout) {
		Objects.requireNonNull(topic, "topic cannot be null");
		if (topics.put(topic, timeout) == null) {
			// requestUpdateForNewTopics();
		}
	}


	/**
	 * The next time to update the cluster info is the maximum of the time the current info will expire and the time the
	 * current info can be updated (i.e. backoff time has elapsed); If an update has been request then the expiry time
	 * is now
	 */
	public synchronized long timeToNextUpdate(long nowMs) {
		long timeToExpire = needUpdate ? 0 : Math.max(this.lastSuccessfulRefreshMs + this.metadataExpireMs - nowMs, 0);
		
		long timeToAllowUpdate = this.lastRefreshMs + this.refreshBackoffMs - nowMs;
		return Math.max(timeToExpire, timeToAllowUpdate);
	}

	/**
	 * Request an update of the current cluster metadata info, return the current version before the update
	 */
	public synchronized int requestUpdate() {
		this.needUpdate = true;
		return this.version;
	}

	/**
	 * Check whether an update has been explicitly requested.
	 * @return true if an update was requested, false otherwise
	 */
	public synchronized boolean updateRequested() {
		return this.needUpdate;
	}

	/**
	 * If any non-retriable authentication exceptions were encountered during
	 * metadata update, clear and return the exception.
	 */
	public synchronized AuthenticationException getAndClearAuthenticationException() {
		if (authenticationException != null) {
			AuthenticationException exception = authenticationException;
			authenticationException = null;
			return exception;
		} else
			return null;
	}

	/**
	 * Wait for metadata update until the current version is larger than the last version we know of
	 */
	public synchronized void awaitUpdate(final int lastVersion, final long maxWaitMs) throws InterruptedException {
		if (maxWaitMs < 0)
			throw new IllegalArgumentException("Max time to wait for metadata updates should not be < 0 milliseconds");

		long begin = System.currentTimeMillis();
		long remainingWaitMs = maxWaitMs;
		while ((this.version <= lastVersion) && !isClosed()) {
			AuthenticationException ex = getAndClearAuthenticationException();
			if (ex != null)
				throw ex;
			if (remainingWaitMs != 0)
				wait(remainingWaitMs);
			long elapsed = System.currentTimeMillis() - begin;
			if (elapsed >= maxWaitMs)
				throw new TimeoutException("Failed to update metadata after " + maxWaitMs + " ms.");
			remainingWaitMs = maxWaitMs - elapsed;
		}
		if (isClosed())
			throw new KafkaException("Requested metadata update after close");
	}

	/**
	 * Replace the current set of topics maintained to the one provided.
	 * If topic expiry is enabled, expiry time of the topics will be
	 * reset on the next update.
	 * @param topics
	 */
	public synchronized void setTopics(Collection<String> topics) {
		if (!this.topics.keySet().containsAll(topics)) {
			requestUpdateForNewTopics();
		}
		this.topics.clear();
		for (String topic : topics)
			this.topics.put(topic, TOPIC_EXPIRY_NEEDS_UPDATE);
	}

	/**
	 * Get the list of topics we are currently maintaining metadata for
	 */
	public synchronized Set<String> topics() {
		return new HashSet<>(this.topics.keySet());
	}

	/**
	 * Check if a topic is already in the topic set.
	 * @param topic topic to check
	 * @return true if the topic exists, false otherwise
	 */
	public synchronized boolean containsTopic(String topic) {
		return this.topics.containsKey(topic);
	}

	/**
	 * Updates the cluster metadata. If topic expiry is enabled, expiry time
	 * is set for topics if required and expired topics are removed from the metadata.
	 *
	 * @param newCluster the cluster containing metadata for topics with valid metadata
	 * @param unavailableTopics topics which are non-existent or have one or more partitions whose
	 *        leader is not known
	 * @param now current time in milliseconds
	 */
	public synchronized void update(Cluster newCluster, Set<String> unavailableTopics, long now, boolean bootstrap) {
		
		log.debug("Update Metadata. isBootstap? " + bootstrap);
		Objects.requireNonNull(newCluster, "cluster should not be null");
		if (isClosed())
			throw new IllegalStateException("Update requested after metadata close");

		this.needUpdate = false;
		this.lastRefreshMs = now;
		this.lastSuccessfulRefreshMs = now;
		this.version += 1;
		if(this.isBootStrap)
			for(org.apache.kafka.common.Node n : this.cluster.nodes())
				((Node)n).setBootstrapFlag(false);
		this.isBootStrap = bootstrap;
		
		/* if (topicExpiryEnabled) {
            // Handle expiry of topics from the metadata refresh set.
            for (Iterator<Map.Entry<String, Long>> it = topics.entrySet().iterator(); it.hasNext(); ) {
                Map.Entry<String, Long> entry = it.next();
                long expireMs = entry.getValue();
                if (expireMs == TOPIC_EXPIRY_NEEDS_UPDATE)
                    entry.setValue(now + TOPIC_EXPIRY_MS);
                else if (expireMs <= now) {
                    log.debug("Removing unused topic {} from the metadata list, expiryMs {} now {}", entry.getKey(), expireMs, now);
                }
            }
        } */

		for (Listener listener: listeners)
			listener.onMetadataUpdate(newCluster, unavailableTopics);
		
		if(bootstrap)
		{
			this.cluster = newCluster;
			log.debug("Updated cluster metadata version {} to {}", this.version, this.cluster);
			return;
		}

		
		String previousClusterId = cluster.clusterResource().clusterId();
		Node newLeaderNode = null;
		
		newLeaderNode = getLeaderNode(this.cluster, newCluster); 
		
		/* If a node previously present is not available in newCluster, then
		 * we still want to keep it because 
		 * existing nodes may go down and at the same time previously disconnected nodes may come up */
		ArrayList<org.apache.kafka.common.Node> newClusterNodes = new ArrayList<org.apache.kafka.common.Node>();
		newClusterNodes.addAll(newCluster.nodes());
		
		boolean oldNodesAdded = false;
		for(org.apache.kafka.common.Node oldNode : cluster.nodes())
		{
			org.apache.kafka.common.Node nodeById = newCluster.nodeById(oldNode.id());
			if(nodeById == null)
			{
				newClusterNodes.add(oldNode);
				//newCluster.nodes().add(oldNode);
				oldNodesAdded = true;
				//new Cluster(clusterId, cluster.nodes(), partitionInfos, unauthorizedTopics, internalTopics, controller);
				
				log.debug("Added Down Node  " + oldNode );
				
			}
		}
		if(oldNodesAdded)
		{
			// Create a new cluster with All previous and current Node. 
			// Maintain the PartitionInfo of the latest cluster
			Cluster newClusterWithOldNodes = getClusterForCurrentTopics(newCluster, newClusterNodes);
			newCluster = newClusterWithOldNodes;
		}
		
		if (this.needMetadataForAllTopics) {
			log.debug("needMetadataForAllTopics = " + needMetadataForAllTopics);
		
			// the listener may change the interested topics, which could cause another metadata refresh.
			// If we have already fetched all topics, however, another fetch should be unnecessary.
			this.needUpdate = false;
			this.cluster = getClusterForCurrentTopics(newCluster, null);
		} else {
			this.cluster = newCluster;
		}
		//Changes for 2.8.1:
		setLeader(newLeaderNode);
		//clusterLeaderMap.put(cluster.clusterResource().clusterId()+"_"+version, newLeaderNode);

		// The bootstrap cluster is guaranteed not to have any useful information
		if (!newCluster.isBootstrapConfigured()) {
			String newClusterId = newCluster.clusterResource().clusterId();
			if (newClusterId == null ? previousClusterId != null : !newClusterId.equals(previousClusterId))
				log.info("Cluster ID: {}", newClusterId);
			clusterResourceListeners.onUpdate(newCluster.clusterResource());
		}

		notifyAll();
		log.debug("Updated cluster metadata version {} to {}", this.version, this.cluster);
	}

	private  Node  getLeaderNode(Cluster oldCluster, Cluster newCluster)
	{
		if(oldCluster == null || newCluster == null)
			return null;

		Node oldLeader = getLeader(this.version-1);
		
		if(oldLeader != null)
		{
			log.debug("Update Metadata: OldLeaderNode as of version"+(this.version-1)+": " + oldLeader);
		}
		else
			log.debug("Update Metadata: No old leader as of now for cluster " + 
					cluster.clusterResource().clusterId() + " for version " + (this.version-1));
		
		if(oldLeader == null)
		{
			oldLeader = (org.oracle.okafka.common.Node)oldCluster.controller();
			log.debug("Update Metadata: Checking with cluster Controller node " + oldLeader);
		}

		if(oldLeader == null)
		{
			log.debug("No Old Leader. Returning new cluster's controller node" + newCluster.controller());
			return (org.oracle.okafka.common.Node)newCluster.controller();
		}

		List<Node> newNodeList = NetworkClient.convertToOracleNodes(newCluster.nodes());
		if(newNodeList == null || newNodeList.size() == 0)
			return null;

		boolean portChange = true;
		boolean hostChange = true;
		boolean serviceChange = true;
		Node bestSoFar = null;
		Node newLeader = null;
		for(Node newNode: newNodeList)
		{
			portChange = false;
			hostChange = false;
			serviceChange = false;
			if(newNode.hashCode() == oldLeader.hashCode())
			{
				newLeader = newNode;
				break;
			}
			else if(newNode.id() == oldLeader.id() && newNode.user().equalsIgnoreCase(oldLeader.user()))
			{
				if(!newNode.serviceName().equalsIgnoreCase(oldLeader.serviceName()))
				{
					serviceChange = true;
				}
				else if(newNode.port() != oldLeader.port()) {
					portChange = true;
				}
				else if(!newNode.host().equalsIgnoreCase(oldLeader.host()))
				{
					hostChange = true;
				}

				if(portChange || hostChange || serviceChange)
				{
					bestSoFar = newNode;
				}
				else
				{
					newLeader = newNode;
					break;
				}
			}
		}
		if(newLeader != null)
		{
			// Remove the newly added node which is same as old leader. Keep the old leader object in use.
			newNodeList.remove(newLeader);
			newNodeList.add(0,oldLeader);

			//setLeader(oldLeader);
			log.debug("Old Leader continuing " + oldLeader);
			return oldLeader;
		}
		else if(bestSoFar != null)
		{
			/* Put Best So Far at the top. 
    		    Do not set leader here. The oldLeader is no more alive.
    		    Connection to the old leader would have disconnected or will be disconnected.
    		    New leader will be found eventually. */
			newNodeList.remove(bestSoFar);
			newNodeList.add(0,bestSoFar);
			log.debug("Update Metadata: New Leader to be chosen. Potential candidate " + bestSoFar);
		}
		else
		{
			 return (org.oracle.okafka.common.Node)newCluster.controller();
		}
		return null;
	}
	/**
	 * Record an attempt to update the metadata that failed. We need to keep track of this
	 * to avoid retrying immediately.
	 */
	public synchronized void failedUpdate(long now, AuthenticationException authenticationException) {
		this.lastRefreshMs = now;
		this.authenticationException = authenticationException;
		if (authenticationException != null)
			this.notifyAll();
	}

	/**
	 * @return The current metadata version
	 */
	public synchronized int version() {
		return this.version;
	}

	/**
	 * The last time metadata was successfully updated.
	 */
	public synchronized long lastSuccessfulUpdate() {
		return this.lastSuccessfulRefreshMs;
	}

	public boolean allowAutoTopicCreation() {
		return allowAutoTopicCreation;
	}

	/**
	 * Set state to indicate if metadata for all topics in Kafka cluster is required or not.
	 * @param needMetadataForAllTopics boolean indicating need for metadata of all topics in cluster.
	 */
	public synchronized void needMetadataForAllTopics(boolean needMetadataForAllTopics) {
		if (needMetadataForAllTopics && !this.needMetadataForAllTopics) {
			// requestUpdateForNewTopics();
		}
		this.needMetadataForAllTopics = needMetadataForAllTopics;
	}

	/**
	 * Get whether metadata for all topics is needed or not
	 */
	public synchronized boolean needMetadataForAllTopics() {
		return this.needMetadataForAllTopics;
	}

	/**
	 * Add a Metadata listener that gets notified of metadata updates
	 */
	public synchronized void addListener(Listener listener) {
		this.listeners.add(listener);
	}

	/**
	 * Stop notifying the listener of metadata updates
	 */
	public synchronized void removeListener(Listener listener) {
		this.listeners.remove(listener);
	}

	/**
	 * "Close" this metadata instance to indicate that metadata updates are no longer possible. This is typically used
	 * when the thread responsible for performing metadata updates is exiting and needs a way to relay this information
	 * to any other thread(s) that could potentially wait on metadata update to come through.
	 */
	@Override
	public synchronized void close() {
		this.isClosed = true;
		/*try {
        	this.cluster.close();
        } catch(SQLException sql) {
        	log.error("failed to close cluster",sql);
        }*/
		this.notifyAll();
	}

	/**
	 * Check if this metadata instance has been closed. See {@link #close()} for more information.
	 * @return True if this instance has been closed; false otherwise
	 */
	public synchronized boolean isClosed() {
		return this.isClosed;
	}

	/**
	 * MetadataUpdate Listener
	 */
	public interface Listener {
		/**
		 * Callback invoked on metadata update.
		 *
		 * @param cluster the cluster containing metadata for topics with valid metadata
		 * @param unavailableTopics topics which are non-existent or have one or more partitions whose
		 *        leader is not known
		 */
		void onMetadataUpdate(Cluster cluster, Set<String> unavailableTopics);
	}

	private synchronized void requestUpdateForNewTopics() {
		// Override the timestamp of last refresh to let immediate update.
		this.lastRefreshMs = 0;
		requestUpdate();
	}

	private Cluster getClusterForCurrentTopics(Cluster cluster, List<org.apache.kafka.common.Node> newNodeList) {
		Set<String> unauthorizedTopics = new HashSet<>();
		
		Collection<PartitionInfo> partitionInfos = new ArrayList<>();
		
		if(newNodeList == null)
			newNodeList = cluster.nodes();
		
		Set<String> internalTopics = Collections.emptySet();
		Node controller = null;
		String clusterId = null;
		if (cluster != null) {
			clusterId = cluster.clusterResource().clusterId();
			internalTopics = cluster.internalTopics();
			unauthorizedTopics.addAll(cluster.unauthorizedTopics());
			unauthorizedTopics.retainAll(this.topics.keySet());

			for (String topic : this.topics.keySet()) {
				List<PartitionInfo> partitionInfoList = cluster.partitionsForTopic(topic);
				if (!partitionInfoList.isEmpty()) {
					partitionInfos.addAll(partitionInfoList);
				}
			}
			controller  = (org.oracle.okafka.common.Node)cluster.controller();
		}
		return new Cluster(clusterId, newNodeList, partitionInfos, unauthorizedTopics, internalTopics, controller);//, cluster.getConfigs());
	}

	/*public synchronized long getUpdate(String topic,Integer partition, long maxWaitMs) {
    	if (maxWaitMs < 0)
            throw new IllegalArgumentException("Max time to wait for metadata updates should not be < 0 milliseconds");
    	if (isClosed())
            throw new KafkaException("Requested metadata update after close");
    	int partitionsCount = cluster.partitionCountForTopic(topic);
    	long elapsed = 0;
    	if (partition >= partitionsCount) {
    		log.trace("Requesting metadata update for topic {}.", topic);
    		long begin = Time.SYSTEM.milliseconds();
    		int partitions = cluster.getPartitions(topic);
    		elapsed = Time.SYSTEM.milliseconds() - begin;
    		if (elapsed >= maxWaitMs)
                throw new TimeoutException("Failed to update metadata after " + maxWaitMs + " ms.");
    		if(partition >= partitions) 
    			throw new KafkaException(
                    String.format("Invalid partition given with record: %d is not in the range [0...%d).", partition, partitionsCount));
    		cluster.updatePartitions(topic, partitions);
        	topics.put(topic, TOPIC_EXPIRY_NEEDS_UPDATE);
        }
    	return elapsed;
    }*/

	public AbstractConfig getConfigs() {
		return this.configs;
	}

	/*
	 * Return org.oracle.okafka.common.Node by Node Id
	 */
	public Node getNodeById(int id)
	{
		if(cluster!=null)
			return (Node)cluster.nodeById(id);
		return null;
	}

	public void setLeader(Node leaderNode)
	{
		if(cluster != null)
		{
			clusterLeaderMap.put(cluster.clusterResource().clusterId()+"_"+version, leaderNode);
			log.debug("Leader Node for Version " +
						cluster.clusterResource().clusterId()+"_"+version + ":" + leaderNode);
		}
	}

	public Node getLeader()
	{
		return getLeader(this.version);
	}

	public Node getLeader(int version)
	{
		if(cluster != null)
			return getLeader(cluster.clusterResource().clusterId(), version);
		else
			return null;
	}

	public Node getLeader(String clusterId, int version)
	{
		return clusterLeaderMap.get(clusterId+"_"+version);
	}

	public boolean isBootstrap()
	{
		return isBootStrap;
	}

	/**
	 * Propagate a fatal error which affects the ability to fetch metadata for the cluster.
	 * Two examples are authentication and unsupported version exceptions.
	 *
	 * @param exception The fatal exception
	 */
	public synchronized void fatalError(KafkaException exception) {
		this.fatalException = exception;
	}

	/**
	 * If any fatal exceptions were encountered during metadata update, throw the exception. This is used by
	 * the producer to abort waiting for metadata if there were fatal exceptions (e.g. authentication failures)
	 * in the last metadata update.
	 */
	protected synchronized void maybeThrowFatalException() {
		KafkaException metadataException = this.fatalException;
		if (metadataException != null) {
			fatalException = null;
			throw metadataException;
		}
	}
	// Parse DB Major and Minor Version
	public void setDBVersion(String dbVersion)
	{
		try {
			StringTokenizer stn = new StringTokenizer(dbVersion,".");
			setDBMajorVersion(Integer.parseInt(stn.nextToken()));
			setDBMinorVersion(Integer.parseInt(stn.nextToken()));
		}catch(Exception e) {

		}
	}

	public void setDBMajorVersion(int dbMVersion)
	{
		this.dbMajorVersion = dbMVersion;
	}

	public void setDBMinorVersion(int dbMinorVersion)
	{
		this.dbMinorVersion = dbMinorVersion;
	}
	public int getDBMajorVersion()
	{
		return this.dbMajorVersion;
	}
	public int getDBMinorVersion()
	{
		return this.dbMinorVersion;
	}

}
