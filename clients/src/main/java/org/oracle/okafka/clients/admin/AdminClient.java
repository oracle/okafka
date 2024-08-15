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

package org.oracle.okafka.clients.admin;

import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

//import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigsOptions;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.AlterReplicaLogDirsOptions;
import org.apache.kafka.clients.admin.AlterReplicaLogDirsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.CreateAclsOptions;
import org.apache.kafka.clients.admin.CreateAclsResult;
import org.apache.kafka.clients.admin.CreateDelegationTokenOptions;
import org.apache.kafka.clients.admin.CreateDelegationTokenResult;
import org.apache.kafka.clients.admin.CreatePartitionsOptions;
import org.apache.kafka.clients.admin.CreatePartitionsResult;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteAclsOptions;
import org.apache.kafka.clients.admin.DeleteAclsResult;
import org.apache.kafka.clients.admin.DeleteConsumerGroupsOptions;
import org.apache.kafka.clients.admin.DeleteConsumerGroupsResult;
import org.apache.kafka.clients.admin.DeleteRecordsOptions;
import org.apache.kafka.clients.admin.DeleteRecordsResult;
import org.apache.kafka.clients.admin.DeleteTopicsOptions;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeAclsOptions;
import org.apache.kafka.clients.admin.DescribeAclsResult;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeConfigsOptions;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsOptions;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.admin.DescribeDelegationTokenOptions;
import org.apache.kafka.clients.admin.DescribeDelegationTokenResult;
import org.apache.kafka.clients.admin.DescribeLogDirsOptions;
import org.apache.kafka.clients.admin.DescribeLogDirsResult;
import org.apache.kafka.clients.admin.DescribeReplicaLogDirsOptions;
import org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ExpireDelegationTokenOptions;
import org.apache.kafka.clients.admin.ExpireDelegationTokenResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.admin.RenewDelegationTokenOptions;
import org.apache.kafka.clients.admin.RenewDelegationTokenResult;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.config.ConfigResource;

/**
  * The administrative client for Transactional Event Queues(TXEQ), which supports managing and inspecting topics.
 * For this release only creation of topic(s) and deletion of topic(s) is supported.
 * A topic can be created by invoking {@code #createTopics(Collection)} and deleted by invoking {@code #deleteTopics(Collection)} method.
 * <p>
 * Topic can be created with following configuration. 
 * <p>
 * retention.ms: Amount of time in milliseconds for which records stay in topic and are available for consumption. Internally, <i>retention.ms</i> value is rounded to the second. Default value for this parameter is 7 days.
 * </p>
 */
@InterfaceStability.Evolving
public abstract class AdminClient  implements Admin {

    /**
     * Create a new AdminClient with the given configuration.
     *
     * @param props The configuration.
     * @return The new KafkaAdminClient.
     */
	final static String DUMMY_BOOTSTRAP ="localhost:1521";
    public static AdminClient create(Properties props) {
    	String bootStrap = (String)props.get(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG);
    	if(bootStrap== null)
    	{
    		String secProtocol = props.getProperty(AdminClientConfig.SECURITY_PROTOCOL_CONFIG);
    		if(secProtocol != null && secProtocol.equalsIgnoreCase("SSL")) {
    			// Connect using Oracle Wallet and tnsnames.ora. 
    			// User does not need to know the database host ip and port.
    			props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, DUMMY_BOOTSTRAP);
    		}
    	}
        return KafkaAdminClient.createInternal(new org.oracle.okafka.clients.admin.AdminClientConfig(props), new KafkaAdminClient.TimeoutProcessorFactory());
    }

    /**
     * Create a new AdminClient with the given configuration.
     *
     * @param conf The configuration.
     * @return The new KafkaAdminClient.
     */
    public static AdminClient create(Map<String, Object> conf) {
    	
    	String bootStrap = (String)conf.get(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG);
    	if(bootStrap == null)
    	{
    		String setSecProtocol = (String)conf.get(AdminClientConfig.SECURITY_PROTOCOL_CONFIG);
    		if(setSecProtocol != null && setSecProtocol.equalsIgnoreCase("SSL")) 
    		{
    			// Connect using Wallet and TNSNAMES.ora. 
    			// User does not need to know the database host ip and port.
    			conf.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, DUMMY_BOOTSTRAP);
    		}
    	}
        return KafkaAdminClient.createInternal(new AdminClientConfig(conf), null);
    }

    /**
     * Close the AdminClient and release all associated resources.
     *
     * See {@link AdminClient#close(long, TimeUnit)}
     */
    @Override
    public void close() {
        close(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    }

    /**
     * Close the AdminClient and release all associated resources.
     *
     * The close operation has a grace period during which current operations will be allowed to
     * complete, specified by the given duration and time unit.
     * New operations will not be accepted during the grace period.  Once the grace period is over,
     * all operations that have not yet been completed will be aborted with a TimeoutException.
     *
     * @param duration  The duration to use for the wait time.
     * @param unit      The time unit to use for the wait time.
     */
    public abstract void close(long duration, TimeUnit unit);

    /**
     * Create a batch of new topics .This call supports only <code>retention.ms</code> option for topic creation.
     * This operation is not transactional so it may succeed for some topics while fail for others.
     * This method is not supported in preview release.
     * 
     * @param newTopics         The new topics to create.
     * @return                  The CreateTopicsResult.
     */
    public CreateTopicsResult createTopics(Collection<NewTopic> newTopics) {
        return createTopics(newTopics, new CreateTopicsOptions());
    }

    /**
     * Create a batch of new topics. This call supports only <code>retention.ms</code> option for topic creation.
     * This operation is not transactional so it may succeed for some topics while fail for others.
     * This method is not supported in preview release.
     *
     * @param newTopics         The new topics to create.
     * @param options           The options to use when creating the new topics.
     * @return                  The CreateTopicsResult.
     */
    public abstract CreateTopicsResult createTopics(Collection<NewTopic> newTopics,
                                                    CreateTopicsOptions options);

    /**
     * Delete a batch of topics. This call doen't consider options for topic deletion.
     * This operation is not transactional so it may succeed for some topics while fail for others.
     *
     * @param topics            The topic names to delete.
     * @return                  The DeleteTopicsResult.
     */
    public DeleteTopicsResult deleteTopics(Collection<String> topics) {
        return deleteTopics(topics, new DeleteTopicsOptions());
    }

    /**
     * Delete a batch of topics. This call doen't consider options for topic deletion.
     *
     * This operation is not transactional so it may succeed for some topics while fail for others.
     *
     * <code>delete.topic.enable</code> is true in Oracle TEQ.
     *
     * @param topics            The topic names to delete.
     * @param options           The options to use when deleting the topics.
     * @return                  The DeleteTopicsResult.
     */
    public abstract DeleteTopicsResult deleteTopics(Collection<String> topics, DeleteTopicsOptions options);

    /**
     * @hidden
     * This method is not yet supported.
     */
    public ListTopicsResult listTopics() {
        return listTopics(new ListTopicsOptions());
    }

    /**
     * @hidden
     * This method is not yet supported.
     */
    public abstract ListTopicsResult listTopics(ListTopicsOptions options);

    /**
     * @hidden
     * This method is not yet supported.
     */
    public DescribeTopicsResult describeTopics(Collection<String> topicNames) {
        return describeTopics(topicNames, new DescribeTopicsOptions());
    }

    /**
     * @hidden
     * This method is not yet supported.
     */
    public abstract DescribeTopicsResult describeTopics(Collection<String> topicNames,
                                                         DescribeTopicsOptions options);

    /**
     * @hidden
     * This method is not yet supported.
     */
    public DescribeClusterResult describeCluster() {
        return describeCluster(new DescribeClusterOptions());
    }

    /**
     * @hidden
     * This method is not yet supported.
     */
    public abstract DescribeClusterResult describeCluster(DescribeClusterOptions options);

    /**
     * @hidden
     * This method is not yet supported.
     */
    public DescribeAclsResult describeAcls(AclBindingFilter filter) {
        return describeAcls(filter, new DescribeAclsOptions());
    }

    /**
     * @hidden
     * This method is not yet supported.
     */
    public abstract DescribeAclsResult describeAcls(AclBindingFilter filter, DescribeAclsOptions options);

    /**
     * @hidden
     * This method is not yet supported.
     */
    public CreateAclsResult createAcls(Collection<AclBinding> acls) {
        return createAcls(acls, new CreateAclsOptions());
    }

    /**
     * @hidden
     * This method is not yet supported.
     */
    public abstract CreateAclsResult createAcls(Collection<AclBinding> acls, CreateAclsOptions options);

    /**
     * @hidden
     * This method is not yet supported.
     */
    public DeleteAclsResult deleteAcls(Collection<AclBindingFilter> filters) {
        return deleteAcls(filters, new DeleteAclsOptions());
    }

    /**
     * @hidden
     * This method is not yet supported.
     */
    public abstract DeleteAclsResult deleteAcls(Collection<AclBindingFilter> filters, DeleteAclsOptions options);


    /**
     * @hidden
     * This method is not yet supported.
     */
    public DescribeConfigsResult describeConfigs(Collection<ConfigResource> resources) {
        return describeConfigs(resources, new DescribeConfigsOptions());
    }

    /**
     * @hidden
     * This method is not yet supported.
     */
    public abstract DescribeConfigsResult describeConfigs(Collection<ConfigResource> resources,
                                                           DescribeConfigsOptions options);

    /**
     * @hidden
     * This method is not yet supported.
     */
    public AlterConfigsResult alterConfigs(Map<ConfigResource, Config> configs) {
        return alterConfigs(configs, new AlterConfigsOptions());
    }

    /**
     * @hidden
     * This method is not yet supported.
     */
    public abstract AlterConfigsResult alterConfigs(Map<ConfigResource, Config> configs, AlterConfigsOptions options);

    /**
     * @hidden
     * This method is not yet supported.
     */
    public AlterReplicaLogDirsResult alterReplicaLogDirs(Map<TopicPartitionReplica, String> replicaAssignment) {
        return alterReplicaLogDirs(replicaAssignment, new AlterReplicaLogDirsOptions());
    }

    /**
     * @hidden
     * This method is not yet supported.
     */
    public abstract AlterReplicaLogDirsResult alterReplicaLogDirs(Map<TopicPartitionReplica, String> replicaAssignment, AlterReplicaLogDirsOptions options);

    /**
     * @hidden
     * This method is not yet supported.
     */
    public DescribeLogDirsResult describeLogDirs(Collection<Integer> brokers) {
        return describeLogDirs(brokers, new DescribeLogDirsOptions());
    }

    /**
     * @hidden
     * This method is not yet supported.
     */
    public abstract DescribeLogDirsResult describeLogDirs(Collection<Integer> brokers, DescribeLogDirsOptions options);

    /**
     * @hidden
     * This method is not yet supported.
     */
    public DescribeReplicaLogDirsResult describeReplicaLogDirs(Collection<TopicPartitionReplica> replicas) {
        return describeReplicaLogDirs(replicas, new DescribeReplicaLogDirsOptions());
    }

    /**
     * @hidden
     * This method is not yet supported.
     */
    public abstract DescribeReplicaLogDirsResult describeReplicaLogDirs(Collection<TopicPartitionReplica> replicas, DescribeReplicaLogDirsOptions options);

    /**
     * @hidden
     * This method is not yet supported.
     */
    public CreatePartitionsResult createPartitions(Map<String, NewPartitions> newPartitions) {
        return createPartitions(newPartitions, new CreatePartitionsOptions());
    }

    /**
     * @hidden
     * This method is not yet supported.
     */
    public abstract CreatePartitionsResult createPartitions(Map<String, NewPartitions> newPartitions,
                                                            CreatePartitionsOptions options);

    /**
     * @hidden
     * This method is not yet supported.
     */
    public DeleteRecordsResult deleteRecords(Map<TopicPartition, RecordsToDelete> recordsToDelete) {
        return deleteRecords(recordsToDelete, new DeleteRecordsOptions());
    }

    /**
     * @hidden
     * This method is not yet supported.
     */
    public abstract DeleteRecordsResult deleteRecords(Map<TopicPartition, RecordsToDelete> recordsToDelete,
                                                      DeleteRecordsOptions options);

    /**
     * @hidden
     * This method is not yet supported.
     */
    public CreateDelegationTokenResult createDelegationToken() {
        return createDelegationToken(new CreateDelegationTokenOptions());
    }


    /**
     * @hidden
     * This method is not yet supported.
     */
    public abstract CreateDelegationTokenResult createDelegationToken(CreateDelegationTokenOptions options);


    /**
     * @hidden
     * This method is not yet supported.
     */
    public RenewDelegationTokenResult renewDelegationToken(byte[] hmac) {
        return renewDelegationToken(hmac, new RenewDelegationTokenOptions());
    }

    /**
     * @hidden
     * This method is not yet supported.
     */
    public abstract RenewDelegationTokenResult renewDelegationToken(byte[] hmac, RenewDelegationTokenOptions options);

    /**
     * @hidden
     * <This method is not yet supported.
     */
    public ExpireDelegationTokenResult expireDelegationToken(byte[] hmac) {
        return expireDelegationToken(hmac, new ExpireDelegationTokenOptions());
    }

    /**
     * @hidden
     * This method is not yet supported.
     */
    public abstract ExpireDelegationTokenResult expireDelegationToken(byte[] hmac, ExpireDelegationTokenOptions options);

    /**
     * @hidden
     * This method is not yet supported.
     */
    public DescribeDelegationTokenResult describeDelegationToken() {
        return describeDelegationToken(new DescribeDelegationTokenOptions());
    }

    /**
     * @hidden
     * This method is not yet supported.
     */
    public abstract DescribeDelegationTokenResult describeDelegationToken(DescribeDelegationTokenOptions options);

    /**
     * @hidden
     * This method is not yet supported.
     */
    public abstract DescribeConsumerGroupsResult describeConsumerGroups(Collection<String> groupIds,
                                                                        DescribeConsumerGroupsOptions options);

    /**
     * @hidden
     * This method is not yet supported.
     */
    public DescribeConsumerGroupsResult describeConsumerGroups(Collection<String> groupIds) {
        return describeConsumerGroups(groupIds, new DescribeConsumerGroupsOptions());
    }

    /**
     * @hidden
     * This method is not yet supported.
     */
    public abstract ListConsumerGroupsResult listConsumerGroups(ListConsumerGroupsOptions options);

    /**
     * @hidden
     * This method is not yet supported.
     */
    public ListConsumerGroupsResult listConsumerGroups() {
        return listConsumerGroups(new ListConsumerGroupsOptions());
    }

    /**
     * @hidden
     * This method is not yet supported.
     */
    public abstract ListConsumerGroupOffsetsResult listConsumerGroupOffsets(String groupId, ListConsumerGroupOffsetsOptions options);

    /**
     * @hidden
     * This method is not yet supported.
     */
    public ListConsumerGroupOffsetsResult listConsumerGroupOffsets(String groupId) {
        return listConsumerGroupOffsets(groupId, new ListConsumerGroupOffsetsOptions());
    }

    /**
     * @hidden
     * This method is not yet supported.
     */
    public abstract DeleteConsumerGroupsResult deleteConsumerGroups(Collection<String> groupIds, DeleteConsumerGroupsOptions options);

    /**
     * @hidden
     * This method is not yet supported.
     */
    public DeleteConsumerGroupsResult deleteConsumerGroups(Collection<String> groupIds) {
        return deleteConsumerGroups(groupIds, new DeleteConsumerGroupsOptions());
    }
}
