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

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsOptions;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicCollection;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.ListOffsetsResult;

/**
 * The administrative client for Kafka, which supports managing and inspecting topics.
 * <p>
 * Instances returned from the {@code create} methods of this interface are guaranteed to be thread safe.
 * However, the {@link KafkaFuture KafkaFutures} returned from request methods are executed
 * by a single thread so it is important that any code which executes on that thread when they complete
 * (using {@link KafkaFuture#thenApply(KafkaFuture.Function)}, for example) doesn't block
 * for too long. If necessary, processing of results should be passed to another thread.
 * <p>
 * The operations exposed by Admin follow a consistent pattern:
 * <ul>
 *     <li>Admin instances should be created using {@link Admin#create(Properties)} or {@link Admin#create(Map)}</li>
 *     <li>Each operation typically has two overloaded methods, one which uses a default set of options and an
 *     overloaded method where the last parameter is an explicit options object.
 *     <li>The operation method's first parameter is a {@code Collection} of items to perform
 *     the operation on. Batching multiple requests into a single call is more efficient and should be
 *     preferred over multiple calls to the same method.
 *     <li>The operation methods execute asynchronously.
 *     <li>Each {@code xxx} operation method returns an {@code XxxResult} class with methods which expose
 *     {@link KafkaFuture} for accessing the result(s) of the operation.
 *     <li>Typically an {@code all()} method is provided for getting the overall success/failure of the batch and a
 *     {@code values()} method provided access to each item in a request batch.
 *     Other methods may also be provided.
 *     <li>For synchronous behaviour use {@link KafkaFuture#get()}
 * </ul>
 * <p>
 * Here is a simple example of using an Admin client instance to create a new topic:
 * <pre>
 * {@code
 * Properties props = new Properties();
 * props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
 *
 * try (Admin admin = Admin.create(props)) {
 *   String topicName = "my-topic";
 *   int partitions = 12;
 *   short replicationFactor = 3;
 *   // Create a compacted topic
 *   CreateTopicsResult result = admin.createTopics(Collections.singleton(
 *     new NewTopic(topicName, partitions, replicationFactor)
 *       .configs(Collections.singletonMap(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT)));
 *
 *   // Call values() to get the result for a specific topic
 *   KafkaFuture<Void> future = result.values().get(topicName);
 *
 *   // Call get() to block until the topic creation is complete or has failed
 *   // if creation failed the ExecutionException wraps the underlying cause.
 *   future.get();
 * }
 * }
 * </pre>
 *
 * <h3>Bootstrap and balancing</h3>
 * <p>
 * The {@code bootstrap.servers} config in the {@code Map} or {@code Properties} passed
 * to {@link Admin#create(Properties)} is only used for discovering the brokers in the cluster,
 * which the client will then connect to as needed.
 * As such, it is sufficient to include only two or three broker addresses to cope with the possibility of brokers
 * being unavailable.
 * <p>
 * Different operations necessitate requests being sent to different nodes in the cluster. For example
 * {@link #createTopics(Collection)} communicates with the controller, but {@link #describeTopics(Collection)}
 * can talk to any broker. When the recipient does not matter the instance will try to use the broker with the
 * fewest outstanding requests.
 * <p>
 * The client will transparently retry certain errors which are usually transient.
 * For example if the request for {@code createTopics()} get sent to a node which was not the controller
 * the metadata would be refreshed and the request re-sent to the controller.
  */
@InterfaceStability.Evolving
public interface Admin extends org.apache.kafka.clients.admin.Admin {

    /**
     * Create a new Admin with the given configuration.
     *
     * @param props The configuration.
     * @return The new KafkaAdminClient.
     */
    static org.apache.kafka.clients.admin.Admin create(Properties props) {
        return KafkaAdminClient.createInternal(new AdminClientConfig(props, true), null);
    }
   
    /**
     * Create a new Admin with the given configuration.
     *
     * @param conf The configuration.
     * @return The new KafkaAdminClient.
     */
    static org.apache.kafka.clients.admin.Admin create(Map<String, Object> conf) {
        return KafkaAdminClient.createInternal(new AdminClientConfig(conf, true), null);
    }

    /**
     * Close the Admin and release all associated resources.
     * <p>
     * See {@link #close(long, TimeUnit)}
     */
    @Override
    default void close() {
        close(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    }

    /**
     * Close the Admin and release all associated resources.
     * <p>
     * The close operation has a grace period during which current operations will be allowed to
     * complete, specified by the given duration and time unit.
     * New operations will not be accepted during the grace period. Once the grace period is over,
     * all operations that have not yet been completed will be aborted with a {@link org.apache.kafka.common.errors.TimeoutException}.
     *
     * @param duration The duration to use for the wait time.
     * @param unit     The time unit to use for the wait time.
     * @deprecated Since 2.2. Use {@link #close(Duration)} or {@link #close()}.
     */
    @Deprecated
    default void close(long duration, TimeUnit unit) {
        close(Duration.ofMillis(unit.toMillis(duration)));
    }

    /**
     * Close the Admin client and release all associated resources.
     * <p>
     * The close operation has a grace period during which current operations will be allowed to
     * complete, specified by the given duration.
     * New operations will not be accepted during the grace period. Once the grace period is over,
     * all operations that have not yet been completed will be aborted with a {@link org.apache.kafka.common.errors.TimeoutException}.
     *
     * @param timeout The time to use for the wait time.
     */
    void close(Duration timeout);

    /**
     * Create a batch of new topics with the default options.
     * <p>
     * This is a convenience method for {@link #createTopics(Collection, CreateTopicsOptions)} with default options.
     * See the overload for more details.
     * <p>
     * This operation is supported by brokers with version 0.10.1.0 or higher.
     *
     * @param newTopics The new topics to create.
     * @return The CreateTopicsResult.
     */
    default CreateTopicsResult createTopics(Collection<NewTopic> newTopics) {
        return createTopics(newTopics, new CreateTopicsOptions());
    }

    /**
     * Create a batch of new topics.
     * <p>
     * This operation is not transactional so it may succeed for some topics while fail for others.
     * <p>
     * It may take several seconds after {@link CreateTopicsResult} returns
     * success for all the brokers to become aware that the topics have been created.
     * During this time, {@link #listTopics()} and {@link #describeTopics(Collection)}
     * may not return information about the new topics.
     * <p>
     *
     * @param newTopics The new topics to create.
     * @param options   The options to use when creating the new topics.
     * @return The CreateTopicsResult.
     */
    CreateTopicsResult createTopics(Collection<NewTopic> newTopics, CreateTopicsOptions options);
    
    /**
     * This is a convenience method for {@link #deleteTopics(TopicCollection, DeleteTopicsOptions)}
     * with default options. See the overload for more details.
     * <p>
     *
     * @param topics The topic names to delete.
     * @return The DeleteTopicsResult.
     */
    default DeleteTopicsResult deleteTopics(Collection<String> topics) {
        return deleteTopics(TopicCollection.ofTopicNames(topics), new DeleteTopicsOptions());
    }

    /**
     * This is a convenience method for {@link #deleteTopics(TopicCollection, DeleteTopicsOptions)}
     * with default options. See the overload for more details.
     * <p>
     *
     * @param topics  The topic names to delete.
     * @param options The options to use when deleting the topics.
     * @return The DeleteTopicsResult.
     */
    default DeleteTopicsResult deleteTopics(Collection<String> topics, DeleteTopicsOptions options) {
        return deleteTopics(TopicCollection.ofTopicNames(topics), options);
    }

    /**
     * This is a convenience method for {@link #deleteTopics(TopicCollection, DeleteTopicsOptions)}
     * with default options. See the overload for more details.
     * <p>
     *
     * @param topics The topics to delete.
     * @return The DeleteTopicsResult.
     */
    default DeleteTopicsResult deleteTopics(TopicCollection topics) {
        return deleteTopics(topics, new DeleteTopicsOptions());
    }

    /**
     * Delete a batch of topics.
     * <p>
     * This operation is not transactional so it may succeed for some topics while fail for others.
     * <p>
     * It may take several seconds after the {@link DeleteTopicsResult} returns
     * success for all the brokers to become aware that the topics are gone.
     * During this time, {@link #listTopics()} and {@link #describeTopics(Collection)}
     * may continue to return information about the deleted topics.
     * <p>
     * If delete.topic.enable is false on the brokers, deleteTopics will mark
     * the topics for deletion, but not actually delete them. The futures will
     * return successfully in this case.
     * <p>
     *
     * @param topics  The topics to delete.
     * @param options The options to use when deleting the topics.
     * @return The DeleteTopicsResult.
     */
    DeleteTopicsResult deleteTopics(TopicCollection topics, DeleteTopicsOptions options);

    /**
     * List the topics available in the cluster with the default options.
     * <p>
     * This is a convenience method for {@link #listTopics(ListTopicsOptions)} with default options.
     * See the overload for more details.
     *
     * @return The ListTopicsResult.
     */
    default ListTopicsResult listTopics() {
        return listTopics(new ListTopicsOptions());
    }

    /**
     * List the topics available in the cluster.
     *
     * @param options The options to use when listing the topics.
     * @return The ListTopicsResult.
     */
    ListTopicsResult listTopics(ListTopicsOptions options);

    /**
     * Describe some topics in the cluster, with the default options.
     * <p>
     * This is a convenience method for {@link #describeTopics(Collection, DescribeTopicsOptions)} with
     * default options. See the overload for more details.
     *
     * @param topicNames The names of the topics to describe.
     * @return The DescribeTopicsResult.
     */
    default DescribeTopicsResult describeTopics(Collection<String> topicNames) {
        return describeTopics(topicNames, new DescribeTopicsOptions());
    }

    /**
     * Describe some topics in the cluster.
     *
     * @param topicNames The names of the topics to describe.
     * @param options    The options to use when describing the topic.
     * @return The DescribeTopicsResult.
     */
    default DescribeTopicsResult describeTopics(Collection<String> topicNames, DescribeTopicsOptions options) {
        return describeTopics(TopicCollection.ofTopicNames(topicNames), options);
    }

    /**
     * This is a convenience method for {@link #describeTopics(TopicCollection, DescribeTopicsOptions)}
     * with default options. See the overload for more details.
     * <p>
     *
     * @param topics The topics to describe.
     * @return The DescribeTopicsResult.
     */
    default DescribeTopicsResult describeTopics(TopicCollection topics) {
        return describeTopics(topics, new DescribeTopicsOptions());
    }

    /**
     * Describe some topics in the cluster.
     *
     * @param topics  The topics to describe.
     * @param options The options to use when describing the topics.
     * @return The DescribeTopicsResult.
     */
    DescribeTopicsResult describeTopics(TopicCollection topics, DescribeTopicsOptions options);

    /**
     * <p>List offset for the specified partitions and OffsetSpec. This operation enables to find
     * the beginning offset, end offset as well as the offset matching a timestamp in partitions.
     *
     * <p>This is a convenience method for {@link #listOffsets(Map, ListOffsetsOptions)}
     *
     * @param topicPartitionOffsets The mapping from partition to the OffsetSpec to look up.
     * @return The ListOffsetsResult.
     */
    default ListOffsetsResult listOffsets(Map<TopicPartition, OffsetSpec> topicPartitionOffsets) {
        return listOffsets(topicPartitionOffsets, new ListOffsetsOptions());
    }

    /**
     * <p>List offset for the specified partitions. This operation enables to find
     * the beginning offset, end offset as well as the offset matching a timestamp in partitions.
     *
     * @param topicPartitionOffsets The mapping from partition to the OffsetSpec to look up.
     * @param options The options to use when retrieving the offsets
     * @return The ListOffsetsResult.
     */
    ListOffsetsResult listOffsets(Map<TopicPartition, OffsetSpec> topicPartitionOffsets, ListOffsetsOptions options);
    
}