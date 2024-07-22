/*
** OKafka Java Client version 23.4.
**
** Copyright (c) 2019, 2024 Oracle and/or its affiliates.
** Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.
*/

package org.oracle.okafka.clients.consumer.internals;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import java.util.Collection;

public class NoOpConsumerRebalanceListener implements ConsumerRebalanceListener{
	
	@Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {}

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {}

}
