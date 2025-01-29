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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.Uuid;
import org.oracle.okafka.clients.TopicTeqParameters;

/**
 * A detailed description of a single topic in the cluster.
 */
public class TopicDescription extends org.apache.kafka.clients.admin.TopicDescription{
	
	private final TopicTeqParameters topicParameters;
	
	public TopicDescription(String name, boolean internal, List<TopicPartitionInfo> partitions, TopicTeqParameters topicTeqParameters, Uuid topicId) {
		super(name, internal, partitions,Collections.emptySet(),topicId);
		this.topicParameters=topicTeqParameters;
		
	}
	
	@Override
	public boolean equals(final Object o) {
		Boolean superEqual = super.equals(o);
		if (superEqual) {
			final TopicDescription that = (TopicDescription) o;
			return this.topicParameters.equals(that.topicParameters);
		}
		return false;
	}
	
	@Override
    public int hashCode() {
        return Objects.hash(this.name(), this.isInternal(), this.partitions(), topicParameters);
    }
	
	private TopicTeqParameters topicTeqParameters() {
		return this.topicParameters;
	}
	
	


}
