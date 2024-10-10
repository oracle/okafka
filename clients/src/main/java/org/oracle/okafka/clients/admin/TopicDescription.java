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

import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartitionInfo;
import org.oracle.okafka.clients.TopicTeqParameters;

public class TopicDescription extends org.apache.kafka.clients.admin.TopicDescription{
	
	private final String name;
	private final TopicTeqParameters topicParameters;
	
	public TopicDescription(String name, boolean internal, List<TopicPartitionInfo> partitions, TopicTeqParameters topicTeqParameters) {
		super(name, internal, partitions);
		
		this.name=name;
		this.topicParameters=topicTeqParameters;
		
	}
	
	@Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final TopicDescription that = (TopicDescription) o;
        return this.isInternal() == that.isInternal() &&
            Objects.equals(name, that.name) &&
            Objects.equals(this.partitions(), that.partitions()) &&
            Objects.equals(this.topicParameters,that.topicParameters);
    }
	
	@Override
    public int hashCode() {
        return Objects.hash(name, this.isInternal(), this.partitions(),topicParameters);
    }
	
	public TopicTeqParameters topicTeqParameters() {
		return this.topicParameters;
	}
	
	


}
