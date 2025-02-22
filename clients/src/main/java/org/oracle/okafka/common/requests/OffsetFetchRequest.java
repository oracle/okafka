package org.oracle.okafka.common.requests;

import java.util.List;
import java.util.Map;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.requests.AbstractResponse;
import org.oracle.okafka.common.protocol.ApiKeys;

public class OffsetFetchRequest extends AbstractRequest{
	
	private static final List<TopicPartition> ALL_TOPIC_PARTITIONS = null;
    private final Map<String,List<TopicPartition>> perGroupTopicPartitions;

	public static class Builder extends AbstractRequest.Builder<OffsetFetchRequest> {
		private final Map<String, List<TopicPartition>> perGroupTopicPartitions;

		public Builder(Map<String, List<TopicPartition>> perGroupTopicPartitions) {
			super(ApiKeys.OFFSET_FETCH);
			this.perGroupTopicPartitions = perGroupTopicPartitions;
		}

		@Override
		public OffsetFetchRequest build(short version) {
			return new OffsetFetchRequest(perGroupTopicPartitions, version);
		}

		@Override
		public String toString() {
			return "(type=ListOffsetsRequest, " + ", " + "GroupIdTopicPartitions:" + perGroupTopicPartitions.toString();
		}
	}
  
    private OffsetFetchRequest(Map<String,List<TopicPartition>> perGroupTopicPartitions, short version) {
        super(ApiKeys.OFFSET_FETCH, version);
        this.perGroupTopicPartitions = perGroupTopicPartitions;
    }
    
    public Map<String,List<TopicPartition>> perGroupTopicpartitions(){
    	return perGroupTopicPartitions;
    }

	@Override
	public ApiMessage data() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
		// TODO Auto-generated method stub
		return null;
	}
}
