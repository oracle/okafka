package org.oracle.okafka.common.requests;

import java.util.List;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.requests.AbstractResponse;
import org.oracle.okafka.common.protocol.ApiKeys;

public class OffsetFetchRequest extends AbstractRequest{
	
	private static final List<TopicPartition> ALL_TOPIC_PARTITIONS = null;
    private final List<TopicPartition> topicPartitions;
	private final String groupId;

    public static class Builder extends AbstractRequest.Builder<OffsetFetchRequest> {
        private final List<TopicPartition> topicPartitions;
    	private final String groupId;

        public Builder(String groupId,
                       List<TopicPartition> partitions) {
            super(ApiKeys.OFFSET_FETCH);
            this.groupId = groupId;
            if(partitions!=null)
            	topicPartitions=partitions;
            else
            	topicPartitions = ALL_TOPIC_PARTITIONS;
            
        }

        boolean isAllTopicPartitions() {
            return this.topicPartitions == ALL_TOPIC_PARTITIONS;
        }

        @Override
        public OffsetFetchRequest build(short version) {
        
            return new OffsetFetchRequest(topicPartitions, groupId, version);
        }

        @Override
        public String toString() {
            return "(type=ListOffsetsRequest, " + "group-id:" + groupId + ", " + "TopicPartitions:" + topicPartitions.toString();
        }
    }

    public List<TopicPartition> partitions() {
        if (isAllPartitions()) {
            return null;
        }
        return topicPartitions;
    }

    public String groupId() {
        return this.groupId;
    }
    
    private OffsetFetchRequest(List<TopicPartition> topicPartitions, String groupId, short version) {
        super(ApiKeys.OFFSET_FETCH, version);
        this.topicPartitions = topicPartitions;
        this.groupId = groupId;
    }

    public boolean isAllPartitions() {
        return this.topicPartitions == ALL_TOPIC_PARTITIONS;
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
