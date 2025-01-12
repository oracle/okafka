package org.oracle.okafka.common.requests;

import java.util.List;
import java.util.Map;

import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.utils.Utils;
import org.oracle.okafka.common.protocol.ApiKeys;

public class ListOffsetsRequest extends AbstractRequest {

	public static final long EARLIEST_TIMESTAMP = -2L;
	public static final long LATEST_TIMESTAMP = -1L;
	public static final long MAX_TIMESTAMP = -3L;

	private final Map<String, List<ListOffsetsPartition>> topicoffsetPartitionMap;

	public static class Builder extends AbstractRequest.Builder<ListOffsetsRequest> {
		private final Map<String, List<ListOffsetsPartition>> topicoffsetPartitionMap;

		public Builder(Map<String, List<ListOffsetsPartition>> topicoffsetPartitionMap) {
			super(ApiKeys.LIST_OFFSETS);
			this.topicoffsetPartitionMap = topicoffsetPartitionMap;
		}

		@Override
		public ListOffsetsRequest build(short version) {
			return new ListOffsetsRequest(topicoffsetPartitionMap, version);
		}

		@Override
		public String toString() {
			return "(type=ListOffsetsRequest, " + topicoffsetPartitionMap.toString();
		}
	}

	private ListOffsetsRequest(Map<String, List<ListOffsetsPartition>> topicoffsetPartitionMap, short version) {
		super(ApiKeys.LIST_OFFSETS, version);
		this.topicoffsetPartitionMap = topicoffsetPartitionMap;
	}

	public Map<String, List<ListOffsetsPartition>> getOffsetPartitionMap() {
		return topicoffsetPartitionMap;
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

	public static class ListOffsetsPartition {
		int partitionIndex;
		long timestamp;

		public ListOffsetsPartition() {
			this.partitionIndex = 0;
			this.timestamp = 0L;
		}

		@Override
		public boolean equals(Object obj) {
			if (!(obj instanceof ListOffsetsPartition))
				return false;
			ListOffsetsPartition other = (ListOffsetsPartition) obj;
			if (partitionIndex != other.partitionIndex)
				return false;
			if (timestamp != other.timestamp)
				return false;
			return true;
		}

		@Override
		public int hashCode() {
			int hashCode = 0;
			hashCode = 31 * hashCode + partitionIndex;
			hashCode = 31 * hashCode + ((int) (timestamp >> 32) ^ (int) timestamp);
			return hashCode;
		}

		@Override
		public String toString() {
			return "ListOffsetsPartition(" + "partitionIndex=" + partitionIndex + ", timestamp=" + timestamp + ")";
		}

		public int partitionIndex() {
			return this.partitionIndex;
		}

		public long timestamp() {
			return this.timestamp;
		}

		public ListOffsetsPartition setPartitionIndex(int v) {
			this.partitionIndex = v;
			return this;
		}

		public ListOffsetsPartition setTimestamp(long v) {
			this.timestamp = v;
			return this;
		}

	}

}
