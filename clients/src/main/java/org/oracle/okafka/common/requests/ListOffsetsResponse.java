package org.oracle.okafka.common.requests;

import java.util.List;
import java.util.Map;

import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.oracle.okafka.common.errors.FeatureNotSupportedException;
import org.oracle.okafka.common.protocol.ApiKeys;

public class ListOffsetsResponse extends AbstractResponse {
	final Map<String, List<ListOffsetsPartitionResponse>> offsetPartitionResponseMap;
	private Exception exception;

	public ListOffsetsResponse(Map<String, List<ListOffsetsPartitionResponse>> offsetPartitionResponseMap) {
		super(ApiKeys.LIST_OFFSETS);
		this.offsetPartitionResponseMap = offsetPartitionResponseMap;
		exception = null;
	}

	public Map<String, List<ListOffsetsPartitionResponse>> getOffsetPartitionResponseMap() {
		return offsetPartitionResponseMap;
	}

	public void setException(Exception ex) {
		if (exception != null) {
			exception = ex;
		}
	}

	public Exception getException() {
		return exception;
	}

	public static class ListOffsetsPartitionResponse {
		int partitionIndex;
		Exception error;
		long timestamp;
		long offset;
		
		public ListOffsetsPartitionResponse() {
            this.partitionIndex = 0;
            this.error = null;
            this.timestamp = -1L;
            this.offset = -1L;
        }

		@Override
		public boolean equals(Object obj) {
			if (!(obj instanceof ListOffsetsPartitionResponse))
				return false;
			ListOffsetsPartitionResponse other = (ListOffsetsPartitionResponse) obj;
			if (partitionIndex != other.partitionIndex)
				return false;
			if (error != other.error)
				return false;
			if (timestamp != other.timestamp)
				return false;
			if (offset != other.offset)
				return false;
			return true;
		}

		@Override
		public int hashCode() {
			int hashCode = 0;
			hashCode = 31 * hashCode + partitionIndex;
			hashCode = 31 * hashCode + ((int) (timestamp >> 32) ^ (int) timestamp);
			hashCode = 31 * hashCode + ((int) (offset >> 32) ^ (int) offset);
			return hashCode;
		}

		@Override
		public String toString() {
			return "ListOffsetsPartitionResponse(" + "partitionIndex=" + partitionIndex + ", error=" + error
					+ ", timestamp=" + timestamp + ", offset=" + offset + ")";
		}

		public int partitionIndex() {
			return this.partitionIndex;
		}

		public Exception getError() {
			return this.error;
		}

		public long timestamp() {
			return this.timestamp;
		}

		public long offset() {
			return this.offset;
		}

		public ListOffsetsPartitionResponse setPartitionIndex(int v) {
			this.partitionIndex = v;
			return this;
		}

		public ListOffsetsPartitionResponse setError(Exception v) {
			this.error = v;
			return this;
		}

		public ListOffsetsPartitionResponse setTimestamp(long v) {
			this.timestamp = v;
			return this;
		}

		public ListOffsetsPartitionResponse setOffset(long v) {
			this.offset = v;
			return this;
		}
	}

	@Override
	public ApiMessage data() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<Errors, Integer> errorCounts() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int throttleTimeMs() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void maybeSetThrottleTimeMs(int throttleTimeMs) {
		throw new FeatureNotSupportedException("This feature is not suported for this release.");
	}

}
