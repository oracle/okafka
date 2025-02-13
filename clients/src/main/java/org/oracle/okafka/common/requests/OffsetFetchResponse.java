package org.oracle.okafka.common.requests;

import java.util.Map;
import java.util.Objects;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.oracle.okafka.common.protocol.ApiKeys;

public class OffsetFetchResponse extends AbstractResponse {
	
	private final Map<TopicPartition, PartitionOffsetData> offsetFetchResponseMap;
	private Exception exception;
	
	public static final class PartitionOffsetData {
		public final long offset;
		public final Exception error;

		public PartitionOffsetData(long offset, Exception error) {
			this.offset = offset;
			this.error = error;
		}

		@Override
		public boolean equals(Object other) {
			if (!(other instanceof PartitionOffsetData))
				return false;
			PartitionOffsetData otherPartition = (PartitionOffsetData) other;
			return Objects.equals(this.offset, otherPartition.offset)
					&& Objects.equals(this.error, otherPartition.error);
		}

		@Override
		public String toString() {
			return "PartitionData(" + "offset=" + offset + ", error='" + error.toString() + ")";
		}

		@Override
		public int hashCode() {
			return Objects.hash(offset, error);
		}
	}
	
	public OffsetFetchResponse(Map<TopicPartition, PartitionOffsetData> offsetFetchResponseMap) {
		super(ApiKeys.OFFSET_FETCH);
		this.offsetFetchResponseMap = offsetFetchResponseMap;
		exception = null;
	}
	
	public void setException(Exception ex) {
		this.exception=ex;
	}
	
	public Map<TopicPartition, PartitionOffsetData> getOffsetFetchResponseMap(){
		return offsetFetchResponseMap;
	}
	
	public Exception getException() {
		return exception;
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
		// TODO Auto-generated method stub
		
	}

}
