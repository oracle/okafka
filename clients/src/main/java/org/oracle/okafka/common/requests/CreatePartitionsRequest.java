package org.oracle.okafka.common.requests;

import java.util.Map;

import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.common.protocol.ApiMessage;
import org.oracle.okafka.common.protocol.ApiKeys;


public class CreatePartitionsRequest extends AbstractRequest {
	
	private final Map<String, NewPartitions> newPartitions;
	
    public static class Builder extends AbstractRequest.Builder<CreatePartitionsRequest> {
    	private final Map<String, NewPartitions> newPartitions;

        public Builder(Map<String, NewPartitions> newPartitions) {
            super(ApiKeys.CREATE_PARTITIONS);
            this.newPartitions = newPartitions;
        }

		@Override
		public CreatePartitionsRequest build(short version) {
			return new CreatePartitionsRequest(this.newPartitions ,version);
		}

        @Override
        public String toString() {
        	   StringBuilder bld = new StringBuilder();
               bld.append("(type=createPartitionsRequest").
                   append(", topicsWithNewPartitionCounts = ").append(newPartitions.toString()).
                   append(")");
               return bld.toString();
        }
    }

    CreatePartitionsRequest(Map<String, NewPartitions> partitionNewCounts, short version) {
        super(ApiKeys.CREATE_PARTITIONS, version);
        this.newPartitions = partitionNewCounts;
    }
    
    public Map<String, NewPartitions> partitionNewCounts(){
    	return newPartitions;
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

