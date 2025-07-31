package org.oracle.okafka.common.requests;

import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.requests.AbstractResponse;
import org.oracle.okafka.common.protocol.ApiKeys;

public class ConnectMeRequest extends AbstractRequest  {
	
	private String schemaName;
	private String topicName;
	private String groupId;
	
	public static class Builder extends AbstractRequest.Builder<ConnectMeRequest>
	{
		private String schemaName;
		private String topicName;
		private String groupId;
		
		public Builder(String _schemaName , String _topicName, String _groupId)
		{
			super(ApiKeys.CONNECT_ME);
			this.schemaName = _schemaName;
			this.topicName = _topicName;
			this.groupId = _groupId;
		}
		
		@Override
		public ConnectMeRequest build(short version) {
			return new ConnectMeRequest(this.schemaName,this.topicName,this.groupId, version);
		}
	}
	
	public ConnectMeRequest(String _schemaName , String _topicName, String _groupId, short version)
	{
		super(ApiKeys.CONNECT_ME,version);
		this.schemaName = _schemaName;
		this.topicName = _topicName;
		this.groupId = _groupId;
	}
	
	public String getSchemaName()
	{
		return schemaName;
	}
	public String getToipcName()
	{
		return topicName;
	}
	public String getGroupId()
	{
		return groupId;
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
