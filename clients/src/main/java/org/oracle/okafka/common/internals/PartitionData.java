package org.oracle.okafka.common.internals;

import org.apache.kafka.common.TopicPartition;

public class PartitionData {
	private TopicPartition topicPartition;
	private int queueId;
	private String subscriberName;
	private int subscriberId;
	private int ownerInstanceId;
	private boolean local;
	
	public PartitionData(String queueName, int queueId, int partitionId,
			             String subName, int subId, int ownerInstanceId, boolean local) {
		this.topicPartition = new TopicPartition(queueName, partitionId);
		this.queueId = queueId;
		this.subscriberName = subName;
		this.subscriberId = subId;
		this.ownerInstanceId = ownerInstanceId;
		this.local = local;
	}
	public String toString()
	{
		if(topicPartition == null)
			return "NULL";
		
		return "{Topic:"+topicPartition.topic()+",ConsumerGroupID:"+subscriberName+
				",Partition:"+topicPartition.partition()+",OwnerInstance:"+ownerInstanceId+",}";
	}
	
	public TopicPartition getTopicPartition() {
		return this.topicPartition;
	}
	public int getOwnerInstanceId() {
		return this.ownerInstanceId;
	}
	
	public void setOwnerInstanceId(int instId)
	{
		this.ownerInstanceId = instId;
	}
	
	public int getQueueId() {
		return this.queueId;
	}
	
	public String getSubName() {
		return this.subscriberName;
	}
	
	public int getSubId() {
		return this.subscriberId;
	}
	
	public void setLocal(boolean _local)
	{
		local = _local;
	}
	public boolean getLocal()
	{
		return local;
	}
	
	public boolean equals(Object obj)
	{
		if(!(obj instanceof PartitionData))
			return false;
		
		PartitionData tPart = (PartitionData)obj;
		return this.topicPartition.equals(tPart.topicPartition);
	}
	
}
