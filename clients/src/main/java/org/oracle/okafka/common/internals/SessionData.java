package org.oracle.okafka.common.internals;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.oracle.okafka.common.internals.PartitionData;

public class SessionData implements Comparable<SessionData>{
	
	public String name;  // instId_sid
	private String schema;
	private String subscribedtopic;
	private String subscriberName;
	private long sessionId;
	private int instanceId;
	private long auditId;
	private int subscriberId;
	private int leader;
	private int version;
	public Date createTime; // Session creation time
	private int flags;
	private int queueId;
	
	public int pendingCnt; 
	public boolean oneMore = false;
	private List<PartitionData> assignedPartitions;
	private List<PartitionData> previousPartitions;
	
	private boolean invalid = false;
	
	public SessionData(long sessionId, int instanceId, String schema,  String subTopic,int queueId, 
			          String subscriberName,int subscriberId, Date createTime, int leader, int version, long auditId) {
		this.sessionId = sessionId;
		this.instanceId = instanceId;
		this.schema = schema;
		this.subscribedtopic = subTopic;
		this.subscriberName = subscriberName;
		this.leader = leader;
		this.version = version;
		this.auditId = auditId;
		this.subscriberId = subscriberId;
		this.name = this.instanceId+"_"+this.sessionId;
		this.assignedPartitions = new ArrayList<>();
		this.previousPartitions = new ArrayList<>();
		this.flags = -1;
		this.queueId = queueId;
		
	}
	
	public String toString()
	{
		String str = "Session:"+sessionId+",Instance:"+instanceId+",SubscribedTopic:"+subscribedtopic+
				      ",Leader"+leader+",Version"+version;
		String partitionListStr="Partitions:[";
		for(PartitionData pData: assignedPartitions)
		{
			if(pData == null)
				partitionListStr+="NULL";
			else
				partitionListStr += pData.toString()+",";
		}
		str = str+","+partitionListStr+"]";
		return str;
	}
	
	public int getInstanceId() {
		return this.instanceId;
	}
	
	public long getSessionId() {
		return this.sessionId;
	}
	
	public String getSubscribedTopics() {
		return this.subscribedtopic;
	}
	
	public String getSchema() {
		return this.schema;
	}
	public int getVersion() {
		return this.version;
	}
	
	public int getLeader() {
		return this.leader;
	}
	
	public long getAuditId() {
		return this.auditId;
	}
	public List<PartitionData> getAssignedPartitions() {
		return this.assignedPartitions;
	}
	
	public void addAssignedPartitions(PartitionData pd) {
		//System.out.println("addAssignedPartitions:: Partition " +pd.getTopicPartition().partition() + " assigned to " + this.name +" Pending " + this.pendingCnt);
		this.assignedPartitions.add(pd);
	}
	
	public void setAssignedPartitions(List<PartitionData> pds) {
		this.assignedPartitions.addAll(pds);
	}
	
	public void setPreviousPartitions(PartitionData pd) {
		this.previousPartitions.add(pd);
	}
	
	public void setPreviousPartitions(List<PartitionData> pds) {
		this.previousPartitions.addAll(pds);
	}
	
	public List<PartitionData> getPreviousPartitions() {
		return previousPartitions;
	}
	
	public String getSubscriberName()
	{
		return subscriberName;
	}
	public int getSubscriberId()
	{
		return subscriberId;
	}
	
	public int getFlags()
	{
		return flags;
	}
	public void setFlags(int flags)
	{
		this.flags = flags;
	}
	public void setQueueId(int queueId)
	{
		this.queueId = queueId;
	}
	public int getQueueId()
	{
		return queueId;
	}
	
	@Override
	public boolean equals(Object obj)
	{
		if(!(obj instanceof SessionData))
			return false;

		SessionData sData = (SessionData)obj;

		if(this.sessionId == sData.sessionId && this.instanceId == sData.instanceId)
			return true;

		return false;
	}

	@Override
	public int compareTo(SessionData sData)
	{
		if(this.auditId < sData.auditId)
			return -1;
		else if (this.auditId > sData.auditId)
			return 1;
		else
			return 0;
	}
	
	public boolean isInvalid()
	{
		return invalid;
	}
	/* Session data to be invalidated when one database connection fails and 
	 * we fail-over and restart a new database connection.
	 * This will eventually trigger rebalancing and we will pass version = -1 this time.
	*/
	public void invalidSessionData()
	{
		this.invalid=true;
		this.version = -1;
		assignedPartitions.clear();
		this.leader = -1;
	}
	
	
}