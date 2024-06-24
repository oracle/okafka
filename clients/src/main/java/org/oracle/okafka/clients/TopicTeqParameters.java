package org.oracle.okafka.clients;


import java.util.HashMap;

public class TopicTeqParameters {

	int keyBased;
	int stickyDeq;
	int shardNum;
	int dbMajorVersion;
	int dbMinorVersion;
	int msgVersion;


	public void setKeyBased(int keyBased)
	{
		this.keyBased = keyBased;
	}

	public void setStickyDeq(int stickyDeq)
	{
		this.stickyDeq = stickyDeq;
	}

	public void setShardNum(int shardNum)
	{
		this.shardNum = shardNum;
	}

	private void setMsgVersion(int msgVersion) 
	{
        this.msgVersion = msgVersion;
	}

	public int getKeyBased()
	{
		return this.keyBased;
	}

	public int getStickyDeq()
	{
		return this.stickyDeq;
	}

	public int getShardNum()
	{
		return this.shardNum;
	}

	public int getMsgVersion() 
	{
		if(getStickyDeq()!=2) {
			this.msgVersion = 1;
		}
		else {
			this.msgVersion = 2;
		}
		return this.msgVersion;
	}
}
