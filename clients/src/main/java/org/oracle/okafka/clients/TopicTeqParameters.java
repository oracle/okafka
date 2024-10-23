/*
** OKafka Java Client version 23.4.
**
** Copyright (c) 2019, 2024 Oracle and/or its affiliates.
** Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.
*/

package org.oracle.okafka.clients;


import java.util.HashMap;

import org.oracle.okafka.clients.admin.TopicDescription;

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
	
	@Override
    public boolean equals(final Object o) {
		if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final TopicTeqParameters that = (TopicTeqParameters) o;
        return this.keyBased == that.keyBased &&
        		this.stickyDeq == that.stickyDeq &&
        		this.shardNum == that.shardNum &&
        		this.dbMajorVersion == that.dbMajorVersion &&
        		this.dbMinorVersion == that.dbMinorVersion &&
        		this.msgVersion == that.msgVersion; 		
	}
	
}
