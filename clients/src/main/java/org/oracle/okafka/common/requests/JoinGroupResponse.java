package org.oracle.okafka.common.requests;

import java.util.List;
import java.util.Map;

import org.oracle.okafka.common.internals.PartitionData;
import org.oracle.okafka.common.internals.SessionData;
import org.oracle.okafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

public class JoinGroupResponse extends AbstractResponse {
    private Map<String, SessionData> sessionData;
    private List<PartitionData> partitions;
    private int leader;
    private int version;
    private Exception exception;
    protected final Logger log ;
    
    public JoinGroupResponse(Map<String, SessionData> sessionData, List<PartitionData> partitions, int leader, int version, Exception exception) {
    	super(ApiKeys.JOIN_GROUP);
    	this.sessionData= sessionData;
    	this.partitions = partitions;
    	this.leader = leader;
    	this.version = version;
    	this.exception = exception;
    	LogContext logContext = new LogContext("[AQ$_JOIN_GROUP:]");
    	this.log = logContext.logger(JoinGroupResponse.class) ;

    	log.debug("QPAT:");

    	for(String mapSessionDataKeyNow : sessionData.keySet() )
    	{
    		log.debug("MapSessionDataKey " + mapSessionDataKeyNow );
    		SessionData sessionDataNow = sessionData.get(mapSessionDataKeyNow);
    		log.debug("Session Data Now: " + sessionDataNow.toString());
    	}

    	if(partitions != null)
    	{
    		log.debug("QPIM:");
    		for(PartitionData pData: partitions)
    		{
    			log.debug("PData: " + pData);
    		}
    	}else 
    	{
    		log.debug("QPIM: NULL");
    	}
    	log.debug("Leader = " +leader +", Verssion: " + version );
    }
    
    public Map<String, SessionData> getSessionData() {
    	return this.sessionData;
    }
    
    public List<PartitionData> partitions() {
    	return this.partitions;
    }
    
    public int leader() {
    	return this.leader;
    }
    
    public int version() {
    	return this.version;
    }
    
    public Exception getException() {
    	return this.exception;
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
	public void maybeSetThrottleTimeMs(int arg0) {
		// TODO Auto-generated method stub
		
	}
}
