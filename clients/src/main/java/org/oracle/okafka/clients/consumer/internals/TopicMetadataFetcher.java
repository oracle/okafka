package org.oracle.okafka.clients.consumer.internals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Timer;
import org.oracle.okafka.common.requests.MetadataRequest;
import org.oracle.okafka.common.requests.MetadataResponse;
import org.slf4j.Logger;

public class TopicMetadataFetcher {
	private final Logger log;
	private final ConsumerNetworkClient client;

	public TopicMetadataFetcher(LogContext logContext, ConsumerNetworkClient client) {
		this.log = logContext.logger(getClass());
		this.client = client;

	}

	public Map<String, List<PartitionInfo>> getAllTopicMetadata(Timer timer) {
		MetadataRequest.Builder request = MetadataRequest.Builder.listAllTopics(true);
		return getTopicMetadata(request, timer);
	}

	private Map<String, List<PartitionInfo>> getTopicMetadata(MetadataRequest.Builder builder, Timer timer) {
		boolean retry = false;
		
		
		do {
			
			retry=false;
			ClientResponse response= client.sendMetadataRequest(builder);
			MetadataResponse metadataResponse= (MetadataResponse)response.responseBody();
				
			if(metadataResponse.getException()==null && !response.wasDisconnected()) {
				Map<String,List<PartitionInfo>> listTopicsMap=new HashMap<>();
				
				List<PartitionInfo> partitionInfoList = metadataResponse.partitions();
				for(int i=0;i<partitionInfoList.size();i++) {
					String topic=partitionInfoList.get(i).topic();
					if (listTopicsMap.containsKey(topic)) {
						listTopicsMap.get(topic).add(partitionInfoList.get(i));
					} else {
						listTopicsMap.put(topic, new ArrayList<>(Arrays.asList(partitionInfoList.get(i))));
					}
				}
				
				return listTopicsMap;
			}
			else if(metadataResponse.getException()!=null) {
				
				log.error("Exception Caught: ",metadataResponse.getException());
				return null;
			}
			else {
				retry=true;
			}	
			
		} while (retry && timer.notExpired());

		throw new TimeoutException("Timeout expired while fetching topic metadata");
	}
}
