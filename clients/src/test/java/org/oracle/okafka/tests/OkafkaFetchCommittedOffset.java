package org.oracle.okafka.tests;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;
import org.oracle.okafka.clients.consumer.KafkaConsumer;

public class OkafkaFetchCommittedOffset {

	@Test
	public void FetchCommittedOffsetTest() {
		Properties prop = new Properties();
		prop = OkafkaSetup.setup();
		prop.put("group.id", "S1");
		prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		
		Consumer<String, String> consumer = new KafkaConsumer<String, String>(prop);
		consumer.subscribe(Arrays.asList("TEQ"));
		try {
			Set<TopicPartition> topicPartitons = new HashSet<>();
			topicPartitons.add(new TopicPartition("TEQ",0));
			topicPartitons.add(new TopicPartition("TEQ",1));
        	Map<TopicPartition,OffsetAndMetadata> committedMap = consumer.committed(topicPartitons);
        	System.out.println(committedMap);
        	
		} catch (Exception e) {
			System.out.println("Exception from consumer " + e);
			e.printStackTrace();
		} finally {
			System.out.println("Closing Consumer");
			consumer.close();
		}
	}

}
