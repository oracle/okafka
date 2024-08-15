package org.oracle.okafka.tests;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;
import org.oracle.okafka.clients.consumer.KafkaConsumer;

public class OkafkaSeekToEnd {
	@Test
	public void SeekEndTest() throws IOException {
		Properties prop = new Properties();
		prop = OkafkaSetup.setup();
        prop.put("group.id" , "S1");
		prop.put("max.poll.records", 1000);
		prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		
        Consumer<String, String> consumer = new KafkaConsumer<String, String>(prop);
		try {
				consumer.subscribe(Arrays.asList("TEQ"), new ConsumerRebalanceListener() {
			        @Override
			        public synchronized void onPartitionsRevoked(Collection<TopicPartition> partitions) {
			        	System.out.println("Partitions revoked for rebalance.");
			        }
			        @Override
			        public synchronized void onPartitionsAssigned(Collection<TopicPartition> partitions) {
			        	 System.out.println("New Partitions assigned after rebalance");
			        	try {
			        		consumer.seekToEnd(partitions);
			       	    }
			        	 catch (Exception e) {
			                 e.printStackTrace();
			        	 }
			        }
			    });
			}
			catch(Exception e) {
				System.out.println(e);
				e.printStackTrace();
		   }
			int expectedMsgCnt = 1000;
    		int msgCnt = 0;
    		try {
    			Instant starttime = Instant.now();
    			long runtime =0;
    			while(true && runtime <=120) {
    				try {
    					ConsumerRecords <String, String> records = consumer.poll(Duration.ofMillis(10000));
    				
    					for (ConsumerRecord<String, String> record : records)				
    						System.out.printf("partition = %d, offset = %d, key = %s, value =%s\n  ", record.partition(), record.offset(), record.key(), record.value());

    					if(records != null && records.count() > 0) {
    						msgCnt += records.count();
    						System.out.println("Committing records " + records.count());
    						consumer.commitSync();
    						
    						if(msgCnt >= expectedMsgCnt )
    						{
    							System.out.println("Received " + msgCnt + " Expected " + expectedMsgCnt +". Exiting Now.");
    							break;
    						}
    					}
    					else {
    						System.out.println("No Record Fetched. Retrying in 1 second");
    						Thread.sleep(1000);
    					}
    					runtime = Duration.between(starttime, Instant.now()).toSeconds();
    					
    				}catch(Exception e)
    				{
    					throw e;
    				}
    				}			
    			
    		}catch(Exception e)
    		{
    			System.out.println("Exception from consumer " + e);
    			e.printStackTrace();
    		}finally {
    			System.out.println("Closing Consumer");
    			consumer.close();
    		}
    	}

    }




