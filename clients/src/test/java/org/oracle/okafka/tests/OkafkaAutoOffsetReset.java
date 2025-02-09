package org.oracle.okafka.tests;


import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.Test;
import org.oracle.okafka.clients.consumer.KafkaConsumer;

public class OkafkaAutoOffsetReset {

	@Test
	public void autoOffsetSeekTest() throws IOException {
		Properties prop = new Properties();
		prop = OkafkaSetup.setup();
        prop.put("group.id" , "S67");
		prop.put("max.poll.records", 1000);
		prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		prop.put("auto.offset.reset", "earliest");
		System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "DEBUG");

		Consumer<String, String> consumer = new KafkaConsumer<String, String>(prop);
		
		consumer.subscribe(Arrays.asList("TEQ"));
		
		int expectedMsgCnt = 1000;
		int msgCnt = 0;		
		try {
			 while(true) {
		     try {
//		    	System.out.println(consumer.listTopics());
		    	ConsumerRecords <String, String> records = consumer.poll(Duration.ofMillis(10000));
//		    	System.out.println(consumer.listTopics());
		   
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
