package org.oracle.okafka.tests;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;
import org.oracle.okafka.clients.consumer.KafkaConsumer;

public class ConsumerMetricsTest {
	public static void getMetricData(Consumer<?,?> consumer,String fileName) {
		try {
		Map<MetricName, ? extends Metric> metricData = consumer.metrics();
		File csvFile = new File(System.getProperty("user.dir") +fileName+ ".csv");
		FileWriter fileWriter = new FileWriter(csvFile);
		StringBuilder headLine = new StringBuilder();
		headLine.append("Name");
		headLine.append(',');
		headLine.append("Group");
		headLine.append(',');
		headLine.append("Description");
		headLine.append(',');
		headLine.append("Tags");
		headLine.append(',');
		headLine.append("Value");
		headLine.append("\n");
		fileWriter.write(headLine.toString());
		metricData.forEach((a, b) -> {
			try {

				StringBuilder line = new StringBuilder();

				line.append(a.name());
				line.append(',');
				line.append(a.group());
				line.append(',');
				line.append(a.description());
				if(a.tags().containsKey("node-id") || a.tags().containsKey("topic")) {
					if(a.tags().containsKey("node-id")) {
						line.append(',');
						line.append(a.tags().get("node-id"));
					}
					if(a.tags().containsKey("topic")) {
						line.append(',');
						line.append("topic-"+a.tags().get("topic"));
					}
					}else{
						line.append(',');
						line.append("");
						
					}
				line.append(',');
				line.append(b.metricValue().toString());
				line.append("\n");
				fileWriter.write(line.toString());

			} catch (IOException e) {
				e.printStackTrace();
			}
 
		});
		fileWriter.close();
		} catch(IOException e) {
			e.printStackTrace();
		}
	}
	


	@Test
	public void ConsumingTest() {
		Properties prop = new Properties();
		prop = OkafkaSetup.setup();
        prop.put("group.id" , "S1");
		prop.put("max.poll.records", 1000);
		prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		Consumer<String, String> consumer = new KafkaConsumer<String, String>(prop);
		consumer.subscribe(Arrays.asList("TEQ"));
		int expectedMsgCnt =1000;
		int retryCount=1;
		int msgCnt = 0;		
		try {
			 while(true) {
		     try {
		    	ConsumerRecords <String, String> records = consumer.poll(Duration.ofMillis(10000));

		    	Collection<TopicPartition> partitions = records.partitions();
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
		    			if(retryCount>3)
		    				break;
		    			System.out.println("No Record Fetched. Retrying in 1 second");
		    			Thread.sleep(1000);
		    			retryCount++;
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
			  ConsumerMetricsTest.getMetricData(consumer, "afterConsumingOkafka");
		    System.out.println("Closing Consumer");
		    consumer.close();
		   }
		}
}
