package org.oracle.okafka.tests;

import org.junit.Test;
import org.oracle.okafka.clients.producer.KafkaProducer;


import java.lang.Thread;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;

public class ProducerMetricsTest{
	
	public static void getMetricData(Producer<String,String> producer,String fileName) {
		try {
		Map<MetricName, ? extends Metric> metricData = producer.metrics();
		File csvFile = new File(System.getProperty("user.dir")+fileName+ ".csv");
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
	public void ProducerTest() {
		try {

			Properties prop = new Properties();
			prop = OkafkaSetup.setup();
		    prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			Producer<String, String> producer = new KafkaProducer<String, String>(prop);
		   
			int msgCnt = 100;
			for(int i=0;i<msgCnt;i++) {
				ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("TEQ", i+"", "Test message # " + i);
				producer.send(producerRecord);
			}
			System.out.println("Produced "+ msgCnt +" messages.");
			
			Thread.sleep(9000);
			ProducerMetricsTest.getMetricData(producer,"afterProducingOkafka");

			producer.close();
			System.out.println("producer closed");
		}		
		catch(Exception e)
		{
			System.out.println("Exception in Main " + e );
			e.printStackTrace();
		}
	}
}
