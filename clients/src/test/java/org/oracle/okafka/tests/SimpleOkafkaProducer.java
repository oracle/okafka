package org.oracle.okafka.tests;

import org.junit.BeforeClass;
import org.junit.Test;
import org.oracle.okafka.clients.producer.KafkaProducer;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class SimpleOkafkaProducer{
	
	@Test
	public void ProducerTest() {
		try {
			Properties prop = OkafkaSetup.setup();
		    prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			Producer<String, String> producer = new KafkaProducer<String, String>(prop);
			Future<RecordMetadata> lastFuture = null;
			int msgCnt = 1000;
			for(int i=0;i<msgCnt;i++) {
				ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("TE1", i+"", "Test message # " + i);
				lastFuture =producer.send(producerRecord);
			}
			System.out.println("Produced "+ msgCnt +" messages.");
			lastFuture.get();
			producer.close();
		}		
		catch(Exception e)
		{
			System.out.println("Exception in Main " + e );
			e.printStackTrace();
		}
	}
}
