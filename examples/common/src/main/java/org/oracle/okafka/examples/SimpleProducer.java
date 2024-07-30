/*
** OKafka Java Client version 23.4.
**
** Copyright (c) 2019, 2024 Oracle and/or its affiliates.
** Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.
*/

package org.oracle.okafka.examples;

import org.oracle.okafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.Future;

public class SimpleProducerOKafka {
	
	public static void main(String[] args) {
		long startTime =0;
		
		try {
			Properties props = new Properties();
			
			// Option 1: Connect to Oracle Database with database username and password
			props.put("security.protocol","PLAINTEXT");
			//IP or Host name where Oracle Database 23ai is running and Database Listener's Port
			props.put("bootstrap.servers", "localhost:1521");
			props.put("oracle.service.name", "freepdb1"); //name of the service running on the database instance
			// location for ojdbc.properties file where user and password properties are saved
			props.put("oracle.net.tns_admin","."); 
			
			/*
			//Option 2: Connect to Oracle Autonomous Database using Oracle Wallet
			//This option to be used when connecting to Oracle autonomous database instance on OracleCloud
			props.put("security.protocol","SSL");
			// location for Oracle Wallet, tnsnames.ora file and ojdbc.properties file
			props.put("oracle.net.tns_admin","."); 
			props.put("tns.alias","Oracle23ai_high"); 
			*/
			
            props.put("enable.idempotence","true");
			props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

			String baseMsg = "This is test with 128 characters Payload used to"+
						 " compare result with Apache Kafka with Oracle"+
						 " Transactional Event Queue.........";

			Producer<String, String> producer = new KafkaProducer<String, String>(props);
			Future<RecordMetadata> lastFuture = null;
			int msgCnt = 3000;
			startTime = System.currentTimeMillis();
			//String key = "KafkaTestKeyWithDifferntCharacters12@$%^&";
			String key = "Just some value for kafka";
			ArrayList<Future<RecordMetadata>> metadataList = new ArrayList<Future<RecordMetadata>>();
			
			for(int i=0;i<msgCnt;i++) {
             /* RecordHeader rH1 = new RecordHeader("CLIENT_ID", "FIRST_CLIENT".getBytes());
				RecordHeader rH2 = new RecordHeader("REPLY_TO", "TOPIC_M5".getBytes()); */
				ProducerRecord<String, String> producerRecord = 
						new ProducerRecord<String, String>("KTOPIC2", key+i, baseMsg + i);
                //producerRecord.headers().add(rH1).add(rH2);
				lastFuture = producer.send(producerRecord);
				metadataList.add(lastFuture);
				/*if(i%10 == 0)
				{
					Thread.sleep(1000);
				}*/
			}
			RecordMetadata  rd = lastFuture.get();
			System.out.println("Last record placed in " + rd.partition() + " Offset " + rd.offset());
			
			Thread.sleep(5000);
			System.out.println("Initiating close");
			producer.close();
			long runTime = System.currentTimeMillis() - startTime;
			System.out.println("Produced "+ msgCnt +" messages. Run duration " + runTime);
		}		
		catch(Exception e)
		{
			System.out.println("Exception in Main " + e );
			e.printStackTrace();
		}
	}
}
