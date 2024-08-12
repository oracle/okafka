/*
** OKafka Java Client version 23.4.
**
** Copyright (c) 2019, 2024 Oracle and/or its affiliates.
** Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.
*/

package org.oracle.okafka.examples;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.oracle.okafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.header.Header;
import org.oracle.okafka.clients.consumer.KafkaConsumer;

public class TransactionalConsumerProducer {

	static int msgNo =0;
	static PreparedStatement instCStmt = null;
	static PreparedStatement instPStmt = null;

	public static void main(String[] args) {
		Properties commonProps = new Properties();
		Properties cProps = new Properties();
		Properties pProps =new Properties();
		
		// Option 1: Connect to Oracle Database with database username and password
		commonProps.put("security.protocol","PLAINTEXT");
		//IP or Host name where Oracle Database 23ai is running and Database Listener's Port
		commonProps.put("bootstrap.servers", "localhost:1521");
		commonProps.put("oracle.service.name", "freepdb1"); //name of the service running on the database instance
		// directory location where ojdbc.properties file is stored which contains user and password properties
		commonProps.put("oracle.net.tns_admin","."); 
		 
		/*
		//Option 2: Connect to Oracle Autonomous Database using Oracle Wallet
		//This option to be used when connecting to Oracle autonomous database instance on OracleCloud
		commonProps.put("security.protocol","SSL");
		// location for Oracle Wallet, tnsnames.ora file and ojdbc.properties file
		commonProps.put("oracle.net.tns_admin","."); 
		commonProps.put("tns.alias","Oracle23ai_high"); 
		*/
		
		cProps.putAll(commonProps);
		pProps.putAll(commonProps);

		//Consumer Group Name
		cProps.put("group.id" , "CG1");
		cProps.put("enable.auto.commit","false");

		// Maximum number of records fetched in single poll call
		cProps.put("max.poll.records", 10);
		cProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		cProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		pProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		pProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		pProps.put("oracle.transactional.producer", "true");

		Consumer<String , String> consumer = new KafkaConsumer<String, String>(cProps);
		ConsumerRebalanceListener rebalanceListener = new ConsumerRebalance();
		consumer.subscribe(Arrays.asList("TXEQ"), rebalanceListener);

		int expectedMsgCnt = 100;
		int msgCnt = 0;
		Connection conn = null;

		Producer<String, String> producer = null;
		try {
			conn = ((KafkaConsumer<String, String>)consumer).getDBConnection();
			producer = new KafkaProducer<String,String>(pProps, conn);
			producer.initTransactions();
			while(true) {
				try {
					//Consumes records from the assigned partitions of 'TXEQ' topic
					ConsumerRecords <String, String> records = consumer.poll(Duration.ofMillis(10000));
					if(records != null && records.count() > 0) {
						msgCnt += records.count();
						
						producer.beginTransaction();
						boolean fail =false;
						for (ConsumerRecord<String, String> record : records) {
							ProducerRecord<String,String> pr = null;
							try {
								String outRecord = processConsumerRecord(conn, record);
								pr = new ProducerRecord<String,String>("TXEQ_2", record.key(), outRecord);
								processProducerRecord(conn, pr);
							}catch(Exception e)
							{
								// Stop processing of this batch 
								fail =true;
								break;
							}
							producer.send(pr);
						}
						if(fail) {
							//Abort consumed and produced records along with any DML operations done using connection object.
							//Next consumer.poll will fetch the same records again.
							producer.abortTransaction();
						}
						else {
							//Commit consumed and produced records along with any DML operations done using connection object
							producer.commitTransaction();
						}
					}
					else {
						System.out.println("No Record Fetched. Retrying in 1 second");
						Thread.sleep(1000);
					}
					
					if(msgCnt >= expectedMsgCnt )
					{
						System.out.println("Received " + msgCnt + " Expected " + expectedMsgCnt +". Exiting Now.");
						break;
					}

				}catch(DisconnectException dcE) {
					System.out.println("Disconnect Exception while committing or aborting records "+ dcE);
					throw dcE;
				}
				catch(KafkaException e)
				{
					System.out.println("Re-triable Exception while committing records "+ e);
					producer.abortTransaction();
				}
				catch(Exception e)
				{
					System.out.println("Exception while processing records " + e.getMessage());
					throw e;
				}			
			}
		}catch(Exception e)
		{
			System.out.println("Exception from OKafka consumer " + e);
			e.printStackTrace();
		}finally {

			System.out.println("Closing OKafka Consumer. Received "+ msgCnt);
			producer.close();
			consumer.close();
		}
	}

	static String processConsumerRecord(Connection conn, ConsumerRecord <String, String> record) throws Exception 
	{
		//Application specific logic to process the record
		System.out.println("Received: " + record.partition() +"," + record.offset() +":" + record.value());
		return record.value();
	}
	static void processProducerRecord(Connection conn, ProducerRecord <String, String> records) throws Exception 
	{
		//Application specific logic to process the record
	}

	static void processRecords(Producer<String,String> porducer, Consumer<String,String> consumer, ConsumerRecords <String, String> records) throws Exception
	{
		Connection conn = ((KafkaProducer<String,String>)porducer).getDBConnection();
		String jsonPayload = null;
		ProducerRecord<String,String> pr = null;
		Future<RecordMetadata> lastFuture = null;
		for (ConsumerRecord<String, String> record : records)	
		{
			msgNo++;
			System.out.println("Processing " + msgNo + " record.value() " + record.value());
			System.out.printf("partition = %d, offset = %d, key = %s, value =%s\n ", record.partition(), record.offset(), record.key(), record.value());
			for(Header h: record.headers())
			{
				System.out.println("Header: " +h.toString());
			} 

			jsonPayload = "{\"name\":\"Programmer"+msgNo+"\",\"status\":\"classy\",\"catagory\":\"general\",\"region\":\"north\",\"title\":\"programmer\"}";
			pr = new ProducerRecord<String,String>("KTOPIC1", record.key(), jsonPayload);
			lastFuture = porducer.send(pr);
			RecordMetadata metadata = lastFuture.get();
		}
	}

	// Dummy implementation of ConsumerRebalanceListener interface
	// It only maintains the list of assigned partitions in assignedPartitions list
	static class ConsumerRebalance implements ConsumerRebalanceListener {

		public List<TopicPartition> assignedPartitions = new ArrayList<TopicPartition>();

		@Override
		public synchronized void onPartitionsAssigned(Collection<TopicPartition>  partitions) { 
			System.out.println("Newly Assigned Partitions:");
			for (TopicPartition tp :partitions ) {
				System.out.println(tp);
				assignedPartitions.add(tp);
			}
		} 

		@Override
		public synchronized void onPartitionsRevoked(Collection<TopicPartition> partitions) {
			System.out.println("Revoked previously assigned partitions. ");
			for (TopicPartition tp :assignedPartitions ) {
				System.out.println(tp);
			}
			assignedPartitions.clear();
		}
	}
}
