/*
** OKafka Java Client version 23.4.
**
** Copyright (c) 2019, 2024 Oracle and/or its affiliates.
** Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.
*/

package org.oracle.okafka.examples;

import java.util.Properties;
import java.time.Duration;
import java.util.Arrays;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class SimpleConsumer {
	public static void main(String[] args) {
		Properties props = new Properties();
		props.put("bootstrap.servers", "den02tgo.us.oracle.com:9092");
		props.put("group.id" , "test-group");
		props.put("enable.auto.commit","true");
		props.put("max.poll.records", 1000);

		props.put("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		
		
		KafkaConsumer<Integer , String> consumer = new KafkaConsumer<Integer, String>(props);
		consumer.subscribe(Arrays.asList("quickstart-events"));

		while(true) {
			try {
				ConsumerRecords <Integer, String> records = consumer.poll(Duration.ofMillis(10000));
			
				for (ConsumerRecord<Integer, String> record : records)
					System.out.printf("partition = %d, offset = %d, key = %d, value =%s\n  ", record.partition(), record.offset(), record.key(), record.value());
			
				if(records != null && records.count() > 0) {
					System.out.println("Committing records" + records.count());
					consumer.commitSync();     
				}
				else {
					System.out.println("No Record Fetched. Waiting for User input");
					System.in.read();
				}
			}catch(Exception e)
			{
				System.out.println("Exception from consumer " + e);
				e.printStackTrace();
			}
			finally {
				consumer.close();
			}
		}
	}

}