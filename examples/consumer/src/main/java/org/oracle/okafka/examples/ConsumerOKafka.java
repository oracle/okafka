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

public class ConsumerOKafka {
	public static void main(String[] args) {
		Properties props = new getProperties();
		String topic = props.getProperty("topic.name", "TXEQ");
		props.remove("topic.name"); // Pass props to build OKafkaConsumer		
		KafkaConsumer<Integer , String> consumer = new KafkaConsumer<Integer, String>(props);
		consumer.subscribe(Arrays.asList(topic));

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
					System.out.println("No Record Fetched. Retrying in 1 second");
					Thread.sleep(1000);
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

	private static java.util.Properties getProperties()  throws IOException {
		InputStream inputStream = null;
		Properties appProperties = null;

		try {
			Properties prop = new Properties();
			String propFileName = "config.properties";
			inputStream = new FileInPutStream(propFileName);
			if (inputStream != null) {
				prop.load(inputStream);
			} else {
				throw new FileNotFoundException("property file '" + propFileName + "' not found.");
			}
			appProperties = prop;

		} catch (Exception e) {
			System.out.println("Exception: " + e);
			throw e;
		} finally {
			inputStream.close();
		}
		return appProperties;
	}
}