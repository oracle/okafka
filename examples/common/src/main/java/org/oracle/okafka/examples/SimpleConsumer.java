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

import org.oracle.okafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class SimpleConsumer {
	public static void main(String[] args) {
		Properties props = new Properties();
		props.put("bootstrap.servers", "den02tgo.us.oracle.com:9092");
		props.put("group.id", "S1");
		props.put("enable.auto.commit", "true");
		props.put("max.poll.records", 1000);

		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		/*
		// Option 2: Connect to Oracle Autonomous Database using Oracle Wallet
		// This option to be used when connecting to Oracle autonomous database instance
		// on OracleCloud
		props.put("security.protocol", "SSL");
		// location for Oracle Wallet, tnsnames.ora file and ojdbc.properties file
		props.put("oracle.net.tns_admin", "./Wallet_Oratest23ai");
		props.put("tns.alias", "oratest23ai_high");
		*/

		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
		consumer.subscribe(Arrays.asList("TXEQ"));

		try {
			while (true) {
				try {
					ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));

					for (ConsumerRecord<String, String> record : records)
						System.out.printf("partition = %d, offset = %d, key = %s, value =%s\n  ", record.partition(),
								record.offset(), record.key(), record.value());

					if (records != null && records.count() > 0) {
						System.out.println("Committing records" + records.count());
						consumer.commitSync();
					} else {
						System.out.println("No Record Fetched. Waiting for User input");
						System.in.read();
					}
				} catch (Exception e) {
					throw e;
				}
			}
		} catch (Exception e) {
			System.out.println("Exception from consumer " + e);
			e.printStackTrace();
		} finally {
			consumer.close();
		}

	}
}