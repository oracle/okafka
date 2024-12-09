package org.oracle.okafka.tests;

import org.junit.Test;
import java.time.Duration;
import java.util.*;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.oracle.okafka.clients.consumer.KafkaConsumer;

public class SimpleOkafkaConsumer {

	@Test
	public void ConsumerTest() {
		Properties prop = new Properties();
		prop = OkafkaSetup.setup();
		prop.put("group.id", "S1");
		prop.put("max.poll.records", 1000);
		prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		Consumer<String, String> consumer = new KafkaConsumer<String, String>(prop);
		consumer.subscribe(Arrays.asList("TEQ"));
		int expectedMsgCnt = 1000;
		int msgCnt = 0;
		try {
			while (true) {
				try {
					ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
					Collection<TopicPartition> partitions = records.partitions();
					for (ConsumerRecord<String, String> record : records)
						System.out.printf("partition = %d, offset = %d, key = %s, value =%s\n  ", record.partition(),
								record.offset(), record.key(), record.value());

					if (records != null && records.count() > 0) {
						msgCnt += records.count();
						System.out.println("Committing records " + records.count());
						consumer.commitSync();

						if (msgCnt >= expectedMsgCnt) {
							System.out.println("Received " + msgCnt + " Expected " + expectedMsgCnt + ". Exiting Now.");
							break;
						}
					} else {
						System.out.println("No Record Fetched. Retrying in 1 second");
						Thread.sleep(1000);
					}
				} catch (Exception e) {
					throw e;
				}
			}
		} catch (Exception e) {
			System.out.println("Exception from consumer " + e);
			e.printStackTrace();
		} finally {
			System.out.println("Closing Consumer");
			consumer.close();
		}
	}
}
