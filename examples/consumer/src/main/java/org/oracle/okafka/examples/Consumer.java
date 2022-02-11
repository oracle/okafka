/*
 ** OKafka Java Client version 0.8.
 **
 ** Copyright (c) 2019, 2020 Oracle and/or its affiliates.
 ** Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.
 */
package org.oracle.okafka.examples;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.oracle.okafka.clients.consumer.ConsumerRecord;
import org.oracle.okafka.clients.consumer.ConsumerRecords;
import org.oracle.okafka.clients.consumer.KafkaConsumer;

public class Consumer {

    public static void main(String[] args) {

        Properties props = new Properties();

        String topic = "topic";

        props.put("oracle.service.name", "serviceid.regress.rdbms.dev.us.oracle.com");
        props.put("oracle.instance.name", "instancename");
        props.put("oracle.net.tns_admin", "location of tnsnames.ora/ojdbc.properties file"); //eg: "/user/home" if ojdbc.properies file is in home

        props.put("bootstrap.servers", "host:port");
        props.put("group.id", "groupid");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "10000");

        props.put("key.deserializer",
                "org.oracle.okafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.oracle.okafka.common.serialization.StringDeserializer");
        props.put("max.poll.records", 100);

        KafkaConsumer<String, String> consumer = null;

        consumer = new KafkaConsumer<String, String>(props);

        consumer.subscribe(Arrays.asList(topic));

        ConsumerRecords<String, String> records = null;

        try {

            records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("topic = , partition=  ,key= , value = \n"+
                        record.topic()+ "  "+record.partition()+ "  "+record.key()+"  "+ record.value());
                System.out.println(".......");
            }

            consumer.commitSync();

        }catch(Exception ex) {
            ex.printStackTrace();

        } finally {
            consumer.close();
        }


    }

}

