/*
 ** OKafka Java Client version 0.8.
 **
 ** Copyright (c) 2019, 2020 Oracle and/or its affiliates.
 ** Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.
 */
package org.oracle.okafka.examples;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.security.Provider;
import java.security.Security;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.oracle.okafka.clients.consumer.ConsumerRecord;
import org.oracle.okafka.clients.consumer.ConsumerRecords;
import org.oracle.okafka.clients.consumer.KafkaConsumer;

public class Consumer {

    public static void main(String[] args) {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "DEBUG");

        // Get application properties
        Properties appProperties = null;
        try {
            appProperties = getProperties();
            if (appProperties == null) {
                System.out.println("Application properties not found!");
                System.exit(-1);
            }
        } catch (Exception e) {
            System.out.println("Application properties not found!");
            System.out.println("Exception: " + e);
            System.exit(-1);
        }

        Properties props = new Properties();

        String topic = appProperties.getProperty("topic.name", "topic");

        // Get Oracle Database Service Name, ex: "serviceid.regress.rdbms.dev.us.oracle.com"
        props.put("oracle.service.name", appProperties.getProperty("oracle.service.name"));

        // Get Oracle Database Instance, ex: "instancename"
        props.put("oracle.instance.name", appProperties.getProperty("oracle.instance.name"));

        // Get location of tnsnames.ora/ojdbc.properties file eg: "/user/home" if ojdbc.properies file is in home
        props.put("oracle.net.tns_admin", appProperties.getProperty("oracle.net.tns_admin")); //

        //SSL communication with ADB
        props.put("security.protocol", appProperties.getProperty("security.protocol", "SSL"));
        if ("SSL".equals(appProperties.getProperty("security.protocol"))) {
            // Add dynamically Oracle PKI Provider required to SSL/Wallet
            addOraclePKIProvider();
            props.put("tns.alias", appProperties.getProperty("tns.alias"));
        }

        // Get Oracle Database address, eg: "host:port"
        props.put("bootstrap.servers", appProperties.getProperty("bootstrap.servers"));

        // Get Oracle TEQ Subscriber ID
        props.put("group.id", appProperties.getProperty("group.id"));

        props.put("enable.auto.commit", appProperties.getProperty("enable.auto.commit", "true"));
        props.put("auto.commit.interval.ms", appProperties.getProperty("auto.commit.interval.ms", "10000"));

        props.put("key.deserializer", appProperties.getProperty("key.deserializer",
                "org.oracle.okafka.common.serialization.StringDeserializer"));
        props.put("value.deserializer", appProperties.getProperty("value.deserializer",
                "org.oracle.okafka.common.serialization.StringDeserializer"));
        props.put("max.poll.records", Integer.parseInt(appProperties.getProperty("max.poll.records", "100")));

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

    private static java.util.Properties getProperties()  throws IOException {
        InputStream inputStream = null;
        Properties appProperties = null;

        try {
            Properties prop = new Properties();
            String propFileName = "config.properties";

            inputStream = Consumer.class.getClassLoader().getResourceAsStream(propFileName);
            if (inputStream != null) {
                prop.load(inputStream);
            } else {
                throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
            }

            appProperties = prop;

        } catch (Exception e) {
            System.out.println("Exception: " + e);
        } finally {
            inputStream.close();
        }
        return appProperties;
    }

    private static void addOraclePKIProvider() {
        System.out.println("Installing Oracle PKI provider.");
        Provider oraclePKI = new oracle.security.pki.OraclePKIProvider();
        Security.insertProviderAt(oraclePKI,3);
    }
}

