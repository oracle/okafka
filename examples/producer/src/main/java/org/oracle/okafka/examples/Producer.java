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
import java.util.Properties;

import org.oracle.okafka.clients.producer.KafkaProducer;
import org.oracle.okafka.clients.producer.ProducerRecord;


public class Producer {
	
	public static void main(String[] args) {
		System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "INFO");

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

		String topic = appProperties.getProperty("topic.name", "topic");

		KafkaProducer<String,String> prod = null;
		Properties props = new Properties();

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

		//props.put("batch.size", Integer.parseInt(appProperties.getProperty("batch.size", "200")));
		props.put("linger.ms", Integer.parseInt(appProperties.getProperty("linger.ms", "100")));
		//props.put("buffer.memory", Integer.parseInt(appProperties.getProperty("buffer.memory", "335544")));

		props.put("key.serializer", appProperties.getProperty("key.serializer",
				"org.oracle.okafka.common.serialization.StringSerializer"));
		props.put("value.serializer", appProperties.getProperty("value.serializer",
				"org.oracle.okafka.common.serialization.StringSerializer"));
		
		System.out.println("Creating producer now 1 2 3..");	
		  
		prod=new KafkaProducer<String, String>(props);

		System.out.println("Producer created.");
		
		 try {
			 int i;	
			 for(i = 0; i < 10; i++)				 
			     prod.send(new ProducerRecord<String, String>(topic ,0, i+"000","This is new message"+i));
 
		     System.out.println("Sent "+ i + "messages");	 
		 } catch(Exception ex) {
			  
			 System.out.println("Failed to send messages:");
			 ex.printStackTrace();
		 }
		 finally {
			 prod.close();
		 }
	}

	private static java.util.Properties getProperties()  throws IOException {
		InputStream inputStream = null;
		Properties appProperties = null;

		try {
			Properties prop = new Properties();
			String propFileName = "config.properties_local";

			inputStream = Producer.class.getClassLoader().getResourceAsStream(propFileName);
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

