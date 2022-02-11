/*
** OKafka Java Client version 0.8.
**
** Copyright (c) 2019, 2020 Oracle and/or its affiliates.
** Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.
*/

package org.oracle.okafka.examples;

import java.util.Properties;

import org.oracle.okafka.clients.producer.KafkaProducer;
import org.oracle.okafka.clients.producer.ProducerRecord;


public class Producer {
	
	public static void main(String[] args) {			
			
		
		String topic = "topic" ;
		
        KafkaProducer<String,String> prod = null;		
		Properties props = new Properties();
		
		props.put("oracle.instance.name", "instancename"); //name of the oracle databse instance
		props.put("oracle.service.name", "servicename");	//name of the service running on the instance    
		props.put("oracle.net.tns_admin", "location of tnsnames.ora/ojdbc.properties file"); //eg: "/user/home" if ojdbc.properies file is in home  
	    
		props.put("bootstrap.servers", "host:port"); //ip address or host name where instance running : port where instance listener running
		props.put("batch.size", 200);
		props.put("linger.ms", 100);
		props.put("buffer.memory", 335544);
		props.put("key.serializer", "org.oracle.okafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.oracle.okafka.common.serialization.StringSerializer");	
		
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
}

