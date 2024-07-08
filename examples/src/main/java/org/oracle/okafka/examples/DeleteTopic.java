/*
** OKafka Java Client version 23.4.
**
** Copyright (c) 2019, 2024 Oracle and/or its affiliates.
** Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.
*/

package org.oracle.okafka.examples;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.oracle.okafka.clients.admin.AdminClient;
import org.oracle.okafka.clients.admin.DeleteTopicsResult;
import org.oracle.okafka.clients.admin.KafkaAdminClient;

public class OKafkaDeleteTopic {
	
	public static void main(String[] args) {
		Properties props = new Properties();
		props.put("oracle.service.name", "cdb1_pdb1.regress.rdbms.dev.us.oracle.com");	//name of the service running on the instance    
		props.put("oracle.net.tns_admin", "C:/Users/ichokshi/eclipse-workspace");  //eg: "/user/home" if ojdbc.properies file is in home  
		props.put("security.protocol","PLAINTEXT");			 
		//props.put("bootstrap.servers", "phoenix266026.dev3sub1phx.databasede3phx.oraclevcn.com:1521"); 
		props.put("bootstrap.servers", "phoenix94147.dev3sub2phx.databasede3phx.oraclevcn.com:1521");

		try (Admin admin = AdminClient.create(props)) {
	
			/*		NewTopic t1 = new NewTopic("TOPIC_TEQ", 5, (short)0);			
			kAdminClient.createTopics(Collections.singletonList(t1)); */
			
			org.apache.kafka.clients.admin.DeleteTopicsResult delResult = admin.deleteTopics(Collections.singletonList("TEQ"));
			
			//DeleteTopicsResult delResult = kAdminClient.deleteTopics(Collections.singletonList("TEQ2"), new org.oracle.okafka.clients.admin.DeleteTopicsOptions());
			
			Thread.sleep(5000);
			System.out.println("Auto Clsoing admin now");
		}
		catch(Exception e)
		{
			System.out.println("Exception while creating topic " + e);
			e.printStackTrace();
		}
		
		System.out.println("Main thread complete ");

	}

}
