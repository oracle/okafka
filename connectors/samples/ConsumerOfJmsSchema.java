/*
** Kafka Connect for TxEventQ.
**
** Copyright (c) 2024, 2025 Oracle and/or its affiliates.
** Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.
*/

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.util.Properties;
import java.util.List;
import java.util.UUID;
import java.util.Iterator;
import java.time.Duration;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.json.JSONObject;
import java.util.Base64;
import java.nio.charset.StandardCharsets;

/*
 * Pass in the name of the Kafka topic to consume from for the first argument.
 * Pass in the number of messages to consume from the Kafka topic for the second argument. 
 */
public class ConsumerOfJmsSchema {
	public static void main(String[] args) {
		
		String kafkaTopic = args[0];
		int numOfMsgToConsume = Integer.valueOf(args[1]);
	    final int BLOCK_TIMEOUT_MS = 3000;
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id" , UUID.randomUUID().toString());
		props.put("auto.offset.reset","earliest");

		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		KafkaConsumer<String , String> consumer = new KafkaConsumer<String, String>(props);

		consumer.subscribe(List.of(kafkaTopic));
        int msgCounter = 0;
		 
		while(msgCounter < numOfMsgToConsume) 
		{
			ConsumerRecords <String, String> records = consumer.poll(Duration.ofMillis(BLOCK_TIMEOUT_MS));
			System.out.println("");
			
			for (ConsumerRecord<String, String> record : records)
			{
				msgCounter++;
				JSONObject jsonObject = new JSONObject(record.value());
				 
				System.out.println("********Gets list of JSON keys for the schema.********");
				
				Iterator<?> keys = jsonObject.keys();
				while (keys.hasNext()) {
					String key = (String)keys.next();
				    System.out.println("JSON Keys:" + key);
				}
				
				System.out.println("");
				
				String messageId = jsonObject.getString("messageId");
				System.out.println("messageId = " + messageId);
				 
				String messageType = jsonObject.getString("messageType");
				System.out.println("messageType = " + messageType);
				 
				if (!jsonObject.get("correlationId").equals(null)) {
					String correlationId = jsonObject.getString("correlationId");
					System.out.println("correlationId = " + correlationId);
				}
				
				// Gets the payloadBytes property and decodes the message.
				if (!jsonObject.get("payloadBytes").equals(null)) {
					String payloadBytes = jsonObject.get("payloadBytes").toString();
					System.out.println("Undecoded payloadBytes: " + payloadBytes);
					System.out.println("Decoded payloadBytes: " + decodeKafkaMessage(payloadBytes));
				}
				
				// Gets the payloadText message.
				if (!jsonObject.get("payloadText").equals(null)) {
					String payloadText = jsonObject.get("payloadText").toString();
					System.out.println("payloadText: " + payloadText);
				}
				
				/**
				 * Gets the payloadMap message that is a Map of keys/values.
				 * If access to different property values are required follow the example
				 * below for the properties schema.
				 */
				if (!jsonObject.get("payloadMap").equals(null)) {
					JSONObject payloadMapJsonObject  =  jsonObject.getJSONObject("payloadMap");
					System.out.println("payloadMap: " + payloadMapJsonObject);
				}
				
				System.out.println("");
				
				/*********************************************************/
				/* Gets the different values from the properties schema. */
				/*********************************************************/
				  
				JSONObject propertiesJsonObject  =  jsonObject.getJSONObject("properties");
				System.out.println("********Gets the different values from the properties schema.********");
				
				// Since the propertyType for JMSXRcvTimestamp property is a long use the getLong to get the value.			 
				JSONObject jmsXrcvTimestamp = propertiesJsonObject.getJSONObject("JMSXRcvTimestamp");
				long jmsXrcvTimestampVal = jmsXrcvTimestamp.getLong("long");
				System.out.println("JMSXRcvTimestamp: " + jmsXrcvTimestampVal);
					
				// Since the propertyType for JMSXDeliveryCount property is a integer use the getInt to get the value.		
				JSONObject jmsXdeliveryCount = propertiesJsonObject.getJSONObject("JMSXDeliveryCount");
				int jmsXdeliveryCountVal = jmsXdeliveryCount.getInt("integer");
				System.out.println("JMSXDeliveryCount: " + jmsXdeliveryCountVal);
				
				// Since the propertyType for JMSXState property is a integer use the getInt to get the value.		
				JSONObject jmsXstate = propertiesJsonObject.getJSONObject("JMSXState");
				int jmsXstateVal = jmsXstate.getInt("integer");
				System.out.println("JMSXState: " + jmsXstateVal);
				
				System.out.println("");
				
				/*********************************************************/
				/* Gets the different values from the destination schema. */
				/*********************************************************/
				  
				JSONObject destinationJsonObject  =  jsonObject.getJSONObject("destination");
				System.out.println("********Gets the different values from the Destination schema.********");
				String destinationType = destinationJsonObject.getString("type");
				System.out.println("type= " + destinationType);
				
				String destinationName = destinationJsonObject.getString("name");
				System.out.println("name= " + destinationName);
				
				if (destinationType.equals("topic") || destinationType.equals("queue")) {
					String destinationOwner = destinationJsonObject.getString("owner");
					System.out.println("owner= " + destinationOwner);
					
					String destinationCompleteName = destinationJsonObject.getString("completeName");
					System.out.println("completeName= " + destinationCompleteName);
					
					String destinationCompleteTableName = destinationJsonObject.getString("completeTableName");
					System.out.println("completeTableName= " + destinationCompleteTableName);
					
				}
				
				System.out.println("");
				
				/*********************************************************/
				/* Gets the different values from the replyTo schema. */
				/*********************************************************/
				
				if (!jsonObject.get("replyTo").equals(null))
				{
					JSONObject replyToJsonObject  =  jsonObject.getJSONObject("replyTo");
					System.out.println("********Gets the different values from the replyTo schema.********");
					String replyToType = replyToJsonObject.getString("type");
					System.out.println("type= " + replyToType);
				
					String replyToName = replyToJsonObject.getString("name");
					System.out.println("name= " + destinationName);
				
					if (replyToType.equals("agent")) {
						if (!replyToJsonObject.get("address").equals(null)) {
							String replyToAddress = replyToJsonObject.getString("address");
							System.out.println("address= " + replyToAddress);
						}else {
							System.out.println("address= ");
						}
						
						if (!replyToJsonObject.get("protocol").equals(null)) {
							int replyToProtocol = replyToJsonObject.getInt("protocol");
							System.out.println("protocol= " + replyToProtocol);
						}
					}
				
					System.out.println("");
				}
				
			}
		}
	}
	 
	// This method is used to decode the JMS Bytes message stored in the payloadBytes schema property.
	public static String decodeKafkaMessage(String encodedMessage) {
		// Decode the Base64 encoded string
        byte[] decodedBytes = Base64.getDecoder().decode(encodedMessage);

        // Convert the byte array to a String (assuming UTF-8 encoding)
        return new String(decodedBytes, StandardCharsets.UTF_8);
    }
}