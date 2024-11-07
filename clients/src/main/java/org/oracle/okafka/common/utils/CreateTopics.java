/*
** OKafka Java Client version 23.4.
**
** Copyright (c) 2019, 2024 Oracle and/or its affiliates.
** Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.
*/

package org.oracle.okafka.common.utils;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.oracle.okafka.common.network.AQClient;
import org.oracle.okafka.common.requests.CreateTopicsRequest.TopicDetails;

public class CreateTopics {

	private static void setQueueParameter(Connection jdbcConn, String topic, String paraName, int paraValue) throws SQLException 
	{
		String qParaTxt = " begin sys.dbms_aqadm.set_queue_parameter(?, ?, ?); end;";
		CallableStatement qParaStmt = null;
		try {
		qParaStmt = jdbcConn.prepareCall(qParaTxt);
		qParaStmt.setString(1, ConnectionUtils.enquote(topic));
		qParaStmt.setString(2, paraName);
		qParaStmt.setInt(3, paraValue);
		qParaStmt.execute(); 
		} catch(SQLException e) {
			throw e;
		}finally {
			if(qParaStmt != null) {
				try {
					qParaStmt.close();
				}catch(Exception e) {}
			}
		}
	}
	
	private static void startTopic(Connection jdbcConn, String topic) throws SQLException 
	{
		String sQTxt = "begin sys.dbms_aqadm.start_queue(?); end;";
		CallableStatement sQStmt = null;
		try {
		sQStmt = jdbcConn.prepareCall(sQTxt);
		sQStmt.setString(1, ConnectionUtils.enquote(topic));
		sQStmt.execute(); 
		} catch(SQLException e) {
			throw e;
		}finally {
			if(sQStmt != null) {
				try {
					sQStmt.close();
				} catch(Exception e) {}
			 }
		}
	}

	private static void setPartitionNum(Connection jdbcConn, String topic, int numPartition) throws SQLException {
		setQueueParameter(jdbcConn, topic, "SHARD_NUM", numPartition);
	}
	private static void setKeyBasedEnqueue(Connection jdbcConn, String topic) throws SQLException {
		setQueueParameter(jdbcConn, topic, "KEY_BASED_ENQUEUE", 2);
	}
	private static void setStickyDeq(Connection jdbcConn, String topic) throws SQLException {
		setQueueParameter(jdbcConn, topic, "STICKY_DEQUEUE", 1);
	}
	
	public static Map<String, Exception> createTopics(Connection jdbcConn, Map<String, TopicDetails> topics, Map<String, Uuid> topicIdMap) throws SQLException { 
		 CallableStatement cStmt = null;
		 Map<String, Exception> result = new HashMap<>();
		 try {		 
			 String topic;
			 TopicDetails details;
			 long retentionSec = 7*24*3600;
			 for(Map.Entry<String, TopicDetails> topicDetails : topics.entrySet()) {
				 topic = topicDetails.getKey().trim();
				 details = topicDetails.getValue();
				 try {
					 for(Map.Entry<String, String> config : details.configs.entrySet()) {
						 String property = config.getKey().trim();
						 if(property.equals("retention.ms")) {
							 retentionSec = Long.parseLong(config.getValue().trim())/1000;
					     }
						 else throw new InvalidConfigurationException("Invalid configuration: " + property + " provided for topic: " + topic);
					 }
					 cStmt = jdbcConn.prepareCall("{call DBMS_TEQK.AQ$_CREATE_KAFKA_TOPIC(topicname=>? ,partition_num=>?, retentiontime=>?)}");
					 cStmt.setString(1, ConnectionUtils.enquote(topic));
					 cStmt.setInt(2, details.numPartitions);
					 cStmt.setLong(3,retentionSec);
					 cStmt.execute();
					 try {
						 topicIdMap.put(topic, AQClient.getIdByTopic(jdbcConn,topic));
					 } catch(Exception e){
						 topicIdMap.put(topic, Uuid.ZERO_UUID);
					 }
					 
				 } catch(SQLException sqlEx) {
					 if ( sqlEx.getErrorCode() == 24019 || sqlEx.getErrorCode() == 44003 ) {
						 result.put(topic, new InvalidTopicException(sqlEx)); 
				   }
					 else if ( sqlEx.getErrorCode() == 24001 ) {
						 result.put(topic, new TopicExistsException("Topic already exists: ", sqlEx));
					 }
					 else result.put(topic, sqlEx);
				 } catch(Exception ex) {
					 result.put(topic, ex);
				 }
				 if(result.get(topic) == null) {
					 result.put(topic, null);
				 }
		    }
		 } finally {
			 try { 
				 if(cStmt != null)
					 cStmt.close();
			   } catch(Exception e) {
				 //do nothing
			   }
		 }
		 return result;
	}
}
