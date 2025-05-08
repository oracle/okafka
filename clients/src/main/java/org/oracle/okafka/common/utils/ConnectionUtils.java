/*
 ** OKafka Java Client version 23.4.
 **
 ** Copyright (c) 2019, 2024 Oracle and/or its affiliates.
 ** Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.
 */

package org.oracle.okafka.common.utils;

import java.io.File;
import java.io.FileReader;
import java.net.InetSocketAddress;
import java.nio.ByteOrder;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import javax.jms.JMSException;
import javax.jms.TopicSession;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;

import org.oracle.okafka.clients.CommonClientConfigs;
import org.oracle.okafka.common.Node;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.AbstractConfig;
import org.oracle.okafka.common.config.SslConfigs;
import org.oracle.okafka.common.errors.ConnectionException;
import org.oracle.okafka.common.errors.RecordNotFoundSQLException;
import org.oracle.okafka.common.utils.MessageIdConverter.OKafkaOffset;
import org.slf4j.Logger;

import oracle.jdbc.driver.OracleConnection;
import oracle.jdbc.pool.OracleDataSource;
import oracle.jms.AQjmsFactory;
import oracle.jms.AQjmsSession;
import oracle.jms.AQjmsTopicConnectionFactory;

public class ConnectionUtils {

	public static String createUrl(Node node, AbstractConfig configs) {

		if( !configs.getString(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG).equalsIgnoreCase("PLAINTEXT")) {
			return "jdbc:oracle:thin:@" + configs.getString(SslConfigs.TNS_ALIAS); // + "?TNS_ADMIN=" + configs.getString(SslConfigs.ORACLE_NET_TNS_ADMIN); 
		}
		StringBuilder urlBuilder =new StringBuilder("jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(PORT=" + Integer.toString(node.port())+")(HOST=" + node.host() +"))");
		urlBuilder.append("(CONNECT_DATA=(SERVICE_NAME=" + node.serviceName() + ")");
		if(node.instanceName()!=null && node.instanceName().length()>0)
		{
			urlBuilder.append("(INSTANCE_NAME=" + node.instanceName() + ")");
		}
		urlBuilder.append("))");
		String url = urlBuilder.toString();
		return url;
	}
	
	public static Connection createJDBCConnection(Node node, AbstractConfig configs, Logger log) throws SQLException{
		OracleDataSource s=new OracleDataSource();
		String dbUrl = createUrl(node, configs);
		s.setURL(dbUrl);
		try {
			Connection conn =  s.getConnection();
			int instId = Integer.parseInt(((oracle.jdbc.internal.OracleConnection)conn).getServerSessionInfo().getProperty("AUTH_INSTANCE_NO"));
			String serviceName = ((oracle.jdbc.internal.OracleConnection)conn).getServerSessionInfo().getProperty("SERVICE_NAME");
			String instanceName = ((oracle.jdbc.internal.OracleConnection)conn).getServerSessionInfo().getProperty("INSTANCE_NAME");
			String userName = conn.getMetaData().getUserName();
			node.setId(instId);
			node.setService(serviceName);
			node.setInstanceName(instanceName);
			node.setUser(userName);
			node.updateHashCode();
			return conn;
		}catch(Exception e)
		{
			log.error("Exception while connecting to database with connection string " + dbUrl +":" + e);
			throw e;
		}
	}
	
	public static TopicConnection createTopicConnection(java.sql.Connection dbConn, AbstractConfig configs, Logger log)
				throws JMSException {
		if(dbConn==null) 
			throw new ConnectionException("Invalid argument: Connection cannot be null");
		
		try {
			log.info("Topic Connection to Oracle Database : " + dbConn.getMetaData().getURL());
		}catch(Exception ignoreE)
		{
		}
		
		TopicConnection conn = AQjmsTopicConnectionFactory.createTopicConnection(dbConn);
		conn.setClientID(configs.getString(CommonClientConfigs.CLIENT_ID_CONFIG));
		return conn; 
	}

	public static TopicConnection createTopicConnection(Node node,AbstractConfig configs, Logger log) throws JMSException {
		if(node==null) 
			throw new ConnectionException("Invalid argument: Node cannot be null");

		String url = createUrl(node, configs);
		log.info("Connecting to Oracle Database : "+ url);
		OracleDataSource dataSource;
		try {
			dataSource =new OracleDataSource();
			dataSource.setURL(url);	
		}
		catch(SQLException sql) {
			throw new JMSException(sql.toString(), String.valueOf(sql.getErrorCode()));
		}
		TopicConnectionFactory connFactory = AQjmsFactory.getTopicConnectionFactory(dataSource);
		TopicConnection conn = connFactory.createTopicConnection();
		conn.setClientID(configs.getString(CommonClientConfigs.CLIENT_ID_CONFIG));
		return conn;  	
	}

	public static TopicSession createTopicSession(TopicConnection conn, int mode, boolean transacted) throws JMSException {
		if(conn == null)
			throw new ConnectionException("Invalid argument: Connection cannot be null");
		TopicSession sess = conn.createTopicSession(transacted, mode);
		//ToDo: Validate if caching of dequeue statement helps or not
		((AQjmsSession)sess).setDeqStmtCachingFlag(true);
		return sess;
	}
	
	public static boolean isSessionClosed(AQjmsSession sess) {
		Connection con;
		try {
			con = ((oracle.jms.AQjmsSession)sess).getDBConnection();
			if(con == null || con.isClosed())
				return true;
		} catch (JMSException | SQLException e) {
			return true;
		}
		return false;
	}

	public static String getUsername(AbstractConfig configs) {
		File file = null;
		FileReader fr = null;
		try {
			file = new File(configs.getString(CommonClientConfigs.ORACLE_NET_TNS_ADMIN)+"/ojdbc.properties");
			fr = new FileReader(file);
			Properties prop = new Properties();
			prop.load(fr);
			return prop.getProperty("user").trim();
		} catch( Exception exception) {
			//do nothing
		} finally {
			try {
				if(fr != null)
					fr.close();
			}catch (Exception e) {

			}	

		}
		return null;
	}
	public static String enquote(String name) throws IllegalArgumentException{
		if( !name.contains("'")) {
			if(!name.contains("\"")) return "\"" + name + "\"";
			if(name.indexOf("\"") == 0 && name.indexOf("\"", 1) == name.length() -1 )
				return name;

		}
		throw new IllegalArgumentException("Invalid argument provided: " + name);	
	}

	public static String getDBVersion(Connection conn) throws Exception
	{
		String dbVersionQuery = "select version_full from PRODUCT_COMPONENT_VERSION where product like  'Oracle Database%'";
		String dbVersionStr = "";
		PreparedStatement dbVerStmt =  null;
		ResultSet rs = null;
		try {
			dbVerStmt = conn.prepareStatement(dbVersionQuery);
			dbVerStmt.execute();
			rs = dbVerStmt.getResultSet();
			if(rs.next()) {
				dbVersionStr = rs.getString(1);
			}
		}catch(Exception e)
		{
			throw e;
		}
		finally {
			if(rs != null)
				rs.close();

			if(dbVerStmt != null)
				dbVerStmt.close();
		}
		return dbVersionStr;
	}
	
	public static int getInstanceId(Connection conn)
	{
		int instNum = 0;
		
		try {
			oracle.jdbc.internal.OracleConnection oracleInternalConn = (oracle.jdbc.internal.OracleConnection)conn;
			instNum = Integer.parseInt(oracleInternalConn.getServerSessionInfo().getProperty("AUTH_INSTANCE_NO"));

		}catch(Exception ignoreE) {
			
		}
		return instNum;
	}

	public static  byte[] convertTo4Byte(int len)
	{
		byte[] bArray = new byte[4];

		bArray[0] = (byte)( len >>> 24 );
		bArray[1] = (byte)( len >>> 16 );
		bArray[2] = (byte)( len >>> 8 );
		bArray[3] = (byte)( len );

		return bArray;
	}

	public static int convertToInt(byte[] bInt)
	{
		return (((bInt[0] & 0xff) << 24) | 
				((bInt[1] & 0xff) << 16) |
				((bInt[2] & 0xff) << 8)  |
				(bInt[3] & 0xff));
	}
	
	public static void remDuplicateEntries(List<InetSocketAddress> address)
	{
		if(address == null || address.size() == 0)
			return;
		
		HashMap<String, InetSocketAddress>  uniqueAddr = new HashMap<String, InetSocketAddress>(address.size());
		Iterator<InetSocketAddress> addIter = address.iterator();
		while(addIter.hasNext())
		{
			InetSocketAddress addr = addIter.next();
			if(uniqueAddr.containsKey(addr.getHostName()))
			{
				addIter.remove();
			}
			else
			{
				uniqueAddr.put(addr.getHostName(), addr);
			}
		}
	}
	
	public static boolean checkIfMsgIdExist(Connection con,String topicName, String msgId , Logger log)
	{
		if(topicName == null || msgId == null)
			return false;
		
		boolean msgIdExists = false;
		OKafkaOffset okafkaOffset = MessageIdConverter.getOKafkaOffset("ID:"+msgId,true, true); 
		
		String qry = 
				"DECLARE " +
				"    queue_name VARCHAR2(128) := ?; " +
				"    shard_num NUMBER := ?; " +
				"    subshard_num NUMBER := ?; " +
				"	 msg_id VARCHAR2(128) := ?; " +
				"    partition_name VARCHAR2(50); " +
				" 	 msgCount NUMBER; " +
				"BEGIN " +
				"    SELECT LOWER(PARTNAME) INTO partition_name " +
				"    FROM USER_QUEUE_PARTITION_MAP " +
				"    WHERE QUEUE_TABLE = queue_name AND SHARD = shard_num AND SUBSHARD = subshard_num; " +
				"    EXECUTE IMMEDIATE " +
				"        'SELECT COUNT(*) " +
				"         FROM ' || DBMS_ASSERT.SQL_OBJECT_NAME(queue_name) || ' PARTITION (' || partition_name || ') " +
				"	 	  WHERE MSGID = ''' || msg_id || ''' ' " +
				"    INTO msgCount; " +
				"    ? := msgCount; " +
				"EXCEPTION " +
				"    WHEN OTHERS THEN " +
				"        RAISE; " +
				"END;";
		
		log.debug("Executing " + qry);
		CallableStatement stmt = null;
		try {
			stmt = con.prepareCall(qry);
			stmt.setString(1, topicName);
			stmt.setLong(2, okafkaOffset.partitionId());
			stmt.setLong(3, okafkaOffset.subPartitionId());
			stmt.setString(4, msgId);
			stmt.registerOutParameter(5, Types.INTEGER);

			stmt.execute();

			int msgCnt = stmt.getInt(5);

			if (msgCnt != 0)
				msgIdExists = true;

		}catch(Exception e)
		{
			log.info("Exception while checking if msgId Exists or not. " + e,e);
			try {
				if (stmt != null)
					stmt.close();
			} catch (Exception ex) {
				// do nothing
			}
		}
		log.debug("Message Id "+  msgId +" Exists?: " + msgIdExists);
		return msgIdExists;
	}
	
	public static String getConnectedService(Connection conn)
	{
		String serviceName  = null;
		try {
			serviceName = ((oracle.jdbc.internal.OracleConnection)conn).getServerSessionInfo().getProperty("SERVICE_NAME");
		}catch(Exception e)
		{
			return null;
		}
		return serviceName;
	}
	public static String getConnectedHostnPort(Connection conn)
	{
		String hostnPort = null;
		try {
			String url = conn.getMetaData().getURL();
			String host = TNSParser.getProperty(url, "HOST");
			if(host == null)
			{
				return null;
			}
			
			String portStr = TNSParser.getProperty(url, "PORT");
			if(portStr== null)
				return null;
			
			hostnPort = host+":"+portStr;
		}
		catch(Exception e) {

		}
		return hostnPort;
	}
	
	public static SQLException getSQLException(Throwable cause) {
		while (cause != null) {
			if (cause instanceof SQLException) {
				return (SQLException) cause;
			}
			cause = cause instanceof JMSException ?
					((JMSException) cause).getLinkedException() : cause.getCause();
		}
		return null; 
	}
	
	public static Uuid getIdByTopic(Connection con,String topic) throws SQLException {
		Uuid topicId;
		String query;
		query="select qid from user_queues where name = upper(?)";
	
		PreparedStatement stmt = null;
		stmt = con.prepareStatement(query);
		stmt.setString(1, topic);
		ResultSet result = stmt.executeQuery();
		if(result.next()) {
			topicId = new Uuid(0,result.getInt("qid"));
		}
		else {
			result.close();
			throw new RecordNotFoundSQLException("topic "+ topic +" doesn't exist");
		}
		result.close();
		try
		{
			if (stmt != null)
				stmt.close();
		}catch(Exception ex){
			// do nothing
		}
		return topicId;
	}
	
	public static String getTopicById(Connection con, Uuid topicId) throws SQLException {
		String topicName;
		String query;
		query="select name from user_queues where qid = ?";
	
		PreparedStatement stmt = null;
		stmt = con.prepareStatement(query);
		stmt.setLong(1, topicId.getLeastSignificantBits());
		ResultSet result = stmt.executeQuery();
		if(result.next()) {
			topicName = result.getString("name");
		}
		else {
			result.close();
			throw new RecordNotFoundSQLException("topic Id "+ topicId.toString() +" doesn't exist");
		}
		result.close();
		try
		{
			if (stmt != null)
				stmt.close();
		}catch(Exception ex){
			// do nothing
		}
		return topicName;
	}
	
	
	
}
