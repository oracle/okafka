/*
** OKafka Java Client version 0.8.
**
** Copyright (c) 2019, 2020 Oracle and/or its affiliates.
** Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.
*/

package org.oracle.okafka.common.utils;

import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import javax.jms.JMSException;
import javax.jms.TopicSession;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;

import org.oracle.okafka.clients.CommonClientConfigs;
import org.oracle.okafka.common.Node;
import org.oracle.okafka.common.config.AbstractConfig;
import org.oracle.okafka.common.config.SslConfigs;
import org.oracle.okafka.common.errors.ConnectionException;

import oracle.jdbc.pool.OracleDataSource;
import oracle.jms.AQjmsFactory;

public class ConnectionUtils {
    
    public static String createUrl(Node node, AbstractConfig configs) {
    	
    	if( !configs.getString(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG).equalsIgnoreCase("PLAINTEXT")) {
		  return "jdbc:oracle:thin:@" + configs.getString(SslConfigs.TNS_ALIAS); // + "?TNS_ADMIN=" + configs.getString(SslConfigs.ORACLE_NET_TNS_ADMIN); 
        }
    	StringBuilder urlBuilder =new StringBuilder("jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(PORT=" + Integer.toString(node.port())+")(HOST=" + node.host() +"))");
		urlBuilder.append("(CONNECT_DATA=(SERVICE_NAME=" + node.serviceName() + ")"+"(INSTANCE_NAME=" + node.instanceName() + ")))");
    	return urlBuilder.toString();
    }
    public static Connection createJDBCConnection(Node node, AbstractConfig configs) throws SQLException{
    	OracleDataSource s=new OracleDataSource();
		s.setURL(createUrl(node, configs));
	    return s.getConnection();
    }
    
    public static TopicConnection createTopicConnection(Node node,AbstractConfig configs) throws JMSException {
    	if(node==null) 
    		throw new ConnectionException("Invalid argument: Node cannot be null");
    	String url = createUrl(node, configs);
    	OracleDataSource dataSource;
    	try {
    		dataSource =new OracleDataSource();
    		dataSource.setURL(url);	
    	}
    	catch(SQLException sql) {
    		throw new JMSException(sql.toString());
    	}
    	TopicConnectionFactory connFactory = AQjmsFactory.getTopicConnectionFactory(dataSource);
    	TopicConnection conn = connFactory.createTopicConnection();
        conn.setClientID(configs.getString(CommonClientConfigs.CLIENT_ID_CONFIG));
        	
        return conn;  	
    }
    
    public static TopicSession createTopicSession(TopicConnection conn, int mode, boolean transacted) throws JMSException {
    	if(conn == null)
    		throw new ConnectionException("Invalid argument: Connection cannot be null");
    	return conn.createTopicSession(transacted, mode);
    	
    }
    
    public static String getUsername(AbstractConfig configs) {
    	File file = null;
    	FileReader fr = null;
    	try {
    	file = new File(configs.getString(CommonClientConfigs.ORACLE_NET_TNS_ADMIN)+"/ojdbc.properties");
    	fr = new FileReader(file);
    	Properties prop = new Properties();
    	prop.load(fr);
    	return prop.getProperty("user");
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

}
