/*
** OKafka Java Client version 23.4.
**
** Copyright (c) 2019, 2024 Oracle and/or its affiliates.
** Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.
*/

package org.oracle.okafka.clients.producer.internals;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.utils.LogContext;
import org.oracle.okafka.common.utils.ConnectionUtils;
import org.slf4j.Logger;

public class OracleTransactionManager {
	
	enum TransactionState
	{
		PRE_INIT,
		INIT_INVOKED,
		BEGIN,
		COMMITTED,
		ABORTED
	}
	
	ArrayList<Future<RecordMetadata>> recordsInTxn;
	Connection conn;
	TransactionState tState = TransactionState.PRE_INIT;
	String clientTransactionId;
	String connId;
	protected final Logger log ;
	private boolean initTxnCalled;
	private String sId;
	private String serialNo;
	private String instanceName;
	private String serverPid;
	
	SimpleDateFormat sdf = new SimpleDateFormat("dd_MM_yyyy_HH_mm_ss_SSS");
	
	public OracleTransactionManager(LogContext logContext )
	{
		log = logContext.logger(OracleTransactionManager.class);
		recordsInTxn = new ArrayList<Future<RecordMetadata>>();
	}
	public void initTxn()
	{
		initTxnCalled = true;
		tState = TransactionState.INIT_INVOKED;
	}
	
	public void beginTransaction() throws IllegalStateException 
	{
		if(tState == TransactionState.PRE_INIT)
			throw new IllegalStateException("InIt transaction not invoked for this KafkaProducer");
		else if(tState == TransactionState.BEGIN)
		{
			throw new IllegalStateException("KafkaProducer already part of an existing transaction. Either abort it or commit it");
		}
		
		tState = TransactionState.BEGIN;
		if(conn != null)
		{
			clientTransactionId = generateLocalTransactionId();
			log.debug("Client Transaction id " + clientTransactionId);
		}
		
	}
	
	public void abortTransaction() throws IllegalStateException, SQLException
	{
		/*if(tState != TransactionState.BEGIN) 
		{
			throw new IllegalStateException("KafkaProducer not part of any transaction.");
		}*/
		tState = TransactionState.ABORTED;
		try {
			conn.rollback();
		}catch(Exception e) {
			log.error(clientTransactionId +": Exception during rollback of transaction." + e, e);
			throw e;
		}finally {
			clientTransactionId = null;
			conn = null;	
		}
	}
	/*
	 *  Commits the records 
	 */
	public void commitTransaction() throws IllegalStateException, Exception 
	{
		log.debug("Commiting Transaction. TransactionState "+tState+". Local Transaction Id:"+clientTransactionId+". Database Connection " + conn);
		
		/*if(tState != TransactionState.BEGIN)
		{
			throw new IllegalStateException("KafkaProducer not part of any transaction.Transaction state is not 'BEGIN' ");
		}*/
		
		if(conn == null)
		{
			return;
		}
		try {
			if(sId == null)
			{
				oracle.jdbc.internal.OracleConnection connInternal = (oracle.jdbc.internal.OracleConnection)conn;
				sId = connInternal.getServerSessionInfo().getProperty("AUTH_SESSION_ID");
				serialNo = connInternal.getServerSessionInfo().getProperty("AUTH_SERIAL_NUM");
				serverPid = connInternal.getServerSessionInfo().getProperty("AUTH_SERVER_PID");
				instanceName = connInternal.getServerSessionInfo().getProperty("");
			}
	        log.debug("Commiting database transaction at instance: "+ instanceName +". Session information: " +  sId +","+serialNo+". Process id:" + serverPid+"."); 
		}catch(Exception ignoreE)
		{
			
		}
		
		RuntimeException rException = null;
		Collections.reverse(recordsInTxn);
		
		for(Future<RecordMetadata> frm : recordsInTxn) {
			try {
				if(!frm.isDone())
				{
					frm.get();
				}
				
			}catch(Exception exeException)
			{
				throw exeException;
			}
			rException  = ((FutureRecordMetadata)frm).error();
			if(rException != null)
				throw rException;
		}
		
		try {
		conn.commit();
		}
		catch(Exception e) {
			String dbInfo = String.format(" .Database Session Information.(instance name, session id, serial#)(%s,%s,%s)", instanceName, sId, serialNo);
			String excpMsg = getLocalTransactionId();
			if(conn.isClosed())
			{
				excpMsg +=":Exception while committing kafka transaction. Database connection found closed. Exception:" +e.getMessage();				
				excpMsg += dbInfo;
				throw new DisconnectException(excpMsg, e);
			}
			boolean isAlive = isConnectionAlive();
			if(isAlive)
			{
				String msgId = null;
				String topicName = null;
				if(recordsInTxn.size() == 0)
				{
					excpMsg += ":No record produced in this transaction. Exception during database commit operation:" + e.getMessage();
					excpMsg += "Application should abort this transaction and retry the operations.";
					throw new KafkaException(excpMsg+dbInfo, e);
				}
				FutureRecordMetadata frm = (FutureRecordMetadata)recordsInTxn.get(0);
				ProduceRequestResult rs = frm.requestResult();
				boolean msgIdExist  = false;
				if(rs != null && rs.msgIds() != null && rs.msgIds().size() > 0)
				{
					msgId = rs.msgIds().get(0).getMsgId();
					msgId = msgId.substring(3);
					topicName = frm.get().topic();
					msgIdExist = ConnectionUtils.checkIfMsgIdExist(conn, topicName,msgId , log);
				}
				
				if(msgIdExist)
				{
					log.info("Commit successful despite exception: " + e + "Application should not need to retry.");
					
					if(log.isDebugEnabled())
					{
						excpMsg += ": Exception while commiting transaction -" +e.getMessage();
						log.error(excpMsg + dbInfo, e);
						log.info("Check database server trace file:  " + dbInfo +" for more information.");
					}
				}
				else 
				{
					excpMsg+= " :KafkaProducer failed to commit this transaction due to exception:"+e.getMessage();
					excpMsg+=" Application should abort this transaction and retry the operations.";
					KafkaException kE = new KafkaException(excpMsg+dbInfo , e);
					throw kE;
				}
			}
			else
			{
				excpMsg +=" : Transactional poruducer disconnected from Oracle Database due to exception -" +e.getMessage();
				throw new DisconnectException(excpMsg + dbInfo , e);
			}
		}
		finally {
			recordsInTxn.clear();
			clientTransactionId = null;
		}
		tState = TransactionState.COMMITTED;
		//Next BeginTransaction may use another connection 
		
		conn = null;
	}
	
	public String getLocalTransactionId()
	{
		return clientTransactionId;
	}
	synchronized public void addRecordToTransaction(Future<RecordMetadata> frm)
	{
		recordsInTxn.add(frm);
	}
	
	synchronized public Connection getDBConnection()
	{
		return conn;
	}
	
	synchronized public void setDBConnection(Connection _conn)
	{
		if(conn!=null && conn ==_conn)
		{
			return;
		}
		
		if(conn == null) 
		{
			conn = _conn;
			if(clientTransactionId == null)
			{
				clientTransactionId = generateLocalTransactionId();
				log.debug("Transaction id " + clientTransactionId);
			}
			
			return;
		}
		
		if(tState == TransactionState.BEGIN)
		{
			log.error("Transaction has already begun with a different database connection.");
			throw new KafkaException("A transaction with another oracle connection already in process");
		}
		conn = _conn;
	}
	
	synchronized  TransactionState getTransactionState()
	{
		return tState;
	}
	
	String generateLocalTransactionId()
	{
		String localTransactionId = null;
		try {
			oracle.jdbc.internal.OracleConnection  connInternal = (oracle.jdbc.internal.OracleConnection)conn;
			sId = connInternal.getServerSessionInfo().getProperty("AUTH_SESSION_ID");
			serialNo = connInternal.getServerSessionInfo().getProperty("AUTH_SERIAL_NUM");
			instanceName = connInternal.getServerSessionInfo().getProperty("INSTANCE_NAME");
			serverPid = connInternal.getServerSessionInfo().getProperty("AUTH_SERVER_PID");
			String  userName = conn.getMetaData().getUserName();

			localTransactionId = instanceName +"_"+ userName +"_" + sId +"_" + serialNo +"_" + (sdf.format(new Date()));  
			log.debug("Client Transaction id " + localTransactionId);

		}catch(Exception e) {
		}
		
		return localTransactionId;
	}
	
	boolean isConnectionAlive() 
	{
		if(conn == null)
			return false;
		
		String PING_QUERY = "SELECT banner FROM v$version where 1<>1";
		try (PreparedStatement stmt = conn.prepareStatement(PING_QUERY)) {
			stmt.setQueryTimeout(1);
			stmt.execute("SELECT banner FROM v$version where 1<>1");
			stmt.close();
		}
		catch(Exception ignoreE)
		{
			return false;
		}
		return true;
	}
}
