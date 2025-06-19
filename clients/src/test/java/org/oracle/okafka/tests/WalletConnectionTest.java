package org.oracle.okafka.tests;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.Test;

//import oracle.jdbc.driver.OracleConnection;
//import oracle.jdbc.OracleConnection;
import oracle.jdbc.internal.OracleConnection;
import oracle.jdbc.pool.OracleDataSource;

public class WalletConnectionTest {
	
	@Test
	public  void connectionTest() throws Exception {
		OracleDataSource ds = new OracleDataSource();

		ds.setURL("jdbc:oracle:thin:@sdb");
//		ds.setURL("jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(PORT=1521)(HOST=PHOENIX307317.DEV3SUB2PHX.DATABASEDE3PHX.ORACLEVCN.COM))(CONNECT_DATA=(SERVICE_NAME=CDB1_PDB1.REGRESS.RDBMS.DEV.US.ORACLE.COM)(INSTANCE_NAME=VS)))");
//		ds.setUser("aq");
//		ds.setPassword("aq");
//				System.setProperty("oracle.net.tns_admin", "");
		ds.setConnectionProperty("oracle.net.tns_admin", "./walletDir");
		ds.setConnectionProperty("oracle.net.wallet_location", "file:" +"./walletDir" );
//		ds.setConnectionProperty("oracle.jdbc.loadBalance", "false");
//		ds.setConnectionProperty("oracle.jdbc.fanEnabled", "false");
		ds.setConnectionProperty("oracle.jdbc.targetInstanceName","vs2");//		System.setProperty("oracle.net.wallet_location",
//				"(SOURCE=(METHOD=FILE)(METHOD_DATA=(DIRECTORY=/path/to/wallet)))");
		Connection conn;
		try {
		conn = ds.getConnection();
		}catch(Exception e){
			System.out.println(e);
			throw e;
		}
		int instId = Integer.parseInt(
				((oracle.jdbc.internal.OracleConnection) conn).getServerSessionInfo().getProperty("AUTH_INSTANCE_NO"));
		String serviceName = ((oracle.jdbc.internal.OracleConnection) conn).getServerSessionInfo()
				.getProperty("SERVICE_NAME");
		String instanceName = ((oracle.jdbc.internal.OracleConnection) conn).getServerSessionInfo()
				.getProperty("INSTANCE_NAME");
		String userName = conn.getMetaData().getUserName();
		String host = ((oracle.jdbc.internal.OracleConnection) conn).getServerSessionInfo()
				.getProperty("HOST_NAME");
		System.out.println(instId+ " "+ instanceName+ " "+ host );
//		try (Statement stmt = conn.createStatement();
//			     ResultSet rs = stmt.executeQuery("SELECT SYS_CONTEXT('USERENV', 'SERVER_HOST') FROM DUAL")) {
//			    if (rs.next()) {
//			        String connectedHost = rs.getString(1);
//			        System.out.println("Connected to host: " + connectedHost);
//			    }
//			}
		OracleConnection oraConn = conn.unwrap(OracleConnection.class);
//		String instanceName = oraConn.getServerSessionInfo().getProperty("INSTANCE_NAME");
		String hostName= oraConn.getServerSessionInfo().getProperty("SERVER_HOST");
		String dbHost = ((oracle.jdbc.internal.OracleConnection)conn).getServerSessionInfo().getProperty("AUTH_SC_SERVER_HOST");
		String dbDomain = ((oracle.jdbc.internal.OracleConnection)conn).getServerSessionInfo().getProperty("AUTH_SC_DB_DOMAIN");
		dbHost = dbHost +"."+dbDomain;
		String port = ((oracle.jdbc.internal.OracleConnection) conn).getServerSessionInfo()
				.getProperty("AUTH_SC_SERVER_PORT");
		System.out.println(port);
		System.out.println("Connected to instance: " + instanceName);
		System.out.println("Connected to host: " + dbHost);
		System.out.println(conn.getMetaData().getURL());
		System.out.println(conn.toString());
	}
}
