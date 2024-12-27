package org.oracle.okafka.common.utils;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

import org.oracle.okafka.common.requests.ListOffsetsResponse.ListOffsetsPartitionResponse;

public class FetchOffsets {
	
	private static final String EARLIEST_OFFSET_PLSQL = """
				DECLARE
			    shard_num NUMBER := ?;
			    queue_name VARCHAR2(128) := ?;
			    partition_name VARCHAR2(50);
			    msg_id RAW(16);
			BEGIN
			    SELECT LOWER(PARTNAME) INTO partition_name
			    FROM USER_QUEUE_PARTITION_MAP
			    WHERE QUEUE_TABLE = queue_name AND SHARD = shard_num AND SUBSHARD = (
			        SELECT MIN(SUBSHARD)
			        FROM USER_QUEUE_PARTITION_MAP
			        WHERE QUEUE_TABLE = queue_name AND SHARD = shard_num
			    );

			    EXECUTE IMMEDIATE
			        'SELECT MIN(MSGID) FROM ' || DBMS_ASSERT.SQL_OBJECT_NAME(queue_name) || ' PARTITION (' || partition_name || ')'
			         INTO msg_id;

			        ? := msg_id;

			    EXCEPTION
			    WHEN OTHERS THEN
			   		RAISE;
			END;
			""";
	
	private static final String LATEST_OFFSET_PLSQL = """
				DECLARE
			    shard_num NUMBER := ?;
			    queue_name VARCHAR2(128) := ?;
			    partition_name VARCHAR2(50);
			    msg_id RAW(16);
			BEGIN
			    SELECT LOWER(PARTNAME) INTO partition_name
			    FROM USER_QUEUE_PARTITION_MAP
			    WHERE QUEUE_TABLE = queue_name AND SHARD = shard_num AND SUBSHARD = (
			        SELECT MAX(SUBSHARD)
			        FROM USER_QUEUE_PARTITION_MAP
			        WHERE QUEUE_TABLE = queue_name AND SHARD = shard_num
			    );

			    EXECUTE IMMEDIATE
			        'SELECT MAX(MSGID) FROM ' || DBMS_ASSERT.SQL_OBJECT_NAME(queue_name) || ' PARTITION (' || partition_name || ')'
			         INTO msg_id;

			        ? := msg_id;

			    EXCEPTION
			    WHEN OTHERS THEN
			   		RAISE;
			END;
			""";

	private static final String OFFSET_BY_TIMESTAMP_PLSQL = """
				DECLARE
			    shard_num NUMBER := ?;
			    queue_name VARCHAR2(128) := ?;
			    user_timestamp TIMESTAMP(6) WITH TIME ZONE := TO_TIMESTAMP_TZ(?, 'DD-MON-YY HH.MI.SSXFF AM TZR');
			    partition_list SYS.ODCIVARCHAR2LIST := SYS.ODCIVARCHAR2LIST();
			    next_timestamp TIMESTAMP(6) WITH TIME ZONE;
			    msg_id RAW(16);
			    found BOOLEAN := FALSE;
			BEGIN
			    SELECT LOWER(PARTNAME)
			    BULK COLLECT INTO partition_list
			    FROM USER_QUEUE_PARTITION_MAP
			    WHERE QUEUE_TABLE = queue_name AND SHARD = shard_num;

			    FOR i IN 1..partition_list.COUNT LOOP
			        BEGIN
			            EXECUTE IMMEDIATE
			                'SELECT MSGID, ENQUEUE_TIME
			                 FROM ' || DBMS_ASSERT.SQL_OBJECT_NAME(queue_name) || ' PARTITION (' || partition_list(i) || ')
			                 WHERE ENQUEUE_TIME >= :1
			                 ORDER BY ENQUEUE_TIME FETCH FIRST 1 ROW ONLY'
			                INTO msg_id, next_timestamp
			                USING user_timestamp;

			            found := TRUE;
			            EXIT;
			        EXCEPTION
			            WHEN NO_DATA_FOUND THEN
			                NULL;
			        END;
			    END LOOP;

			    IF NOT found THEN
			        RAISE_APPLICATION_ERROR(20003, 'No messages in the given partition');
			    END IF;

			    ? := msg_id;
			    ? := next_timestamp;

			    EXCEPTION
			    WHEN OTHERS THEN
			   		RAISE;
			END;
			""";
	
	private static final String MAX_TIMESTAMP_OFFSET_PLSQL = """
				DECLARE
			    shard_num NUMBER := ?;
			    queue_name VARCHAR2(128) := ?;
			    partition_name VARCHAR2(50);
			    msg_id RAW(16);
			    enqueue_time TIMESTAMP(6) WITH TIME ZONE;

			BEGIN
			    SELECT LOWER(PARTNAME) INTO partition_name
			    FROM USER_QUEUE_PARTITION_MAP
			    WHERE QUEUE_TABLE = queue_name AND SHARD = shard_num AND SUBSHARD = (
			        SELECT MAX(SUBSHARD)
			        FROM USER_QUEUE_PARTITION_MAP
			        WHERE QUEUE_TABLE = queue_name AND SHARD = shard_num
			    );

			    EXECUTE IMMEDIATE
			        'SELECT MSGID, ENQUEUE_TIME
			         FROM ' || DBMS_ASSERT.SQL_OBJECT_NAME(queue_name) || ' PARTITION (' || partition_name || ')
			         WHERE MSGID = (SELECT MAX(MSGID) FROM ' || DBMS_ASSERT.SQL_OBJECT_NAME(queue_name) || ' PARTITION (' || partition_name || '))'
			        INTO msg_id, enqueue_time;

			        ? := msg_id;
			        ? := enqueue_time;

			    EXCEPTION
			  	WHEN OTHERS THEN
			   		RAISE;
			END;
			""";
	
	public static ListOffsetsPartitionResponse fetchEarliestOffset(String topic, int partition, Connection jdbcConn)
			throws SQLException {
		ListOffsetsPartitionResponse response = new ListOffsetsPartitionResponse().setPartitionIndex(partition);
		CallableStatement cStmt = null;
		
		try {
			cStmt = jdbcConn.prepareCall(EARLIEST_OFFSET_PLSQL);
			cStmt.setInt(1, partition * 2);
			cStmt.setString(2, topic);

			cStmt.registerOutParameter(3, Types.BINARY);

			cStmt.executeQuery();

			byte[] msgIdBytes = cStmt.getBytes(3);

			StringBuilder msgIdHex = new StringBuilder();
			for (byte b : msgIdBytes) {
				msgIdHex.append(String.format("%02X", b));
			}

			long offset = MessageIdConverter.getOKafkaOffset("ID:" + msgIdHex, true, true).getOffset();
			response.setOffset(offset);

		} catch (SQLException sqle) {
			if (sqle.getErrorCode() == 1403) {
				response.setOffset(0);
			} else
				throw sqle;
		} finally {
			try {
				if (cStmt != null)
					cStmt.close();
			} catch (Exception ex) {
				// do nothing
			}
		}
		return response;
	}

	public static ListOffsetsPartitionResponse fetchLatestOffset(String topic, int partition, Connection jdbcConn)
			throws SQLException {

		ListOffsetsPartitionResponse response = new ListOffsetsPartitionResponse().setPartitionIndex(partition);
		CallableStatement cStmt = null;
		
		try {
			cStmt = jdbcConn.prepareCall(LATEST_OFFSET_PLSQL);
			cStmt.setInt(1, partition * 2);
			cStmt.setString(2, topic);

			cStmt.registerOutParameter(3, Types.BINARY);

			cStmt.executeQuery();

			byte[] msgIdBytes = cStmt.getBytes(3);

			StringBuilder msgIdHex = new StringBuilder();
			for (byte b : msgIdBytes) {
				msgIdHex.append(String.format("%02X", b));
			}
			
			long offset = MessageIdConverter.getOKafkaOffset("ID:" + msgIdHex, true, true).getOffset();
			response.setOffset(offset+1);

		} catch (SQLException sqle) {
			if (sqle.getErrorCode() == 1403) {
				response.setOffset(0);
			} else
				throw sqle;
		} finally {
			try {
				if (cStmt != null)
					cStmt.close();
			} catch (Exception ex) {
				// do nothing
			}
		}
		return response;

	}

	public static ListOffsetsPartitionResponse fetchOffsetByTimestamp(String topic, int partition, long timestamp,
			Connection jdbcConn) throws SQLException {

		ListOffsetsPartitionResponse response = new ListOffsetsPartitionResponse().setPartitionIndex(partition);
		CallableStatement cStmt = null;

		try {
			cStmt = jdbcConn.prepareCall(OFFSET_BY_TIMESTAMP_PLSQL);
			cStmt.setInt(1, partition * 2);
			cStmt.setString(2, topic);
			Instant instant = Instant.ofEpochMilli(timestamp);

			DateTimeFormatter formatter = DateTimeFormatter
					.ofPattern("dd-MMM-yy hh.mm.ss.SSSSSS a", java.util.Locale.ENGLISH).withZone(ZoneOffset.UTC);

			String okafkaTimestampUTC = formatter.format(instant) + " " + "+00:00";
			System.out.println(okafkaTimestampUTC.toUpperCase());
			cStmt.setString(3, okafkaTimestampUTC.toUpperCase());

			cStmt.registerOutParameter(4, Types.BINARY);
			cStmt.registerOutParameter(5, Types.TIMESTAMP_WITH_TIMEZONE);

			cStmt.executeQuery();

			byte[] msgIdBytes = cStmt.getBytes(4);
			Timestamp enqueueTimestamp = cStmt.getTimestamp(5);

			StringBuilder msgIdHex = new StringBuilder();
			for (byte b : msgIdBytes) {
				msgIdHex.append(String.format("%02X", b));
			}

			long offset = MessageIdConverter.getOKafkaOffset("ID:" + msgIdHex, true, true).getOffset();
			long okafkaTimestamp = enqueueTimestamp.toInstant().toEpochMilli();

			response.setOffset(offset).setTimestamp(okafkaTimestamp);

		} catch (SQLException sqle) {
			if (sqle.getErrorCode() == 1403) {
				// do nothing;
			} else
				throw sqle;
		} finally {
			try {
				if (cStmt != null)
					cStmt.close();
			} catch (Exception ex) {
				// do nothing
			}
		}
		return response;
	}

	public static ListOffsetsPartitionResponse fetchMaxTimestampOffset(String topic, int partition, Connection jdbcConn)
			throws SQLException {

		ListOffsetsPartitionResponse response = new ListOffsetsPartitionResponse().setPartitionIndex(partition);
		CallableStatement cStmt = null;
		
		try {
			cStmt = jdbcConn.prepareCall(MAX_TIMESTAMP_OFFSET_PLSQL);
			cStmt.setInt(1, partition * 2);
			cStmt.setString(2, topic);

			cStmt.registerOutParameter(3, Types.BINARY);
			cStmt.registerOutParameter(4, Types.TIMESTAMP_WITH_TIMEZONE);

			cStmt.executeQuery();

			byte[] msgIdBytes = cStmt.getBytes(3);
			Timestamp enqueueTimestamp = cStmt.getTimestamp(4);

			StringBuilder msgIdHex = new StringBuilder();
			for (byte b : msgIdBytes) {
				msgIdHex.append(String.format("%02X", b));
			}

			long offset = MessageIdConverter.getOKafkaOffset("ID:" + msgIdHex, true, true).getOffset();
			long okafkaTimestamp = enqueueTimestamp.toInstant().toEpochMilli();

			response.setOffset(offset).setTimestamp(okafkaTimestamp);

		} catch (SQLException sqle) {
			if (sqle.getErrorCode() == 1403) {
				// do nothing
			} else
				throw sqle;
		} finally {
			try {
				if (cStmt != null)
					cStmt.close();
			} catch (Exception ex) {
				// do nothing
			}
		}
		return response;

	}
}
