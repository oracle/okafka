package org.oracle.okafka.common.utils;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;

import org.oracle.okafka.common.requests.ListOffsetsResponse.ListOffsetsPartitionResponse;

public class FetchOffsets {

	public static ListOffsetsPartitionResponse fetchEarliestOffset(String topic, int partition, Connection jdbcConn)
			throws SQLException {
		ListOffsetsPartitionResponse response = new ListOffsetsPartitionResponse().setPartitionIndex(partition);
		CallableStatement cStmt = null;
		String plsql = """
				DECLARE
				    shard_num NUMBER := ?;
				    queue_name VARCHAR2(128) := ?;
				    partition_name VARCHAR2(50);
				    msg_id RAW(16);
				    total_shards NUMBER;
				BEGIN
					EXECUTE IMMEDIATE 'SELECT COUNT(DISTINCT(SHARD)) FROM ' || DBMS_ASSERT.SQL_OBJECT_NAME(queue_name)
				    INTO total_shards;

				   	IF shard_num>=total_shards THEN
				   	   	RAISE_APPLICATION_ERROR(20001, 'Invalid partition number');
				   	END IF;
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
				    WHEN NO_DATA_FOUND THEN
				    	RAISE_APPLICATION_ERROR(20003, 'No messages in the given partition');
				END;
				   	EXCEPTION
				   	WHEN NO_DATA_FOUND THEN
				   		RAISE_APPLICATION_ERROR(20002, 'Invalid queue name');
				   	WHEN OTHERS THEN
				   		RAISE;

				END;
				""";
		try {
			cStmt = jdbcConn.prepareCall(plsql);
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
			if (sqle.getErrorCode() == 20001) {
				response.setError(new IllegalArgumentException("Partition Number Is Invalid"));
			} else if (sqle.getErrorCode() == 20002) {
				response.setError(new IllegalArgumentException("Queue with the provided name doesn't exist"));
			} else if (sqle.getErrorCode() == 20003) {
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
		String plsql = """
				DECLARE
				    shard_num NUMBER := ?;
				    queue_name VARCHAR2(128) := ?;
				    partition_name VARCHAR2(50);
				    msg_id RAW(16);
				    total_shards NUMBER;
				BEGIN
					EXECUTE IMMEDIATE 'SELECT COUNT(DISTINCT(SHARD)) FROM ' || DBMS_ASSERT.SQL_OBJECT_NAME(queue_name)
				    INTO total_shards;

				   	IF shard_num>=total_shards THEN
				   	   	RAISE_APPLICATION_ERROR(20001, 'Invalid partition number');
				   	END IF;
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
				    WHEN NO_DATA_FOUND THEN
				    	RAISE_APPLICATION_ERROR(20003, 'No messages in the given partition');
				END;
				   	EXCEPTION
				   	WHEN NO_DATA_FOUND THEN
				   		RAISE_APPLICATION_ERROR(20002, 'Invalid queue name');
				   	WHEN OTHERS THEN
				   		RAISE;

				END;
				""";
		try {
			cStmt = jdbcConn.prepareCall(plsql);
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
			if (sqle.getErrorCode() == 20001) {
				response.setError(new IllegalArgumentException("Partition Number Is Invalid"));
			} else if (sqle.getErrorCode() == 20002) {
				response.setError(new IllegalArgumentException("Queue with the provided name doesn't exist"));
			} else if (sqle.getErrorCode() == 20003) {
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
		String plsql = """
				DECLARE
				    shard_num NUMBER := ?;
				    queue_name VARCHAR2(128) := ?;
				    user_timestamp TIMESTAMP(6) WITH TIME ZONE := TO_TIMESTAMP_TZ(?, 'DD-MON-YY HH.MI.SSXFF AM TZR');
				    partition_list SYS.ODCIVARCHAR2LIST := SYS.ODCIVARCHAR2LIST();
				    next_timestamp TIMESTAMP(6) WITH TIME ZONE;
				    msg_id RAW(16);
				    found BOOLEAN := FALSE;
				    total_shards NUMBER;
				BEGIN
				EXECUTE IMMEDIATE 'SELECT COUNT(DISTINCT(SHARD)) FROM ' || DBMS_ASSERT.SQL_OBJECT_NAME(queue_name)
				    INTO total_shards;

				   	IF shard_num>=total_shards THEN
				   	   	RAISE_APPLICATION_ERROR(20001, 'Invalid partition number');
				   	END IF;
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
				END;
				EXCEPTION
				   	WHEN NO_DATA_FOUND THEN
				   		RAISE_APPLICATION_ERROR(20002, 'Invalid queue name');
				   	WHEN OTHERS THEN
				   		RAISE;
				END;
				""";
		try {
			cStmt = jdbcConn.prepareCall(plsql);
			cStmt.setInt(1, partition * 2);
			cStmt.setString(2, topic);
			cStmt.setString(3, TimestampConverter.timestampInUTCTimeZone(timestamp));

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
			if (sqle.getErrorCode() == 20001) {
				response.setError(new IllegalArgumentException("Partition Number Is Invalid"));
			} else if (sqle.getErrorCode() == 20002) {
				response.setError(new IllegalArgumentException("Queue with the provided name doesn't exist"));
			} else if (sqle.getErrorCode() == 20003) {
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
		String plsql = """
				DECLARE
				    shard_num NUMBER := ?;
				    queue_name VARCHAR2(128) := ?;
				    partition_name VARCHAR2(50);
				    msg_id RAW(16);
				    enqueue_time TIMESTAMP(6) WITH TIME ZONE;
				    total_shards NUMBER;
				BEGIN
					EXECUTE IMMEDIATE 'SELECT COUNT(DISTINCT(SHARD)) FROM ' || DBMS_ASSERT.SQL_OBJECT_NAME(queue_name)
				    INTO total_shards;

				   	IF shard_num>=total_shards THEN
				   	   	RAISE_APPLICATION_ERROR(20001, 'Invalid partition number');
				   	END IF;
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
				    WHEN NO_DATA_FOUND THEN
				    	RAISE_APPLICATION_ERROR(20003, 'No messages in the given partition');
				END;
				   	EXCEPTION
				   	WHEN NO_DATA_FOUND THEN
				   		RAISE_APPLICATION_ERROR(20002, 'Invalid queue name');
				   	WHEN OTHERS THEN
				   		RAISE;

				END;
				""";
		try {
			cStmt = jdbcConn.prepareCall(plsql);
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
			if (sqle.getErrorCode() == 20001) {
				response.setError(new IllegalArgumentException("Partition Number Is Invalid"));
			} else if (sqle.getErrorCode() == 20002) {
				response.setError(new IllegalArgumentException("Queue with the provided name doesn't exist"));
			} else if (sqle.getErrorCode() == 20003) {
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
