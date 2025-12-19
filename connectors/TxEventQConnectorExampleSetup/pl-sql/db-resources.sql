-- If you are using the free container
ALTER SESSION SET CONTAINER = FREEPDB1;

--- User creation and permissions for TxEventQ. Add password to use.
CREATE USER TXEVENTQ_ADMIN IDENTIFIED BY <add valid password for specified username>;

-- Basic login permission
GRANT CREATE SESSION TO TXEVENTQ_ADMIN;

-- Required roles for TxEventQ (Advanced Queuing)
GRANT AQ_ADMINISTRATOR_ROLE TO TXEVENTQ_ADMIN;
GRANT CONNECT, RESOURCE TO TXEVENTQ_ADMIN;

-- Enable usage of Oracle Streams AQ (used by TxEventQ)
GRANT EXECUTE ON DBMS_AQ TO TXEVENTQ_ADMIN;
GRANT EXECUTE ON DBMS_AQADM TO TXEVENTQ_ADMIN;
GRANT EXECUTE ON DBMS_AQIN TO TXEVENTQ_ADMIN;
GRANT EXECUTE ON DBMS_AQJMS TO TXEVENTQ_ADMIN;

-- Uncomment the GRANT statement below if working with onprem or local database
--GRANT SELECT ON sys.V_$PARAMETER TO TXEVENTQ_ADMIN;

-- Uncomment the GRANT statement below if working with an autonomous cloud database
--GRANT SELECT ON sys.V$PARAMETER TO TXEVENTQ_ADMIN;

-- Uncomment the GRANT below if you want to have access to performance views (optional, for diagnostics). In this particular example the information will be used by Promethus and Grafana.
--GRANT SELECT_CATALOG_ROLE TO TXEVENTQ_ADMIN;

-- Give quota on tablespace (adjust if needed)
ALTER USER TXEVENTQ_ADMIN QUOTA UNLIMITED ON USERS;

-- or if you're using DATA tablespace
-- ALTER USER TXEVENTQ_ADMIN QUOTA UNLIMITED ON DATA;

------Add the password specified for this user below.
CONNECT TXEVENTQ_ADMIN/<add valid password for specified username>@//localhost:1521/FREEPDB1;

-- This topic will be used for sink connector to consume messages from the Kafka topic
-- and the source connector to produce messages to a Kafka topic.
BEGIN
    DBMS_AQADM.CREATE_DATABASE_KAFKA_TOPIC(
        topicname                => 'ORDERS_ORACLE_KAFKA_TOPIC',
        partition_num            => 3,           -- 3 partitions
        retentiontime            => 7*24*3600,   -- Retain messages for 7 days (default)
        partition_assignment_mode => 1,
        replication_mode         => SYS.DBMS_AQADM.NONE -- No replication
    );
    
    DBMS_AQADM.ADD_SUBSCRIBER(
        queue_name => 'ORDERS_ORACLE_KAFKA_TOPIC',
        subscriber => SYS.AQ$_AGENT('OrdersKafka_SUBSCRIBER_LOCAL', NULL, NULL)
   );
    
END;
/

BEGIN
    -- Create a JMS Type (the default queue_payload_type) Transactional Event Queue named 'TXEVENTQ_FOR_CONNECTORS'
    DBMS_AQADM.CREATE_TRANSACTIONAL_EVENT_QUEUE(
        queue_name          => 'TXEVENTQ_FOR_CONNECTORS',
        multiple_consumers  => TRUE, -- Allows multiple subscribers
        comment             => 'Transactional Event Queue for JMS messages for use with the Sink and Source Connector',
        queue_payload_type  => DBMS_AQADM.JMS_TYPE
    );
	
    DBMS_AQADM.SET_QUEUE_PARAMETER('TXEVENTQ_FOR_CONNECTORS', 'SHARD_NUM', 6);
   	DBMS_AQADM.SET_QUEUE_PARAMETER('TXEVENTQ_FOR_CONNECTORS', 'STICKY_DEQUEUE', 1);
   	DBMS_AQADM.SET_QUEUE_PARAMETER('TXEVENTQ_FOR_CONNECTORS', 'KEY_BASED_ENQUEUE', 1);
    
    -- Start the queue for enqueuing and dequeuing
    DBMS_AQADM.START_QUEUE(
        queue_name => 'TXEVENTQ_FOR_CONNECTORS'
    );
    
    DBMS_AQADM.ADD_SUBSCRIBER('TXEVENTQ_FOR_CONNECTORS', SYS.AQ$_AGENT('TXEVENTQFORCONNECTOR_SUBSCRIBER_LOCAL', NULL, NULL));
END;
/

BEGIN
    -- Create a JMS Type (the default queue_payload_type) Transactional Event Queue named 'HEADER_TRANSFORM_TEQ'
    DBMS_AQADM.CREATE_TRANSACTIONAL_EVENT_QUEUE(
        queue_name          => 'HEADER_TRANSFORM_TEQ',
        multiple_consumers  => TRUE, -- Allows multiple subscribers
        comment             => 'Transactional Event Queue for JMS messages for use with the Sink and Source Connector to show headers being stored and how Kafka transform properties work.',
        queue_payload_type  => DBMS_AQADM.JMS_TYPE
    );
	
    DBMS_AQADM.SET_QUEUE_PARAMETER('HEADER_TRANSFORM_TEQ', 'SHARD_NUM', 5);
   	DBMS_AQADM.SET_QUEUE_PARAMETER('HEADER_TRANSFORM_TEQ', 'STICKY_DEQUEUE', 1);
   	DBMS_AQADM.SET_QUEUE_PARAMETER('HEADER_TRANSFORM_TEQ', 'KEY_BASED_ENQUEUE', 1);
    
    -- Start the queue for enqueuing and dequeuing
    DBMS_AQADM.START_QUEUE(
        queue_name => 'HEADER_TRANSFORM_TEQ'
    );
    
    DBMS_AQADM.ADD_SUBSCRIBER('HEADER_TRANSFORM_TEQ', SYS.AQ$_AGENT('HeaderTransform0_SUBSCRIBER_LOCAL', NULL, NULL));
    DBMS_AQADM.ADD_SUBSCRIBER('HEADER_TRANSFORM_TEQ', SYS.AQ$_AGENT('HeaderTransform1_SUBSCRIBER_LOCAL', NULL, NULL));
END;
/

