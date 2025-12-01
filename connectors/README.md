# Installation and Setup Guide for Oracle TxEventQ JMS Connectors

This guide covers the common installation and setup steps required for both the TxEventQ Sink and Source connectors. For connector-specific configuration, see:
* [TxEventQ Sink Connector](TxEventQ-Sink-Connector.md)
* [TxEventQ Source Connector](TxEventQ-Source-Connector.md)
* [Use Single Message Transforms with Connectors](connector-single-message-transform.md)

For a complete connector demo setup using docker that also uses Prometheus and Grafana see:
* [Complete Connector Demo Setup Using Docker](connector-docker-demo-setup.md)

## Prerequisites

Before installing the connectors, ensure you have:

* **Java 17 or higher** - Required to run Kafka Connect and the connectors
* **Apache Kafka 3.1.0 or higher** - The Kafka cluster must be running and accessible
* **Oracle Database 23 or higher** - Required for all TxEventQ functionality
* **Database credentials** - Must be configured securely for database authentication (see [Database Authentication](#database-authentication))
* **Transactional Event Queue** - Must be created in Oracle Database before using the connectors (see [Create Transactional Event Queue](#create-transactional-event-queue))

## Install the Connector
You can install the connector in two supported ways: build it from source, or download the pre-built JAR from a Maven repository.

### Option A: Build the connector from source

Clone the project from the repository and build it using Maven:

```bash
mvn clean package
```

This will create the connector JAR file in the `target` directory.

### Option B: Download the connector
Download `txeventq-connector-<version>.jar` from the [Maven repository](https://mvnrepository.com/artifact/com.oracle.database.messaging/txeventq-connector)

### Install the connector manually

1. Obtain the connector JAR file by performing [Option A](#option-a-build-the-connector-from-source) or [Option B](#option-b-download-the-connector).

2. **Download dependencies**: Download the following required JAR files from Maven repositories:
   * `ojdbc11-<version>.jar` - Oracle JDBC driver
   * `oraclepki-<version>.jar` - Oracle PKI library
   * `osdt_core-<version>.jar` - Oracle Security Developer Tools core
   * `osdt_cert-<version>.jar` - Oracle Security Developer Tools certificate
   * `aqapi-<version>.jar` - Oracle Advanced Queuing API
   * `javax.jms-api-<version>.jar` - JMS API (version 2.0.1 or higher)
   * `jta-<version>.jar` - Java Transaction API (version 1.1 or higher)

3. **Create plugin directory**: Create a directory with a descriptive name (e.g., `txeventq-connector-23.26.0.25.12`) and place all JAR files in this directory.

4. **Configure plugin path**: In your Kafka Connect worker configuration file (`connect-distributed.properties` or `connect-standalone.properties`), set the `plugin.path` property to point to the parent directory containing the connector folder:

```properties
plugin.path=/usr/local/share/kafka/plugins/
```

**Best Practice**: Use absolute paths for the plugin path to avoid issues with relative path resolution.

Below is an example file structure for a directory-based plugin:

```text
/usr/local/share/kafka/plugins/
└── txeventq-connector-23.26.0.25.12/
    ├── txeventq-connector-23.26.0.25.12.jar
    ├── aqapi-23.8.0.0.jar
    ├── javax.jms-api-2.0.1.jar
    ├── jta-1.1.jar
    ├── ojdbc11-23.26.0.0.0.jar
    ├── oraclepki-23.26.0.0.0.jar
    ├── osdt_cert-21.20.0.0.jar
    └── osdt_core-21.20.0.0.jar
```

**Note:** The TxEventQ Connector JAR file can be downloaded from the [Maven repository](https://mvnrepository.com/artifact/com.oracle.database.messaging/txeventq-connector) if you don't want to build the JAR. You will still need to get the additional JAR files mentioned above and place them in the same directory as the TxEventQ Connector JAR. The JAR files listed above can also be downloaded from Maven.

## Oracle Database Setup

### Database User and Privileges

Before using the connectors, create a dedicated database user with the necessary privileges. This user will be used by the connectors to connect to the database and access the Transactional Event Queue.

**Best Practice**: Create a dedicated user for the connectors rather than using a shared or administrative account. This follows the principle of least privilege and improves security.

Run the following SQL commands as a database administrator (e.g., `SYS` or `SYSTEM`):

```sql
-- Create the user
CREATE USER txeventq_user IDENTIFIED BY <secure_password>;

-- Grant basic privileges
GRANT CONNECT, RESOURCE TO txeventq_user;

-- Grant privileges for Advanced Queuing operations
GRANT EXECUTE ON dbms_aqadm TO txeventq_user;
GRANT EXECUTE ON dbms_aqin TO txeventq_user;
GRANT EXECUTE ON dbms_aqjms TO txeventq_user;

-- Grant privileges for monitoring and catalog access
GRANT SELECT_CATALOG_ROLE TO txeventq_user;
GRANT SELECT ON sys.V_$PARAMETER TO txeventq_user;
```

**Note**: Replace `txeventq_user` and `<secure_password>` with your desired username and a strong password. Store credentials securely using your organization's credential management solution (see [Database Authentication](#database-authentication)).

### Create Transactional Event Queue

The Transactional Event Queue must be created before using the connectors. The queue should be created with appropriate parameters based on your use case.

**Important considerations:**

* The Sink Connector supports a JMS type Transactional Event Queue payload, which is the default payload. The TxEventQ Sink Connector will store the payload as a JMS_BYTES message.
* If you plan to use both the TxEventQ Sink and Source connectors on the same transactional event queue, all relevant parameters such as `KEY_BASED_ENQUEUE`, `SHARD_NUM`, and `STICKY_DEQUEUE` should be set before using the connectors.
* The `KEY_BASED_ENQUEUE` and `STICKY_DEQUEUE` parameters cannot be changed once they have been set.

#### KEY_BASED_ENQUEUE and SHARD_NUM parameters

The `KEY_BASED_ENQUEUE` parameter determines which shard a message is enqueued to. The table below describes the different `KEY_BASED_ENQUEUE` values and what is required for `SHARD_NUM`:

| KEY_BASED_ENQUEUE | SHARD_NUM | Description |
|-------------------|-----------|-------------|
| 0 (default) | >= 1 | A key is not used during the enqueue. A session is bound to a shard at the time of first enqueue to the queue. All messages enqueued by the session will go to the same shard to which the session is bound. |
| 1 | >= 1 | The database correlation value will be used as the key. The correlation value will be used to determine which shard the messages will be enqueued to. The key is limited to 128 bytes. The TxEventQ sink connector will truncate the key if it is greater than the maximum length. |
| 2 | >= the number of Kafka partitions | Messages from a Kafka topic partition will be mapped to its respective shard by specifying the shard number in the `AQINTERNAL_PARTITION` property. In this case, the `SHARD_NUM` value for the queue will need to be greater than or equal to the number of Kafka partitions for the topic that the TxEventQ sink connector will be consuming from. |

#### STICKY_DEQUEUE parameter

The `STICKY_DEQUEUE` parameter controls how messages are distributed across shards when dequeuing. When set to 1, it ensures that a dequeue session sticks to a specific shard in the queue. A session is bound to a shard on the first dequeue operation, and all subsequent messages dequeued by that session come from the same shard.

**When to use STICKY_DEQUEUE=1**:
* You need to maintain message ordering within shards
* You want to map shards to Kafka partitions for ordering guarantees (Source Connector)
* You're using the Source Connector with `txeventq.map.shard.to.kafka_partition=true`

**Important**: 
* If `STICKY_DEQUEUE=1` and you're running Oracle Database version less than 23.4, the `tasks.max` configuration for the Source Connector must equal the `SHARD_NUM` of the queue
* The `STICKY_DEQUEUE` parameter cannot be changed after queue creation, so plan your ordering requirements carefully

#### Example PL/SQL script to create Transactional Event Queue

```sql
EXEC sys.dbms_aqadm.create_transactional_event_queue(queue_name=>'TxEventQ', multiple_consumers => TRUE);
EXEC sys.dbms_aqadm.set_queue_parameter('TxEventQ', 'SHARD_NUM', 1);
EXEC sys.dbms_aqadm.set_queue_parameter('TxEventQ', 'STICKY_DEQUEUE', 1);
EXEC sys.dbms_aqadm.set_queue_parameter('TxEventQ', 'KEY_BASED_ENQUEUE', 1);
EXEC sys.dbms_aqadm.start_queue('TxEventQ');

-- Also need to add subscriber if creating a multiple_consumers queue
EXEC sys.dbms_aqadm.add_subscriber('TxEventQ', SYS.AQ$_AGENT('SUB1', NULL, 0));
```

**Note:** Remove the `set_queue_parameter` calls for either the `STICKY_DEQUEUE` or `KEY_BASED_ENQUEUE` properties if you want to create the queue with the default value of 0 for either of these.

**Note:** If running on an Oracle database version less than 23.4 with `STICKY_DEQUEUE` set to 1, the Source Connector configuration property `tasks.max` value must be equal to the `SHARD_NUM` specified. If the `tasks.max` is not equal to the `SHARD_NUM`, dequeue from all event streams will not be performed.

### Create OKafka Topic
If using the TxEventQ Sink or Source Connector on an Okafka topic it is important to understand what parameters the Okafka topic will have and what needs to be set.
|KEY_BASED_ENQUEUE| partition_num                   |Description|
|-----------------|---------------------------------|-----------|
|2 (always)       | >= the number of Kafka partitons|In this case messages from a Kafka topic partition will be mapped to its respective shard by specifying the shard number in the AQINTERNAL_PARTITION property. In this case the partition_num parameter value for the Okafka topic will need to be greater than or equal to the number of Kafka partitions for the topic that the TxEventQ sink connector will be consuming from.  

 
If attempting to use the TxEventQ Source Connector on an Okafka topic the `partition_assignment_mode` will need to be set to 1 as shown below. Setting `partition_assignment_mode` to 1 means setting `STICKY_DEQUEUE` to 1.

|partition_assignment_mode| Description                     |
|-------------------------|---------------------------------|
|0                        | **Currently setting sticky dequeue to 0 for an Okafka topic is not allowed, but will be available soon.** When this property value is allowed which indicates no sticky dequeue is set, which means messages dequeued by a session can spread across multiple shards of the queue.
|1                        | Sticky dequeue is set and dequeue session sticks to a shard in the queue. A session is bound to a shard on first dequeue from the queue. All messages dequeued by the session come from the same shard to which it is bound.

```roomsql
exec sys.dbms_aqadm.create_database_kafka_topic( topicname=> 'TxEventQ', partition_num=>1, retentiontime => 7*24*3600, partition_assignment_mode => 1);

-- Also need to add subscribers because the Okafka topic is a multiple_consumers queue
exec sys.dbms_aqadm.add_subscriber('TxEventQ', SYS.AQ$_AGENT('SUB1', NULL, 0));
```

### Database Authentication

The connectors require database credentials to connect to Oracle Database. Use your organization's secure credential management solution (e.g., secret managers, vaults, or Oracle Wallet) to store and manage these credentials. Never hardcode passwords in configuration files.

The connectors support Oracle Wallet for credential management. If using Oracle Wallet:

1. **Create or modify `tnsnames.ora` file**: The entry in the file should have the following form:

```text
alias=(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=host)(PORT=port))(CONNECT_DATA=(SERVICE_NAME=service)))
```

For Oracle RAC databases, see [Connecting to an Oracle RAC Database](#connecting-to-an-oracle-rac-database) for additional configuration.

2. **Create the Oracle Wallet** using the `mkstore` command:

```bash
mkstore -wrl <wallet_location> -create
```

The `mkstore` command will prompt for a password that will be used for subsequent commands. Passwords must have a minimum length of eight characters and contain alphabetic characters combined with numbers or special characters.

3. **Add the credential** for the data source name to the Oracle Wallet:

```bash
mkstore -wrl <wallet_location> -createCredential <alias name from tnsnames.ora> <username> <password>
```

The wallet directory that will need to be specified in the connection properties should contain the following files:
* `cwallet.sso`
* `ewallet.p12`

For additional details on creating an Oracle Wallet, refer to:
* [JDBC Thin Connections with a Wallet](https://docs.oracle.com/en/cloud/paas/autonomous-database/adbsa/connect-jdbc-thin-wallet.html#GUID-BE543CFD-6FB4-4C5B-A2EA-9638EC30900D)
* [orapki Utility](https://docs.oracle.com/cd/B19306_01/network.102/b14268/asoappf.htm#CDEFHBGA)

### Connecting to an Oracle RAC Database

If connecting to an Oracle RAC database, the `tnsnames.ora` file will need to specify an **ADDRESS_LIST** with the **ADDRESS** for each of the nodes if the nodes are on different host machines. If the nodes are all on the same host machine, a single **ADDRESS** entry in the `tnsnames.ora` file is sufficient.

Here is an example of what the entry in the `tnsnames.ora` file would be if the nodes are on different machines:

```text
RAC_DB = (DESCRIPTION = 
	(CONNECT_TIMEOUT= 90)(RETRY_COUNT=50)(RETRY_DELAY=3)(TRANSPORT_CONNECT_TIMEOUT=3)
    (ADDRESS_LIST = 
        (ADDRESS = (PROTOCOL=tcp)(HOST=racnode1.example.com)(PORT=1521)) 
        (ADDRESS = (PROTOCOL=tcp)(HOST=racnode2.example.com)(PORT=1521)) 
        (ADDRESS = (PROTOCOL=tcp)(HOST=racnode3.example.com)(PORT=1521)) 
    ) 
    (CONNECT_DATA = (SERVICE_NAME=mydatabase)) 
)
```

If connecting to an Oracle RAC database using [SCANs](https://docs.oracle.com/en/database/oracle/oracle-database/23/rilin/about-connecting-to-an-oracle-rac-database-using-scans.html), the `tnsnames.ora` file should reference the SCAN as the example below shows:

```text
RAC_DB_WITH_SCAN = (DESCRIPTION = 
	(CONNECT_TIMEOUT= 90)(RETRY_COUNT=50)(RETRY_DELAY=3)(TRANSPORT_CONNECT_TIMEOUT=3)
    (ADDRESS = (PROTOCOL = tcp)(HOST = rac-scan.example.com)(PORT = 1521)) 
    (CONNECT_DATA = (SERVICE_NAME = myrac_service)) 
)
```

### Data Restrictions

**Key Size Limitation**: In Transactional Event Queues (TEQ), the correlation value (used to store the Kafka record key) is restricted to 128 bytes. If a Kafka record key exceeds 128 bytes, the connector will automatically truncate it to fit this limit.

**Best Practice**: 
* Design your Kafka keys to be 128 bytes or less to avoid truncation
* If you must use longer keys, consider using a hash or shortened identifier
* Be aware that truncated keys may cause collisions if multiple records have keys that differ only after the 128-byte mark

### Oracle Database Automatic Memory Management

It is recommended that the database be configured to allow automatic memory management. Refer to [About Automatic Memory Management](https://docs.oracle.com/en/database/oracle/oracle-database/23/admin/managing-memory.html#GUID-0F348EAB-9970-4207-8EF3-0F58B64E959A) for information on how to allow the Oracle Database instance to automatically manage instance memory.

## Best Practices

### Transactional Event Queue Configuration

* **Plan queue parameters before creation**: The `KEY_BASED_ENQUEUE` and `STICKY_DEQUEUE` parameters cannot be changed after queue creation. Carefully plan your sharding strategy and ordering requirements before creating the queue.

* **Choose KEY_BASED_ENQUEUE strategy wisely**:
  * Use `KEY_BASED_ENQUEUE=0` (default) for simple scenarios where session-based sharding is sufficient
  * Use `KEY_BASED_ENQUEUE=1` when you need to route messages to specific shards based on correlation ID (Kafka key). Remember that keys are limited to 128 bytes and will be truncated if longer
  * Use `KEY_BASED_ENQUEUE=2` when you need to map Kafka topic partitions to TxEventQ shards. Ensure `SHARD_NUM` is greater than or equal to the number of Kafka topic partitions

* **Set SHARD_NUM appropriately**: The number of shards determines parallelism. More shards allow more parallel processing but may increase database overhead. Consider your throughput requirements and database capacity.

* **Use multiple consumers for flexibility**: Create the queue with `multiple_consumers => TRUE` if you plan to use multiple source connectors or other consumers. This allows multiple subscribers to consume from the same queue independently.

* **Start queue after configuration**: Always ensure the queue is started using `dbms_aqadm.start_queue()` after setting all parameters. Verify queue status before using the connectors.

* **Set STICKY_DEQUEUE for ordering**: If you need to maintain message ordering within shards, set `STICKY_DEQUEUE=1` when creating the queue. This ensures that a dequeue session sticks to a specific shard, maintaining order within that shard. Remember that this parameter cannot be changed after queue creation.

### Oracle Database Considerations

* **Secure credential management**: Use your organization's secure credential management solution (e.g., secret managers, vaults, or Oracle Wallet) to store database credentials. Never hardcode passwords in configuration files. This simplifies credential rotation and follows security best practices.

* **Limit database user privileges**: Grant only the minimum required privileges to the database user used by the connectors. The required privileges are: `CONNECT`, `RESOURCE`, `EXECUTE` on `dbms_aqadm`, `dbms_aqin`, and `dbms_aqjms`, `SELECT_CATALOG_ROLE`, and `SELECT` on `sys.V_$PARAMETER`.

* **Secure credential storage**: If using file-based credential storage (e.g., Oracle Wallet), ensure files have appropriate file permissions (readable only by the Kafka Connect process user) and are stored in a secure location.

* **Monitor database connections**: The connectors manage database connections internally. For high-throughput scenarios, monitor database connection pool usage and ensure your database can handle the connection load. Consider using connection pooling at the database level if needed.

* **Verify queue schema**: Always specify the correct queue schema in the connector configuration. This ensures the connector connects to the right queue in the correct schema.

* **Consider database AMM**: Oracle recommends configuring Automatic Memory Management (AMM) for optimal TxEventQ performance. Ensure your database has sufficient memory allocated for queue operations.

## Troubleshooting Common Setup Issues

**Connection to database fails**
* Verify credential configuration (e.g., `wallet.path` if using Oracle Wallet points to a directory containing `cwallet.sso` and `ewallet.p12`)
* Verify `tnsnames.path` contains a valid `tnsnames.ora` file
* Verify `db_tns_alias` matches an entry in `tnsnames.ora`
* Verify credentials are correctly configured in your credential management solution
* Test database connectivity: `sqlplus user@alias`

**Queue not accessible**
* Verify the queue is started: `SELECT queue_name, enabled FROM user_queues WHERE queue_name = 'YOUR_QUEUE';`
* Verify the user has `EXECUTE` privileges on `dbms_aqjms`
* Check queue name and schema values in connector configuration

**Subscriber not found (Source Connector)**
* Verify subscriber exists: `SELECT * FROM user_queue_subscribers WHERE queue_name = 'YOUR_QUEUE';`
* Ensure subscriber was added using `dbms_aqadm.add_subscriber()` before starting the connector

## Additional Resources

* [Oracle Transactional Event Queues Documentation](https://docs.oracle.com/en/database/oracle/oracle-database/23/adque/Kafka_cient_interface_TEQ.html#GUID-C329D40D-21D6-454E-8B6A-49D96F0C8795)
* [Oracle Database Advanced Queuing](https://docs.oracle.com/en/database/oracle/oracle-database/23/adque/)
* [Apache Kafka Connect Documentation](https://kafka.apache.org/documentation/#connect)

