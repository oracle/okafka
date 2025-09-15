# Kafka Connectors for TxEventQ

Repository for the Kafka Sink Connector and Kafka Source Connector for Oracle Transactional Event Queues application.
The repository contains an application for a Sink Connector that reads from Kafka and stores into Oracle's TxEventQ.
The repository also contains an application for a Source Connector that reads from an Oracle TxEventQ and stores into a Kafka topic.

## Getting started

To use the application Kafka with a minimum version number of 3.1.0 will need to be downloaded and installed on a server. Refer to [Kafka Apache](https://kafka.apache.org/) for
information on how to start Kafka. Refer to this [Confluent](https://docs.confluent.io/platform/current/kafka/authentication_ssl.html#crep-full) page
for information on how to setup SSL connection for Kafka.

The Kafka Sink and Source Connector requires a minimum Oracle Database version of 21c in order to create a Transactional Event Queue. 

Clone the project from the repository. Open a bash window and change the directory to the location where the cloned project has been saved.
Run the following command from the bash window to compile the source.

```bash
mvn clean package
```

You will need to grab the following jar files from the \target\libs directory after performing the build command above and place them into Kafka's libs directory.

* `ojdbc11-<version>.jar`
* `oraclepki-<version>.jar`
* `osdt_core-<version>.jar`
* `osdt_cert-<version>.jar`
* `aqapi-<version>.jar`
* `javax.jms-api-<version>.jar`
* `jta-<version>.jar`

**Note:** The TxEventQ Connector jar file can be downloaded from this [maven repository](https://mvnrepository.com/artifact/com.oracle.database.messaging/txeventq-connector) if you don't want
to build the jar. You will still need to get the additional jar files mentioned above and place in the required location.

### Oracle Database Setup
To run the Kafka Sink and Source Connector against Oracle Database, a database user should be created and should be granted the below privileges.

```roomsql
create user <username> identified by <password>
grant connect, resource to <username>
grant execute on dbms_aqadm to <username>
grant execute on dbms_aqin to <username>
grant execute on dbms_aqjms to <username>
grant select_catalog_role to <username>
grant select on sys.V_$PARAMETER to <username>;
```

Once user is created and above privileges are granted, connect to Oracle Database as this user and create a Transactional Event Queue using below PL/SQL script.
In the below script `SHARD_NUM` parameter for TxEventQ is set to 1, but this value should be modified to be less than or equal to the number of Kafka partitions
assigned to the Kafka topic that the Sink Connector will be consuming from. The Sink Connector perform `KEY_BASED_ENQUEUE`, which means that messages from a Kafka
partition will be moved into the corresponding Transactional Event Queue shard. **The Sink Connector supports a JMS type Transactional Event Queue and will store the 
payload as a JMS_BYTES message.**

```roomsql
exec sys.dbms_aqadm.create_transactional_event_queue(queue_name=>"TxEventQ", multiple_consumers => TRUE); 
exec sys.dbms_aqadm.set_queue_parameter('TxEventQ', 'SHARD_NUM', 1);
exec sys.dbms_aqadm.set_queue_parameter('TxEventQ', 'KEY_BASED_ENQUEUE', 1);
exec sys.dbms_aqadm.start_queue('TxEventQ');
exec sys.dbms_aqadm.add_subscriber('TxEventQ', SYS.AQ$_AGENT('SUB1', NULL, 0));
```

**If using the Source Connector and ordering of the events are important then the Transactional Event Queue that the Kafka Source connector will be pulling from needs to have been created with the `STICKY_DEQUEUE` parameter set to 1**
**and the source connector configuration file needs to have the property `txeventq.map.shard.to.kafka_partition` set to true.**
**The `SHARD_NUM` assigned to the queue should be less than or equal to the number of Kafka partitions assigned to the Kafka topic.**
**The messages from the Transactional Event Queue will be placed into the specified Kafka topic partition based on the TxEventQ shard the message was dequeued from.**

**Note: If running on a database version less than 23.4 with `STICKY_DEQUEUE` set to 1 the source connector configuration file property `tasks.max` value must be equal to the `SHARD_NUM` specified. If the `tasks.max` is not equal to the `SHARD_NUM` dequeue from all event streams will not be performed.**

Here is the source connector configuration property that needs to be set in the configuration properties file that is discussed in more details below.

```roomsql
txeventq.map.shard.to.kafka_partition=true
```
Here is the PL/SQL script to create the Transactional Event Queue that will be used by the Source Connector if ordering of the events is required.

```roomsql
exec sys.dbms_aqadm.create_transactional_event_queue(queue_name=>"TxEventQ", multiple_consumers => TRUE); 
exec sys.dbms_aqadm.set_queue_parameter('TxEventQ', 'SHARD_NUM', 1);
exec sys.dbms_aqadm.set_queue_parameter('TxEventQ', 'STICKY_DEQUEUE', 1);
exec sys.dbms_aqadm.set_queue_parameter('TxEventQ', 'KEY_BASED_ENQUEUE', 1);
exec sys.dbms_aqadm.start_queue('TxEventQ');
exec sys.dbms_aqadm.add_subscriber('TxEventQ', SYS.AQ$_AGENT('SUB1', NULL, 0));
```

**Note: If attempting to use this Source Connector on a Oracle Kafka topic the `partition_assignment_mode` will need to be set to 1 as shown below. Setting `partition_assignment_mode` to 1 means setting `STICKY_DEQUEUE` to 1.**

```roomsql
exec sys.dbms_aqadm.create_database_kafka_topic( topicname=> 'TxEventQ', partition_num=>1, retentiontime => 7*24*3600, partition_assignment_mode => 1);
```
### Oracle Database Automatic Memory Management 
It is recommended that the database be configured to allow automatic memory management. Refer to [About Automatic Memory Management](https://docs.oracle.com/en/database/oracle/oracle-database/23/admin/managing-memory.html#GUID-0F348EAB-9970-4207-8EF3-0F58B64E959A)
for information on how to allow the Oracle Database instance to automatically manage instance memory.

### Oracle Database Data Restrictions
In the Transactional Event Queues (TEQ) the correlation id value is restricted to 128 bytes. As a result of this restriction the key used
for a Kafka record is also limited to 128 bytes.

### Connecting to an Oracle RAC Database
If connecting to an Oracle RAC database the tnsnames.ora file will need to specify an **ADDRESS_LIST** with the **ADDRESS** for each of the nodes if the nodes are on different host machines. If the nodes are all on the same host machine a single **ADDRESS**
entry in the tnsnames.ora file is sufficient.

Here is an example of what the entry in the tnsnames.ora file would be if the nodes are on different machines:

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

If connecting to an Oracle RAC database using [SCANs](https://docs.oracle.com/en/database/oracle/oracle-database/23/rilin/about-connecting-to-an-oracle-rac-database-using-scans.html) the tnsnames.ora file should reference
the SCAN as the example below will show:

```text
RAC_DB_WITH_SCAN = (DESCRIPTION = 
	(CONNECT_TIMEOUT= 90)(RETRY_COUNT=50)(RETRY_DELAY=3)(TRANSPORT_CONNECT_TIMEOUT=3)
    (ADDRESS = (PROTOCOL = tcp)(HOST = rac-scan.example.com)(PORT = 1521)) 

    (CONNECT_DATA = (SERVICE_NAME = myrac_service)) 
)
```

### Steps to Create an Oracle Wallet

Create or modify a tnsnames.ora file. The entry in the file should have the following form.

```text
alias=(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=host)(PORT=port))(CONNECT_DATA=(SERVICE_NAME=service)))
```

An Oracle Wallet will also need to be created in order for the Connector to connect to the database.
Refer to the following site for additional details on how to create an Oracle Wallet [JDBC Thin Connections with a Wallet](https://docs.oracle.com/en/cloud/paas/autonomous-database/adbsa/connect-jdbc-thin-wallet.html#GUID-BE543CFD-6FB4-4C5B-A2EA-9638EC30900D)
and [orapki Utility](https://docs.oracle.com/cd/B19306_01/network.102/b14268/asoappf.htm#CDEFHBGA).

Oracle recommends creating and managing the Wallet in a database environment since this environment provides all the necessary commands and libraries,
including the $ORACLE_HOME/oracle_common/bin/mkstore command.

Enter the following command to create a wallet:

```bash
mkstore -wrl <wallet_location> -create
```

The mkstore command above will prompt for a password that will be used for subsequent commands. Passwords must have a minimum length of eight characters and contain alphabetic characters combined with numbers or special characters.
If the create is successful when you go to the wallet location specified above a couple of cwallet and ewallet files should have been created in that directory.

Enter the following command to add the credential for the data source name added previously to tnsnames.ora to the Oracle Wallet:

```bash
mkstore -wrl <wallet_location> -createCredential <alias name from tnsnames.ora> <username> <password>
```

If a password is requested enter in the password from the step above.

The wallet directory that will need to be specified in the connection properties file below should contain the following files.

- cwallet.sso
- ewallet.p12

# Sink Connector
## Setup the Sink Connector Configuration Properties

Copy the example properties files for the sink connector below into a text editor and update all the required fields as noted below and save the properties
file as any file name, for example `connect-txeventq-sink.properties` and place the properties file in the Kafka config directory.

Here is the full `connect-txeventq-sink.properties` file below.

```text
name=TxEventQ-sink
connector.class=oracle.jdbc.txeventq.kafka.connect.sink.TxEventQSinkConnector

# Maximum number of tasks to use for this connector.
tasks.max=1

# The Kafka topic to read the data from.
# Note: This property will need to be updated before the Sink Connector can connect.
topics=<Kafka topic>

# Indicate the directory location of where the Oracle wallet is place i.e. C:/tmp/wallet.
# The cwallet.sso and ewallet.p12 files should be placed into this directory.
# Oracle Wallet provides a simple and easy method to manage database credentials across multiple domains.
# We will be using the Oracle TNS (Transport Network Substrate) administrative file to hide the details
# of the database connection string (host name, port number, and service name) from the datasource definition
# and instead us an alias.
# Note: This property will need to be updated before the Sink Connector can connect.
wallet.path=<wallet directory>

# Indicate the directory location of the where the tnsnames.ora location is located i.e C:/tmp/tnsnames.
# The entry in the tnsnames.ora should have the following format: 
# <aliasname> = (DESCRIPTION =(ADDRESS_LIST =(ADDRESS = (PROTOCOL = TCP)(Host = <hostname>)(Port = <port>)))
#(CONNECT_DATA =(SERVICE_NAME = <service_name>)))
# Note: This property will need to be updated before the Sink Connector can connect.
tnsnames.path=<tnsnames.ora directory>

# The TNS alias name for the database to connect to stored in the tnsnames.ora.
# An Oracle Wallet must be created and will be used to connect to the database.
# Note: This property will need to be updated before the Sink Connector can connect.
db_tns_alias=<tns alias>

# The TxEventQ to put the Kafka data into.
# Note: This property will need to be updated before the Sink Connector can connect.
txeventq.queue.name=<txEventQ queue name>

# The name of the schema for the txEventQ queue specified in the txeventq.queue.name property.
# Note: This property will need to be updated to ensure exactly-once delivery.
txeventq.queue.schema=<schema for the txEventQ queue>

# List of Kafka brokers used for bootstrapping
# format: host1:port1,host2:port2 ...
# Note: This property will need to be updated before the Sink Connector can connect.
bootstrap.servers=<broker i.e localhost:9092>

# Converter class used to convert between Kafka Connect format and the serialized form that is written to Kafka.
# This controls the format of the keys in messages written to or read from Kafka, and since this is independent
# of connectors it allows any connector to work with any serialization format.
key.converter=org.apache.kafka.connect.storage.StringConverter

# Converter class used to convert between Kafka Connect format and the serialized form that is written to Kafka.
# This controls the format of the values in messages written to or read from Kafka, and since this is independent
# of connectors it allows any connector to work with any serialization format.
value.converter=org.apache.kafka.connect.storage.StringConverter

```

# Source Connector
## Setup the Source Connector Configuration Properties

Copy the example properties files for the source connector below into a text editor and update all the required fields as noted below and save the properties
file as any file name, for example `connect-txeventq-source.properties` and place the properties file in the Kafka config directory.

Here is the full `connect-txeventq-source.properties` file below.

```text
name=TxEventQ-source
connector.class=oracle.jdbc.txeventq.kafka.connect.source.TxEventQSourceConnector

# If the transactional event queue has STICKY_DEQUEUE set and running on a database version less than 23.4
# the tasks.max number specified must be equal to the number of event streams (SHARD_NUM) for the queue.
# If the `tasks.max` is not equal to the event streams (SHARD_NUM) dequeue from all event streams will not be performed when
# using a database with a version less than 23.4.
tasks.max=1

# The maximum number of records to read from the Oracle Transactional Event Queue before writing to Kafka. The minimum is 1 and the default is 250.
txeventq.batch.size=1

# The name of the Kafka topic where the connector writes all records that were read from the JMS broker.
# Note: This property will need to be updated before the Source Connector can connect.
kafka.topic=<Kafka topic>

# This property will specify whether to use the built in schema for JMS Messages. The JMS messages types that
# are supported are BytesMessage, TextMessage, and MapMessage.
# The default value for this property is false.
use.schema.for.jms.msgs=false

# Converter class used to convert between Kafka Connect format and the serialized form that is written to Kafka.
# This controls the format of the keys in messages written to or read from Kafka, and since this is independent
# of connectors it allows any connector to work with any serialization format. Some common
# Kafka Connect Converters are: org.apache.kafka.connect.json.JsonConverter, org.apache.kafka.connect.storage.StringConverter,
# and org.apache.kafka.connect.converters.ByteArrayConverter
# Note: If the connector is processing JMS type messages and the `use.schema.for.jms.msgs` configuration property described above
# is set to true these messages will be set as structured data in JSON format. As a result of this the 
# org.apache.kafka.connect.json.JsonConverter should be used.
key.converter=org.apache.kafka.connect.storage.StringConverter

# This configuration property determines whether the schema of the key is included with the data when it is serialized.
# Setting to false (the default) excludes the schema if a schema is available, resulting in a smaller payload. However,
# setting the property to true will include the schema with the data when it is serialized.
# Set property to false:
#   -- If you are using a JSON converter and you don't need to include the schema information in your messages.
#   -- To reduce payload overhead, especially if your downstream applications don't need schema information.
#
# Set property to true:
#   -- If you are using a schema-aware converter.
#   -- If you need to ensure that each record has the correct structure and schema evolution is enable.
#   -- If you need to store schemas with JSON messages for interoperability with certain 3rd-party sink connector.
key.converter.schemas.enable=false

# Converter class used to convert between Kafka Connect format and the serialized form that is written to Kafka.
# This controls the format of the values in messages written to or read from Kafka, and since this is independent
# of connectors it allows any connector to work with any serialization format.
# Kafka Connect Converters are: org.apache.kafka.connect.json.JsonConverter, org.apache.kafka.connect.storage.StringConverter,
# and org.apache.kafka.connect.converters.ByteArrayConverter
# Note: If the connector is processing JMS type messages and the `use.schema.for.jms.msgs` configuration property described above
# is set to true these messages will be set as structured data in JSON format. As a result of this the 
# org.apache.kafka.connect.json.JsonConverter should be used.
value.converter=org.apache.kafka.connect.converters.ByteArrayConverter

# This configuration property determines whether the schema of the value is included with the data when it is serialized.
# Setting to false (the default) excludes the schema if a schema is available, resulting in a smaller payload. However,
# setting the property to true will include the schema with the data when it is serialized.
# Set property to false:
#   -- If you are using a JSON converter and you don't need to include the schema information in your messages.
#   -- To reduce payload overhead, especially if your downstream applications don't need schema information.
#
# Set property to true:
#   -- If you are using a schema-aware converter.
#   -- If you need to ensure that each record has the correct structure and schema evolution is enable.
#   -- If you need to store schemas with JSON messages for interoperability with certain 3rd-party sink connector.
value.converter.schemas.enable=false

# Indicate the directory location of where the Oracle wallet is placed i.e. C:/tmp/wallet.
# The cwallet.sso, ewallet.p12, and tnsnames.ora files should be placed into this directory.
# Oracle Wallet provides a simple and easy method to manage database credentials across multiple domains.
# We will be using the Oracle TNS (Transport Network Substrate) administrative file to hide the details
# of the database connection string (host name, port number, and service name) from the datasource definition
# and instead us an alias.
# Note: This property will need to be updated before the Source Connector can connect.
wallet.path=<wallet directory>

# The TNS alias name for the database to connect to stored in the tnsnames.ora.
# An Oracle Wallet must be created and will be used to connect to the database.
# Note: This property will need to be updated before the Source Connector can connect.
db_tns_alias=<tns alias>

# Indicate the directory location of the where the tnsnames.ora location is located i.e C:/tmp/tnsnames.
# The entry in the tnsnames.ora should have the following format: 
# <aliasname> = (DESCRIPTION =(ADDRESS_LIST =(ADDRESS = (PROTOCOL = TCP)(Host = <hostname>)(Port = <port>)))(CONNECT_DATA =(SERVICE_NAME = <service_name>)))
# Note: This property will need to be updated before the Source Connector can connect.
tnsnames.path=<tnsnames.ora directory>

# The TxEventQ to pull data from to put into the specified Kafka topic.
# Note: This property will need to be updated before the Source Connector can connect.
txeventq.queue.name=<txEventQ queue name>

# The subscriber for the TxEventQ that data will be pulled from to put into the specified Kafka topic.
# Note: This property will need to be updated before the Source Connector can connect.
txeventq.subscriber=<txEventQ subscriber>

# List of Kafka brokers used for bootstrapping
# format: host1:port1,host2:port2 ...
# Note: This property will need to be updated before the Source Connector can connect.
bootstrap.servers=<broker i.e localhost:9092>

# Indicates the amount of time the connector should wait for the prior batch of messages to be sent to Kafka before
# a new poll request is made.
# Note: The time specified here should be less than the time defined for the task.shutdown.graceful.timeout.ms property
# since this is the time used by Kafka to determine the amount of time to wait for the tasks to shutdown gracefully. 
source.max.poll.blocked.time.ms=<time in milliseconds, default is 2000>

# This property will specify whether the messages from a TxEventQ shard will be placed into the respective Kafka partitions.
# If ordering within the shards need to be maintained when sent to the Kafka topic this property will
# need to be set to true and the TxEventQ will need to be created with 'STICKY_DEQUEUE' queue parameter set to 1. 
# If this property is set to true the number of Kafka partitions for the topic will need to be equal or greater 
# than the number of shards for the TxEventQ.
# If this property is set to false messages will be sent to a Kafka partition based on the message key or a 
# round-robin approach if no key is provided.
# The default value for this property is false.
txeventq.map.shard.to.kafka_partition=false

```
## Supported Message Types in Transactional Event Queue
* JMS message types </br>
<t>The Source Connector supports `BytesMessage`, `TextMessage`, and `MapMessage`. The Source Connector does not currently
support `ObjectMessage` and `StreamMessage`. </t></br>

* Raw message types </br>
* JSON message types </br>

## Schemas
The Oracle TxEventQ Source Connector will produce JMS messages with keys and values that
correspond to the schemas that will be described below if the message is a supported JMS message type
and the **use.schema.for.jms.msgs** configuration property is set to **true**. Click this [link](https://docs.oracle.com/en/database/oracle/oracle-database/23/adque/java_message_service.html#GUID-88D337F6-3C9F-41C6-9D55-559B48B350EF) for additional
information on JMS messages for an Oracle Transactional Event Queues.

#### Key Schema
This schema is used to store the incoming correlation id if one is present on the message interface. It will
be used as the key for the message that will be placed into a Kafka topic. The schema defines the following fields:

| Field Name    | Schema                      							| Required | Default Value | Description                                                  |
|---------------|-------------------------------------------------------|----------|---------------|--------------------------------------------------------------|
| correlation   | String                                                | Yes      |               |This field stores the value of [Message.getJMSCorrelationID()](https://docs.oracle.com/javaee/7/api/javax/jms/Message.html#getJMSCorrelationID()).|

#### JMS Message Schema
This schema is used to store the value of the JMS message. The schema defines the following fields:

| Field Name    | Schema                      							| Required | Default Value | Description                                                  |
|---------------|-------------------------------------------------------|----------|---------------|--------------------------------------------------------------|
| messageId     | String                      							| Yes      | 			   | This field stores the value of [Message.getJMSMessageID()](https://docs.oracle.com/javaee/7/api/javax/jms/Message.html#getJMSMessageID()).|
| messageType   | String                      							| Yes      | 			   | `text`, `map` or `bytes` depending on received message type. |
| correlationId | String                      							| No       | 			   | This field stores the value of [Message.getJMSCorrelationID()](https://docs.oracle.com/javaee/7/api/javax/jms/Message.html#getJMSCorrelationID()).|
| destination   | [JMS Destination](#jms-destination-schema)			| No       | 			   | This schema is used to represent a JMS Destination, and is either [queue](https://docs.oracle.com/javaee/7/api/javax/jms/Queue.html) or [topic](https://docs.oracle.com/javaee/7/api/javax/jms/Topic.html).|
| replyTo       | [JMS Destination](#jms-destination-schema)			| No       | 			   | This schema is used to represent a JMS Destination, and is either [queue](https://docs.oracle.com/javaee/7/api/javax/jms/Queue.html) or [topic](https://docs.oracle.com/javaee/7/api/javax/jms/Topic.html).|
| priority      | Int32                       							| No       | 	    	   | This field stores the value of [Message.getJMSPriority()](https://docs.oracle.com/javaee/7/api/javax/jms/Message.html#getJMSPriority()).|
| expiration    | Int64                       							| No       | 			   | This field stores the value of [Message.getJMSExpiration()](https://docs.oracle.com/javaee/7/api/javax/jms/Message.html#getJMSExpiration()).|
| type          | String                      							| No       | 			   | This field stores the value of [Message.getJMSType()](https://docs.oracle.com/javaee/6/api/javax/jms/Message.html#getJMSType()).|   
| timestamp     | Int64                       							| Yes      | 			   | Data from the [getJMSTimestamp()](https://docs.oracle.com/javaee/7/api/javax/jms/Message.html#getJMSTimestamp()) method.|
| deliveryMode  | Int32                                                 | Yes      |               | This field stores the value of [Message.getJMSDeliveryMode()](https://docs.oracle.com/javaee/7/api/javax/jms/Message.html#getJMSDeliveryMode()).|
| retry_count   | Int32                                                 | Yes      |               | This field stores the number of attempts for a retry on the message.|
| redelivered   | Boolean                     							| Yes      | 			   | This field stores the value of [Message.getJMSRedelivered()](https://docs.oracle.com/javaee/7/api/javax/jms/Message.html#getJMSRedelivered()).|
| properties    | Map<String, [Property Value](#property-value-schema)> | Yes      | 			   | This field stores the data from all of the properties for the Message indexed by their propertyName.|
| payloadBytes  | Bytes                       							| No       | 			   | This field stores the value from [BytesMessage.readBytes(byte[])](https://docs.oracle.com/javaee/7/api/javax/jms/BytesMessage.html#readBytes(byte[])). Empty for other types.| 
| payloadText   | String                      							| No       | 			   | This field stores the value from [TextMessage.getText()](https://docs.oracle.com/javaee/7/api/javax/jms/TextMessage.html#getText()). Empty for other types.|
| payloadMap    | Map<String, [Property Value](#property-value-schema)> | No       | 			   | This field stores the data from all of the map entries returned from [MapMessage.getMapNames()](https://docs.oracle.com/javaee/7/api/javax/jms/MapMessage.html#getMapNames()) for the Message indexed by their key. Empty for other types.|


#### JMS Destination Schema
This schema is used to depict a JMS Destination, and is either a [queue](https://docs.oracle.com/javaee/7/api/javax/jms/Queue.html) or [topic](https://docs.oracle.com/javaee/7/api/javax/jms/Topic.html).
The schema defines the following fields:

| Field Name 	   | Schema | Required | Default Value | Description                 |
|------------------|--------|----------|---------------|-----------------------------|
| type             | String | Yes      | 			   | JMS destination type (`queue` or `topic`).|
| name             | String | Yes      | 			   | JMS destination name. If the JMS destination type is a `queue` this will be the value of [AQjmsDestination.getQueueName()](https://docs.oracle.com/en/database/oracle/oracle-database/23/jajms/oracle/jms/AQjmsDestination.html#getQueueName__). If the JMS destination type is a `topic` this will be the value of [AQjmsDestination.getTopicName()](https://docs.oracle.com/en/database/oracle/oracle-database/23/jajms/oracle/jms/AQjmsDestination.html#getTopicName__).|
| owner            | String | No       |               | If JMS destination type is a `queue` this will be the value of [AQjmsDestination.getQueueOwner()](https://docs.oracle.com/en/database/oracle/oracle-database/23/jajms/oracle/jms/AQjmsDestination.html#getQueueOwner__). If JMS destination type is a `topic` this will be the value of [AQjmsDestination.getTopicOwner()](https://docs.oracle.com/en/database/oracle/oracle-database/23/jajms/oracle/jms/AQjmsDestination.html#getTopicOwner__)|
| completeName     | String | No       |               | If the JMS destination type is either a `queue` or `topic` this will be the value of [AQjmsDestination.getCompleteName()](https://docs.oracle.com/en/database/oracle/oracle-database/23/jajms/oracle/jms/AQjmsDestination.html#getCompleteName__).|
| completeTableName| String | No       |               | If the JMS destination type is either a `queue` or `topic` this will be the value of [AQjmsDestination.getCompleteTableName()](https://docs.oracle.com/en/database/oracle/oracle-database/23/jajms/oracle/jms/AQjmsDestination.html#getCompleteTableName__).|

#### Property Value Schema
This schema is used to store the data stored in the properties of the message. In order to
make sure the proper type mappings are preserved the field `propertyType` will store value
type of the field. The corresponding field in the schema will contain the data for the property.
This ensures that the data is retrievable as the type returned by [Message.getObjectProperty()](https://docs.oracle.com/javaee/7/api/javax/jms/Message.html#getObjectProperty(java.lang.String)).
The schema defines the following fields: 

| Field Name  | Schema  | Required | Default Value | Description                                                                             |
|-------------|---------|----------|---------------|-----------------------------------------------------------------------------------------|
| propertyType| String  | Yes      |			   | The Java type of the property on the Message. Types that can be specified are (`boolean`, `byte`, `short`, `integer`, `long`, `float`, `double`, or `string`). |
| boolean     | Boolean | No       |               | Boolean value is stored, empty otherwise. The `propertyType` is set to `boolean`.|
| byte        | Byte    | No       |               | Byte value is stored, empty otherwise. The `propertyType` is set to `byte`.|
| short       | Short   | No       |               | Short value is stored, empty otherwise. The `propertyType` is set to `short`.|
| integer     | Int32   | No       |               | Integer value is stored, empty otherwise. The `propertyType` is set to `integer`.|
| long        | Int64   | No       |               | Long value is stored, empty otherwise. The `propertyType` is set to `long`.|
| float       | Float32 | No       |               | Float value is stored, empty otherwise. The `propertyType` is set to `float`.|
| double      | Float64 | No       |               | Double value is stored, empty otherwise. The `propertyType` is set to `double`.|
| string      | String  | No       |               | String value is stored, empty otherwise. The `propertyType` is set to `string`.|


### Running TxEventQ Kafka connect sink or source connectors

Update Kafka's `connect-standalone.properties` or `connect-distributed.properties` configuration file located in Kafka's config directory `plugin.path=` property with the 
directory path to where the jar file for the connectors is located. Add the `consumer.max.poll.records` property to either the `connect-standalone.properties` or `connect-distributed.properties`
to increase the number of records that will be sent by the sink connector for each poll. The default value for the `consumer.max.poll.records` is 500. 

In the Kafka's config directory locate and open the zookeeper.properties file and update th dataDir property with the directory path where you installed Kafka.
The property should have a value such as dataDir=c:/kafka/zookeeper-data if the path to Kafka is c:\kafka. The same file will need to be updated in a Linux environment,
but use the relevant linux path for Kafka.

Now in the same Kafka config directory locate and open the server.properties file and locate the log.dirs and add the Kafka directory path to the /kafka-logs.
The property should have a value such as log.dirs=c:/kafka/kafka-logs if the path to Kafka is c:\kafka. The same file will need to be updated in a Linux environment,
but use the relevant linux path for Kafka.

If running Kafka in a Windows environment open 3 different command prompt windows and change to the directory where Kafka has been installed.

Run the following command in one of the command prompt windows to start zookeeper:

```bash
.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
```

In another command prompt start the Kafka server by running the following command:

```bash
.\bin\windows\kafka-server-start.bat .\config\server.properties
```

In the third command prompt start the connector in either standalone (connect-standalone.bat) or distributed (connect-distributed.bat) mode by running the following command.
The command below is connecting in standalone mode. If you want to run the source connector replace the properties file below with the properties file for the source connector.

```bash
.\bin\windows\connect-standalone.bat .\config\connect-standalone.properties .\config\connect-txeventq-sink.properties
```

If connecting in distributed mode on a Windows environment enter the following command in a command prompt.

```bash
.\bin\windows\connect-distributed.bat .\config\connect-distributed.properties 
```

If running Kafka in a Linux environment open 3 different terminals and change to the directory where Kafka has been installed.

Run the following command in one of the terminals to start zookeeper:

```bash
bin/zookeeper-server-start.sh config/zookeeper.properties 
```

In another terminal start the Kafka server by running the following command:

```bash
bin/kafka-server-start.sh config/server.properties 
```

In the third terminal start the connector in either standalone (connect-standalone.sh) or distributed (connect-distributed.sh) mode by running the following command.
The command below is connecting in standalone mode. If you want to run the source connector replace the properties file below with the properties file for the source connector.

```bash
bin/connect-standalone.sh config/connect-standalone.properties config/connect-TxEventQ-sink.properties 
```

If connecting in distributed mode on a Linux environment enter the following command in a command prompt.

```bash
bin/connect-distributed.sh config/connect-distributed.properties
```

Use REST calls to post configuration properties when running in distributed mode. An example of a JSON configuration is shown below.

```bash
{
        "connector.class": "oracle.jdbc.txeventq.kafka.connect.source.TxEventQSourceConnector",
        "tasks.max": "5",
        "kafka.topic": <Kafka topic>,
        "value.converter": "org.apache.kafka.connect.converters.ByteArrayConverter",
        "db_tns_alias": <tns alias>,
        "wallet.path": <specify wallet path>,
        "tnsnames.path": <specify tnsnames path>,
        "txeventq.queue.name": <txEventQ queue name>,
        "txeventq.subscriber": <txEventQ subscriber>,
        "bootstrap.servers": <broker i.e localhost:9092>
}
```
