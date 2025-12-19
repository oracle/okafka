# TxEventQ Source Connector for Kafka Connect

The Kafka Connect TxEventQ Source connector is used to move messages from Oracle Transactional Event Queues (TxEventQ) into Apache Kafka® topics. This connector reads records from a specified Transactional Event Queue in an Oracle Database and writes them to a specified Kafka topic.

The connector uses secure credential management for database authentication and Oracle TNS (Transport Network Substrate) for database connection management.

## Quick Start

To get started with the TxEventQ Source Connector:

1. **Complete installation and setup**: Follow the [Installation and Setup Guide](README.md#installation-and-setup-guide-for-oracle-txeventq-jms-connectors) to install the connector and set up Oracle Database (including creating a queue with a subscriber)
2. **Configure the connector**: Create a configuration file with connection details, queue information, and target Kafka topic (see [Configuration Reference](#configuration-reference))
3. **Start the connector**: Run in standalone or distributed mode (see [Examples](#examples))

For connector-specific configuration details, see the sections below.

## Features

### At least once delivery

This connector guarantees that records are delivered at least once to the Kafka topic. If the connector restarts, there may be some duplicate records in the Kafka topic.

### Multiple tasks

The TxEventQ Source connector supports running one or more tasks. You can specify the number of tasks in the `tasks.max` configuration parameter. This can lead to performance gains when processing multiple shards or event streams.

**Important:** If the transactional event queue has `STICKY_DEQUEUE` set and you are running on an Oracle database version less than 23.4, the `tasks.max` number specified must be equal to the number of event streams (`SHARD_NUM`) for the queue. If the `tasks.max` is not equal to the `SHARD_NUM`, dequeue from all event streams will not be performed when using a database with a version less than 23.4.

### Support for JMS message types

The connector supports multiple JMS message types including `BytesMessage`, `TextMessage`, and `MapMessage`. The connector does not currently support `ObjectMessage` and `StreamMessage`.

### Schema support

The connector can use built-in schemas for JMS messages, providing structured data representation in JSON format. This enables better integration with schema-aware systems and tools.

### Shard to partition mapping

The connector can map messages from TxEventQ shards to respective Kafka partitions, maintaining ordering within shards when required.

## Installation and Setup

For installation instructions, Oracle Database setup, and common best practices, see the [Installation and Setup Guide](README.md#installation-and-setup-guide-for-oracle-txeventq-jms-connectors).

**Note for Source Connector**: The Source Connector requires a queue with a subscriber. When creating the queue:
* Ensure the queue is created with `multiple_consumers => TRUE` if you plan to use multiple source connectors
* Add a subscriber using `dbms_aqadm.add_subscriber()` before starting the connector
* If `STICKY_DEQUEUE=1` and using Oracle Database < 23.4, set `tasks.max` equal to `SHARD_NUM`

## Configuration Reference

To use this connector, specify the name of the connector class in the `connector.class` configuration property.

```
connector.class=oracle.jdbc.txeventq.kafka.connect.source.TxEventQSourceConnector
```

Connector-specific configuration properties are described below.

## Kafka Connect

`name`

Globally unique name to use for this connector.

* Type: string
* Importance: high

`connector.class`

Name or alias of the class for this connector. Must be a subclass of `org.apache.kafka.connect.connector.Connector`.

* Type: string
* Importance: high

`tasks.max`

Maximum number of tasks to use for this connector.

**Note:** If the transactional event queue has `STICKY_DEQUEUE` set and running on a database version less than 23.4, the `tasks.max` number specified must be equal to the number of event streams (`SHARD_NUM`) for the queue. If the `tasks.max` is not equal to the `SHARD_NUM`, dequeue from all event streams will not be performed when using a database with a version less than 23.4.

* Type: int
* Default: 1
* Valid Values: [1,...]
* Importance: high

`kafka.topic`

The name of the Kafka topic where the connector writes all records that were read from the TxEventQ.

* Type: string
* Default: ""
* Importance: high

`bootstrap.servers`

A list of host/port pairs used to establish the initial connection to the Kafka cluster. This list must be in the form `host1:port1,host2:port2,....`

* Type: list
* Importance: high

## Oracle Database Connection

`wallet.path`

The directory path containing Oracle Wallet files (`cwallet.sso`, `ewallet.p12`) if using Oracle Wallet for credential management. The wallet stores database credentials securely and is used with TNS aliases for connection management.

* Type: string
* Importance: high

`tnsnames.path`

The directory path containing the `tnsnames.ora` file. The `tnsnames.ora` file contains TNS alias definitions that map aliases to database connection details. See [Database Authentication](README.md#database-authentication) for the required format.

* Type: string
* Importance: high

`db_tns_alias`

The TNS alias name for the database to connect to stored in the tnsnames.ora.

* Type: string
* Importance: high

## Transactional Event Queue

`txeventq.queue.name`

The TxEventQ to pull data from to put into the specified Kafka topic.

* Type: string
* Importance: high

`txeventq.subscriber`

The subscriber for the TxEventQ that data will be pulled from to put into the specified Kafka topic.

* Type: string
* Importance: high

`txeventq.batch.size`

The maximum number of records to dequeue from the Oracle Transactional Event Queue before writing to Kafka.

* Type: int
* Default: 250
* Valid Values: [1,...]
* Importance: low

`txeventq.map.shard.to.kafka_partition`

If `true`, maps each TxEventQ shard to a corresponding Kafka partition, preserving ordering within shards. Requires `STICKY_DEQUEUE=1` on the queue and Kafka topic partitions >= queue shards. If `false`, partition assignment uses message key or round-robin.

* Type: boolean
* Default: false
* Importance: low

`source.max.poll.blocked.time.ms`

Maximum time (milliseconds) to wait for a batch to be sent to Kafka before starting a new poll. Must be less than `task.shutdown.graceful.timeout.ms` to allow graceful shutdowns.

* Type: int
* Default: 2000
* Importance: medium

## JMS Message Configuration

`use.schema.for.jms.msgs`

If set to `true`, the built-in schema for JMS Messages will be used and stored in the message payload. The JMS message types that are supported are `BytesMessage`, `TextMessage`, and `MapMessage`.

* Type: boolean
* Default: false
* Importance: low
* Dependents: `key.converter`, `value.converter`

`header.jms.allowlist`

Comma-separated list of JMS header keys to include in Kafka headers. Valid keys: `jmsMessageType`, `jmsMessageId`, `jmsCorrelationId`, `jmsDestination`, `jmsReplyTo`, `jmsPriority`, `jmsDeliveryMode`, `jmsRetry_count`, `jmsExpiration`, `jmsTimestamp`, `jmsType`, `jmsRedelivered`, `jmsProperties`. Ignored when `use.schema.for.jms.msgs=true` (all properties included in schema).

* Type: list
* Default: ""
* Importance: low

`header.denylist`

Comma-separated list of header keys to exclude from Kafka headers. If the Sink Connector used `txeventq.jms.bytes.include.kafka.metadata=true`, Kafka metadata headers (`KAFKA_TOPIC`, `KAFKA_PARTITION`, `KAFKA_OFFSET`, `KAFKA_TIMESTAMP`) may be present and can be excluded here.

* Type: list
* Default: ""
* Importance: low

## Converters

`key.converter`

Converter class for serializing keys. Use `org.apache.kafka.connect.json.JsonConverter` when `use.schema.for.jms.msgs=true`. Common choices: `org.apache.kafka.connect.storage.StringConverter`, `org.apache.kafka.connect.json.JsonConverter`, `org.apache.kafka.connect.converters.ByteArrayConverter`.

* Type: class
* Importance: low

`key.converter.schemas.enable`

If `true`, includes schema with key data (composite JSON object). If `false`, only data is serialized (reduces payload size). Set based on downstream consumer requirements.

* Type: boolean
* Default: false
* Importance: low

`value.converter`

Converter class for serializing values. Use `org.apache.kafka.connect.json.JsonConverter` when `use.schema.for.jms.msgs=true`. Common choices: `org.apache.kafka.connect.storage.StringConverter`, `org.apache.kafka.connect.json.JsonConverter`, `org.apache.kafka.connect.converters.ByteArrayConverter`.

* Type: class
* Importance: low

`value.converter.schemas.enable`

If `true`, includes schema with value data (composite JSON object). If `false`, only data is serialized (reduces payload size). Set based on downstream consumer requirements.

* Type: boolean
* Default: false
* Importance: low

## Supported Message Types

The TxEventQ Source Connector is able to read the following types of data from a Transactional Event Queue:

* **JMS message types:** The TxEventQ Source Connector supports `BytesMessage`, `TextMessage`, and `MapMessage`. The TxEventQ Source Connector does not currently support `ObjectMessage` and `StreamMessage`.
* **Raw message types**
* **JSON message types**

## Headers

The Oracle TxEventQ Source Connector can process header information for TxEventQ messages of type **JmsBytesMessage** that have a `MessageVersion` property value of 2. If a message of this type is encountered, the header information that is stored in the message payload will be extracted and placed into the required Kafka topic's header field. The number of headers that will need to be processed is determined by looking at the count value stored in the property `AQINTERNAL_HEADERCOUNT`.

## Schemas

The Oracle TxEventQ Source Connector will produce JMS messages with keys and values that correspond to the schemas described below if the message is a supported JMS message type and the **`use.schema.for.jms.msgs`** configuration property is set to **`true`**.

For additional information on JMS messages for Oracle Transactional Event Queues, see the [Oracle Database documentation](https://docs.oracle.com/en/database/oracle/oracle-database/23/adque/java_message_service.html#GUID-88D337F6-3C9F-41C6-9D55-559B48B350EF).

### Key Schema

This schema is used to store the incoming correlation ID if one is present on the message interface. It will be used as the key for the message that will be placed into a Kafka topic. The schema defines the following fields:

| Field Name | Schema | Required | Default Value | Description |
|------------|--------|----------|---------------|-------------|
| `correlation` | String | Yes | | This field stores the value of [Message.getJMSCorrelationID()](https://docs.oracle.com/javaee/7/api/javax/jms/Message.html#getJMSCorrelationID()). |

### JMS Message Schema

This schema is used to store the value of the JMS message. The schema defines the following fields:

| Field Name | Schema | Required | Default Value | Description |
|------------|--------|----------|---------------|-------------|
| `jmsMessageId` | String | Yes | | This field stores the value of [Message.getJMSMessageID()](https://docs.oracle.com/javaee/7/api/javax/jms/Message.html#getJMSMessageID()). |
| `jmsMessageType` | String | Yes | | `text`, `map`, or `bytes` depending on received message type. |
| `jmsCorrelationId` | String | No | | This field stores the value of [Message.getJMSCorrelationID()](https://docs.oracle.com/javaee/7/api/javax/jms/Message.html#getJMSCorrelationID()). |
| `jmsDestination` | [JMS Destination](#jms-destination-schema) | No | | This schema is used to represent a JMS Destination, and is either [queue](https://docs.oracle.com/javaee/7/api/javax/jms/Queue.html) or [topic](https://docs.oracle.com/javaee/7/api/javax/jms/Topic.html). |
| `jmsReplyTo` | [JMS Destination](#jms-destination-schema) | No | | This schema is used to represent a JMS Destination, and is either [queue](https://docs.oracle.com/javaee/7/api/javax/jms/Queue.html) or [topic](https://docs.oracle.com/javaee/7/api/javax/jms/Topic.html). |
| `jmsPriority` | Int32 | No | | This field stores the value of [Message.getJMSPriority()](https://docs.oracle.com/javaee/7/api/javax/jms/Message.html#getJMSPriority()). |
| `jmsExpiration` | Int64 | No | | This field stores the value of [Message.getJMSExpiration()](https://docs.oracle.com/javaee/7/api/javax/jms/Message.html#getJMSExpiration()). |
| `jmsType` | String | No | | This field stores the value of [Message.getJMSType()](https://docs.oracle.com/javaee/6/api/javax/jms/Message.html#getJMSType()). |
| `jmsTimestamp` | Int64 | Yes | | Data from the [getJMSTimestamp()](https://docs.oracle.com/javaee/7/api/javax/jms/Message.html#getJMSTimestamp()) method. |
| `jmsDeliveryMode` | Int32 | Yes | | This field stores the value of [Message.getJMSDeliveryMode()](https://docs.oracle.com/javaee/7/api/javax/jms/Message.html#getJMSDeliveryMode()). |
| `jmsRetry_count` | Int32 | Yes | | This field stores the number of attempts for a retry on the message. |
| `jmsRedelivered` | Boolean | Yes | | This field stores the value of [Message.getJMSRedelivered()](https://docs.oracle.com/javaee/7/api/javax/jms/Message.html#getJMSRedelivered()). |
| `jmsProperties` | Map<String, [Property Value](#property-value-schema)> | Yes | | This field stores the data from all of the properties for the Message indexed by their propertyName. |
| `payloadBytes` | Bytes | No | | This field stores the value from [BytesMessage.readBytes(byte[])](https://docs.oracle.com/javaee/7/api/javax/jms/BytesMessage.html#readBytes(byte[])). Empty for other types. |
| `payloadText` | String | No | | This field stores the value from [TextMessage.getText()](https://docs.oracle.com/javaee/7/api/javax/jms/TextMessage.html#getText()). Empty for other types. |
| `payloadMap` | Map<String, [Property Value](#property-value-schema)> | No | | This field stores the data from all of the map entries returned from [MapMessage.getMapNames()](https://docs.oracle.com/javaee/7/api/javax/jms/MapMessage.html#getMapNames()) for the Message indexed by their key. Empty for other types. |

### JMS Destination Schema

This schema is used to depict a JMS Destination, and is either a [queue](https://docs.oracle.com/javaee/7/api/javax/jms/Queue.html) or [topic](https://docs.oracle.com/javaee/7/api/javax/jms/Topic.html). The schema defines the following fields:

| Field Name | Schema | Required | Default Value | Description |
|------------|--------|----------|---------------|-------------|
| `type` | String | Yes | | JMS destination type (`queue` or `topic`). |
| `name` | String | Yes | | JMS destination name. If the JMS destination type is a `queue`, this will be the value of [AQjmsDestination.getQueueName()](https://docs.oracle.com/en/database/oracle/oracle-database/23/jajms/oracle/jms/AQjmsDestination.html#getQueueName__). If the JMS destination type is a `topic`, this will be the value of [AQjmsDestination.getTopicName()](https://docs.oracle.com/en/database/oracle/oracle-database/23/jajms/oracle/jms/AQjmsDestination.html#getTopicName__). |
| `owner` | String | No | | If JMS destination type is a `queue`, this will be the value of [AQjmsDestination.getQueueOwner()](https://docs.oracle.com/en/database/oracle/oracle-database/23/jajms/oracle/jms/AQjmsDestination.html#getQueueOwner__). If JMS destination type is a `topic`, this will be the value of [AQjmsDestination.getTopicOwner()](https://docs.oracle.com/en/database/oracle/oracle-database/23/jajms/oracle/jms/AQjmsDestination.html#getTopicOwner__). |
| `completeName` | String | No | | If the JMS destination type is either a `queue` or `topic`, this will be the value of [AQjmsDestination.getCompleteName()](https://docs.oracle.com/en/database/oracle/oracle-database/23/jajms/oracle/jms/AQjmsDestination.html#getCompleteName__). |
| `completeTableName` | String | No | | If the JMS destination type is either a `queue` or `topic`, this will be the value of [AQjmsDestination.getCompleteTableName()](https://docs.oracle.com/en/database/oracle/oracle-database/23/jajms/oracle/jms/AQjmsDestination.html#getCompleteTableName__). |

### Property Value Schema

This schema is used to store the data stored in the properties of the message. In order to make sure the proper type mappings are preserved, the field `propertyType` will store the value type of the field. The corresponding field in the schema will contain the data for the property. This ensures that the data is retrievable as the type returned by [Message.getObjectProperty()](https://docs.oracle.com/javaee/7/api/javax/jms/Message.html#getObjectProperty(java.lang.String)). The schema defines the following fields:

| Field Name | Schema | Required | Default Value | Description |
|------------|--------|----------|---------------|-------------|
| `propertyType` | String | Yes | | The Java type of the property on the Message. Types that can be specified are (`boolean`, `byte`, `short`, `integer`, `long`, `float`, `double`, or `string`). |
| `boolean` | Boolean | No | | Boolean value is stored, empty otherwise. The `propertyType` is set to `boolean`. |
| `byte` | Byte | No | | Byte value is stored, empty otherwise. The `propertyType` is set to `byte`. |
| `short` | Short | No | | Short value is stored, empty otherwise. The `propertyType` is set to `short`. |
| `integer` | Int32 | No | | Integer value is stored, empty otherwise. The `propertyType` is set to `integer`. |
| `long` | Int64 | No | | Long value is stored, empty otherwise. The `propertyType` is set to `long`. |
| `float` | Float32 | No | | Float value is stored, empty otherwise. The `propertyType` is set to `float`. |
| `double` | Float64 | No | | Double value is stored, empty otherwise. The `propertyType` is set to `double`. |
| `string` | String | No | | String value is stored, empty otherwise. The `propertyType` is set to `string`. |

## Best Practices

For general best practices on queue configuration, Oracle Database setup, and performance tuning, see the [Installation and Setup Guide](README.md#best-practices).

### Source Connector-Specific Best Practices

* **Configure tasks.max based on database version and STICKY_DEQUEUE**:
  * For Oracle Database 23.4+: You can set `tasks.max` flexibly based on throughput requirements and number of shards
  * For Oracle Database < 23.4 with `STICKY_DEQUEUE=1`: You must set `tasks.max` equal to `SHARD_NUM` to ensure all shards are processed. If `tasks.max` is not equal to `SHARD_NUM`, some shards will not be dequeued.

* **Configure subscriber correctly**: Ensure the subscriber specified in `txeventq.subscriber` exists and is properly configured for the queue. The subscriber must be added using `dbms_aqadm.add_subscriber()` before the connector can dequeue messages.

* **Use schema for JMS messages when needed**: Set `use.schema.for.jms.msgs=true` if you need structured JSON output with full JMS message metadata. This is useful for integration with schema-aware systems or when you need to preserve all JMS message properties. When enabled, all JMS properties are stored in the Kafka message as a JSON schema, and `header.jms.allowlist` is ignored.

* **Configure header allowlist efficiently**: When `use.schema.for.jms.msgs=false`, use `header.jms.allowlist` to include only the JMS headers you actually need. Valid headers include: `jmsMessageType`, `jmsMessageId`, `jmsCorrelationId`, `jmsDestination`, `jmsReplyTo`, `jmsPriority`, `jmsDeliveryMode`, `jmsRetry_count`, `jmsExpiration`, `jmsTimestamp`, `jmsType`, `jmsRedelivered`, and `jmsProperties`.

* **Use header denylist to filter metadata**: If the Sink Connector was used with `txeventq.jms.bytes.include.kafka.metadata=true`, Kafka metadata (KAFKA_TOPIC, KAFKA_PARTITION, KAFKA_OFFSET, KAFKA_TIMESTAMP) will be in the headers. Use `header.denylist` to exclude these if not needed.

* **Enable shard-to-partition mapping for ordering**: Set `txeventq.map.shard.to.kafka_partition=true` when you need to maintain ordering within shards. This maps each TxEventQ shard to a corresponding Kafka partition, preserving order within each shard. Requires `STICKY_DEQUEUE=1` on the queue and Kafka topic partitions >= queue shards.

* **Configure batch size appropriately**: The default `txeventq.batch.size` of 250 is a good starting point. Increase for higher throughput (if the database can handle it), decrease for lower latency. Larger batches improve throughput but increase memory usage and latency.

* **Balance source.max.poll.blocked.time.ms**: This controls how long the connector waits for a batch to be sent to Kafka before starting a new poll. Adjust based on your latency requirements, but ensure it's less than `task.shutdown.graceful.timeout.ms` to allow graceful shutdowns.

## Examples

### Running TxEventQ Source Connector in Standalone Mode

Standalone mode is suitable for development and testing.

1. **Prepare configuration files**:
   * Worker configuration: `connect-standalone.properties` (Kafka distribution default)
   * Connector configuration: `connect-txeventq-source.properties`

2. **Update `connect-standalone.properties`**:
   * Set `plugin.path` to the parent directory containing the connector plugin folder

3. **Start Kafka server** (if not already running).

4. **Start the connector**:

```bash
bin/connect-standalone.sh config/connect-standalone.properties config/connect-txeventq-source.properties
```

### Running TxEventQ Source Connector in Distributed Mode

Distributed mode provides scalability and fault tolerance for production environments.

1. **Update `connect-distributed.properties`**:
   * Set `plugin.path` to the parent directory containing the connector plugin folder

2. **Start Kafka server** (if not already running).

3. **Start Kafka Connect**:

```bash
bin/connect-distributed.sh config/connect-distributed.properties
```

4. **Deploy the connector** via REST API:

```bash
curl -X POST -H "Content-Type: application/json" --data "@./config/txeventQ-source.json" http://localhost:8083/connectors
```

### Example Configuration File (JSON)

This example shows a basic configuration for a source connector that reads from a Transactional Event Queue and writes to a Kafka topic:

```json
{
  "name": "TxEventQ-source",
  "config": {
    "connector.class": "oracle.jdbc.txeventq.kafka.connect.source.TxEventQSourceConnector",
    "tasks.max": "1",
    "kafka.topic": "orders-kafka-topic",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": false,
    "value.converter.schemas.enable": false,
    "db_tns_alias": "ORCLPDB",
    "wallet.path": "/etc/kafka-connect/wallets/oracle",
    "tnsnames.path": "/etc/kafka-connect/wallets",
    "txeventq.queue.name": "ORDERS_QUEUE",
    "txeventq.subscriber": "ORDERS_SUBSCRIBER",
    "bootstrap.servers": "kafka-broker1:9092,kafka-broker2:9092",
    "txeventq.batch.size": "250",
    "use.schema.for.jms.msgs": false,
    "txeventq.map.shard.to.kafka_partition": false,
    "source.max.poll.blocked.time.ms": 2000
  }
}
```

**Configuration Notes**:
* Update `kafka.topic`, `db_tns_alias`, `wallet.path`, `tnsnames.path`, `txeventq.queue.name`, `txeventq.subscriber`, and `bootstrap.servers` with your environment-specific values
* Set `use.schema.for.jms.msgs=true` if you need structured JMS message schemas

### Example Configuration File (Properties)

```properties
name=TxEventQ-source
connector.class=oracle.jdbc.txeventq.kafka.connect.source.TxEventQSourceConnector
tasks.max=1
kafka.topic=orders-kafka-topic
wallet.path=/etc/kafka-connect/wallets/oracle
tnsnames.path=/etc/kafka-connect/wallets
db_tns_alias=ORCLPDB
txeventq.queue.name=ORDERS_QUEUE
txeventq.subscriber=ORDERS_SUBSCRIBER
bootstrap.servers=kafka-broker1:9092,kafka-broker2:9092
txeventq.batch.size=250
use.schema.for.jms.msgs=false
txeventq.map.shard.to.kafka_partition=false
source.max.poll.blocked.time.ms=2000
key.converter=org.apache.kafka.connect.json.JsonConverter
value.converter=org.apache.kafka.connect.json.JsonConverter
key.converter.schemas.enable=false
value.converter.schemas.enable=false
```

## License

This connector is licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.

## Troubleshooting

For common setup issues (installation, database connection, queue setup), see the [Installation and Setup Guide](README.md#troubleshooting-common-setup-issues).

### Source Connector-Specific Issues

**No messages being dequeued**
* Verify queue is started: `SELECT queue_name, enabled FROM user_queues WHERE queue_name = 'YOUR_QUEUE';`
* Verify subscriber exists: `SELECT * FROM user_queue_subscribers WHERE queue_name = 'YOUR_QUEUE';`
* Verify user has `EXECUTE` on `dbms_aqjms`
* Check `txeventq.queue.name` and `txeventq.subscriber` values

**Not all shards being processed (Database < 23.4)**
* When `STICKY_DEQUEUE=1`, set `tasks.max` equal to `SHARD_NUM`

**Messages not appearing in Kafka topic**
* Verify Kafka brokers are accessible
* Verify target topic exists or auto-creation is enabled
* Check converter configuration matches message format
* Review Connect worker logs

**Schema-related errors when using use.schema.for.jms.msgs=true**
* Ensure `key.converter` and `value.converter` are set to `JsonConverter`
* Verify messages are supported JMS types (BytesMessage, TextMessage, MapMessage)
* Check `key.converter.schemas.enable` and `value.converter.schemas.enable` settings

**Performance issues**
* Increase `txeventq.batch.size` for higher throughput (if database can handle it)
* Increase `tasks.max` if using Oracle Database 23.4+ or when `STICKY_DEQUEUE=0`
* Monitor database connection pool usage
* Adjust `source.max.poll.blocked.time.ms` based on latency requirements

## Additional Resources

* [Oracle Transactional Event Queues Documentation](https://docs.oracle.com/en/database/oracle/oracle-database/23/adque/Kafka_cient_interface_TEQ.html#GUID-C329D40D-21D6-454E-8B6A-49D96F0C8795)
* [Oracle Database Advanced Queuing](https://docs.oracle.com/en/database/oracle/oracle-database/23/adque/)
* [Apache Kafka Connect Documentation](https://kafka.apache.org/documentation/#connect)

