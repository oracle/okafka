# TxEventQ Sink Connector for Kafka Connect

The Kafka Connect TxEventQ Sink connector is used to move messages from Apache Kafka® topics into Oracle Transactional Event Queues (TxEventQ). This connector reads records from Kafka topics and writes them to a specified Transactional Event Queue in an Oracle Database.

The connector uses secure credential management for database authentication and Oracle TNS (Transport Network Substrate) for database connection management.

## Quick Start

To get started with the TxEventQ Sink Connector:

1. **Complete installation and setup**: Follow the [Installation and Setup Guide](README.md#installation-and-setup-guide-for-oracle-txeventq-jms-connectors) to install the connector and set up Oracle Database
2. **Configure the connector**: Create a configuration file with connection details and queue information (see [Configuration Reference](#configuration-reference))
3. **Start the connector**: Run in standalone or distributed mode (see [Examples](#examples))

For connector-specific configuration details, see the sections below.

## Features

### Exactly once delivery

This connector guarantees that records are delivered exactly once to the Transactional Event Queue. If the connector restarts, the sink connector will determine the correct offset to start from.

### Multiple tasks

The TxEventQ Sink connector supports running one or more tasks. You can specify the number of tasks in the `tasks.max` configuration parameter. This can lead to performance gains when processing multiple Kafka topic partitions.

### JMS Bytes Message support

The connector supports JMS type Transactional Event Queue payload, which is the default payload type. The TxEventQ Sink Connector stores the payload as a JMS_BYTES message. The connector can optionally include Kafka headers and metadata in the message payload.

### Header and metadata processing

The connector can process header information from Kafka messages into the Transactional Event Queue as part of the payload. When enabled, the connector can also forward Kafka metadata (topic name, partition, offset, and timestamp) to TxEventQ as header information.

## Installation and Setup

For installation instructions, Oracle Database setup, and common best practices, see the [Installation and Setup Guide](README.md#installation-and-setup-guide-for-oracle-txeventq-jms-connectors).

**Note for Sink Connector**: The Sink Connector stores messages as JMS_BYTES messages. When planning your queue configuration, consider that:
* If you plan to use both the TxEventQ Sink and Source connectors on the same queue, set all relevant parameters (`KEY_BASED_ENQUEUE`, `SHARD_NUM`, `STICKY_DEQUEUE`) before using the connectors
* For `KEY_BASED_ENQUEUE=2`, ensure `SHARD_NUM` is greater than or equal to the number of Kafka topic partitions

## Configuration Reference

To use this connector, specify the name of the connector class in the `connector.class` configuration property.

```
connector.class=oracle.jdbc.txeventq.kafka.connect.sink.TxEventQSinkConnector
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

* Type: int
* Default: 1
* Valid Values: [1,...]
* Importance: high

`topics`

List of topics to consume, separated by commas.

* Type: list
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

The TxEventQ to put the Kafka data into.

* Type: string
* Importance: high

`txeventq.queue.schema`

The name of the schema for the TxEventQ specified in the `txeventq.queue.name` property field.

* Type: string
* Importance: high

## JMS Message Configuration

`txeventq.jms.bytes.include.kafka.headers`

Indicates whether the JMS Bytes message will process Header information from Kafka header into TxEventQ. The default value for this property is `false` and Header information from Kafka will not be processed into TxEventQ.

* Type: boolean
* Default: false
* Importance: low

`txeventq.jms.bytes.include.kafka.metadata`

If enabled, forwards Kafka record metadata (topic, partition, offset, timestamp) to TxEventQ as JMS message properties. Requires `txeventq.jms.bytes.include.kafka.headers=true`. Metadata is stored as: `KAFKA_TOPIC` (String), `KAFKA_PARTITION` (Int), `KAFKA_OFFSET` (Long), `KAFKA_TIMESTAMP` (Long).

* Type: boolean
* Default: false
* Importance: low
* Dependents: `txeventq.jms.bytes.include.kafka.headers`

## Converters

`key.converter`

Converter class for serializing keys. Common choices: `org.apache.kafka.connect.storage.StringConverter`, `org.apache.kafka.connect.json.JsonConverter`, `org.apache.kafka.connect.converters.ByteArrayConverter`.

* Type: class
* Importance: low

`value.converter`

Converter class for serializing values. Common choices: `org.apache.kafka.connect.storage.StringConverter`, `org.apache.kafka.connect.json.JsonConverter`, `org.apache.kafka.connect.converters.ByteArrayConverter`.

* Type: class
* Importance: low

### Message Payload Format

The connector stores messages as JMS_BYTES messages. By default, only key and value are stored. When `txeventq.jms.bytes.include.kafka.headers` or `txeventq.jms.bytes.include.kafka.metadata` is `true`, headers are included in the payload.

**Payload structure** (when headers are enabled):
```
[KEY LENGTH (4 bytes)] [KEY] [VALUE LENGTH (4 bytes)] [VALUE]
[HEADER NAME LENGTH (4 bytes)] [HEADER NAME] [HEADER VALUE LENGTH (4 bytes)] [HEADER VALUE]
... (additional headers)
```

The header count is stored in the `AQINTERNAL_HEADERCOUNT` property.

## Best Practices

For general best practices on queue configuration, Oracle Database setup, and performance tuning, see the [Installation and Setup Guide](README.md#best-practices).

### Sink Connector-Specific Best Practices

* **Enable header/metadata forwarding selectively**: Setting `txeventq.jms.bytes.include.kafka.headers` or `txeventq.jms.bytes.include.kafka.metadata` to `true` increases payload size in TxEventQ. Only enable these if downstream consumers (e.g., Source Connector) need this information. The header count is stored in `AQINTERNAL_HEADERCOUNT` property.

* **Design keys for 128-byte limit**: Since TxEventQ correlation values (used for Kafka keys) are limited to 128 bytes, design your Kafka keys to be within this limit. If keys exceed 128 bytes, they will be truncated, which may cause collisions.

* **Balance batch size with database load**: The connector processes records in batches. Adjust `consumer.max.poll.records` in the Connect worker configuration to balance throughput and database load. Larger batches improve throughput but may increase database connection time.

* **Match tasks to partitions**: Set `tasks.max` to match the number of Kafka topic partitions for optimal parallelism.

## Examples

### Running TxEventQ Sink Connector in Standalone Mode

Standalone mode is suitable for development and testing.

1. **Prepare configuration files**:
   * Worker configuration: `connect-standalone.properties` (Kafka distribution default)
   * Connector configuration: `connect-txeventq-sink.properties`

2. **Update `connect-standalone.properties`**:
   * Set `plugin.path` to the parent directory containing the connector plugin folder
   * Optionally adjust `consumer.max.poll.records` (default: 500)

3. **Start Kafka server** (if not already running).

4. **Start the connector**:

```bash
bin/connect-standalone.sh config/connect-standalone.properties config/connect-txeventq-sink.properties
```

### Running TxEventQ Sink Connector in Distributed Mode

Distributed mode provides scalability and fault tolerance for production environments.

1. **Update `connect-distributed.properties`**:
   * Set `plugin.path` to the parent directory containing the connector plugin folder
   * Optionally adjust `consumer.max.poll.records` (default: 500)

2. **Start Kafka server** (if not already running).

3. **Start Kafka Connect**:

```bash
bin/connect-distributed.sh config/connect-distributed.properties
```

4. **Deploy the connector** via REST API:

```bash
curl -X POST -H "Content-Type: application/json" --data "@./config/txeventQ-sink.json" http://localhost:8083/connectors
```

### Example Configuration File (JSON)

This example shows a basic configuration for a sink connector that reads from a Kafka topic and writes to a Transactional Event Queue:

```json
{
  "name": "TxEventQ-sink",
  "config": {
    "connector.class": "oracle.jdbc.txeventq.kafka.connect.sink.TxEventQSinkConnector",
    "tasks.max": "1",
    "topics": "orders-topic",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.storage.StringConverter",
    "db_tns_alias": "ORCLPDB",
    "wallet.path": "/etc/kafka-connect/wallets/oracle",
    "tnsnames.path": "/etc/kafka-connect/wallets",
    "txeventq.queue.name": "ORDERS_QUEUE",
    "txeventq.queue.schema": "TXEVENTQ_ADMIN",
    "txeventq.jms.bytes.include.kafka.headers": "false",
    "txeventq.jms.bytes.include.kafka.metadata": "false",
    "bootstrap.servers": "kafka-broker1:9092,kafka-broker2:9092"
  }
}
```

**Configuration Notes**:
* Update `topics`, `db_tns_alias`, `wallet.path`, `tnsnames.path`, `txeventq.queue.name`, `txeventq.queue.schema`, and `bootstrap.servers` with your environment-specific values

### Example Configuration File (Properties)

```properties
name=TxEventQ-sink
connector.class=oracle.jdbc.txeventq.kafka.connect.sink.TxEventQSinkConnector
tasks.max=1
topics=orders-topic
wallet.path=/etc/kafka-connect/wallets/oracle
tnsnames.path=/etc/kafka-connect/wallets
db_tns_alias=ORCLPDB
txeventq.queue.name=ORDERS_QUEUE
txeventq.queue.schema=TXEVENTQ_ADMIN
txeventq.jms.bytes.include.kafka.headers=false
txeventq.jms.bytes.include.kafka.metadata=false
bootstrap.servers=kafka-broker1:9092,kafka-broker2:9092
key.converter=org.apache.kafka.connect.storage.StringConverter
value.converter=org.apache.kafka.connect.storage.StringConverter
```

## License

This connector is licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.

## Troubleshooting

For common setup issues (installation, database connection, queue setup), see the [Installation and Setup Guide](README.md#troubleshooting-common-setup-issues).

### Sink Connector-Specific Issues

**Messages not appearing in queue**
* Verify queue is started: `SELECT queue_name, enabled FROM user_queues WHERE queue_name = 'YOUR_QUEUE';`
* Verify user has `EXECUTE` on `dbms_aqjms`
* Check `txeventq.queue.name` and `txeventq.queue.schema` values

**Key truncation warnings**
* Kafka keys exceeding 128 bytes are truncated. Redesign keys to be ≤128 bytes or use hash/identifier

**Performance issues**
* Increase `tasks.max` to match Kafka topic partition count
* Adjust `consumer.max.poll.records` in Connect worker configuration
* Monitor database connection pool usage

## Additional Resources

* [Oracle Transactional Event Queues Documentation](https://docs.oracle.com/en/database/oracle/oracle-database/23/adque/Kafka_cient_interface_TEQ.html#GUID-C329D40D-21D6-454E-8B6A-49D96F0C8795)
* [Oracle Database Advanced Queuing](https://docs.oracle.com/en/database/oracle/oracle-database/23/adque/)
* [Apache Kafka Connect Documentation](https://kafka.apache.org/documentation/#connect)

