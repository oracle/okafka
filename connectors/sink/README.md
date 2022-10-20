# Kafka Connectors for TxEventQ

Repository for the Kafka Sink Connector for Oracle Transactional Event Queues application.
The repository contains an application for a Sink Connector that reads from Kafka and stores into Oracle's TxEventQ.

## Getting started

To use the application Kafka will need to be downloaded and installed on a server. Refer to [Kafka Apache](https://kafka.apache.org/) for
information on how to start Kafka. Refer to this [Confluent](https://docs.confluent.io/platform/current/kafka/authentication_ssl.html#crep-full) page
for information on how to setup SSL connection for Kafka.

The Kafka Sink Connector requires a minimum Oracle Database version of 21c in order to create a Transactional Event Queue. 

Clone the project from the repository. Open a bash window and change the directory to the location where the cloned project has been saved.
Run the following command from the bash window to compile the source.

```bash
mvn clean package
```

You will need to grab the following jar files from the \target\libs directory after performing the build command above and place them into Kafka's libs directory.

- ojdbc8-21.5.0.0.jar
- oraclepki-21.5.0.0.jar
- osdt_core-21.5.0.0.jar
- osdt_cert-21.5.0.0.jar

### Oracle Database Setup
To run the Kafka Sink Connector against Oracle Database, a database user should be created and should be granted the below privileges.

```roomsql
create user <username> identified by <password>
grant connect, resource to user
grant execute on dbms_aqadm to use`
grant execute on dbms_aqin to user
grant execute on dbms_aqjms to user
grant select_catalog_role to user
```

Once user is created and above privileges are granted, connect to Oracle Database as this user and create a Transactional Event Queue using below PL/SQL script.
In the below script `SHARD_NUM` parameter for TxEventQ is set to 1, but this value should be modified to be less than or equal to the number of Kafaka partitions
assigned to the Kafka topic that the Sink Connector will be consuming from.

```roomsql
begin
    sys.dbms_aqadm.create_sharded_queue(queue_name=>"TxEventQ", multiple_consumers => TRUE); 
    sys.dbms_aqadm.set_queue_parameter('TxEventQ', 'SHARD_NUM', 1);
    sys.dbms_aqadm.set_queue_parameter('TxEventQ', 'STICKY_DEQUEUE', 1);
    sys.dbms_aqadm.set_queue_parameter('TxEventQ', 'KEY_BASED_ENQUEUE', 2);
    sys.dbms_aqadm.start_queue('TxEventQ');
    sys.dbms_aqadm.add_subscriber('TxEventQ', SYS.AQ$_AGENT('SUB1', NULL, 0)) ;
end;
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

### Setup the Connection Properties

Copy the example properties file below into a text editor and update all the required fields as noted below and save the properties file as any file name, for example `connect-txeventq-sink.properties`
and place the properites file in the Kafka config directory.

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

# List of Kafka brokers used for bootstrapping
# format: host1:port1,host2:port2 ...
# Note: This property will need to be updated before the Sink Connector can connect.
bootstrap.servers=<broker i.e localhost:9092>

# The name of the type of TxEvent queue.
# For example, it can be "RAW", "SYS.ANYDATA" or "SCOTT.MY_OBJECT_TYPE".
# Types: ANYDATA, RAW, STRUCT, XMLType, JSON
txeventq.queue.type=RAW

# Converter class used to convert between Kafka Connect format and the serialized form that is written to Kafka.
# This controls the format of the keys in messages written to or read from Kafka, and since this is independent
# of connectors it allows any connector to work with any serialization format.
key.converter=org.apache.kafka.connect.storage.StringConverter

# Converter class used to convert between Kafka Connect format and the serialized form that is written to Kafka.
# This controls the format of the values in messages written to or read from Kafka, and since this is independent
# of connectors it allows any connector to work with any serialization format.
value.converter=org.apache.kafka.connect.storage.StringConverter

```

### Running TxnEventQ Kafka connect sink connector

Update Kafka's `connect-standalone.properties` or `connect-distributed.properties` configuration file located in Kafka's config directory `plugin.path=` property with the 
directory path to where the jar file for the Sink Connector is located.

In the Kafaka's config directory locate and open the zookeeper.properties file and update th dataDir property with the directory path where you installed Kafka.
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
The command below is connecting in standalone mode. If connecting is distributed mode replace the bat file with the connect-distributed.bat file.

```bash
.\bin\windows\connect-standalone.bat .\config\connect-standalone.properties .\config\connect-txeventq-sink.properties
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
The command below is connecting in standalone mode. If connecting is distributed mode replace the bat file with the connect-distributed.sh file.

```bash
bin/connect-standalone.sh config/connect-standalone.properties config/connect-TxEventQ-sink.properties 
```
