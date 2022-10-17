# Kafka Java Client for Oracle Transactional Event Queues (Preview v0.8)

## Building the Kafka Java Client for Oracle TxEventQ distribution

This distribution contains Java source code to provide Kafka Java client compatibility to Oracle Transactional Event Queues. Some Kafka Java producer and consumer applications can migrate seamlessly to Oracle Transactional Event Queues for scalable event streaming directly built into the Oracle Database.

You need to have [Gradle 7.3 or above](http://www.gradle.org/installation) and [Java](http://www.oracle.com/technetwork/java/javase/downloads/index.html) installed.

This distribution contains preview version 0.8 of the `Kafka Java Client for Oracle Transactional Event Queues` project. This is tested with [JRE 8u162](https://www.oracle.com/technetwork/java/javase/downloads/jre8-downloads-2133155.html) but we recommend using the latest version.

Java 8u162 or above(recommended is 8U251) should be used for building in order to support both Java 8 and Java 10 at runtime.

The Kafka Java Client works with Oracle Database 20c Cloud Preview (Database Cloud Service), although some features will work with Autonomous Database for testing the simpler producer/consumer examples.

To test this distribution in free Oracle Cloud environment create [Oracle Cloud account](https://docs.cloud.oracle.com/en-us/iaas/Content/FreeTier/freetier.htm) then create [Oracle Autonomous Transaction Processing Database instance](https://docs.oracle.com/en/cloud/paas/autonomous-data-warehouse-cloud/tutorial-getting-started-autonomous-db/index.html) in cloud.   

A database user should be created and should be granted the privileges mentioned in configuration section. Then create a Transactional Event Queue to produce and consume messages.

Finally, build `okafka.jar` and run [Producer.java](./examples/producer/src/main/java/org/oracle/okafka/examples/Producer.java) to produce into Oracle TxEventQ, [Consumer.java](./examples/consumer/src/main/java/org/oracle/okafka/examples/Consumer.java) to consume from Oracle TxEventQ.
 
### Configuration ###

To run `okafka.jar` against Oracle Database, a database user should be created and should be granted below privileges.

```roomsql
create user <username> identified by <password>
grant connect, resource to user
grant execute on dbms_aqadm to use`
grant execute on dbms_aqin to user
grant execute on dbms_aqjms to user
grant select_catalog_role to user
```

Once user is created and above privileges are granted, connect to Oracle Database as this user and create a Transactional Event Queue using below PL/SQL script. For this preview release, upper case Topic/queue names are only allowed. Also this preview supports TxEventQ with only 1 partition hence, in below script `SHARD_NUM` parameter for TxEventQ is set to 1.

```roomsql
begin
    sys.dbms_aqadm.create_sharded_queue(queue_name=>"TxEventQ", multiple_consumers => TRUE); 
    sys.dbms_aqadm.set_queue_parameter('TxEventQ', 'SHARD_NUM', 1);
    sys.dbms_aqadm.set_queue_parameter('TxEventQ', 'STICKY_DEQUEUE', 1);
    sys.dbms_aqadm.set_queue_parameter('TxEventQ', 'KEY_BASED_ENQUEUE', 1);
    sys.dbms_aqadm.start_queue('TxEventQ');
end;
```

#### Connection configuration ####

This project uses JDBC(thin driver) connection to connect to Oracle Database instance using any one of two security protocols.
 
1. PLAINTEXT 
2. SSL.
 
The following properties have to be provided to use these protocols.
 
1. PLAINTEXT: In this protocol a JDBC connection uses user_name and password to connect to Oracle instance. To use PLAINTEXT protocol then user must provide following properties through application.  

        security.protocol = "PLAINTEXT"
        oracle.net.tns_admin = "location of tnsnames.ora file"  (for parsing JDBC connection string)
        oracle.service.name = "name of the service running on the instance"
        oracle.instance.name = "name of the oracle database instance"
        bootstrap.servers  = "host:port"
        
   and following properties in `ojdbc.properties` file and `ojdbc.properties` file should be in location  `oracle.net.tns_admin`.  
   
        user(in lowercase) = "user name of database user" 
        password(in lowercase) = "user password"
   
        
                             
2. SSL: To use SSL secured connections to connect to Autonomous Database  on Oracle Cloud follow these steps.
    * JDBC Thin Driver Connection prerequisites for SSL security: Use JDK8u162 or higher(recommended latest). Use 18.3 JDBC Thin driver or higher(recommended latest)

      * To leverage JDBC SSL security to connect to Oracle Database instance the following properties have to be set.  
        JDBC supports SSL secured connections to Oracle Database in two ways 1. Wallets 2. Java Key Store.
          + Using wallets: 
              - Add the following required dependent jars for using Oracle Wallets in classpath. Download oraclepki.jar, osdt_cert.jar, and osdt_core.jar files along with JDBC thin driver from [JDBC and UCP download page](https://www.oracle.com/database/technologies/appdev/jdbc-downloads.html) and add these jars to classpath.
              - Enable Oracle PKI provider: Enable it statically as follows, Add OraclePKIProvider at the end of file  `java.security` located at `$JRE_HOME/jre/lib/security/java.security`. If SSO wallet i.e cwallet.sso  is used for providing SSL security.
            
                    security.provider.1=sun.security.provider.Sun
                    security.provider.2=sun.security.rsa.SunRsaSign            
                    security.provider.3=com.sun.net.ssl.internal.ssl.Provider
                    security.provider.4=com.sun.crypto.provider.SunJCE
                    security.provider.5=sun.security.jgss.SunProvider
                    security.provider.6=com.sun.security.sasl.Provider
                    security.provider.7=oracle.security.pki.OraclePKIProvider  
                  
                To use ewallet.p12 for SSL security then place OraclePKIProvider before sun provider in file `java.security`.  
              
                    security.provider.1=sun.security.provider.Sun
                    security.provider.2=sun.security.rsa.SunRsaSign
                    security.provider.3=oracle.security.pki.OraclePKIProvider
                    security.provider.4=com.sun.net.ssl.internal.ssl.Provider
                    security.provider.5=com.sun.crypto.provider.SunJCE
                    security.provider.6=sun.security.jgss.SunProvider
                    security.provider.7=com.sun.security.sasl.Provider
              
                Also, it is possible enabling it dynamically by code including Oracle PKI library in project dependencies
                
                ```
                implementation group: 'com.oracle.database.security', name: 'oraclepki', version: '21.5.0.0'
                ```
                
                and the following code in your project.
            
                ```java
                private static void addOraclePKIProvider(){
                    System.out.println("Installing Oracle PKI provider.");
                    Provider oraclePKI = new oracle.security.pki.OraclePKIProvider();
                    Security.insertProviderAt(oraclePKI,3);
                }
                ```
                  
              - Must provide following properties through application.  
            
                     security.protocol = "SSL"
                     oracle.net.tns_admin = "location of tnsnames.ora file"  (for parsing JDBC connection string)
                     tns.alias = "alias of connection string in tnsnames.ora"
                   
                 and following properties in `ojdbc.properties` file and `ojdbc.properties` file should be in location  `oracle.net.tns_admin`
               
                     user(in lowercase) = "name of database user" 
                     password(in lowercase) = "user password"
                     oracle.net.ssl_server_dn_match=true
                     oracle.net.wallet_location="(SOURCE=(METHOD=FILE)(METHOD_DATA=(DIRECTORY=/location../wallet_dbname)))"  
                   
          + using Java Key Store: 
            To Provide JDBC SSL security with Java Key Store then provide following properties through application.  
          
                security.protocol = "SSL"
                oracle.net.tns_admin = "location of tnsnames.ora file"
                tns.alias = "alias of connection string in tnsnames.ora"
          
            and following properties in `ojdbc.properties` file and `ojdbc.properties` file should be in location  `oracle.net.tns_admin`
          
                user(in lowercase) = "user name of database user" 
                password(in lowercase) = "user password"
                oracle.net.ssl_server_dn_match=true
                javax.net.ssl.trustStore==${TNS_ADMIN}/truststore.jks
                javax.net.ssl.trustStorePassword = password
                javax.net.ssl.keyStore= ${TNS_ADMIN}/keystore.jks
                javax.net.ssl.keyStorePassword="password "" 
              
Note: tnsnames.ora file in wallet downloaded from Oracle Autonomous Database contains JDBC connection string which is used for establishing JDBC connection.

Learn more about [JDBC SSL security](https://docs.oracle.com/en/cloud/paas/atp-cloud/atpug/connect-jdbc-thin-wallet.html#GUID-5ED3C08C-1A84-4E5A-B07A-A5114951AA9E) to establish SSL secured JDBC connections.
            
 
### First bootstrap and download the wrapper ###

```
cd okafka_source_dir
gradle wrapper
```

### Building okafka.jar

Simplest way to build the `okafka.jar` file is by using Gradle build tool.
This distribution contains gradle build files which will work for Gradle 7.3 or higher.

To build the `okafka.jar` file which includes all the dependent jar files in itself.

```
./gradlew fullJar 
```
This generates `okafka-0.8-full.jar` in `okafka_source_dir/clients/build/libs`.

To build the `okafka.jar` file without including the dependent jar files in the `okafka.jar` itself.

```
./gradlew jar
```
This generates `okafka-0.8.jar` in `okafka_source_dir/clients/build/libs` and `okafka-0.8-[producer|consumer].jar` in `okafka_source_dir/examples/[producer|consumer]/build/libs`.

**Project Dependency:**

Mandatory jar files for this project to work.

* `ojdbc11-<version>.jar`
* `aqapi-<version>.jar`
* `oraclepki-<version>.jar`
* `osdt_core-<version>.jar`
* `osdt_cert-<version>.jar`
* `javax.jms-api-<version>.jar`
* `jta-<version>.jar`
* `slf4j-api-<version>.jar`

All these jars are downloaded from Maven Repository during gradle build.

If one is using the `okafka-0.8-full.jar` file generated using `./gradlew fullJar` command, then it is not required to add other jar files in the classpath while running the Oracle Kafka application.

## Using the okafka-0.8-full.jar

This section describes the sample Producer and Consumer application that uses `okafka-0.8-full.jar` file. These files are available in the examples directory.  Assuming user has built the `okafka-0.8-full.jar` file using the `./gradlew fullJar` build command, so that no other jar file is required to be placed in the classpath.

To compile `Producer.java` you can use java, for example: 

```java
javac -classpath  .:okafka-0.8-full.jar Producer.java
```

or gradle

```bash
gradle :examples:producer:build -x test 
```

To run `Producer.java` 

```java
java -classpath  .:okafka-0.8-full.jar Producer
```

or using gradle

```bash
gradle :examples:producer:run 
```

To compile `Consumer.java`

```java
javac -classpath  .:okafka-0.8-full.jar Consumer.java
```

or gradle

```bash
gradle :examples:consumer:build -x test 
```

To run `Consumer.java`

```
java -classpath  .:okafka-0.8-full.jar Consumer 10
```

or gradle

```bash
gradle :examples:consumer:run
```

## Build javadoc

This command generates javadoc in `okafka_source_dir/clients/build/docs/javadoc`.

```
./gradlew javadoc
```
    
Bellow, there is a sample code for the Producer and one for the Consumer. Both are available in examples folder, to use fill the properties externalized on application.properties to point to your Oracle Database. 

**Producer.java - A simple Producer application that uses okafka.jar**

```java
import java.util.Properties;
import org.oracle.okafka.clients.producer.*;

public class Producer {

public static void main(String[] args) {

   KafkaProducer<String,String> prod = null;
   int msgCnt =10;
   Properties props = new Properties();

   /* change the properties to point to the Oracle Database */
   props.put("oracle.service.name", "serviceid.regress.rdbms.dev.us.oracle.com"); //name of the service running on the instance                     
   props.put("oracle.instance.name", "instancename"); //name of the Oracle Database instance
   props.put("oracle.net.tns_admin", "location of tnsnames.ora/ojdbc.properties file");	//eg: "/user/home" if ojdbc.properies file is in home
   props.put("bootstrap.servers", "host:port"); //ip address or host name where instance running : port where instance listener running
   props.put("linger.ms", 1000);
   props.put("key.serializer", "org.oracle.okafka.common.serialization.StringSerializer");     
   props.put("value.serializer", "org.oracle.okafka.common.serialization.StringSerializer");

   prod=new KafkaProducer<String, String>(props);
   try {
       System.out.println("Producing messages " + msgCnt);
       for(int j=0;j < msgCnt; j++) {
        prod.send(new ProducerRecord<String, String>("TOPIC1" , "Key","This is new message"+j));
    }

   System.out.println("Messages sent " );

   } catch(Exception ex) {
        ex.printStackTrace();
   } finally {
      prod.close();
   }
  }
}
```

**Consumer.java - A simple Consumer application that uses okafka.jar**

```java
import java.util.Properties;
import java.time.Duration;
import java.util.Arrays;

import org.oracle.okafka.clients.consumer.*;

public class Consumer {

public static void main(String[] args) {
   Properties props = new Properties();

   /*change the bootstrap server to point to the Oracle Database*/
   props.put("oracle.service.name", "serviceid.regress.rdbms.dev.us.oracle.com"); //name of the service running on the instance
   props.put("oracle.instance.name", "instancename"); //name of Oracle Database instance
   props.put("oracle.net.tns_admin", "location of tnsnames.ora/ojdbc.properties file"); //eg: "/user/home" if ojdbc.properies file is in home
   props.put("bootstrap.servers", "host:port"); //ip address or host name where instance running : port where instance listener running
   props.put("group.id", "subscriber");
   props.put("enable.auto.commit", "false");
   props.put("key.deserializer",  "org.oracle.okafka.common.serialization.StringDeserializer");
   props.put("value.deserializer", "org.oracle.okafka.common.serialization.StringDeserializer");
   props.put("max.poll.records", 500);
   
   KafkaConsumer<String, String> consumer = null;

   try {
       consumer = new KafkaConsumer<String, String>(props);
       consumer.subscribe(Arrays.asList("TOPIC1"));
       ConsumerRecords<String, String> records;

       records = consumer.poll(Duration.ofMillis(1000));

              for (ConsumerRecord<String, String> record : records)
             {
                 System.out.println("topic = , key = , value = \n" +
                 record.topic()+ "\t" + record.key()+ "\t" + record.value());
             }
             consumer.commitSync();
        } catch(Exception ex) {
          ex.printStackTrace();
       }  finally {
           consumer.close();
       }
   }
}
```
## Kafka Java Client APIs supported

### KafkaProducer APIs supported

* `KafkaProducer`: Constructor that creates a producer object and internal AQ JMS objects. KafkaProducer class has 4 types of constructor defined which all take configuration parameters as input.

* `send(ProducerRecord)`: Produces a message into Oracle Transactional Event Queue (Oracle TxEventQ). A message is called  'Producer Record' for Kafka application and called 'Event' for Oracle TxEventQ. Both the overloaded versions of send, that is, `send(ProducerRecord)` and `send(ProducerRecord, Callback)` will be supported. Records will be published into the topic using AQJMS.

* `close`: Closes the producer, its sender thread and frees the accumulator. It also closes internal AQJMS objects like connection, session JMS producer etc.

* `ProducerRecord`: Class that represents a message in Kafka platform. It will be translated into an 'event' for Oracle TxEventQ Platform which is an AQJMS Message. Relevant fields like payload and key can be directly translated into Oracle TxEventQ payload and message key for Oracle TxEventQ.

* `RecordMetadata`: This class contains metadata of the record like offset, timestamp etc. of the Record in Kafka platform. This will be assigned value relevant for Oracle TxEventQ.  An event id of Oracle TxEventQ will be converted into an offset of RecordMetadata.

* `Callback Interface`: A callback function which will be executed once a Record is successfully published into Oracle TxEventQ Event Stream.

* `Partitioner Interface`: An Interface which maps a key of the message to a partition number of the topic. A partition number is analogous to a Event Stream Id of Oracle TxEventQ. Application developer can implement their own Partitioner interface to map messages to a partition. The partition of a topic is analogous to a 'Event Stream' of Oracle TxEventQ. Thus a message is published into the assigned Event Stream of Oracle TxEventQ.

* `Property: bootstrap.servers`: IP address and port of a machine where database instance is running. 

* `Property: Key Serializer and Value serializer`: Converts key and payload into byte array respectively.

* `Property: acks`: For this project, only value relevant for acks property is 'all'. Any other field set by the user is ignored.

* `Property: linger.ms`: Time in milliseconds for which sender thread will wait before publishing the records in Oracle TxEventQ.

*  `Property: batch.size`: Size of accumulator buffer in bytes for which sender thread will wait before publishing records in  Oracle TxEventQ.

* `Property: buffer.memory`: Total memory in bytes the accumulator can hold.

* `Property: max.block.ms`: If buffer.memory size is full in accumulator then wait for max.block.ms amount of time before send() method can receive out of memory error.

* `Property: retries` : This property enables producer to resend the record in case of transient errors. This value is an upper limit on how many resends.
* `Property: retry.backoff.ms` : The amount of time to wait before attempting to retry a failed request to a given topic partition. This avoids repeatedly sending requests in a tight loop under some failure scenarios.

### KafkaConsumer APIs supported

* `KafkaConsumer`: Constructor that creates a consumer that allows application to consume messages from a queue of Oracle TxEventQ. Internal to client, Oracle AQJMS objects will be created which will not be visible to client application. All variations of the KafkaConsumer constructor are supported in this version.

* `Subscribe(java.util.Collection<String>)`: This method takes a list of topics to subscribe to. In this version only the first topic of the list will be subscribed to. An exception will be thrown if size of list is greater than one. This method will create a durable subscriber on Transactional Event Queue at Oracle TxEventQ server side with Group-Id as subscriber name.

* `Poll(java.time.Duration)`: Poll attempts to dequeue messages from the Oracle TxEventQ for the subscriber. It dequeues a batch of messages from the Oracle TxEventQ within a timeout provided as argument in poll. Size of the batch depends on the parameter max.poll.records set by the Kafka client application. In this preview release, when poll is invoked for the first time, Oracle TxEventQ assigns a single available partition to this consumer. This assignment will stay for the lifetime of the consumer and no other partition is assigned to this consumer. Each poll to this consumer returns messages belonging to the partition assigned to this consumer. It is the responsibility of the application developer to start as many consumers as number of partitions of the queue. If number of consumers are less than number of partitions then messages from unassigned partitions will never be consumed. If number of consumers are more than number of partitions then extra consumers will not be assigned any partition and hence will not be able to consume any messages. No two consumer applications will consume from same partition at the same time.

* `commitSync()`: Commit all consumed messages. Commit to an offset is not supported in this version. This call will directly call commit on database which will commit all consumed messages from Oracle TxEventQ. `Kafka Java Client for Oracle Transactional Event Queues` maintains only a single session for a connection. And this session is transactional, calling commit on session() either succeeds or rolls back. So commit is not retried in case of commit failure. 

* `commitSync(java.time.Duration)`: Commit all consumed messages. Commit to an offset is not supported in this preview release. This call will directly call commit on database which will commit all consumed messages from Oracle TxEventQ. `Kafka Java Client for Oracle Transactional Event Queues` maintains only a single session for a connection. And this session is transactional, calling commit on session() either succeeds or rolls back. commit is not retried in case of commit failure.

* `commitAsync()`: This call is translated into `commitSync` internally.  

* `commitAsync(OffsetCommitCallback)`: This call is translated into `commitSync`.  A callback function passed as argument will get executed once the commit is successful.

* `Unsubscribe`: Unsubscribes the topic it has subscribed to. Unsubscribed consumer can no longer consume messages from unsubscribed topics. Other consumer applications in same group can still continue to consume.

* `seekToBeginning(Collection<TopicPartition>)`: Seek to first available offset .

* `seek(TopicPartition, long)`: Seek to offset for a given topic partition. 

* `seekToEnd(Collection<TopicPartition>)`: Seek to last offset for a given list of topic partitions.

* `close()`: closes the consumer i.e close the connection, release resources.

* `ConsumerRecord`: A class representing a consumed record in Kafka Platform. In this implementation, AQJMS Message will be converted into ConsumerRecord.

* `Property: bootstrap.servers`: IP address and port of a machine where database instance is running. 

* `Property: key.deserializer and value.deserialzer`: In Oracle TxEventQ's queue key, value are stored as byte array in user property, payload of JMS message respectively. On consuming these byte arrays are deserialized into key, value having user provided format internally by the consumer using `key.deserializer` and `value.deserializer` respectively. 

* `Property: group.id:` This is a Consumer Group name for which messages are consumed from the Kafka topic. This property will be used as a durable subscriber name for Oracle TxEventQ's queue.

* `Property: max.poll.records`: Max number of records to fetch from Oracle TxEventQ server in a single poll call.

* `Property: fetch.max.wait.ms`: Maximum amount of time in milliseconds to wait for fetching messages if not available.

* `Property: enable.auto.commit`: Enables  auto commit of consumed messages for every specified interval.

* `Property: auto.commit.interval.ms`: Interval in milliseconds for auto commit of messages.

### Kafka Admin APIs supported

* `create(props) & create(config)`: Create an object of KafkaAdmin class which uses passed parameters. For this implementation, we will create a database session which will be used for further operations. Client application should provide Oracle specific properties which are `bootstrap.servers`,  `oracle.servicename`, `oracle.instancename`, `user`, `password` that will be used to setup the database connection.

* `close`: Closes database session.

* `createTopic(Collection<NewTopic>, CreateTopicsOptions)`: Create an Oracle TxEventQ with initial partition count (or Event Streams count) passed by the application as an argument into the function. This method is not supported in preview release.

* `deleteTopic(Collection<String>, DeleteTopicsOptions)`: Stop and Drop Oracle TxEventQ queue.

* `Property: bootstrap.servers`: IP address and port of a machine where database instance is running. 

* `Property: retention.ms`: Amount of time in milliseconds a message is retained in queue after all consumer groups or subscribers dequeued a message. This is a topic level config.
