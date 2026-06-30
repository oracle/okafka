# Kafka To OKafka Conversion Guide

## Conversion Scope

This guide is for converting Java applications from Apache Kafka clients to Oracle OKafka clients backed by Oracle Database Transactional Event Queues. The input may be a service, worker, command-line tool, Spring application, benchmark, sample, or test. Test conversion is a special case; do not make the whole migration test-specific.

For every application conversion, inspect and update these places:

- Java imports and constructors for producer, consumer, and admin clients.
- Dependency/build files such as Gradle, Maven, shaded jars, Docker images, and startup scripts.
- Runtime property files, environment variables, config classes, and secret/wallet handling.
- Framework wiring such as Spring Kafka factories, listener containers, and admin beans.
- Topic/group naming assumptions, topic creation, offset handling, and transaction flows.
- Unsupported Kafka APIs that must be removed, redesigned, or guarded.
- Operational docs or README steps needed to connect to Oracle Database TEQ.

This guide is self-contained. Use the rules in this document as the conversion baseline for a normal Kafka-to-OKafka migration.

If conversion becomes ambiguous or the application uses behavior not covered here, use these fallback references only for that unclear area:

- Oracle OKafka source: https://github.com/oracle/okafka
- Apache Kafka 3.9 source: https://github.com/apache/kafka/tree/3.9

The conversion target is an application that constructs OKafka producer, consumer, and admin clients while continuing to use many Apache Kafka interfaces and data model classes. The main migration work is changing concrete client construction, replacing Kafka broker connection settings with Oracle Database TEQ settings, and redesigning unsupported Kafka broker-only APIs.

## Self-Contained OKafka Baseline

Use these baseline facts during conversion:

- OKafka connects to Oracle Database Transactional Event Queues through JDBC.
- `bootstrap.servers` is the Oracle database host and listener port, for example `db-host:1521`.
- PLAINTEXT mode requires `security.protocol=PLAINTEXT`, `oracle.service.name`, and `oracle.net.tns_admin`.
- SSL/wallet mode requires `security.protocol=SSL`, `oracle.net.tns_admin`, and `tns.alias`.
- `oracle.net.tns_admin` is always a directory, not a file path.
- For PLAINTEXT, the `oracle.net.tns_admin` directory must contain `ojdbc.properties` with `user` and `password`.
- For wallet/SSL, the `oracle.net.tns_admin` directory is the wallet/TNS directory and usually contains `tnsnames.ora` plus wallet files.
- OKafka active client classes live under `org.oracle.okafka.clients.*`.
- Apache Kafka interfaces and model types are still commonly used by OKafka APIs.
- Consumers should subscribe to one topic at a time.
- Pattern subscription and manual partition assignment should be redesigned.
- Kafka broker transaction assumptions do not map one-to-one to OKafka.
- OKafka transactional producer mode uses `oracle.transactional.producer=true`.
- Oracle Database exposes committed records to consumers by default; do not add Kafka `isolation.level=read_committed` just to make OKafka consumers see committed data.
- Admin, topic, offset, group, producer, and consumer operations are real Oracle Database operations and may need database privileges.
- Topic names often need to be uppercase or treated case-carefully.

Minimum dependency expectation for converted applications:

- OKafka client artifact.
- Apache Kafka client API classes used by OKafka-compatible interfaces and models.
- Oracle JDBC driver.
- Oracle AQ/JMS dependencies required by the OKafka distribution.
- Oracle wallet/security dependencies when SSL/wallet mode is used.
- Logging dependency such as SLF4J binding as required by the application.

## Package And Class Mapping

Use OKafka facade classes for the active clients:

| Kafka usage | OKafka replacement |
| --- | --- |
| `org.apache.kafka.clients.producer.KafkaProducer` | `org.oracle.okafka.clients.producer.KafkaProducer` |
| `org.apache.kafka.clients.consumer.KafkaConsumer` | `org.oracle.okafka.clients.consumer.KafkaConsumer` |
| `org.apache.kafka.clients.admin.AdminClient` | `org.oracle.okafka.clients.admin.AdminClient` |
| `org.apache.kafka.clients.admin.KafkaAdminClient` | Prefer `org.oracle.okafka.clients.admin.AdminClient.create(...)`; use OKafka `KafkaAdminClient` internals only if code already requires concrete internals. |

Usually keep these Apache Kafka types because OKafka APIs intentionally interoperate with them:

- `org.apache.kafka.clients.producer.Producer`
- `org.apache.kafka.clients.consumer.Consumer`
- `org.apache.kafka.clients.admin.Admin`
- `ProducerRecord`, `RecordMetadata`, `Callback`
- `ConsumerRecord`, `ConsumerRecords`, `OffsetAndMetadata`, `OffsetAndTimestamp`
- `NewTopic`, `NewPartitions`, `TopicDescription`, `TopicCollection`, admin result/spec classes
- `TopicPartition`, `PartitionInfo`, `Metric`, `MetricName`, serializers, deserializers

Prefer programming to Kafka interfaces where the application already does so:

```java
import org.apache.kafka.clients.producer.Producer;
import org.oracle.okafka.clients.producer.KafkaProducer;

Producer<String, String> producer = new KafkaProducer<>(props);
```

This minimizes the surface area changed during migration.

## Where To Change A Kafka Application

Search the application for these patterns before editing:

```text
org.apache.kafka.clients.producer.KafkaProducer
org.apache.kafka.clients.consumer.KafkaConsumer
org.apache.kafka.clients.admin.AdminClient
org.apache.kafka.clients.admin.KafkaAdminClient
ProducerConfig
ConsumerConfig
AdminClientConfig
bootstrap.servers
security.protocol
transactional.id
enable.idempotence
subscribe(
assign(
subscribe(Pattern
sendOffsetsToTransaction
createTopics
describeCluster
describeConfigs
alterConfigs
pause(
resume(
wakeup(
clientInstanceId
```

Then classify the code:

| Code shape | Migration action |
| --- | --- |
| Direct `new KafkaProducer<>(props)` | Replace concrete import with OKafka `KafkaProducer`; keep `Producer` interface if used. |
| Direct `new KafkaConsumer<>(props)` | Replace concrete import with OKafka `KafkaConsumer`; review subscription and offset behavior. |
| `AdminClient.create(props)` | Replace factory import with `org.oracle.okafka.clients.admin.AdminClient`; keep Kafka `Admin` interface where possible. |
| Shared Kafka types | Usually keep Apache Kafka `ProducerRecord`, `ConsumerRecord`, `TopicPartition`, `NewTopic`, `RecordMetadata`, serializers, deserializers, futures, and admin result/spec classes. |
| Framework factories | Change the concrete client factory and config source, not only imports in business code. |
| Main/service startup | Add Oracle DB config and wallet/credential resolution to the app's normal config path. |
| Tests | Convert to the project's test framework only if the input is test code. |

## Configuration Mapping

Kafka broker-style config:

```properties
bootstrap.servers=broker1:9092
```

OKafka PLAINTEXT DB config:

```properties
security.protocol=PLAINTEXT
bootstrap.servers=db-host:1521
oracle.service.name=service_or_pdb_name
oracle.net.tns_admin=/path/to/config-directory
```

The `oracle.net.tns_admin` value is a directory, not the `ojdbc.properties` file itself. For PLAINTEXT it should contain:

```properties
user=<db_user>
password=<db_password>
```

OKafka SSL/wallet config:

```properties
security.protocol=SSL
oracle.net.tns_admin=/path/to/wallet-directory
tns.alias=<tns_alias>
```

The wallet directory usually contains `tnsnames.ora`, wallet files such as `cwallet.sso` or `ewallet.p12`, and optionally `ojdbc.properties`.

Keep app-level Kafka configs only after checking OKafka support:

- Keep serializers/deserializers.
- Keep `group.id` for consumers.
- Keep supported consumer settings such as `auto.offset.reset` and `max.poll.records`.
- Review transaction, idempotence, partition assignment, rebalance, metrics, and timeout configs against OKafka implementation.

When converting sample runners or tests, split config into two groups:

| Config kind | Examples | Pass to OKafka client? |
| --- | --- | --- |
| Client config | `bootstrap.servers`, `security.protocol`, `oracle.service.name`, `oracle.net.tns_admin`, `group.id`, serializers/deserializers | Yes |
| Runner/test config | `test.topic`, `poll.timeout.ms`, fixed message counts | No |

If the source app has a helper like `KafkaTestConfig`, convert it so `clientProperties()` removes runner-only keys before constructing OKafka producer, consumer, or admin clients.

For PLAINTEXT, validate early that `oracle.net.tns_admin` points to an existing directory and contains `ojdbc.properties` with nonblank `user` and `password`. This gives a clearer app error than a later Oracle login failure.

For Gradle/Maven test projects, prefer:

```text
src/test/resources/test.config
src/test/resources/ojdbc.properties
```

Use `oracle.net.tns_admin=./src/test/resources` only as a repo-local default; preserve any absolute/custom user path.

## Application Runtime Setup

For normal applications, the converted code should get OKafka settings through the same configuration mechanism the application already uses: properties files, environment variables, Spring configuration, command-line args, or secrets manager. Do not bake developer-machine paths into reusable application files.

Minimum PLAINTEXT runtime properties:

```properties
security.protocol=PLAINTEXT
bootstrap.servers=<db-host>:<db-port>
oracle.service.name=<database-service-or-pdb>
oracle.net.tns_admin=<directory-containing-ojdbc.properties>
```

The `oracle.net.tns_admin` value is a directory. For PLAINTEXT, that directory should contain:

```properties
user=<db_user>
password=<db_password>
```

Minimum wallet/SSL runtime properties:

```properties
security.protocol=SSL
oracle.net.tns_admin=<wallet-directory>
tns.alias=<alias_from_tnsnames>
```

The wallet directory usually contains `tnsnames.ora`, wallet files, and any required JDBC properties. Preserve absolute wallet paths or environment-specific paths supplied by the user.

For reusable GitHub code:

- Commit examples such as `application-okafka-example.properties`, not real credentials.
- Document required Oracle Database TEQ privileges and topic setup.
- Allow config override from environment variables or command-line properties.
- Validate that `oracle.net.tns_admin` exists and is a directory before creating clients.
- Keep application-only keys out of the `Properties` object passed to OKafka if OKafka does not define them.
- Close producers, consumers, and admin clients to release JDBC/database resources.

For Spring-style apps, migrate the factory configuration rather than only business classes:

```java
props.put("bootstrap.servers", dbHostPort);
props.put("oracle.service.name", serviceName);
props.put("oracle.net.tns_admin", tnsAdminDir);
props.put("security.protocol", securityProtocol);
props.put("key.serializer", StringSerializer.class.getName());
props.put("value.serializer", StringSerializer.class.getName());

Producer<String, String> producer =
        new org.oracle.okafka.clients.producer.KafkaProducer<>(props);
```

Keep the same principle for consumers and admin clients.

## Making Converted Tests Runnable

This section applies only when the input is test code. When converting Kafka tests, do not stop after changing imports. Make the converted test executable through the project's normal test framework.

For normal applications, prefer the application runtime setup above instead of forcing test-resource layout.


Prefer this layout for Gradle Java projects:

```text
src/test/java/.../ConvertedOkafkaTest.java
src/test/resources/test.config
src/test/resources/ojdbc.properties
```

The Java test should be under the package scanned by the Gradle test task. If the project separates unit tests and integration tests, place DB-backed OKafka tests under the integration-test package or source set used by that task.

Do not keep runtime config under `src/test/java`. Files under `src/test/resources` are copied to the test runtime classpath, so the same test can run from Gradle, VS Code, IntelliJ, or another IDE. Java source folders are for compiled `.java` files; non-Java config files there are easier to miss and often depend on the current working directory.

For a generic GitHub-ready test setup:

- Commit a `test.config` with non-secret defaults or placeholder values.
- Commit an `ojdbc.properties` template only if the repository policy allows it, and keep credentials as placeholders.
- Document that every user must edit these files for their Oracle Database or wallet.
- Use classpath/resource loading when possible, so tests do not depend on being launched from one exact working directory.
- If the project supports custom external config, allow absolute paths for wallet/TNS directories and do not rewrite them.

Example PLAINTEXT test config:

```properties
security.protocol=PLAINTEXT
bootstrap.servers=localhost:1521
oracle.service.name=FREEPDB1
oracle.net.tns_admin=./src/test/resources
```

Example `ojdbc.properties`:

```properties
user=<db_user>
password=<db_password>
```

For wallet-based environments, `oracle.net.tns_admin` should point to the wallet directory and the config should include the TNS alias expected by the OKafka client:

```properties
security.protocol=SSL
oracle.net.tns_admin=/absolute/path/to/wallet
tns.alias=<alias_from_tnsnames>
```

Converted tests should use unique database objects:

```java
String topic = OkafkaTestSupport.uniqueTopic("TEQ_MY_TEST");
String groupId = OkafkaTestSupport.uniqueGroup("G_MY_TEST");
```

This avoids collisions between repeated local runs, IDE runs, Gradle runs, and CI jobs.

Use the project's existing support helper if one exists. If no helper exists, create one small test helper for loading config, creating admin/producer/consumer clients, generating unique topic/group names, waiting on futures with timeouts, and cleaning up topics.

For Gradle, document commands like:

```powershell
.\gradlew.bat :clients:testClasses --console=plain
.\gradlew.bat :clients:integrationTest --tests org.oracle.okafka.tests.MyConvertedTest --rerun-tasks --console=plain
.\gradlew.bat :clients:integrationTest --rerun-tasks --console=plain
.\gradlew.bat :clients:runIntegrationTestSuite --console=plain
```

For IDEs, ensure the converted test is a normal JUnit/TestNG test class, not only a `main` method. The IDE should be able to run the test method directly after importing the Gradle project and after the user edits the resource config files.

## Producer Conversion

Before:

```java
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
```

After:

```java
import org.apache.kafka.clients.producer.Producer;
import org.oracle.okafka.clients.producer.KafkaProducer;
```

Keep `ProducerRecord` from Apache Kafka.

Watch for unsupported or Oracle-specific behavior:

- `clientInstanceId(Duration)` is unsupported.
- Kafka `transactional.id` is not the same migration knob as OKafka's Oracle transaction mode.
- OKafka transactional producer code depends on `oracle.transactional.producer=true` and Oracle-specific transaction behavior.
- `sendOffsetsToTransaction(...)` overloads should be treated as unsupported for conversion.
- Topic names may be uppercased internally.

Kafka producer configs often use constants:

```java
properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers());
properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
```

OKafka code can keep string serializer class names, but DB connection settings usually come from the OKafka config file/helper:

```java
Properties producerProperties = config.clientProperties();
producerProperties.put("key.serializer", StringSerializer.class.getName());
producerProperties.put("value.serializer", StringSerializer.class.getName());
```

Keep `send(...).get()` and `flush()` semantics for smoke tests when the original code waits for delivery.

## Consumer Conversion

Before:

```java
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.Consumer;
```

After:

```java
import org.apache.kafka.clients.consumer.Consumer;
import org.oracle.okafka.clients.consumer.KafkaConsumer;
```

Check these differences:

- Subscribe to exactly one topic.
- Pattern subscription is unsupported.
- Manual `assign` is unsupported.
- Offset-map commit overloads are unsupported.
- `pause`, `resume`, `paused`, `enforceRebalance`, and `clientInstanceId` may be unsupported depending on the current codebase version.
- `wakeup()` and `seek(TopicPartition, OffsetAndMetadata)` should be treated as unsupported for conversion.
- Kafka `isolation.level` settings should be reviewed and usually removed during OKafka conversion because Oracle Database does not use that Kafka broker isolation setting to decide consumer visibility; committed records are visible by default.
- Treat `currentLag` as usable only after subscription/assignment and after the application has a clear expected end-offset/committed-offset state.
- `poll(Duration)` requires a subscription and may trigger metadata/group/DB work before fetch.

Kafka code that relies on manual assignment should be redesigned around `subscribe(Collections.singletonList(topic))` and poll until assignment is visible.

Kafka code that subscribes to multiple topics should be redesigned into one consumer per topic, separate processing loops, or an application-level fan-in pattern.

For count/round-trip runners, keep bounded polling loops with explicit empty-poll limits or deadlines. OKafka poll can involve database metadata, group, and fetch work.

## Admin Conversion

Prefer the OKafka factory:

```java
import org.apache.kafka.clients.admin.Admin;
import org.oracle.okafka.clients.admin.AdminClient;

Admin admin = AdminClient.create(props);
```

Supported or commonly implemented flows include:

- create/delete topics
- create partitions
- list topics
- describe topics
- list offsets
- list/delete consumer groups
- list consumer group offsets

Many newer Kafka Admin APIs may throw `FeatureNotSupportedException`. Do not assume every Apache Kafka Admin API exists with identical behavior.

Kafka admin code often uses:

```java
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;

Properties props = new Properties();
props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
AdminClient admin = AdminClient.create(props);
```

OKafka should become:

```java
import org.apache.kafka.clients.admin.Admin;
import org.oracle.okafka.clients.admin.AdminClient;

Admin admin = AdminClient.create(okafkaClientProperties);
```

Keep Apache Kafka admin request/result model types where supported:

- `NewTopic`
- `NewPartitions`
- `ListOffsetsResult`
- `OffsetSpec`
- `TopicDescription`
- `TopicCollection.TopicNameCollection`
- `TopicCollection.TopicIdCollection`

Admin operations are DB-backed and can be slower or privilege-sensitive. Wrap test cleanup defensively and use timeouts/futures consistently.

## API Compatibility Checklist

OKafka intentionally reuses many Apache Kafka interfaces and model types, but behavior is not full Kafka broker parity.

| Area | Usually supported / keep | Must review or rewrite |
| --- | --- | --- |
| Producer basics | `send`, callback, `flush`, `close`, `partitionsFor`, metrics | `clientInstanceId`, Kafka transaction offset handoff, broker-specific tuning assumptions |
| Producer transactions | OKafka `initTransactions`, `beginTransaction`, `commitTransaction`, `abortTransaction`, `getDBConnection` when configured for Oracle transactions | Kafka `transactional.id`-centric designs and `sendOffsetsToTransaction` |
| Consumer basics | `subscribe(Collection)` with one topic, `poll`, `commitSync`, `commitAsync`, `seek(long)`, `seekToBeginning`, `seekToEnd`, `position`, committed offsets, beginning/end offsets, `offsetsForTimes`, `partitionsFor`, `currentLag` | Multi-topic subscription, pattern subscription, manual `assign`, offset-map commit overloads, `pause`, `resume`, `paused`, `wakeup`, `enforceRebalance`, `clientInstanceId`, Kafka `isolation.level` assumptions |
| Admin basics | create/delete/list/describe topics, create partitions, list offsets, list/delete consumer groups, list group offsets | ACLs, config mutation/description, broker log dirs, record deletion, delegation tokens, quotas, SCRAM, feature updates, KRaft/quorum APIs, producer/transaction admin APIs |
| Config | serializers/deserializers, `group.id`, `auto.offset.reset`, `max.poll.records`, `bootstrap.servers` as DB host/port, `security.protocol`, Oracle service/TNS settings | Kafka broker security/SASL config, multi-broker bootstrap assumptions, Kafka-only topic/log/cluster configs |
| Topic names | Kafka model types still compile | OKafka/TEQ often expects uppercase topic names; avoid case-sensitive Kafka assumptions |

## Offset And Metadata Patterns

Kafka runners commonly exercise:

- `AdminClient.listOffsets`
- `consumer.beginningOffsets`
- `consumer.endOffsets`
- `consumer.offsetsForTimes`
- `consumer.position`
- `consumer.committed`
- `consumer.currentLag`
- `consumer.partitionsFor`

For OKafka:

- Keep `TopicPartition`, `OffsetSpec`, `OffsetAndTimestamp`, and `OffsetAndMetadata` imports from Apache Kafka.
- Ensure the topic exists in Oracle TEQ before offset queries.
- Use a subscribed consumer where OKafka requires subscription/assignment.
- Do not assume Kafka broker parity for offset behavior; use bounded, observable assertions based on produced records and committed offsets.
- Expect timestamp and offset behavior to depend on Oracle TEQ message metadata and DB state.
- When converting `offsetsForTimes` tests, do not blindly preserve Kafka assertions based on timestamps calculated between local `send()` calls. Local `System.currentTimeMillis()` values are not guaranteed to match the Oracle TEQ message timestamp boundaries used by OKafka. Prefer stable assertions that verify non-null results for known produced records, null results for future timestamps when appropriate, and monotonic offset behavior instead of exact Kafka broker boundary assumptions.

## Migration Checklist

1. Replace concrete producer/consumer/admin client imports.
2. Keep Kafka interfaces and shared data types where compatible.
3. Update build/dependency/framework factory wiring so the app constructs OKafka clients.
4. Replace broker connection config with Oracle DB/TEQ config.
5. Remove application-only or runner-only keys before passing `Properties` to OKafka clients.
6. Provide `ojdbc.properties` or wallet/TNS files under `oracle.net.tns_admin`.
7. Validate PLAINTEXT credentials or wallet directory early in application startup.
8. Remove or rewrite unsupported APIs.
9. Check one-topic consumer subscription assumptions.
10. Check topic case assumptions.
11. Review transactional producer code for OKafka's Oracle-specific transaction mode.
12. Compile.
13. Run admin smoke: create/list/delete topic.
14. Run producer smoke: send and flush.
15. Run consumer smoke: subscribe, poll, commit.
16. Run offset/admin focused paths if the app uses offsets, topic IDs, groups, or partitions.
17. For converted tests only, put test config under test resources and make the test Gradle/IDE discoverable.
18. Document unsupported behavior, config files, app run commands, Gradle/IDE commands where relevant, and DB setup required.

## Minimal Generic Smoke Flows

After conversion, validate these flows with the converted application's normal configuration mechanism.

Admin smoke:

```java
Properties props = okafkaClientProperties();
try (Admin admin = AdminClient.create(props)) {
    String topic = "TEQ_SMOKE_" + System.currentTimeMillis();
    admin.createTopics(Collections.singletonList(new NewTopic(topic, 1, (short) 1))).all().get();
    if (!admin.listTopics().names().get().contains(topic)) {
        throw new IllegalStateException("Created topic was not listed: " + topic);
    }
    admin.deleteTopics(Collections.singletonList(topic)).all().get();
}
```

Producer smoke:

```java
Properties props = okafkaClientProperties();
props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

try (Producer<String, String> producer =
        new org.oracle.okafka.clients.producer.KafkaProducer<>(props)) {
    producer.send(new ProducerRecord<>("TEQ_SMOKE", "K1", "V1")).get();
    producer.flush();
}
```

Consumer smoke:

```java
Properties props = okafkaClientProperties();
props.put("group.id", "G_SMOKE_" + System.currentTimeMillis());
props.put("auto.offset.reset", "earliest");
props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

try (Consumer<String, String> consumer =
        new org.oracle.okafka.clients.consumer.KafkaConsumer<>(props)) {
    consumer.subscribe(Collections.singletonList("TEQ_SMOKE"));
    long deadline = System.currentTimeMillis() + 60000L;
    while (System.currentTimeMillis() < deadline) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
        if (!records.isEmpty()) {
            consumer.commitSync();
            break;
        }
    }
}
```
