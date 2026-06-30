# AGENTS.md

## Repository Overview

This repository contains Oracle's implementation of Apache Kafka-Client Library.
This implementation is called OKafka.

OKafka provides Kafka-compatible client APIs backed by Oracle Database and Oracle TEQ.

Tech Stack:
  API implementation: JAVA
  Backend: Oracle Database

Main Directories:

 - 'clients/src/main/java/org/oracle/okafka' - Entire Source code for OKafka 
 - 'clients/src/main/java/org/oracle/okafka/clients/admin' - Implementation for Apache-Kafka Admin API
 - 'clients/src/main/java/org/oracle/okafka/clients/consumer' - Implementation for Apache-Kafka Consumer API
 - 'clients/src/main/java/org/oracle/okafka/clients/producer' - Implementation for Apache-Kafka Producer API
 - 'clients/src/main/java/org/oracle/okafka/common' - Core component to communicate with Oracle Database

## Development Rules

 - Prefer small focused commits.
 - Do not introduce new JAR dependencies unless explicitly approved.
 - Reuse existing utilities before creating new ones
 - Preserve existing code architecture
 
## Build Validation 

 - Use below command after changes to validate that okafka.jar file is built successfully
    ./gradlew build
 
## Oracle Database Integration Rules

 - Preserve existing JDBC connection lifecycle patterns.
 - Ensure database resources are always cleaned up.
 - Do not introduce connection leaks.
 - Preserve transactional semantics between Kafka APIs and Oracle TEQ operations.

## Java Guidelines

 - Follow existing package and naming conventions.
 - Prefer existing utility/helper classes before adding new abstractions.
 - Avoid large refactors unless explicitly requested.
 - Keep methods focused and small.
 - Maintain backward-compatible method signatures.

## Forbidden Actions

 - The agent must NOT do below:
    - Introduce breaking public API changes without explicit request.
    - Replace existing Oracle JDBC patterns with alternative implementations.
    - Add new dependencies without approval.
    - Remove compatibility checks.
    - Modify Gradle build structure unless requested.


## Preferred Workflow
 - Read nearby implementation classes before making changes.
 - Identify the existing pattern used by producer, consumer, admin, or common code.
 - Make the smallest safe change that satisfies the task.
 - Run the relevant build or test command.
 - Summarize:
   - what changed
   - why it changed
   - files modified
   - any risks or follow-up work
   
## Skills

  Use `KAFKA_TO_OKAFKA_SKILL.md` when converting an Apache Kafka Java application to an OKafka application backed by Oracle Database TEQ.

  Use this skill for:
    - migrating Kafka producer, consumer, and admin client construction to OKafka
    - updating imports while keeping Kafka shared model types where OKafka still uses them
    - converting Kafka client properties to OKafka and Oracle TEQ properties
    - checking for APIs that OKafka does not support or handles differently
    - preserving OKafka-specific behavior around topic handling, offsets, Oracle transactions, and transactional producer usage
    - preparing validation commands and noting behavior that requires a live Oracle Database TEQ environment

  How to use it:
    - First read `clients/KAFKA_TO_OKAFKA_SKILL.md`.
    - Then read `clients/KAFKA_TO_OKAFKA_CONVERSION_GUIDE.md` for the detailed conversion checklist and API notes.
    - Inspect the target Kafka application before editing.
    - Apply the smallest conversion that preserves the application's behavior under OKafka.
    - Document any unsupported APIs, required Oracle DB/TEQ configuration, and behavior that still needs live database validation.

  If an AI assistant does not support structured skills, treat `KAFKA_TO_OKAFKA_SKILL.md` and `KAFKA_TO_OKAFKA_CONVERSION_GUIDE.md` as ordinary markdown instructions and follow them directly.
