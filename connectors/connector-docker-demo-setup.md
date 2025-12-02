# Kafka Connect Docker Demo Setup
A complete Kafka Connect setup with automated JAR downloading, creation of Kafka topics, creation of Oracle Transactional Event Queues, deployment of Kafka JMS Connectors for Oracle Transactional Event Queues, deployment of RedPanda, deployment of Prometheus, and deployment of Grafana.

### Requirements
- **Docker** (Works on Windows, Mac, and Linux) 

### Quick Start
 * Change your directory to the TxEventQConnectorExampleSetup directory where the docker compose file is located within this repository. Before running the docker compose to startup the containers check that the `TxEventQConnectorExampleSetup/scripts/download-jars.sh` line ending is Unix (LF). If the `TxEventQConnectorExampleSetup/scripts/download-jars.sh` script is not (LF), most code editors (VS Code, Notepad++, Sublime Text) have an option to change the End of Line (EOL) conversion.
 * Open the [TxEventQConnectorExampleSetup/pl-sql/db-resources.sql](TxEventQConnectorExampleSetup/pl-sql/db-resources.sql) file and search in the file for the text `<add valid password for specified username>` and replace with the password `WElcomeHome123##` for the user if planning to use the wallet currently available in the TxEventQConnectorExampleSetup/plugins/wallets/freedb directory. If using your own generated wallet replace with the appropriate password.
 * Open the [TxEventQConnectorExampleSetup/oracle-exporter/config.yaml](TxEventQConnectorExampleSetup/oracle-exporter/config.yaml) file and search in the file for the text `<add valid password for specified username>` and replace with the password for the user that you created in file you modified above.
 
Just run this one command:

```bash
docker-compose up
```

Use the `-d` argument to run in detached mode.

```bash
docker-compose up -d
```

### Available Commands

```bash
# Start everything
docker-compose up

# Stop everything
docker-compose down

# Reset everything (remove all data)
docker-compose down -v

# View logs
docker-compose logs -f
docker-compose logs -f connect
docker-compose logs -f kafka

# Used to list the containers associated with a Docker Compose application. It provides information about the status of the services defined in your docker-compose.yml file.
docker-compose ps

# Reset everything
docker-compose down -v && docker-compose up

# List all connectors
curl -s http://localhost:8083/connectors

# Check specific connector status
curl -s http://localhost:8083/connectors/txeventq-ordersToKafka-source/status
curl -s http://localhost:8083/connectors/txeventq-ordersOKafkaTopic-sink/status
curl -s http://localhost:8083/connectors/txeventq-sampleTxeventQ-sink/status
curl -s http://localhost:8083/connectors/txeventq-sampleTxeventQ-source/status
curl -s http://localhost:8083/connectors/txeventq-transformExample-sink/status
curl -s http://localhost:8083/connectors/txeventq-transformSinkExample-source/status
curl -s http://localhost:8083/connectors/txeventq-transformSourceExample-source/status

```
### Container Descriptions
After running the docker compose file several different containers will be created. Descriptions of each of the containers will be provided below.

- **Kafka**
    - Message broker.
    - Used to create Kafka topics.
    - Load some messages into a Kafka topic.
- **Jar Downloader**
	- Used to run the TxEventQConnectorExampleSetup/scripts/download-jars.sh script to automatically download the TxEventQ Connector jar, all of its required dependent jar files, and the Prometheus JMX Exporter jar.
	- The [README-JAR-DOWNLOAD.md](TxEventQConnectorExampleSetup/scripts/README-JAR-DOWNLOAD.md) will provide additional details about what updates can be made to the download script file.
- **Kafka Connect**
    - Connector framework.
    - Add TxEventQ Connector jar and all of the required dependencies as a plugin.
    - Configure Prometheus JMX Exporter which is a tool used to expose Java Management Extensions (JMX) metrics. This will be used by Kafka to gather metrics for Prometheus and used by the Grafana dashboard.
- **Connector Deployer**
	- Used to deploy the sink and source connectors by using the JSON files in TxEventQConnectorExampleSetup/connectors and calling the appropriate curl commands.
    - The [txEventQ-ordersToOracleKafkaTopic-sink-connector-config.json](TxEventQConnectorExampleSetup/connectors/txEventQ-ordersToOracleKafkaTopic-sink-connector-config.json) and [txEventQ-ordersToKafka-source-connector-config.json](TxEventQConnectorExampleSetup/connectors/txEventQ-ordersToKafka-source-connector-config.json) will create a sink and source connector that will sink to an Okafka topic and source from the same Okafka topic.
    - The [txEventQ-sampleTxeventQ-sink-connector-config.json](TxEventQConnectorExampleSetup/connectors/txEventQ-sampleTxeventQ-sink-connector-config.json) and [txEventQ-sampleTxeventQ-source-connector-config.json](TxEventQConnectorExampleSetup/connectors/txEventQ-sampleTxeventQ-source-connector-config.json) will create a sink and source connector that will sink to an transactional event queue and source from the same transactional event queue.
    - The [txEventQ-transformHeader-sink-connector-config.json](TxEventQConnectorExampleSetup/connectors/txEventQ-transformHeader-sink-connector-config.json) and [txEventQ-transformHeader-source-connector-config.json](TxEventQConnectorExampleSetup/connectors/txEventQ-transformHeader-source-connector-config.json) will create a sink and source connector that will sink to an transactional event queue and source from the same transactional event queue. However, this sink connector has additional properties that allows transform of header value to key value and properties are set to store header information to the transactional event queue.
    - The [txEventQ-transformHeader1-source-connector-config.json](TxEventQConnectorExampleSetup/connectors/txEventQ-transformHeader1-source-connector-config.json) will create a source connector that will transform a specified header value to be the key when the messages are set to the specified Kafka topic.
- **Oracle Database**
	- Configure database with the required user and permissions.
	- Create transactional event queues.
	- Used for testing the sink and source connector.
- **Redpanda Console**
    - Web UI for monitoring the Kafka clusters, connectors, and tasks.
    - Fully compatible with Kafka API, use the Kafka Connect connectors to build a wide variety of data streaming pipelines.
    - Create or delete Kafka topics.
    - Produce records to topics.
    - Delete records from topics.
- **Oracle Database Exporter**
	- Provides OpenTelemetry-compatible metrics so you can monitor the health, performance, and availability of your Oracle databases from anywhere.
    - Also used to monitor transactional event queues.
    - For additional information on the Oracle Database Exporter go [here](https://oracle.github.io/oracle-db-appdev-monitoring/docs/intro) .
- **Prometheus**
    - Used to collect and store metrics for Kafka Connect and Oracle database. 
- **Grafana**
    - Uses the metrics scrapped by Prometheus to help generate a visualization tool with dashboards.
    - The 3 dashboards created are:
        - Kafka Connectors for TxEventQ
            - Displays information about the Sink and Source connector
        - Oracle Dashboard
            - Monitor the health, performance, and availability of your databases.
        - TxEventQ Monitor
            - Access to the real-time broker, producer, and consumer metrics.
            - Additional information about monitoring transactional event queues can be found [here](https://oracle.github.io/oracle-db-appdev-monitoring/docs/advanced/txeventq).  

### Access Points

- **Redpanda Console**: http://localhost:8080
- **Kafka Connect API**: http://localhost:8083
- **Grafana**: http://localhost:3000
    - Login with username: admin and password: grafana
- **Prometheus**: http://localhost:9090

## Additional Resources
**Complete installation and setup**: Follow the [Installation and Setup Guide](README.md#installation-and-setup-guide-for-oracle-txeventq-jms-connectors) to install the connector and set up Oracle Database
