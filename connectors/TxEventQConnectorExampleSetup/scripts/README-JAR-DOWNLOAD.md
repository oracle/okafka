# Automated JAR Download System

This project includes an automated system to download required JAR files from Maven repositories as part of the Docker Compose setup.

## How It Works

1. **Init Container**: A `jar-downloader` init container runs before the Kafka Connect container starts
2. **Download Script**: The `scripts/download-jars.sh` script downloads all required JAR files from Maven Central
3. **Volume Mounting**: Downloaded JARs are placed in the `plugins/txeventq-connector/libs/` directory
4. **Automatic Verification**: The script verifies all required JARs are present before completing

## Required JAR Files for TxEventQ Connector

The following JAR files are automatically downloaded:

- `javax.jms-api-2.0.1.jar` - JMS API
- `jta-1.1.jar` - Java Transaction API
- `aqapi-23.8.0.0.jar` - Oracle Advanced Queuing API
- `ojdbc11-23.26.0.0.0.jar` - Oracle JDBC Driver
- `oraclepki-23.26.0.0.0.jar` - Oracle PKI
- `osdt_cert-21.20.0.0.jar` - Oracle Security Developer Tools Certificate
- `osdt_core-21.20.0.0.jar` - Oracle Security Developer Tools Core

## Required JAR File Gathering Kafka Connector Metrics with Prometheus
- `jmx_prometheus_javaagent-1.0.1.jar` - Prometheus JMX Exporter Java Agent

## Manual Download

If you need to manually download the JARs, you can run:

```bash
./scripts/download-jars.sh
```

You need to restart the connector container to pick up the manually downloaded jars.

## Customization

### Updating JAR Versions

To update JAR versions, simply change the version number at the top of `scripts/download-jars.sh`:

```bash
# Example: Update OJDBC to a new version
OJDBC_VERSION="23.27.0.0.0"  # Just change this!
```

**That's it!** The script automatically:

- Generates the correct filename (`ojdbc11-23.27.0.0.0.jar`)
- Downloads the new version
- Verifies the new JAR is present
