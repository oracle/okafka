# TxEventQ Connector Plugin Directory

This directory contains the **TxEventQ Connector** and its dependencies, downloaded from Maven Central repositories.

## 📁 Directory Structure

```bash
plugins/txeventq-connector/
├── txeventq-connector-23.8.0.25.06.jar  # Main connector JAR 
└── libs/                                # Dependencies 
    ├── aqapi-23.7.0.0.jar              # Oracle AQ API
    ├── javax.jms-api-2.0.1.jar         # JMS API
    ├── jta-1.1.jar                     # Java Transaction API
    ├── ojdbc11-23.8.0.25.04.jar        # Oracle JDBC Driver
    ├── oraclepki-23.8.0.25.04.jar      # Oracle PKI
    ├── osdt_cert-21.17.0.0.jar         # Oracle Security Certificates
    └── osdt_core-21.17.0.0.jar         # Oracle Security Core
```

### Docker Integration

- **Volume Mount**: This directory is mounted to `/etc/kafka-connect/custom-plugins` in the Kafka Connect container
- **Plugin Discovery**: Kafka Connect automatically discovers and loads the connector from this location