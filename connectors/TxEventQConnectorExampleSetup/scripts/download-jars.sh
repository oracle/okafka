#!/bin/sh

# Script to download JAR files from Maven repositories
# This script will be run in an init container

set -e

# =============================================================================
# JAR VERSION CONFIGURATION
# =============================================================================
# Update versions here when you need to upgrade JAR files
# Just change the version number - everything else is automatic!

# Standard Maven Central JARs
JAVAX_JMS_VERSION="2.0.1"
JTA_VERSION="1.1"

# Oracle JARs (available on Maven Central)
AQAPI_VERSION="23.8.0.0"
OJDBC_VERSION="23.26.0.0.0"
ORACLEPKI_VERSION="23.26.0.0.0"
OSDT_CERT_VERSION="21.20.0.0"
OSDT_CORE_VERSION="21.20.0.0"

# Transaction Event Queue Connector
TXEVENTQ_CONNECTOR_VERSION="23.26.0.25.12"

# Prometheus JMX Exporter
PROMETHEUS_JMX_EXPORTER_VERSION="1.0.1"

# =============================================================================
# JAR METADATA CONFIGURATION
# =============================================================================
# JAR definitions with group/artifact info (rarely changes)

# Standard Maven Central JARs
JAVAX_JMS_GROUP="javax.jms"
JAVAX_JMS_ARTIFACT="javax.jms-api"

JTA_GROUP="javax.transaction"
JTA_ARTIFACT="jta"

# Oracle JARs (available on Maven Central)
AQAPI_GROUP="com.oracle.database.messaging"
AQAPI_ARTIFACT="aqapi"

OJDBC_GROUP="com.oracle.database.jdbc"
OJDBC_ARTIFACT="ojdbc11"

ORACLEPKI_GROUP="com.oracle.database.security"
ORACLEPKI_ARTIFACT="oraclepki"

OSDT_CERT_GROUP="com.oracle.database.security"
OSDT_CERT_ARTIFACT="osdt_cert"

OSDT_CORE_GROUP="com.oracle.database.security"
OSDT_CORE_ARTIFACT="osdt_core"

# Transaction Event Queue Connector
TXEVENTQ_CONNECTOR_GROUP="com.oracle.database.messaging"
TXEVENTQ_CONNECTOR_ARTIFACT="txeventq-connector"

# Prometheus JMX Exporter
PROMETHEUS_JMX_EXPORTER_GROUP="io.prometheus.jmx"
PROMETHEUS_JMX_EXPORTER_ARTIFACT="jmx_prometheus_javaagent"
# =============================================================================
# SCRIPT CONFIGURATION
# =============================================================================

# Create directory structure
echo "Creating directory structure..."
echo "Current directory: $(pwd)"
echo "Contents of /shared:"
ls -la /shared/ 2>/dev/null || echo "Directory /shared does not exist"
echo "Creating directories step by step..."
mkdir -p /shared/txeventq-connector
echo "Created /shared/txeventq-connector"
mkdir -p /shared/txeventq-connector/libs
echo "Created /shared/txeventq-connector/libs"
echo "Directory structure created successfully"
echo "Contents of /shared/txeventq-connector:"
ls -la /shared/txeventq-connector 2>/dev/null || echo "Directory /shared/txeventq-connector does not exist"

# Function to generate filename from artifact and version
generate_filename() {
    local artifact="$1"
    local version="$2"
    echo "${artifact}-${version}.jar"
}

# Function to download JAR from Maven Central
download_jar() {
    local group_id="$1"
    local artifact_id="$2"
    local version="$3"
    local filename="$4"
    
    echo "Downloading $artifact_id-$version.jar..."
    
    # Convert group ID to path (replace dots with slashes)
    local group_path=$(echo "$group_id" | tr '.' '/')
    
    # Maven Central URL
    local url="https://repo1.maven.org/maven2/$group_path/$artifact_id/$version/$artifact_id-$version.jar"
    
    # Download the JAR file
    if curl -L -f -o "$filename" "$url"; then
        echo "Successfully downloaded $filename"
    else
        echo "Failed to download $filename from $url"
        return 1
    fi
}

# Download Oracle JARs from Maven Central (they are available there)
download_oracle_jar() {
    local group_id="$1"
    local artifact_id="$2"
    local version="$3"
    local filename="$4"
    
    echo "Downloading Oracle JAR: $artifact_id-$version.jar..."
    
    # Convert group ID to path (replace dots with slashes)
    local group_path=$(echo "$group_id" | tr '.' '/')
    
    # Maven Central URL for Oracle JARs
    local url="https://repo1.maven.org/maven2/$group_path/$artifact_id/$version/$artifact_id-$version.jar"
    
    if curl -L -f -o "$filename" "$url"; then
        echo "Successfully downloaded $filename"
    else
        echo "Failed to download $filename from Maven Central"
        echo "Trying alternative sources..."
        
        # Try Oracle's Maven repository as fallback
        local oracle_url="https://maven.oracle.com/com/oracle/database/$artifact_id/$version/$artifact_id-$version.jar"
        if curl -L -f -o "$filename" "$oracle_url"; then
            echo "Successfully downloaded $filename from Oracle repository"
        else
            echo "Failed to download $filename from all sources"
            return 1
        fi
    fi
}

# Check if JAR already exists to avoid re-downloading
check_and_download() {
    local filename="$1"
    local download_func="$2"
    shift 2
    
    if [ -f "$filename" ]; then
        echo "$filename already exists, skipping download"
    else
        # Create directory if it doesn't exist
        local dir=$(dirname "$filename")
        mkdir -p "$dir"
        $download_func "$@"
    fi
}

# Download JARs using configuration variables
echo "Starting JAR download process..."
echo "Configuration loaded:"
echo "  - JMS API: $JAVAX_JMS_VERSION"
echo "  - JTA: $JTA_VERSION"
echo "  - AQAPI: $AQAPI_VERSION"
echo "  - OJDBC: $OJDBC_VERSION"
echo "  - Oracle PKI: $ORACLEPKI_VERSION"
echo "  - OSDT Cert: $OSDT_CERT_VERSION"
echo "  - OSDT Core: $OSDT_CORE_VERSION"
echo "  - TX EventQ Connector: $TXEVENTQ_CONNECTOR_VERSION"
echo ""

# Generate filenames automatically
JAVAX_JMS_FILENAME=$(generate_filename "$JAVAX_JMS_ARTIFACT" "$JAVAX_JMS_VERSION")
JTA_FILENAME=$(generate_filename "$JTA_ARTIFACT" "$JTA_VERSION")
AQAPI_FILENAME=$(generate_filename "$AQAPI_ARTIFACT" "$AQAPI_VERSION")
OJDBC_FILENAME=$(generate_filename "$OJDBC_ARTIFACT" "$OJDBC_VERSION")
ORACLEPKI_FILENAME=$(generate_filename "$ORACLEPKI_ARTIFACT" "$ORACLEPKI_VERSION")
OSDT_CERT_FILENAME=$(generate_filename "$OSDT_CERT_ARTIFACT" "$OSDT_CERT_VERSION")
OSDT_CORE_FILENAME=$(generate_filename "$OSDT_CORE_ARTIFACT" "$OSDT_CORE_VERSION")
TXEVENTQ_CONNECTOR_FILENAME=$(generate_filename "$TXEVENTQ_CONNECTOR_ARTIFACT" "$TXEVENTQ_CONNECTOR_VERSION")
PROMETHEUS_JMX_EXPORTER_FILENAME=$(generate_filename "$PROMETHEUS_JMX_EXPORTER_ARTIFACT" "$PROMETHEUS_JMX_EXPORTER_VERSION")

# Download Transaction Event Queue Connector (main JAR)
echo "Downloading main connector JAR..."
check_and_download "shared/txeventq-connector/$TXEVENTQ_CONNECTOR_FILENAME" download_oracle_jar "$TXEVENTQ_CONNECTOR_GROUP" "$TXEVENTQ_CONNECTOR_ARTIFACT" "$TXEVENTQ_CONNECTOR_VERSION" "/shared/txeventq-connector/$TXEVENTQ_CONNECTOR_FILENAME"

# Download dependency JARs to libs folder
echo "Downloading dependency JARs..."
check_and_download "/shared/txeventq-connector/libs/$JAVAX_JMS_FILENAME" download_jar "$JAVAX_JMS_GROUP" "$JAVAX_JMS_ARTIFACT" "$JAVAX_JMS_VERSION" "/shared/txeventq-connector/libs/$JAVAX_JMS_FILENAME"
check_and_download "/shared/txeventq-connector/libs/$JTA_FILENAME" download_jar "$JTA_GROUP" "$JTA_ARTIFACT" "$JTA_VERSION" "/shared/txeventq-connector/libs/$JTA_FILENAME"
check_and_download "/shared/txeventq-connector/libs/$AQAPI_FILENAME" download_oracle_jar "$AQAPI_GROUP" "$AQAPI_ARTIFACT" "$AQAPI_VERSION" "/shared/txeventq-connector/libs/$AQAPI_FILENAME"
check_and_download "/shared/txeventq-connector/libs/$OJDBC_FILENAME" download_oracle_jar "$OJDBC_GROUP" "$OJDBC_ARTIFACT" "$OJDBC_VERSION" "/shared/txeventq-connector/libs/$OJDBC_FILENAME"
check_and_download "/shared/txeventq-connector/libs/$ORACLEPKI_FILENAME" download_oracle_jar "$ORACLEPKI_GROUP" "$ORACLEPKI_ARTIFACT" "$ORACLEPKI_VERSION" "/shared/txeventq-connector/libs/$ORACLEPKI_FILENAME"
check_and_download "/shared/txeventq-connector/libs/$OSDT_CERT_FILENAME" download_oracle_jar "$OSDT_CERT_GROUP" "$OSDT_CERT_ARTIFACT" "$OSDT_CERT_VERSION" "/shared/txeventq-connector/libs/$OSDT_CERT_FILENAME"
check_and_download "/shared/txeventq-connector/libs/$OSDT_CORE_FILENAME" download_oracle_jar "$OSDT_CORE_GROUP" "$OSDT_CORE_ARTIFACT" "$OSDT_CORE_VERSION" "/shared/txeventq-connector/libs/$OSDT_CORE_FILENAME"

echo "Downloading Prometheus JAR..."
check_and_download "/Prometheus/$PROMETHEUS_JMX_EXPORTER_FILENAME" download_jar "$PROMETHEUS_JMX_EXPORTER_GROUP" "$PROMETHEUS_JMX_EXPORTER_ARTIFACT" "$PROMETHEUS_JMX_EXPORTER_VERSION" "/Prometheus/$PROMETHEUS_JMX_EXPORTER_FILENAME"

echo "All JAR files downloaded successfully!"
echo "Contents of /shared/txeventq-connector/:"
ls -la /shared/txeventq-connector/
echo ""
echo "Contents of /shared/txeventq-connector/libs/:"
ls -la /shared/txeventq-connector/libs/

echo ""
echo "Contents of /Prometheus/:"
ls -la /Prometheus/

# Verify all required JARs are present
echo "Verifying all required JARs are present..."

# Check main connector JAR
if [ -f "/shared/txeventq-connector/$TXEVENTQ_CONNECTOR_FILENAME" ]; then
    echo "✓ $TXEVENTQ_CONNECTOR_FILENAME (main connector)"
else
    echo "✗ $TXEVENTQ_CONNECTOR_FILENAME (main connector) - MISSING!"
    exit 1
fi

# Check dependency JARs
for jar in "$JAVAX_JMS_FILENAME" "$JTA_FILENAME" "$AQAPI_FILENAME" "$OJDBC_FILENAME" "$ORACLEPKI_FILENAME" "$OSDT_CERT_FILENAME" "$OSDT_CORE_FILENAME"; do
    if [ -f "/shared/txeventq-connector/libs/$jar" ]; then
        echo "✓ libs/$jar"
    else
        echo "✗ libs/$jar - MISSING!"
        exit 1
    fi
done

# Check for Prometheus JMX Exporter JAR
if [ -f "/Prometheus/$PROMETHEUS_JMX_EXPORTER_FILENAME" ]; then
    echo "✓ $PROMETHEUS_JMX_EXPORTER_FILENAME"
else
    echo "✗ $PROMETHEUS_JMX_EXPORTER_FILENAME - MISSING!"
    exit 1
fi

echo "All required JAR files are present and ready!"
