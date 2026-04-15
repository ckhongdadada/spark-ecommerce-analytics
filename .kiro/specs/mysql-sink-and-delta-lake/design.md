# Design Document: MySQL Sink and Delta Lake Integration

## Overview

This design introduces two independent enhancements to the e-commerce Spark analytics project's data persistence layer:

1. **MySQL Sink Integration**: Comprehensive JDBC-based persistence for all analytical outputs (batch reports from Module2 and streaming metrics from Module3), enabling real-time dashboard integration with tools like Grafana and Tableau.

2. **Delta Lake Format Support**: Configuration-driven storage format selection between Parquet and Delta Lake across all data processing modules (Module1, Module2, Module3), bringing ACID transaction guarantees to the data lake.

These features are designed to be independently deployable through separate Git branches (`feature/mysql-sink` and `feature/delta-lake`), with Delta Lake building upon MySQL sink functionality.

### Key Design Principles

- **Configuration-Driven**: All format and sink selections controlled through AppConfig without code changes
- **Backward Compatibility**: Existing Parquet-based workflows remain fully functional
- **Graceful Degradation**: MySQL connection failures log warnings but don't halt processing
- **Separation of Concerns**: File format selection independent of sink destination (MySQL/Kafka/file)

## Architecture

### High-Level Component Interaction

```mermaid
graph TB
    subgraph "Data Sources"
        CSV[CSV Raw Data]
        Socket[Socket Stream]
        Kafka[Kafka Stream]
    end
    
    subgraph "Processing Modules"
        M1[Module1: Data Preprocessing]
        M2[Module2: Spark SQL Analytics]
        M3[Module3: Structured Streaming]
    end
    
    subgraph "Storage Layer"
        Format{Format Selection}
        Parquet[Parquet Files]
        Delta[Delta Lake]
    end
    
    subgraph "Sink Layer"
        MySQL[(MySQL Database)]
        KafkaSink[Kafka Topic]
        FileSink[File System]
    end
    
    subgraph "Configuration"
        AppConfig[AppConfig Object]
    end
    
    CSV --> M1
    Socket --> M3
    Kafka --> M3
    
    M1 --> Format
    M2 --> Format
    M2 --> MySQL
    M3 --> Format
    M3 --> MySQL
    M3 --> KafkaSink
    
    Format -->|DATA_FORMAT=parquet| Parquet
    Format -->|DATA_FORMAT=delta| Delta
    
    AppConfig -.->|Configuration| M1
    AppConfig -.->|Configuration| M2
    AppConfig -.->|Configuration| M3
    AppConfig -.->|Configuration| Format
