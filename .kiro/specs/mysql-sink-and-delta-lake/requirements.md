# Requirements Document

## Introduction

This document specifies requirements for two independent features in the e-commerce Spark analytics project: MySQL Sink for Dashboard Integration and Delta Lake Integration. These features enhance the data persistence layer by adding comprehensive MySQL support for all analytical reports and introducing modern data lake capabilities through Delta Lake format support.

## Glossary

- **Module2_SparkSQL**: Spark SQL analytics module that produces three batch reports (funnel, hourly trend, top3 items)
- **Module3_Streaming**: Structured Streaming module that computes real-time PV/UV metrics
- **AppConfig**: Configuration hub object that manages all application settings
- **Funnel_Report**: Category conversion funnel showing pv/cart/buy counts and conversion rates
- **Hourly_Trend_Report**: Hourly PV/UV trend aggregated by hour
- **Top3_Report**: Top 3 items per category ranked by buy count
- **Stream_Metrics**: Real-time PV/UV metrics computed in streaming windows
- **MySQL_Sink**: JDBC-based persistence mechanism for writing DataFrames to MySQL tables
- **Delta_Lake**: Open-source storage layer that brings ACID transactions to Apache Spark
- **SparkSessionFactory**: Factory object responsible for creating and configuring SparkSession instances
- **Format_Selection**: Configuration-driven choice between parquet and delta storage formats

## Requirements

### Requirement 1: MySQL Sink for Batch Reports

**User Story:** As a data analyst, I want all three Spark SQL reports written to MySQL tables, so that I can query them from dashboard tools like Grafana or Tableau.

#### Acceptance Criteria

1. WHEN Module2_SparkSQL completes funnel analysis, THE MySQL_Sink SHALL write the Funnel_Report to the MySQL table specified by MYSQL_TABLE_FUNNEL
2. WHEN Module2_SparkSQL completes hourly trend analysis, THE MySQL_Sink SHALL write the Hourly_Trend_Report to the MySQL table specified by MYSQL_TABLE_TREND
3. WHEN Module2_SparkSQL completes top3 items analysis, THE MySQL_Sink SHALL write the Top3_Report to the MySQL table specified by MYSQL_TABLE_TOP3
4. THE MySQL_Sink SHALL use SaveMode.Overwrite for batch report tables to ensure fresh data on each run
5. THE MySQL_Sink SHALL use the JDBC options from AppConfig (MYSQL_URL, MYSQL_USER, MYSQL_PASSWORD, MYSQL_BATCH_SIZE, MYSQL_PARTITIONS)
6. IF MySQL connection fails, THEN THE Module2_SparkSQL SHALL log a warning and continue execution without failing

### Requirement 2: MySQL Sink for Streaming Metrics

**User Story:** As a real-time monitoring engineer, I want streaming PV/UV metrics written to MySQL when STREAM_SINK="mysql", so that I can build live dashboards showing current user activity.

#### Acceptance Criteria

1. WHEN STREAM_SINK configuration equals "mysql", THE Module3_Streaming SHALL write Stream_Metrics to MySQL using foreachBatch pattern
2. THE Module3_Streaming SHALL write each micro-batch to the MySQL table specified by MYSQL_TABLE_STREAM_METRICS
3. THE MySQL_Sink SHALL use SaveMode.Append for streaming metrics to preserve historical window data
4. THE MySQL_Sink SHALL include window_start, window_end, behavior, pv, and uv columns in each batch write
5. FOR ALL micro-batches, THE Module3_Streaming SHALL log the batch ID and row count after successful MySQL write
6. THE Module3_Streaming SHALL use the same JDBC options as batch reports (MYSQL_URL, MYSQL_USER, MYSQL_PASSWORD, MYSQL_BATCH_SIZE, MYSQL_PARTITIONS)

### Requirement 3: Delta Lake Dependency Configuration

**User Story:** As a data engineer, I want Delta Lake dependencies added to the project, so that I can use delta format for ACID-compliant data storage.

#### Acceptance Criteria

1. THE pom.xml SHALL include delta-spark_2.12 dependency with version 2.3.0
2. THE delta-spark dependency SHALL have compile scope to be available at runtime
3. THE delta-spark version SHALL be compatible with Spark 3.3.2
4. THE pom.xml SHALL maintain existing Spark dependencies without version conflicts

### Requirement 4: Delta Lake Spark Session Configuration

**User Story:** As a data engineer, I want SparkSession configured with Delta extensions, so that Delta Lake format operations are enabled.

#### Acceptance Criteria

1. THE SparkSessionFactory SHALL configure spark.sql.extensions with io.delta.sql.DeltaSparkSessionExtension
2. THE SparkSessionFactory SHALL configure spark.sql.catalog.spark_catalog with org.apache.spark.sql.delta.catalog.DeltaCatalog
3. THE SparkSession configuration SHALL be applied before any Delta format operations
4. THE SparkSessionFactory SHALL maintain backward compatibility with existing parquet-based operations

### Requirement 5: Delta Format Support in Module1

**User Story:** As a data engineer, I want Module1_DataPreprocessing to support delta format, so that cleaned data can be stored with ACID guarantees.

#### Acceptance Criteria

1. WHERE delta format is configured, THE Module1_DataPreprocessing SHALL write cleaned data using .format("delta")
2. WHERE parquet format is configured, THE Module1_DataPreprocessing SHALL write cleaned data using .format("parquet")
3. THE Module1_DataPreprocessing SHALL read the format selection from AppConfig
4. THE Module1_DataPreprocessing SHALL use the same output path (CLEAN_OUTPUT_PATH) regardless of format
5. FOR ALL valid format configurations, THE Module1_DataPreprocessing SHALL successfully write and verify data

### Requirement 6: Delta Format Support in Module2

**User Story:** As a data engineer, I want Module2_SparkSQL to support delta format for report outputs, so that analytical results have transactional consistency.

#### Acceptance Criteria

1. WHERE delta format is configured, THE Module2_SparkSQL SHALL write all three reports using .format("delta")
2. WHERE parquet format is configured, THE Module2_SparkSQL SHALL write all three reports using .format("parquet")
3. THE Module2_SparkSQL SHALL read the format selection from AppConfig
4. THE Module2_SparkSQL SHALL maintain existing MySQL JDBC writes regardless of file format configuration
5. FOR ALL valid format configurations, THE Module2_SparkSQL SHALL successfully write funnel, trend, and top3 reports

### Requirement 7: Delta Format Support in Module3

**User Story:** As a data engineer, I want Module3_Streaming to support delta format for streaming outputs, so that real-time data has ACID properties.

#### Acceptance Criteria

1. WHERE delta format is configured AND STREAM_SINK equals "file", THE Module3_Streaming SHALL write metrics using .format("delta")
2. WHERE parquet format is configured AND STREAM_SINK equals "file", THE Module3_Streaming SHALL write metrics using .format("parquet")
3. THE Module3_Streaming SHALL read the format selection from AppConfig
4. THE Module3_Streaming SHALL maintain existing kafka and mysql sink behaviors regardless of file format configuration
5. FOR ALL valid format configurations, THE Module3_Streaming SHALL successfully write streaming metrics, invalid rows, and quality alerts

### Requirement 8: Configuration-Driven Format Selection

**User Story:** As a DevOps engineer, I want format selection controlled by configuration, so that I can switch between parquet and delta without code changes.

#### Acceptance Criteria

1. THE AppConfig SHALL define a DATA_FORMAT configuration parameter with default value "parquet"
2. THE DATA_FORMAT parameter SHALL accept values "parquet" or "delta"
3. THE AppConfig SHALL validate DATA_FORMAT at startup and reject invalid values
4. THE AppConfig SHALL provide the format value to all modules through a public accessor
5. WHEN DATA_FORMAT is set via environment variable or JVM property, THE AppConfig SHALL use that value instead of the default

### Requirement 9: Git Branch Management

**User Story:** As a developer, I want features developed in separate Git branches, so that MySQL and Delta Lake changes can be tested and merged independently.

#### Acceptance Criteria

1. THE feature/mysql-sink branch SHALL contain all MySQL sink enhancements for Module2 and Module3
2. THE feature/delta-lake branch SHALL be based on feature/mysql-sink branch
3. THE feature/delta-lake branch SHALL contain Delta Lake dependency, SparkSession configuration, and format support changes
4. THE feature/mysql-sink branch SHALL be independently testable without Delta Lake dependencies
5. THE feature/delta-lake branch SHALL include all MySQL sink functionality from its base branch

### Requirement 10: Windows Environment Compatibility

**User Story:** As a Windows developer, I want Delta Lake to work correctly on Windows, so that I can develop and test locally without Linux dependencies.

#### Acceptance Criteria

1. WHERE Hadoop native libraries are required, THE project documentation SHALL provide Winutils setup instructions
2. THE Delta Lake integration SHALL function on Windows 10/11 with appropriate Hadoop configuration
3. IF Hadoop native library errors occur, THEN THE error messages SHALL indicate the missing Winutils requirement
4. THE project SHALL document any Windows-specific Delta Lake configuration requirements
5. THE local development workflow SHALL remain functional on Windows after Delta Lake integration
