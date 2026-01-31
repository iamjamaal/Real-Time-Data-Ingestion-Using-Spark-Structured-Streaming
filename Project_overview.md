# Project Overview: Real-Time E-Commerce Data Pipeline

## Executive Summary

This project implements a complete real-time data pipeline that simulates an e-commerce platform tracking user activity. The system generates fake user events, streams them through Apache Spark Structured Streaming, and stores processed data in PostgreSQL for analytics.

## System Architecture

### Components

1. **Data Generator (`data_generator.py`)**
   - Generates realistic e-commerce events every 30 seconds
   - Creates 100 events per batch
   - Events include views and purchases with product details
   - Outputs CSV files to monitored directory

2. **Stream Processor (`spark_streaming_to_postgres.py`)**
   - Apache Spark Structured Streaming application
   - Monitors directory for new CSV files
   - Processes files in 30-second micro-batches
   - Applies data transformations and validations
   - Writes to PostgreSQL in real-time

3. **Database (`PostgreSQL`)**
   - Stores all processed events
   - Optimized schema for analytics queries
   - Automatic event_id generation
   - Timestamps for audit trail

### Data Flow

```
Data Generator → CSV Files → Spark Streaming → Transformations → PostgreSQL
     ↓              ↓              ↓                 ↓              ↓
  30s batch    /data/streaming  File Monitor    Validation    ecommerce_events
```

## Key Features

### Data Generation
- **Event Types:** View and Purchase actions
- **Product Categories:** Electronics, Home, Sports, Books, Fashion
- **Realistic Data:** Using Faker library for authentic user agents, IPs, locations
- **Configurable:** Batch size, interval, and product range adjustable

### Stream Processing
- **Micro-batch Processing:** 30-second trigger intervals
- **Automatic Schema Detection:** Infers CSV schema
- **Data Quality:** Validates prices, quantities, and timestamps
- **Fault Tolerance:** Checkpointing enabled for recovery

### Data Transformations
1. **Type Conversions:** String timestamps → TimestampType
2. **Data Cleaning:** Trim whitespace from product names
3. **Normalization:** Lowercase product categories
4. **Validation:** Ensure price > 0, quantity >= 1
5. **Enrichment:** Add ingestion and processing timestamps

### Database Schema

```sql
event_id (SERIAL)           - Auto-incrementing primary key
user_id (VARCHAR)           - User identifier
session_id (VARCHAR)        - Session tracking
event_type (VARCHAR)        - 'view' or 'purchase'
product_id (VARCHAR)        - Product identifier
product_name (VARCHAR)      - Product display name
product_category (VARCHAR)  - Product category (normalized)
price (DECIMAL)             - Product price
quantity (INTEGER)          - Quantity (purchases only)
timestamp (TIMESTAMP)       - Event occurrence time
user_agent (VARCHAR)        - Browser/device info
ip_address (VARCHAR)        - User IP address
country (VARCHAR)           - User location
city (VARCHAR)              - User city
ingestion_timestamp (TIMESTAMP) - When data entered pipeline
processing_timestamp (TIMESTAMP) - When Spark processed event
```

## Technology Stack

| Component | Technology | Version |
|-----------|-----------|---------|
| Container Orchestration | Docker Compose | Latest |
| Stream Processing | Apache Spark | 3.5.7 |
| Programming Language | Python | 3.11 |
| Database | PostgreSQL | 15-alpine |
| Data Generation | Faker | Latest |
| JDBC Driver | PostgreSQL JDBC | 42.6.0 |

## Docker Architecture

### Containers

1. **spark-master**
   - Spark master node
   - Port 8080: Web UI
   - Port 7077: Spark communication

2. **spark-worker-1**
   - Spark worker node
   - 2 cores, 2GB RAM
   - Executes streaming jobs

3. **postgres**
   - PostgreSQL database
   - Port 5432: Database connection
   - Persistent volume for data

4. **data-generator**
   - Python container
   - Generates CSV files
   - Runs continuously

### Networking

- **Network:** ecommerce_network (bridge)
- **Internal Communication:** Container names as hostnames
- **External Access:** Mapped ports for web UIs and database

## Performance Characteristics

### Throughput
- **Event Generation:** 100 events/30 seconds = ~3.3 events/second
- **Batch Processing:** Typically 1-3 seconds per batch
- **Database Writes:** Batch inserts for efficiency

### Latency
- **End-to-End:** < 35 seconds (generation + processing)
- **Processing Delay:** < 5 seconds typical
- **Database Write:** < 1 second per batch

### Scalability
- **Horizontal Scaling:** Add more Spark workers
- **Vertical Scaling:** Increase worker cores/memory
- **Data Volume:** Tested with 1000+ events

## Use Cases

1. **Real-Time Analytics:** Track user behavior as it happens
2. **Product Performance:** Monitor which products are viewed/purchased
3. **User Journey Analysis:** Track session-based user behavior
4. **Geographic Insights:** Analyze user distribution by location
5. **Performance Monitoring:** System health and throughput metrics

## Advantages

1. **Scalable:** Can handle increasing data volumes
2. **Fault-Tolerant:** Checkpointing ensures no data loss
3. **Real-Time:** Near-instantaneous data availability
4. **Extensible:** Easy to add new transformations or outputs
5. **Production-Ready:** Docker containerization for deployment

## Limitations & Future Improvements

### Current Limitations
- Single Spark worker (can add more)
- File-based streaming (could use Kafka)
- Basic transformations (could add ML)

### Potential Enhancements
1. **Add Kafka:** Replace file-based streaming
2. **Machine Learning:** Real-time product recommendations
3. **Visualization:** Add Grafana/Kibana dashboards
4. **Data Lake:** Add HDFS/S3 for historical data
5. **Multiple Workers:** Increase processing capacity
6. **Advanced Analytics:** Windowing, aggregations, sessionization

## Conclusion

This project demonstrates a complete, production-ready real-time data pipeline using industry-standard tools. It successfully ingests, processes, and stores streaming e-commerce data with proper error handling, monitoring, and scalability considerations.