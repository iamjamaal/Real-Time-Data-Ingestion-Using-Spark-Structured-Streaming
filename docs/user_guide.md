# User Guide: Real-Time E-Commerce Data Pipeline

## Table of Contents
1. [Prerequisites](#prerequisites)
2. [Installation](#installation)
3. [Starting the Pipeline](#starting-the-pipeline)
4. [Monitoring the System](#monitoring-the-system)
5. [Stopping the Pipeline](#stopping-the-pipeline)
6. [Troubleshooting](#troubleshooting)
7. [Querying the Data](#querying-the-data)

---

## Prerequisites

### Required Software
- **Docker Desktop:** Version 20.10+
- **Docker Compose:** Version 2.0+
- **PowerShell:** For Windows users
- **Minimum System Requirements:**
  - 8GB RAM
  - 20GB free disk space
  - 4 CPU cores recommended

### Verify Installation
```powershell
# Check Docker
docker --version

# Check Docker Compose
docker-compose --version
```

---

### 2. Navigate to Project Directory
```powershell
cd C:\Path\To\Real-Time-Data-Ingestion-Using-Spark-Structured-Streaming
```

---

## Starting the Pipeline

### Step 1: Start Docker Containers

```powershell
# Navigate to docker directory
cd docker

# Start all containers
docker-compose up -d

# Verify all containers are running
docker-compose ps
```

**Expected Output:**
```
NAME                          STATUS              PORTS
ecommerce-data-generator      Up                  
ecommerce-postgres            Up                  0.0.0.0:5432->5432/tcp
ecommerce-spark-master        Up                  0.0.0.0:8080->8080/tcp, 7077/tcp
ecommerce-spark-worker-1      Up                  8081/tcp
```

### Step 2: Initialize Database

```powershell
# Create database and table
docker exec ecommerce-postgres psql -U spark_user -d postgres -c "CREATE DATABASE ecommerce_events;"

docker exec ecommerce-postgres psql -U spark_user -d ecommerce_events -f /sql/postgres_setup.sql

# Verify table creation
docker exec ecommerce-postgres psql -U spark_user -d ecommerce_events -c "\dt"
```

### Step 3: Wait for Spark Workers

```powershell
# Wait 20 seconds for workers to register
Start-Sleep -Seconds 20

# Check Spark UI: http://localhost:8080
# You should see "Workers: 1" with status ALIVE
```

### Step 4: Start Spark Streaming Job

```powershell
# Return to project root
cd ..

# Submit Spark job
docker exec ecommerce-spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --jars /opt/spark/jars/postgresql-42.6.0.jar \
  /opt/spark-jobs/streaming_to_postgres.py
```

**Expected Output:**
```
INFO:__main__:Starting Ecommerce Streaming Pipeline
INFO:__main__:Monitoring directory: /data/streaming
INFO:__main__:✓ Batch 0: 100 records in 2.45s (40 records/s)
INFO:__main__:✓ Batch 1: 100 records in 1.83s (54 records/s)
...
```

---

## Monitoring the System

### 1. Check Data Generation

```powershell
# View CSV files being generated
Get-ChildItem -Path "data\streaming" -Filter "*.csv" | 
  Select-Object Name, Length, LastWriteTime

# Count total CSV files
(Get-ChildItem -Path "data\streaming" -Filter "*.csv").Count
```

### 2. Monitor Spark Processing

#### Web UI
- **URL:** http://localhost:8080
- **Check:**
  - Workers registered
  - Running applications
  - Completed stages

#### Logs
```powershell
# View Spark master logs
docker logs ecommerce-spark-master -f

# View Spark worker logs
docker logs ecommerce-spark-worker-1 -f
```

### 3. Check Database Records

```powershell
# Count total records
docker exec ecommerce-postgres psql -U spark_user -d ecommerce_events -c \
  "SELECT COUNT(*) as total_records FROM ecommerce_events;"

# View latest records
docker exec ecommerce-postgres psql -U spark_user -d ecommerce_events -c \
  "SELECT event_type, product_name, price, timestamp 
   FROM ecommerce_events 
   ORDER BY timestamp DESC 
   LIMIT 10;"

# Check event distribution
docker exec ecommerce-postgres psql -U spark_user -d ecommerce_events -c \
  "SELECT event_type, COUNT(*) as count 
   FROM ecommerce_events 
   GROUP BY event_type;"
```

### 4. Monitor System Resources

```powershell
# Check Docker container stats
docker stats

# Check disk usage
docker system df
```

---

## Stopping the Pipeline

### 1. Stop Spark Job
Press `Ctrl+C` in the terminal where Spark is running

### 2. Stop All Containers
```powershell
cd docker
docker-compose down
```

### 3. Stop and Remove Everything (including data)
```powershell
# ⚠️ WARNING: This deletes all data!
docker-compose down -v
```

---

## Troubleshooting

### Issue 1: Containers Won't Start

**Symptoms:** `docker-compose up` fails
**Solutions:**
```powershell
# Check if ports are in use
netstat -ano | findstr "5432"
netstat -ano | findstr "8080"

# Remove old containers
docker-compose down
docker-compose up -d
```

### Issue 2: Spark Workers Not Connecting

**Symptoms:** Spark UI shows 0 workers
**Solutions:**
```powershell
# Restart Spark containers
cd docker
docker-compose restart spark-master spark-worker-1
Start-Sleep -Seconds 20

# Check worker logs
docker logs ecommerce-spark-worker-1
```

### Issue 3: No Data in Database

**Symptoms:** COUNT(*) returns 0
**Solutions:**
```powershell
# Check if CSV files are being created
Get-ChildItem -Path "data\streaming" -Filter "*.csv"

# Check data generator logs
docker logs ecommerce-data-generator

# Verify Spark is processing
# Look for "✓ Batch X" messages in Spark output
```

### Issue 4: Database Connection Errors

**Symptoms:** "Connection refused" or "Authentication failed"
**Solutions:**
```powershell
# Test database connection
docker exec ecommerce-postgres psql -U spark_user -d ecommerce_events -c "SELECT 1;"

# Restart postgres
docker-compose restart postgres
```

### Issue 5: Schema Mismatch Errors

**Symptoms:** "Column X not found" or "Type mismatch"
**Solutions:**
```powershell
# Check table schema
docker exec ecommerce-postgres psql -U spark_user -d ecommerce_events -c "\d ecommerce_events"

# Recreate table if needed
docker exec ecommerce-postgres psql -U spark_user -d ecommerce_events -f /sql/postgres_setup.sql
```

### Issue 6: Spark Job Fails with "No resources"

**Symptoms:** "Initial job has not accepted any resources"
**Solutions:**
```powershell
# Restart Spark master and workers
docker-compose restart spark-master spark-worker-1
Start-Sleep -Seconds 20

# Then resubmit job
```

---

## Querying the Data

### Basic Queries

```powershell
# Total events by type
docker exec ecommerce-postgres psql -U spark_user -d ecommerce_events -c "
SELECT event_type, COUNT(*) as total
FROM ecommerce_events
GROUP BY event_type;"

# Top 5 products by views
docker exec ecommerce-postgres psql -U spark_user -d ecommerce_events -c "
SELECT product_name, COUNT(*) as views
FROM ecommerce_events
WHERE event_type = 'view'
GROUP BY product_name
ORDER BY views DESC
LIMIT 5;"

# Revenue by category (purchases only)
docker exec ecommerce-postgres psql -U spark_user -d ecommerce_events -c "
SELECT product_category, SUM(price * quantity) as total_revenue
FROM ecommerce_events
WHERE event_type = 'purchase'
GROUP BY product_category
ORDER BY total_revenue DESC;"

# Events by country
docker exec ecommerce-postgres psql -U spark_user -d ecommerce_events -c "
SELECT country, COUNT(*) as events
FROM ecommerce_events
GROUP BY country
ORDER BY events DESC
LIMIT 10;"

# Processing latency
docker exec ecommerce-postgres psql -U spark_user -d ecommerce_events -c "
SELECT 
  AVG(EXTRACT(EPOCH FROM (processing_timestamp - ingestion_timestamp))) as avg_latency_seconds,
  MAX(EXTRACT(EPOCH FROM (processing_timestamp - ingestion_timestamp))) as max_latency_seconds
FROM ecommerce_events;"
```

### Advanced Analytics

```powershell
# Conversion rate (purchases / views)
docker exec ecommerce-postgres psql -U spark_user -d ecommerce_events -c "
SELECT 
  COUNT(CASE WHEN event_type = 'view' THEN 1 END) as views,
  COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) as purchases,
  ROUND(100.0 * COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) / 
        NULLIF(COUNT(CASE WHEN event_type = 'view' THEN 1 END), 0), 2) as conversion_rate
FROM ecommerce_events;"

# Hourly event volume
docker exec ecommerce-postgres psql -U spark_user -d ecommerce_events -c "
SELECT 
  DATE_TRUNC('hour', timestamp) as hour,
  COUNT(*) as events
FROM ecommerce_events
GROUP BY hour
ORDER BY hour DESC
LIMIT 24;"
```

---

## Performance Tuning

### Increase Spark Workers
Edit `docker-compose.yml` to add more workers:
```yaml
spark-worker-2:
  image: bitnami/spark:3.5.7
  environment:
    - SPARK_MODE=worker
    - SPARK_MASTER_URL=spark://spark-master:7077
    - SPARK_WORKER_CORES=2
    - SPARK_WORKER_MEMORY=2G
```

### Adjust Batch Interval
Edit `data_generator.py`:
```python
BATCH_INTERVAL = 15  # Faster generation (15 seconds instead of 30)
```

### Optimize PostgreSQL
```sql
-- Create indexes for better query performance
CREATE INDEX idx_event_type ON ecommerce_events(event_type);
CREATE INDEX idx_timestamp ON ecommerce_events(timestamp);
CREATE INDEX idx_product_category ON ecommerce_events(product_category);
```

---

## Maintenance

### Clear Old Data
```powershell
# Archive old CSV files
Move-Item -Path "data\streaming\*.csv" -Destination "data\archive\"

# Truncate database table
docker exec ecommerce-postgres psql -U spark_user -d ecommerce_events -c \
  "TRUNCATE TABLE ecommerce_events RESTART IDENTITY;"

# Clear checkpoints
Remove-Item -Path "data\checkpoints\*" -Recurse -Force
```

### Backup Database
```powershell
# Backup to file
docker exec ecommerce-postgres pg_dump -U spark_user ecommerce_events > backup.sql

# Restore from backup
Get-Content backup.sql | docker exec -i ecommerce-postgres psql -U spark_user -d ecommerce_events
```

---

## Getting Help

- **Check logs:** All components log to stdout/stderr
- **Spark UI:** http://localhost:8080 for cluster status
- **Database logs:** `docker logs ecommerce-postgres`
- **Project documentation:** See `project_overview.md`

---

## Quick Reference Commands

```powershell
# Start everything
cd docker && docker-compose up -d

# Check status
docker-compose ps

# View logs
docker logs ecommerce-data-generator -f

# Query database
docker exec ecommerce-postgres psql -U spark_user -d ecommerce_events -c "SELECT COUNT(*) FROM ecommerce_events;"

# Stop everything
docker-compose down
```
