# Test Cases: Real-Time E-Commerce Data Pipeline

## Test Plan Overview

**Project:** Real-Time Data Ingestion Using Spark Structured Streaming & PostgreSQL  
**Testing Date:** January 30, 2026  
**Tester:** Noah Jamal Nabila  
**Environment:** Docker Desktop on Windows

---

## Test Case 1: CSV File Generation

### Objective
Verify that `data_generator.py` creates CSV files with correct format and data

### Prerequisites
- Data generator container is running
- `/data/streaming` directory is accessible

### Test Steps

#### Step 1.1: Check CSV File Creation
```powershell
# Wait for one batch cycle (30 seconds)
Start-Sleep -Seconds 35

# List CSV files
Get-ChildItem -Path "data\streaming" -Filter "*.csv" | Select-Object Name, Length, LastWriteTime
```

**Expected Result:**
- At least one CSV file exists
- File name format: `ecommerce_events_YYYYMMDD_HHMMSS.csv`
- File size > 0 bytes

**Actual Result:** ✅ PASS / ❌ FAIL  
**Notes:**
```
[Record actual file names, sizes, and timestamps here]
```

#### Step 1.2: Verify CSV Structure
```powershell
# View first 3 lines of latest CSV
$latestFile = Get-ChildItem -Path "data\streaming" -Filter "*.csv" | 
              Sort-Object LastWriteTime -Descending | Select-Object -First 1
Get-Content $latestFile.FullName | Select-Object -First 3
```

**Expected Result:**
- First line contains headers: `user_id,session_id,event_type,product_id,...`
- Data rows follow CSV format
- No malformed records

**Actual Result:** ✅ PASS / ❌ FAIL  
**Notes:**
```
[Record header structure and sample data here]
```

#### Step 1.3: Validate Data Content
```powershell
# Import and check data
$csv = Import-Csv -Path $latestFile.FullName
$csv | Select-Object -First 5 | Format-Table
```

**Expected Result:**
- All required columns present
- `event_type` contains only "view" or "purchase"
- `price` values are numeric and > 0
- `quantity` values are integers >= 1
- `timestamp` is in valid format

**Actual Result:** ✅ PASS / ❌ FAIL  
**Notes:**
```
Column Name       | Data Type | Valid | Sample Value
------------------|-----------|-------|-------------
user_id           |           |       |
session_id        |           |       |
event_type        |           |       |
product_id        |           |       |
price             |           |       |
quantity          |           |       |
timestamp         |           |       |
```

### Overall Test Result: ✅ PASS / ❌ FAIL

---

## Test Case 2: Spark Streaming Detection

### Objective
Verify that Spark Structured Streaming detects and processes new CSV files

### Prerequisites
- Spark master and worker containers running
- Spark job submitted and running
- CSV files being generated

### Test Steps

#### Step 2.1: Check Spark Job Status
```powershell
# Check Spark UI: http://localhost:8080
# Look for "Running Applications"
```

**Expected Result:**
- One application running: "Ecommerce Streaming to PostgreSQL"
- Application status: RUNNING
- At least 1 worker allocated

**Actual Result:** ✅ PASS / ❌ FAIL  
**Screenshot/Notes:**
```
Application ID:
Start Time:
Workers Allocated:
Cores Allocated:
```



#### Step 2.2: Confirm Batch Processing
```powershell
# Look for batch completion messages
docker logs ecommerce-spark-master 2>&1 | Select-String "Batch"
```

**Expected Result:**
- Messages like: "✓ Batch X: Y records in Z.ZZs"
- Batch numbers incrementing (0, 1, 2, ...)
- Processing time reasonable (< 5 seconds per batch)

**Actual Result:** ✅ PASS / ❌ FAIL  
**Processing Times:**
```
Batch | Records | Time (s) | Records/s
------|---------|----------|----------
0     |         |          |
1     |         |          |
2     |         |          |
```

### Overall Test Result: ✅ PASS / ❌ FAIL

---

## Test Case 3: Data Transformations

### Objective
Verify that Spark applies correct data transformations

### Prerequisites
- Data successfully written to PostgreSQL
- At least 100 records in database

### Test Steps

#### Step 3.1: Verify Type Conversions
```powershell
docker exec ecommerce-postgres psql -U spark_user -d ecommerce_events -c "
SELECT 
  pg_typeof(timestamp) as timestamp_type,
  pg_typeof(price) as price_type,
  pg_typeof(quantity) as quantity_type,
  pg_typeof(processing_timestamp) as processing_timestamp_type
FROM ecommerce_events LIMIT 1;"
```

**Expected Result:**
- `timestamp`: timestamp without time zone
- `price`: numeric
- `quantity`: integer
- `processing_timestamp`: timestamp without time zone

**Actual Result:** ✅ PASS / ❌ FAIL  
**Observed Types:**
```
timestamp: 
price: 
quantity: 
processing_timestamp:
```

#### Step 3.2: Check Data Cleaning
```powershell
docker exec ecommerce-postgres psql -U spark_user -d ecommerce_events -c "
SELECT 
  product_name,
  LENGTH(product_name) as name_length,
  product_name = TRIM(product_name) as is_trimmed
FROM ecommerce_events LIMIT 10;"
```

**Expected Result:**
- `is_trimmed` = true for all rows
- No leading/trailing spaces in `product_name`

**Actual Result:** ✅ PASS / ❌ FAIL  
**Notes:**
```
[Record any rows with is_trimmed = false]
```

#### Step 3.3: Verify Normalization
```powershell
docker exec ecommerce-postgres psql -U spark_user -d ecommerce_events -c "
SELECT 
  product_category,
  product_category = LOWER(TRIM(product_category)) as is_normalized
FROM ecommerce_events 
GROUP BY product_category;"
```

**Expected Result:**
- All categories in lowercase
- `is_normalized` = true for all categories
- Categories: electronics, home, sports, books, fashion

**Actual Result:** ✅ PASS / ❌ FAIL  
**Categories Found:**
```
Category          | Is Normalized | Count
------------------|---------------|------
electronics       |               |
home              |               |
sports            |               |
books             |               |
fashion           |               |
```

#### Step 3.4: Validate Business Rules
```powershell
docker exec ecommerce-postgres psql -U spark_user -d ecommerce_events -c "
SELECT 
  COUNT(*) as total_records,
  COUNT(CASE WHEN price <= 0 THEN 1 END) as invalid_price,
  COUNT(CASE WHEN quantity < 1 THEN 1 END) as invalid_quantity,
  MIN(price) as min_price,
  MIN(quantity) as min_quantity
FROM ecommerce_events;"
```

**Expected Result:**
- `invalid_price` = 0 (all prices > 0)
- `invalid_quantity` = 0 (all quantities >= 1)
- `min_price` > 0
- `min_quantity` >= 1

**Actual Result:** ✅ PASS / ❌ FAIL  
**Validation Results:**
```
Total Records: 
Invalid Prices: 
Invalid Quantities: 
Min Price: 
Min Quantity:
```

### Overall Test Result: ✅ PASS / ❌ FAIL

---

## Test Case 4: PostgreSQL Data Storage

### Objective
Verify that data is written to PostgreSQL correctly without errors

### Prerequisites
- Spark streaming job running
- Database table created
- Data flowing through pipeline

### Test Steps

#### Step 4.1: Check Data Insertion
```powershell
# Count records over time
docker exec ecommerce-postgres psql -U spark_user -d ecommerce_events -c "SELECT COUNT(*) FROM ecommerce_events;"
Start-Sleep -Seconds 35
docker exec ecommerce-postgres psql -U spark_user -d ecommerce_events -c "SELECT COUNT(*) FROM ecommerce_events;"
```

**Expected Result:**
- Record count increases after 35 seconds
- Increase approximately 100 records (one batch)

**Actual Result:** ✅ PASS / ❌ FAIL  
**Counts:**
```
Initial Count: 
Count After 35s: 
Difference: 
```

#### Step 4.2: Verify Data Integrity
```powershell
docker exec ecommerce-postgres psql -U spark_user -d ecommerce_events -c "
SELECT 
  COUNT(*) as total,
  COUNT(DISTINCT user_id) as unique_users,
  COUNT(DISTINCT product_id) as unique_products,
  COUNT(*) FILTER (WHERE event_type NOT IN ('view', 'purchase')) as invalid_events
FROM ecommerce_events;"
```

**Expected Result:**
- `total` > 0
- `unique_users` > 0
- `unique_products` > 0
- `invalid_events` = 0

**Actual Result:** ✅ PASS / ❌ FAIL  
**Integrity Check:**
```
Total Records: 
Unique Users: 
Unique Products: 
Invalid Events:
```

#### Step 4.3: Check Primary Key
```powershell
docker exec ecommerce-postgres psql -U spark_user -d ecommerce_events -c "
SELECT 
  COUNT(*) as total_records,
  COUNT(DISTINCT event_id) as unique_ids,
  MIN(event_id) as min_id,
  MAX(event_id) as max_id
FROM ecommerce_events;"
```

**Expected Result:**
- `total_records` = `unique_ids` (no duplicates)
- `event_id` auto-incrementing (1, 2, 3, ...)

**Actual Result:** ✅ PASS / ❌ FAIL  
**Primary Key Check:**
```
Total Records: 
Unique IDs: 
Min ID: 
Max ID:
Duplicate IDs: 
```

#### Step 4.4: Verify Timestamps
```powershell
docker exec ecommerce-postgres psql -U spark_user -d ecommerce_events -c "
SELECT 
  MIN(timestamp) as earliest_event,
  MAX(timestamp) as latest_event,
  MIN(ingestion_timestamp) as first_ingestion,
  MAX(processing_timestamp) as last_processing,
  AVG(EXTRACT(EPOCH FROM (processing_timestamp - ingestion_timestamp))) as avg_latency
FROM ecommerce_events;"
```

**Expected Result:**
- All timestamps valid (not null)
- `avg_latency` < 10 seconds
- Timestamps in logical order

**Actual Result:** ✅ PASS / ❌ FAIL  
**Timestamp Analysis:**
```
Earliest Event: 
Latest Event: 
First Ingestion: 
Last Processing: 
Avg Latency (s):
```

### Overall Test Result: ✅ PASS / ❌ FAIL

---

## Test Case 5: Performance Metrics

### Objective
Verify that system performance is within expected limits

### Prerequisites
- System running for at least 5 minutes
- At least 500 records processed

### Test Steps

#### Step 5.1: Measure Throughput
```powershell
# Count records and time range
docker exec ecommerce-postgres psql -U spark_user -d ecommerce_events -c "
SELECT 
  COUNT(*) as total_records,
  EXTRACT(EPOCH FROM (MAX(processing_timestamp) - MIN(processing_timestamp))) as time_span_seconds,
  COUNT(*) / NULLIF(EXTRACT(EPOCH FROM (MAX(processing_timestamp) - MIN(processing_timestamp))), 0) as records_per_second
FROM ecommerce_events;"
```

**Expected Result:**
- `records_per_second` >= 3 (minimum expected throughput)
- Consistent throughput over time

**Actual Result:** ✅ PASS / ❌ FAIL  
**Throughput Metrics:**
```
Total Records: 
Time Span: 
Records/Second:
```

#### Step 5.2: Analyze Processing Latency
```powershell
docker exec ecommerce-postgres psql -U spark_user -d ecommerce_events -c "
SELECT 
  MIN(EXTRACT(EPOCH FROM (processing_timestamp - ingestion_timestamp))) as min_latency,
  AVG(EXTRACT(EPOCH FROM (processing_timestamp - ingestion_timestamp))) as avg_latency,
  MAX(EXTRACT(EPOCH FROM (processing_timestamp - ingestion_timestamp))) as max_latency,
  PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (processing_timestamp - ingestion_timestamp))) as p95_latency
FROM ecommerce_events;"
```

**Expected Result:**
- `min_latency` < 1 second
- `avg_latency` < 5 seconds
- `max_latency` < 10 seconds
- `p95_latency` < 7 seconds

**Actual Result:** ✅ PASS / ❌ FAIL  
**Latency Analysis:**
```
Min Latency: 
Avg Latency: 
Max Latency: 
P95 Latency:
```

#### Step 5.3: Check Batch Processing Time
```powershell
# Extract from Spark logs
docker logs ecommerce-spark-master 2>&1 | 
  Select-String "Batch.*records in" | 
  Select-Object -Last 10
```

**Expected Result:**
- Batch processing time < 5 seconds
- Consistent processing times
- No failed batches

**Actual Result:** ✅ PASS / ❌ FAIL  
**Batch Performance:**
```
Batch | Records | Time (s) | Records/s
------|---------|----------|----------
      |         |          |
      |         |          |
      |         |          |
[Record last 10 batches]
```

#### Step 5.4: Monitor Resource Usage
```powershell
# Check Docker stats
docker stats --no-stream --format "table {{.Container}}\t{{.CPUPerc}}\t{{.MemUsage}}"
```

**Expected Result:**
- CPU usage < 50% per container
- Memory usage within allocated limits
- No containers restarting

**Actual Result:** ✅ PASS / ❌ FAIL  
**Resource Usage:**
```
Container              | CPU %  | Memory
-----------------------|--------|--------
ecommerce-spark-master |        |
ecommerce-spark-worker |        |
ecommerce-postgres     |        |
ecommerce-data-gen     |        |
```

### Overall Test Result: ✅ PASS / ❌ FAIL

---

## Test Case 6: Error Handling

### Objective
Verify that the system handles errors gracefully

### Prerequisites
- System running normally
- Admin access to containers

### Test Steps

#### Step 6.1: Test Worker Restart
```powershell
# Restart worker during processing
docker restart ecommerce-spark-worker-1
Start-Sleep -Seconds 25

# Check if job continues
docker logs ecommerce-spark-master 2>&1 | Select-String "Batch" | Select-Object -Last 3
```

**Expected Result:**
- Job pauses briefly
- Resumes after worker reconnects
- No data loss

**Actual Result:** ✅ PASS / ❌ FAIL  
**Notes:**
```
[Record behavior during and after restart]
```

#### Step 6.2: Test Invalid CSV Data
```powershell
# Create CSV with invalid data
@"
user_id,session_id,event_type,product_id,product_name,product_category,price,quantity,timestamp,user_agent,ip_address,country,city
USER001,invalid,view,PROD001,Test Product,electronics,invalid_price,1,2026-01-30 12:00:00,Mozilla,192.168.1.1,USA,NYC
"@ | Out-File -FilePath "data\streaming\test_invalid.csv" -Encoding UTF8
```

**Expected Result:**
- Spark handles gracefully
- Logs warning/error
- Other records processed normally

**Actual Result:** ✅ PASS / ❌ FAIL  
**Error Handling:**
```
[Record how system handled invalid data]
```

#### Step 6.3: Test Database Connection Loss
```powershell
# Stop PostgreSQL temporarily
docker stop ecommerce-postgres
Start-Sleep -Seconds 10
docker start ecommerce-postgres
```

**Expected Result:**
- Spark job logs connection errors
- Retries connection automatically
- Resumes writing after reconnection

**Actual Result:** ✅ PASS / ❌ FAIL  
**Recovery Behavior:**
```
[Record error messages and recovery time]
```

### Overall Test Result: ✅ PASS / ❌ FAIL

---

## Test Summary

| Test Case | Description | Result | Notes |
|-----------|-------------|--------|-------|
| TC1 | CSV File Generation | ⬜ PASS / FAIL | |
| TC2 | Spark Streaming Detection | ⬜ PASS / FAIL | |
| TC3 | Data Transformations | ⬜ PASS / FAIL | |
| TC4 | PostgreSQL Storage | ⬜ PASS / FAIL | |
| TC5 | Performance Metrics | ⬜ PASS / FAIL | |
| TC6 | Error Handling | ⬜ PASS / FAIL | |

**Overall Project Status:** ✅ PASS / ❌ FAIL

---

## Issues Found

| Issue ID | Severity | Description | Status | Resolution |
|----------|----------|-------------|--------|------------|
| | | | | |

---

## Recommendations

1. **Performance:**
   - [ ] Add more Spark workers if throughput insufficient
   - [ ] Optimize batch interval based on data volume
   - [ ] Add database indexes for common queries

2. **Reliability:**
   - [ ] Implement automated health checks
   - [ ] Add alerting for job failures
   - [ ] Configure automatic restart policies

3. **Monitoring:**
   - [ ] Add Grafana dashboards
   - [ ] Implement custom metrics collection
   - [ ] Set up log aggregation

---

## Sign-off

**Tester:** _______________________  
**Date:** _________________________  
**Status:** ⬜ Approved / ⬜ Rejected  
**Comments:**
```
[Add final testing comments here]
```
