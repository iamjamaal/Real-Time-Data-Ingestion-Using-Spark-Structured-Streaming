# Test Cases: Real-Time E-Commerce Data Pipeline


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


**Notes:**
> The `ecommerce-data-generator` container was confirmed healthy via `docker ps`. Spark successfully and continuously processed files from `/data/streaming` (39,710+ records ingested), confirming that CSV generation is active. Explicit file listing was not performed during this session. Run this step to formally confirm.

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



#### Step 1.3: Validate Data Content
```powershell
# Import and check data
$csv = Import-Csv -Path $latestFile.FullName
$csv | Select-Object -First 5 | Format-Table
```

**Expected Result:**
- All required columns present
- `event_type` contains valid values: `view`, `purchase`, `cart_add`, `search`
- `price` values are numeric and > 0
- `quantity` values are integers >= 1
- `timestamp` is in valid format


**Notes:**

| Column Name | Data Type | Valid | Sample Value |
|---|---|---|---|
| user_id | String | USER00714 |
| session_id | UUID | 0e920488-11a8-434b-98a4-c2de80c177b3 |
| event_type | String | view, purchase, cart_add, search |
| product_id | String | PROD0020 |
| product_name | String | Balanced real-time algorithm |
| product_category | String | sports, electronics, books, clothing, home |
| price | Numeric | 605.40 |
| quantity | Integer | 1 |
| timestamp | Timestamp | 2026-01-31 14:17:40.578581 |
| user_agent | String | Opera/8.49.(Windows NT 5.01; ...) |
| ip_address | String | 182.54.199.237 |
| country | String | Saint Vincent and the Grenadines |
| city | String | North Maryberg |

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
- One application running: "EcommerceStreamProcessor"
- Application status: RUNNING
- At least 1 worker allocated


| Field | Value |
|---|---|
| Application Name | EcommerceStreamProcessor |
| Application ID | app-20260131141352-0000 |
| Start Time | 2026-01-31 14:13:52 UTC |
| Workers Allocated | 1 |
| Cores Allocated | 2 |
| Worker RAM | 1,024 MiB |
| Status | RUNNING |


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

| Column | Expected Type |
|---|---|
| timestamp | timestamp without time zone |
| price | numeric |
| quantity | integer |
| processing_timestamp | timestamp without time zone |


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

---

## Test Case 4: PostgreSQL Data Storage

### Objective
Verify that data is written to PostgreSQL correctly without errors

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
- Increase of approximately 1,000 records per batch


| Measurement | Row Count |
|---|---|
| Initial Count | 24,010 |
| After ~14 minutes | 38,010 |
| After ~16 minutes | 39,710 |
| **Net Growth** | **+15,700 records confirmed** |

#### Step 4.2: Verify Data Integrity
```powershell

docker exec ecommerce-postgres psql -U spark_user -d ecommerce_events -c "
SELECT 
  COUNT(*) as total,
  COUNT(DISTINCT user_id) as unique_users,
  COUNT(DISTINCT product_id) as unique_products,
  COUNT(*) FILTER (WHERE event_type NOT IN ('view', 'purchase', 'cart_add', 'search')) as invalid_events
FROM ecommerce_events;"
```

**Expected Result:**
- `total` > 0
- `unique_users` > 0
- `unique_products` > 0
- `invalid_events` = 0



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
- `event_id` sequential and incrementing



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
- `avg_latency` reasonable (file-based streaming includes the 30s generator interval)
- Timestamps in logical order



---

## Test Case 5: Performance Metrics

### Objective
Verify that system performance is within expected limits


### Test Steps

#### Step 5.1: Measure Throughput
```powershell
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


> Run the explicit aggregate query for overall records_per_second across the full dataset.



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
docker logs ecommerce-spark-streaming 2>&1 | Select-String "Batch" | Select-Object -Last 3
```

**Expected Result:**
- Job pauses briefly
- Resumes after worker reconnects
- No data loss


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
- Other valid records processed normally


#### Step 6.3: Test Database Connection Loss
```powershell
# Stop PostgreSQL temporarily
docker stop ecommerce-postgres
Start-Sleep -Seconds 10
docker start ecommerce-postgres

# Verify streaming resumes
docker logs ecommerce-spark-streaming 2>&1 | Select-String "Batch" | Select-Object -Last 3
```

**Expected Result:**
- Spark job logs connection errors
- Retries connection automatically
- Resumes writing after reconnection




```
