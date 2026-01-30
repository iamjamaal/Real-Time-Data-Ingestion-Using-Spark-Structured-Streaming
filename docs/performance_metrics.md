# Performance Metrics

## Throughput Metrics
- **Ingestion Rate**: Records processed per minute
- **Target**: ≥ 500 records/minute
- **Current**: _To be measured_

## Latency Metrics
- **End-to-End Latency**: Time from event generation to PostgreSQL
- **Target**: < 60 seconds (p95)
- **Processing Latency**: Spark transformation time
- **Target**: < 5 seconds

## Resource Utilization
- **Spark Executor Memory**: Monitor for OOM errors
- **PostgreSQL Connections**: Track connection pool usage
- **Disk Usage**: /data directory growth rate

## Data Quality
- **Null Values**: < 1% of records
- **Duplicate Events**: < 0.5% of records
- **Schema Violations**: 0% (strict enforcement)

## Measurement Methods
1. **Airflow DAG**: Automated metric collection every 15 minutes
2. **Spark UI**: Real-time job monitoring (port 8080)
3. **PostgreSQL**: Query-based metrics from pipeline_metrics table
4. **Prometheus**: Time-series metrics aggregation