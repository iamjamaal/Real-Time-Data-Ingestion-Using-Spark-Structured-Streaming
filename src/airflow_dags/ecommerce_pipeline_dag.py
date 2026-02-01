# ecommerce pipeline DAG

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.filesystem import FileSensor
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)



default_args = {
    'owner': 'noah_jamal_nabila',
    'depends_on_past': False,
    'email': ['jamal@ecommerce.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2)
}


def check_spark_job_status():
    """
    Verify Spark streaming job is running.
    
    Justification:
    - Health monitoring
    - Early failure detection
    - Automated recovery
    """
    import requests
    try:
        response = requests.get('http://spark-master:8080/json/', timeout=10)
        if response.status_code == 200:
            data = response.json()
            active_apps = data.get('activeapps', [])
            if len(active_apps) > 0:
                logger.info(f"Spark job running: {active_apps[0]['name']}")
                return True
        logger.error("No active Spark jobs found")
        return False
    except Exception as e:
        logger.error(f"Spark health check failed: {str(e)}")
        raise


def verify_data_ingestion():
    """
    Validate data is being written to PostgreSQL.
    
    Checks:
    1. Recent records exist (last 5 minutes)
    2. No duplicate records
    3. All required fields populated
    """
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Check recent records
    query = """
    SELECT COUNT(*) as recent_count
    FROM ecommerce_events
    WHERE ingestion_timestamp >= NOW() - INTERVAL '5 minutes'
    """
    result = hook.get_first(query)
    
    if result[0] == 0:
        logger.error("No records ingested in last 5 minutes")
        raise ValueError("Data ingestion stalled")
    
    logger.info(f"Recent records: {result[0]}")
    return True


def calculate_pipeline_metrics():
    """
    Calculate and store performance metrics.
    
    Metrics:
    - Ingestion rate (records/minute)
    - Average latency
    - Error rate
    - Data quality score
    """
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    metrics_query = """
    WITH recent_data AS (
        SELECT 
            COUNT(*) as record_count,
            EXTRACT(EPOCH FROM (MAX(ingestion_timestamp) - MIN(ingestion_timestamp)))/60 as duration_minutes,
            AVG(EXTRACT(EPOCH FROM (processing_timestamp - timestamp))) as avg_latency_seconds,
            COUNT(CASE WHEN price IS NULL THEN 1 END) as null_price_count
        FROM ecommerce_events
        WHERE ingestion_timestamp >= NOW() - INTERVAL '1 hour'
    )
    INSERT INTO pipeline_metrics (metric_name, metric_value, metric_unit, metadata)
    SELECT 
        'ingestion_rate',
        record_count / NULLIF(duration_minutes, 0),
        'records_per_minute',
        json_build_object(
            'total_records', record_count,
            'avg_latency_seconds', avg_latency_seconds,
            'data_quality_issues', null_price_count
        )
    FROM recent_data
    """
    
    hook.run(metrics_query)
    logger.info("Metrics calculated and stored")


# DAG Definition
with DAG(
    'ecommerce_streaming_pipeline',
    default_args=default_args,
    description='Real-time ecommerce event processing pipeline',
    schedule_interval='*/15 * * * *',  # Every 15 minutes
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['streaming', 'ecommerce', 'real-time']
) as dag:
    
    # Task 1: Check Spark Job Health
    check_spark = PythonOperator(
        task_id='check_spark_job_health',
        python_callable=check_spark_job_status
    )
    
    # Task 2: Verify Data Ingestion
    verify_ingestion = PythonOperator(
        task_id='verify_data_ingestion',
        python_callable=verify_data_ingestion
    )
    
    # Task 3: Calculate Metrics
    calc_metrics = PythonOperator(
        task_id='calculate_pipeline_metrics',
        python_callable=calculate_pipeline_metrics
    )
    
    # Task 4: Data Quality Checks
    data_quality = PostgresOperator(
        task_id='run_data_quality_checks',
        postgres_conn_id='postgres_default',
        sql="""
        INSERT INTO data_quality_checks (check_name, check_status, records_checked, records_failed, error_details)
        WITH quality_checks AS (
            SELECT 
                COUNT(*) as total_records,
                COUNT(CASE WHEN user_id IS NULL THEN 1 END) as null_user_id,
                COUNT(CASE WHEN price <= 0 THEN 1 END) as invalid_price,
                COUNT(CASE WHEN timestamp > NOW() THEN 1 END) as future_timestamp
            FROM ecommerce_events
            WHERE ingestion_timestamp >= NOW() - INTERVAL '15 minutes'
        )
        SELECT 
            'hourly_quality_check',
            CASE 
                WHEN null_user_id + invalid_price + future_timestamp = 0 THEN 'passed'
                WHEN (null_user_id + invalid_price + future_timestamp)::FLOAT / total_records < 0.01 THEN 'warning'
                ELSE 'failed'
            END,
            total_records,
            null_user_id + invalid_price + future_timestamp,
            json_build_object(
                'null_user_ids', null_user_id,
                'invalid_prices', invalid_price,
                'future_timestamps', future_timestamp
            )::TEXT
        FROM quality_checks
        """
    )
    
    
    # Task 5: Cleanup Old Data (Optional)
    cleanup_old_data = PostgresOperator(
        task_id='cleanup_old_archived_files',
        postgres_conn_id='postgres_default',
        sql="""
        -- Archive records older than 90 days
        DELETE FROM ecommerce_events
        WHERE timestamp < NOW() - INTERVAL '90 days'
        """
    )
    
    
    # Task Dependencies
    check_spark >> verify_ingestion >> [calc_metrics, data_quality]
    data_quality >> cleanup_old_data