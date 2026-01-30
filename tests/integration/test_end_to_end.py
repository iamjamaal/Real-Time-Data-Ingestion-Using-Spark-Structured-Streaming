import pytest
import time
import os
from src.data_generator.generator import EcommerceEventGenerator
from src.spark_jobs.streaming_to_postgres import EcommerceStreamProcessor

def test_full_pipeline():
    """
    End-to-end pipeline test.
    
    Steps:
    1. Generate test CSV files
    2. Start Spark streaming
    3. Verify data in PostgreSQL
    4. Validate metrics
    """
    # Setup
    test_dir = "/tmp/test_streaming"
    os.makedirs(test_dir, exist_ok=True)
    
    # Generate test data
    generator = EcommerceEventGenerator(output_dir=test_dir, events_per_file=10)
    generator.generate_file(1)
    
    # Start processor (with timeout)
    processor = EcommerceStreamProcessor()
    query = processor.run_stream_once(test_dir)
    
    # Wait for processing
    time.sleep(10)
    
    # Verify data in PostgreSQL
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    hook = PostgresHook(postgres_conn_id='postgres_test')
    result = hook.get_first("SELECT COUNT(*) FROM ecommerce_events")
    
    assert result[0] == 10, f"Expected 10 records, found {result[0]}"
    
    # Cleanup
    query.stop()
    os.rmdir(test_dir)