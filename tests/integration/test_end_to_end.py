import pytest
import time
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../src')))
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
    
    # Clean target table
    import psycopg2
    conn = psycopg2.connect(
        host="postgres",
        port=5432,
        dbname="ecommerce_events",
        user="spark_user",
        password="spark_password"
    )
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE ecommerce_events")
    conn.commit()
    cur.close()
    conn.close()
    
    # Generate test data
    generator = EcommerceEventGenerator(output_dir=test_dir, events_per_file=10)
    generator.generate_file(1)
    
    # Start processor (with timeout)
    processor = EcommerceStreamProcessor()
    query = processor.run_stream_once(test_dir)
    
    # Wait for processing
    time.sleep(10)
    
    # Verify data in PostgreSQL
    conn = psycopg2.connect(host="postgres", port=5432, dbname="ecommerce_events",
                            user="spark_user", password="spark_password")
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM ecommerce_events")
    count = cur.fetchone()[0]
    cur.close()
    conn.close()
    assert count == 10, f"Expected 10 records, found {count}"
    
    # Cleanup
    query.stop()
    import shutil
    shutil.rmtree(test_dir, ignore_errors=True)
