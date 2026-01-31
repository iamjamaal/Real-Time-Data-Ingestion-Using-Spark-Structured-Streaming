"""
End-to-end integration tests for the data pipeline.

Run with: pytest tests/integration/ -v
"""
import os
import pytest
import psycopg2
import time
from pathlib import Path


@pytest.mark.integration
@pytest.mark.docker
def test_database_connection():
    """Test that we can connect to PostgreSQL from Windows"""
    try:
        conn = psycopg2.connect(
            host="localhost",  # Use localhost when running from Windows
            port=5432,
            dbname="ecommerce_events",
            user="spark_user",
            password="spark_password",
            connect_timeout=3
        )
        
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        
        assert result[0] == 1, "Database query failed"
        print("✓ Database connection successful")
        
        cursor.close()
        conn.close()
        
    except psycopg2.OperationalError as e:
        pytest.skip(f"Database not accessible. Is Docker running? Error: {e}")


@pytest.mark.integration
@pytest.mark.docker
def test_data_exists():
    """Verify that data exists in the database"""
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            dbname="ecommerce_events",
            user="spark_user",
            password="spark_password"
        )
        
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM ecommerce_events")
        count = cursor.fetchone()[0]
        
        if count == 0:
            pytest.skip("No data in database yet. Run the pipeline first.")
        
        print(f"✓ Found {count} records in database")
        assert count > 0
        
        cursor.close()
        conn.close()
        
    except psycopg2.OperationalError as e:
        pytest.skip(f"Database not accessible: {e}")


@pytest.mark.integration
@pytest.mark.docker
def test_data_transformations_applied():
    """Verify that Spark transformations are applied correctly"""
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            dbname="ecommerce_events",
            user="spark_user",
            password="spark_password"
        )
        
        cursor = conn.cursor()
        
        # Check if data exists
        cursor.execute("SELECT COUNT(*) FROM ecommerce_events")
        count = cursor.fetchone()[0]
        
        if count == 0:
            pytest.skip("No data in database to test transformations")
        
        # Check that categories are normalized (lowercase)
        cursor.execute("""
            SELECT product_category, COUNT(*) 
            FROM ecommerce_events 
            WHERE product_category != LOWER(TRIM(product_category))
            GROUP BY product_category
        """)
        
        non_normalized = cursor.fetchall()
        assert len(non_normalized) == 0, f"Found non-normalized categories: {non_normalized}"
        print("✓ All categories properly normalized")
        
        # Check that prices are valid
        cursor.execute("SELECT COUNT(*) FROM ecommerce_events WHERE price <= 0")
        invalid_prices = cursor.fetchone()[0]
        assert invalid_prices == 0, f"Found {invalid_prices} invalid prices"
        print("✓ All prices valid")
        
        # Check that quantities are valid
        cursor.execute("SELECT COUNT(*) FROM ecommerce_events WHERE quantity < 1")
        invalid_quantities = cursor.fetchone()[0]
        assert invalid_quantities == 0, f"Found {invalid_quantities} invalid quantities"
        print("✓ All quantities valid")
        
        cursor.close()
        conn.close()
        
    except psycopg2.OperationalError as e:
        pytest.skip(f"Database not accessible: {e}")


@pytest.mark.integration
@pytest.mark.docker
@pytest.mark.slow
def test_full_pipeline():
    """
    End-to-end pipeline test.
    
    Steps:
    1. Count initial records
    2. Wait for new batch
    3. Verify new data was added
    4. Validate data quality
    """
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            dbname="ecommerce_events",
            user="spark_user",
            password="spark_password"
        )
        
        cursor = conn.cursor()
        
        # Get initial count
        cursor.execute("SELECT COUNT(*) FROM ecommerce_events")
        initial_count = cursor.fetchone()[0]
        print(f"\nInitial count: {initial_count}")
        
        # Wait for new batch (30 second interval + processing time)
        print("Waiting 35 seconds for new data...")
        time.sleep(35)
        
        # Get final count
        cursor.execute("SELECT COUNT(*) FROM ecommerce_events")
        final_count = cursor.fetchone()[0]
        print(f"Final count: {final_count}")
        
        new_records = final_count - initial_count
        print(f"New records: {new_records}")
        
        # Should have at least some new records
        assert new_records >= 50, f"Expected >= 50 new records, got {new_records}"
        print(f"✓ Pipeline added {new_records} new records")
        
        # Validate recent data
        cursor.execute("""
            SELECT 
                COUNT(*) as total,
                COUNT(DISTINCT user_id) as unique_users,
                MIN(price) as min_price,
                MIN(quantity) as min_quantity
            FROM ecommerce_events
            WHERE processing_timestamp > NOW() - INTERVAL '1 minute'
        """)
        
        stats = cursor.fetchone()
        if stats[0] > 0:  # If we have recent data
            total, unique_users, min_price, min_quantity = stats
            
            assert unique_users > 0, "No unique users in recent data"
            assert min_price > 0, f"Invalid price in recent data: {min_price}"
            assert min_quantity >= 1, f"Invalid quantity in recent data: {min_quantity}"
            
            print(f"✓ Recent data validated: {total} records, {unique_users} unique users")
        
        cursor.close()
        conn.close()
        
    except psycopg2.OperationalError as e:
        pytest.skip(f"Database not accessible: {e}")


if __name__ == "__main__":
    # Run tests directly
    print("Running integration tests...")
    print("Note: Docker containers must be running!\n")
    
    test_database_connection()
    test_data_exists()
    test_data_transformations_applied()
    test_full_pipeline()
    
    print("\n✓ All integration tests passed!")
