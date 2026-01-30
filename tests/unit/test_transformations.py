import pytest
from pyspark.sql import SparkSession
from src.spark_jobs.transformations import clean_text_fields, validate_numeric_fields

@pytest.fixture(scope="session")
def spark():
    try:
        return SparkSession.builder.master("local[2]").appName("unit_tests").getOrCreate()
    except Exception:
        pytest.skip("Spark is not available in this environment")

def test_clean_text_fields(spark):
    """Test text field standardization."""
    data = [("  Product Name  ", "ELECTRONICS", " PURCHASE ")]
    df = spark.createDataFrame(data, ["product_name", "product_category", "event_type"])
    
    result = clean_text_fields(df)
    
    assert result.collect()[0]["product_name"] == "Product Name"
    assert result.collect()[0]["product_category"] == "electronics"
    assert result.collect()[0]["event_type"] == "purchase"

def test_validate_numeric_fields(spark):
    """Test numeric field validation."""
    data = [(100.50, 2), (-10.0, 0), (None, -5)]
    df = spark.createDataFrame(data, ["price", "quantity"])
    
    result = validate_numeric_fields(df)
    
    assert result.collect()[0]["price"] == 100.50
    assert result.collect()[1]["price"] is None  # Negative filtered
    assert result.collect()[2]["quantity"] == 1   # Corrected to 1
