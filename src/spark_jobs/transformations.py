# spark jobs transformations
from pyspark.sql.functions import (
    col, when, lower, trim, regexp_replace,
    unix_timestamp, from_unixtime, date_format,
    hour, dayofweek, month
)



def clean_text_fields(df):
    """Standardize text fields."""
    return df \
        .withColumn("product_name", trim(col("product_name"))) \
        .withColumn("product_category", lower(trim(col("product_category")))) \
        .withColumn("event_type", lower(trim(col("event_type"))))

def validate_numeric_fields(df):
    """Validate and correct numeric fields."""
    return df \
        .withColumn("price", when(col("price") > 0, col("price")).otherwise(None)) \
        .withColumn("quantity", when(col("quantity") > 0, col("quantity")).otherwise(1))

def add_derived_fields(df):
    """Add calculated and time-based fields."""
    return df \
        .withColumn("revenue", col("price") * col("quantity")) \
        .withColumn("event_hour", hour(col("timestamp"))) \
        .withColumn("event_day_of_week", dayofweek(col("timestamp"))) \
        .withColumn("event_month", month(col("timestamp"))) \
        .withColumn("event_date", date_format(col("timestamp"), "yyyy-MM-dd"))