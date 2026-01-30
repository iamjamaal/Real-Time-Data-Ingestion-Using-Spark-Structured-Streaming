
"""
Real-Time E-commerce Event Stream Processor
Simplified version matching current database schema
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, to_timestamp, 
    when, lit, trim, lower
)
from pyspark.sql.types import (
    StructType, StructField, StringType, 
    DecimalType, IntegerType, TimestampType
)
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class EcommerceStreamProcessor:
    """
    Spark Structured Streaming job for processing e-commerce events.
    Simplified version - matches current PostgreSQL schema exactly.
    """
    
    def __init__(self, app_name: str = "EcommerceStreamProcessor"):
        """Initialize the stream processor."""
        self.app_name = app_name
        self.spark = self._create_spark_session()
        self.schema = self._define_schema()
        
        # PostgreSQL connection details
        self.postgres_url = "jdbc:postgresql://postgres:5432/ecommerce_events"
        self.postgres_properties = {
            "user": "spark_user",
            "password": "spark_password",
            "driver": "org.postgresql.Driver",
            "batchsize": "1000",
            "isolationLevel": "READ_COMMITTED"
        }
        
    def _create_spark_session(self) -> SparkSession:
        """Create and configure Spark session."""
        try:
            spark = SparkSession.builder \
                .appName(self.app_name) \
                .config("spark.sql.shuffle.partitions", "4") \
                .config("spark.sql.streaming.schemaInference", "false") \
                .config("spark.jars", "/opt/spark/jars/postgresql-42.6.0.jar") \
                .getOrCreate()
            
            spark.sparkContext.setLogLevel("WARN")
            logger.info(f"Spark session created: {self.app_name}")
            return spark
            
        except Exception as e:
            logger.error(f"Failed to create Spark session: {e}")
            raise
    
    def _define_schema(self) -> StructType:
        """
        Define explicit schema for incoming CSV files.
        """
        return StructType([
            StructField("user_id", StringType(), False),
            StructField("session_id", StringType(), True),
            StructField("event_type", StringType(), False),
            StructField("product_id", StringType(), False),
            StructField("product_name", StringType(), True),
            StructField("product_category", StringType(), True),
            StructField("price", DecimalType(10, 2), True),
            StructField("quantity", IntegerType(), True),
            StructField("timestamp", StringType(), False),  # Will convert to timestamp
            StructField("user_agent", StringType(), True),
            StructField("ip_address", StringType(), True),
            StructField("country", StringType(), True),
            StructField("city", StringType(), True)
        ])
    
    def read_stream(self, input_path: str = "/data/streaming"):
        """
        Read streaming data from CSV files.
        """
        logger.info(f"Reading stream from: {input_path}")
        
        try:
            stream_df = self.spark.readStream \
                .format("csv") \
                .option("header", "true") \
                .option("maxFilesPerTrigger", "10") \
                .option("cleanSource", "archive") \
                .option("sourceArchiveDir", "/data/archive") \
                .schema(self.schema) \
                .load(input_path)
            
            return stream_df
            
        except Exception as e:
            logger.error(f"Failed to read stream: {e}")
            raise
    
    def transform_data(self, df):
        """
        Apply transformations - simplified version.
        
        Database will auto-generate event_id (SERIAL/AUTO_INCREMENT)
        No revenue column - we'll calculate it in queries if needed
        """
        logger.info("Applying transformations...")
        
        try:
            transformed_df = df \
                .withColumn("timestamp", to_timestamp(col("timestamp"))) \
                .withColumn("ingestion_timestamp", current_timestamp()) \
                .withColumn("processing_timestamp", current_timestamp()) \
                .withColumn("product_name", trim(col("product_name"))) \
                .withColumn("product_category", lower(trim(col("product_category")))) \
                .withColumn(
                    "price",
                    when(col("price") > 0, col("price")).otherwise(lit(0.00))
                ) \
                .withColumn(
                    "quantity",
                    when((col("quantity") > 0) & (col("quantity").isNotNull()), 
                         col("quantity")).otherwise(lit(1))
                )
            
            logger.info("Transformations applied successfully")
            return transformed_df
            
        except Exception as e:
            logger.error(f"Transformation failed: {e}")
            raise
    
    def write_to_postgres(self, df, checkpoint_location: str = "/data/checkpoints"):
        """
        Write streaming data to PostgreSQL using foreachBatch.
        """
        
        def write_batch(batch_df, batch_id):
            """
            Write each micro-batch to PostgreSQL.
            """
            try:
                if batch_df.isEmpty():
                    logger.info(f"Batch {batch_id}: Empty batch, skipping")
                    return
                
                start_time = datetime.now()
                record_count = batch_df.count()
                
                # Write to PostgreSQL (event_id will be auto-generated by database)
                batch_df.write \
                    .jdbc(
                        url=self.postgres_url,
                        table="ecommerce_events",
                        mode="append",
                        properties=self.postgres_properties
                    )
                
                # Calculate metrics
                duration = (datetime.now() - start_time).total_seconds()
                throughput = record_count / duration if duration > 0 else 0
                
                logger.info(
                    f"✓ Batch {batch_id}: {record_count} records in {duration:.2f}s "
                    f"({throughput:.0f} records/s)"
                )
                
            except Exception as e:
                logger.error(f"✗ Batch {batch_id} failed: {str(e)}")
                raise
        
        try:
            logger.info("Configuring PostgreSQL sink...")
            
            query = df.writeStream \
                .foreachBatch(write_batch) \
                .outputMode("append") \
                .option("checkpointLocation", checkpoint_location) \
                .trigger(processingTime="30 seconds") \
                .start()
            
            logger.info(f"Stream started with checkpoint: {checkpoint_location}")
            logger.info("=" * 68)
            logger.info(f"Query ID: {query.id}")
            logger.info("Streaming query is running. Press Ctrl+C to stop.")
            logger.info("=" * 68)
            
            return query
            
        except Exception as e:
            logger.error(f"Failed to start streaming query: {e}")
            raise
    
    def run(self):
        """
        Main execution method.
        """
        try:
            logger.info("=" * 68)
            logger.info("Starting E-commerce Stream Processor")
            logger.info("=" * 68)
            
            # Read stream
            raw_stream = self.read_stream()
            
            # Transform
            transformed_stream = self.transform_data(raw_stream)
            
            # Write to PostgreSQL
            query = self.write_to_postgres(transformed_stream)
            
            # Await termination
            query.awaitTermination()
            
        except KeyboardInterrupt:
            logger.info("\n" + "=" * 68)
            logger.info("Stopping stream gracefully...")
            logger.info("=" * 68)
            if 'query' in locals():
                query.stop()
            self.spark.stop()
            logger.info("Stream stopped successfully")
            
        except Exception as e:
            logger.error(f"Stream processor failed: {e}")
            if 'query' in locals():
                query.stop()
            self.spark.stop()
            raise


    def run_stream_once(self, input_dir: str):
        """
        Run a single micro-batch from the given input directory using trigger(once).
        """
        try:
            raw_stream = self.read_stream(input_dir)
            transformed_stream = self.transform_data(raw_stream)
            query = transformed_stream.writeStream \
                .foreachBatch(lambda df, bid: df.write.jdbc(
                    url=self.postgres_url,
                    table="ecommerce_events",
                    mode="append",
                    properties=self.postgres_properties
                )) \
                .option("checkpointLocation", "/data/checkpoints/test_once") \
                .trigger(once=True) \
                .start()
            return query
        except Exception as e:
            logger.error(f"run_stream_once failed: {e}")
            raise
if __name__ == "__main__":
    # Create and run processor
    processor = EcommerceStreamProcessor()
    processor.run()
