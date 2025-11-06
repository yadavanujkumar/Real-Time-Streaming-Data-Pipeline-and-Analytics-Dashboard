"""
PySpark Structured Streaming ETL Job
Consumes IoT sensor data from Kafka, performs transformations, and writes to Hive/Parquet
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, avg, max, min, count,
    to_timestamp, current_timestamp, date_format
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)
import logging
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class IoTStreamingETL:
    """
    PySpark Structured Streaming job for processing IoT sensor data.
    """
    
    def __init__(self, 
                 kafka_bootstrap_servers='localhost:9092',
                 kafka_topic='iot-sensor-data',
                 checkpoint_location='/tmp/checkpoint',
                 output_path='/tmp/output'):
        """
        Initialize Streaming ETL job.
        
        Args:
            kafka_bootstrap_servers: Kafka broker address
            kafka_topic: Kafka topic to consume
            checkpoint_location: Directory for checkpointing
            output_path: Output directory for processed data
        """
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.kafka_topic = kafka_topic
        self.checkpoint_location = checkpoint_location
        self.output_path = output_path
        self.spark = None
        
        self._initialize_spark_session()
    
    def _initialize_spark_session(self):
        """Initialize Spark session with optimal configurations."""
        try:
            self.spark = SparkSession.builder \
                .appName("IoT-Sensor-Streaming-ETL") \
                .config("spark.sql.streaming.schemaInference", "true") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.streaming.stopGracefullyOnShutdown", "true") \
                .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
                .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
                .config("spark.sql.parquet.compression.codec", "snappy") \
                .config("spark.sql.shuffle.partitions", "4") \
                .config("spark.default.parallelism", "4") \
                .enableHiveSupport() \
                .getOrCreate()
            
            # Set log level
            self.spark.sparkContext.setLogLevel("WARN")
            logger.info("Spark session initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize Spark session: {e}")
            raise
    
    def define_schema(self):
        """
        Define schema for incoming JSON data from Kafka.
        
        Returns:
            StructType: Schema definition for sensor data
        """
        return StructType([
            StructField("sensor_id", StringType(), True),
            StructField("location", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("temperature", DoubleType(), True),
            StructField("humidity", DoubleType(), True),
            StructField("pressure", DoubleType(), True),
            StructField("battery_level", DoubleType(), True),
            StructField("status", StringType(), True)
        ])
    
    def read_from_kafka(self):
        """
        Read streaming data from Kafka topic.
        
        Returns:
            DataFrame: Streaming DataFrame from Kafka
        """
        try:
            df = self.spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
                .option("subscribe", self.kafka_topic) \
                .option("startingOffsets", "latest") \
                .option("failOnDataLoss", "false") \
                .load()
            
            logger.info(f"Reading stream from Kafka topic: {self.kafka_topic}")
            return df
            
        except Exception as e:
            logger.error(f"Error reading from Kafka: {e}")
            raise
    
    def transform_data(self, raw_df):
        """
        Parse and transform streaming data.
        
        Args:
            raw_df: Raw streaming DataFrame from Kafka
            
        Returns:
            DataFrame: Transformed DataFrame
        """
        schema = self.define_schema()
        
        # Parse JSON from Kafka value
        parsed_df = raw_df.select(
            from_json(col("value").cast("string"), schema).alias("data")
        ).select("data.*")
        
        # Convert timestamp string to timestamp type
        transformed_df = parsed_df.withColumn(
            "event_timestamp",
            to_timestamp(col("timestamp"))
        )
        
        # Add processing timestamp
        transformed_df = transformed_df.withColumn(
            "processing_timestamp",
            current_timestamp()
        )
        
        # Add date and hour partitions
        transformed_df = transformed_df \
            .withColumn("date", date_format(col("event_timestamp"), "yyyy-MM-dd")) \
            .withColumn("hour", date_format(col("event_timestamp"), "HH"))
        
        # Data quality: filter out null sensor_ids and invalid readings
        cleaned_df = transformed_df.filter(
            (col("sensor_id").isNotNull()) &
            (col("temperature").isNotNull()) &
            (col("temperature") > -50) & (col("temperature") < 100)
        )
        
        logger.info("Data transformation completed")
        return cleaned_df
    
    def aggregate_data(self, transformed_df):
        """
        Perform windowed aggregations on streaming data.
        
        Args:
            transformed_df: Transformed DataFrame
            
        Returns:
            DataFrame: Aggregated DataFrame
        """
        # Aggregate by sensor and 1-minute window
        aggregated_df = transformed_df \
            .withWatermark("event_timestamp", "10 minutes") \
            .groupBy(
                window(col("event_timestamp"), "1 minute"),
                col("sensor_id"),
                col("location")
            ) \
            .agg(
                avg("temperature").alias("avg_temperature"),
                max("temperature").alias("max_temperature"),
                min("temperature").alias("min_temperature"),
                avg("humidity").alias("avg_humidity"),
                max("humidity").alias("max_humidity"),
                min("humidity").alias("min_humidity"),
                avg("pressure").alias("avg_pressure"),
                avg("battery_level").alias("avg_battery_level"),
                count("*").alias("reading_count")
            )
        
        # Flatten window structure
        aggregated_df = aggregated_df.select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("sensor_id"),
            col("location"),
            col("avg_temperature"),
            col("max_temperature"),
            col("min_temperature"),
            col("avg_humidity"),
            col("max_humidity"),
            col("min_humidity"),
            col("avg_pressure"),
            col("avg_battery_level"),
            col("reading_count")
        )
        
        # Add partition columns
        aggregated_df = aggregated_df \
            .withColumn("date", date_format(col("window_start"), "yyyy-MM-dd")) \
            .withColumn("hour", date_format(col("window_start"), "HH"))
        
        logger.info("Data aggregation completed")
        return aggregated_df
    
    def write_to_parquet(self, df, output_name="raw_data"):
        """
        Write streaming data to Parquet files with partitioning.
        
        Args:
            df: DataFrame to write
            output_name: Name for output directory
            
        Returns:
            StreamingQuery: The streaming query object
        """
        try:
            query = df.writeStream \
                .outputMode("append") \
                .format("parquet") \
                .option("path", f"{self.output_path}/{output_name}") \
                .option("checkpointLocation", f"{self.checkpoint_location}/{output_name}") \
                .partitionBy("date", "hour") \
                .trigger(processingTime='30 seconds') \
                .start()
            
            logger.info(f"Writing stream to Parquet: {self.output_path}/{output_name}")
            return query
            
        except Exception as e:
            logger.error(f"Error writing to Parquet: {e}")
            raise
    
    def write_to_console(self, df, output_name="console"):
        """
        Write streaming data to console for debugging.
        
        Args:
            df: DataFrame to write
            output_name: Query name
            
        Returns:
            StreamingQuery: The streaming query object
        """
        try:
            query = df.writeStream \
                .outputMode("append") \
                .format("console") \
                .option("truncate", "false") \
                .queryName(output_name) \
                .trigger(processingTime='10 seconds') \
                .start()
            
            logger.info(f"Writing stream to console: {output_name}")
            return query
            
        except Exception as e:
            logger.error(f"Error writing to console: {e}")
            raise
    
    def write_aggregated_to_parquet(self, df):
        """
        Write aggregated data to Parquet with complete mode.
        
        Args:
            df: Aggregated DataFrame
            
        Returns:
            StreamingQuery: The streaming query object
        """
        try:
            query = df.writeStream \
                .outputMode("update") \
                .format("parquet") \
                .option("path", f"{self.output_path}/aggregated_data") \
                .option("checkpointLocation", f"{self.checkpoint_location}/aggregated_data") \
                .partitionBy("date", "hour") \
                .trigger(processingTime='1 minute') \
                .start()
            
            logger.info(f"Writing aggregated stream to Parquet: {self.output_path}/aggregated_data")
            return query
            
        except Exception as e:
            logger.error(f"Error writing aggregated data: {e}")
            raise
    
    def run(self, mode="all"):
        """
        Run the streaming ETL pipeline.
        
        Args:
            mode: Execution mode - "raw", "aggregated", "all", or "console"
        """
        logger.info("Starting IoT Sensor Streaming ETL job")
        logger.info(f"Mode: {mode}")
        
        try:
            # Read from Kafka
            raw_df = self.read_from_kafka()
            
            # Transform data
            transformed_df = self.transform_data(raw_df)
            
            queries = []
            
            if mode in ["raw", "all"]:
                # Write raw transformed data
                query_raw = self.write_to_parquet(transformed_df, "raw_data")
                queries.append(query_raw)
            
            if mode in ["aggregated", "all"]:
                # Aggregate and write
                aggregated_df = self.aggregate_data(transformed_df)
                query_agg = self.write_aggregated_to_parquet(aggregated_df)
                queries.append(query_agg)
            
            if mode == "console":
                # Write to console for debugging
                query_console = self.write_to_console(transformed_df, "raw_console")
                queries.append(query_console)
                
                aggregated_df = self.aggregate_data(transformed_df)
                query_agg_console = self.write_to_console(aggregated_df, "aggregated_console")
                queries.append(query_agg_console)
            
            logger.info("All streaming queries started successfully")
            
            # Wait for all queries to terminate
            for query in queries:
                query.awaitTermination()
                
        except KeyboardInterrupt:
            logger.info("Streaming job interrupted by user")
        except Exception as e:
            logger.error(f"Error in streaming job: {e}")
            raise
        finally:
            if self.spark:
                self.spark.stop()
                logger.info("Spark session stopped")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='IoT Sensor Streaming ETL Job')
    parser.add_argument(
        '--kafka-bootstrap-servers',
        default='localhost:9092',
        help='Kafka bootstrap servers (default: localhost:9092)'
    )
    parser.add_argument(
        '--kafka-topic',
        default='iot-sensor-data',
        help='Kafka topic name (default: iot-sensor-data)'
    )
    parser.add_argument(
        '--checkpoint-location',
        default='/tmp/checkpoint',
        help='Checkpoint directory (default: /tmp/checkpoint)'
    )
    parser.add_argument(
        '--output-path',
        default='/tmp/output',
        help='Output directory for Parquet files (default: /tmp/output)'
    )
    parser.add_argument(
        '--mode',
        choices=['raw', 'aggregated', 'all', 'console'],
        default='all',
        help='Execution mode (default: all)'
    )
    
    args = parser.parse_args()
    
    # Create and run ETL job
    etl = IoTStreamingETL(
        kafka_bootstrap_servers=args.kafka_bootstrap_servers,
        kafka_topic=args.kafka_topic,
        checkpoint_location=args.checkpoint_location,
        output_path=args.output_path
    )
    
    etl.run(mode=args.mode)
