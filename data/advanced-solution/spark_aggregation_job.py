#!/usr/bin/env python3
"""
Advanced Solution: Spark Job for NEO Data Aggregations

This Spark job reads parquet files containing NEO data and calculates aggregations
for close approaches under 0.2 AU and yearly breakdowns.

Usage:
    spark-submit spark_aggregation_job.py --start_batch 1 --end_batch 200 --input_dir /path/to/input --output_dir /path/to/output
"""

import argparse
import logging
import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, when, count, sum as spark_sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType

# Add utils to path for error handling
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))
from utils.error_handling import (
    handle_errors, DataProcessingError, FileOperationError,
    log_and_continue, ErrorSeverity
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("neo_aggregation_job")


@handle_errors("spark_session", ErrorSeverity.CRITICAL)
def create_spark_session():
    """
    Create and configure Spark session.
    """
    try:
        return SparkSession.builder \
            .appName("NEO Aggregation Job") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
            .config("spark.hadoop.parquet.enable.summary-metadata", "false") \
            .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
            .config("spark.hadoop.fs.local.impl", "org.apache.hadoop.fs.LocalFileSystem") \
            .config("spark.hadoop.fs.file.impl.disable.cache", "true") \
            .config("spark.hadoop.fs.local.impl.disable.cache", "true") \
            .getOrCreate()
    except Exception as e:
        raise DataProcessingError(f"Failed to create Spark session: {str(e)}", 
                                ErrorSeverity.CRITICAL, "spark_session", e)


def define_neo_schema():
    """
    Define the schema for NEO data.
    """
    return StructType([
        StructField("id", StringType(), True),
        StructField("neo_reference_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("name_limited", StringType(), True),
        StructField("designation", StringType(), True),
        StructField("nasa_jpl_url", StringType(), True),
        StructField("absolute_magnitude_h", DoubleType(), True),
        StructField("is_potentially_hazardous_asteroid", BooleanType(), True),
        StructField("minimum_estimated_diameter_meters", DoubleType(), True),
        StructField("maximum_estimated_diameter_meters", DoubleType(), True),
        StructField("closest_approach_miss_distance_km", DoubleType(), True),
        StructField("closest_approach_date", StringType(), True),
        StructField("closest_approach_relative_velocity_km_per_sec", DoubleType(), True),
        StructField("first_observation_date", StringType(), True),
        StructField("last_observation_date", StringType(), True),
        StructField("observations_used", IntegerType(), True),
        StructField("orbital_period", DoubleType(), True),
        StructField("close_approach_data", StringType(), True)  # JSON string containing full close approach data
    ])


@handle_errors("spark_reader", ErrorSeverity.HIGH)
def read_neo_data(spark, input_dir, start_batch, end_batch):
    """
    Read NEO data from parquet files for the specified batch range.
    """
    logger.info(f"Reading NEO data from {input_dir} for batches {start_batch} to {end_batch}")
    
    # Calculate partition ranges
    start_partition = ((start_batch - 1) // 20) * 20 + 1
    end_partition = ((end_batch - 1) // 20) * 20 + 1
    
    logger.info(f"Reading partitions from {start_partition} to {end_partition}")
    
    # Build the path pattern for reading specific partitions
    raw_data_path = f"{input_dir}/raw"
    
    # Generate specific partition paths based on batch range
    partition_paths = []
    current_partition = start_partition
    while current_partition <= end_partition:
        partition_dir = f"batch-number={current_partition}-{current_partition + 19}"
        partition_path = f"{raw_data_path}/{partition_dir}/*.parquet"
        partition_paths.append(partition_path)
        current_partition += 20
    
    logger.info(f"Reading from partition paths: {partition_paths}")
    logger.info(f"Total partitions to read: {len(partition_paths)}")
    
    try:
        # Read only the specific parquet files
        df = spark.read \
            .schema(define_neo_schema()) \
            .parquet(*partition_paths)
        
        total_records = df.count()
        logger.info(f"Read {total_records} records from parquet files")
        
        # Log partition information for distribution analysis
        logger.info(f"DataFrame partitions: {df.rdd.getNumPartitions()}")
        logger.info(f"DataFrame schema: {df.schema}")
        
        return df
    except Exception as e:
        raise FileOperationError(f"Failed to read parquet files from {input_dir}", 
                               input_dir, e)


@handle_errors("spark_aggregation", ErrorSeverity.HIGH)
def calculate_aggregations(df, spark):
    """
    Calculate the required aggregations:
    1. Total number of close approaches under 0.2 AU
    2. Number of close approaches by year
    """
    logger.info("Calculating aggregations")
    logger.info(f"Input DataFrame has {df.rdd.getNumPartitions()} partitions")
    
    try:
        # Parse JSON and explode close approach data
        from pyspark.sql.functions import from_json, explode, col, year, split, regexp_extract
        from pyspark.sql.types import ArrayType, StructType, StructField, StringType, DoubleType
        
        # Define schema for close approach data
        close_approach_schema = ArrayType(StructType([
            StructField("close_approach_date", StringType(), True),
            StructField("epoch_date_close_approach", StringType(), True),
            StructField("relative_velocity", StructType([
                StructField("kilometers_per_second", StringType(), True),
                StructField("kilometers_per_hour", StringType(), True),
                StructField("miles_per_hour", StringType(), True)
            ]), True),
            StructField("miss_distance", StructType([
                StructField("astronomical", StringType(), True),
                StructField("lunar", StringType(), True),
                StructField("kilometers", StringType(), True),
                StructField("miles", StringType(), True)
            ]), True),
            StructField("orbiting_body", StringType(), True)
        ]))
        
        # Parse JSON and explode close approach data
        logger.info("Parsing and exploding close approach data...")
        df_with_approaches = df.filter(col("close_approach_data").isNotNull()) \
            .withColumn("close_approach_parsed", from_json(col("close_approach_data"), close_approach_schema)) \
            .withColumn("approach", explode(col("close_approach_parsed"))) \
            .drop("close_approach_data", "close_approach_parsed")
        
        logger.info(f"Exploded DataFrame has {df_with_approaches.rdd.getNumPartitions()} partitions")
        
        # Filter for close approaches under 0.2 AU
        logger.info("Filtering for close approaches under 0.2 AU...")
        df_close_approaches = df_with_approaches \
            .filter(col("approach.miss_distance.astronomical").cast("double") < 0.2)
        
        logger.info(f"Filtered DataFrame has {df_close_approaches.rdd.getNumPartitions()} partitions")
        
        # 1. Total number of close approaches under 0.2 AU
        total_close_approaches = df_close_approaches.count()
        logger.info(f"Total close approaches under 0.2 AU: {total_close_approaches}")
        
        # 2. Extract year and count by year
        df_with_year = df_close_approaches \
            .withColumn("approach_year", year(col("approach.close_approach_date")))
        
        # Count approaches by year
        yearly_aggregations = df_with_year \
            .filter(col("approach_year").isNotNull()) \
            .groupBy("approach_year") \
            .agg(count("*").alias("count")) \
            .orderBy("approach_year")
        
        # Convert yearly data to the expected format
        yearly_data = yearly_aggregations.collect()
        
        aggregations_data = [
            ("total_close_approaches_under_02_au", total_close_approaches)
        ]
        
        # Add yearly breakdowns
        for row in yearly_data:
            year_val = row["approach_year"]
            count_val = row["count"]
            aggregations_data.append((f"close_approaches_year_{year_val}", count_val))
        
        # Create final DataFrame
        aggregations_df = spark.createDataFrame(aggregations_data, ["metric", "value"])
        
        logger.info(f"Total close approaches under 0.2 AU: {total_close_approaches}")
        logger.info(f"Yearly breakdown: {len(yearly_data)} years with close approaches")
        
        return aggregations_df
        
    except Exception as e:
        raise DataProcessingError(f"Failed to calculate aggregations: {str(e)}", 
                                ErrorSeverity.HIGH, "spark_aggregation", e)


@handle_errors("spark_writer", ErrorSeverity.HIGH)
def write_aggregations(aggregations_df, output_dir, start_batch, end_batch):
    """
    Write aggregations to output directory with batch range in the path.
    """
    logger.info(f"Writing aggregations to {output_dir}")
    
    try:
        # Create output path with batch range
        batch_range = f"batches-{start_batch}-{end_batch}"
        output_path = f"{output_dir}/aggregations/{batch_range}"

        # used coalesce for simplicity, not production code.
        aggregations_df.coalesce(1).write \
            .mode("overwrite") \
            .option("parquet.enable.summary-metadata", "false") \
            .option("parquet.enable.dictionary", "false") \
            .option("parquet.block.size", "134217728") \
            .option("mapreduce.fileoutputcommitter.algorithm.version", "2") \
            .option("mapreduce.fileoutputcommitter.cleanup-failures.ignored", "true") \
            .parquet(output_path)
        
        logger.info(f"Aggregations written to {output_path}")
        
        # Show results
        logger.info("Aggregation results:")
        aggregations_df.show(truncate=False)
        
        return output_path
        
    except Exception as e:
        raise FileOperationError(f"Failed to write aggregations to {output_dir}", 
                               output_dir, e)


def main():
    """
    Main function for the Spark aggregation job.
    """
    parser = argparse.ArgumentParser(description='NEO Data Aggregation Spark Job')
    parser.add_argument('--start_batch', type=int, required=True, help='Starting batch number')
    parser.add_argument('--end_batch', type=int, required=True, help='Ending batch number')
    parser.add_argument('--input_dir', type=str, required=True, help='Input directory containing parquet files')
    parser.add_argument('--output_dir', type=str, required=True, help='Output directory for aggregations')
    
    args = parser.parse_args()
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        logger.info("Starting NEO aggregation job")
        logger.info(f"Parameters: start_batch={args.start_batch}, end_batch={args.end_batch}")
        logger.info(f"Input: {args.input_dir}, Output: {args.output_dir}")
        
        # Read NEO data
        df = read_neo_data(spark, args.input_dir, args.start_batch, args.end_batch)
        
        # Calculate aggregations
        aggregations_df = calculate_aggregations(df, spark)
        
        # Write results
        output_path = write_aggregations(aggregations_df, args.output_dir, args.start_batch, args.end_batch)
        
        logger.info("Aggregation job completed successfully")
        return 0
        
    except Exception as e:
        logger.error(f"Error in aggregation job: {e}", exc_info=True)
        return 1
    
    finally:
        spark.stop()


if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)
