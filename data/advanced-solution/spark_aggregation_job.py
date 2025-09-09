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
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, when, count, sum as spark_sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("neo_aggregation_job")


def create_spark_session():
    """
    Create and configure Spark session.
    """
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
    
    # Read only the specific parquet files
    df = spark.read \
        .schema(define_neo_schema()) \
        .parquet(*partition_paths)
    
    logger.info(f"Read {df.count()} records from parquet files")
    return df


def calculate_close_approaches_aggregations(df, spark):
    """
    Calculate close approaches aggregations from the NEO data by exploding the close_approach_data.
    """
    logger.info("Calculating close approaches aggregations")
    
    # Parse JSON and explode close approach data
    from pyspark.sql.functions import from_json, explode, col, when, size, split, regexp_extract
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
    df_with_approaches = df.filter(col("close_approach_data").isNotNull()) \
        .withColumn("close_approach_parsed", from_json(col("close_approach_data"), close_approach_schema)) \
        .withColumn("approach", explode(col("close_approach_parsed"))) \
        .drop("close_approach_data", "close_approach_parsed")
    
    # Count total NEOs
    total_neos = df.count()
    
    # Count hazardous asteroids
    hazardous_count = df.filter(col("is_potentially_hazardous_asteroid") == True).count()
    
    # Count NEOs with close approach data
    neos_with_close_approaches = df.filter(col("close_approach_data").isNotNull()).count()
    
    # Count close approaches under 0.2 AU
    close_approaches_under_02_au = df_with_approaches \
        .filter(col("approach.miss_distance.astronomical").cast("double") < 0.2) \
        .count()
    
    # Create aggregations DataFrame
    aggregations_data = [
        ("total_neos", total_neos),
        ("hazardous_asteroids", hazardous_count),
        ("neos_with_close_approach_data", neos_with_close_approaches),
        ("close_approaches_under_02_au", close_approaches_under_02_au)
    ]
    
    aggregations_df = spark.createDataFrame(aggregations_data, ["metric", "value"])
    
    logger.info(f"Total NEOs: {total_neos}")
    logger.info(f"Hazardous asteroids: {hazardous_count}")
    logger.info(f"NEOs with close approach data: {neos_with_close_approaches}")
    logger.info(f"Close approaches under 0.2 AU: {close_approaches_under_02_au}")
    
    return aggregations_df


def calculate_yearly_aggregations(df, spark):
    """
    Calculate yearly aggregations from the NEO data by exploding close_approach_data.
    """
    logger.info("Calculating yearly aggregations")
    
    # Parse JSON and explode close approach data
    from pyspark.sql.functions import from_json, explode, col, when, size, split, regexp_extract, year
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
    df_with_approaches = df.filter(col("close_approach_data").isNotNull()) \
        .withColumn("close_approach_parsed", from_json(col("close_approach_data"), close_approach_schema)) \
        .withColumn("approach", explode(col("close_approach_parsed"))) \
        .drop("close_approach_data", "close_approach_parsed")
    
    # Filter for close approaches under 0.2 AU and extract year
    df_with_year = df_with_approaches \
        .filter(col("approach.miss_distance.astronomical").cast("double") < 0.2) \
        .withColumn("approach_year", year(col("approach.close_approach_date")))
    
    # Count approaches by year
    yearly_aggregations = df_with_year \
        .filter(col("approach_year").isNotNull()) \
        .groupBy("approach_year") \
        .agg(count("*").alias("count")) \
        .orderBy("approach_year")
    
    # Convert to the expected format
    yearly_data = yearly_aggregations.collect()
    
    aggregations_data = []
    for row in yearly_data:
        year_val = row["approach_year"]
        count_val = row["count"]
        aggregations_data.append((f"close_approaches_year_{year_val}", count_val))
    
    # Convert to DataFrame
    if aggregations_data:
        yearly_df = spark.createDataFrame(aggregations_data, ["metric", "value"])
    else:
        yearly_df = spark.createDataFrame([], ["metric", "value"])
    
    return yearly_df


def write_aggregations(aggregations_df, yearly_df, output_dir, start_batch, end_batch):
    """
    Write aggregations to output directory with batch range in the path.
    """
    logger.info(f"Writing aggregations to {output_dir}")
    
    # Combine all aggregations
    all_aggregations = aggregations_df.union(yearly_df)
    
    # Create output path with batch range
    batch_range = f"batches-{start_batch}-{end_batch}"
    output_path = f"{output_dir}/aggregations/{batch_range}"
    
    all_aggregations.write \
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
    all_aggregations.show(truncate=False)
    
    return output_path


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
        aggregations_df = calculate_close_approaches_aggregations(df, spark)
        yearly_df = calculate_yearly_aggregations(df, spark)
        
        # Write results
        output_path = write_aggregations(aggregations_df, yearly_df, args.output_dir, args.start_batch, args.end_batch)
        
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
