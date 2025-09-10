#!/bin/bash

# Helper script to submit Spark jobs to the cluster
# Usage: ./submit_spark_job.sh <start_batch> <end_batch>

if [ $# -ne 2 ]; then
    echo "Usage: $0 <start_batch> <end_batch>"
    echo "Example: $0 1 20"
    exit 1
fi

START_BATCH=$1
END_BATCH=$2

echo "Submitting Spark job for batches $START_BATCH to $END_BATCH to the cluster..."

docker-compose -f docker-compose-cluster.yml exec spark-submit spark-submit \
    --master spark://spark-master:7077 \
    --py-files /app/models/models.py,/app/services/writer_service_adv_sol.py \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.sql.adaptive.coalescePartitions.enabled=true \
    --conf spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2 \
    --conf spark.hadoop.parquet.enable.summary-metadata=false \
    /app/spark_aggregation_job.py \
    --start_batch $START_BATCH \
    --end_batch $END_BATCH \
    --input_dir /data \
    --output_dir /data

echo "Job submitted! Check the Spark Web UI at http://localhost:8080 to monitor progress."


