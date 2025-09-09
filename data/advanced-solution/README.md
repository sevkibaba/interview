   # NASA NEO Data Recaller - Advanced Solution

This is the advanced solution for the NASA Near Earth Object (NEO) data recaller system, built with PyArrow and Apache Spark for improved performance and scalability.

## Architecture Overview

The advanced solution consists of several key components:

### Core Components

1. **PyArrow Models** (`../models/models.py`)
   - Data classes and schemas for NEO data
   - PyArrow table management utilities
   - Type-safe data structures

2. **PyArrow Writer Service** (`writer_service_adv_sol.py`)
   - Data transformation using PyArrow
   - Parquet file writing and reading
   - Raw data management only (no aggregations)

3. **Main Script** (`recall_data_adv_sol.py`)
   - CLI interface for data fetching
   - Orchestrates the data pipeline
   - Supports backfill operations

4. **Spark Aggregation Job** (`spark_aggregation_job.py`)
   - Distributed aggregation processing
   - Handles large datasets efficiently
   - Calculates close approaches and yearly statistics

5. **Docker Infrastructure**
   - Containerized Spark environment
   - Easy deployment and scaling
   - Parameterized job execution

## Features

### PyArrow Integration
- **High Performance**: PyArrow provides faster data processing compared to pandas
- **Memory Efficiency**: Better memory management for large datasets
- **Schema Validation**: Strong typing and schema enforcement
- **Cross-Language**: Compatible with other Arrow-based tools

### Spark Processing
- **Distributed Computing**: Handles large datasets across multiple cores/nodes
- **Scalable Aggregations**: Efficient calculation of statistics
- **Partition Pruning**: Optimized reading of partitioned data
- **Fault Tolerance**: Built-in error handling and recovery

### Containerization
- **Docker Support**: Easy deployment and environment consistency
- **Parameterized Jobs**: Flexible batch processing with command-line arguments
- **Volume Mounting**: Persistent data storage
- **Multi-Service**: Docker Compose for complex workflows

## Installation

### Prerequisites
- Python 3.8+
- Docker and Docker Compose
- Java 11+ (for Spark)

### Setup

1. **Clone the repository and navigate to the data directory:**
   ```bash
   cd data
   ```

2. **Install Python dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Build the Docker image:**
   ```bash
   docker build -t neo-spark-job .
   ```

## Usage

### 1. Data Fetching (PyArrow-based)

#### Full Run
Fetch all 200 NEOs and save to parquet files:
```bash
python advanced-solution/recall_data_adv_sol.py --output-dir ./advanced-solution/output
```

**Note**: Aggregations are handled separately by Spark jobs. After fetching data, run the Spark job to calculate aggregations.

#### Backfill Specific Batches
```bash
# Backfill batches 1-20
python advanced-solution/recall_data_adv_sol.py --backfill 1 20 --output-dir ./advanced-solution/output

# Backfill batches 1-100
python advanced-solution/recall_data_adv_sol.py --backfill 1 100 --output-dir ./advanced-solution/output
```


### 2. Spark Aggregation Job

#### Using Docker (Recommended)
```bash
# Navigate to the data directory
cd ../data

# Build the Docker image
docker build -t neo-spark-job .

# Run with specific batch range
docker run --rm -u root \
  -v $(pwd)/advanced-solution/output:/app/input \
  -v $(pwd)/advanced-solution/output:/app/output \
  neo-spark-job \
  --start_batch 1 --end_batch 200 \
  --input_dir /app/input --output_dir /app/output

# Run with different batch range
docker run --rm -u root \
  -v $(pwd)/advanced-solution/output:/app/input \
  -v $(pwd)/advanced-solution/output:/app/output \
  neo-spark-job \
  --start_batch 21 --end_batch 40 \
  --input_dir /app/input --output_dir /app/output
```

#### Using Docker Compose
```bash
# Navigate to the data directory
cd ../data

# Run only the Spark job
docker-compose run neo-spark-job

# Run only the data fetcher
docker-compose run neo-data-fetcher

# Run the full pipeline (fetch + aggregate)
docker-compose run neo-full-pipeline
```

#### Direct Spark Execution
```bash
# Run Spark job directly (requires Spark installation)
spark-submit --master local[*] spark_aggregation_job.py \
  --start_batch 1 \
  --end_batch 200 \
  --input_dir ./output \
  --output_dir ./output
```

### 3. Environment Variables

Create a `.env` file in the project root:
```bash
API_KEY=your_nasa_api_key_here
OUTPUT_DIR=./output
LOG_LEVEL=INFO
```

## Data Schema

### NEO Data Schema (17 columns)
- `id`: NEO unique identifier
- `neo_reference_id`: NEO reference ID
- `name`: NEO name
- `name_limited`: Limited name
- `designation`: NEO designation
- `nasa_jpl_url`: JPL URL
- `absolute_magnitude_h`: Absolute magnitude
- `is_potentially_hazardous_asteroid`: Hazard flag
- `minimum_estimated_diameter_meters`: Min diameter in meters
- `maximum_estimated_diameter_meters`: Max diameter in meters
- `closest_approach_miss_distance_km`: Closest approach distance in km
- `closest_approach_date`: Closest approach date
- `closest_approach_relative_velocity_km_per_sec`: Relative velocity
- `first_observation_date`: First observation date
- `last_observation_date`: Last observation date
- `observations_used`: Number of observations
- `orbital_period`: Orbital period

### Aggregations Schema
- `metric`: Aggregation metric name
- `value`: Aggregation value

## File Structure

```
data/
├── advanced-solution/
│   ├── recall_data_adv_sol.py        # Main data fetching script
│   ├── spark_aggregation_job.py      # Spark aggregation job
│   └── README.md                    # This file
├── models/
│   └── models.py                    # PyArrow models and schemas
├── services/
│   └── writer_service_adv_sol.py   # PyArrow-based writer service
├── requirements.txt                  # Python dependencies
├── Dockerfile                        # Docker image definition
└── docker-compose.yml               # Multi-service orchestration
```

## Output Structure

```
output/
├── raw/
│   ├── batch-number=1-20/
│   │   └── neo_partition_1-20.parquet
│   ├── batch-number=21-40/
│   │   └── neo_partition_21-40.parquet
│   └── ... (continues for all batches)
└── aggregations/
    ├── batches-1-20/
    │   ├── part-00000-*.parquet
    │   ├── part-00001-*.parquet
    │   └── ... (multiple parquet files)
    ├── batches-21-40/
    │   ├── part-00000-*.parquet
    │   └── ... (multiple parquet files)
    └── ... (continues for all processed batches)
```

## Performance Characteristics

### PyArrow Benefits
- **2-3x faster** data processing compared to pandas
- **50% less memory** usage for large datasets
- **Better I/O performance** for parquet files
- **Schema validation** at runtime

### Spark Benefits
- **Parallel processing** across multiple cores
- **Scalable** to large datasets (millions of records)
- **Optimized** parquet reading with partition pruning
- **Fault tolerance** with automatic retry

### Expected Performance
- **Data Fetching**: ~5-10 seconds for 200 NEOs
- **PyArrow Processing**: ~1-2 seconds for transformations
- **Spark Aggregations**: ~2-5 seconds for 200 NEOs
- **Memory Usage**: ~100-200 MB peak during processing

## Troubleshooting

### Common Issues

1. **Docker Build Failures**
   ```bash
   # Clean Docker cache and rebuild
   docker system prune -a
   docker build --no-cache -t neo-spark-job .
   ```

2. **Spark Job Failures**
   ```bash
   # Check Spark logs
   docker logs <container_id>
   
   # Verify input data exists
   ls -la ./output/raw/
   ```

3. **Memory Issues**
   ```bash
   # Increase Docker memory limits
   # In Docker Desktop: Settings > Resources > Memory
   ```

4. **Permission Issues**
   ```bash
   # Fix script permissions
   chmod +x run_spark_job.sh
   ```

### Debug Mode

Enable debug logging:
```bash
export LOG_LEVEL=DEBUG
python recall_data_adv_sol.py --output-dir ./output
```

## Advanced Configuration

### Spark Configuration

Modify `spark_aggregation_job.py` for custom Spark settings:
```python
spark = SparkSession.builder \
    .appName("NEO Aggregation Job") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.memory", "1g") \
    .getOrCreate()
```

### PyArrow Configuration

Modify `../models/models.py` for custom PyArrow settings:
```python
# Enable compression
pa.parquet.write_table(table, filepath, compression='snappy')

# Enable dictionary encoding
pa.parquet.write_table(table, filepath, use_dictionary=True)
```

## Future Enhancements

### Planned Improvements
1. **Streaming Processing**: Real-time data processing with Spark Streaming
2. **ML Integration**: Machine learning pipelines for NEO classification
3. **Graph Processing**: Network analysis of NEO relationships
4. **Time Series**: Advanced time series analysis for orbital data
5. **API Gateway**: REST API for data access
6. **Monitoring**: Prometheus/Grafana integration
7. **Caching**: Redis integration for frequently accessed data

### Scalability Options
1. **Kubernetes**: Deploy on K8s for production scaling
2. **Distributed Storage**: S3/HDFS for large datasets
3. **Message Queues**: Kafka for real-time data ingestion
4. **Database Integration**: PostgreSQL/MongoDB for metadata
5. **Cloud Deployment**: AWS/Azure/GCP integration

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

This project is part of the Tekmetric interview process.
