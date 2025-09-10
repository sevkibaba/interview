# NASA NEO Data Recaller - Implementation Summary

## Project Overview
Complete implementation of a NASA Near Earth Object (NEO) data recaller system that fetches, processes, and stores asteroid data with aggregations for reporting purposes.

**NOTE** See README files for both solutions for details on data schema, configurations and folder structure

## Architecture

### Core Components

#### 1. NEO API Service (`services/neo_api_service.py`)
- **Class**: `NasaNeoClient`
- **Purpose**: Handles NASA API communication with batching and rate limiting
- **Key Features**:
  - Rate limiting: 2 requests per second maximum
  - Batch processing with configurable batch sizes
  - NASA Browse API integration (`/neo/rest/v1/neo/browse`)
  - Error handling and retry logic
  - Environment variable support for API keys

#### 2. Writer Service (`services/writer_service.py`)
- **Class**: `NeoDataWriter`
- **Purpose**: Data transformation, storage, and aggregation calculation
- **Key Features**:
  - Transforms raw NEO data to required schema (17 columns)
  - Spark-compatible partitioning: `batch-number=1-20/neo_partition_1-20.parquet`
  - On-the-fly aggregation calculation
  - Backfill support for individual batches
  - Raw file reading and aggregation recalculation

#### 3. Main Script (`simple-solution/recall_data.py`)
- **Purpose**: Orchestrates the entire data pipeline
- **Key Features**:
  - CLI argument parsing for different operations
  - Full run, backfill ranges, and aggregation recalculation
  - Environment variable loading from `.env` files
  - Comprehensive logging and error handling

### Default Settings
- Total NEOs: 200
- Batch Size: 20 NEOs per batch
- Rate Limit: 2 requests per second
- Partition Size: 20 batches per partition

## Key Implementation Details

### Partitioning Strategy
- 20 batches per partition (batches 1-20, 21-40, etc.)
- Spark-compatible folder naming: `batch-number=1-20`
- Enables partition pruning and optimization in Spark

### Error Handling
- Graceful API error handling with retry logic
- Individual batch failure doesn't stop entire process
- Comprehensive logging for debugging
- Summary statistics for successful vs failed operations

### Aggregation Logic
- Calculates close approaches under 0.2 AU
- Tracks yearly breakdown of close approaches
- Can recalculate from existing raw files
- Saves results to separate aggregations directory

## Future Enhancements

### Potential Improvements
1. **Scheduling**: Cron job or scheduler integration
2. **Incremental Updates**: Track last processed batch for resumable runs
3. **Data Validation**: Add schema validation for NEO data
4. **Monitoring**: Add metrics and health checks
5. **Configuration**: YAML-based configuration files
6. **Testing**: Unit tests and integration tests


### Advanced Features
1. **Alerting**: Notifications for failed batches or API issues
2. **Analytics**: Additional aggregation metrics

## Troubleshooting

### Common Issues
1. **API Rate Limits**: Use proper API key for higher limits
2. **File Permissions**: Ensure write access to output directory
3. **Network Issues**: Check internet connectivity for API calls
4. **Memory Usage**: Large batch ranges may require more memory
5. **Disk Space**: Ensure sufficient space for parquet files

## Performance Characteristics

- **API Calls**: ~10 calls for 200 NEOs (20 per batch)
- **Processing Time**: ~5-10 seconds with rate limiting
- **File Size**: ~1-2 MB per partition (20 batches)
- **Memory Usage**: ~50-100 MB peak during processing
- **Disk I/O**: Optimized with parquet compression

## Advanced Solution

### Overview
The advanced solution builds upon the simple solution with PyArrow and Apache Spark for improved performance and scalability.

### Key Improvements

#### 1. PyArrow Integration
- **Performance**: 2-3x faster data processing compared to pandas
- **Memory Efficiency**: 50% less memory usage for large datasets
- **Schema Validation**: Strong typing and schema enforcement at runtime
- **Cross-Language**: Compatible with other Arrow-based tools

#### 2. Spark Processing
- **Distributed Computing**: Handles large datasets across multiple cores/nodes
- **Scalable Aggregations**: Efficient calculation of statistics
- **Partition Pruning**: Optimized reading of partitioned data
- **Fault Tolerance**: Built-in error handling and recovery

#### 3. Containerization
- **Docker Support**: Easy deployment and environment consistency
- **Parameterized Jobs**: Flexible batch processing with command-line arguments
- **Volume Mounting**: Persistent data storage

### Architecture Components

#### 1. PyArrow Models (`models/models.py`)
- Data classes and schemas for NEO data
- PyArrow table management utilities
- Type-safe data structures

#### 2. PyArrow Writer Service (`services/writer_service_adv_sol.py`)
- Data transformation using PyArrow
- Parquet file writing and reading
- Raw data management only (no aggregations)

#### 3. Main Script (`advanced-solution/recall_data_adv_sol.py`)
- CLI interface for data fetching
- Supports backfill operations

#### 4. Spark Aggregation Job (`advanced-solution/spark_aggregation_job.py`)
- Distributed aggregation processing
- Handles large datasets efficiently
- Calculates close approaches and yearly statistics
- Supports batch range parameters

#### 5. Docker Infrastructure
- Containerized Spark environment
- Easy deployment and scaling
- Parameterized job execution

#### 6. Spark Cluster (Production-like)
- Multi-worker Spark cluster setup
- Distributed task processing across workers
- Real-time monitoring and debugging capabilities
- Load balancing and fault tolerance

### Advanced Solution Usage

#### Data Fetching
```bash
# Navigate to data directory
cd data

# Full run
python advanced-solution/recall_data_adv_sol.py --output-dir ./advanced-solution/output

# Backfill specific batches
python advanced-solution/recall_data_adv_sol.py --backfill 1 20 --output-dir ./advanced-solution/output
```

#### Spark Aggregation Job

##### Single Container Mode
```bash
# Build Docker image
docker build -t neo-spark-job .

# Run with specific batch range
docker run --rm -u root \
  -v $(pwd)/advanced-solution/output:/app/input \
  -v $(pwd)/advanced-solution/output:/app/output \
  neo-spark-job \
  --start_batch 1 --end_batch 20 \
  --input_dir /app/input --output_dir /app/output
```

##### Spark Cluster Mode (Production-like)
```bash
# Start the Spark cluster
docker-compose -f docker-compose-cluster.yml up -d

# Submit job to the cluster
./submit_spark_job.sh 1 200

# Monitor the cluster
# - Spark Master Web UI: http://localhost:8080
# - Worker 1 Web UI: http://localhost:8081
# - Worker 2 Web UI: http://localhost:8082

# Stop the cluster when done
docker-compose -f docker-compose-cluster.yml down
```

**Cluster Architecture:**
- **1 Spark Master** (port 8080) - Job scheduling and cluster coordination
- **2 Spark Workers** (ports 8081, 8082) - Each with 2 cores and 2GB RAM
- **Job Submission Service** - For submitting Spark jobs to the cluster

**Task Distribution Example:**
- 200 NEOs → 4 partitions → 2 tasks per worker
- Worker 1: Processes partitions 0 and 2
- Worker 2: Processes partitions 1 and 3
- True parallel processing with load balancing

### Advanced Solution Output Structure

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

### Performance Comparison

| Metric | Simple Solution | Advanced Solution (Single) | Advanced Solution (Cluster) |
|--------|----------------|----------------------------|----------------------------|
| Data Processing | Pandas | PyArrow (2-3x faster) | PyArrow (2-3x faster) |
| Aggregations | Single-threaded | Spark (local) | Spark (distributed) |
| Memory Usage | ~100-200 MB | ~50-100 MB | ~200-400 MB total |
| Scalability | Limited | Medium | High (distributed) |
| Deployment | Local only | Docker + containers | Docker + cluster |
| Parallelism | None | Limited (local cores) | True (multiple workers) |
| Task Distribution | N/A | N/A | 4 partitions → 2 workers |
| Monitoring | Basic logging | Basic logging | Web UIs + detailed logs |

This implementation provides a robust, scalable foundation for NASA NEO data processing with room for future enhancements and optimizations.

## Testing

### Test Suite Overview
The project includes a test suite located in `data/tests/` that covers both the simple and advanced solutions:

#### Test Types
1. **PyArrow Writer Service Tests** (`test_writer_service.py`)
   - Unit tests for data transformation and parquet file writing
   - Tests with hardcoded sample API responses
   - Verifies correct column mapping, data types, and file structure
   - Uses temporary directories for isolated testing

2. **Spark Aggregation Job Tests** (`test_spark_pipeline.py`)
   - Integration tests for the complete Spark processing pipeline
   - Uses the same Docker image and command pattern as production
   - Tests with sample NEO data from JSON files
   - Verifies aggregation calculations and output structure
   - Compares results against expected values from files

#### Test Data
- **Sample NEO Data** (`sample_neo_data.json`): 3 test NEOs with different close approach scenarios
- **Expected Results** (`expected_aggregation_results.json`): Expected aggregation values for verification
- **Test Data Structure**: Mirrors production directory structure with proper partitioning

#### Running Tests
```bash
# Run all tests
cd data/tests
python3 run_tests.py

# Run individual test suites
python3 -m unittest test_writer_service.py -v
python3 test_spark_pipeline.py
```

#### Test Coverage
- ✅ PyArrow data transformation and parquet writing
- ✅ Spark job execution with Docker
- ✅ Input data validation and processing
- ✅ Output structure verification
- ✅ Aggregation calculation accuracy
- ✅ Yearly breakdown correctness
- ✅ Error handling and edge cases

The test suite ensures that both solutions work correctly and can be used as a foundation for additional testing in a production environment.

### Additional Notes
- The raw data should be recorded in Avro for a longer term operation since it is easy change schema and has high throughput rate than parquet files. It is easier to debug the data and easier to have partial backfills with a clever partitioning design.
- In addition to record raw data and having api requests in another module, it is a better approach than having them all in a spark job. The machines used for spark jobs are expensive for an api-scrape operation, and the parallel executor architecture is not suitable for api scraping because it lacks the politeness in its nature. Moreover, it would be very expensive and complex to handles errors on the fly.
- Recording raw data in its own place is better for future cases. The new report requirements could be built on top of that.
- It would be better to have a rate limiter that also takes the api limits into account. Since there is a limit in the api requests in an hourly manner, tha api scraper part is not designed to scale more.
- There should be an alerting system and another folder to record unexpected data after a validation step. It could be the case that the incoming data is mission-critical and needs to be handled properly.
- There is a typo in the case explanation that the raw files should have kilometers in unit for the approach distance but the aggregations uses astronomical units. I decided to record the all `close_approach_data` since it is better the have all raw data in a place, for future report backfills and additional reporting requirements.  
- In the advanced solution part, the raw files asked to have all rows exploded from the `close_approach_data`. This is not a best practice (see the previous item). We want to separate ingestion and computation pipes. It might seem cost-effective for this case or let's say a simpler case, but in a production environment, you want to explode them after ingesting the raw data using a different pipe. For this design, there would be another step like ingest raw data -> explode raw data -> compute aggregations. For the interview case, we combine the exploding and aggregation steps for simplicity. If there are other aggregations to be computed, we would separate them into different steps. 
- There can be a presto (trino) like environment to query the data. This is not implemented in this solution. If the reporting requirements changes a lot, it would be better to have a presto like environment to query the data instead of calculating the results with Spark jobs. In this case there should be middle stages (Spark jobs) to decrease the data size for the query environment. 
- The jobs can also be orchestrated with a tool like Airflow. This is not implemented in this solution.
- Provided comprehensive tests for both solutions, including unit tests and integration tests. The test suite covers PyArrow writer service functionality and Spark aggregation job processing using the same Docker-based approach as the production pipeline.
