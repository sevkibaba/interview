# Spark Cluster Setup for Advanced Solution

This setup provides a production-like Spark cluster environment for running the advanced NEO data processing solution.

## Architecture

The cluster consists of:
- **1 Spark Master** (spark-master): Manages the cluster and job scheduling
- **2 Spark Workers** (spark-worker-1, spark-worker-2): Execute tasks in parallel
- **1 Job Submission Service** (spark-submit): For submitting jobs to the cluster

## Services and Ports

| Service | Port | Description |
|---------|------|-------------|
| spark-master | 8080 | Spark Master Web UI |
| spark-master | 7077 | Spark Master RPC (for job submission) |
| spark-worker-1 | 8081 | Worker 1 Web UI |
| spark-worker-2 | 8082 | Worker 2 Web UI |

## Quick Start

### 1. Start the Cluster

```bash
# Start the entire cluster
docker-compose -f docker-compose-cluster.yml up -d

# Check cluster status
docker-compose -f docker-compose-cluster.yml ps
```

### 2. Monitor the Cluster

- **Spark Master Web UI**: http://localhost:8080
- **Worker 1 Web UI**: http://localhost:8081  
- **Worker 2 Web UI**: http://localhost:8082

### 3. Submit Jobs

#### Option A: Using the Helper Script (Recommended)

```bash
# Submit job for batches 1-20
./submit_spark_job.sh 1 20

# Submit job for batches 21-40
./submit_spark_job.sh 21 40
```

#### Option B: Direct Docker Command

```bash
docker-compose -f docker-compose-cluster.yml exec spark-submit spark-submit \
    --master spark://spark-master:7077 \
    --py-files /app/models/models.py,/app/services/writer_service_adv_sol.py \
    /app/spark_aggregation_job.py \
    --start_batch 1 --end_batch 20 \
    --input_dir /data --output_dir /data
```

### 4. Stop the Cluster

```bash
docker-compose -f docker-compose-cluster.yml down
```

## Cluster Configuration

### Worker Resources
- **Memory per worker**: 2GB
- **Cores per worker**: 2
- **Total cluster capacity**: 4GB RAM, 4 cores

### Data Volumes
- Raw NEO data: `./advanced-solution/output/raw` → `/data/raw`
- Output aggregations: `./advanced-solution/output/aggregations` → `/data/aggregations`

## Monitoring and Debugging

### Check Cluster Health
```bash
# View all service logs
docker-compose -f docker-compose-cluster.yml logs

# View specific service logs
docker-compose -f docker-compose-cluster.yml logs spark-master
docker-compose -f docker-compose-cluster.yml logs spark-worker-1
```

### Access Cluster Shell
```bash
# Access the job submission container
docker-compose -f docker-compose-cluster.yml exec spark-submit bash

# Access a worker container
docker-compose -f docker-compose-cluster.yml exec spark-worker-1 bash
```

## Performance Benefits

Compared to local mode (`--master local[*]`), the cluster setup provides:

1. **True Parallelism**: Jobs run across multiple worker nodes
2. **Resource Isolation**: Each worker has dedicated memory and CPU
3. **Scalability**: Easy to add more workers by extending docker-compose
4. **Production-like Environment**: Mirrors real Spark cluster behavior

## Troubleshooting

### Common Issues

1. **Job Hangs on Submission**
   - Ensure all workers are running: `docker-compose -f docker-compose-cluster.yml ps`
   - Check worker logs for connection issues
   - Verify network connectivity between services

2. **Out of Memory Errors**
   - Increase worker memory in docker-compose-cluster.yml
   - Reduce batch size or data processing scope

3. **Serialization Errors**
   - Ensure Python/PySpark versions match between driver and cluster
   - Check that all dependencies are installed in the cluster

## Scaling the Cluster

To add more workers, simply duplicate the `spark-worker-*` service in `docker-compose-cluster.yml` and update the port mappings and service names accordingly.

## Integration with Advanced Solution

This cluster setup is designed to work seamlessly with:
- The advanced solution's PyArrow-based data processing
- The Spark aggregation job for calculating NEO statistics
- The existing data partitioning structure (batch-number=X-Y)
- The coalesced single-file output format

The cluster reads from the same data volumes as the local Docker setup, ensuring consistency across different execution environments.


