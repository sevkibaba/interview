# Advanced Solution: Dockerfile for Spark Aggregation Job
FROM bitnami/spark:3.5.1

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Set working directory
WORKDIR /app

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy application files
COPY advanced-solution/spark_aggregation_job.py .
COPY models/models.py ./models/
COPY services/writer_service_adv_sol.py ./services/

# Create directories for input and output
RUN mkdir -p /app/input /app/output

# Set the default command
ENTRYPOINT ["spark-submit", "--master", "local[*]", "spark_aggregation_job.py"]
