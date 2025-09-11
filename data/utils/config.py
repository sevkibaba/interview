#!/usr/bin/env python3
"""
Configuration utility for NASA NEO Data Recaller

This module loads configuration from environment variables with sensible defaults.
"""

import os
import logging
from typing import Optional
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

logger = logging.getLogger("tekmetric")


class Config:
    """Configuration class that loads settings from environment variables."""
    
    def __init__(self):
        self._load_config()
    
    def _load_config(self):
        """Load configuration from environment variables."""
        
        # NASA API Configuration
        self.NASA_API_BASE_URL = os.getenv("NASA_API_BASE_URL", "https://api.nasa.gov")
        self.NASA_API_ENDPOINT = os.getenv("NASA_API_ENDPOINT", "/neo/rest/v1/neo/browse")
        self.NASA_API_DEMO_KEY = os.getenv("NASA_API_DEMO_KEY", "DEMO_KEY")
        
        # Rate Limiting
        self.API_RATE_LIMIT_INTERVAL = float(os.getenv("API_RATE_LIMIT_INTERVAL", "0.5"))
        self.API_MAX_REQUESTS_PER_SECOND = int(os.getenv("API_MAX_REQUESTS_PER_SECOND", "2"))
        
        # Data Processing
        self.TOTAL_NEO_LIMIT = int(os.getenv("TOTAL_NEO_LIMIT", "200"))
        self.BATCH_SIZE = int(os.getenv("BATCH_SIZE", "20"))
        self.PARTITION_SIZE = int(os.getenv("PARTITION_SIZE", "20"))
        self.CLOSE_APPROACH_THRESHOLD_AU = float(os.getenv("CLOSE_APPROACH_THRESHOLD_AU", "0.2"))
        
        # Retry Configuration
        self.RETRY_COUNT = int(os.getenv("RETRY_COUNT", "5"))
        
        # File Paths
        self.OUTPUT_DIR = os.getenv("OUTPUT_DIR", "output")
        self.RAW_DATA_DIR = os.getenv("RAW_DATA_DIR", "raw")
        self.AGGREGATIONS_DIR = os.getenv("AGGREGATIONS_DIR", "aggregations")
        self.PARTITION_PREFIX = os.getenv("PARTITION_PREFIX", "batch-number")
        self.PARQUET_FILE_PREFIX = os.getenv("PARQUET_FILE_PREFIX", "neo_partition")
        self.AGGREGATIONS_FILE_NAME = os.getenv("AGGREGATIONS_FILE_NAME", "close_approaches_aggregations.parquet")
        
        # Spark Configuration
        self.SPARK_APP_NAME = os.getenv("SPARK_APP_NAME", "NASA_NEO_Aggregations")
        self.SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")
        self.SPARK_SQL_ADAPTIVE_ENABLED = os.getenv("SPARK_SQL_ADAPTIVE_ENABLED", "true").lower() == "true"
        self.SPARK_SQL_ADAPTIVE_COALESCE_ENABLED = os.getenv("SPARK_SQL_ADAPTIVE_COALESCE_ENABLED", "true").lower() == "true"
        self.SPARK_PARQUET_BLOCK_SIZE = int(os.getenv("SPARK_PARQUET_BLOCK_SIZE", "134217728"))
        
        # Logging
        self.LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
        self.LOG_FORMAT = os.getenv("LOG_FORMAT", "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        
        # Docker
        self.DOCKER_IMAGE_NAME = os.getenv("DOCKER_IMAGE_NAME", "neo-spark-job")
        self.DOCKER_WORKING_DIR = os.getenv("DOCKER_WORKING_DIR", "/app")
        
        # Test Configuration
        self.TEST_DATA_DIR = os.getenv("TEST_DATA_DIR", "test_data")
        self.TEST_OUTPUT_DIR = os.getenv("TEST_OUTPUT_DIR", "test_output")
        
        # Log configuration loading
        logger.info("Configuration loaded from environment variables")
        logger.debug(f"NASA API Base URL: {self.NASA_API_BASE_URL}")
        logger.debug(f"Total NEO Limit: {self.TOTAL_NEO_LIMIT}")
        logger.debug(f"Batch Size: {self.BATCH_SIZE}")
        logger.debug(f"Output Directory: {self.OUTPUT_DIR}")
    
    def get_nasa_api_url(self) -> str:
        """Get the full NASA API URL."""
        return f"{self.NASA_API_BASE_URL}{self.NASA_API_ENDPOINT}"
    
    def get_raw_data_dir(self) -> str:
        """Get the raw data directory path."""
        return os.path.join(self.OUTPUT_DIR, self.RAW_DATA_DIR)
    
    def get_aggregations_dir(self) -> str:
        """Get the aggregations directory path."""
        return os.path.join(self.OUTPUT_DIR, self.AGGREGATIONS_DIR)
    
    def get_partition_dir_name(self, start_batch: int, end_batch: int) -> str:
        """Get the partition directory name for a batch range."""
        return f"{self.PARTITION_PREFIX}={start_batch}-{end_batch}"
    
    def get_parquet_filename(self, start_batch: int, end_batch: int) -> str:
        """Get the parquet filename for a batch range."""
        return f"{self.PARQUET_FILE_PREFIX}_{start_batch}-{end_batch}.parquet"
    
    def get_aggregations_filepath(self) -> str:
        """Get the full path to the aggregations file."""
        return os.path.join(self.get_aggregations_dir(), self.AGGREGATIONS_FILE_NAME)


# Global configuration instance
config = Config()
