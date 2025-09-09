#!/usr/bin/env python3
"""
Advanced Solution: PyArrow-based Writer Service for NEO Data

This service handles data transformation and storage using PyArrow.
Aggregations are handled separately by Spark jobs.
"""

import logging
import os
import glob
from typing import List, Dict, Any, Optional
from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'models'))

from models import (
    NeoDataAdvSol, 
    PyArrowTableManagerAdvSol,
    NEO_SCHEMA_ADV_SOL
)

logger = logging.getLogger("tekmetric")


class NeoDataWriterAdvSol:
    """
    Advanced solution service to write NEO data to parquet files using PyArrow.
    """
    def __init__(self, output_dir: str = "output"):
        self.output_dir = output_dir
        self.raw_dir = os.path.join(output_dir, "raw")
        
        # Create directories if they don't exist
        os.makedirs(self.raw_dir, exist_ok=True)
        
    def _extract_neo_data_adv_sol(self, neo: Dict[str, Any]) -> NeoDataAdvSol:
        """
        Extract and transform NEO data according to the required schema using PyArrow models.
        """
        # Find the closest approach
        closest_approach = None
        if 'close_approach_data' in neo and neo['close_approach_data']:
            # Sort by miss distance to find closest
            approaches = sorted(
                neo['close_approach_data'], 
                key=lambda x: float(x.get('miss_distance', {}).get('astronomical', '999'))
            )
            closest_approach = approaches[0]
        
        # Extract diameter information
        estimated_diameter = neo.get('estimated_diameter', {})
        meters_diameter = estimated_diameter.get('meters', {})
        
        # Extract orbital data
        orbital_data = neo.get('orbital_data', {})
        
        # Handle data type conversions and None values
        def safe_float(value):
            if value is None:
                return None
            try:
                return float(value)
            except (ValueError, TypeError):
                return None
        
        def safe_int(value):
            if value is None:
                return None
            try:
                return int(value)
            except (ValueError, TypeError):
                return None
        
        def safe_bool(value):
            if value is None:
                return None
            return bool(value)
        
        return NeoDataAdvSol(
            id=neo.get('id'),
            neo_reference_id=neo.get('neo_reference_id'),
            name=neo.get('name'),
            name_limited=neo.get('name_limited'),
            designation=neo.get('designation'),
            nasa_jpl_url=neo.get('nasa_jpl_url'),
            absolute_magnitude_h=safe_float(neo.get('absolute_magnitude_h')),
            is_potentially_hazardous_asteroid=safe_bool(neo.get('is_potentially_hazardous_asteroid')),
            minimum_estimated_diameter_meters=safe_float(meters_diameter.get('estimated_diameter_min')),
            maximum_estimated_diameter_meters=safe_float(meters_diameter.get('estimated_diameter_max')),
            closest_approach_miss_distance_km=(
                safe_float(closest_approach.get('miss_distance', {}).get('kilometers', 0)) 
                if closest_approach else None
            ),
            closest_approach_date=(
                closest_approach.get('close_approach_date') 
                if closest_approach else None
            ),
            closest_approach_relative_velocity_km_per_sec=(
                safe_float(closest_approach.get('relative_velocity', {}).get('kilometers_per_second', 0)) 
                if closest_approach else None
            ),
            first_observation_date=orbital_data.get('first_observation_date'),
            last_observation_date=orbital_data.get('last_observation_date'),
            observations_used=safe_int(orbital_data.get('observations_used')),
            orbital_period=safe_float(orbital_data.get('orbital_period')),
            close_approach_data=neo.get('close_approach_data')  # Store full close approach data
        )

    def write_batch_adv_sol(self, neos: List[Dict[str, Any]], batch_number: int) -> str:
        """
        Write a batch of NEOs to a parquet file with partitioning using PyArrow.
        :param neos: List of NEO objects
        :param batch_number: Batch number for filename
        :return: Path to the written file
        """
        if not neos:
            logger.warning(f"No NEOs to write for batch {batch_number}")
            return None
            
        # Transform data using PyArrow models
        transformed_data = []
        for neo in neos:
            transformed_neo = self._extract_neo_data_adv_sol(neo)
            transformed_data.append(transformed_neo)
        
        # Create PyArrow table
        table = PyArrowTableManagerAdvSol.create_neo_table_adv_sol(transformed_data)
        
        # Calculate partition range (20 batches per partition) - same as simple solution
        partition_start = ((batch_number - 1) % 20) * 20 + 1
        partition_end = partition_start + 19
        
        # Create partition directory
        partition_dir = f"batch-number={partition_start}-{partition_end}"
        partition_path = os.path.join(self.raw_dir, partition_dir)
        os.makedirs(partition_path, exist_ok=True)
        
        # Write to parquet with batch info in filename
        filename = f"neo_partition_{partition_start}-{partition_end}.parquet"
        filepath = os.path.join(partition_path, filename)
        
        # Use PyArrow to write parquet
        PyArrowTableManagerAdvSol.write_parquet_adv_sol(table, filepath)
        
        logger.info(f"Written batch {batch_number} with {len(transformed_data)} NEOs to {filepath}")
        return filepath
    
    def backfill_batch_adv_sol(self, neos: List[Dict[str, Any]], batch_number: int) -> str:
        """
        Backfill a specific batch, overwriting existing data using PyArrow.
        :param neos: List of NEO objects
        :param batch_number: Batch number to backfill
        :return: Path to the written file
        """
        logger.info(f"Backfilling batch {batch_number}")
        return self.write_batch_adv_sol(neos, batch_number)
