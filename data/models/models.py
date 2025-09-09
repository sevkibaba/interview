#!/usr/bin/env python3
"""
Advanced Solution: PyArrow Models for NEO Data

This module defines PyArrow schemas and models for Near Earth Object data processing.
"""

import pyarrow as pa
from typing import Dict, Any, List, Optional
from dataclasses import dataclass


@dataclass
class NeoDataAdvSol:
    """
    Data class representing a single NEO record for the advanced solution.
    """
    id: Optional[str]
    neo_reference_id: Optional[str]
    name: Optional[str]
    name_limited: Optional[str]
    designation: Optional[str]
    nasa_jpl_url: Optional[str]
    absolute_magnitude_h: Optional[float]
    is_potentially_hazardous_asteroid: Optional[bool]
    minimum_estimated_diameter_meters: Optional[float]
    maximum_estimated_diameter_meters: Optional[float]
    closest_approach_miss_distance_km: Optional[float]
    closest_approach_date: Optional[str]
    closest_approach_relative_velocity_km_per_sec: Optional[float]
    first_observation_date: Optional[str]
    last_observation_date: Optional[str]
    observations_used: Optional[int]
    orbital_period: Optional[float]

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for PyArrow table creation."""
        return {
            'id': self.id,
            'neo_reference_id': self.neo_reference_id,
            'name': self.name,
            'name_limited': self.name_limited,
            'designation': self.designation,
            'nasa_jpl_url': self.nasa_jpl_url,
            'absolute_magnitude_h': self.absolute_magnitude_h,
            'is_potentially_hazardous_asteroid': self.is_potentially_hazardous_asteroid,
            'minimum_estimated_diameter_meters': self.minimum_estimated_diameter_meters,
            'maximum_estimated_diameter_meters': self.maximum_estimated_diameter_meters,
            'closest_approach_miss_distance_km': self.closest_approach_miss_distance_km,
            'closest_approach_date': self.closest_approach_date,
            'closest_approach_relative_velocity_km_per_sec': self.closest_approach_relative_velocity_km_per_sec,
            'first_observation_date': self.first_observation_date,
            'last_observation_date': self.last_observation_date,
            'observations_used': self.observations_used,
            'orbital_period': self.orbital_period
        }


# PyArrow schema for NEO data with proper nullable fields
NEO_SCHEMA_ADV_SOL = pa.schema([
    pa.field("id", pa.string()),
    pa.field("neo_reference_id", pa.string()),
    pa.field("name", pa.string()),
    pa.field("name_limited", pa.string(), nullable=True),
    pa.field("designation", pa.string()),
    pa.field("nasa_jpl_url", pa.string()),
    pa.field("absolute_magnitude_h", pa.float32()),
    pa.field("is_potentially_hazardous_asteroid", pa.bool_()),
    pa.field("minimum_estimated_diameter_meters", pa.float64()),
    pa.field("maximum_estimated_diameter_meters", pa.float64()),
    pa.field("closest_approach_miss_distance_km", pa.float64(), nullable=True),
    pa.field("closest_approach_date", pa.string(), nullable=True),
    pa.field("closest_approach_relative_velocity_km_per_sec", pa.float64(), nullable=True),
    pa.field("first_observation_date", pa.string()),
    pa.field("last_observation_date", pa.string()),
    pa.field("observations_used", pa.int32()),
    pa.field("orbital_period", pa.float64())
])




class PyArrowTableManagerAdvSol:
    """
    Manager class for PyArrow table operations in the advanced solution.
    """
    
    @staticmethod
    def create_neo_table_adv_sol(neo_records: List[NeoDataAdvSol]) -> pa.Table:
        """
        Create a PyArrow table from a list of NEO records.
        """
        if not neo_records:
            return pa.table([], schema=NEO_SCHEMA_ADV_SOL)
        
        # Convert records to dictionaries
        data = [record.to_dict() for record in neo_records]
        
        # Process data to ensure it matches schema
        processed_data = PyArrowTableManagerAdvSol._process_data_for_schema(data)
        
        # Convert list of dicts to columnar format for PyArrow
        # Create arrays for each field
        arrays = []
        for field in NEO_SCHEMA_ADV_SOL:
            field_name = field.name
            column_data = [record.get(field_name) for record in processed_data]
            arrays.append(pa.array(column_data, type=field.type))
        
        # Create table from arrays
        return pa.table(arrays, schema=NEO_SCHEMA_ADV_SOL)
    
    @staticmethod
    def _process_data_for_schema(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Process data to ensure it matches the PyArrow schema structure.
        """
        processed_data = []
        
        for record in data:
            processed_record = {}
            
            # Process each field according to schema requirements
            for field in NEO_SCHEMA_ADV_SOL:
                field_name = field.name
                field_type = field.type
                is_nullable = field.nullable
                
                # Get the value from the record
                value = record.get(field_name)
                
                # Process based on field type and nullability
                if value is None:
                    if is_nullable:
                        processed_record[field_name] = None
                    else:
                        # Provide default values for non-nullable fields
                        if pa.types.is_string(field_type):
                            processed_record[field_name] = ""
                        elif pa.types.is_integer(field_type):
                            processed_record[field_name] = 0
                        elif pa.types.is_floating(field_type):
                            processed_record[field_name] = 0.0
                        elif pa.types.is_boolean(field_type):
                            processed_record[field_name] = False
                        else:
                            processed_record[field_name] = None
                else:
                    # Convert value to appropriate type
                    try:
                        if pa.types.is_string(field_type):
                            processed_record[field_name] = str(value) if value is not None else None
                        elif pa.types.is_integer(field_type):
                            processed_record[field_name] = int(value) if value is not None else None
                        elif pa.types.is_floating(field_type):
                            processed_record[field_name] = float(value) if value is not None else None
                        elif pa.types.is_boolean(field_type):
                            processed_record[field_name] = bool(value) if value is not None else None
                        else:
                            processed_record[field_name] = value
                    except (ValueError, TypeError):
                        # If conversion fails, use default value
                        if is_nullable:
                            processed_record[field_name] = None
                        else:
                            if pa.types.is_string(field_type):
                                processed_record[field_name] = ""
                            elif pa.types.is_integer(field_type):
                                processed_record[field_name] = 0
                            elif pa.types.is_floating(field_type):
                                processed_record[field_name] = 0.0
                            elif pa.types.is_boolean(field_type):
                                processed_record[field_name] = False
                            else:
                                processed_record[field_name] = None
            
            processed_data.append(processed_record)
        
        return processed_data
    
    
    @staticmethod
    def write_parquet_adv_sol(table: pa.Table, filepath: str) -> str:
        """
        Write a PyArrow table to parquet format.
        """
        pa.parquet.write_table(table, filepath)
        return filepath
    
    @staticmethod
    def read_parquet_adv_sol(filepath: str) -> pa.Table:
        """
        Read a parquet file into a PyArrow table.
        """
        return pa.parquet.read_table(filepath)
    
    @staticmethod
    def read_multiple_parquet_adv_sol(filepaths: List[str]) -> pa.Table:
        """
        Read multiple parquet files and combine them into a single PyArrow table.
        """
        if not filepaths:
            return pa.table([], schema=NEO_SCHEMA_ADV_SOL)
        
        tables = []
        for filepath in filepaths:
            try:
                table = pa.parquet.read_table(filepath)
                tables.append(table)
            except Exception as e:
                print(f"Warning: Could not read {filepath}: {e}")
                continue
        
        if not tables:
            return pa.table([], schema=NEO_SCHEMA_ADV_SOL)
        
        # Combine all tables
        return pa.concat_tables(tables)
    
    @staticmethod
    def filter_table_adv_sol(table: pa.Table, column: str, value: Any) -> pa.Table:
        """
        Filter a PyArrow table by column value.
        """
        # Create a boolean mask
        mask = pa.compute.equal(table[column], value)
        return pa.compute.filter(table, mask)
    
    @staticmethod
    def group_by_adv_sol(table: pa.Table, group_column: str, agg_columns: List[str], agg_functions: List[str]) -> pa.Table:
        """
        Group a PyArrow table by a column and apply aggregation functions.
        """
        # This is a simplified implementation - PyArrow's groupby is more complex
        # For now, we'll return the original table and handle aggregations differently
        return table
    
    @staticmethod
    def get_table_info_adv_sol(table: pa.Table) -> Dict[str, Any]:
        """
        Get information about a PyArrow table.
        """
        return {
            'num_rows': len(table),
            'num_columns': len(table.schema),
            'column_names': table.column_names,
            'schema': str(table.schema)
        }
