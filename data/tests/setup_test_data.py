#!/usr/bin/env python3
"""
Helper script to set up test data for the Spark aggregation job tests.

This script:
1. Reads sample NEO data from JSON file
2. Converts it to parquet format in the correct directory structure
3. Creates the expected directory layout for the Spark job
"""

import json
import os
import pandas as pd
import argparse
from pathlib import Path

def setup_test_data(input_json_file, output_dir):
    """
    Set up test data from JSON file to parquet format.
    
    Args:
        input_json_file: Path to the JSON file containing sample NEO data
        output_dir: Directory where the test data should be created
    """
    # Read sample data
    with open(input_json_file, 'r') as f:
        sample_data = json.load(f)
    
    # Create directory structure
    raw_dir = os.path.join(output_dir, "raw")
    batch_dir = os.path.join(raw_dir, "batch-number=1-20")
    
    os.makedirs(batch_dir, exist_ok=True)
    
    # Convert to DataFrame and save as parquet
    df = pd.DataFrame(sample_data)
    parquet_path = os.path.join(batch_dir, "neo_partition_1-20.parquet")
    df.to_parquet(parquet_path, index=False)
    
    print(f"Created test data at: {parquet_path}")
    print(f"Test data contains {len(df)} records")
    print(f"Columns: {list(df.columns)}")
    
    return parquet_path

def main():
    """Main function to set up test data."""
    parser = argparse.ArgumentParser(description='Set up test data for Spark aggregation job')
    parser.add_argument('--input-json', default='sample_neo_data.json', 
                       help='Input JSON file with sample NEO data')
    parser.add_argument('--output-dir', required=True, 
                       help='Output directory for test data')
    
    args = parser.parse_args()
    
    # Get the directory of this script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    input_json_path = os.path.join(script_dir, args.input_json)
    
    # Set up test data
    setup_test_data(input_json_path, args.output_dir)

if __name__ == '__main__':
    main()