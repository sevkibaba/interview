#!/usr/bin/env python3
"""
Test script for the Spark aggregation job using the same strategy as the normal pipeline.

This script:
1. Sets up test data in the correct directory structure
2. Builds the Docker image (same as normal pipeline)
3. Runs the Spark job with test data (same command pattern)
4. Verifies the results match expected values
"""

import os
import sys
import subprocess
import json
import pandas as pd
import tempfile
import shutil

def run_command(cmd, cwd=None, check=True):
    """Run a command and return the result."""
    print(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)
    
    if check and result.returncode != 0:
        print(f"Command failed with return code {result.returncode}")
        print(f"STDOUT: {result.stdout}")
        print(f"STDERR: {result.stderr}")
        raise RuntimeError(f"Command failed: {' '.join(cmd)}")
    
    return result

def setup_test_data():
    """Set up test data in the correct directory structure."""
    print("Setting up test data...")
    
    # Get the directory of this script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    test_data_dir = os.path.join(script_dir, "test_data")
    
    # Set up test data using the helper script
    sample_json = os.path.join(script_dir, "sample_neo_data.json")
    setup_script = os.path.join(script_dir, "setup_test_data.py")
    
    run_command([
        "python3", setup_script,
        "--input-json", sample_json,
        "--output-dir", test_data_dir
    ])
    
    print(f"Test data set up in: {test_data_dir}")
    return test_data_dir

def build_docker_image():
    """Build the Docker image (same as normal pipeline)."""
    print("Building Docker image...")
    
    # Navigate to data directory (parent of tests)
    data_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    run_command([
        "docker", "build",
        "-t", "neo-spark-job",
        "."
    ], cwd=data_dir)
    
    print("Docker image built successfully")

def run_spark_job_with_test_data(test_data_dir):
    """Run the Spark job with test data (same pattern as normal pipeline)."""
    print("Running Spark job with test data...")
    
    # Get the directory of this script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    test_output_dir = os.path.join(script_dir, "test_output")
    
    # Create output directory
    os.makedirs(test_output_dir, exist_ok=True)
    
    # Run the Spark job using the same pattern as normal pipeline
    result = run_command([
        "docker", "run", "--rm", "-u", "root",
        "-v", f"{test_data_dir}:/app/input",
        "-v", f"{test_output_dir}:/app/output",
        "neo-spark-job",
        "--start_batch", "1", "--end_batch", "20",
        "--input_dir", "/app/input", "--output_dir", "/app/output"
    ], check=False)
    
    if result.returncode != 0:
        print(f"Spark job failed with return code {result.returncode}")
        print(f"STDOUT: {result.stdout}")
        print(f"STDERR: {result.stderr}")
        return False, None
    
    print("Spark job completed successfully")
    print(f"Job output: {result.stdout}")
    return True, test_output_dir

def verify_results(output_dir):
    """Verify that the results match expected values."""
    print("Verifying results...")
    
    # Load expected results
    script_dir = os.path.dirname(os.path.abspath(__file__))
    expected_file = os.path.join(script_dir, "expected_aggregation_results.json")
    
    with open(expected_file, 'r') as f:
        expected_results = json.load(f)
    
    # Read actual results
    results_path = os.path.join(output_dir, "aggregations", "batches-1-20")
    
    if not os.path.exists(results_path):
        print(f"ERROR: Results directory not found: {results_path}")
        return False
    
    # Find parquet files
    parquet_files = [f for f in os.listdir(results_path) if f.endswith('.parquet')]
    if not parquet_files:
        print("ERROR: No parquet files found in results")
        return False
    
    # Read the first parquet file
    parquet_file = os.path.join(results_path, parquet_files[0])
    df = pd.read_parquet(parquet_file)
    
    print(f"Results DataFrame:\n{df}")
    
    # Convert to dictionary for easier comparison
    results_dict = dict(zip(df['metric'], df['value']))
    
    # Verify results
    all_passed = True
    
    for metric, expected_value in expected_results.items():
        actual_value = results_dict.get(metric, 0)
        if actual_value == expected_value:
            print(f"✓ {metric}: {actual_value} (expected: {expected_value})")
        else:
            print(f"✗ {metric}: {actual_value} (expected: {expected_value})")
            all_passed = False
    
    return all_passed

def main():
    """Main function to run the Spark job test."""
    print("Starting Spark aggregation job test using normal pipeline strategy...")
    
    try:
        # Set up test data
        test_data_dir = setup_test_data()
        
        # Build Docker image (same as normal pipeline)
        build_docker_image()
        
        # Run Spark job with test data (same pattern as normal pipeline)
        success, output_dir = run_spark_job_with_test_data(test_data_dir)
        
        if not success:
            print("Spark job failed")
            return 1
        
        # Verify results
        if not verify_results(output_dir):
            print("Results verification failed")
            return 1
        
        print("All tests passed! ✓")
        print("\nTest Summary:")
        print(f"- Test data: {test_data_dir}")
        print(f"- Output: {output_dir}")
        print("- Used same Docker image and command pattern as normal pipeline")
        return 0
        
    except Exception as e:
        print(f"Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == '__main__':
    exit_code = main()
    sys.exit(exit_code)

