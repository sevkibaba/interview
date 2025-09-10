#!/usr/bin/env python3
"""
Test runner for the NASA NEO data recaller tests.

This script runs all tests in the tests directory, including both unittest-based tests
and standalone test scripts.
"""

import unittest
import sys
import os
import subprocess

# Add the tests directory to the path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def run_unittest_tests():
    """Run unittest-based tests."""
    print("=" * 60)
    print("Running unittest-based tests...")
    print("=" * 60)
    
    # Discover and run tests
    loader = unittest.TestLoader()
    start_dir = os.path.dirname(os.path.abspath(__file__))
    suite = loader.discover(start_dir, pattern='test_*.py')
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    return result.wasSuccessful()

def run_spark_pipeline_test():
    """Run the Spark pipeline test."""
    print("=" * 60)
    print("Running Spark pipeline test...")
    print("=" * 60)
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    test_script = os.path.join(script_dir, "test_spark_pipeline.py")
    
    try:
        result = subprocess.run([sys.executable, test_script], 
                              capture_output=True, text=True, cwd=script_dir)
        
        print(result.stdout)
        if result.stderr:
            print("STDERR:", result.stderr)
        
        return result.returncode == 0
    except Exception as e:
        print(f"Error running Spark pipeline test: {e}")
        return False

def run_tests():
    """Run all tests in the tests directory."""
    print("Starting NASA NEO Data Recaller Test Suite")
    print("=" * 60)
    
    all_passed = True
    
    # Run unittest-based tests
    unittest_passed = run_unittest_tests()
    all_passed = all_passed and unittest_passed
    
    print("\n")
    
    # Run Spark pipeline test
    spark_passed = run_spark_pipeline_test()
    all_passed = all_passed and spark_passed
    
    print("\n" + "=" * 60)
    print("Test Summary:")
    print(f"  Unittest tests: {'PASSED' if unittest_passed else 'FAILED'}")
    print(f"  Spark pipeline test: {'PASSED' if spark_passed else 'FAILED'}")
    print(f"  Overall: {'PASSED' if all_passed else 'FAILED'}")
    print("=" * 60)
    
    return 0 if all_passed else 1

if __name__ == '__main__':
    exit_code = run_tests()
    sys.exit(exit_code)
