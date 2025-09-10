# NASA NEO Data Recaller Tests

This directory contains comprehensive tests for the NASA NEO data recaller system, including both PyArrow writer service tests and Spark aggregation job tests.

## Test Suite Overview

The test suite includes two main types of tests:

1. **PyArrow Writer Service Tests** - Unit tests for the data writing functionality
2. **Spark Aggregation Job Tests** - Integration tests for the Spark processing pipeline

## Running All Tests

### Complete Test Suite
```bash
cd data/tests
python3 run_tests.py
```

This will run both test suites and provide a comprehensive summary.

### Individual Test Suites

#### PyArrow Writer Service Tests
```bash
cd data/tests
python3 -m unittest test_writer_service.py -v
```

#### Spark Aggregation Job Tests
```bash
cd data/tests
python3 test_spark_pipeline.py
```

## Test Files

### PyArrow Writer Service Tests
- `test_writer_service.py` - Unit tests for PyArrow writer service
- Tests data transformation and parquet file writing
- Uses hardcoded sample API responses
- Verifies correct column mapping and data types

### Spark Aggregation Job Tests
- `test_spark_pipeline.py` - Integration test for Spark aggregation job
- `setup_test_data.py` - Helper script to set up test data from JSON
- `sample_neo_data.json` - Sample NEO data for testing
- `expected_aggregation_results.json` - Expected results for verification

## Test Approach

The Spark tests follow the exact same pattern as the normal pipeline:

1. **Build Docker image**: `docker build -t neo-spark-job .`
2. **Run Spark job**: `docker run --rm -u root -v <input>:/app/input -v <output>:/app/output neo-spark-job --start_batch 1 --end_batch 20 --input_dir /app/input --output_dir /app/output`

### Test Data Structure
```
tests/
├── test_data/                    # Test input data (created by setup script)
│   └── raw/
│       └── batch-number=1-20/
│           └── neo_partition_1-20.parquet
├── test_output/                  # Test output data (created by Spark job)
│   └── aggregations/
│       └── batches-1-20/
│           ├── _SUCCESS
│           └── part-00000-*.parquet
├── sample_neo_data.json         # Source test data (3 sample NEOs)
├── expected_aggregation_results.json  # Expected results for verification
├── test_writer_service.py       # PyArrow writer service unit tests
├── test_spark_pipeline.py       # Spark aggregation job integration tests
├── setup_test_data.py           # Helper script to create test data
├── run_tests.py                 # Test runner for all tests
└── README.md                    # This documentation
```

## Running the Tests

### Quick Test
```bash
cd data/tests
python3 test_spark_pipeline.py
```

This will:
1. Set up test data in the correct directory structure
2. Build the Docker image (same as normal pipeline)
3. Run the Spark job with test data (same command pattern)
4. Verify the results match expected values

## Test Data

The test data includes 3 sample NEOs with different close approach scenarios:

### NEO 1 (test_neo_1)
- 2 close approaches under 0.2 AU (both in 2023)
- Miss distances: 0.1 AU and 0.15 AU

### NEO 2 (test_neo_2)
- 1 close approach under 0.2 AU (in 2024)
- 1 close approach over 0.2 AU (in 2023) - should be filtered out
- Miss distances: 0.25 AU (filtered) and 0.18 AU (included)

### NEO 3 (test_neo_3)
- 1 close approach under 0.2 AU (in 2023)
- Miss distance: 0.05 AU

### Expected Results
- Total close approaches under 0.2 AU: 4
- Close approaches in 2023: 3
- Close approaches in 2024: 1

## Test Output

When running successfully, you should see:
```
Starting Spark aggregation job test using normal pipeline strategy...
Setting up test data...
Building Docker image...
Running Spark job with test data...
Spark job completed successfully
Verifying results...
✓ total_close_approaches_under_02_au: 4 (expected: 4)
✓ close_approaches_year_2023: 3 (expected: 3)
✓ close_approaches_year_2024: 1 (expected: 1)
All tests passed! ✓
```

## Key Benefits

1. **Same as Production**: Uses the exact same Docker image and command pattern as the normal pipeline
2. **File-based**: All test data and expected results are in separate files, not hardcoded
3. **Isolated**: Test data is in a separate directory structure
4. **Verifiable**: Results are compared against expected values from files
5. **Simple**: Easy to understand and modify

## Adding New Tests

To add new test cases:

1. Add test data to `sample_neo_data.json`
2. Update expected results in `expected_aggregation_results.json`
3. Run the test to verify

The test will automatically use the new data and verify the results.