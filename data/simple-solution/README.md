# NASA NEO Data Recaller - Simple Solution

This script fetches Near Earth Object (NEO) data from NASA's API and saves it to parquet files with aggregations for reporting purposes.

## Features

- **Batch Processing**: Fetches NEO data in configurable batches with rate limiting
- **Parquet Storage**: Saves data in partitioned parquet format for Spark compatibility
- **Aggregations**: Calculates close approach statistics on-the-fly
- **Backfill Support**: Overwrite individual batches with fresh data
- **Aggregation Recalculation**: Recalculate aggregations from existing raw files
- **Rate Limiting**: Respects API limits (max 2 requests per second)

## Prerequisites

1. **Python 3.11**
2. **Dependencies**: Install required packages
   ```bash
   pip install -r requirements.txt
   ```

3. **Environment Configuration**: Copy and configure the environment file
   ```bash
   cp .env_example .env
   # Edit .env file with your configuration
   ```

## Installation

1. Navigate to the project directory:
   ```bash
   cd /path/to/tekmetric/interview
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Configure environment variables:
   ```bash
   cp .env_example .env
   # Edit .env file with your settings
   ```

## Usage

### Full Run (Default)
Fetch all 200 NEOs and calculate aggregations:
```bash
python simple-solution/recall_data.py
```

### Backfill Batch Range
Overwrite a range of batches with fresh data:
```bash
# Backfill batches 1-20
python simple-solution/recall_data.py --backfill 1 20

# Backfill batches 1-100
python simple-solution/recall_data.py --backfill 1 100


### Recalculate Aggregations
Recalculate aggregations from existing raw files:
```bash
# Recalculate from batches 1-100
python simple-solution/recall_data.py --recalc 1 100

# Recalculate from all batches (1-200)
python simple-solution/recall_data.py --recalc 1 200
```

### Custom Output Directory
Specify a custom output directory:
```bash
python simple-solution/recall_data.py --output-dir /path/to/custom/output
```

### Environment Variables
The script automatically loads environment variables from a `.env` file. You can also set them manually:

```bash
# Using .env file (recommended)
# Edit .env file with your configuration

# Or set manually
export API_KEY="your_nasa_api_key"
export OUTPUT_DIR="/path/to/output"
export LOG_LEVEL="DEBUG"
python simple-solution/recall_data.py
```

## Output Structure

The script creates the following directory structure:

```
output/
├── raw/
│   ├── batch-number=1-20/
│   │   └── neo_partition_1-20.parquet
│   ├── batch-number=21-40/
│   │   └── neo_partition_21-40.parquet
│   ├── batch-number=41-60/
│   │   └── neo_partition_41-60.parquet
│   └── ... (continues for all batches)
└── aggregations/
    └── close_approaches_aggregations.parquet
```

## Data Schema

### Raw Data Columns
- `id`: NEO unique identifier
- `neo_reference_id`: NEO reference ID
- `name`: NEO name
- `name_limited`: Limited name
- `designation`: NEO designation
- `nasa_jpl_url`: JPL URL
- `absolute_magnitude_h`: Absolute magnitude
- `is_potentially_hazardous_asteroid`: Hazard flag
- `minimum_estimated_diameter_meters`: Min diameter in meters
- `maximum_estimated_diameter_meters`: Max diameter in meters
- `closest_approach_miss_distance_km`: Closest approach distance in km
- `closest_approach_date`: Closest approach date
- `closest_approach_relative_velocity_km_per_sec`: Relative velocity
- `first_observation_date`: First observation date
- `last_observation_date`: Last observation date
- `observations_used`: Number of observations
- `orbital_period`: Orbital period

### Aggregations
- Total close approaches under 0.2 AU
- Close approaches by year

## Configuration

### Default Settings
- **Total NEOs**: 200
- **Batch Size**: 20 NEOs per batch
- **Rate Limit**: 2 requests per second
- **Partition Size**: 20 batches per partition

### API Endpoints
- **Base URL**: https://api.nasa.gov
- **Browse API**: `/neo/rest/v1/neo/browse`
- **Default Key**: DEMO_KEY (limited rate)

## Logging

The script provides detailed logging including:
- API request status
- Batch processing progress
- File write operations
- Aggregation calculations
- Error handling

Log level can be adjusted by modifying the logging configuration in the script.

## Error Handling

- **API Errors**: Graceful handling of API failures with retry logic
- **File I/O**: Safe file operations with error reporting
- **Data Validation**: Checks for required data fields
- **Rate Limiting**: Automatic delays to respect API limits

## Performance

- **Rate Limited**: Maximum 2 requests per second to NASA API
- **Batch Processing**: Processes data in chunks for memory efficiency
- **Partitioned Storage**: Optimized for Spark and big data tools
- **Incremental Updates**: Support for backfilling without full reprocessing

## Dependencies

- `requests>=2.28.0`: HTTP client for API calls
- `pandas>=1.5.0`: Data manipulation and analysis
- `pyarrow>=10.0.0`: Parquet file format support
- `python-dotenv>=1.0.0`: Environment variable loading from .env files

## Support

For issues or questions, check the logs for detailed error messages and ensure all dependencies are properly installed.

## Advantages
- Cost efficient
- Faster for small operations
- Easier to maintain and understand if the context is fixed

## Disadvantages
- Limited scalability
- If there occur errors, cost is increased more than a modular approach 
- Hard to reuse for other purposes
