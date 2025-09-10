#!/usr/bin/env python3
"""
Test for PyArrow writer service.

This test hardcodes a sample API response and verifies that the writer service
correctly saves it to a parquet file with expected output.
"""

import unittest
import sys
import os
import tempfile
import shutil
import pyarrow.parquet as pq
import pyarrow as pa
from datetime import datetime

# Add the services directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'services'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'models'))

from writer_service_adv_sol import NeoDataWriterAdvSol


class TestWriterService(unittest.TestCase):
    """Test cases for PyArrow writer service."""
    
    def setUp(self):
        """Set up test environment with temporary directory."""
        self.temp_dir = tempfile.mkdtemp()
        self.writer = NeoDataWriterAdvSol(output_dir=self.temp_dir)
    
    def tearDown(self):
        """Clean up temporary directory."""
        shutil.rmtree(self.temp_dir)
    
    def test_write_batch_adv_sol(self):
        """Test writing a batch of NEO data to parquet file."""
        
        # Hardcoded sample API response
        sample_api_response = {
            "near_earth_objects": {
                "2023-01-15": [
                    {
                        "id": "2000433",
                        "neo_reference_id": "2000433",
                        "name": "433 Eros (A898 PA)",
                        "name_limited": None,
                        "designation": "433",
                        "nasa_jpl_url": "https://ssd-api.jpl.nasa.gov/asteroid.api?des=433",
                        "absolute_magnitude_h": 10.31,
                        "is_potentially_hazardous_asteroid": False,
                        "estimated_diameter": {
                            "meters": {
                                "estimated_diameter_min": 1000.0,
                                "estimated_diameter_max": 2000.0
                            }
                        },
                        "close_approach_data": [
                            {
                                "close_approach_date": "2023-01-15",
                                "epoch_date_close_approach": "2023-01-15",
                                "relative_velocity": {
                                    "kilometers_per_second": "5.0",
                                    "kilometers_per_hour": "18000.0",
                                    "miles_per_hour": "11184.68"
                                },
                                "miss_distance": {
                                    "astronomical": "0.1",
                                    "lunar": "38.9",
                                    "kilometers": "14960000",
                                    "miles": "9290000"
                                },
                                "orbiting_body": "Earth"
                            }
                        ],
                        "orbital_data": {
                            "first_observation_date": "1898-08-13",
                            "last_observation_date": "2023-01-15",
                            "observations_used": 1000,
                            "orbital_period": 643.219
                        }
                    },
                    {
                        "id": "2000001",
                        "neo_reference_id": "2000001",
                        "name": "1 Ceres (A801 AA)",
                        "name_limited": None,
                        "designation": "1",
                        "nasa_jpl_url": "https://ssd-api.jpl.nasa.gov/asteroid.api?des=1",
                        "absolute_magnitude_h": 3.36,
                        "is_potentially_hazardous_asteroid": False,
                        "estimated_diameter": {
                            "meters": {
                                "estimated_diameter_min": 500.0,
                                "estimated_diameter_max": 1000.0
                            }
                        },
                        "close_approach_data": [
                            {
                                "close_approach_date": "2023-06-15",
                                "epoch_date_close_approach": "2023-06-15",
                                "relative_velocity": {
                                    "kilometers_per_second": "3.0",
                                    "kilometers_per_hour": "10800.0",
                                    "miles_per_hour": "6708.0"
                                },
                                "miss_distance": {
                                    "astronomical": "0.15",
                                    "lunar": "58.35",
                                    "kilometers": "22440000",
                                    "miles": "13940000"
                                },
                                "orbiting_body": "Earth"
                            }
                        ],
                        "orbital_data": {
                            "first_observation_date": "1801-01-01",
                            "last_observation_date": "2023-06-15",
                            "observations_used": 2000,
                            "orbital_period": 1680.0
                        }
                    }
                ]
            }
        }
        
        # Extract NEOs from API response
        neos = sample_api_response["near_earth_objects"]["2023-01-15"]
        
        # Write batch to parquet file
        batch_number = 1
        filepath = self.writer.write_batch_adv_sol(neos, batch_number)
        
        # Assert file was created
        self.assertTrue(os.path.exists(filepath))
        self.assertTrue(filepath.endswith('.parquet'))
        
        # Read the parquet file and verify contents
        table = pq.read_table(filepath)
        df = table.to_pandas()
        
        # Assert correct number of rows
        self.assertEqual(len(df), 2)
        
        # Assert expected columns are present
        expected_columns = [
            'id', 'neo_reference_id', 'name', 'name_limited', 'designation',
            'nasa_jpl_url', 'absolute_magnitude_h', 'is_potentially_hazardous_asteroid',
            'minimum_estimated_diameter_meters', 'maximum_estimated_diameter_meters',
            'closest_approach_miss_distance_km', 'closest_approach_date',
            'closest_approach_relative_velocity_km_per_sec', 'first_observation_date',
            'last_observation_date', 'observations_used', 'orbital_period',
            'close_approach_data'
        ]
        
        for col in expected_columns:
            self.assertIn(col, df.columns)
        
        # Assert specific values for first NEO (433 Eros)
        eros_row = df[df['id'] == '2000433'].iloc[0]
        self.assertEqual(eros_row['id'], '2000433')
        self.assertEqual(eros_row['name'], '433 Eros (A898 PA)')
        self.assertEqual(eros_row['designation'], '433')
        self.assertAlmostEqual(eros_row['absolute_magnitude_h'], 10.31, places=2)
        self.assertEqual(eros_row['is_potentially_hazardous_asteroid'], False)
        self.assertAlmostEqual(eros_row['minimum_estimated_diameter_meters'], 1000.0, places=1)
        self.assertAlmostEqual(eros_row['maximum_estimated_diameter_meters'], 2000.0, places=1)
        self.assertAlmostEqual(eros_row['closest_approach_miss_distance_km'], 14960000.0, places=1)
        self.assertEqual(eros_row['closest_approach_date'], '2023-01-15')
        self.assertAlmostEqual(eros_row['closest_approach_relative_velocity_km_per_sec'], 5.0, places=1)
        self.assertEqual(eros_row['first_observation_date'], '1898-08-13')
        self.assertEqual(eros_row['last_observation_date'], '2023-01-15')
        self.assertEqual(eros_row['observations_used'], 1000)
        self.assertAlmostEqual(eros_row['orbital_period'], 643.219, places=3)
        
        # Assert close_approach_data is stored as JSON string
        self.assertIsInstance(eros_row['close_approach_data'], str)
        self.assertIn('"close_approach_date": "2023-01-15"', eros_row['close_approach_data'])
        self.assertIn('"astronomical": "0.1"', eros_row['close_approach_data'])
        
        # Assert specific values for second NEO (1 Ceres)
        ceres_row = df[df['id'] == '2000001'].iloc[0]
        self.assertEqual(ceres_row['id'], '2000001')
        self.assertEqual(ceres_row['name'], '1 Ceres (A801 AA)')
        self.assertEqual(ceres_row['designation'], '1')
        self.assertAlmostEqual(ceres_row['absolute_magnitude_h'], 3.36, places=2)
        self.assertEqual(ceres_row['is_potentially_hazardous_asteroid'], False)
        self.assertAlmostEqual(ceres_row['minimum_estimated_diameter_meters'], 500.0, places=1)
        self.assertAlmostEqual(ceres_row['maximum_estimated_diameter_meters'], 1000.0, places=1)
        self.assertAlmostEqual(ceres_row['closest_approach_miss_distance_km'], 22440000.0, places=1)
        self.assertEqual(ceres_row['closest_approach_date'], '2023-06-15')
        self.assertAlmostEqual(ceres_row['closest_approach_relative_velocity_km_per_sec'], 3.0, places=1)
        self.assertEqual(ceres_row['first_observation_date'], '1801-01-01')
        self.assertEqual(ceres_row['last_observation_date'], '2023-06-15')
        self.assertEqual(ceres_row['observations_used'], 2000)
        self.assertAlmostEqual(ceres_row['orbital_period'], 1680.0, places=1)
        
        # Assert close_approach_data is stored as JSON string
        self.assertIsInstance(ceres_row['close_approach_data'], str)
        self.assertIn('"close_approach_date": "2023-06-15"', ceres_row['close_approach_data'])
        self.assertIn('"astronomical": "0.15"', ceres_row['close_approach_data'])
        
        # Assert file is in correct partition directory
        self.assertIn('batch-number=1-20', filepath)
        self.assertIn('neo_partition_1-20.parquet', filepath)


if __name__ == '__main__':
    unittest.main()
