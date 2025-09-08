#!/usr/bin/env python3
"""
Simple Solution: NASA NEO Data Recaller

This script fetches Near Earth Object data from NASA's API and saves it to parquet files
with aggregations for reporting purposes.

Usage:
    python recall_data.py                    # Full run (fetch 200 NEOs)
    python recall_data.py --backfill 1 20    # Backfill batches 1-20
    python recall_data.py --backfill 1 100   # Backfill batches 1-100
    python recall_data.py --recalc 1 100     # Recalculate aggregations from batches 1-100
"""

import logging
import sys
import os
import argparse
from dotenv import load_dotenv

load_dotenv()

# Add the services directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'services'))

from data.services.neo_api_service import NasaNeoClient
from data.services.writer_service import NeoDataWriter

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("tekmetric")


def backfill_batch_range(start_batch: int, end_batch: int, output_dir: str):
    """
    Backfill a range of batches.
    """
    logger.info(f"Starting backfill for batches {start_batch} to {end_batch}")
    
    try:
        # Initialize services
        neo_client = NasaNeoClient()
        data_writer = NeoDataWriter(output_dir)
        
        batch_size = 20
        successful_batches = 0
        failed_batches = 0
        
        # Loop through each batch in the range
        for batch_number in range(start_batch, end_batch + 1):
            logger.info(f"Processing batch {batch_number} of {end_batch}")
            
            try:
                # Calculate which NEOs to fetch for this batch
                start_index = (batch_number - 1) * batch_size
                end_index = start_index + batch_size
                
                # Fetch the specific batch
                logger.info(f"Fetching NEOs {start_index + 1} to {end_index}")
                response = neo_client.fetch_neo_batch(page=batch_number - 1, size=batch_size)
                
                if 'error' in response:
                    logger.error(f"Error fetching batch {batch_number}: {response['error']}")
                    failed_batches += 1
                    continue
                    
                if 'near_earth_objects' not in response:
                    logger.error(f"No NEOs found in batch {batch_number}")
                    failed_batches += 1
                    continue
                    
                neos = response['near_earth_objects']
                if not neos:
                    logger.error(f"No NEOs in batch {batch_number}")
                    failed_batches += 1
                    continue
                
                # Backfill the batch
                filepath = data_writer.backfill_batch(neos, batch_number)
                
                if filepath:
                    logger.info(f"Backfill complete for batch {batch_number}: {filepath}")
                    successful_batches += 1
                else:
                    logger.error(f"Failed to write batch {batch_number}")
                    failed_batches += 1
                    
            except Exception as e:
                logger.error(f"Error processing batch {batch_number}: {e}")
                failed_batches += 1
        
        # Summary
        logger.info("=== BACKFILL COMPLETE ===")
        logger.info(f"Successful batches: {successful_batches}")
        logger.info(f"Failed batches: {failed_batches}")
        logger.info(f"Total batches processed: {successful_batches + failed_batches}")
        
        return 0 if failed_batches == 0 else 1
        
    except Exception as e:
        logger.error(f"Error during backfill: {e}", exc_info=True)
        return 1


def recalculate_aggregations(start_batch: int, end_batch: int, output_dir: str):
    """
    Recalculate aggregations from existing raw files.
    """
    logger.info(f"Starting aggregation recalculation from batches {start_batch} to {end_batch}")
    
    try:
        # Initialize writer service
        data_writer = NeoDataWriter(output_dir)
        
        # Recalculate aggregations
        aggregations_file = data_writer.recalculate_aggregations_from_raw(start_batch, end_batch)
        
        if aggregations_file:
            logger.info(f"Aggregations recalculated and saved to: {aggregations_file}")
            
            # Print summary
            summary = data_writer.get_aggregation_summary()
            logger.info("=== AGGREGATION RECALCULATION COMPLETE ===")
            logger.info(f"Total close approaches under 0.2 AU: {summary['total_close_approaches_under_0_2_au']}")
            logger.info(f"Close approaches by year: {summary['close_approaches_by_year']}")
            return 0
        else:
            logger.error("Failed to recalculate aggregations")
            return 1
        
    except Exception as e:
        logger.error(f"Error during aggregation recalculation: {e}", exc_info=True)
        return 1


def full_run(output_dir: str):
    """
    Full run: fetch all NEOs and calculate aggregations.
    """
    logger.info("Starting full NASA NEO data recall process")
    
    # Configuration
    total_limit = 200
    batch_size = 20
    
    try:
        # Initialize services
        logger.info("Initializing services...")
        neo_client = NasaNeoClient()
        data_writer = NeoDataWriter(output_dir)
        
        # Fetch all NEOs in batches
        logger.info(f"Fetching {total_limit} NEOs with batch size {batch_size}")
        all_neos = neo_client.fetch_all_neos(total_limit=total_limit, batch_size=batch_size)
        
        if not all_neos:
            logger.error("No NEOs were fetched. Exiting.")
            return 1
        
        logger.info(f"Successfully fetched {len(all_neos)} NEOs")
        
        # Process NEOs in batches and write to files
        batch_number = 1
        for i in range(0, len(all_neos), batch_size):
            batch = all_neos[i:i + batch_size]
            filepath = data_writer.write_batch(batch, batch_number)
            if filepath:
                logger.info(f"Batch {batch_number} written to {filepath}")
            batch_number += 1
        
        # Write aggregations
        logger.info("Writing aggregations...")
        aggregations_file = data_writer.write_aggregations()
        
        # Print summary
        summary = data_writer.get_aggregation_summary()
        logger.info("=== PROCESSING COMPLETE ===")
        logger.info(f"Total NEOs processed: {len(all_neos)}")
        logger.info(f"Total close approaches under 0.2 AU: {summary['total_close_approaches_under_0_2_au']}")
        logger.info(f"Close approaches by year: {summary['close_approaches_by_year']}")
        logger.info(f"Raw data saved to: {data_writer.raw_dir}")
        logger.info(f"Aggregations saved to: {data_writer.aggregations_dir}")
        
        return 0
        
    except Exception as e:
        logger.error(f"Error during processing: {e}", exc_info=True)
        return 1


def main():
    """
    Main function with argument parsing.
    """
    parser = argparse.ArgumentParser(description='NASA NEO Data Recaller')
    parser.add_argument('--backfill', nargs=2, type=int, metavar=('START', 'END'), 
                       help='Backfill batches from START to END (e.g., --backfill 1 20)')
    parser.add_argument('--recalc', nargs=2, type=int, metavar=('START', 'END'), 
                       help='Recalculate aggregations from START to END batch numbers')
    parser.add_argument('--output-dir', default=os.getenv("OUTPUT_DIR", "output"),
                       help='Output directory (default: output)')
    
    args = parser.parse_args()
    
    if args.backfill:
        start_batch, end_batch = args.backfill
        return backfill_batch_range(start_batch, end_batch, args.output_dir)
    elif args.recalc:
        start_batch, end_batch = args.recalc
        return recalculate_aggregations(start_batch, end_batch, args.output_dir)
    else:
        return full_run(args.output_dir)


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)