"""
GTFS Realtime Local Data Collector
==================================

Runs continuously to collect GTFS data every 20 seconds.
Press Ctrl+C to stop gracefully.

Usage:
    python scripts/gtfs_realtime_collector.py                    # Run unlimited
    python scripts/gtfs_realtime_collector.py --single           # Single run only
    python scripts/gtfs_realtime_collector.py --duration 2       # Run for 2 hours
    python scripts/gtfs_realtime_collector.py --duration 0.5     # Run for 30 minutes
"""

import os
import sys
import json
import time
import logging
import argparse
from datetime import datetime, timedelta
from pathlib import Path
import requests
import pandas as pd
from google.transit import gtfs_realtime_pb2
from google.protobuf.json_format import MessageToDict

# Add parent directory to path for config import
sys.path.append(str(Path(__file__).parent.parent))
from config.config import Config

# Setup logging
def setup_logging():
    """Setup logging to both file and console with Windows-compatible output"""
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    
    # Create file handler with UTF-8 encoding for emojis
    file_handler = logging.FileHandler(log_dir / 'gtfs_collector.log', encoding='utf-8')
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)
    
    # Create console handler with plain text (no emojis) for Windows compatibility
    console_handler = logging.StreamHandler()
    console_formatter = PlainTextFormatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)
    
    # Setup logger
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

class PlainTextFormatter(logging.Formatter):
    """Custom formatter that removes emojis for console output on Windows"""
    
    def format(self, record):
        # Get the formatted message
        formatted = super().format(record)
        
        # Replace emojis with plain text equivalents
        emoji_replacements = {
            '🚀': '[INIT]',
            '🔑': '[KEY]',
            '📁': '[DATA]',
            '📡': '[API]',
            '🔄': '[RUN]',
            '✅': '[OK]',
            '❌': '[ERROR]',
            '📊': '[STATS]',
            '⚠️': '[WARN]',
            '🕐': '[TIME]',
            '📈': '[TOTAL]',
            '💾': '[SAVE]',
            '⏳': '[WAIT]',
            '🛑': '[STOP]',
            '🏁': '[END]',
            '🔍': '[DEBUG]',
            '🧪': '[TEST]',
            '💥': '[CRASH]',
            '📋': '[LOG]',
            '🎮': '[CTRL]'
        }
        
        for emoji, replacement in emoji_replacements.items():
            formatted = formatted.replace(emoji, replacement)
        
        return formatted

logger = setup_logging()

class GTFSRealtimeCollector:
    """Local GTFS Realtime data collector with command line interface"""
    
    def __init__(self):
        """Initialize the collector with config from .env file"""
        self.config = Config()
        self.config.ensure_directories()
        
        # API Configuration from your .env file
        self.base_url = "https://data-exchange-api.vicroads.vic.gov.au/opendata/v1/gtfsr"
        self.api_key = self.config.VICROADS_API_KEY
        
        # GTFS Realtime endpoints
        self.endpoints = {
            'metro_train_trip_updates': f"{self.base_url}/metrotrain-tripupdates",
            'metro_bus_trip_updates': f"{self.base_url}/metrobus-tripupdates",
            'metro_train_alerts': f"{self.base_url}/metrotrain-servicealerts",
            'metro_train_vehicle_positions': f"{self.base_url}/metrotrain-vehicleposition-updates"
        }
        
        # Request headers
        self.headers = {
            'User-Agent': 'Resilient-Public-Services-Platform/1.0',
            'Accept': 'application/octet-stream',
            'Ocp-Apim-Subscription-Key': self.api_key
        }
        
        # Validate API key
        if not self.api_key:
            raise ValueError("❌ VICROADS_API_KEY not found in config/.env file!")
        
        # Stats tracking
        self.run_count = 0
        self.total_entities = 0
        self.total_errors = 0
        self.start_time = datetime.now()
        
        # Quota management tracking
        self.consecutive_quota_failures = 0
        self.quota_backoff_seconds = 60  # Start with 1 minute backoff
        self.total_quota_failures = 0
        
        logger.info("GTFS Collector initialized")
        logger.info(f"API Key: {self.api_key[:8]}...{self.api_key[-4:]}")
        logger.info(f"Data will be saved to: {self.config.RAW_DATA_PATH}")
        logger.info(f"Monitoring {len(self.endpoints)} endpoints")
    
    def collect_endpoint_data(self, endpoint_name, url):
        """Collect data from a single GTFS endpoint with improved rate limiting"""
        try:
            logger.debug(f"📡 Fetching {endpoint_name}")
            
            # Make API request with improved error handling
            response = requests.get(url, headers=self.headers, timeout=30)
            
            # Handle specific HTTP status codes
            if response.status_code == 429:
                # Rate limited - wait and retry
                retry_after = int(response.headers.get('Retry-After', 60))
                logger.warning(f"⚠️ Rate limited for {endpoint_name}. Waiting {retry_after}s before retry...")
                time.sleep(retry_after)
                
                # Retry the request once
                logger.info(f"🔄 Retrying {endpoint_name} after rate limit...")
                response = requests.get(url, headers=self.headers, timeout=30)
                
            elif response.status_code == 403:
                # Quota exceeded - log detailed error and suggest longer wait
                error_detail = response.text[:200] if response.text else "No error details"
                logger.error(f"❌ API quota exceeded for {endpoint_name}: {error_detail}")
                logger.error(f"❌ Consider reducing collection frequency or checking daily/monthly limits")
                return {'success': False, 'error': f'API quota exceeded: {error_detail}', 'quota_exceeded': True}
                
            elif response.status_code == 401:
                # Unauthorized - API key issue
                logger.error(f"❌ Unauthorized access for {endpoint_name} - check API key validity")
                return {'success': False, 'error': 'Unauthorized - invalid API key', 'auth_failed': True}
            
            # Raise for any remaining HTTP errors
            response.raise_for_status()
            
            # Parse protobuf data
            feed = gtfs_realtime_pb2.FeedMessage()
            feed.ParseFromString(response.content)
            feed_dict = MessageToDict(feed)
            
            # Extract metadata
            entity_count = len(feed_dict.get('entity', []))
            feed_timestamp = feed_dict.get('header', {}).get('timestamp', '')
            feed_version = feed_dict.get('header', {}).get('gtfsRealtimeVersion', '')
            
            logger.info(f"✅ {endpoint_name}: {entity_count} entities (v{feed_version})")
            
            return {
                'success': True,
                'data': feed_dict,
                'metadata': {
                    'endpoint': endpoint_name,
                    'entity_count': entity_count,
                    'feed_timestamp': feed_timestamp,
                    'feed_version': feed_version,
                    'response_size_bytes': len(response.content),
                    'collection_timestamp': datetime.now().isoformat(),
                    'http_status': response.status_code
                }
            }
            
        except requests.exceptions.Timeout as e:
            error_msg = f"Timeout for {endpoint_name}: {str(e)}"
            logger.error(f"❌ {error_msg}")
            return {'success': False, 'error': error_msg}
            
        except requests.exceptions.ConnectionError as e:
            error_msg = f"Connection error for {endpoint_name}: {str(e)}"
            logger.error(f"❌ {error_msg}")
            return {'success': False, 'error': error_msg}
            
        except requests.exceptions.HTTPError as e:
            error_msg = f"HTTP error for {endpoint_name}: {str(e)}"
            logger.error(f"❌ {error_msg}")
            return {'success': False, 'error': error_msg}
        
        except requests.exceptions.RequestException as e:
            error_msg = f"Request failed for {endpoint_name}: {str(e)}"
            logger.error(f"❌ {error_msg}")
            return {'success': False, 'error': error_msg}
        
        except Exception as e:
            error_msg = f"Processing failed for {endpoint_name}: {str(e)}"
            logger.error(f"❌ {error_msg}")
            return {'success': False, 'error': error_msg}
    
    def save_data(self, endpoint_name, data, metadata, run_timestamp):
        """Save collected data to local storage with partitioning"""
        try:
            # Create date-based directory structure (same as original script)
            current_date = datetime.now()
            partition_path = (
                Path(self.config.RAW_DATA_PATH) / 
                f"year={current_date.year}" / 
                f"month={current_date.month:02d}" / 
                f"day={current_date.day:02d}" /
                f"run_{run_timestamp}"
            )
            partition_path.mkdir(parents=True, exist_ok=True)
            
            # Save raw JSON data
            json_filename = f"gtfs_realtime_{endpoint_name}_{run_timestamp}.json"
            json_filepath = partition_path / json_filename
            
            with open(json_filepath, 'w') as f:
                json.dump(data, f, indent=2)
            
            # Save metadata
            metadata_filename = f"gtfs_realtime_{endpoint_name}_metadata_{run_timestamp}.json"
            metadata_filepath = partition_path / metadata_filename
            
            with open(metadata_filepath, 'w') as f:
                json.dump(metadata, f, indent=2)
            
            # Convert to DataFrame and save as parquet (for analytics)
            entity_count = 0
            if data.get('entity'):
                # Flatten entity data for tabular storage
                entities_df = pd.json_normalize(data['entity'])
                
                # Add ingestion metadata
                entities_df['ingestion_timestamp'] = datetime.now()
                entities_df['ingestion_run_id'] = run_timestamp
                entities_df['endpoint_name'] = endpoint_name
                entities_df['feed_timestamp'] = metadata.get('feed_timestamp', '')
                
                # Save as parquet
                parquet_filename = f"gtfs_realtime_{endpoint_name}_{run_timestamp}.parquet"
                parquet_filepath = partition_path / parquet_filename
                
                entities_df.to_parquet(parquet_filepath, index=False)
                entity_count = len(entities_df)
                
                logger.info(f"💾 Saved {entity_count} entities to {partition_path.name}")
            else:
                logger.info(f"💾 No entities for {endpoint_name}, saved metadata only")
            
            return {
                'success': True,
                'entity_count': entity_count,
                'files_created': [str(json_filepath), str(metadata_filepath)]
            }
            
        except Exception as e:
            error_msg = f"Failed to save {endpoint_name}: {str(e)}"
            logger.error(f"❌ {error_msg}")
            return {'success': False, 'error': error_msg, 'entity_count': 0}
    
    def run_single_collection(self):
        """Run one complete collection cycle across all endpoints"""
        self.run_count += 1
        run_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        collection_start = datetime.now()
        
        logger.info(f"🔄 Starting collection run #{self.run_count} ({run_timestamp})")
        
        # Track this run's results
        run_entities = 0
        run_errors = 0
        endpoints_processed = 0
        quota_failures = 0
        
        for i, (endpoint_name, url) in enumerate(self.endpoints.items()):
            try:
                # Collect data from endpoint
                result = self.collect_endpoint_data(endpoint_name, url)
                
                if result['success']:
                    # Save data to storage
                    save_result = self.save_data(
                        endpoint_name, 
                        result['data'], 
                        result['metadata'],
                        run_timestamp
                    )
                    
                    if save_result['success']:
                        run_entities += save_result['entity_count']
                        endpoints_processed += 1
                    else:
                        run_errors += 1
                        self.total_errors += 1
                else:
                    run_errors += 1
                    self.total_errors += 1
                    
                    # Track quota failures specifically
                    if result.get('quota_exceeded'):
                        quota_failures += 1
                        self.total_quota_failures += 1
                        logger.warning(f"⚠️ Quota failure #{self.total_quota_failures} for {endpoint_name}")
                        
                        # If we hit quota on this endpoint, likely to hit on others too
                        # Consider breaking out of the loop to avoid more failures
                        if quota_failures >= 2:
                            logger.error(f"❌ Multiple quota failures in single run. Stopping current run.")
                            break
                
                # Apply 20-second delay after each endpoint to prevent rate limiting
                # This includes the last endpoint to space out the next run
                if i < len(self.endpoints) - 1:
                    next_endpoint = list(self.endpoints.keys())[i + 1]
                    logger.info(f"⏳ Waiting 20 seconds before next endpoint ({next_endpoint})...")
                    time.sleep(20)
                else:
                    # Delay after last endpoint to space out next run
                    logger.info(f"⏳ Waiting 20 seconds before next run...")
                    time.sleep(20)
                
            except Exception as e:
                error_msg = f"Unexpected error processing {endpoint_name}: {str(e)}"
                logger.error(f"💥 {error_msg}")
                run_errors += 1
                self.total_errors += 1
        
        # Update totals
        self.total_entities += run_entities
        
        # Calculate execution time
        execution_time = datetime.now() - collection_start
        total_runtime = datetime.now() - self.start_time
        
        # Log run summary
        logger.info(f"📊 Run #{self.run_count} completed in {execution_time.total_seconds():.1f}s:")
        logger.info(f"    ✅ Endpoints: {endpoints_processed}/{len(self.endpoints)}")
        logger.info(f"    📊 Entities: {run_entities}")
        logger.info(f"    ⚠️ Errors: {run_errors}")
        logger.info(f"    🕐 Total Runtime: {str(total_runtime).split('.')[0]}")
        logger.info(f"    📈 Session Total: {self.total_entities} entities from {self.run_count} runs")
        
        return {
            'run_number': self.run_count,
            'entities_collected': run_entities,
            'endpoints_processed': endpoints_processed,
            'errors': run_errors,
            'quota_failures': quota_failures,
            'execution_time_seconds': execution_time.total_seconds()
        }
    
    def run_continuous(self, duration_hours=None):
        """Run continuous collection with improved quota management"""
        # Calculate end time if duration specified
        end_time = None
        if duration_hours:
            end_time = datetime.now() + timedelta(hours=duration_hours)
            logger.info(f"🚀 Starting {duration_hours}h collection session")
            logger.info(f"🏁 Will stop at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        else:
            logger.info("🚀 Starting unlimited collection session")
        
        logger.info("⚠️ Press Ctrl+C to stop gracefully")
        logger.info("📊 Collection will run with quota-aware delays")
        
        try:
            while True:
                # Check time limit
                if end_time and datetime.now() >= end_time:
                    logger.info("🏁 Time limit reached!")
                    break
                
                # Run single collection cycle
                run_result = self.run_single_collection()
                
                # Check for quota issues and implement smart backoff
                if run_result.get('quota_failures', 0) > 0:
                    self.consecutive_quota_failures += 1
                    
                    # Implement exponential backoff for quota issues
                    if self.consecutive_quota_failures >= 2:
                        logger.warning(f"⚠️ {self.consecutive_quota_failures} consecutive quota failures detected.")
                        logger.warning(f"⚠️ Backing off for {self.quota_backoff_seconds} seconds...")
                        
                        # Wait with progress indication
                        for i in range(self.quota_backoff_seconds, 0, -10):
                            if i <= 60:
                                logger.info(f"⏳ Quota backoff: {i}s remaining...")
                            time.sleep(min(10, i))
                            
                            # Check if we should stop due to time limit
                            if end_time and datetime.now() >= end_time:
                                logger.info("🏁 Time limit reached during backoff!")
                                return
                        
                        # Increase backoff time for next failure (max 5 minutes)
                        self.quota_backoff_seconds = min(self.quota_backoff_seconds * 2, 300)
                    else:
                        # First quota failure - wait longer than normal
                        logger.info(f"⏳ Quota failure detected. Waiting 120 seconds before retry...")
                        time.sleep(120)
                        
                elif run_result.get('errors', 0) > 0:
                    # Reset quota failure count on successful run, but handle other errors
                    self.consecutive_quota_failures = 0
                    self.quota_backoff_seconds = 60
                    
                    # Standard delay for non-quota errors
                    logger.info(f"⏳ Errors detected. Waiting 60 seconds before next run...")
                    time.sleep(60)
                    
                else:
                    # Reset backoff on successful run
                    self.consecutive_quota_failures = 0
                    self.quota_backoff_seconds = 60
                    
                    # Implement smart scheduling based on time of day
                    delay = self.get_collection_delay()
                    logger.info(f"⏳ Successful run. Waiting {delay} seconds before next run...")
                    time.sleep(delay)
                
                # Check if we should continue
                if end_time and datetime.now() >= end_time:
                    logger.info("🏁 Time limit reached after collection!")
                    break
                
        except KeyboardInterrupt:
            logger.info("🛑 Collection stopped by user (Ctrl+C)")
        except Exception as e:
            logger.error(f"💥 Collection failed with error: {str(e)}")
        finally:
            # Print final session summary
            self.print_final_summary()
    
    def get_collection_delay(self):
        """Get collection delay based on time of day and transport patterns"""
        current_hour = datetime.now().hour
        
        # Peak hours: More frequent collection (but still respecting quotas)
        if 6 <= current_hour <= 9 or 17 <= current_hour <= 19:
            return 180  # Every 3 minutes during peak hours
        # Business hours: Regular collection  
        elif 9 <= current_hour <= 17:
            return 300  # Every 5 minutes during business hours
        # Off-peak: Reduced collection to preserve quota
        else:
            return 600  # Every 10 minutes during off-peak hours
    
    def print_final_summary(self):
        """Print final collection session summary"""
        total_time = datetime.now() - self.start_time
        
        logger.info("=" * 60)
        logger.info("📊 FINAL COLLECTION SESSION SUMMARY")
        logger.info("=" * 60)
        logger.info(f"🕐 Session Duration: {str(total_time).split('.')[0]}")
        logger.info(f"📈 Total Runs: {self.run_count}")
        logger.info(f"📊 Total Entities Collected: {self.total_entities}")
        logger.info(f"⚠️ Total Errors: {self.total_errors}")
        logger.info(f"� Total Quota Failures: {self.total_quota_failures}")
        logger.info(f"�📁 Data Location: {self.config.RAW_DATA_PATH}")
        logger.info(f"📋 Logs Location: logs/gtfs_collector.log")
        
        if self.run_count > 0:
            avg_entities = self.total_entities / self.run_count
            success_rate = ((self.run_count * len(self.endpoints) - self.total_errors) / (self.run_count * len(self.endpoints))) * 100
            logger.info(f"📊 Average Entities per Run: {avg_entities:.1f}")
            logger.info(f"✅ Success Rate: {success_rate:.1f}%")
        
        if self.total_quota_failures > 0:
            logger.info(f"⚠️ Quota Management:")
            logger.info(f"    • Total quota failures: {self.total_quota_failures}")
            logger.info(f"    • Consecutive failures: {self.consecutive_quota_failures}")
            logger.info(f"    • Current backoff: {self.quota_backoff_seconds}s")
            logger.info(f"💡 Consider reducing collection frequency or checking API limits")
        
        if self.total_errors == 0:
            logger.info("✅ No errors encountered!")
        else:
            error_rate = (self.total_errors / (self.run_count * len(self.endpoints))) * 100
            logger.info(f"⚠️ Error Rate: {error_rate:.1f}%")
        
        logger.info("=" * 60)

def main():
    """Main function with command line argument parsing"""
    parser = argparse.ArgumentParser(
        description='GTFS Realtime Data Collector',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/gtfs_realtime_collector.py                    # Run unlimited
  python scripts/gtfs_realtime_collector.py --single           # Single run only
  python scripts/gtfs_realtime_collector.py --duration 2       # Run for 2 hours
  python scripts/gtfs_realtime_collector.py --duration 0.5     # Run for 30 minutes
        """
    )
    
    parser.add_argument(
        '--duration', 
        type=float, 
        help='Collection duration in hours (e.g., 2 for 2 hours, 0.5 for 30 minutes)'
    )
    parser.add_argument(
        '--single', 
        action='store_true', 
        help='Run single collection cycle only (for testing)'
    )
    parser.add_argument(
        '--verbose', 
        action='store_true', 
        help='Enable verbose debug logging'
    )
    
    args = parser.parse_args()
    
    # Adjust logging level if verbose
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.info("🔍 Verbose logging enabled")
    
    try:
        # Check for required dependencies
        try:
            import google.protobuf
            from google.transit import gtfs_realtime_pb2
        except ImportError:
            logger.error("❌ GTFS Realtime protobuf library not found!")
            logger.error("📦 Please install it with: pip install gtfs-realtime-bindings")
            return 1
        
        # Initialize collector
        collector = GTFSRealtimeCollector()
        
        # Run based on arguments
        if args.single:
            logger.info("🧪 Running single collection cycle...")
            result = collector.run_single_collection()
            
            if result['entities_collected'] > 0:
                logger.info("✅ Single collection completed successfully!")
                return 0
            else:
                logger.error("❌ Single collection failed - no entities collected")
                return 1
        else:
            # Run continuous collection
            collector.run_continuous(args.duration)
            
            if collector.run_count > 0:
                logger.info("✅ Collection session completed successfully!")
                return 0
            else:
                logger.error("❌ No collections completed")
                return 1
        
    except KeyboardInterrupt:
        logger.info("🛑 Program interrupted by user")
        return 0
    except Exception as e:
        logger.error(f"💥 Program failed: {str(e)}")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
