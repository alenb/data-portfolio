"""
GTFS Schedule Data Ingestion Component
=====================================

Handles ingestion of GTFS Schedule (static) data with real platform numbers.
This component downloads and processes the complete Victorian transport GTFS schedule.

Key Features:
- Downloads 258MB GTFS Schedule ZIP from VicTrack
- Extracts static timetable data (routes, stops, trips, etc.)
- Processes REAL platform numbers from stops.txt
- Converts to Parquet format for analytics
- Handles all transport modes (train, tram, bus)
"""

import requests
import zipfile
import pandas as pd
from datetime import datetime
from pathlib import Path
import logging
import shutil
from .base_ingestion import BaseIngestionComponent


class GTFSScheduleIngestionComponent(BaseIngestionComponent):
    """
    Component for ingesting GTFS Schedule (static) data with real platform numbers
    """
    
    def get_component_name(self):
        return "GTFS Schedule Data (Static Timetables)"
    
    def ingest_data(self):
        """Ingest GTFS Schedule data with REAL platform numbers"""
        self.log_progress("🚂 Starting GTFS Schedule data ingestion with REAL platform numbers...")
        
        try:
            # Step 1: Download GTFS Schedule
            zip_path = self._download_gtfs_schedule()
            if not zip_path:
                raise Exception("Failed to download GTFS Schedule")
            
            # Step 2: Extract GTFS data
            temp_path = self._extract_gtfs_data(zip_path)
            if not temp_path:
                raise Exception("Failed to extract GTFS data")
            
            # Step 3: Process GTFS files
            self._process_gtfs_files(temp_path)
            
            # Step 4: Platform analysis
            self._create_platform_analysis()
            
            # Step 5: Cleanup
            self._cleanup(temp_path)
            
            self.log_progress(f"🎯 GTFS Schedule ingestion complete. Datasets: {len(self.datasets_ingested)}")
            self.log_progress("✅ Successfully collected REAL platform numbers from GTFS Schedule!")
            
        except Exception as e:
            error_msg = f"GTFS Schedule ingestion failed: {str(e)}"
            self.log_progress(error_msg, "error")
            self.errors.append(error_msg)
        
        return self.get_summary()
    
    def _download_gtfs_schedule(self):
        """Download the latest GTFS Schedule dataset via public resource URL"""
        # Use configuration for GTFS Schedule URL
        gtfs_url = self.config.GTFS_SCHEDULE_URL
        if not gtfs_url:
            # Fallback URL construction if not configured
            dataset_id = self.config.GTFS_SCHEDULE_DATASET_ID or "3f4e292e-7f8a-4ffe-831f-1953be0fe448"
            resource_id = self.config.GTFS_SCHEDULE_RESOURCE_ID or "aeee08cf-6da4-4206-b6b4-202ad488401f"
            gtfs_url = f"{self.config.DATA_VIC_DATASET_BASE_URL}/{dataset_id}/resource/{resource_id}/download/gtfs.zip"
        
        self.log_progress("📥 Downloading GTFS Schedule (258MB)...")
        self.log_progress("🚂 This includes ALL Victorian transport with REAL platform numbers!")
        
        try:
            # Get retry configuration from orchestration
            retry_count = 0
            max_retries = self.get_max_retries()
            retry_delay = self.get_retry_delay()
            
            self.log_progress(f"🔧 Using orchestration config: max_retries={max_retries}, retry_delay={retry_delay}s")
            
            # Handle case where max_retries is 0
            if max_retries == 0:
                self.log_progress("⚠️ max_retries=0, skipping GTFS Schedule download")
                return
            
            response = None
            while retry_count < max_retries:
                try:
                    response = requests.get(gtfs_url, stream=True, timeout=300)  # 5 minute timeout
                    response.raise_for_status()
                    break
                except requests.exceptions.HTTPError as e:
                    if response and response.status_code == 404:
                        # Resource might be temporarily unavailable
                        if retry_count < max_retries - 1:
                            self.log_progress(f"GTFS Schedule not found (404), retrying in {retry_delay} seconds... ({retry_count + 1}/{max_retries})")
                            import time
                            time.sleep(retry_delay)
                            retry_count += 1
                            continue
                        else:
                            raise Exception(f"GTFS Schedule resource not found after {max_retries} attempts: {e}")
                    else:
                        raise Exception(f"HTTP Error {response.status_code if response else 'Unknown'}: {e}")
                except requests.exceptions.RequestException as e:
                    if retry_count < max_retries - 1:
                        self.log_progress(f"Network error, retrying in {retry_delay//2} seconds... ({retry_count + 1}/{max_retries})")
                        import time
                        time.sleep(retry_delay//2)
                        retry_count += 1
                        continue
                    else:
                        raise Exception(f"Network error after {max_retries} attempts: {e}")
            
            # If we get here and response is None, all retries failed
            if response is None:
                raise Exception("Failed to get response after all retries")
            
            # Create temporary directory in raw data path
            temp_dir = self.config.RAW_DATA_PATH / f"temp_gtfs_{self.run_timestamp}"
            temp_dir.mkdir(parents=True, exist_ok=True)
            
            zip_path = temp_dir / "gtfs_schedule.zip"
            
            # Download with progress indication
            total_size = int(response.headers.get('content-length', 0))
            downloaded = 0
            
            with open(zip_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        if total_size > 0:
                            progress = (downloaded / total_size) * 100
                            if downloaded % (1024 * 1024 * 10) == 0:  # Every 10MB
                                self.log_progress(f"📥 Downloaded: {progress:.1f}% ({downloaded / (1024*1024):.1f}MB)")
            
            self.log_progress(f"✅ GTFS Schedule downloaded: {zip_path} ({downloaded / (1024*1024):.1f}MB)")
            return zip_path
            
        except requests.RequestException as e:
            error_msg = f"Failed to download GTFS Schedule: {e}"
            self.log_progress(error_msg, "error")
            self.errors.append(error_msg)
            return None
    
    def _extract_gtfs_data(self, zip_path):
        """Extract GTFS ZIP file"""
        self.log_progress("📂 Extracting GTFS Schedule data...")
        
        try:
            temp_path = zip_path.parent / "extracted"
            temp_path.mkdir(parents=True, exist_ok=True)
            
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(temp_path)
            
            self.log_progress(f"✅ GTFS data extracted to: {temp_path}")
            
            # List the contents to understand structure
            gtfs_files = list(temp_path.rglob("*.txt"))
            self.log_progress(f"📄 Found {len(gtfs_files)} GTFS .txt files")
            
            return temp_path
            
        except Exception as e:
            error_msg = f"Failed to extract GTFS data: {e}"
            self.log_progress(error_msg, "error")
            self.errors.append(error_msg)
            return None
    
    def _process_gtfs_files(self, temp_path):
        """Process key GTFS files and convert to Parquet"""
        self.log_progress("🔄 Processing GTFS files to Parquet format...")
        
        # Key GTFS files we want to process
        gtfs_files = {
            'routes': 'Complete route information (train lines, tram routes, bus routes)',
            'stops': 'Station and platform information WITH REAL PLATFORM NUMBERS',
            'stop_times': 'Departure times for each platform',
            'trips': 'Individual service trips',
            'calendar': 'Service operating days',
            'agency': 'Transport operators',
            'shapes': 'Route geographic paths'
        }
        
        for file_name, description in gtfs_files.items():
            self.log_progress(f"📊 Processing {file_name}.txt: {description}")
            
            # Look for the file in extracted data
            txt_files = list(temp_path.rglob(f"{file_name}.txt"))
            
            if txt_files:
                # Process the first matching file
                txt_file = txt_files[0]
                
                try:
                    # Read CSV with appropriate encoding
                    df = pd.read_csv(txt_file, encoding='utf-8')
                    
                    # Add ingestion metadata
                    df['gtfs_ingestion_timestamp'] = self.run_timestamp
                    df['gtfs_ingestion_date'] = datetime.now().date()
                    
                    # Save using parent class method
                    self.save_as_parquet(df.to_dict('records'), f"gtfs_schedule_{file_name}")
                    
                    self.log_progress(f"💾 Saved {len(df)} {file_name} records")
                    
                    # Log key details for important files
                    if file_name == 'stops':
                        # Check for platform information
                        platform_stops = df[df['stop_name'].str.contains('Platform', na=False, case=False)]
                        self.log_progress(f"🚂 Found {len(platform_stops)} platform records in stops.txt!")
                        
                        # Show sample platform records
                        if len(platform_stops) > 0:
                            sample_platforms = platform_stops[['stop_id', 'stop_name', 'platform_code']].head(3) if 'platform_code' in platform_stops.columns else platform_stops[['stop_id', 'stop_name']].head(3)
                            self.log_progress(f"📋 Sample platforms: {sample_platforms.to_dict('records')}")
                    
                    elif file_name == 'routes':
                        # Show route types
                        route_types = df['route_type'].value_counts() if 'route_type' in df.columns else {}
                        self.log_progress(f"🚊 Route types: {dict(route_types)}")
                    
                except Exception as e:
                    error_msg = f"Failed to process {file_name}.txt: {e}"
                    self.log_progress(error_msg, "error")
                    self.errors.append(error_msg)
            else:
                self.log_progress(f"⚠️ {file_name}.txt not found in GTFS data", "warning")
    
    def _create_platform_analysis(self):
        """Create a specific analysis of platform information"""
        self.log_progress("🚂 Creating platform number analysis...")
        
        try:
            # Look for stops data in our saved datasets
            stops_data = None
            for dataset in self.datasets_ingested:
                if dataset['name'] == 'gtfs_schedule_stops':
                    stops_data = dataset['data']
                    break
            
            if not stops_data:
                self.log_progress("⚠️ No stops data found for platform analysis", "warning")
                return
            
            stops_df = pd.DataFrame(stops_data)
            
            # Analyze platform information
            platform_analysis = {
                'total_stops': len(stops_df),
                'platform_stops': len(stops_df[stops_df['stop_name'].str.contains('Platform', na=False, case=False)]),
                'stations_with_platforms': 0,
                'platform_codes_available': 'platform_code' in stops_df.columns,
                'analysis_timestamp': self.run_timestamp
            }
            
            # Find parent stations
            if 'parent_station' in stops_df.columns:
                stations = stops_df[stops_df['parent_station'].isna() | (stops_df['parent_station'] == '')]
                platform_analysis['total_stations'] = len(stations)
                
                # Count stations with platform children
                platforms_df = stops_df[stops_df['parent_station'].notna() & (stops_df['parent_station'] != '')]
                platform_analysis['stations_with_platforms'] = len(platforms_df['parent_station'].unique())
            
            # Platform code analysis
            if 'platform_code' in stops_df.columns:
                platform_codes = stops_df[stops_df['platform_code'].notna() & (stops_df['platform_code'] != '')]
                platform_analysis['stops_with_platform_codes'] = len(platform_codes)
                platform_analysis['unique_platform_codes'] = len(platform_codes['platform_code'].unique())
            
            # Save analysis
            self.save_as_parquet([platform_analysis], "gtfs_schedule_platform_analysis")
            
            self.log_progress(f"📊 Platform Analysis: {platform_analysis}")
            
        except Exception as e:
            error_msg = f"Platform analysis failed: {e}"
            self.log_progress(error_msg, "error")
            self.errors.append(error_msg)
    
    def _cleanup(self, temp_path):
        """Clean up temporary files"""
        try:
            if temp_path and temp_path.exists():
                shutil.rmtree(temp_path.parent)  # Remove entire temp directory
                self.log_progress("🧹 Cleaned up temporary files")
        except Exception as e:
            self.log_progress(f"⚠️ Cleanup warning: {e}", "warning")
