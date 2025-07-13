"""
GTFS Realtime Data Ingestion Component
=====================================

Handles ingestion of GTFS Realtime data from VicRoads API.
This component collects live transport data including vehicle positions, 
trip updates, and service alerts.

Key Features:
- VicRoads GTFS Realtime API integration
- Protobuf data parsing and JSON conversion
- Live vehicle positions and trip updates
- Service disruption alerts
- Respects API rate limits and quotas
"""

import json
import pandas as pd
import requests
from datetime import datetime, timedelta
from google.transit import gtfs_realtime_pb2
from google.protobuf.json_format import MessageToDict
from .base_ingestion import BaseIngestionComponent


class GTFSRealtimeIngestionComponent(BaseIngestionComponent):
    """
    Component for ingesting GTFS Realtime data from VicRoads API
    """
    
    def get_component_name(self):
        return "GTFS Realtime Data (Live Transport Updates)"
    
    def ingest_data(self):
        """Ingest GTFS Realtime data from VicRoads API"""
        self.log_progress("📡 Starting GTFS Realtime data ingestion...")
        
        # Check for required dependencies
        try:
            import google.protobuf
            from google.transit import gtfs_realtime_pb2
        except ImportError:
            error_msg = "GTFS Realtime protobuf library not found! Please install: pip install gtfs-realtime-bindings"
            self.log_progress(error_msg, "error")
            self.errors.append(error_msg)
            return self.get_summary()
        
        # VicRoads GTFS Realtime API configuration
        base_url = self.config.GTFS_REALTIME_BASE_URL
        api_key = self.config.VICROADS_API_KEY
        
        if not api_key:
            error_msg = "VICROADS_API_KEY not found in configuration. Please set it in config/.env"
            self.log_progress(error_msg, "error")
            self.errors.append(error_msg)
            return self.get_summary()
        
        # GTFS Realtime endpoints
        endpoints = {
            'metro_train_trip_updates': f"{base_url}/metrotrain-tripupdates",
            'metro_bus_trip_updates': f"{base_url}/metrobus-tripupdates",
            'metro_train_alerts': f"{base_url}/metrotrain-servicealerts",
            'metro_train_vehicle_positions': f"{base_url}/metrotrain-vehicleposition-updates"
        }
        
        # Initialize quota tracking
        self.quota_tracker = self._initialize_quota_tracker()
        
        # Request headers
        headers = {
            'User-Agent': 'Resilient-Public-Services-Platform/1.0',
            'Accept': 'application/octet-stream',
            'Ocp-Apim-Subscription-Key': api_key
        }
        
        # Process each endpoint with enhanced retry logic
        total_entities = 0
        for endpoint_name, url in endpoints.items():
            # Check quota before processing endpoint
            if not self._check_quota_limit(endpoint_name):
                continue
            
            retry_count = 0
            max_retries = self.get_max_retries()
            retry_delay = self.get_retry_delay()
            
            self.log_progress(f"🔧 Using orchestration config: max_retries={max_retries}, retry_delay={retry_delay}s")
            
            # Handle case where max_retries is 0
            if max_retries == 0:
                self.log_progress(f"⚠️ max_retries=0, skipping {endpoint_name}")
                continue
            
            while retry_count < max_retries:
                try:
                    self.log_progress(f"📡 Fetching {endpoint_name}...")
                    
                    # Make API request
                    response = requests.get(url, headers=headers, timeout=30)
                    
                    # Update quota counter on successful request
                    self._update_quota_counter()
                    
                    # Handle specific HTTP status codes
                    if response.status_code == 429:
                        if retry_count < max_retries - 1:
                            error_msg = f"Rate limited for {endpoint_name}. Waiting {retry_delay} seconds before retry {retry_count + 1}/{max_retries}"
                            self.log_progress(error_msg, "warning")
                            self.add_rate_limit_delay(retry_delay)
                            retry_count += 1
                            continue
                        else:
                            error_msg = f"Rate limited for {endpoint_name}. Max retries exceeded."
                            self.log_progress(error_msg, "error")
                            self.errors.append(error_msg)
                            break
                        
                    elif response.status_code == 403:
                        if retry_count < max_retries - 1:
                            error_msg = f"API quota exceeded for {endpoint_name}. Waiting {retry_delay * 2} seconds before retry {retry_count + 1}/{max_retries}"
                            self.log_progress(error_msg, "warning")
                            self.add_rate_limit_delay(retry_delay * 2)
                            retry_count += 1
                            continue
                        else:
                            error_msg = f"API quota exceeded for {endpoint_name}. Check daily limits."
                            self.log_progress(error_msg, "error")
                            self.errors.append(error_msg)
                            break
                        
                    elif response.status_code == 401:
                        error_msg = f"Unauthorized access for {endpoint_name}. Check API key."
                        self.log_progress(error_msg, "error")
                        self.errors.append(error_msg)
                        break
                    
                    response.raise_for_status()
                    
                    # Parse protobuf data
                    feed = gtfs_realtime_pb2.FeedMessage()
                    feed.ParseFromString(response.content)
                    
                    # Convert to dictionary for easier processing
                    feed_dict = MessageToDict(feed)
                    
                    # Extract metadata
                    entity_count = len(feed_dict.get('entity', []))
                    feed_timestamp = feed_dict.get('header', {}).get('timestamp', '')
                    feed_version = feed_dict.get('header', {}).get('gtfsRealtimeVersion', '')
                    
                    self.log_progress(f"✅ {endpoint_name}: {entity_count} entities, version {feed_version}")
                    
                    # Save raw JSON data
                    self.save_as_json(feed_dict, f"gtfs_realtime_{endpoint_name}")
                    
                    # Save metadata
                    metadata = {
                        'endpoint': endpoint_name,
                        'entity_count': entity_count,
                        'feed_timestamp': feed_timestamp,
                        'feed_version': feed_version,
                        'response_size_bytes': len(response.content),
                        'collection_timestamp': datetime.now().isoformat(),
                        'http_status': response.status_code
                    }
                    self.save_as_json(metadata, f"gtfs_realtime_{endpoint_name}_metadata")
                    
                    # Convert to DataFrame for analytics if entities exist
                    if feed_dict.get('entity'):
                        # Flatten entity data for tabular storage
                        entities_df = pd.json_normalize(feed_dict['entity'])
                        
                        # Add ingestion metadata
                        entities_df['ingestion_timestamp'] = datetime.now()
                        entities_df['ingestion_run_id'] = self.run_timestamp
                        entities_df['endpoint_name'] = endpoint_name
                        entities_df['feed_timestamp'] = feed_timestamp
                        
                        # Save as parquet for analytics
                        self.save_as_parquet(entities_df.to_dict('records'), f"gtfs_realtime_{endpoint_name}_entities")
                        
                        total_entities += entity_count
                        self.log_progress(f"💾 Saved {entity_count} entities from {endpoint_name}")
                    else:
                        self.log_progress(f"💾 No entities for {endpoint_name}, saved metadata only")
                    
                    # Success - break out of retry loop
                    break
                    
                except requests.exceptions.RequestException as e:
                    if retry_count < max_retries - 1:
                        error_msg = f"Request failed for {endpoint_name}: {str(e)}. Retrying in 15 seconds..."
                        self.log_progress(error_msg, "warning")
                        self.add_rate_limit_delay(15)
                        retry_count += 1
                        continue
                    else:
                        error_msg = f"Request failed for {endpoint_name}: {str(e)}"
                        self.log_progress(error_msg, "error")
                        self.errors.append(error_msg)
                        break
                    
                except Exception as e:
                    if retry_count < max_retries - 1:
                        error_msg = f"Processing failed for {endpoint_name}: {str(e)}. Retrying in 10 seconds..."
                        self.log_progress(error_msg, "warning")
                        self.add_rate_limit_delay(10)
                        retry_count += 1
                        continue
                    else:
                        error_msg = f"Processing failed for {endpoint_name}: {str(e)}"
                        self.log_progress(error_msg, "error")
                        self.errors.append(error_msg)
                        break
                
            # Much longer delay between endpoints to respect rate limits
            # GTFS realtime APIs have strict quotas - use 10 second delays
            self.add_rate_limit_delay(10)
        
        self.log_progress(f"🎯 GTFS Realtime ingestion complete. Total entities: {total_entities}")
        self.log_progress(f"📊 Processed {len(endpoints)} endpoints with {len(self.errors)} errors")
        
        return self.get_summary()
    
    def _initialize_quota_tracker(self):
        """Initialize quota tracking for GTFS Realtime API"""
        quota_config = self.orchestration_config.get("components", {}).get("gtfs_realtime", {}).get("quota_management", {})
        
        if not quota_config:
            return None
        
        # Load existing quota data
        quota_file = self.run_data_path / "gtfs_realtime_quota.json"
        quota_data = {"date": None, "daily_calls": 0, "last_reset": None}
        
        try:
            if quota_file.exists():
                with open(quota_file, 'r') as f:
                    quota_data = json.load(f)
        except Exception as e:
            self.log_progress(f"⚠️ Could not load quota data: {e}", "warning")
        
        # Reset if it's a new day
        today = datetime.now().date().isoformat()
        if quota_data.get("date") != today:
            quota_data = {
                "date": today,
                "daily_calls": 0,
                "last_reset": datetime.now().isoformat()
            }
        
        return {
            "config": quota_config,
            "data": quota_data,
            "file_path": quota_file
        }
    
    def _check_quota_limit(self, endpoint_name: str) -> bool:
        """Check if we've exceeded the daily quota limit"""
        if not self.quota_tracker:
            return True  # No quota tracking, allow
        
        config = self.quota_tracker["config"]
        data = self.quota_tracker["data"]
        
        daily_limit = config.get("daily_limit", 1000)
        priority_endpoints = config.get("priority_endpoints", [])
        
        # Check if we've exceeded the daily limit
        if data["daily_calls"] >= daily_limit:
            # Only allow priority endpoints if over quota
            if endpoint_name not in priority_endpoints:
                self.log_progress(f"⚠️ Daily quota exceeded ({data['daily_calls']}/{daily_limit}), skipping non-priority endpoint: {endpoint_name}", "warning")
                return False
            else:
                self.log_progress(f"🎯 Priority endpoint {endpoint_name} allowed despite quota limit", "info")
        
        return True
    
    def _update_quota_counter(self):
        """Update quota counter after successful API call"""
        if not self.quota_tracker:
            return
        
        self.quota_tracker["data"]["daily_calls"] += 1
        
        # Save updated quota data
        try:
            with open(self.quota_tracker["file_path"], 'w') as f:
                json.dump(self.quota_tracker["data"], f, indent=2)
        except Exception as e:
            self.log_progress(f"⚠️ Could not save quota data: {e}", "warning")
    
    def _handle_quota_exceeded(self, endpoint_name: str):
        """Handle quota exceeded scenario"""
        if not self.quota_tracker:
            return
        
        config = self.quota_tracker["config"]
        retry_after = config.get("retry_after_quota", "24h")
        
        # Parse retry_after (simplified - just handle hours)
        if retry_after.endswith("h"):
            hours = int(retry_after[:-1])
            retry_timestamp = datetime.now() + timedelta(hours=hours)
            
            error_msg = f"Quota exceeded for {endpoint_name}. Retry after {retry_after} at {retry_timestamp.strftime('%Y-%m-%d %H:%M:%S')}"
            self.log_progress(error_msg, "error")
            self.errors.append(error_msg)
    
    def save_as_json(self, data, name):
        """Save data as JSON file"""
        try:
            # Create JSON file in raw data path
            json_filename = f"{name}_{self.run_timestamp}.json"
            json_filepath = self.config.RAW_DATA_PATH / json_filename
            
            with open(json_filepath, 'w') as f:
                json.dump(data, f, indent=2, default=str)
            
            # Track in datasets
            self.datasets_ingested.append({
                'name': name,
                'type': 'json',
                'records': 1,
                'file_path': str(json_filepath),
                'timestamp': self.run_timestamp
            })
            
        except Exception as e:
            error_msg = f"Failed to save JSON {name}: {str(e)}"
            self.log_progress(error_msg, "error")
            self.errors.append(error_msg)
