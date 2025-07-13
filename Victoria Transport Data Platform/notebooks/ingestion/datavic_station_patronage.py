"""
Data.vic Annual Station Patronage Ingestion Component
Handles ingestion of annual metropolitan train station patronage data
"""
import requests
from .base_ingestion import BaseIngestionComponent


class DataVicStationPatronageComponent(BaseIngestionComponent):
    """
    Component for ingesting annual metropolitan train station patronage data from Data.vic.gov.au
    """
    
    def get_component_name(self):
        return "Data.vic Annual Station Patronage"
    
    def ingest_data(self):
        """Ingest annual station patronage data from Data.vic.gov.au"""
        self.log_progress("Starting Data.vic annual station patronage ingestion...")
        
        if not self.config.DATA_VIC_API_KEY:
            self.log_progress("Data.vic API key not configured - skipping station patronage ingestion", "warning")
            return self.get_summary()
        
        # Resource ID for annual metropolitan train station patronage
        resource_id = "57faf356-36a3-4bbe-87fe-f0f05d1b8996"
        
        # Ingest the full dataset (only ~222 records)
        patronage_data = self._fetch_station_patronage_data(resource_id)
        
        if patronage_data:
            self.save_as_parquet(patronage_data, "station_patronage_annual")
            self.log_progress(f"✅ Successfully ingested annual station patronage data")
        else:
            self.log_progress("❌ Failed to ingest annual station patronage data", "error")
        
        self.log_progress(f"Data.vic annual station patronage ingestion complete. Datasets: {len(self.datasets_ingested)}")
        return self.get_summary()
    
    def _fetch_station_patronage_data(self, resource_id):
        """Fetch station patronage data from Data.vic datastore API"""
        try:
            # Fetch all records (dataset is small at ~222 records)
            params = {
                'resource_id': resource_id,
                'limit': 1000  # Ensure we get all records
            }
            
            response_data = self._call_datavic_datastore_api(params)
            
            if response_data and 'result' in response_data:
                records = response_data['result'].get('records', [])
                self.log_progress(f"Retrieved {len(records)} station patronage records")
                
                # Return the records directly for parquet conversion
                return records
            else:
                self.log_progress("No station patronage data found in API response", "warning")
                return None
                
        except Exception as e:
            error_msg = f"Error fetching station patronage data: {e}"
            self.log_progress(error_msg, "error")
            self.errors.append(error_msg)
            return None
    
    def _call_datavic_datastore_api(self, params):
        """Make authenticated call to Data.vic datastore API"""
        headers = {
            'accept': 'application/json',
            'apikey': self.config.DATA_VIC_API_KEY
        }
        
        # Use the datastore_search endpoint for direct data access
        url = self.config.DATA_VIC_DATASTORE_SEARCH_URL
        
        try:
            response = requests.get(url, headers=headers, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            if data.get('success'):
                self.log_progress(f"Successfully called Data.vic datastore API")
                
                # Add rate limiting delay
                self.add_rate_limit_delay(1)
                
                return data
            else:
                error_msg = f"Data.vic API returned success=false: {data.get('error', 'Unknown error')}"
                self.log_progress(error_msg, "error")
                self.errors.append(error_msg)
                return None
                
        except requests.RequestException as e:
            error_msg = f"Data.vic datastore API error: {e}"
            self.log_progress(error_msg, "error")
            self.errors.append(error_msg)
            
            # Add delay on error
            self.add_rate_limit_delay(2)
            
            return None
