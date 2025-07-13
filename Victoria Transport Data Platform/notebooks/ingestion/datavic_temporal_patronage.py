"""
Data.vic Monthly Patronage Ingestion Component
Handles ingestion of monthly average patronage by day type and mode data
"""
import requests
from datetime import datetime, timedelta
from .base_ingestion import BaseIngestionComponent


class DataVicTemporalPatronageComponent(BaseIngestionComponent):
    """
    Component for ingesting monthly average patronage by day type and mode from Data.vic.gov.au
    """
    
    def get_component_name(self):
        return "Data.vic Monthly Patronage by Mode"
    
    def ingest_data(self):
        """Ingest monthly patronage data from Data.vic.gov.au"""
        self.log_progress("Starting Data.vic monthly patronage ingestion...")
        
        if not self.config.DATA_VIC_API_KEY:
            self.log_progress("Data.vic API key not configured - skipping monthly patronage ingestion", "warning")
            return self.get_summary()
        
        # Resource ID for monthly average patronage by day type and mode
        resource_id = "d49915dc-98ac-45d1-afd8-3aea823ce292"
        
        # Ingest recent data (last 3 months to avoid overwhelming the system)
        patronage_data = self._fetch_monthly_patronage_data(resource_id)
        
        if patronage_data:
            self.save_as_parquet(patronage_data, "patronage_monthly")
            self.log_progress(f"✅ Successfully ingested monthly patronage data")
        else:
            self.log_progress("❌ Failed to ingest monthly patronage data", "error")
        
        self.log_progress(f"Data.vic monthly patronage ingestion complete. Datasets: {len(self.datasets_ingested)}")
        return self.get_summary()
    
    def _fetch_monthly_patronage_data(self, resource_id):
        """Fetch monthly patronage data from Data.vic datastore API"""
        try:
            # Calculate filter for recent data (last 6 months)
            current_date = datetime.now()
            cutoff_date = current_date - timedelta(days=180)
            cutoff_year = cutoff_date.year
            cutoff_month = cutoff_date.month
            
            # Fetch filtered data to avoid getting all 4000+ records at once
            params = {
                'resource_id': resource_id,
                'limit': 2000,  # Reasonable limit for recent data
                'filters': f'{{"Year": "{cutoff_year}", "Month": "{cutoff_month}"}}'
            }
            
            response_data = self._call_datavic_datastore_api(params)
            
            if response_data and 'result' in response_data:
                records = response_data['result'].get('records', [])
                self.log_progress(f"Retrieved {len(records)} monthly patronage records")
                
                # If we got fewer records than expected, try without date filter
                if len(records) < 100:
                    self.log_progress("Few records found with date filter, trying without filter...")
                    params_no_filter = {
                        'resource_id': resource_id,
                        'limit': 2000
                    }
                    response_data = self._call_datavic_datastore_api(params_no_filter)
                    if response_data and 'result' in response_data:
                        records = response_data['result'].get('records', [])
                        self.log_progress(f"Retrieved {len(records)} records without date filter")
                
                return records
            else:
                self.log_progress("No monthly patronage data found in API response", "warning")
                return None
                
        except Exception as e:
            error_msg = f"Error fetching monthly patronage data: {e}"
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
