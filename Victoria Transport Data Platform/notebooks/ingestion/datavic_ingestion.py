"""
Data.vic API Data Ingestion Component
Handles ingestion of transport dataset metadata from Data.vic.gov.au
"""

import requests
import time
from .base_ingestion import BaseIngestionComponent


class DataVicIngestionComponent(BaseIngestionComponent):
    """
    Component for ingesting transport dataset metadata from Data.vic.gov.au
    """
    
    def get_component_name(self):
        return "Data.vic Transport Metadata"
    
    def ingest_data(self):
        """Ingest priority transport dataset metadata from Data.vic.gov.au"""
        self.log_progress("Starting Data.vic transport dataset metadata ingestion...")
        
        if not self.config.DATA_VIC_API_KEY:
            self.log_progress("Data.vic API key not configured - skipping Data.vic ingestion", "warning")
            return self.get_summary()
        
        # Priority transport datasets for metadata collection only
        priority_searches = [
            "traffic volume",
            "public transport disruption", 
            "road disruption",
            "freight movement"
        ]
        
        for search_term in priority_searches:
            self._search_and_ingest_metadata(search_term)
            # Add delay between searches
            self.add_rate_limit_delay(1)
        
        self.log_progress(f"Data.vic transport metadata ingestion complete. Datasets: {len(self.datasets_ingested)}")
        return self.get_summary()
    
    def _search_and_ingest_metadata(self, search_term):
        """Search for datasets and ingest metadata"""
        # Search for transport-related datasets (metadata only)
        search_params = {
            'q': search_term,
            'fq': 'res_format:API OR tags:transport',
            'rows': 3  # Limit results to avoid clutter
        }
        
        search_result = self._call_datavic_api("/package_search", search_params)
        
        if search_result and search_result.get('result', {}).get('results'):
            # Process first matching dataset (metadata only)
            dataset = search_result['result']['results'][0]
            dataset_id = dataset['id']
            dataset_name = dataset.get('name', search_term.replace(' ', '_'))
            
            # Get full dataset metadata (this is what we actually need)
            dataset_details = self._call_datavic_api("/package_show", {'id': dataset_id})
            if dataset_details:
                self.save_as_parquet(dataset_details, f"datavic_{dataset_name}_metadata")
                self.log_progress(f"✅ Collected metadata for: {dataset_name}")
    
    def _call_datavic_api(self, endpoint, params=None):
        """Make authenticated Data.vic.gov.au API call"""
        if not self.config.DATA_VIC_API_KEY:
            self.log_progress("Skipping Data.vic API call - no API key configured", "warning")
            return None
            
        headers = {
            'accept': 'application/json',
            'apikey': self.config.DATA_VIC_API_KEY
        }
        
        # Use the Data.vic base URL from config
        url = f"{self.config.DATA_VIC_BASE_URL}{endpoint}"
        
        try:
            response = requests.get(url, headers=headers, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            self.log_progress(f"Successfully called Data.vic API: {endpoint}")
            
            # Add delay for Data.vic API as well
            self.add_rate_limit_delay(1)  # 1 second delay for Data.vic API
            
            return data
        except requests.RequestException as e:
            error_msg = f"Data.vic API error for {endpoint}: {e}"
            self.log_progress(error_msg, "error")
            self.errors.append(error_msg)
            
            # Add delay on error
            self.add_rate_limit_delay(2)
            
            return None
