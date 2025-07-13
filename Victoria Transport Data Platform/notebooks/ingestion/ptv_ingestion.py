"""
PTV API Data Ingestion Component
Handles ingestion of PTV API data for real-time disruptions and route information
"""

import requests
import hmac
import hashlib
import time
from .base_ingestion import BaseIngestionComponent


class PTVIngestionComponent(BaseIngestionComponent):
    """
    Component for ingesting PTV API data for real-time disruptions and route status
    """
    
    def get_component_name(self):
        return "PTV API Data"
    
    def ingest_data(self):
        """Ingest PTV data for real-time disruptions and route status"""
        self.log_progress("Starting PTV core data ingestion for real-time disruptions...")
        
        if not self.config.PTV_API_KEY:
            self.log_progress("PTV API key not configured - skipping PTV ingestion", "warning")
            return self.get_summary()
        
        # Get all routes (for route status)
        self._ingest_routes()
        
        # Add pause between different endpoint types
        self.log_progress("⏱️ Pausing between endpoints...")
        self.add_rate_limit_delay(3)
        
        # Get current disruptions (real-time data)
        self._ingest_disruptions()
        
        # Add pause before route types
        self.log_progress("⏱️ Pausing before route types...")
        self.add_rate_limit_delay(3)
        
        # Get route types for proper classification
        self._ingest_route_types()
        
        self.log_progress(f"🎯 PTV real-time data ingestion complete. Datasets: {len(self.datasets_ingested)}")
        self.log_progress("✅ Successfully collected real-time disruption and route status data")
        
        return self.get_summary()
    
    def _ingest_routes(self):
        """Ingest routes data"""
        self.log_progress("🛤️ Collecting routes data...")
        routes_data = self._call_ptv_api("/v3/routes")
        if routes_data:
            self.save_as_parquet(routes_data, "ptv_routes")
    
    def _ingest_disruptions(self):
        """Ingest disruptions data"""
        self.log_progress("🚨 Collecting disruptions data...")
        disruptions_data = self._call_ptv_api("/v3/disruptions")
        if disruptions_data:
            self.save_as_parquet(disruptions_data, "ptv_disruptions")
    
    def _ingest_route_types(self):
        """Ingest route types data"""
        self.log_progress("🚊 Collecting route types data...")
        route_types = self._call_ptv_api("/v3/route_types")
        if route_types:
            self.save_as_parquet(route_types, "ptv_route_types")
    
    def _generate_ptv_signature(self, request_url):
        """Generate HMAC-SHA1 signature for PTV API authentication"""
        if not self.config.PTV_API_KEY:
            return None
            
        signature = hmac.new(
            self.config.PTV_API_KEY.encode('utf-8'),
            request_url.encode('utf-8'),
            hashlib.sha1
        ).hexdigest()
        return signature
    
    def _call_ptv_api(self, endpoint, params=None):
        """Make authenticated PTV API call with proper parameter formatting"""
        if not self.config.PTV_API_KEY:
            self.log_progress("Skipping PTV API call - no API key configured", "warning")
            return None
            
        if params is None:
            params = {}
        
        # Fix parameter formatting issues that cause 403 errors
        formatted_params = {}
        
        for key, value in params.items():
            if key == 'expand' and isinstance(value, list):
                # Fix: ['All'] becomes "All", ['Run', 'Disruption'] becomes "Run,Disruption"
                formatted_params[key] = ','.join(value)
            elif key in ['platform_numbers'] and isinstance(value, list):
                # Fix: Don't include empty arrays that cause issues
                if value:  # Only include if not empty
                    formatted_params[key] = ','.join(map(str, value))
                # Skip empty arrays entirely
            elif key in ['include_cancelled', 'include_geopath', 'gtfs'] and isinstance(value, bool):
                # Fix: Boolean to lowercase string
                formatted_params[key] = str(value).lower()
            elif value is not None:
                formatted_params[key] = str(value)
        
        # Add developer ID
        formatted_params['devid'] = self.config.PTV_USER_ID
        
        # Build query string manually to avoid URL encoding issues
        query_parts = []
        for key, value in formatted_params.items():
            query_parts.append(f"{key}={value}")
        
        query_string = "&".join(query_parts)
        request_url = f"{endpoint}?{query_string}"
        
        # Generate signature
        signature = self._generate_ptv_signature(request_url)
        
        # Make request
        full_url = f"{self.config.PTV_BASE_URL}{request_url}&signature={signature}"
        
        try:
            response = requests.get(full_url, timeout=30)
            response.raise_for_status()
            data = response.json()
            self.log_progress(f"Successfully called PTV API: {endpoint}")
            
            # Add mandatory delay to respect rate limits
            self.add_rate_limit_delay(2)  # 2 second delay between API calls
            
            return data
        except requests.RequestException as e:
            error_msg = f"PTV API error for {endpoint}: {e}"
            self.log_progress(error_msg, "error")
            self.errors.append(error_msg)
            
            # Add delay even on failure to avoid hammering the API
            self.add_rate_limit_delay(3)  # Longer delay on error
            
            return None
