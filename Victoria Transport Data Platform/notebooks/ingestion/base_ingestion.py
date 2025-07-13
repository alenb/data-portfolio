"""
Base Ingestion Component
Provides common functionality for all ingestion components
"""

import pandas as pd
import json
import logging
from datetime import datetime
from pathlib import Path
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)


class BaseIngestionComponent(ABC):
    """
    Base class for all ingestion components.
    Provides common functionality like data saving, logging, and error handling.
    """
    
    def __init__(self, config, run_timestamp, run_data_path, orchestration_config=None):
        """
        Initialize base ingestion component
        
        Args:
            config: Configuration object with API keys and settings
            run_timestamp: Timestamp for this ingestion run
            run_data_path: Path where data should be saved
            orchestration_config: Orchestration configuration for retry/rate limiting settings
        """
        self.config = config
        self.run_timestamp = run_timestamp
        self.run_data_path = run_data_path
        self.orchestration_config = orchestration_config or {}
        self.logger = logger
        self.datasets_ingested = []
        self.errors = []
        
    def save_as_parquet(self, data, filename, metadata=None):
        """Save data as Parquet file with optional metadata"""
        if data is None:
            self.logger.warning(f"No data to save for {filename}")
            return None
            
        filepath = self.run_data_path / f"{filename}_{self.run_timestamp}.parquet"
        
        try:
            if isinstance(data, dict):
                # Convert dict to DataFrame
                if 'result' in data and isinstance(data['result'], dict):
                    # Handle API response with nested result
                    if isinstance(data['result'], list):
                        df = pd.DataFrame(data['result'])
                    else:
                        df = pd.json_normalize(data['result'])
                elif isinstance(data, dict) and any(isinstance(v, list) for v in data.values()):
                    # Handle dict with list values
                    df = pd.json_normalize(data)
                else:
                    # Convert simple dict to single-row DataFrame
                    df = pd.DataFrame([data])
            elif isinstance(data, list):
                df = pd.DataFrame(data)
            else:
                df = pd.DataFrame([{'data': data}])
            
            # Add ingestion metadata
            df['ingestion_timestamp'] = self.run_timestamp
            df['ingestion_date'] = datetime.now().date()
            
            # Save as Parquet
            df.to_parquet(filepath, index=False)
            self.logger.info(f"💾 Saved {len(df)} records to {filepath.name}")
            self.datasets_ingested.append(filename)
            return filepath
            
        except Exception as e:
            self.logger.error(f"Error saving {filename} as Parquet: {e}")
            self.errors.append(f"Error saving {filename}: {str(e)}")
            
            # Fallback to JSON if Parquet fails
            try:
                json_filepath = self.run_data_path / f"{filename}_{self.run_timestamp}.json"
                with open(json_filepath, 'w') as f:
                    json.dump(data, f, indent=2, default=str)
                self.logger.info(f"Saved as JSON fallback: {json_filepath}")
                self.datasets_ingested.append(filename)
                return json_filepath
            except Exception as json_error:
                self.logger.error(f"JSON fallback also failed for {filename}: {json_error}")
                self.errors.append(f"JSON fallback failed for {filename}: {str(json_error)}")
                return None
    
    def add_rate_limit_delay(self, seconds=1):
        """Add delay to respect API rate limits"""
        import time
        time.sleep(seconds)
    
    def get_max_retries(self, component_name=None):
        """Get maximum retries from orchestration configuration"""
        # Get component-specific name if not provided
        if not component_name:
            component_name = self.get_component_name()
        
        # Check rate limiting configuration first
        rate_config = self.orchestration_config.get("rate_limiting", {})
        component_specific = rate_config.get("component_specific", {})
        
        # Convert component display name to config key
        config_key_map = {
            "GTFS Schedule Data (Static Timetables)": "gtfs_schedule",
            "GTFS Realtime Data (Live Transport Updates)": "gtfs_realtime",
            "PTV Timetable API Data": "ptv_timetable",
            "Data.vic Transport Dataset Metadata": "datavic_discovery"
        }
        
        config_key = config_key_map.get(component_name, component_name.lower().replace(" ", "_"))
        
        if config_key in component_specific:
            return component_specific[config_key].get("max_retries", 0)
        
        # Fallback to runtime settings
        return self.orchestration_config.get("runtime_settings", {}).get("max_retries", 0)
    
    def get_retry_delay(self, component_name=None):
        """Get retry delay from orchestration configuration"""
        if not component_name:
            component_name = self.get_component_name()
        
        rate_config = self.orchestration_config.get("rate_limiting", {})
        component_specific = rate_config.get("component_specific", {})
        
        config_key_map = {
            "GTFS Schedule Data (Static Timetables)": "gtfs_schedule",
            "GTFS Realtime Data (Live Transport Updates)": "gtfs_realtime",
            "PTV Timetable API Data": "ptv_timetable",
            "Data.vic Transport Dataset Metadata": "datavic_discovery"
        }
        
        config_key = config_key_map.get(component_name, component_name.lower().replace(" ", "_"))
        
        if config_key in component_specific:
            return component_specific[config_key].get("retry_delay", 30)
        
        # Fallback to runtime settings
        return self.orchestration_config.get("runtime_settings", {}).get("retry_delay_seconds", 30)
    
    def log_progress(self, message, level="info"):
        """Log progress with consistent formatting"""
        if level == "info":
            self.logger.info(message)
        elif level == "warning":
            self.logger.warning(message)
        elif level == "error":
            self.logger.error(message)
    
    def get_summary(self):
        """Get summary of ingestion results"""
        return {
            "component": self.__class__.__name__,
            "datasets_ingested": self.datasets_ingested,
            "total_datasets": len(self.datasets_ingested),
            "errors": self.errors,
            "has_errors": len(self.errors) > 0
        }
    
    @abstractmethod
    def ingest_data(self):
        """
        Main ingestion method - must be implemented by subclasses
        
        Returns:
            dict: Summary of ingestion results
        """
        pass
    
    @abstractmethod
    def get_component_name(self):
        """
        Get display name for this component
        
        Returns:
            str: Human-readable component name
        """
        pass
