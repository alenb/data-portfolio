"""
Base Transformation Component
Provides common functionality for all data transformations
"""
import pandas as pd
import logging
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path


class BaseTransformation(ABC):
    """
    Base class for all data transformations.
    
    Provides common functionality:
    - Data loading and saving
    - Logging setup
    - Error handling
    - Type conversion utilities
    """
    
    def __init__(self, config, run_timestamp):
        """Initialize transformation component"""
        self.config = config
        self.run_timestamp = run_timestamp
        self.logger = logging.getLogger(self.__class__.__name__)
        self.transformation_date = str(datetime.now().date())
    
    @abstractmethod
    def transform(self, latest_raw_run):
        """
        Transform raw data - implemented by each component
        
        Args:
            latest_raw_run: Path to latest raw data run directory
            
        Returns:
            pd.DataFrame: Transformed data
        """
        pass
    
    @abstractmethod
    def get_output_filename(self):
        """Return the output filename for this transformation"""
        pass
    
    def load_raw_parquet(self, latest_raw_run, pattern):
        """Load raw parquet files matching pattern"""
        files = list(latest_raw_run.glob(f"{pattern}*.parquet"))
        if not files:
            self.logger.warning(f"No files found matching pattern: {pattern}")
            return None
        
        # Load the most recent file
        latest_file = max(files, key=lambda x: x.stat().st_mtime)
        self.logger.info(f"Loading: {latest_file.name}")
        return pd.read_parquet(latest_file)
    
    def save_data(self, df, base_path):
        """Common save functionality"""
        if df is None or df.empty:
            self.logger.warning(f"No data to save for {self.get_output_filename()}")
            return None
        
        filepath = base_path / f"{self.get_output_filename()}_{self.run_timestamp}.parquet"
        df.to_parquet(filepath, index=False)
        self.logger.info(f"Saved {len(df)} records to {filepath}")
        return filepath
    
    def add_transformation_metadata(self, df):
        """Add common transformation metadata columns"""
        if df is None or df.empty:
            return df
        
        df['transformation_timestamp'] = pd.Series([self.run_timestamp] * len(df)).astype('string')
        df['transformation_date'] = pd.Series([self.transformation_date] * len(df)).astype('string')
        return df
    
    def ensure_string_type(self, series, default_value=''):
        """Ensure series is string type with default value for nulls"""
        return series.fillna(default_value).astype('string')
    
    def ensure_int_type(self, series, default_value=0):
        """Ensure series is Int64 type with default value for nulls"""
        return series.fillna(default_value).astype('Int64')
    
    def ensure_float_type(self, series, default_value=0.0):
        """Ensure series is float64 type with default value for nulls"""
        return series.fillna(default_value).astype('float64')
    
    def get_route_type_mapping(self):
        """Map route type IDs to names"""
        return {
            0: 'Train',
            1: 'Tram', 
            2: 'Bus',
            3: 'V/Line',
            4: 'Night Bus'
        }
    
    def get_station_id_mapping(self):
        """Map station names to IDs"""
        return {
            "Flinders_Street": 1071,
            "Southern_Cross": 1104, 
            "Melbourne_Central": 1155,
            "Parliament": 1181
        }
