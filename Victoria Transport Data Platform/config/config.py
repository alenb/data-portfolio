"""
Configuration management for the Disruption-Aware Network Resilience Intelligence project.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from config/.env
config_dir = Path(__file__).parent
env_file = config_dir / '.env'
load_dotenv(env_file)

class Config:
    """Configuration class for the project."""
    
    # Project paths
    PROJECT_ROOT = Path(__file__).parent.parent
    DATA_ROOT = PROJECT_ROOT / "data"
    RAW_DATA_PATH = DATA_ROOT / "raw"
    CURATED_DATA_PATH = DATA_ROOT / "curated"
    LOGS_PATH = PROJECT_ROOT / "logs"
    
    # API Configuration
    PTV_USER_ID = os.getenv("PTV_USER_ID")
    PTV_API_KEY = os.getenv("PTV_API_KEY")
    PTV_BASE_URL = os.getenv("PTV_BASE_URL", "https://timetableapi.ptv.vic.gov.au")
    
    # Data.vic.gov.au API Configuration
    DATA_VIC_API_KEY = os.getenv("DATA_VIC_API_KEY")
    DATA_VIC_SECRET_KEY = os.getenv("DATA_VIC_SECRET_KEY")
    DATA_VIC_BASE_URL = os.getenv("DATA_VIC_BASE_URL", "https://wovg-community.gateway.prod.api.vic.gov.au/datavic/v1.2")
    DATA_VIC_PACKAGE_SEARCH_URL = os.getenv("DATA_VIC_PACKAGE_SEARCH_URL")
    DATA_VIC_DATASTORE_SEARCH_URL = os.getenv("DATA_VIC_DATASTORE_SEARCH_URL")
    DATA_VIC_DATASET_BASE_URL = os.getenv("DATA_VIC_DATASET_BASE_URL", "https://data.vic.gov.au/data/dataset")
    
    # GTFS Schedule Configuration
    GTFS_SCHEDULE_DATASET_ID = os.getenv("GTFS_SCHEDULE_DATASET_ID")
    GTFS_SCHEDULE_RESOURCE_ID = os.getenv("GTFS_SCHEDULE_RESOURCE_ID")
    GTFS_SCHEDULE_URL = os.getenv("GTFS_SCHEDULE_URL")
    
    # GTFS Realtime Configuration
    GTFS_REALTIME_BASE_URL = os.getenv("GTFS_REALTIME_BASE_URL", "https://data-exchange-api.vicroads.vic.gov.au/opendata/v1/gtfsr")
    
    # VicRoads API Configuration (for GTFS Realtime)
    VICROADS_API_KEY = os.getenv("VICROADS_API_KEY")
    
    # GTFS Realtime URLs
    GTFS_TRIP_UPDATES_URL = os.getenv("GTFS_TRIP_UPDATES_URL")
    GTFS_BUS_TRIP_UPDATES_URL = os.getenv("GTFS_BUS_TRIP_UPDATES_URL")
    GTFS_ALERTS_URL = os.getenv("GTFS_ALERTS_URL")
    GTFS_VEHICLE_POSITIONS_URL = os.getenv("GTFS_VEHICLE_POSITIONS_URL")
    
    # EPA API (free access)
    EPA_BASE_URL = os.getenv("EPA_BASE_URL", "https://environment.data.gov.au")
    
    # ABS API (free access - no API key required as of Nov 2024)
    ABS_BASE_URL = os.getenv("ABS_BASE_URL", "https://api.abs.gov.au")
    
    # Victorian Important Dates
    VIC_DATES_API_URL = os.getenv("VIC_DATES_API_URL")
    
    # Databricks Configuration
    DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
    DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
    DATABRICKS_CLUSTER_ID = os.getenv("DATABRICKS_CLUSTER_ID")
    
    # DBFS Paths
    DBFS_RAW_PATH = os.getenv("DBFS_RAW_PATH", "/FileStore/shared_uploads/disruption_intelligence/raw")
    DBFS_CURATED_PATH = os.getenv("DBFS_CURATED_PATH", "/FileStore/shared_uploads/disruption_intelligence/curated")
    
    # Processing Configuration
    BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1000"))
    MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
    RETRY_DELAY = int(os.getenv("RETRY_DELAY", "5"))
    
    # Logging Configuration
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    LOG_FILE = os.getenv("LOG_FILE", "logs/disruption_intelligence.log")
    
    # Geographic Boundaries
    MIN_LATITUDE = float(os.getenv("MIN_LATITUDE", "-38.5"))
    MAX_LATITUDE = float(os.getenv("MAX_LATITUDE", "-37.5"))
    MIN_LONGITUDE = float(os.getenv("MIN_LONGITUDE", "144.5"))
    MAX_LONGITUDE = float(os.getenv("MAX_LONGITUDE", "145.5"))
    
    # Risk Modeling Thresholds
    HIGH_TEMP_THRESHOLD = int(os.getenv("HIGH_TEMP_THRESHOLD", "35"))
    HIGH_WIND_THRESHOLD = int(os.getenv("HIGH_WIND_THRESHOLD", "50"))
    AIR_QUALITY_THRESHOLD = int(os.getenv("AIR_QUALITY_THRESHOLD", "100"))
    DELAY_THRESHOLD_MINUTES = int(os.getenv("DELAY_THRESHOLD_MINUTES", "15"))
    
    @classmethod
    def ensure_directories(cls):
        """Ensure all required directories exist."""
        directories = [
            cls.DATA_ROOT,
            cls.RAW_DATA_PATH,
            cls.CURATED_DATA_PATH,
            cls.LOGS_PATH
        ]
        
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
    
    @classmethod
    def validate_config(cls):
        """Validate that required configuration is present."""
        required_configs = [
            ("PTV_API_KEY", cls.PTV_API_KEY),
            ("VICROADS_API_KEY", cls.VICROADS_API_KEY),
            ("DATABRICKS_TOKEN", cls.DATABRICKS_TOKEN),
        ]
        
        missing_configs = []
        for config_name, config_value in required_configs:
            if not config_value:
                missing_configs.append(config_name)
        
        if missing_configs:
            raise ValueError(f"Missing required configuration: {', '.join(missing_configs)}")
        
        return True
