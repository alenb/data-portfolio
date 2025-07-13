"""
Change Detection Module for Data Ingestion Pipeline
Implements intelligent change detection to avoid unnecessary data processing
"""

import hashlib
import json
import logging
import os
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, Optional, List
import requests
import pandas as pd

logger = logging.getLogger(__name__)


class ChangeDetector:
    """
    Intelligent change detection system that determines if data sources have changed
    since the last successful ingestion, avoiding unnecessary processing.
    """
    
    def __init__(self, config: Dict, cache_directory: Path):
        """
        Initialize change detector with configuration
        
        Args:
            config: Change detection configuration
            cache_directory: Directory to store change detection cache
        """
        self.config = config
        self.cache_directory = Path(cache_directory)
        self.cache_directory.mkdir(parents=True, exist_ok=True)
        
        # Initialize detection methods
        self.methods = self.config.get("methods", {})
        self.enabled_methods = [
            method for method, settings in self.methods.items() 
            if settings.get("enabled", False)
        ]
        
        logger.info(f"🔍 Change detector initialized with methods: {', '.join(self.enabled_methods)}")
    
    def has_changed(self, component_name: str, data_source: Dict) -> bool:
        """
        Check if a data source has changed since last ingestion
        
        Args:
            component_name: Name of the component
            data_source: Data source configuration with URL, headers, etc.
            
        Returns:
            True if data has changed, False otherwise
        """
        cache_file = self.cache_directory / f"{component_name}_change_cache.json"
        
        # Load previous cache
        previous_cache = self._load_cache(cache_file)
        current_cache = {}
        
        # Check each enabled detection method
        change_detected = False
        
        for method in self.enabled_methods:
            try:
                if method == "file_hash":
                    change_detected |= self._check_file_hash(data_source, previous_cache, current_cache)
                elif method == "api_etag":
                    change_detected |= self._check_api_etag(data_source, previous_cache, current_cache)
                elif method == "record_count":
                    change_detected |= self._check_record_count(data_source, previous_cache, current_cache)
                elif method == "last_modified":
                    change_detected |= self._check_last_modified(data_source, previous_cache, current_cache)
                
            except Exception as e:
                logger.warning(f"Change detection method {method} failed for {component_name}: {e}")
                # On error, assume changed to be safe
                change_detected = True
        
        # Update cache with current values
        current_cache["last_check"] = datetime.now().isoformat()
        self._save_cache(cache_file, current_cache)
        
        logger.info(f"🔍 Change detection for {component_name}: {'CHANGED' if change_detected else 'NO CHANGE'}")
        return change_detected
    
    def _check_file_hash(self, data_source: Dict, previous_cache: Dict, current_cache: Dict) -> bool:
        """Check if file hash has changed"""
        file_path = data_source.get("file_path")
        if not file_path or not Path(file_path).exists():
            return True  # File doesn't exist, assume changed
        
        algorithm = self.methods["file_hash"].get("algorithm", "sha256")
        
        # Calculate current file hash
        current_hash = self._calculate_file_hash(file_path, algorithm)
        current_cache["file_hash"] = current_hash
        
        previous_hash = previous_cache.get("file_hash")
        return current_hash != previous_hash
    
    def _check_api_etag(self, data_source: Dict, previous_cache: Dict, current_cache: Dict) -> bool:
        """Check if API ETag has changed"""
        url = data_source.get("url")
        if not url:
            return True
        
        headers = data_source.get("headers", {})
        
        try:
            # Make HEAD request to get ETag with shorter timeout
            response = requests.head(url, headers=headers, timeout=5)
            current_etag = response.headers.get("ETag")
            
            if current_etag:
                current_cache["api_etag"] = current_etag
                previous_etag = previous_cache.get("api_etag")
                return current_etag != previous_etag
            else:
                # No ETag available, check last-modified or assume changed
                return True
                
        except (requests.RequestException, requests.exceptions.Timeout) as e:
            # Only log DNS errors at debug level to reduce noise
            if "getaddrinfo failed" in str(e) or "Failed to resolve" in str(e):
                logger.debug(f"DNS resolution failed for ETag check: {url}")
            else:
                logger.warning(f"Failed to check ETag: {e}")
            return True  # On error, assume changed
    
    def _check_record_count(self, data_source: Dict, previous_cache: Dict, current_cache: Dict) -> bool:
        """Check if record count has changed significantly"""
        # This would require a sample query to the data source
        # For now, implement basic logic - can be enhanced per data source
        threshold = self.methods["record_count"].get("threshold_percentage", 5)
        
        # Sample implementation - would need to be customized per data source type
        current_count = self._get_approximate_record_count(data_source)
        if current_count is None:
            return True
        
        current_cache["record_count"] = current_count
        previous_count = previous_cache.get("record_count")
        
        if previous_count is None:
            return True
        
        # Calculate percentage change
        if previous_count == 0:
            return current_count > 0
        
        percentage_change = abs((current_count - previous_count) / previous_count) * 100
        return percentage_change > threshold
    
    def _check_last_modified(self, data_source: Dict, previous_cache: Dict, current_cache: Dict) -> bool:
        """Check if last modified timestamp has changed"""
        url = data_source.get("url")
        if not url:
            return True
        
        headers = data_source.get("headers", {})
        
        try:
            response = requests.head(url, headers=headers, timeout=5)
            last_modified = response.headers.get("Last-Modified")
            
            if last_modified:
                current_cache["last_modified"] = last_modified
                previous_last_modified = previous_cache.get("last_modified")
                return last_modified != previous_last_modified
            else:
                return True  # No last-modified header, assume changed
                
        except (requests.RequestException, requests.exceptions.Timeout) as e:
            # Only log DNS errors at debug level to reduce noise
            if "getaddrinfo failed" in str(e) or "Failed to resolve" in str(e):
                logger.debug(f"DNS resolution failed for last-modified check: {url}")
            else:
                logger.warning(f"Failed to check last-modified: {e}")
            return True
    
    def _get_approximate_record_count(self, data_source: Dict) -> Optional[int]:
        """Get approximate record count from data source"""
        # This is a placeholder - would need to be implemented per data source type
        # For APIs, could make a count query
        # For files, could sample or use metadata
        return None
    
    def _calculate_file_hash(self, file_path: str, algorithm: str) -> str:
        """Calculate hash of a file"""
        hash_obj = hashlib.new(algorithm)
        
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_obj.update(chunk)
        
        return hash_obj.hexdigest()
    
    def _load_cache(self, cache_file: Path) -> Dict:
        """Load cache from file"""
        try:
            if cache_file.exists():
                with open(cache_file, 'r') as f:
                    return json.load(f)
        except Exception as e:
            logger.warning(f"Failed to load cache file {cache_file}: {e}")
        
        return {}
    
    def _save_cache(self, cache_file: Path, cache_data: Dict):
        """Save cache to file"""
        try:
            with open(cache_file, 'w') as f:
                json.dump(cache_data, f, indent=2, default=str)
        except Exception as e:
            logger.error(f"Failed to save cache file {cache_file}: {e}")
    
    def get_cache_summary(self) -> Dict:
        """Get summary of all cached change detection data"""
        cache_files = list(self.cache_directory.glob("*_change_cache.json"))
        
        summary = {
            "total_components": len(cache_files),
            "components": {}
        }
        
        for cache_file in cache_files:
            component_name = cache_file.stem.replace("_change_cache", "")
            cache_data = self._load_cache(cache_file)
            
            summary["components"][component_name] = {
                "last_check": cache_data.get("last_check"),
                "methods_cached": [key for key in cache_data.keys() if key != "last_check"]
            }
        
        return summary
    
    def clear_cache(self, component_name: Optional[str] = None):
        """Clear change detection cache"""
        if component_name:
            cache_file = self.cache_directory / f"{component_name}_change_cache.json"
            if cache_file.exists():
                cache_file.unlink()
                logger.info(f"🗑️ Cleared change detection cache for {component_name}")
        else:
            # Clear all cache files
            for cache_file in self.cache_directory.glob("*_change_cache.json"):
                cache_file.unlink()
            logger.info("🗑️ Cleared all change detection cache")


class DataQualityValidator:
    """
    Data quality validation system that checks ingested data against quality rules
    """
    
    def __init__(self, config: Dict):
        """
        Initialize data quality validator
        
        Args:
            config: Data quality configuration
        """
        self.config = config
        self.validation_rules = config.get("validation_rules", {})
        self.quarantine_enabled = config.get("quarantine_failed_data", False)
        
        logger.info("✅ Data quality validator initialized")
    
    def validate_data(self, data: Any, component_name: str) -> Dict[str, Any]:
        """
        Validate data against quality rules
        
        Args:
            data: Data to validate (DataFrame, dict, list, etc.)
            component_name: Name of the component that produced the data
            
        Returns:
            Validation results dictionary
        """
        validation_result = {
            "component": component_name,
            "timestamp": datetime.now().isoformat(),
            "passed": True,
            "checks": {},
            "issues": [],
            "metrics": {}
        }
        
        try:
            # Convert to DataFrame if possible for consistent validation
            if isinstance(data, pd.DataFrame):
                df = data
            elif isinstance(data, list) and data:
                df = pd.DataFrame(data)
            elif isinstance(data, dict):
                df = pd.DataFrame([data])
            else:
                logger.warning(f"Cannot validate data type {type(data)} for {component_name}")
                validation_result["passed"] = False
                validation_result["issues"].append(f"Unsupported data type: {type(data)}")
                return validation_result
            
            # Record count validation
            record_count = len(df)
            validation_result["metrics"]["record_count"] = record_count
            
            min_records = self.validation_rules.get("record_count_min", 1)
            if record_count < min_records:
                validation_result["passed"] = False
                validation_result["issues"].append(f"Record count {record_count} below minimum {min_records}")
            
            validation_result["checks"]["record_count"] = record_count >= min_records
            
            # Null percentage validation
            if not df.empty:
                null_percentage = (df.isnull().sum().sum() / (len(df) * len(df.columns))) * 100
                validation_result["metrics"]["null_percentage"] = null_percentage
                
                max_null_percentage = self.validation_rules.get("null_percentage_max", 10)
                if null_percentage > max_null_percentage:
                    validation_result["passed"] = False
                    validation_result["issues"].append(f"Null percentage {null_percentage:.1f}% exceeds maximum {max_null_percentage}%")
                
                validation_result["checks"]["null_percentage"] = null_percentage <= max_null_percentage
            
            # Duplicate percentage validation
            if not df.empty:
                duplicate_count = df.duplicated().sum()
                duplicate_percentage = (duplicate_count / len(df)) * 100
                validation_result["metrics"]["duplicate_percentage"] = duplicate_percentage
                
                max_duplicate_percentage = self.validation_rules.get("duplicate_percentage_max", 5)
                if duplicate_percentage > max_duplicate_percentage:
                    validation_result["passed"] = False
                    validation_result["issues"].append(f"Duplicate percentage {duplicate_percentage:.1f}% exceeds maximum {max_duplicate_percentage}%")
                
                validation_result["checks"]["duplicate_percentage"] = duplicate_percentage <= max_duplicate_percentage
            
            # Schema validation (basic)
            if self.validation_rules.get("schema_validation", False):
                schema_issues = self._validate_schema(df, component_name)
                if schema_issues:
                    validation_result["passed"] = False
                    validation_result["issues"].extend(schema_issues)
                
                validation_result["checks"]["schema_validation"] = len(schema_issues) == 0
            
        except Exception as e:
            logger.error(f"Data quality validation failed for {component_name}: {e}")
            validation_result["passed"] = False
            validation_result["issues"].append(f"Validation error: {str(e)}")
        
        logger.info(f"✅ Data quality validation for {component_name}: {'PASSED' if validation_result['passed'] else 'FAILED'}")
        return validation_result
    
    def _validate_schema(self, df: pd.DataFrame, component_name: str) -> List[str]:
        """Validate data schema"""
        issues = []
        
        # Basic schema validation - can be enhanced with specific schemas per component
        if df.empty:
            issues.append("Dataset is empty")
        
        # Check for completely null columns
        null_columns = df.columns[df.isnull().all()].tolist()
        if null_columns:
            issues.append(f"Columns with all null values: {null_columns}")
        
        return issues
