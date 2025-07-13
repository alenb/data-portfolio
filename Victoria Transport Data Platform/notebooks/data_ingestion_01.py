"""
Real Data Ingestion Pipeline
Collects transport resilience data from validated APIs and stores in raw data layer
"""

import logging
from datetime import datetime
from pathlib import Path
import sys
import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add parent directory to path for config import
sys.path.append(str(Path(__file__).parent.parent))

from config.config import Config
from ingestion import PTVIngestionComponent, DataVicIngestionComponent
from ingestion.gtfs_schedule_ingestion import GTFSScheduleIngestionComponent
from ingestion.gtfs_realtime_ingestion import GTFSRealtimeIngestionComponent
from ingestion.datavic_station_patronage import DataVicStationPatronageComponent
from ingestion.datavic_temporal_patronage import DataVicTemporalPatronageComponent
from orchestration import DependencyManager

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TransportDataIngestionPipeline:
    """
    Real data ingestion pipeline for transport resilience intelligence.
    
    This pipeline collects data from:
    - GTFS Schedule data (with REAL platform numbers!)
    - PTV Timetable API (for real-time disruptions)
    - Data.vic.gov.au API (transport datasets)
    
    Uses modular components for maintainability.
    """
    
    def __init__(self, config_path=None):
        """Initialize pipeline with API credentials from Config"""
        self.config = Config
        self.config.ensure_directories()
        self.run_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.setup_directories()
        logger.info(f"Initialized ingestion pipeline - Run: {self.run_timestamp}")
        
        # Load orchestration configuration first
        self.orchestration_config = self.load_orchestration_config()
        
        # Initialize modular components with orchestration config
        self.gtfs_schedule_component = GTFSScheduleIngestionComponent(self.config, self.run_timestamp, self.run_data_path, self.orchestration_config)
        self.gtfs_realtime_component = GTFSRealtimeIngestionComponent(self.config, self.run_timestamp, self.run_data_path, self.orchestration_config)
        self.ptv_component = PTVIngestionComponent(self.config, self.run_timestamp, self.run_data_path, self.orchestration_config)
        self.datavic_component = DataVicIngestionComponent(self.config, self.run_timestamp, self.run_data_path, self.orchestration_config)
        
        # Phase 1: Add new Data.vic patronage components
        self.datavic_station_patronage_component = DataVicStationPatronageComponent(self.config, self.run_timestamp, self.run_data_path, self.orchestration_config)
        self.datavic_temporal_patronage_component = DataVicTemporalPatronageComponent(self.config, self.run_timestamp, self.run_data_path, self.orchestration_config)
        
        # Phase 2: Initialize dependency manager with app config
        self.dependency_manager = DependencyManager(self.orchestration_config, self.run_data_path, self.config)
        
        # Validate configuration
        try:
            self.config.validate_config()
            logger.info("Configuration validation successful")
        except ValueError as e:
            logger.warning(f"Configuration validation failed: {e}")
        
    def setup_directories(self):
        """Create data directory structure optimized for multi-day historical data collection"""
        self.base_path = self.config.RAW_DATA_PATH
        self.base_path.mkdir(parents=True, exist_ok=True)
        
        # Create date-partitioned directories for current ingestion run
        current_time = datetime.now()
        self.date_partition = f"year={current_time.year}/month={current_time.month:02d}/day={current_time.day:02d}"
        self.data_path = self.base_path / self.date_partition
        self.data_path.mkdir(parents=True, exist_ok=True)
        
        # Create subdirectory for this specific ingestion run
        self.run_data_path = self.data_path / f"run_{self.run_timestamp}"
        self.run_data_path.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"📁 Multi-day historical data will be saved to: {self.run_data_path}")
        logger.info(f"📊 Collecting comprehensive transport intelligence...")
    
    def ingest_gtfs_schedule_data(self):
        """Ingest GTFS Schedule data with REAL platform numbers using new dedicated component"""
        logger.info("🚂 Starting GTFS Schedule data ingestion...")
        result = self.gtfs_schedule_component.ingest_data()
        return result.get('datasets_ingested', [])
    
    def ingest_gtfs_realtime_data(self):
        """Ingest GTFS Realtime data using new dedicated component"""
        logger.info("� Starting GTFS Realtime data ingestion...")
        result = self.gtfs_realtime_component.ingest_data()
        return result.get('datasets_ingested', [])
    
    def ingest_legacy_gtfs_data(self):
        """Ingest legacy GTFS data using schedule component (for backward compatibility)"""
        logger.info("🚂 Starting legacy GTFS data ingestion...")
        result = self.gtfs_schedule_component.ingest_data()
        return result.get('datasets_ingested', [])
    
    def ingest_ptv_core_data(self):
        """Ingest PTV data for real-time disruptions using modular component"""
        logger.info("🚨 Starting PTV core data ingestion...")
        result = self.ptv_component.ingest_data()
        return result.get('datasets_ingested', [])
    
    def ingest_datavic_transport_datasets(self):
        """Ingest Data.vic transport dataset metadata using modular component"""
        logger.info("📊 Starting Data.vic transport dataset metadata ingestion...")
        result = self.datavic_component.ingest_data()
        return result.get('datasets_ingested', [])
    
    def ingest_datavic_station_patronage(self):
        """Ingest Data.vic annual station patronage data"""
        logger.info("🚉 Starting Data.vic station patronage ingestion...")
        result = self.datavic_station_patronage_component.ingest_data()
        return result.get('datasets_ingested', [])
    
    def ingest_datavic_temporal_patronage(self):
        """Ingest Data.vic monthly patronage data"""
        logger.info("📈 Starting Data.vic temporal patronage ingestion...")
        result = self.datavic_temporal_patronage_component.ingest_data()
        return result.get('datasets_ingested', [])
    
    def load_orchestration_config(self):
        """Load orchestration configuration with fallback to defaults"""
        config_path = Path(__file__).parent.parent / "config" / "orchestration.json"
        
        try:
            with open(config_path, 'r') as f:
                config = json.load(f)
                logger.info("✅ Loaded orchestration configuration")
                return config
        except Exception as e:
            logger.warning(f"Could not load orchestration config: {e}")
            # Return default configuration
            return {
                "components": {
                    "gtfs_schedule": {"enabled": True, "frequency": "weekly"},
                    "gtfs_realtime": {"enabled": True, "frequency": "daily"},
                    "ptv_timetable": {"enabled": True, "frequency": "daily"},
                    "datavic_discovery": {"enabled": True, "frequency": "weekly"},
                    "datavic_station_patronage": {"enabled": True, "frequency": "monthly"},
                    "datavic_temporal_patronage": {"enabled": True, "frequency": "monthly"}
                },
                "override_settings": {
                    "force_run_all": False,
                    "skip_frequency_check": False
                }
            }
    
    def run_component(self, component_name):
        """Run a single component with Phase 3 enhancements: performance monitoring, data quality validation, and timeout monitoring"""
        # Mapping of component names to their execution methods
        component_methods = {
            "gtfs_schedule": self.ingest_gtfs_schedule_data,
            "gtfs_realtime": self.ingest_gtfs_realtime_data,
            "ptv_timetable": self.ingest_ptv_core_data,
            "datavic_discovery": self.ingest_datavic_transport_datasets,
            "datavic_station_patronage": self.ingest_datavic_station_patronage,
            "datavic_temporal_patronage": self.ingest_datavic_temporal_patronage
        }
        
        if component_name not in component_methods:
            logger.error(f"Unknown component: {component_name}")
            return []
        
        # Phase 3: Start performance monitoring
        self.dependency_manager.start_performance_monitoring(component_name)
        
        # Record component start
        self.dependency_manager.record_component_start(component_name)
        
        try:
            # Execute the component with timeout monitoring
            datasets = self._execute_component_with_timeout(component_name, component_methods[component_name])
            
            # Phase 3: Validate data quality
            component_obj = self._get_component_object(component_name)
            if component_obj and hasattr(component_obj, 'datasets_ingested'):
                # Get the last ingested data for quality validation
                last_data = getattr(component_obj, 'last_ingested_data', None)
                if last_data:
                    quality_result = self.dependency_manager.validate_data_quality(component_name, last_data)
                    if not quality_result["passed"]:
                        logger.warning(f"⚠️ Data quality issues detected for {component_name}: {quality_result['issues']}")
            
            # Record successful completion
            result = {
                "datasets_ingested": datasets,
                "total_datasets": len(datasets),
                "errors": []
            }
            
            # Get component errors if available
            if component_obj and hasattr(component_obj, 'errors'):
                result["errors"] = component_obj.errors
            
            # Phase 3: Stop performance monitoring
            self.dependency_manager.stop_performance_monitoring(component_name, result)
            
            self.dependency_manager.record_component_completion(component_name, result)
            return datasets
            
        except Exception as e:
            error_msg = f"Component {component_name} failed: {str(e)}"
            logger.error(error_msg)
            
            # Phase 3: Stop performance monitoring on error
            error_result = {
                "datasets_ingested": [],
                "total_datasets": 0,
                "errors": [error_msg]
            }
            self.dependency_manager.stop_performance_monitoring(component_name, error_result)
            
            self.dependency_manager.record_component_failure(component_name, error_msg)
            return []
    
    def _execute_component_with_timeout(self, component_name: str, method):
        """Execute a component method with timeout monitoring"""
        # Check global timeout before starting
        if self.dependency_manager.check_global_timeout():
            raise Exception("Global execution timeout exceeded")
        
        # Use threading to monitor for timeout during execution
        result = []
        exception_holder = []
        
        def target():
            try:
                result.extend(method())
            except Exception as e:
                exception_holder.append(e)
        
        thread = threading.Thread(target=target)
        thread.daemon = True
        thread.start()
        
        # Monitor for timeout while thread is running
        while thread.is_alive():
            # Check component timeout
            if self.dependency_manager.check_component_timeout(component_name):
                logger.error(f"❌ Component {component_name} timed out, attempting to stop...")
                # In a production system, you might want to implement proper thread interruption
                raise Exception(f"Component {component_name} execution timeout")
            
            # Check global timeout
            if self.dependency_manager.check_global_timeout():
                logger.error(f"❌ Global timeout exceeded while running {component_name}")
                raise Exception("Global execution timeout exceeded")
            
            time.sleep(1)  # Check every second
        
        # Join the thread to ensure it's completed
        thread.join()
        
        # Check if there was an exception in the thread
        if exception_holder:
            raise exception_holder[0]
        
        return result
    
    def _get_component_object(self, component_name):
        """Get the component object by name"""
        component_mapping = {
            "gtfs_schedule": self.gtfs_schedule_component,
            "gtfs_realtime": self.gtfs_realtime_component,
            "ptv_timetable": self.ptv_component,
            "datavic_discovery": self.datavic_component,
            "datavic_station_patronage": self.datavic_station_patronage_component,
            "datavic_temporal_patronage": self.datavic_temporal_patronage_component
        }
        return component_mapping.get(component_name)
    
    def run_full_ingestion(self):
        """Execute complete data ingestion pipeline using dependency-aware orchestration with concurrency support"""
        logger.info(f"🚀 Starting dependency-aware ingestion pipeline - Run: {self.run_timestamp}")
        
        ingestion_summary = {
            "run_timestamp": self.run_timestamp,
            "start_time": datetime.now().isoformat(),
            "status": "started",
            "datasets_ingested": [],
            "errors": []
        }
        
        try:
            # Start global timeout
            self.dependency_manager.start_global_timeout()
            
            # Phase 2: Get optimal execution order based on dependencies
            execution_order = self.dependency_manager.get_execution_order()
            logger.info(f"🔄 Execution order: {' → '.join(execution_order)}")
            
            # Check if parallel execution is supported
            max_concurrent = self.orchestration_config.get("runtime_settings", {}).get("max_concurrent_components", 1)
            parallel_groups = self.orchestration_config.get("dependency_management", {}).get("parallel_groups", [])
            
            if max_concurrent > 1 and parallel_groups:
                # Execute with parallelization
                self._execute_with_parallelization(execution_order, parallel_groups, max_concurrent, ingestion_summary)
            else:
                # Execute sequentially (original behavior)
                self._execute_sequentially(execution_order, ingestion_summary)
            
            # Finalize execution tracking
            self.dependency_manager.finalize_execution()
            
            # Get execution summary from dependency manager
            execution_summary = self.dependency_manager.get_execution_summary()
            
            # Update ingestion summary
            ingestion_summary["status"] = "completed" if not execution_summary["failed_components"] else "partial"
            ingestion_summary["total_datasets"] = len(ingestion_summary["datasets_ingested"])
            ingestion_summary["end_time"] = datetime.now().isoformat()
            ingestion_summary["execution_summary"] = execution_summary
            
            # Collect any errors from components
            for component_name in execution_summary["successful_components"]:
                component_obj = self._get_component_object(component_name)
                if component_obj and hasattr(component_obj, 'errors') and component_obj.errors:
                    ingestion_summary["errors"].extend(component_obj.errors)
            
        except Exception as e:
            logger.error(f"Ingestion pipeline failed: {e}")
            ingestion_summary["status"] = "failed"
            ingestion_summary["error"] = str(e)
            ingestion_summary["errors"].append(str(e))
            
            # Still try to finalize dependency manager
            try:
                self.dependency_manager.finalize_execution()
            except Exception as dm_error:
                logger.error(f"Failed to finalize dependency manager: {dm_error}")
        
        # Save ingestion summary using first component's save method
        self.gtfs_schedule_component.save_as_parquet(ingestion_summary, "ingestion_summary")
        
        # Print summary
        self._print_summary(ingestion_summary)
        
        return ingestion_summary
    
    def _execute_sequentially(self, execution_order, ingestion_summary):
        """Execute components sequentially with timeout monitoring"""
        for i, component_name in enumerate(execution_order):
            logger.info(f"📋 Processing {component_name}...")
            
            # Check global timeout
            if self.dependency_manager.check_global_timeout():
                logger.error("❌ Global timeout exceeded, stopping execution")
                break
            
            # Check if component should run
            if self.dependency_manager.should_run_component(component_name):
                # Start component timeout
                self.dependency_manager.start_component_timeout(component_name)
                
                # Run the component
                datasets = self.run_component(component_name)
                ingestion_summary["datasets_ingested"].extend(datasets)
                
                # Stop component timeout
                self.dependency_manager.stop_component_timeout(component_name)
                
                # Add delay between components (except for the last one)
                if i < len(execution_order) - 1:
                    delay_seconds = self.orchestration_config.get("rate_limiting", {}).get("global_delays", {}).get("between_components", 2)
                    logger.info(f"⏱️ Waiting {delay_seconds} seconds before next component...")
                    time.sleep(delay_seconds)
            else:
                logger.info(f"⏭️ Skipped {component_name}")
    
    def _execute_with_parallelization(self, execution_order, parallel_groups, max_concurrent, ingestion_summary):
        """Execute components with parallel execution support"""
        logger.info(f"🚀 Parallel execution enabled with max_concurrent={max_concurrent}")
        
        # Create a set of all components that can run in parallel
        parallel_components = set()
        for group in parallel_groups:
            parallel_components.update(group)
        
        # Process components
        processed_components = set()
        
        for component_name in execution_order:
            if component_name in processed_components:
                continue
                
            # Check if this component can run in parallel
            current_group = None
            for group in parallel_groups:
                if component_name in group:
                    current_group = group
                    break
            
            if current_group:
                # Execute parallel group
                self._execute_parallel_group(current_group, max_concurrent, ingestion_summary)
                processed_components.update(current_group)
            else:
                # Execute single component
                logger.info(f"📋 Processing {component_name}...")
                
                # Check global timeout
                if self.dependency_manager.check_global_timeout():
                    logger.error("❌ Global timeout exceeded, stopping execution")
                    break
                
                if self.dependency_manager.should_run_component(component_name):
                    self.dependency_manager.start_component_timeout(component_name)
                    datasets = self.run_component(component_name)
                    ingestion_summary["datasets_ingested"].extend(datasets)
                    self.dependency_manager.stop_component_timeout(component_name)
                else:
                    logger.info(f"⏭️ Skipped {component_name}")
                
                processed_components.add(component_name)
    
    def _execute_parallel_group(self, group, max_concurrent, ingestion_summary):
        """Execute a group of components in parallel"""
        logger.info(f"🚀 Executing parallel group: {group}")
        
        # Filter components that should run
        components_to_run = [comp for comp in group if self.dependency_manager.should_run_component(comp)]
        
        if not components_to_run:
            logger.info("⏭️ No components in parallel group need to run")
            return
        
        # Use ThreadPoolExecutor for parallel execution
        with ThreadPoolExecutor(max_workers=min(max_concurrent, len(components_to_run))) as executor:
            # Submit all tasks
            future_to_component = {}
            for component_name in components_to_run:
                self.dependency_manager.start_component_timeout(component_name)
                future = executor.submit(self.run_component, component_name)
                future_to_component[future] = component_name
            
            # Collect results
            for future in as_completed(future_to_component):
                component_name = future_to_component[future]
                try:
                    datasets = future.result()
                    ingestion_summary["datasets_ingested"].extend(datasets)
                    self.dependency_manager.stop_component_timeout(component_name)
                    logger.info(f"✅ Parallel component {component_name} completed")
                except Exception as e:
                    logger.error(f"❌ Parallel component {component_name} failed: {e}")
                    self.dependency_manager.stop_component_timeout(component_name)
    
    def _print_summary(self, ingestion_summary):
        """Print comprehensive ingestion summary with dependency management details"""
        exec_summary = ingestion_summary.get("execution_summary", {})
        
        print("=" * 80)
        print("🎯 TRANSPORT DATA INGESTION COMPLETE")
        print("=" * 80)
        print(f"📅 Run Timestamp: {self.run_timestamp}")
        print(f"📊 Total Datasets Collected: {ingestion_summary.get('total_datasets', 0)}")
        print(f"📁 Data Location: {self.run_data_path}")
        print(f"✅ Status: {ingestion_summary['status'].upper()}")
        
        # Show execution order and dependency management
        if exec_summary:
            print(f"\n� DEPENDENCY MANAGEMENT:")
            execution_order = exec_summary.get("execution_order", [])
            if execution_order:
                print(f"   📋 Execution Order: {' → '.join(execution_order)}")
            
            successful = exec_summary.get("successful_components", [])
            if successful:
                print(f"   ✅ Successful Components: {', '.join(successful)}")
            
            skipped = exec_summary.get("skipped_components", [])
            if skipped:
                print(f"   ⏭️ Skipped Components: {', '.join(skipped)}")
            
            failed = exec_summary.get("failed_components", [])
            if failed:
                print(f"   ❌ Failed Components: {', '.join(failed)}")
        
        if ingestion_summary["datasets_ingested"]:
            total_datasets = len(ingestion_summary["datasets_ingested"])
            print(f"\n📋 Successfully Collected {total_datasets} Datasets:")
            for i, dataset in enumerate(ingestion_summary["datasets_ingested"][:12]):
                print(f"   • {dataset}")
            if total_datasets > 12:
                print(f"   • ... and {total_datasets - 12} more datasets")
        
        if ingestion_summary.get("errors"):
            print("\n⚠️ Errors Encountered:")
            for error in ingestion_summary["errors"]:
                print(f"   • {error}")


def main():
    """Main execution function"""
    print("🚀 Starting Transport Resilience Data Ingestion Pipeline")
    print("🧠 Using AI-Powered Orchestration Intelligence")
    print("📦 GTFS Schedule + PTV API + Data.vic Metadata + Patronage Data")
    print("🔍 Change Detection + Performance Monitoring + Data Quality Validation")
    print("⚡ Adaptive Scheduling + Smart Dependencies + Failure Recovery")
    print()
    
    # Initialize and run pipeline
    pipeline = TransportDataIngestionPipeline()
    summary = pipeline.run_full_ingestion()
    
    return summary


if __name__ == "__main__":
    main()
