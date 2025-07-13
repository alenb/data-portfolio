"""
Silver Layer Data Transformation
Transforms raw transport data into clean, standardized datasets for analytics
"""

import pandas as pd
from datetime import datetime
import sys
from pathlib import Path
import logging

# Add parent directory to path for config import
sys.path.append(str(Path(__file__).parent.parent))

from config.config import Config

# Import transformation components
from transformations import (
    RoutesTransformationComponent,
    DisruptionsTransformationComponent,
    DeparturesTransformationComponent,
    TrafficTransformationComponent,
    DRICalculationComponent
)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TransportDataTransformation:
    """
    Silver layer transformation pipeline for transport resilience intelligence.
    Uses modular transformation components for clean, maintainable code.
    """
    
    def __init__(self):
        """Initialize transformation pipeline with modular components"""
        self.config = Config
        self.config.ensure_directories()
        self.run_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Setup directories
        self.raw_path = self.config.RAW_DATA_PATH
        self.curated_path = self.config.CURATED_DATA_PATH
        self.curated_path.mkdir(parents=True, exist_ok=True)
        
        # Find latest raw data run
        self.latest_raw_run = self.find_latest_raw_run()
        if not self.latest_raw_run:
            raise ValueError("No raw data found to transform")
        
        # Initialize transformation components
        self.transformations = {
            'routes': RoutesTransformationComponent(self.config, self.run_timestamp),
            'disruptions': DisruptionsTransformationComponent(self.config, self.run_timestamp),
            'departures': DeparturesTransformationComponent(self.config, self.run_timestamp),
            'traffic': TrafficTransformationComponent(self.config, self.run_timestamp),
            'dri': DRICalculationComponent(self.config, self.run_timestamp)
        }
        
        logger.info(f"Initialized transformation pipeline - Run: {self.run_timestamp}")
        logger.info(f"Source data: {self.latest_raw_run}")
        logger.info(f"Target path: {self.curated_path}")
    
    def find_latest_raw_run(self):
        """Find the most recent raw data ingestion run"""
        # Look for date partitioned directories with run subdirectories
        run_dirs = list(self.raw_path.glob("year=*/month=*/day=*/run_*"))
        if not run_dirs:
            return None
        
        # Get the directory with the most recent data files
        latest_dir = None
        latest_time = 0
        
        for dir_path in run_dirs:
            parquet_files = list(dir_path.glob("*.parquet"))
            if parquet_files:
                # Get the newest file time in this directory
                newest_file_time = max(f.stat().st_mtime for f in parquet_files)
                if newest_file_time > latest_time:
                    latest_time = newest_file_time
                    latest_dir = dir_path
        
        return latest_dir
    
    def load_raw_parquet(self, pattern):
        """Load raw parquet files matching pattern"""
        files = list(self.latest_raw_run.glob(f"{pattern}*.parquet"))
        if not files:
            logger.warning(f"No files found matching pattern: {pattern}")
            return None
        
        # Load the most recent file
        latest_file = max(files, key=lambda x: x.stat().st_mtime)
        logger.info(f"Loading: {latest_file.name}")
        return pd.read_parquet(latest_file)
    
    def save_curated_data(self, df, filename):
        """Save transformed data to curated layer"""
        if df is None or df.empty:
            logger.warning(f"No data to save for {filename}")
            return None
        
        filepath = self.curated_path / f"{filename}_{self.run_timestamp}.parquet"
        df.to_parquet(filepath, index=False)
        logger.info(f"Saved {len(df)} records to {filepath}")
        return filepath
    
    def run_full_transformation(self):
        """Execute complete data transformation pipeline using modular components"""
        logger.info(f"🔄 Starting modular transformation pipeline - Run: {self.run_timestamp}")
        
        transformation_summary = {
            "run_timestamp": self.run_timestamp,
            "start_time": datetime.now().isoformat(),
            "status": "started",
            "datasets_transformed": [],
            "errors": []
        }
        
        # Store transformed data for DRI calculation
        routes_df = None
        disruptions_df = None
        departures_df = None
        
        try:
            # Transform each dataset using modular components
            for name, transformer in self.transformations.items():
                if name == 'dri':  # Skip DRI for now, do it last
                    continue
                    
                try:
                    logger.info(f"🔄 Running {name} transformation...")
                    
                    # All transformations now take the latest_raw_run directory path
                    # and handle their own data loading internally
                    transformed_data = transformer.transform(self.latest_raw_run)
                    
                    # Store specific transformations for DRI calculation
                    if name == 'routes':
                        routes_df = transformed_data
                    elif name == 'disruptions':
                        disruptions_df = transformed_data
                    elif name == 'departures':
                        departures_df = transformed_data
                    
                    # Save transformed data
                    if transformed_data is not None and not transformed_data.empty:
                        saved_path = transformer.save_data(transformed_data, self.curated_path)
                        transformation_summary["datasets_transformed"].append(f"{name}_curated")
                        logger.info(f"✅ {name} transformation completed")
                    else:
                        logger.warning(f"⚠️ {name} transformation returned no data")
                        
                except Exception as e:
                    logger.error(f"❌ {name} transformation failed: {e}")
                    transformation_summary["errors"].append(f"{name}: {str(e)}")
            
            # Calculate DRI using transformed data
            try:
                logger.info("🔄 Calculating Disruption Risk Index...")
                dri_transformer = self.transformations['dri']
                dri_data = dri_transformer.transform(
                    self.latest_raw_run, 
                    routes_df=routes_df, 
                    disruptions_df=disruptions_df, 
                    departures_df=departures_df
                )
                
                if dri_data is not None and not dri_data.empty:
                    saved_path = dri_transformer.save_data(dri_data, self.curated_path)
                    transformation_summary["datasets_transformed"].append("disruption_risk_index")
                    logger.info("✅ DRI calculation completed")
                else:
                    logger.warning("⚠️ DRI calculation returned no data")
                    
            except Exception as e:
                logger.error(f"❌ DRI calculation failed: {e}")
                transformation_summary["errors"].append(f"DRI: {str(e)}")

            transformation_summary["status"] = "completed"
            transformation_summary["total_datasets"] = len(transformation_summary["datasets_transformed"])
            transformation_summary["end_time"] = datetime.now().isoformat()
            
        except Exception as e:
            logger.error(f"Transformation pipeline failed: {e}")
            transformation_summary["status"] = "failed"
            transformation_summary["error"] = str(e)
            transformation_summary["errors"].append(str(e))
        
        # Save transformation summary
        summary_df = pd.DataFrame([transformation_summary])
        self.save_curated_data(summary_df, "transformation_summary")
        
        # Print summary
        print("=" * 60)
        print("🥈 MODULAR TRANSFORMATION PIPELINE COMPLETE")
        print("=" * 60)
        print(f"📅 Run Timestamp: {self.run_timestamp}")
        print(f"📊 Datasets Transformed: {transformation_summary.get('total_datasets', 0)}")
        print(f"📁 Curated Data Location: {self.curated_path}")
        print(f"✅ Status: {transformation_summary['status'].upper()}")
        
        if transformation_summary["datasets_transformed"]:
            print("\n📋 Successfully Transformed Datasets:")
            for dataset in transformation_summary["datasets_transformed"]:
                print(f"   • {dataset}")
        
        if transformation_summary.get("errors"):
            print("\n⚠️ Errors Encountered:")
            for error in transformation_summary["errors"]:
                print(f"   • {error}")
        
        print("=" * 60)
        print("🚀 MODULAR ARCHITECTURE BENEFITS:")
        print("   ✅ Each transformation runs independently")
        print("   ✅ Easy to test individual components")
        print("   ✅ Simple to add new data sources")
        print("   ✅ Clear separation of concerns")
        print("   ✅ Maintainable and scalable code")
        print("=" * 60)
        print("🔄 Next Step: Create Power BI Dashboard")
        print("📊 Key Analytics Ready:")
        print("   • Disruption Risk Index (DRI)")
        print("   • Route Performance Metrics")
        print("   • Station Delay Analytics")
        print("   • Service Status Trends")
        print("=" * 60)
        
        return transformation_summary


def main():
    """Main execution function"""
    print("🥈 Starting Transport Data Silver Layer Transformation")
    print("🔄 Converting Raw Data → Analytics-Ready Datasets")
    print("🏗️ Using Modular Component Architecture")
    print()
    
    # Initialize and run transformation
    transformer = TransportDataTransformation()
    summary = transformer.run_full_transformation()
    
    return summary


if __name__ == "__main__":
    main()
