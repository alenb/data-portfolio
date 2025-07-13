"""
Disruptions Transformation Component
Transforms PTV disruptions data into standardized format
"""
import pandas as pd
from .base_transformation import BaseTransformation


class DisruptionsTransformation(BaseTransformation):
    """Transform PTV disruptions data into standardized format"""
    
    def transform(self, latest_raw_run):
        """Transform disruptions data"""
        self.logger.info("Transforming PTV disruptions data...")
        
        # Try both old and new naming patterns
        raw_df = self.load_raw_parquet(latest_raw_run, "ptv_disruptions_expanded")
        if raw_df is None:
            raw_df = self.load_raw_parquet(latest_raw_run, "ptv_disruptions")
        
        if raw_df is None:
            self.logger.warning("No disruptions data found with either naming pattern")
            return None
        
        # Extract disruptions from nested structure
        disruptions_list = []
        for _, row in raw_df.iterrows():
            if 'disruptions' in row and isinstance(row['disruptions'], dict):
                # Flatten different disruption types
                for disruption_type, disruption_data in row['disruptions'].items():
                    if isinstance(disruption_data, list):
                        for disruption in disruption_data:
                            disruption['disruption_type'] = disruption_type
                            disruptions_list.append(disruption)
        
        if not disruptions_list:
            self.logger.warning("No disruptions found - this is actually good news!")
            # Create empty DataFrame with expected structure
            return pd.DataFrame(columns=[
                'disruption_id', 'disruption_type', 'title', 'description',
                'disruption_status', 'severity_score', 'affected_routes',
                'transformation_timestamp', 'transformation_date'
            ])
        
        disruptions_df = pd.DataFrame(disruptions_list)
        
        # Standardize disruption data with explicit types
        transformed_disruptions = pd.DataFrame({
            'disruption_id': self.ensure_int_type(
                disruptions_df.get('disruption_id', range(len(disruptions_df)))
            ),
            'disruption_type': self.ensure_string_type(disruptions_df['disruption_type']),
            'title': self.ensure_string_type(disruptions_df.get('title', 'Unknown')),
            'description': self.ensure_string_type(disruptions_df.get('description', '')),
            'disruption_status': self.ensure_string_type(disruptions_df.get('disruption_status', 'Unknown')),
            'severity_score': disruptions_df.apply(self._calculate_disruption_severity, axis=1).astype('Int64'),
            'affected_routes': disruptions_df.apply(self._extract_affected_routes, axis=1).astype('Int64'),
        })
        
        # Add transformation metadata
        transformed_disruptions = self.add_transformation_metadata(transformed_disruptions)
        
        self.logger.info(f"Transformed {len(transformed_disruptions)} disruptions")
        return transformed_disruptions
    
    def get_output_filename(self):
        return "disruptions_curated"
    
    def _calculate_disruption_severity(self, row):
        """Calculate numeric severity score for disruptions"""
        status = row.get('disruption_status', 'Unknown')
        severity_map = {
            'Current': 5,
            'Planned': 2,
            'Ended': 1,
            'Unknown': 3
        }
        return severity_map.get(status, 3)
    
    def _extract_affected_routes(self, row):
        """Extract affected route information from disruption"""
        if 'routes' in row and isinstance(row['routes'], list):
            return len(row['routes'])
        return 0
