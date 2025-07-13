"""
Routes Transformation Component
Transforms PTV routes data into standardized format
"""
import pandas as pd
from .base_transformation import BaseTransformation


class RoutesTransformation(BaseTransformation):
    """Transform PTV routes data into standardized format"""
    
    def transform(self, latest_raw_run):
        """Transform routes data"""
        self.logger.info("Transforming PTV routes data...")
        
        # Try both old and new naming patterns
        raw_df = self.load_raw_parquet(latest_raw_run, "ptv_routes_expanded")
        if raw_df is None:
            raw_df = self.load_raw_parquet(latest_raw_run, "ptv_routes")
        
        if raw_df is None:
            self.logger.warning("No routes data found with either naming pattern")
            return None
        
        # Extract routes from the nested structure
        routes_list = []
        for _, row in raw_df.iterrows():
            if 'routes' in row:
                routes_data = row['routes']
                # Handle both list and numpy array
                if hasattr(routes_data, '__iter__') and len(routes_data) > 0:
                    routes_list.extend(list(routes_data))
        
        if not routes_list:
            self.logger.warning("No routes data found in raw file")
            return None
        
        routes_df = pd.DataFrame(routes_list)
        
        # Standardize route data with explicit data types
        transformed_routes = pd.DataFrame({
            'route_id': self.ensure_int_type(routes_df['route_id']),
            'route_name': self.ensure_string_type(routes_df['route_name']),
            'route_number': self.ensure_string_type(routes_df.get('route_number', '')),
            'route_gtfs_id': self.ensure_string_type(routes_df.get('route_gtfs_id', '')),
            'route_type': self.ensure_int_type(routes_df['route_type']),
            'route_type_name': routes_df['route_type'].map(self.get_route_type_mapping()).astype('string'),
            'service_status': routes_df.apply(self._extract_service_status, axis=1).astype('string'),
            'service_status_timestamp': routes_df.apply(self._extract_service_timestamp, axis=1).astype('string'),
            'disruption_severity': routes_df.apply(self._calculate_route_disruption_severity, axis=1).astype('Int64'),
        })
        
        # Add route classification
        transformed_routes['route_classification'] = transformed_routes.apply(
            self._classify_route_importance, axis=1
        ).astype('string')
        
        # Add transformation metadata
        transformed_routes = self.add_transformation_metadata(transformed_routes)
        
        self.logger.info(f"Transformed {len(transformed_routes)} routes")
        return transformed_routes
    
    def get_output_filename(self):
        return "routes_curated"
    
    def _extract_service_status(self, row):
        """Extract service status from route data"""
        if 'route_service_status' in row and isinstance(row['route_service_status'], dict):
            return row['route_service_status'].get('description', 'Unknown')
        return 'Unknown'
    
    def _extract_service_timestamp(self, row):
        """Extract service status timestamp"""
        if 'route_service_status' in row and isinstance(row['route_service_status'], dict):
            return row['route_service_status'].get('timestamp', '')
        return ''
    
    def _calculate_route_disruption_severity(self, row):
        """Calculate disruption severity for a route"""
        status = self._extract_service_status(row)
        severity_map = {
            'Good Service': 1,
            'Minor Delays': 2,
            'Major Delays': 4,
            'Planned Works': 3,
            'Suspended': 5,
            'Unknown': 2
        }
        return severity_map.get(status, 2)
    
    def _classify_route_importance(self, row):
        """Classify route importance based on type and usage patterns"""
        if row['route_type'] == 0:  # Train
            return 'High'
        elif row['route_type'] == 1:  # Tram
            return 'Medium'
        else:  # Bus
            return 'Medium'
