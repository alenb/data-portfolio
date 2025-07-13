"""
DRI (Disruption Risk Index) Calculation Component
Calculates disruption risk index for routes and stations
"""
import pandas as pd
from datetime import datetime
from .base_transformation import BaseTransformation


class DRICalculation(BaseTransformation):
    """Calculate Disruption Risk Index (DRI) for each route/station"""
    
    def __init__(self, config, run_timestamp):
        super().__init__(config, run_timestamp)
        self.delay_threshold_minutes = getattr(config, 'DELAY_THRESHOLD_MINUTES', 15)
    
    def transform(self, latest_raw_run, routes_df=None, disruptions_df=None, departures_df=None):
        """Calculate DRI using transformed data"""
        self.logger.info("Calculating Disruption Risk Index...")
        
        if routes_df is None or routes_df.empty:
            self.logger.warning("No routes data available for DRI calculation")
            return None
        
        # Initialize DRI components
        dri_components = []
        
        for _, route in routes_df.iterrows():
            route_id = route['route_id']
            
            # Component 1: Service Status Score (0-1)
            service_score = self._calculate_service_status_score(route['service_status'])
            
            # Component 2: Disruption Count Score (0-1)
            disruption_score = self._calculate_disruption_score(route_id, disruptions_df)
            
            # Component 3: Delay Performance Score (0-1)
            delay_score = self._calculate_delay_score(route_id, departures_df)
            
            # Component 4: Route Importance Weight (metro lines = higher weight)
            importance_weight = 1.5 if route['route_type'] == 0 else 1.0
            
            # Calculate weighted DRI (0-100 scale)
            raw_dri = (service_score * 0.4 + disruption_score * 0.3 + delay_score * 0.3)
            weighted_dri = min(raw_dri * importance_weight, 1.0) * 100
            
            dri_components.append({
                'route_id': int(route_id),
                'route_name': str(route['route_name']),
                'route_type': int(route['route_type']),
                'route_type_name': str(route['route_type_name']),
                'service_status_score': float(service_score),
                'disruption_count_score': float(disruption_score),
                'delay_performance_score': float(delay_score),
                'importance_weight': float(importance_weight),
                'disruption_risk_index': float(round(weighted_dri, 2)),
                'risk_category': str(self._categorize_risk_level(weighted_dri)),
                'calculation_timestamp': str(self.run_timestamp),
                'calculation_date': str(datetime.now().date())
            })
        
        dri_df = pd.DataFrame(dri_components)
        self.logger.info(f"Calculated DRI for {len(dri_df)} routes")
        return dri_df
    
    def get_output_filename(self):
        return "disruption_risk_index"
    
    def _calculate_service_status_score(self, status):
        """Convert service status to numeric score (0-1, higher = worse)"""
        status_scores = {
            'Good Service': 0.1,
            'Minor Delays': 0.3,
            'Major Delays': 0.8,
            'Planned Works': 0.5,
            'Suspended': 1.0,
            'Unknown': 0.4
        }
        return status_scores.get(status, 0.4)
    
    def _calculate_disruption_score(self, route_id, disruptions_df):
        """Calculate disruption score for a route"""
        disruption_score = 0
        if disruptions_df is not None and not disruptions_df.empty:
            route_disruptions = disruptions_df[
                disruptions_df['affected_routes'].astype(str).str.contains(str(route_id), na=False)
            ]
            disruption_score = min(len(route_disruptions) * 0.2, 1.0)
        return disruption_score
    
    def _calculate_delay_score(self, route_id, departures_df):
        """Calculate delay score for a route"""
        delay_score = 0
        if departures_df is not None and not departures_df.empty:
            route_departures = departures_df[departures_df['route_id'] == route_id]
            if not route_departures.empty:
                avg_delay = route_departures['delay_minutes'].mean()
                delay_score = min(avg_delay / self.delay_threshold_minutes, 1.0)
        return delay_score
    
    def _categorize_risk_level(self, dri_score):
        """Categorize DRI score into risk levels"""
        if dri_score <= 20:
            return 'Low Risk'
        elif dri_score <= 40:
            return 'Moderate Risk'
        elif dri_score <= 70:
            return 'High Risk'
        else:
            return 'Critical Risk'
