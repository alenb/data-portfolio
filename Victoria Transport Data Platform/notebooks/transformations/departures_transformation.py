"""
Departures Transformation Component
Transforms GTFS departure data with REAL platform numbers
"""
import pandas as pd
from datetime import datetime
from .base_transformation import BaseTransformation


class DeparturesTransformation(BaseTransformation):
    """Transform GTFS departure data with REAL platform numbers"""
    
    def transform(self, latest_raw_run):
        """Transform GTFS departure data with REAL platform numbers"""
        self.logger.info("Transforming GTFS departure data with REAL platform numbers...")
        
        # Process GTFS departure data instead of PTV API data
        gtfs_departures_df = self.load_raw_parquet(latest_raw_run, "gtfs_flinders_departures")
        gtfs_stops_df = self.load_raw_parquet(latest_raw_run, "gtfs_train_stops")
        gtfs_trips_df = self.load_raw_parquet(latest_raw_run, "gtfs_train_trips")
        
        if gtfs_departures_df is None:
            self.logger.warning("No GTFS departures data found")
            return None
        
        # Create lookup tables
        stops_lookup = self._create_stops_lookup(gtfs_stops_df)
        trips_lookup = self._create_trips_lookup(gtfs_trips_df)
        
        # Process GTFS departures data
        all_departures = self._process_gtfs_departures(
            gtfs_departures_df, stops_lookup, trips_lookup
        )
        
        if not all_departures:
            self.logger.warning("No GTFS departure data found")
            return None
        
        # Create standardized departure DataFrame
        transformed_departures = self._create_standardized_departures(all_departures)
        
        # Add transformation metadata
        transformed_departures = self.add_transformation_metadata(transformed_departures)
        
        # Log platform statistics
        platform_count = len(transformed_departures[transformed_departures['platform_number'] != ''])
        self.logger.info(f"🎯 Successfully processed {platform_count} departures with REAL platform numbers from GTFS!")
        
        return transformed_departures
    
    def get_output_filename(self):
        return "departures_curated"
    
    def _create_stops_lookup(self, gtfs_stops_df):
        """Create stops lookup for platform information"""
        stops_lookup = {}
        if gtfs_stops_df is not None:
            for _, stop in gtfs_stops_df.iterrows():
                stops_lookup[stop['stop_id']] = {
                    'stop_name': stop.get('stop_name', ''),
                    'platform_code': stop.get('platform_code', ''),
                    'parent_station': stop.get('parent_station', ''),
                    'stop_lat': stop.get('stop_lat', 0),
                    'stop_lon': stop.get('stop_lon', 0)
                }
        return stops_lookup
    
    def _create_trips_lookup(self, gtfs_trips_df):
        """Create trips lookup for route information"""
        trips_lookup = {}
        if gtfs_trips_df is not None:
            for _, trip in gtfs_trips_df.iterrows():
                trips_lookup[trip['trip_id']] = {
                    'route_id': trip.get('route_id', ''),
                    'direction_id': trip.get('direction_id', 0),
                    'service_id': trip.get('service_id', '')
                }
        return trips_lookup
    
    def _process_gtfs_departures(self, gtfs_departures_df, stops_lookup, trips_lookup):
        """Process GTFS departures data"""
        all_departures = []
        
        self.logger.info(f"Processing {len(gtfs_departures_df)} GTFS departure records...")
        
        for _, departure in gtfs_departures_df.iterrows():
            stop_id = departure.get('stop_id', '')
            trip_id = departure.get('trip_id', '')
            
            # Get stop information (including REAL platform number)
            stop_info = stops_lookup.get(stop_id, {})
            trip_info = trips_lookup.get(trip_id, {})
            
            # Extract platform number - this is REAL data from GTFS!
            platform_number = stop_info.get('platform_code', '')
            
            clean_departure = {
                'station_id': stop_info.get('parent_station', stop_id),  # Use parent station for grouping
                'station_name': stop_info.get('stop_name', 'Unknown'),
                'stop_id': stop_id,  # Individual platform/stop
                'platform_number': platform_number,  # REAL platform number from GTFS!
                'route_id': trip_info.get('route_id', ''),
                'direction_id': trip_info.get('direction_id', 0),
                'trip_id': trip_id,
                'scheduled_departure_time': departure.get('departure_time', ''),
                'scheduled_arrival_time': departure.get('arrival_time', ''),
                'stop_sequence': departure.get('stop_sequence', 0),
                'pickup_type': departure.get('pickup_type', 0),
                'drop_off_type': departure.get('drop_off_type', 0),
                'stop_lat': stop_info.get('stop_lat', 0),
                'stop_lon': stop_info.get('stop_lon', 0),
                'data_source': 'GTFS_Schedule',
                'data_collection_date': datetime.now().strftime('%Y-%m-%d'),
                'data_collection_time': datetime.now().strftime('%H:%M:%S')
            }
            
            all_departures.append(clean_departure)
        
        return all_departures
    
    def _create_standardized_departures(self, all_departures):
        """Create standardized departure DataFrame"""
        # Create DataFrame with proper structure
        transformed_departures = pd.DataFrame({
            'station_id': [d.get('station_id') for d in all_departures],
            'station_name': [d.get('station_name', 'Unknown') for d in all_departures],
            'stop_id': [d.get('stop_id') for d in all_departures],
            'platform_number': [d.get('platform_number', '') for d in all_departures],
            'route_id': [d.get('route_id') for d in all_departures],
            'direction_id': [d.get('direction_id') for d in all_departures],
            'trip_id': [d.get('trip_id') for d in all_departures],
            'scheduled_departure_time': [d.get('scheduled_departure_time', '') for d in all_departures],
            'scheduled_arrival_time': [d.get('scheduled_arrival_time', '') for d in all_departures],
            'stop_sequence': [d.get('stop_sequence') for d in all_departures],
            'stop_lat': [d.get('stop_lat') for d in all_departures],
            'stop_lon': [d.get('stop_lon') for d in all_departures],
            'data_source': [d.get('data_source') for d in all_departures],
            'data_collection_date': [d.get('data_collection_date', '') for d in all_departures],
            'data_collection_time': [d.get('data_collection_time', '') for d in all_departures],
        })
        
        # Convert GTFS time format to proper datetime (HH:MM:SS)
        transformed_departures['scheduled_hour'] = transformed_departures['scheduled_departure_time'].apply(
            lambda x: int(x.split(':')[0]) % 24 if x and ':' in str(x) else 0
        )
        
        # Add delay analysis (GTFS is schedule data, so delays would be 0)
        transformed_departures['delay_minutes'] = 0.0
        transformed_departures['service_status'] = 'Scheduled'
        transformed_departures['delay_category'] = 'On Time'
        
        # Convert to Power BI compatible types
        transformed_departures = transformed_departures.astype({
            'station_id': 'string',
            'station_name': 'string',
            'stop_id': 'string',
            'platform_number': 'string',
            'route_id': 'string',
            'direction_id': 'Int64',
            'trip_id': 'string',
            'scheduled_departure_time': 'string',
            'scheduled_arrival_time': 'string',
            'stop_sequence': 'Int64',
            'stop_lat': 'float64',
            'stop_lon': 'float64',
            'data_source': 'string',
            'data_collection_date': 'string',
            'data_collection_time': 'string',
            'delay_minutes': 'float64',
            'service_status': 'string',
            'delay_category': 'string',
            'scheduled_hour': 'Int64'
        })
        
        # Fill any remaining nulls
        transformed_departures = transformed_departures.fillna({
            'platform_number': '',
            'data_collection_date': '',
            'data_collection_time': '',
            'delay_minutes': 0.0,
            'service_status': 'Scheduled',
            'delay_category': 'On Time',
            'scheduled_hour': 0
        })
        
        self.logger.info(f"Transformed {len(transformed_departures)} GTFS departure records with REAL platform numbers!")
        return transformed_departures
