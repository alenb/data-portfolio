"""
GTFS Ingestion Components
========================

This module provides access to both GTFS Schedule and GTFS Realtime ingestion components.
Import the specific component you need for your pipeline.

Components:
- GTFSScheduleIngestionComponent: For static timetable data with platform numbers
- GTFSRealtimeIngestionComponent: For live transport updates from VicRoads API

Usage:
    from .gtfs_ingestion import GTFSScheduleIngestionComponent, GTFSRealtimeIngestionComponent
"""

# Import the specific GTFS components
from .gtfs_schedule_ingestion import GTFSScheduleIngestionComponent
from .gtfs_realtime_ingestion import GTFSRealtimeIngestionComponent

# Make them available when importing this module
__all__ = ['GTFSScheduleIngestionComponent', 'GTFSRealtimeIngestionComponent']
