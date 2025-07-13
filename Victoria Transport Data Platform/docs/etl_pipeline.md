# ETL Pipeline Documentation

## Overview

The Resilient Public Services Platform implements a comprehensive ETL (Extract, Transform, Load) pipeline following the medallion architecture pattern. The pipeline processes Victorian transport data from multiple sources to create analytics-ready datasets for operational intelligence.

## Architecture Overview

```
📊 ETL Pipeline Architecture
├── 🥉 Bronze Layer (Raw Data Ingestion)
│   ├── GTFS Schedule Ingestion (Weekly)
│   ├── GTFS Realtime Ingestion (Hourly)
│   ├── PTV API Ingestion (Daily)
│   ├── Data.vic Metadata Ingestion (Weekly)
│   ├── Station Patronage Ingestion (Monthly)
│   └── Temporal Patronage Ingestion (Monthly)
├── 🥈 Silver Layer (Data Transformation)
│   ├── Routes Transformation
│   ├── Disruptions Transformation
│   ├── Departures Transformation
│   └── DRI Calculation
└── 🥇 Gold Layer (Analytics)
    ├── Power BI Dashboard
    ├── Operational Reports
    └── Risk Analytics
```

## Pipeline Status

✅ **PRODUCTION READY** - All components operational with enterprise orchestration

### Performance Metrics
- **Full Pipeline Runtime**: 5-12 minutes end-to-end (with parallelization)
- **Ingestion Only**: 3-8 minutes (with intelligent skipping)
- **Transformation Only**: 2-5 minutes
- **Data Volume**: ~50-100MB raw → ~20-40MB curated
- **Concurrent Execution**: Up to 2 parallel components
- **Timeout Protection**: Global 120min, component-specific limits

### Enterprise Features
- **🔄 Dependency Management**: Smart execution ordering
- **⏱️ Timeout Management**: Prevents runaway processes
- **🔢 Quota Management**: API usage tracking and limits
- **🚀 Parallel Execution**: Concurrent independent components
- **📧 Notification System**: Real-time alerts for failures
- **📊 Performance Monitoring**: Real-time metrics and thresholds
- **🔍 Change Detection**: Skip unchanged data sources
- **✅ Data Quality Validation**: Automated quality checks

---

## Bronze Layer - Data Ingestion

### Main Ingestion Pipeline

**File**: `notebooks/data_ingestion_01.py`

#### Purpose
Orchestrates collection of raw transport data from multiple APIs using modular components with advanced enterprise orchestration capabilities.

#### Architecture
```python
class TransportDataIngestionPipeline:
    def __init__(self):
        # Initialize modular components
        self.gtfs_schedule_component = GTFSScheduleIngestionComponent(...)
        self.gtfs_realtime_component = GTFSRealtimeIngestionComponent(...)
        self.ptv_component = PTVIngestionComponent(...)
        self.datavic_component = DataVicIngestionComponent(...)
```

#### Execution Flow
1. **GTFS Schedule Data**: Download and process complete Victorian transport timetables
2. **GTFS Realtime Data**: Collect live transport updates every 20 seconds
3. **PTV API Data**: Fetch real-time disruptions and route status
4. **Data.vic Metadata**: Collect transport dataset metadata

#### Output Structure
```
data/raw/
├── year=2025/month=07/day=13/
│   └── run_20250713_143000/
│       ├── gtfs_train_routes_*.parquet
│       ├── gtfs_train_stops_*.parquet
│       ├── gtfs_flinders_departures_*.parquet
│       ├── gtfs_train_trips_*.parquet
│       ├── gtfs_tram_routes_*.parquet
│       ├── gtfs_tram_stops_*.parquet
│       ├── gtfs_realtime_trip_updates_*.parquet
│       ├── gtfs_realtime_vehicle_positions_*.parquet
│       ├── gtfs_realtime_service_alerts_*.parquet
│       ├── ptv_routes_*.parquet
│       ├── ptv_disruptions_*.parquet
│       ├── ptv_route_types_*.parquet
│       └── datavic_*_metadata_*.parquet
```

### Component Details

## Advanced Orchestration Features

### Dependency Management
**File**: `orchestration/dependency_manager.py`

#### Features
- **Topological Sorting**: Components execute in correct dependency order
- **Frequency-based Scheduling**: Daily, weekly, monthly execution patterns
- **Change Detection**: Skip components if source data unchanged
- **Execution Tracking**: Persistent history of all pipeline runs
- **Intelligent Skipping**: Skip components based on dependency state

#### Configuration-Driven Execution
```json
{
  "components": {
    "gtfs_schedule": {
      "enabled": true,
      "frequency": "weekly",
      "dependencies": [],
      "max_runtime_minutes": 30
    },
    "gtfs_realtime": {
      "enabled": true,
      "frequency": "hourly",
      "dependencies": ["gtfs_schedule"],
      "max_runtime_minutes": 15,
      "quota_management": {
        "daily_limit": 1000,
        "retry_after_quota": "24h"
      }
    }
  }
}
```

### Timeout Management
- **Global Timeout**: 120-minute maximum execution time
- **Component Timeouts**: Individual limits (5-30 minutes per component)
- **Graceful Termination**: Proper cleanup on timeout
- **Performance Monitoring**: Real-time execution tracking

### Parallel Execution
- **Concurrent Components**: Execute independent components simultaneously
- **Thread Pool Management**: Configurable worker limits
- **Dependency Respect**: Only parallel-safe components run concurrently
- **Error Isolation**: Failures in one component don't affect others

### Quota Management
**File**: `notebooks/ingestion/gtfs_realtime_ingestion.py`

#### Features
- **Daily Limits**: Track API calls per day
- **Priority Endpoints**: Critical endpoints bypass quota limits
- **Persistent Tracking**: Quota state preserved across runs
- **Automatic Reset**: Daily quota resets at midnight

### Notification System
**File**: `orchestration/notification_manager.py`

#### Features
- **Multi-Channel Alerts**: Email and Slack notifications
- **Performance Alerts**: Real-time threshold monitoring
- **Failure Notifications**: Immediate alerts for component failures
- **Execution Summaries**: Detailed pipeline completion reports

#### GTFS Schedule Ingestion
**Component**: `GTFSScheduleIngestionComponent`
**File**: `notebooks/ingestion/gtfs_schedule_ingestion.py`

##### Features
- Downloads 258MB GTFS ZIP file from VicTrack
- Extracts **real platform numbers** from stops.txt
- Processes all transport modes (train, tram, bus, V/Line)
- Handles large files with streaming processing
- Focuses on Flinders Street for departure analytics
- **NEW**: Timeout protection and retry logic

##### Key Processing Steps
1. **Download**: Fetch GTFS ZIP from Victorian Open Data
2. **Extract**: Unzip to temporary directory
3. **Process**: Parse CSV files with pandas
4. **Transform**: Convert to Parquet format
5. **Sample**: Extract manageable datasets

##### Datasets Produced
- `gtfs_train_routes` - Train line information
- `gtfs_train_stops` - Station and platform data **with real platform numbers**
- `gtfs_flinders_departures` - Flinders Street departures with platforms
- `gtfs_train_trips` - Service trip information
- `gtfs_tram_routes` - Tram route information
- `gtfs_tram_stops` - Tram stop information

#### GTFS Realtime Ingestion
**Component**: `GTFSRealtimeIngestionComponent`
**File**: `notebooks/ingestion/gtfs_realtime_ingestion.py`

##### Features
- VicRoads API integration with Protocol Buffers
- Real-time data with quota management
- Handles binary protobuf format
- Converts to JSON and Parquet
- Respects API rate limits with intelligent retry
- **NEW**: Quota tracking and priority endpoint management

##### Key Processing Steps
1. **Quota Check**: Verify daily limits before processing
2. **Fetch**: Download binary protobuf data
3. **Parse**: Convert using gtfs-realtime-bindings
4. **Transform**: Convert to JSON format
5. **Store**: Save as Parquet files
6. **Quota Update**: Track API usage

##### Datasets Produced
- `gtfs_realtime_trip_updates` - Real-time arrival/departure predictions
- `gtfs_realtime_vehicle_positions` - Live vehicle locations
- `gtfs_realtime_service_alerts` - Current service disruptions

#### PTV API Ingestion
**Component**: `PTVIngestionComponent`
**File**: `notebooks/ingestion/ptv_ingestion.py`

##### Features
- HMAC-SHA1 signature authentication
- Real-time disruption data
- Route status information
- Intelligent rate limiting
- Comprehensive error handling

##### Key Processing Steps
1. **Authenticate**: Generate HMAC-SHA1 signature
2. **Fetch Routes**: Get all transport routes
3. **Fetch Disruptions**: Get current service disruptions
4. **Fetch Route Types**: Get transport mode classifications
5. **Store**: Save as Parquet files

##### Datasets Produced
- `ptv_routes` - All transport routes with service status
- `ptv_disruptions` - Current disruptions with severity
- `ptv_route_types` - Route classification data

#### Data.vic Metadata Ingestion
**Component**: `DataVicIngestionComponent`
**File**: `notebooks/ingestion/datavic_ingestion.py`

##### Features
- Government open data catalog integration
- Metadata collection for transport datasets
- Priority search terms for relevant data
- API key authentication

##### Key Processing Steps
1. **Search**: Query for transport-related datasets
2. **Retrieve**: Get detailed dataset metadata
3. **Filter**: Focus on priority transport categories
4. **Store**: Save metadata as Parquet files

##### Datasets Produced
- `datavic_traffic_volume_metadata` - Traffic count datasets
- `datavic_public_transport_disruption_metadata` - Transport disruption data
- `datavic_road_disruption_metadata` - Road closure information
- `datavic_freight_movement_metadata` - Commercial transport data

---

## Silver Layer - Data Transformation

### Main Transformation Pipeline

**File**: `notebooks/data_transformation_02.py`

#### Purpose
Transforms raw transport data into standardized, analytics-ready datasets using modular transformation components.

#### Architecture
```python
class TransportDataTransformation:
    def __init__(self):
        # Initialize transformation components
        self.transformations = {
            'routes': RoutesTransformationComponent(...),
            'disruptions': DisruptionsTransformationComponent(...),
            'departures': DeparturesTransformationComponent(...),
            'traffic': TrafficTransformationComponent(...),
            'dri': DRICalculationComponent(...)
        }
```

#### Execution Flow
1. **Find Latest Data**: Locate most recent raw data ingestion
2. **Transform Routes**: Standardize route information
3. **Transform Disruptions**: Process service disruptions
4. **Transform Departures**: Process departure data with platform numbers
5. **Calculate DRI**: Generate Disruption Risk Index
6. **Save Results**: Store curated datasets

#### Output Structure
```
data/curated/
├── routes_curated_20250713_143000.parquet
├── disruptions_curated_20250713_143000.parquet
├── departures_curated_20250713_143000.parquet
├── disruption_risk_index_20250713_143000.parquet
└── transformation_summary_20250713_143000.parquet
```

### Transformation Components

#### Routes Transformation
**Component**: `RoutesTransformationComponent`
**File**: `notebooks/transformations/routes_transformation.py`

##### Purpose
Standardizes route information across all transport modes.

##### Key Transformations
- **Route Classification**: Categorizes routes by importance and type
- **Service Status**: Standardizes service status across sources
- **Disruption Severity**: Calculates route-level disruption scores
- **Geographic Mapping**: Adds geographic context to routes

##### Output Schema
```python
{
    'route_id': 'int64',
    'route_name': 'string',
    'route_type': 'int64',
    'route_type_name': 'string',
    'service_status': 'string',
    'disruption_severity': 'int64',
    'route_classification': 'string',
    'transformation_timestamp': 'string'
}
```

#### Disruptions Transformation
**Component**: `DisruptionsTransformationComponent`
**File**: `notebooks/transformations/disruptions_transformation.py`

##### Purpose
Processes service disruptions into standardized format with severity scoring.

##### Key Transformations
- **Severity Scoring**: Converts disruption status to numeric scale (1-10)
- **Affected Routes**: Extracts and counts affected routes
- **Time Standardization**: Normalizes timestamps across sources
- **Classification**: Categorizes disruptions by type and impact

##### Severity Mapping
```python
severity_map = {
    'Current': 5,      # Active disruption
    'Planned': 2,      # Planned maintenance
    'Ended': 1,        # Resolved disruption
    'Unknown': 3       # Default for unclear status
}
```

##### Output Schema
```python
{
    'disruption_id': 'int64',
    'title': 'string',
    'description': 'string',
    'disruption_status': 'string',
    'severity_score': 'int64',
    'affected_routes': 'int64',
    'start_time': 'timestamp',
    'end_time': 'timestamp',
    'transformation_timestamp': 'string'
}
```

#### Departures Transformation
**Component**: `DeparturesTransformationComponent`
**File**: `notebooks/transformations/departures_transformation.py`

##### Purpose
Processes GTFS departure data with **real platform numbers** for analytics.

##### Key Transformations
- **Platform Mapping**: Links departures to real platform numbers
- **Route Mapping**: Connects departures to route information
- **Time Standardization**: Normalizes departure times
- **Delay Calculation**: Calculates delay metrics where available

##### Key Features
- **Real Platform Numbers**: Extracted from GTFS stops.txt
- **Flinders Street Focus**: Detailed analysis of major hub
- **Service Frequency**: Calculates service intervals
- **Peak Period Analysis**: Identifies rush hour patterns

##### Output Schema
```python
{
    'route_id': 'int64',
    'route_name': 'string',
    'stop_id': 'int64',
    'stop_name': 'string',
    'platform_number': 'string',
    'departure_time': 'string',
    'direction_id': 'int64',
    'service_id': 'string',
    'delay_minutes': 'float64',
    'transformation_timestamp': 'string'
}
```

#### DRI Calculation
**Component**: `DRICalculationComponent`
**File**: `notebooks/transformations/dri_calculation.py`

##### Purpose
Calculates Disruption Risk Index (DRI) for each route combining multiple risk factors.

##### Algorithm Components
1. **Service Status Score** (40% weight): Current service performance
2. **Disruption Count Score** (30% weight): Number of active disruptions
3. **Delay Performance Score** (30% weight): Average delay metrics
4. **Importance Weight**: Metro lines receive 1.5x multiplier

##### Calculation Formula
```python
raw_dri = (service_score * 0.4 + disruption_score * 0.3 + delay_score * 0.3)
weighted_dri = min(raw_dri * importance_weight, 1.0) * 100
```

##### Risk Categories
- **Low Risk**: 0-20 DRI score
- **Moderate Risk**: 21-40 DRI score
- **High Risk**: 41-70 DRI score
- **Critical Risk**: 71-100 DRI score

##### Output Schema
```python
{
    'route_id': 'int64',
    'route_name': 'string',
    'route_type': 'int64',
    'route_type_name': 'string',
    'service_status_score': 'float64',
    'disruption_count_score': 'float64',
    'delay_performance_score': 'float64',
    'importance_weight': 'float64',
    'disruption_risk_index': 'float64',
    'risk_category': 'string',
    'calculation_timestamp': 'string',
    'calculation_date': 'string'
}
```

---

## Data Quality & Validation

### Data Quality Features

#### Type Enforcement
- **Explicit Data Types**: All columns have defined types for Power BI compatibility
- **Null Handling**: Comprehensive null value management across all datasets
- **String Standardization**: Consistent string formatting and encoding

#### Cross-Dataset Validation
- **Referential Integrity**: Route IDs consistent across datasets
- **Timestamp Validation**: Consistent time formats across sources
- **Geographic Validation**: Coordinate validation for location data

#### Error Handling
- **Graceful Degradation**: Pipeline continues if individual components fail
- **Comprehensive Logging**: Detailed error reporting and debugging
- **Retry Logic**: Automatic retry for transient failures

### Performance Optimization

#### File Formats
- **Parquet**: Optimized compression and query performance
- **Partitioning**: Date-based partitioning for efficient queries
- **Compression**: Automatic compression for space efficiency

#### Memory Management
- **Streaming Processing**: Handles large datasets without memory issues
- **Chunked Processing**: Processes data in manageable chunks
- **Incremental Updates**: Only processes new data since last run

---

## Configuration & Deployment

### Configuration Management

#### Environment Variables
All configuration stored in `config/.env`:
```bash
# API Configuration
PTV_USER_ID=your_user_id
PTV_API_KEY=your_api_key
VICROADS_API_KEY=your_vicroads_key
DATA_VIC_API_KEY=your_datavic_key

# Processing Configuration
BATCH_SIZE=1000
DELAY_THRESHOLD_MINUTES=15
```

#### Directory Structure
```python
# Automatic directory creation
PROJECT_ROOT = Path(__file__).parent.parent
DATA_ROOT = PROJECT_ROOT / "data"
RAW_DATA_PATH = DATA_ROOT / "raw"
CURATED_DATA_PATH = DATA_ROOT / "curated"
LOGS_PATH = PROJECT_ROOT / "logs"
```

### Execution Options

#### Individual Pipeline Components
```bash
# Run specific ingestion components
python notebooks/ingestion/gtfs_schedule_ingestion.py
python notebooks/ingestion/ptv_ingestion.py

# Run specific transformation components
python notebooks/transformations/routes_transformation.py
python notebooks/transformations/dri_calculation.py
```

#### Full Pipeline Execution
```bash
# Complete ingestion pipeline
python notebooks/data_ingestion_01.py

# Complete transformation pipeline
python notebooks/data_transformation_02.py

# End-to-end pipeline
python notebooks/data_ingestion_01.py && python notebooks/data_transformation_02.py
```

#### Scheduled Execution
```bash
# Daily batch processing
0 6 * * * /path/to/python notebooks/data_ingestion_01.py
0 7 * * * /path/to/python notebooks/data_transformation_02.py

# Real-time GTFS ingestion (every 20 seconds)
*/20 * * * * /path/to/python notebooks/ingestion/gtfs_realtime_ingestion.py
```

---

## Monitoring & Troubleshooting

### Logging & Monitoring

#### Structured Logging
- **Log Level**: INFO, WARNING, ERROR with timestamps
- **Component Tracking**: Each component logs its progress
- **Error Context**: Full stack traces for debugging
- **Performance Metrics**: Execution times and data volumes

#### Progress Tracking
```python
# Example logging output
2025-07-13 14:30:00 - INFO - Starting GTFS Schedule data ingestion...
2025-07-13 14:30:15 - INFO - Downloaded 258MB GTFS ZIP file
2025-07-13 14:31:00 - INFO - Processed 15,000 train stops with platform numbers
2025-07-13 14:31:30 - INFO - ✅ GTFS Schedule ingestion complete
```

### Debug Tools

#### Test Files
Located in `tests/` directory:
- `test_api_connections.py` - API connectivity validation
- `debug_ingestion.ipynb` - Step-by-step ingestion debugging
- `debug_transformation.ipynb` - Transformation debugging

#### Validation Checks
- **Data Volume**: Monitor for unexpected data size changes
- **Schema Validation**: Ensure consistent data structures
- **API Status**: Regular connectivity checks

### Common Issues & Solutions

#### Missing Raw Data
**Symptoms**: Transformation pipeline fails to find input data
**Solutions**:
1. Verify ingestion pipeline ran successfully
2. Check API connectivity and credentials
3. Validate data directory permissions

#### Transformation Failures
**Symptoms**: Curated datasets not generated
**Solutions**:
1. Check raw data quality and format
2. Review transformation logs for specific errors
3. Use debug notebooks for step-by-step analysis

#### Performance Issues
**Symptoms**: Pipeline execution takes too long
**Solutions**:
1. Monitor memory usage during processing
2. Implement data sampling for development
3. Check disk space for temporary files

---

## Integration Points

### Power BI Integration

#### Data Connection
- **Source**: Curated Parquet files
- **Location**: `data/curated/` directory
- **Refresh**: Manual or scheduled via Power BI Gateway

#### Key Datasets
- **routes_curated**: Route information and status
- **disruptions_curated**: Service disruption data
- **departures_curated**: Departure data with platform numbers
- **disruption_risk_index**: DRI scores for risk visualization

### Future Enhancements

#### Orchestration
- **Apache Airflow**: Workflow management and scheduling
- **Azure Data Factory**: Cloud-based ETL orchestration
- **Databricks Jobs**: Managed job execution

#### Real-time Processing
- **Apache Kafka**: Real-time data streaming
- **Azure Event Hubs**: Event-driven architecture
- **Stream Processing**: Live analytics and alerts

#### Advanced Analytics
- **Machine Learning**: Predictive disruption modeling
- **Anomaly Detection**: Automated incident detection
- **Forecasting**: Service demand prediction

---

## Maintenance & Support

### Regular Maintenance Tasks

#### Daily
- Monitor pipeline execution logs
- Verify data quality metrics
- Check API connectivity status

#### Weekly
- Review data volume trends
- Validate transformation accuracy
- Update API credentials if needed

#### Monthly
- Performance optimization review
- Schema validation across datasets
- Capacity planning assessment

### Support Resources

#### Documentation
- **API Reference**: Complete API documentation
- **Component Guides**: Individual component documentation
- **Configuration Reference**: All configuration options

#### Troubleshooting
- **Error Codes**: Common error patterns and solutions
- **Debug Procedures**: Step-by-step debugging guides
- **Performance Tuning**: Optimization recommendations

---

**Last Updated**: July 13, 2025  
**Version**: 2.0  
**Status**: Production Ready
