# Resilient Public Services Platform

A production-ready data engineering and analytics platform that processes Victorian transport data to provide operational intelligence for service planning and network resilience monitoring.

## Overview

The Resilient Public Services Platform implements a comprehensive ETL pipeline that transforms raw transport data from multiple Victorian government APIs into analytics-ready datasets. The platform enables data-driven decision making for transport operators through real-time disruption monitoring, risk assessment, and operational dashboards.

## ✅ Current Status: Enterprise Production Ready

- **ETL Pipeline**: Fully operational with advanced orchestration
- **Data Sources**: 6 validated APIs providing comprehensive transport data
- **Processing**: 5-12 minutes end-to-end with parallel execution
- **Output**: Analytics-ready datasets with real platform numbers
- **Monitoring**: Enterprise-grade monitoring, alerting, and performance tracking
- **Orchestration**: Advanced dependency management, timeout protection, and quota management

## Key Features

### 🚂 Real Platform Numbers
- **Source**: GTFS Schedule data with actual platform numbers
- **Coverage**: Complete Victorian train network
- **Usage**: Powers departure analytics and passenger information

### 📊 Disruption Risk Index (DRI)
- **Algorithm**: Multi-factor risk scoring (0-100 scale)
- **Inputs**: Service status, disruptions, delays, route importance
- **Output**: Route-level risk scores for operational planning

### 🔄 Real-time Integration
- **GTFS Realtime**: Live vehicle positions and trip updates (hourly with quotas)
- **PTV API**: Real-time disruptions and service alerts (daily)
- **Data.vic**: Government transport dataset metadata (weekly)
- **Station Patronage**: Annual patronage data (monthly checks)
- **Temporal Patronage**: Monthly patronage patterns (monthly)

### 📈 Analytics Dashboard
- **Power BI**: Interactive transport resilience dashboard
- **Metrics**: Service performance, disruption patterns, risk hotspots
- **Visualization**: Geographic risk mapping and trend analysis

### 🎯 Enterprise Orchestration
- **Dependency Management**: Smart execution ordering with topological sorting
- **Timeout Protection**: Global (120min) and component-level timeouts
- **Quota Management**: API usage tracking with priority endpoints
- **Parallel Execution**: Concurrent processing of independent components
- **Notification System**: Real-time alerts via email and Slack
- **Performance Monitoring**: Real-time metrics and threshold alerting
- **Change Detection**: Skip unchanged data sources automatically
- **Data Quality Validation**: Automated quality checks and quarantine

## Architecture

```
📊 ETL Pipeline Architecture
├── 🥉 Bronze Layer (Raw Data)
│   ├── GTFS Schedule Ingestion (Weekly)
│   ├── GTFS Realtime Ingestion (Hourly)
│   ├── PTV API Ingestion (Daily)
│   ├── Data.vic Metadata (Weekly)
│   ├── Station Patronage (Monthly)
│   └── Temporal Patronage (Monthly)
├── 🥈 Silver Layer (Curated Data)
│   ├── Routes Transformation
│   ├── Disruptions Transformation
│   ├── Departures Transformation
│   └── DRI Calculation
└── 🥇 Gold Layer (Analytics)
    ├── Power BI Dashboard
    ├── Operational Reports
    └── Risk Analytics
```

## Project Structure

```
├── notebooks/              # ETL pipeline components
│   ├── data_ingestion_01.py           # Main ingestion orchestrator
│   ├── data_transformation_02.py      # Main transformation pipeline
├── orchestration/             # Advanced orchestration engine
│   ├── dependency_manager.py         # Dependency and execution management
│   ├── change_detection.py           # Intelligent change detection
│   ├── performance_monitor.py        # Real-time performance monitoring
│   └── notification_manager.py       # Multi-channel notification system
├── notebooks/              # Core pipeline scripts
│   ├── data_ingestion_01.py          # Main ingestion orchestrator
│   ├── data_transformation_02.py     # Transformation pipeline
│   ├── ingestion/                     # Modular ingestion components
│   │   ├── base_ingestion.py         # Base class with orchestration support
│   │   ├── gtfs_schedule_ingestion.py
│   │   ├── gtfs_realtime_ingestion.py # With quota management
│   │   ├── ptv_ingestion.py
│   │   ├── datavic_ingestion.py
│   │   ├── datavic_station_patronage.py
│   │   └── datavic_temporal_patronage.py
│   └── transformations/               # Modular transformation components
│       ├── routes_transformation.py
│       ├── disruptions_transformation.py
│       ├── departures_transformation.py
│       └── dri_calculation.py
├── data/                   # Data storage (medallion architecture)
│   ├── raw/               # Bronze layer - raw ingested data
│   └── curated/           # Silver layer - analytics-ready data
├── config/                 # Configuration and credentials
│   └── orchestration.json # Advanced orchestration configuration
├── dashboards/             # Power BI dashboard files
├── docs/                   # Comprehensive documentation
├── tests/                  # Debug and validation tools
└── requirements.txt        # Python dependencies
```

## Quick Start

### 1. Environment Setup
```bash
# Install dependencies
pip install -r requirements.txt

# Configure API credentials
cp config/config.env.template config/.env
# Edit config/.env with your API keys
```

### 2. Run ETL Pipeline
```bash
# Full ingestion pipeline with advanced orchestration
python notebooks/data_ingestion_01.py

# Data transformation pipeline
python notebooks/data_transformation_02.py

# Or run end-to-end
python notebooks/data_ingestion_01.py && python notebooks/data_transformation_02.py
```

### 3. Connect Power BI
- Open `dashboards/TransportResilienceDashboard.pbip`
- Connect to curated Parquet files in `data/curated/`
- Refresh data and explore analytics

## Data Sources

### Primary APIs
- **PTV Timetable API** - Real-time transport data, disruptions, routes
- **GTFS Schedule Data** - Complete Victorian transport timetables with platform numbers
- **GTFS Realtime API** - Live vehicle positions and trip updates
- **Data.vic.gov.au API** - Government transport dataset metadata

### Data Coverage
- **Geographic**: Complete Victoria
- **Transport Modes**: Train, Tram, Bus, V/Line
- **Update Frequency**: Real-time to daily depending on source
- **Volume**: ~50-100MB raw → ~20-40MB curated per run

## Key Datasets

### Curated Outputs
- **routes_curated**: Standardized route information with service status
- **disruptions_curated**: Service disruptions with severity scoring
- **departures_curated**: Departure data with real platform numbers
- **disruption_risk_index**: DRI scores for operational planning

### Analytics Features
- **Platform Analysis**: Real platform numbers for accurate passenger information
- **Risk Scoring**: 0-100 DRI scale with risk categories
- **Service Monitoring**: Real-time disruption tracking
- **Performance Metrics**: Delay analysis and service reliability

## Documentation

### 📋 [ETL Pipeline Documentation](docs/etl_pipeline.md)
Complete technical documentation covering:
- Pipeline architecture and components
- Data flow and transformation logic
- Performance metrics and optimization
- Configuration and deployment

### 🔌 [API Reference](docs/api_reference.md)
Comprehensive API documentation including:
- All data source APIs and endpoints
- Authentication and rate limiting
- Error handling and troubleshooting
- Performance and integration patterns

### 📊 [Power BI DAX Measures](docs/power_bi_dax_measures.md)
Ready-to-use Power BI measures for:
- Transport analytics and KPIs
- Risk scoring and categorization
- Service performance monitoring

## Performance Metrics

### Pipeline Performance
- **Full Pipeline**: 7-15 minutes end-to-end
- **Ingestion Only**: 5-10 minutes
- **Transformation Only**: 2-5 minutes
- **GTFS Realtime**: 1-3 seconds per collection

### Data Quality
- **GTFS Platform Data**: 100% coverage with real platform numbers
- **API Reliability**: Robust error handling and retry logic
- **Data Validation**: Comprehensive type checking and quality controls

## Business Value

### For Transport Operators
- **Operational Planning**: DRI scores identify high-risk routes and time periods
- **Resource Allocation**: Data-driven decision making for service deployment
- **Performance Monitoring**: Real-time service status and disruption tracking

### For Passengers
- **Accurate Information**: Real platform numbers and service status
- **Disruption Awareness**: Early warning of service issues
- **Journey Planning**: Reliable departure and arrival information

### For Analysts
- **Analytics Platform**: Production-ready datasets for further analysis
- **Extensible Architecture**: Modular design for easy enhancement
- **Comprehensive Monitoring**: Full observability and debugging tools

## Support & Maintenance

### Documentation
- **ETL Pipeline**: Complete technical documentation
- **API Reference**: All data source integration details
- **Troubleshooting**: Common issues and solutions

### Debug Tools
- **Test Files**: API connectivity validation
- **Debug Notebooks**: Step-by-step pipeline debugging
- **Monitoring**: Comprehensive logging and error tracking

### Configuration
- **Environment Variables**: Secure credential management
- **Flexible Settings**: Configurable processing parameters
- **Directory Management**: Automatic path creation and validation

---

**Last Updated**: July 13, 2025  
**Version**: 2.0  
**Status**: Production Ready
