# API Reference

## Overview

The Resilient Public Services Platform integrates with multiple APIs to collect real-time and historical transport data across Victoria. This document provides comprehensive reference information for all APIs used in the ETL pipeline.

## API Status Summary

| API | Status | Purpose | Auth Method | Rate Limit | Coverage |
|-----|--------|---------|-------------|------------|----------|
| [PTV Timetable API](#ptv-timetable-api) | ✅ Operational | Real-time transport data | HMAC-SHA1 | Yes | Victoria |
| [GTFS Schedule Data](#gtfs-schedule-data) | ✅ Operational | Static timetables | None | No | Victoria |
| [GTFS Realtime API](#gtfs-realtime-api) | ✅ Operational | Live transport updates | API Key | Every 20s | Victoria |
| [Data.vic.gov.au API](#datavic-api) | ✅ Operational | Government datasets | API Key | Yes | Victoria |

---

## PTV Timetable API

### Overview
The Public Transport Victoria (PTV) Timetable API provides real-time public transport data including routes, stops, departures, and disruptions across Victoria.

### Authentication
- **Method**: HMAC-SHA1 signature authentication
- **Required**: User ID and API Key
- **Implementation**: Custom signature generation for each request

### Base URL
```
https://timetableapi.ptv.vic.gov.au
```

### Configuration
```python
PTV_USER_ID = "your_user_id"
PTV_API_KEY = "your_api_key"
PTV_BASE_URL = "https://timetableapi.ptv.vic.gov.au"
```

### Key Endpoints

#### Routes
- **Endpoint**: `/v3/routes`
- **Purpose**: Get all transport routes
- **Response**: Route information including names, types, and service status
- **Used in**: Routes transformation

#### Disruptions
- **Endpoint**: `/v3/disruptions`
- **Purpose**: Get current service disruptions
- **Response**: Active disruptions with severity and affected routes
- **Used in**: Disruptions transformation, DRI calculation

#### Route Types
- **Endpoint**: `/v3/route_types`
- **Purpose**: Get transport mode classifications
- **Response**: Route type mappings (0=Train, 1=Tram, 2=Bus, etc.)
- **Used in**: Route standardization

### Rate Limiting
- **Limit**: Enforced by PTV
- **Implementation**: 3-second delays between different endpoint calls
- **Monitoring**: Built-in error handling for rate limit violations

### Error Handling
- **403 Forbidden**: Usually signature generation issues
- **429 Too Many Requests**: Rate limit exceeded
- **500 Server Error**: PTV service issues

### Sample Request
```python
def _generate_ptv_signature(self, request_url):
    """Generate HMAC-SHA1 signature for PTV API authentication"""
    signature = hmac.new(
        self.config.PTV_API_KEY.encode('utf-8'),
        request_url.encode('utf-8'),
        hashlib.sha1
    ).hexdigest()
    return signature
```

---

## GTFS Schedule Data

### Overview
General Transit Feed Specification (GTFS) Schedule data provides complete Victorian transport timetables including **real platform numbers**.

### Authentication
- **Method**: None (public download)
- **Required**: No API key needed

### Data Source
- **URL**: https://opendata.transport.vic.gov.au/dataset/gtfs-schedule
- **Format**: ZIP file containing text files
- **Size**: ~258MB
- **Update Frequency**: Daily

### Key Files Processed

#### routes.txt
- **Purpose**: Transport route definitions
- **Fields**: route_id, route_short_name, route_long_name, route_type
- **Used in**: Route identification and classification

#### stops.txt
- **Purpose**: Station and platform information with **real platform numbers**
- **Fields**: stop_id, stop_name, stop_lat, stop_lon, platform_code
- **Used in**: Platform analysis, departure processing

#### stop_times.txt
- **Purpose**: Departure times for each platform
- **Fields**: trip_id, stop_id, departure_time, platform_code
- **Used in**: Departure analytics with real platform numbers

#### trips.txt
- **Purpose**: Individual service trips
- **Fields**: trip_id, route_id, service_id, direction_id
- **Used in**: Route-trip mapping

### Processing Pipeline
1. **Download**: 258MB ZIP file from VicTrack
2. **Extract**: Unzip to temporary directory
3. **Process**: Parse CSV files with platform numbers
4. **Transform**: Convert to Parquet format
5. **Sample**: Extract manageable datasets for analytics

### Key Features
- **Real Platform Numbers**: Extracted from stops.txt platform_code field
- **Complete Coverage**: All Victorian transport modes
- **Geographic Data**: Latitude/longitude coordinates
- **Service Patterns**: Complete timetable information

---

## GTFS Realtime API

### Overview
GTFS Realtime API provides live transport updates including vehicle positions, trip updates, and service alerts using Protocol Buffers format.

### Authentication
- **Method**: API Key in header
- **Header**: `Ocp-Apim-Subscription-Key`
- **Required**: VicRoads API Key (different from Data.vic key)

### Base URL
```
https://data-exchange-api.vicroads.vic.gov.au/opendata/v1/gtfsr
```

### Configuration
```python
VICROADS_API_KEY = "your_vicroads_api_key"
```

### Key Endpoints

#### Trip Updates
- **Endpoint**: `/metrotrain-tripupdates`
- **Purpose**: Real-time arrival/departure predictions
- **Format**: Protocol Buffers (binary)
- **Update Frequency**: Every 20 seconds

#### Vehicle Positions
- **Endpoint**: `/metrotrain-vehicleposition-updates`
- **Purpose**: Live vehicle locations
- **Format**: Protocol Buffers (binary)
- **Update Frequency**: Every 20 seconds

#### Service Alerts
- **Endpoint**: `/metrotrain-servicealerts`
- **Purpose**: Current service disruptions
- **Format**: Protocol Buffers (binary)
- **Update Frequency**: Real-time

### Data Processing
1. **Fetch**: Download binary protobuf data
2. **Parse**: Convert using gtfs-realtime-bindings
3. **Transform**: Convert to JSON format
4. **Store**: Save as Parquet files

### Rate Limiting
- **Limit**: Every 20 seconds (VicRoads requirement)
- **Implementation**: Built-in delay mechanisms
- **Monitoring**: Error handling for quota violations

### Dependencies
```python
pip install gtfs-realtime-bindings
```

### Sample Usage
```python
from google.transit import gtfs_realtime_pb2
from google.protobuf.json_format import MessageToDict

response = requests.get(endpoint, headers=headers)
feed = gtfs_realtime_pb2.FeedMessage()
feed.ParseFromString(response.content)
data = MessageToDict(feed)
```

---

## Data.vic API

### Overview
Data.vic.gov.au API provides access to Victorian government open data catalog including transport dataset metadata.

### Authentication
- **Method**: API Key in header
- **Header**: `apikey`
- **Required**: Data.vic API Key

### Base URL
```
https://wovg-community.gateway.prod.api.vic.gov.au/datavic/v1.2
```

### Configuration
```python
DATA_VIC_API_KEY = "your_datavic_api_key"
```

### Key Endpoints

#### Package Search
- **Endpoint**: `/package_search`
- **Purpose**: Search for datasets by keyword
- **Parameters**: q (query), fq (filter query), rows (limit)
- **Used in**: Dataset discovery

#### Package Show
- **Endpoint**: `/package_show`
- **Purpose**: Get detailed dataset information
- **Parameters**: id (dataset ID)
- **Used in**: Metadata collection

### Search Terms Used
- "traffic volume" - Traffic count datasets
- "public transport disruption" - Transport disruption data
- "road disruption" - Road closure information
- "freight movement" - Commercial transport data

### Rate Limiting
- **Limit**: Enforced by Data.vic
- **Implementation**: 1-2 second delays between requests
- **Monitoring**: Built-in error handling

### Data Usage
- **Purpose**: Metadata collection only
- **Storage**: Dataset information and API endpoints
- **Integration**: Links to additional transport datasets

### Sample Request
```python
def _call_datavic_api(self, endpoint, params=None):
    headers = {
        'accept': 'application/json',
        'apikey': self.config.DATA_VIC_API_KEY
    }
    url = f"https://wovg-community.gateway.prod.api.vic.gov.au/datavic/v1.2{endpoint}"
    response = requests.get(url, headers=headers, params=params)
    return response.json()
```

---

## API Integration Architecture

### Component Structure
```
notebooks/ingestion/
├── base_ingestion.py          # Base API client functionality
├── ptv_ingestion.py           # PTV API integration
├── gtfs_schedule_ingestion.py # GTFS Schedule processing
├── gtfs_realtime_ingestion.py # GTFS Realtime integration
└── datavic_ingestion.py       # Data.vic API integration
```

### Common Patterns

#### Error Handling
All API components implement:
- Retry logic with exponential backoff
- Rate limit compliance
- Graceful degradation on failures
- Comprehensive error logging

#### Authentication
Each API uses appropriate authentication:
- PTV: HMAC-SHA1 signature generation
- GTFS Schedule: No authentication required
- GTFS Realtime: API key in header
- Data.vic: API key in header

#### Rate Limiting
Implemented across all APIs:
- PTV: 3-second delays between endpoint types
- GTFS Realtime: 20-second minimum intervals
- Data.vic: 1-2 second delays between requests

### Configuration Management
All API credentials stored in `config/.env`:
```bash
# PTV Configuration
PTV_USER_ID=your_user_id
PTV_API_KEY=your_api_key

# VicRoads Configuration
VICROADS_API_KEY=your_vicroads_key

# Data.vic Configuration
DATA_VIC_API_KEY=your_datavic_key
```

---

## Troubleshooting

### Common Issues

#### PTV API 403 Forbidden
**Cause**: Signature generation issues
**Solution**: 
1. Verify API key and user ID
2. Check signature generation algorithm
3. Ensure proper URL encoding

#### GTFS Realtime Quota Exceeded
**Cause**: Too frequent requests
**Solution**:
1. Respect 20-second minimum interval
2. Implement proper request throttling
3. Monitor API usage patterns

#### Data.vic Rate Limiting
**Cause**: Too many requests too quickly
**Solution**:
1. Increase delays between requests
2. Implement exponential backoff
3. Use batch processing where possible

### Monitoring and Debugging

#### API Status Checks
Each component provides:
- Connection validation
- Response time monitoring
- Error rate tracking
- Data quality validation

#### Logging
Comprehensive logging includes:
- API request/response details
- Error messages and stack traces
- Performance metrics
- Rate limit compliance

#### Debug Tools
Located in `tests/` directory:
- `test_api_connections.py` - API connectivity tests
- Individual component test files
- Debug notebooks for step-by-step testing

---

## Performance Metrics

### API Response Times
- **PTV API**: 1-3 seconds per endpoint
- **GTFS Schedule**: 2-5 minutes (download + processing)
- **GTFS Realtime**: 1-3 seconds per endpoint
- **Data.vic API**: 1-2 seconds per request

### Data Volumes
- **PTV API**: ~5-10MB per full collection
- **GTFS Schedule**: 258MB download, ~10MB processed
- **GTFS Realtime**: ~1-5MB per collection
- **Data.vic API**: ~1-2MB metadata only

### Update Frequencies
- **PTV API**: On-demand execution
- **GTFS Schedule**: Daily batch updates
- **GTFS Realtime**: Every 20 seconds (continuous)
- **Data.vic API**: Weekly metadata updates

---

**Last Updated**: July 13, 2025  
**Version**: 1.0  
**Status**: Production Ready
