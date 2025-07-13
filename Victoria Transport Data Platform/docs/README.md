# Documentation Index

## 📚 Complete Documentation Guide

This directory contains comprehensive documentation for the **Resilient Public Services Platform** - a production-ready ETL pipeline and analytics system for Victorian transport data.

---

## 🏗️ Core Documentation

### [ETL Pipeline Documentation](./etl_pipeline.md)
**Complete Technical Reference** - Comprehensive ETL pipeline documentation
- **Architecture**: Medallion architecture with Bronze, Silver, and Gold layers
- **Components**: Modular ingestion and transformation components
- **Performance**: Pipeline metrics, optimization, and capacity planning
- **Configuration**: Deployment, scheduling, and maintenance
- **Enterprise Features**: Advanced orchestration, monitoring, and alerting
- **Audience**: Developers, data engineers, DevOps engineers

### [Enterprise Orchestration Features](./enterprise_orchestration.md)
**Production-Ready Orchestration** - Advanced pipeline management capabilities
- **Dependency Management**: Smart execution ordering and scheduling
- **Timeout Management**: Global and component-level timeout protection
- **Quota Management**: API usage tracking and priority endpoints
- **Parallel Execution**: Concurrent component processing
- **Notification System**: Multi-channel alerts and performance monitoring
- **Change Detection**: Intelligent skipping of unchanged data sources
- **Audience**: DevOps engineers, system administrators, data engineers

### [Orchestration Strategy](./orchestrations.md)
**Implementation Roadmap** - Complete orchestration evolution and strategy
- **Phase 1**: Configuration-driven scheduling
- **Phase 2**: Dependency management and execution ordering
- **Phase 3**: Orchestration intelligence and adaptive scheduling
- **Phase 4**: Advanced features (timeouts, quotas, parallel execution)
- **Enterprise Readiness**: Production deployment considerations
- **Audience**: Project managers, architects, technical leads

### [API Reference](./api_reference.md)
**Complete API Integration Guide** - All data source APIs and integrations
- **PTV Timetable API**: Real-time transport data with HMAC authentication
- **GTFS Data Sources**: Schedule and realtime data with platform numbers
- **Data.vic API**: Government transport dataset metadata
- **Integration Patterns**: Error handling, rate limiting, authentication
- **Audience**: Developers, data engineers, system integrators

### [Power BI DAX Measures](./power_bi_dax_measures.md)
**Dashboard Development Guide** - Ready-to-use Power BI measures
- **Transport Analytics**: Service performance and disruption metrics
- **Risk Scoring**: DRI calculation and risk categorization
- **Operational KPIs**: Real-time monitoring and historical analysis
- **Audience**: Business analysts, Power BI developers

---

## 🚀 Quick Start Guide

### For New Team Members
1. **Project Overview**: Start with main [README](../README.md)
2. **Technical Details**: Review [ETL Pipeline Documentation](./etl_pipeline.md)
3. **Enterprise Features**: Check [Enterprise Orchestration Features](./enterprise_orchestration.md)
4. **API Understanding**: Check [API Reference](./api_reference.md)
5. **Dashboard Development**: Use [Power BI DAX Measures](./power_bi_dax_measures.md)

### For Developers
1. **Architecture**: [ETL Pipeline Documentation](./etl_pipeline.md) - Technical architecture
2. **Orchestration**: [Enterprise Orchestration Features](./enterprise_orchestration.md) - Advanced features
3. **API Integration**: [API Reference](./api_reference.md) - All data source details
4. **Configuration**: [Orchestration Strategy](./orchestrations.md) - Implementation roadmap
5. **Troubleshooting**: All documents contain debugging and support information

### For DevOps Engineers
1. **Production Deployment**: [Enterprise Orchestration Features](./enterprise_orchestration.md)
2. **Monitoring Setup**: [ETL Pipeline Documentation](./etl_pipeline.md) - Performance monitoring
3. **Configuration Management**: [Orchestration Strategy](./orchestrations.md) - Config management
4. **Alert Configuration**: [Enterprise Orchestration Features](./enterprise_orchestration.md) - Notification system

### For Business Analysts
1. **Business Value**: Main [README](../README.md) - Project overview and value
2. **Data Understanding**: [API Reference](./api_reference.md) - Data sources and coverage
3. **Dashboard Development**: [Power BI DAX Measures](./power_bi_dax_measures.md) - Ready-to-use measures
4. **Pipeline Overview**: [ETL Pipeline Documentation](./etl_pipeline.md) - Data flow and transformations

---

## � Documentation Status

| Document | Status | Last Updated | Version |
|----------|--------|-------------|---------|
| ETL Pipeline Documentation | ✅ Current | July 13, 2025 | 2.0 |
| API Reference | ✅ Current | July 13, 2025 | 1.0 |
| Power BI DAX Measures | ✅ Current | July 6, 2025 | 1.0 |

---

## 🏗️ Architecture Overview

The platform implements a medallion architecture ETL pipeline:

```
📊 ETL Pipeline Architecture
├── 🥉 Bronze Layer (Raw Data)
│   ├── GTFS Schedule Ingestion (Daily)
│   ├── GTFS Realtime Ingestion (Every 20s)
│   ├── PTV API Ingestion (On-demand)
│   └── Data.vic Metadata (Weekly)
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

## 📋 Key Features

### ✅ Production Ready
- **Status**: Fully operational ETL pipeline
- **Performance**: 7-15 minutes end-to-end execution
- **Reliability**: Comprehensive error handling and retry logic
- **Monitoring**: Full observability and debugging tools

### 🚂 Real Platform Numbers
- **Source**: GTFS Schedule data with actual platform numbers
- **Coverage**: Complete Victorian train network
- **Usage**: Powers accurate departure analytics

### 📊 Disruption Risk Index (DRI)
- **Algorithm**: Multi-factor risk scoring (0-100 scale)
- **Components**: Service status, disruptions, delays, route importance
- **Application**: Operational planning and resource allocation

### 🔄 Real-time Integration
- **GTFS Realtime**: Live updates every 20 seconds
- **PTV API**: Real-time disruptions and service alerts
- **Data Quality**: Robust validation and error handling

---

## 🔧 Technical Implementation

### Modular Architecture
```
notebooks/
├── data_ingestion_01.py          # Main ingestion orchestrator
├── data_transformation_02.py     # Main transformation pipeline
├── ingestion/                    # Modular ingestion components
│   ├── gtfs_schedule_ingestion.py
│   ├── gtfs_realtime_ingestion.py
│   ├── ptv_ingestion.py
│   └── datavic_ingestion.py
└── transformations/              # Modular transformation components
    ├── routes_transformation.py
    ├── disruptions_transformation.py
    ├── departures_transformation.py
    └── dri_calculation.py
```

### Data Processing
- **Input**: Raw transport data from 4 validated APIs
- **Processing**: Medallion architecture with Bronze → Silver → Gold layers
- **Output**: Analytics-ready Parquet datasets
- **Volume**: ~50-100MB raw → ~20-40MB curated per run

### Integration Points
- **Power BI**: Direct connection to curated Parquet files
- **Scheduling**: Cron jobs or orchestration tools
- **Monitoring**: Comprehensive logging and error tracking

---

## � Maintenance & Support

### Regular Updates
- **Weekly**: Review data quality metrics and API status
- **Monthly**: Performance optimization and capacity planning
- **Quarterly**: Documentation updates and feature enhancements

### Support Resources
- **Debug Tools**: Located in `tests/` directory
- **Configuration**: Environment variables in `config/.env`
- **Logs**: Structured logging with full error context

### Contributing
- Update documentation when making pipeline changes
- Use clear, concise language for technical documentation
- Include practical examples and troubleshooting guides
- Maintain consistency with existing documentation structure

---

## 📞 Getting Help

### For Technical Issues
1. **Pipeline Problems**: Check [ETL Pipeline Documentation](./etl_pipeline.md)
2. **API Issues**: Review [API Reference](./api_reference.md)
3. **Dashboard Issues**: Use [Power BI DAX Measures](./power_bi_dax_measures.md)
4. **Debug Tools**: Run tests in `tests/` directory

### For Business Questions
1. **Project Overview**: Main [README](../README.md)
2. **Data Understanding**: [API Reference](./api_reference.md)
3. **Analytics Capabilities**: [Power BI DAX Measures](./power_bi_dax_measures.md)

---

**Last Updated**: July 13, 2025  
**Maintained By**: Data Engineering Team  
**Next Review**: August 13, 2025
