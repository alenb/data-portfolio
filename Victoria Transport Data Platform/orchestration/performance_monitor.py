"""
Performance Monitoring Module for Data Ingestion Pipeline
Tracks execution metrics, identifies bottlenecks, and provides optimization insights
"""

import json
import logging
import psutil
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, List, Optional
import threading
from collections import defaultdict, deque

logger = logging.getLogger(__name__)


class PerformanceMonitor:
    """
    Comprehensive performance monitoring system that tracks execution metrics,
    identifies bottlenecks, and provides optimization recommendations.
    """
    
    def __init__(self, config: Dict, run_data_path: Path):
        """
        Initialize performance monitor
        
        Args:
            config: Performance monitoring configuration
            run_data_path: Directory to store performance data
        """
        self.config = config
        self.run_data_path = run_data_path
        self.metrics_enabled = config.get("metrics", {})
        self.thresholds = config.get("thresholds", {})
        
        # Performance data storage
        self.performance_data = {
            "run_start": datetime.now().isoformat(),
            "components": {},
            "system_metrics": [],
            "alerts": []
        }
        
        # Component tracking
        self.component_timers = {}
        self.component_metrics = {}
        
        # System monitoring
        self.system_monitoring_active = False
        self.system_metrics_thread = None
        
        # Rolling averages for adaptive insights
        self.historical_metrics = defaultdict(lambda: deque(maxlen=100))
        
        logger.info("📊 Performance monitor initialized")
    
    def start_component_monitoring(self, component_name: str):
        """Start monitoring a component"""
        start_time = time.time()
        
        self.component_timers[component_name] = {
            "start_time": start_time,
            "start_timestamp": datetime.now().isoformat()
        }
        
        self.component_metrics[component_name] = {
            "memory_samples": [],
            "cpu_samples": [],
            "api_calls": [],
            "data_points": []
        }
        
        # Start system monitoring if not already active
        if not self.system_monitoring_active:
            self._start_system_monitoring()
        
        logger.debug(f"📊 Started monitoring {component_name}")
    
    def stop_component_monitoring(self, component_name: str, result: Dict):
        """Stop monitoring a component and record results"""
        if component_name not in self.component_timers:
            logger.warning(f"Component {component_name} was not being monitored")
            return
        
        end_time = time.time()
        start_time = self.component_timers[component_name]["start_time"]
        execution_time = end_time - start_time
        
        # Collect final metrics
        component_metrics = self.component_metrics[component_name]
        
        # Calculate performance metrics
        performance_summary = {
            "execution_time_seconds": execution_time,
            "start_timestamp": self.component_timers[component_name]["start_timestamp"],
            "end_timestamp": datetime.now().isoformat(),
            "datasets_processed": len(result.get("datasets_ingested", [])),
            "records_processed": result.get("total_datasets", 0),
            "errors": len(result.get("errors", [])),
            "success_rate": 1.0 if not result.get("errors") else 0.0
        }
        
        # Add memory metrics if available
        if component_metrics["memory_samples"]:
            performance_summary["memory_peak_mb"] = max(component_metrics["memory_samples"])
            performance_summary["memory_avg_mb"] = sum(component_metrics["memory_samples"]) / len(component_metrics["memory_samples"])
        
        # Add CPU metrics if available
        if component_metrics["cpu_samples"]:
            performance_summary["cpu_avg_percent"] = sum(component_metrics["cpu_samples"]) / len(component_metrics["cpu_samples"])
            performance_summary["cpu_peak_percent"] = max(component_metrics["cpu_samples"])
        
        # Calculate data throughput
        if execution_time > 0 and performance_summary["records_processed"] > 0:
            performance_summary["records_per_second"] = performance_summary["records_processed"] / execution_time
        
        # API performance metrics
        if component_metrics["api_calls"]:
            api_times = [call["duration"] for call in component_metrics["api_calls"]]
            performance_summary["api_calls_count"] = len(api_times)
            performance_summary["api_avg_response_ms"] = sum(api_times) / len(api_times)
            performance_summary["api_max_response_ms"] = max(api_times)
        
        # Store performance data
        self.performance_data["components"][component_name] = performance_summary
        
        # Check thresholds and generate alerts
        self._check_performance_thresholds(component_name, performance_summary)
        
        # Update historical metrics for adaptive insights
        self._update_historical_metrics(component_name, performance_summary)
        
        # Cleanup
        del self.component_timers[component_name]
        del self.component_metrics[component_name]
        
        logger.info(f"📊 Completed monitoring {component_name} - {execution_time:.2f}s execution time")
    
    def record_api_call(self, component_name: str, url: str, duration_ms: float, status_code: int):
        """Record an API call for performance tracking"""
        if component_name in self.component_metrics:
            self.component_metrics[component_name]["api_calls"].append({
                "url": url,
                "duration": duration_ms,
                "status_code": status_code,
                "timestamp": datetime.now().isoformat()
            })
    
    def record_data_point(self, component_name: str, metric_name: str, value: float):
        """Record a custom data point"""
        if component_name in self.component_metrics:
            self.component_metrics[component_name]["data_points"].append({
                "metric": metric_name,
                "value": value,
                "timestamp": datetime.now().isoformat()
            })
    
    def _start_system_monitoring(self):
        """Start system-wide monitoring thread"""
        if self.system_monitoring_active:
            return
        
        self.system_monitoring_active = True
        self.system_metrics_thread = threading.Thread(target=self._system_monitoring_loop)
        self.system_metrics_thread.daemon = True
        self.system_metrics_thread.start()
        
        logger.debug("🖥️ Started system monitoring thread")
    
    def _system_monitoring_loop(self):
        """System monitoring loop that runs in background"""
        while self.system_monitoring_active:
            try:
                # Collect system metrics
                system_metric = {
                    "timestamp": datetime.now().isoformat(),
                    "cpu_percent": psutil.cpu_percent(),
                    "memory_percent": psutil.virtual_memory().percent,
                    "memory_available_mb": psutil.virtual_memory().available / (1024 * 1024),
                    "disk_usage_percent": psutil.disk_usage('/').percent
                }
                
                self.performance_data["system_metrics"].append(system_metric)
                
                # Update component-specific metrics
                for component_name in self.component_metrics:
                    self.component_metrics[component_name]["memory_samples"].append(system_metric["memory_percent"])
                    self.component_metrics[component_name]["cpu_samples"].append(system_metric["cpu_percent"])
                
                # Keep only recent system metrics (last 1000 samples)
                if len(self.performance_data["system_metrics"]) > 1000:
                    self.performance_data["system_metrics"] = self.performance_data["system_metrics"][-1000:]
                
                time.sleep(5)  # Sample every 5 seconds
                
            except Exception as e:
                logger.error(f"System monitoring error: {e}")
                time.sleep(10)  # Wait longer on error
    
    def _check_performance_thresholds(self, component_name: str, metrics: Dict):
        """Check if performance metrics exceed thresholds"""
        alerts = []
        
        # Execution time threshold
        max_execution_time = self.thresholds.get("max_execution_time_minutes", 60) * 60
        if metrics["execution_time_seconds"] > max_execution_time:
            alerts.append({
                "component": component_name,
                "type": "execution_time",
                "message": f"Execution time {metrics['execution_time_seconds']:.1f}s exceeds threshold {max_execution_time}s",
                "severity": "warning",
                "timestamp": datetime.now().isoformat()
            })
        
        # Memory threshold
        max_memory_mb = self.thresholds.get("max_memory_mb", 1024)
        if metrics.get("memory_peak_mb", 0) > max_memory_mb:
            alerts.append({
                "component": component_name,
                "type": "memory_usage",
                "message": f"Peak memory {metrics['memory_peak_mb']:.1f}MB exceeds threshold {max_memory_mb}MB",
                "severity": "warning",
                "timestamp": datetime.now().isoformat()
            })
        
        # API response time threshold
        max_api_response_ms = self.thresholds.get("max_api_response_ms", 30000)
        if metrics.get("api_max_response_ms", 0) > max_api_response_ms:
            alerts.append({
                "component": component_name,
                "type": "api_response_time",
                "message": f"Max API response time {metrics['api_max_response_ms']:.1f}ms exceeds threshold {max_api_response_ms}ms",
                "severity": "warning",
                "timestamp": datetime.now().isoformat()
            })
        
        # Success rate threshold
        min_success_rate = self.thresholds.get("min_success_rate_percentage", 95) / 100
        if metrics["success_rate"] < min_success_rate:
            alerts.append({
                "component": component_name,
                "type": "success_rate",
                "message": f"Success rate {metrics['success_rate']*100:.1f}% below threshold {min_success_rate*100:.1f}%",
                "severity": "error",
                "timestamp": datetime.now().isoformat()
            })
        
        # Add alerts to performance data
        self.performance_data["alerts"].extend(alerts)
        
        # Log alerts
        for alert in alerts:
            if alert["severity"] == "error":
                logger.error(f"🚨 {alert['message']}")
            else:
                logger.warning(f"⚠️ {alert['message']}")
    
    def _update_historical_metrics(self, component_name: str, metrics: Dict):
        """Update historical metrics for trend analysis"""
        key_metrics = ["execution_time_seconds", "memory_peak_mb", "api_avg_response_ms", "records_per_second"]
        
        for metric in key_metrics:
            if metric in metrics:
                self.historical_metrics[f"{component_name}_{metric}"].append(metrics[metric])
    
    def get_performance_insights(self) -> Dict:
        """Generate performance insights and recommendations"""
        insights = {
            "summary": {},
            "recommendations": [],
            "trends": {}
        }
        
        # Calculate overall summary
        components = self.performance_data["components"]
        if components:
            total_execution_time = sum(comp["execution_time_seconds"] for comp in components.values())
            total_records = sum(comp["records_processed"] for comp in components.values())
            avg_success_rate = sum(comp["success_rate"] for comp in components.values()) / len(components)
            
            insights["summary"] = {
                "total_components": len(components),
                "total_execution_time_seconds": total_execution_time,
                "total_records_processed": total_records,
                "average_success_rate": avg_success_rate,
                "alerts_count": len(self.performance_data["alerts"])
            }
        
        # Generate recommendations
        recommendations = []
        
        # Slow components
        slow_components = [
            name for name, metrics in components.items()
            if metrics["execution_time_seconds"] > 60  # Components taking more than 1 minute
        ]
        if slow_components:
            recommendations.append({
                "type": "performance",
                "message": f"Components with slow execution: {', '.join(slow_components)}",
                "action": "Consider optimizing API calls or data processing logic"
            })
        
        # Memory intensive components
        memory_intensive = [
            name for name, metrics in components.items()
            if metrics.get("memory_peak_mb", 0) > 500  # Components using more than 500MB
        ]
        if memory_intensive:
            recommendations.append({
                "type": "memory",
                "message": f"Memory intensive components: {', '.join(memory_intensive)}",
                "action": "Consider streaming or batching data processing"
            })
        
        # Components with errors
        error_components = [
            name for name, metrics in components.items()
            if metrics["success_rate"] < 1.0
        ]
        if error_components:
            recommendations.append({
                "type": "reliability",
                "message": f"Components with errors: {', '.join(error_components)}",
                "action": "Review error handling and retry logic"
            })
        
        insights["recommendations"] = recommendations
        
        # Calculate trends from historical data
        for metric_key, values in self.historical_metrics.items():
            if len(values) >= 2:
                recent_avg = sum(list(values)[-5:]) / min(5, len(values))
                older_avg = sum(list(values)[:5]) / min(5, len(values))
                
                if older_avg > 0:
                    trend_percentage = ((recent_avg - older_avg) / older_avg) * 100
                    insights["trends"][metric_key] = {
                        "trend_percentage": trend_percentage,
                        "direction": "improving" if trend_percentage < 0 else "degrading"
                    }
        
        return insights
    
    def finalize_monitoring(self):
        """Finalize monitoring and save performance data"""
        # Stop system monitoring
        self.system_monitoring_active = False
        if self.system_metrics_thread:
            self.system_metrics_thread.join(timeout=5)
        
        # Add final timestamp
        self.performance_data["run_end"] = datetime.now().isoformat()
        
        # Save performance data
        performance_file = self.run_data_path / f"performance_metrics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        try:
            with open(performance_file, 'w') as f:
                json.dump(self.performance_data, f, indent=2, default=str)
            
            logger.info(f"📊 Performance data saved to {performance_file}")
            
        except Exception as e:
            logger.error(f"Failed to save performance data: {e}")
        
        # Log performance summary
        insights = self.get_performance_insights()
        if insights["summary"]:
            logger.info(f"📊 Performance Summary: {insights['summary']['total_components']} components, "
                       f"{insights['summary']['total_execution_time_seconds']:.1f}s total time, "
                       f"{insights['summary']['average_success_rate']*100:.1f}% success rate")
        
        return performance_file
    
    def get_real_time_metrics(self) -> Dict:
        """Get current real-time metrics"""
        current_time = datetime.now().isoformat()
        
        # Get latest system metrics
        latest_system = self.performance_data["system_metrics"][-1] if self.performance_data["system_metrics"] else {}
        
        # Get active components
        active_components = list(self.component_timers.keys())
        
        return {
            "timestamp": current_time,
            "system_metrics": latest_system,
            "active_components": active_components,
            "alerts_count": len(self.performance_data["alerts"]),
            "total_components_completed": len(self.performance_data["components"])
        }
