"""
Dependency Management for Data Ingestion Pipeline
Handles component execution ordering, dependency checking, and skip logic
"""

import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Set, Optional
import json
import threading
import signal
import time
from .change_detection import ChangeDetector, DataQualityValidator
from .performance_monitor import PerformanceMonitor
from .notification_manager import NotificationManager

logger = logging.getLogger(__name__)


class DependencyManager:
    """
    Manages component dependencies and execution ordering for the data ingestion pipeline.
    Provides intelligent scheduling that respects dependencies and skips unchanged components.
    """
    
    def __init__(self, config: Dict, run_data_path: Path, app_config=None):
        """
        Initialize dependency manager with orchestration configuration
        
        Args:
            config: Orchestration configuration dictionary
            run_data_path: Path where execution tracking files are stored
            app_config: Application configuration object (Config class instance)
        """
        self.config = config
        self.run_data_path = run_data_path
        self.dependency_config = config.get("dependency_management", {})
        self.runtime_settings = config.get("runtime_settings", {})
        self.app_config = app_config  # Store application config for endpoint URLs
        
        # Set execution log path early
        self.execution_log_path = run_data_path / "execution_tracking.json"
        
        # Load previous execution history
        self.execution_history = self._load_execution_history()
        
        # Track current execution state
        self.current_execution = {
            "timestamp": datetime.now().isoformat(),
            "components": {},
            "execution_order": [],
            "skipped_components": [],
            "failed_components": []
        }
        
        # Component execution tracking for timeouts and concurrency
        self.component_start_times = {}
        self.component_threads = {}
        self.execution_start_time = None
        self.global_timeout_seconds = self.runtime_settings.get("execution_timeout_minutes", 120) * 60
        
        # Phase 3: Initialize advanced features
        self._initialize_phase3_features()
        
    def _initialize_phase3_features(self):
        """Initialize Phase 3 advanced features"""
        # Change detection
        change_config = self.config.get("change_detection", {})
        if change_config.get("enabled", True):
            cache_dir = self.run_data_path.parent / change_config.get("cache_directory", "cache/change_detection")
            self.change_detector = ChangeDetector(change_config, cache_dir)
            logger.info("🔍 Change detection enabled")
        else:
            self.change_detector = None
        
        # Data quality validation
        quality_config = self.config.get("data_quality", {})
        if quality_config.get("enabled", True):
            self.data_quality_validator = DataQualityValidator(quality_config)
            logger.info("✅ Data quality validation enabled")
        else:
            self.data_quality_validator = None
        
        # Performance monitoring
        perf_config = self.config.get("performance_monitoring", {})
        if perf_config.get("enabled", True):
            self.performance_monitor = PerformanceMonitor(perf_config, self.run_data_path)
            logger.info("📊 Performance monitoring enabled")
        else:
            self.performance_monitor = None
        
        # Adaptive scheduling
        adaptive_config = self.config.get("adaptive_scheduling", {})
        self.adaptive_scheduling_enabled = adaptive_config.get("enabled", False)
        if self.adaptive_scheduling_enabled:
            logger.info("🧠 Adaptive scheduling enabled")
        
        # Notification manager
        self.notification_manager = NotificationManager(self.config)
        logger.info("📧 Notification manager initialized")
    
    def has_data_changed(self, component_name: str) -> bool:
        """Check if data has changed using change detection"""
        if not self.change_detector:
            return True  # No change detection, assume changed
        
        if self.config.get("override_settings", {}).get("skip_change_detection", False):
            return True  # Skip change detection override
        
        # Get data source configuration for the component
        # This would typically come from component configuration
        data_source = self._get_data_source_config(component_name)
        
        return self.change_detector.has_changed(component_name, data_source)
    
    def _get_data_source_config(self, component_name: str) -> Dict:
        """Get data source configuration for change detection"""
        # Use application config for endpoints if available
        if not self.app_config:
            return {"url": None, "type": "unknown"}
        
        # Map component names to their corresponding configuration endpoints
        component_data_sources = {
            "gtfs_schedule": {
                "url": f"{self.app_config.DATA_VIC_DATASET_BASE_URL}/{self.app_config.GTFS_SCHEDULE_DATASET_ID}", 
                "type": "gtfs"
            },
            "gtfs_realtime": {
                "url": self.app_config.GTFS_REALTIME_BASE_URL, 
                "type": "gtfs_rt"
            },
            "ptv_timetable": {
                "url": self.app_config.PTV_BASE_URL, 
                "type": "api"
            },
            "datavic_discovery": {
                "url": self.app_config.DATA_VIC_PACKAGE_SEARCH_URL, 
                "type": "api"
            },
            "datavic_station_patronage": {
                "url": self.app_config.DATA_VIC_DATASTORE_SEARCH_URL, 
                "type": "api"
            },
            "datavic_temporal_patronage": {
                "url": self.app_config.DATA_VIC_DATASTORE_SEARCH_URL, 
                "type": "api"
            }
        }
        
        return component_data_sources.get(component_name, {"url": None, "type": "unknown"})
    
    def validate_data_quality(self, component_name: str, data: any) -> Dict:
        """Validate data quality"""
        if not self.data_quality_validator:
            return {"passed": True, "component": component_name, "issues": []}
        
        if self.config.get("override_settings", {}).get("skip_data_quality", False):
            return {"passed": True, "component": component_name, "issues": []}
        
        return self.data_quality_validator.validate_data(data, component_name)
    
    def start_performance_monitoring(self, component_name: str):
        """Start performance monitoring for a component"""
        if self.performance_monitor:
            self.performance_monitor.start_component_monitoring(component_name)
    
    def stop_performance_monitoring(self, component_name: str, result: Dict):
        """Stop performance monitoring for a component"""
        if self.performance_monitor:
            self.performance_monitor.stop_component_monitoring(component_name, result)
            
            # Check performance thresholds and send alerts if exceeded
            self._check_component_performance_thresholds(component_name, result)
    
    def get_performance_insights(self) -> Dict:
        """Get performance insights"""
        if self.performance_monitor:
            return self.performance_monitor.get_performance_insights()
        return {}
    
    def _check_component_performance_thresholds(self, component_name: str, result: Dict):
        """Check component performance thresholds and send alerts"""
        if not self.performance_monitor:
            return
        
        thresholds = self.config.get("performance_monitoring", {}).get("thresholds", {})
        
        # Get performance data for the component
        performance_data = self.performance_monitor.get_component_performance(component_name)
        if not performance_data:
            return
        
        # Check execution time threshold
        execution_time = performance_data.get("execution_time_minutes", 0)
        max_execution_time = thresholds.get("max_execution_time_minutes", 60)
        
        if execution_time > max_execution_time:
            self.notification_manager.send_performance_alert(
                "Component Execution Time", 
                execution_time, 
                max_execution_time, 
                component_name
            )
        
        # Check memory usage threshold
        memory_usage = performance_data.get("memory_usage_mb", 0)
        max_memory = thresholds.get("max_memory_mb", 1024)
        
        if memory_usage > max_memory:
            self.notification_manager.send_performance_alert(
                "Component Memory Usage", 
                memory_usage, 
                max_memory, 
                component_name
            )
        
        # Check API response time threshold
        api_response_time = performance_data.get("api_response_time_ms", 0)
        max_api_response = thresholds.get("max_api_response_ms", 30000)
        
        if api_response_time > max_api_response:
            self.notification_manager.send_performance_alert(
                "Component API Response Time", 
                api_response_time, 
                max_api_response, 
                component_name
            )
    
    def start_global_timeout(self):
        """Start global execution timeout"""
        self.execution_start_time = datetime.now()
        logger.info(f"⏱️ Global execution timeout set to {self.global_timeout_seconds/60:.1f} minutes")
    
    def check_global_timeout(self) -> bool:
        """Check if global execution timeout has been exceeded"""
        if not self.execution_start_time:
            return False
        
        elapsed_time = (datetime.now() - self.execution_start_time).total_seconds()
        if elapsed_time > self.global_timeout_seconds:
            logger.error(f"❌ Global execution timeout exceeded ({elapsed_time/60:.1f} minutes)")
            return True
        return False
    
    def start_component_timeout(self, component_name: str):
        """Start timeout tracking for a component"""
        self.component_start_times[component_name] = datetime.now()
        component_config = self.config.get("components", {}).get(component_name, {})
        max_runtime = component_config.get("max_runtime_minutes", 30)
        logger.info(f"⏱️ Component {component_name} timeout set to {max_runtime} minutes")
    
    def check_component_timeout(self, component_name: str) -> bool:
        """Check if component timeout has been exceeded"""
        if component_name not in self.component_start_times:
            return False
        
        start_time = self.component_start_times[component_name]
        elapsed_time = (datetime.now() - start_time).total_seconds()
        
        component_config = self.config.get("components", {}).get(component_name, {})
        max_runtime_seconds = component_config.get("max_runtime_minutes", 30) * 60
        
        if elapsed_time > max_runtime_seconds:
            logger.error(f"❌ Component {component_name} timeout exceeded ({elapsed_time/60:.1f} minutes)")
            return True
        return False
    
    def stop_component_timeout(self, component_name: str):
        """Stop timeout tracking for a component"""
        if component_name in self.component_start_times:
            start_time = self.component_start_times[component_name]
            elapsed_time = (datetime.now() - start_time).total_seconds()
            logger.info(f"⏱️ Component {component_name} completed in {elapsed_time/60:.1f} minutes")
            del self.component_start_times[component_name]
        
    def _load_execution_history(self) -> Dict:
        """Load execution history from previous runs"""
        try:
            if self.execution_log_path.exists():
                with open(self.execution_log_path, 'r') as f:
                    history = json.load(f)
                    logger.info(f"📊 Loaded execution history with {len(history.get('runs', []))} previous runs")
                    return history
        except Exception as e:
            logger.warning(f"Could not load execution history: {e}")
        
        return {"runs": [], "last_successful_run": None}
    
    def _save_execution_history(self):
        """Save current execution to history"""
        try:
            # Add current execution to history
            self.execution_history["runs"].append(self.current_execution)
            
            # Keep only last 50 runs to avoid file bloat
            if len(self.execution_history["runs"]) > 50:
                self.execution_history["runs"] = self.execution_history["runs"][-50:]
            
            # Update last successful run if current run was successful
            if not self.current_execution["failed_components"]:
                self.execution_history["last_successful_run"] = self.current_execution["timestamp"]
            
            # Save to file
            with open(self.execution_log_path, 'w') as f:
                json.dump(self.execution_history, f, indent=2, default=str)
            
            logger.info("💾 Saved execution history")
            
        except Exception as e:
            logger.error(f"Could not save execution history: {e}")
    
    def get_execution_order(self) -> List[str]:
        """
        Calculate optimal execution order based on dependencies
        
        Returns:
            List of component names in execution order
        """
        # Get all enabled components
        enabled_components = [
            name for name, config in self.config.get("components", {}).items()
            if config.get("enabled", True)
        ]
        
        # Use configured execution order if available
        configured_order = self.dependency_config.get("execution_order", [])
        if configured_order:
            # Filter to only include enabled components
            execution_order = [comp for comp in configured_order if comp in enabled_components]
            
            # Add any enabled components not in configured order
            for comp in enabled_components:
                if comp not in execution_order:
                    execution_order.append(comp)
            
            return execution_order
        
        # Fallback: topological sort based on dependencies
        return self._topological_sort(enabled_components)
    
    def _topological_sort(self, components: List[str]) -> List[str]:
        """
        Perform topological sort based on component dependencies
        
        Args:
            components: List of component names to sort
            
        Returns:
            List of components in dependency order
        """
        # Build dependency graph
        graph = {}
        in_degree = {}
        
        for comp in components:
            graph[comp] = []
            in_degree[comp] = 0
        
        # Add edges based on dependencies
        for comp in components:
            comp_config = self.config.get("components", {}).get(comp, {})
            dependencies = comp_config.get("dependencies", [])
            
            for dep in dependencies:
                if dep in components:
                    graph[dep].append(comp)
                    in_degree[comp] += 1
        
        # Topological sort using Kahn's algorithm
        queue = [comp for comp in components if in_degree[comp] == 0]
        result = []
        
        while queue:
            current = queue.pop(0)
            result.append(current)
            
            for neighbor in graph[current]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)
        
        if len(result) != len(components):
            logger.warning("Circular dependency detected, using priority-based ordering")
            # Fallback to priority-based ordering
            return sorted(components, key=lambda x: self.config.get("components", {}).get(x, {}).get("priority", 999))
        
        return result
    
    def should_run_component(self, component_name: str) -> bool:
        """
        Determine if a component should run based on dependencies and change detection
        Enhanced with Phase 3 features: change detection, adaptive scheduling
        
        Args:
            component_name: Name of the component to check
            
        Returns:
            True if component should run, False otherwise
        """
        component_config = self.config.get("components", {}).get(component_name, {})
        
        # Check if component is enabled
        if not component_config.get("enabled", True):
            logger.info(f"⏭️ Skipping {component_name} - disabled in configuration")
            return False
        
        # Check override settings
        override_settings = self.config.get("override_settings", {})
        if override_settings.get("force_run_all", False):
            logger.info(f"🔄 Force running {component_name} - override enabled")
            return True
        
        # Check if dependency checking is enabled
        if not self.runtime_settings.get("dependency_check_enabled", True):
            return True
        
        # Check if dependencies are satisfied
        if not self._check_dependencies_satisfied(component_name):
            logger.warning(f"❌ Skipping {component_name} - dependencies not satisfied")
            return False
        
        # Phase 3: Check if data has changed (change detection)
        if self.runtime_settings.get("change_detection_enabled", True):
            if not self.has_data_changed(component_name):
                logger.info(f"🔍 Skipping {component_name} - no data changes detected")
                self.current_execution["skipped_components"].append(component_name)
                return False
        
        # Check if component should be skipped due to unchanged dependencies
        if self._should_skip_due_to_unchanged_dependencies(component_name):
            logger.info(f"⏭️ Skipping {component_name} - dependencies unchanged")
            self.current_execution["skipped_components"].append(component_name)
            return False
        
        # Phase 3: Adaptive scheduling check
        if self.adaptive_scheduling_enabled:
            if not self._should_run_adaptive_scheduling(component_name):
                logger.info(f"🧠 Skipping {component_name} - adaptive scheduling optimization")
                self.current_execution["skipped_components"].append(component_name)
                return False
        
        return True
    
    def _should_run_adaptive_scheduling(self, component_name: str) -> bool:
        """Check if component should run based on adaptive scheduling"""
        # This is a simplified implementation of adaptive scheduling
        # In a full implementation, this would analyze historical patterns,
        # system load, and optimal execution times
        
        adaptive_config = self.config.get("adaptive_scheduling", {})
        
        # Check if we're in peak hours (avoid heavy processing)
        current_time = datetime.now().time()
        peak_start = datetime.strptime(adaptive_config.get("peak_hours", {}).get("start", "07:00"), "%H:%M").time()
        peak_end = datetime.strptime(adaptive_config.get("peak_hours", {}).get("end", "09:00"), "%H:%M").time()
        
        # Get component runtime estimate
        component_config = self.config.get("components", {}).get(component_name, {})
        max_runtime_minutes = component_config.get("max_runtime_minutes", 30)
        
        # Skip heavy components during peak hours
        if peak_start <= current_time <= peak_end and max_runtime_minutes > 15:
            logger.info(f"🧠 Adaptive scheduling: Deferring {component_name} during peak hours")
            return False
        
        return True
    
    def _check_dependencies_satisfied(self, component_name: str) -> bool:
        """Check if all dependencies for a component are satisfied"""
        component_config = self.config.get("components", {}).get(component_name, {})
        dependencies = component_config.get("dependencies", [])
        
        if not dependencies:
            return True
        
        # Check if all dependencies have been executed successfully in current run
        for dep in dependencies:
            if dep not in self.current_execution["components"]:
                logger.debug(f"Dependency {dep} not executed yet for {component_name}")
                return False
            
            if self.current_execution["components"][dep].get("status") != "completed":
                logger.debug(f"Dependency {dep} failed for {component_name}")
                return False
        
        return True
    
    def _should_skip_due_to_unchanged_dependencies(self, component_name: str) -> bool:
        """Check if component should be skipped due to unchanged dependencies"""
        if not self.runtime_settings.get("skip_unchanged_dependencies", True):
            return False
        
        # For Phase 2, implement basic frequency-based skipping
        # In Phase 3, we'll add more sophisticated change detection
        
        component_config = self.config.get("components", {}).get(component_name, {})
        frequency = component_config.get("frequency", "daily")
        
        # Check when component was last run successfully
        last_run = self._get_last_successful_run(component_name)
        if not last_run:
            return False  # Never run before, so run now
        
        # Calculate if enough time has passed based on frequency
        now = datetime.now()
        time_since_last_run = now - last_run
        
        frequency_thresholds = {
            "daily": timedelta(days=1),
            "weekly": timedelta(weeks=1),
            "monthly": timedelta(days=30)
        }
        
        threshold = frequency_thresholds.get(frequency, timedelta(days=1))
        
        if time_since_last_run < threshold:
            logger.info(f"⏰ {component_name} last run {time_since_last_run.total_seconds()/3600:.1f} hours ago (threshold: {threshold.total_seconds()/3600:.1f} hours)")
            return True
        
        return False
    
    def _get_last_successful_run(self, component_name: str) -> Optional[datetime]:
        """Get the timestamp of the last successful run for a component"""
        runs = self.execution_history.get("runs", [])
        
        for run in reversed(runs):  # Start from most recent
            if component_name in run.get("components", {}):
                comp_info = run["components"][component_name]
                if comp_info.get("status") == "completed":
                    return datetime.fromisoformat(comp_info["end_time"])
        
        return None
    
    def record_component_start(self, component_name: str):
        """Record that a component has started execution"""
        self.current_execution["components"][component_name] = {
            "status": "running",
            "start_time": datetime.now().isoformat(),
            "datasets_ingested": [],
            "errors": []
        }
        
        if component_name not in self.current_execution["execution_order"]:
            self.current_execution["execution_order"].append(component_name)
        
        logger.info(f"▶️ Started {component_name}")
    
    def record_component_completion(self, component_name: str, result: Dict):
        """Record that a component has completed execution"""
        if component_name in self.current_execution["components"]:
            self.current_execution["components"][component_name].update({
                "status": "completed",
                "end_time": datetime.now().isoformat(),
                "datasets_ingested": result.get("datasets_ingested", []),
                "total_datasets": result.get("total_datasets", 0),
                "errors": result.get("errors", [])
            })
        
        logger.info(f"✅ Completed {component_name} - {result.get('total_datasets', 0)} datasets")
    
    def record_component_failure(self, component_name: str, error: str):
        """Record that a component has failed execution"""
        if component_name in self.current_execution["components"]:
            self.current_execution["components"][component_name].update({
                "status": "failed",
                "end_time": datetime.now().isoformat(),
                "error": error
            })
        
        self.current_execution["failed_components"].append(component_name)
        logger.error(f"❌ Failed {component_name}: {error}")
        
        # Send immediate failure notification
        self.notification_manager.send_component_failure_alert(component_name, error)
    
    def finalize_execution(self):
        """Finalize execution tracking and save history with Phase 3 enhancements"""
        self.current_execution["end_time"] = datetime.now().isoformat()
        
        # Calculate summary statistics
        total_components = len(self.current_execution["components"])
        successful_components = sum(1 for comp in self.current_execution["components"].values() if comp.get("status") == "completed")
        
        self.current_execution["summary"] = {
            "total_components": total_components,
            "successful_components": successful_components,
            "failed_components": len(self.current_execution["failed_components"]),
            "skipped_components": len(self.current_execution["skipped_components"])
        }
        
        # Phase 3: Finalize advanced features
        if self.performance_monitor:
            performance_file = self.performance_monitor.finalize_monitoring()
            self.current_execution["performance_file"] = str(performance_file)
            
            # Add performance insights to execution summary
            performance_insights = self.get_performance_insights()
            self.current_execution["performance_insights"] = performance_insights
        
        # Save execution history
        self._save_execution_history()
        
        logger.info(f"📊 Execution complete: {successful_components}/{total_components} components successful")
        
        # Log Phase 3 insights
        if self.performance_monitor:
            insights = self.get_performance_insights()
            if insights.get("recommendations"):
                logger.info("💡 Performance recommendations:")
                for rec in insights["recommendations"]:
                    logger.info(f"   • {rec['message']}: {rec['action']}")
        
        if self.change_detector:
            cache_summary = self.change_detector.get_cache_summary()
            logger.info(f"🔍 Change detection: {cache_summary['total_components']} components monitored")
        
        # Send execution summary notification
        execution_summary = self.get_execution_summary()
        self.notification_manager.send_execution_summary(execution_summary)
    
    def get_execution_summary(self) -> Dict:
        """Get summary of current execution"""
        return {
            "execution_order": self.current_execution["execution_order"],
            "successful_components": [name for name, info in self.current_execution["components"].items() if info.get("status") == "completed"],
            "failed_components": self.current_execution["failed_components"],
            "skipped_components": self.current_execution["skipped_components"],
            "summary": self.current_execution.get("summary", {})
        }
