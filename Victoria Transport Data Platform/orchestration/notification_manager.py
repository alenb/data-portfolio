"""
Notification Manager for Data Ingestion Pipeline
Handles alerts, notifications, and performance threshold monitoring
"""

import logging
import json
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from typing import Dict, List, Optional
import requests

logger = logging.getLogger(__name__)


class NotificationManager:
    """
    Manages notifications for the data ingestion pipeline.
    Supports email notifications, Slack webhooks, and performance monitoring alerts.
    """
    
    def __init__(self, config: Dict):
        """
        Initialize notification manager
        
        Args:
            config: Orchestration configuration dictionary
        """
        self.config = config
        self.runtime_settings = config.get("runtime_settings", {})
        self.performance_config = config.get("performance_monitoring", {})
        self.notification_enabled = self.runtime_settings.get("enable_notifications", False)
        
        # Performance thresholds
        self.thresholds = self.performance_config.get("thresholds", {})
        
        # Alerting configuration
        self.alerting_config = self.performance_config.get("alerting", {})
        self.alerting_enabled = self.alerting_config.get("enabled", False)
        
        logger.info(f"📧 Notification system initialized: enabled={self.notification_enabled}")
        
    def send_execution_summary(self, execution_summary: Dict):
        """Send execution summary notification"""
        if not self.notification_enabled:
            return
        
        # Prepare notification content
        status = execution_summary.get("status", "unknown")
        failed_components = execution_summary.get("failed_components", [])
        successful_components = execution_summary.get("successful_components", [])
        
        subject = f"Data Ingestion Pipeline - {status.upper()}"
        
        # Create detailed message
        message = self._create_execution_summary_message(execution_summary)
        
        # Send notifications
        self._send_email_notification(subject, message)
        self._send_slack_notification(subject, message)
        
        # Send performance alerts if thresholds exceeded
        if self.alerting_enabled:
            self._check_performance_thresholds(execution_summary)
    
    def send_component_failure_alert(self, component_name: str, error_message: str):
        """Send immediate alert for component failure"""
        if not self.notification_enabled:
            return
        
        subject = f"URGENT: Component {component_name} Failed"
        message = f"""
Component Failure Alert
=====================

Component: {component_name}
Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Error: {error_message}

This component has failed during execution. Please investigate immediately.
"""
        
        self._send_email_notification(subject, message)
        self._send_slack_notification(subject, message, urgent=True)
    
    def send_performance_alert(self, metric_name: str, current_value: float, threshold: float, component_name: str = None):
        """Send performance threshold alert"""
        if not self.alerting_enabled:
            return
        
        subject = f"Performance Alert: {metric_name} threshold exceeded"
        
        component_info = f" for component {component_name}" if component_name else ""
        message = f"""
Performance Alert
================

Metric: {metric_name}
Current Value: {current_value}
Threshold: {threshold}
Component: {component_name or 'Global'}
Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

Performance threshold has been exceeded{component_info}. Please investigate.
"""
        
        self._send_email_notification(subject, message)
        self._send_slack_notification(subject, message, urgent=True)
    
    def _create_execution_summary_message(self, execution_summary: Dict) -> str:
        """Create detailed execution summary message"""
        status = execution_summary.get("status", "unknown")
        successful_components = execution_summary.get("successful_components", [])
        failed_components = execution_summary.get("failed_components", [])
        skipped_components = execution_summary.get("skipped_components", [])
        
        message = f"""
Data Ingestion Pipeline Summary
==============================

Status: {status.upper()}
Execution Time: {execution_summary.get('start_time', 'N/A')} - {execution_summary.get('end_time', 'N/A')}

Components Summary:
✅ Successful: {len(successful_components)} components
❌ Failed: {len(failed_components)} components  
⏭️ Skipped: {len(skipped_components)} components

"""
        
        if successful_components:
            message += f"Successful Components:\n"
            for component in successful_components:
                message += f"  • {component}\n"
            message += "\n"
        
        if failed_components:
            message += f"Failed Components:\n"
            for component in failed_components:
                message += f"  • {component}\n"
            message += "\n"
        
        if skipped_components:
            message += f"Skipped Components:\n"
            for component in skipped_components:
                message += f"  • {component}\n"
            message += "\n"
        
        # Add performance metrics if available
        performance_data = execution_summary.get("performance_data", {})
        if performance_data:
            message += "Performance Metrics:\n"
            for metric, value in performance_data.items():
                message += f"  • {metric}: {value}\n"
        
        return message
    
    def _send_email_notification(self, subject: str, message: str):
        """Send email notification"""
        try:
            email_list = self.alerting_config.get("email_notifications", [])
            if not email_list:
                return
            
            # This is a basic implementation - in production, you'd configure SMTP settings
            logger.info(f"📧 Would send email notification to {len(email_list)} recipients")
            logger.info(f"📧 Subject: {subject}")
            logger.debug(f"📧 Message: {message}")
            
            # TODO: Implement actual email sending with proper SMTP configuration
            # For now, just log the notification
            
        except Exception as e:
            logger.error(f"Failed to send email notification: {e}")
    
    def _send_slack_notification(self, subject: str, message: str, urgent: bool = False):
        """Send Slack notification"""
        try:
            webhook_url = self.alerting_config.get("slack_webhook")
            if not webhook_url:
                return
            
            # Format message for Slack
            color = "danger" if urgent else "good"
            slack_message = {
                "text": subject,
                "attachments": [
                    {
                        "color": color,
                        "fields": [
                            {
                                "title": "Details",
                                "value": message,
                                "short": False
                            }
                        ],
                        "footer": "Resilient Public Services Platform",
                        "ts": int(datetime.now().timestamp())
                    }
                ]
            }
            
            # Send to Slack
            response = requests.post(webhook_url, json=slack_message, timeout=10)
            if response.status_code == 200:
                logger.info(f"📱 Slack notification sent successfully")
            else:
                logger.error(f"Failed to send Slack notification: {response.status_code}")
                
        except Exception as e:
            logger.error(f"Failed to send Slack notification: {e}")
    
    def _check_performance_thresholds(self, execution_summary: Dict):
        """Check performance thresholds and send alerts"""
        performance_data = execution_summary.get("performance_data", {})
        
        # Check execution time threshold
        execution_time = performance_data.get("total_execution_time_minutes", 0)
        max_execution_time = self.thresholds.get("max_execution_time_minutes", 60)
        
        if execution_time > max_execution_time:
            self.send_performance_alert("Execution Time", execution_time, max_execution_time)
        
        # Check memory usage threshold
        memory_usage = performance_data.get("max_memory_mb", 0)
        max_memory = self.thresholds.get("max_memory_mb", 1024)
        
        if memory_usage > max_memory:
            self.send_performance_alert("Memory Usage", memory_usage, max_memory)
        
        # Check success rate threshold
        successful_count = len(execution_summary.get("successful_components", []))
        total_count = successful_count + len(execution_summary.get("failed_components", []))
        
        if total_count > 0:
            success_rate = (successful_count / total_count) * 100
            min_success_rate = self.thresholds.get("min_success_rate_percentage", 95)
            
            if success_rate < min_success_rate:
                self.send_performance_alert("Success Rate", success_rate, min_success_rate)
