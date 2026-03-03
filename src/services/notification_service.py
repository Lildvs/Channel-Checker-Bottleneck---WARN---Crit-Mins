"""Notification Service for infrastructure alerts via email and Slack.

Handles operational notifications for:
- Collector failures
- API key expiration warnings
- High error rates
- System health issues

This is separate from AlertService which handles bottleneck alerts.
"""

from dataclasses import dataclass, field
from datetime import datetime, UTC
from enum import Enum
from typing import Any
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

import httpx
import structlog

logger = structlog.get_logger()


class NotificationSeverity(str, Enum):
    """Notification severity levels."""
    
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class Notification:
    """Represents an infrastructure notification."""
    
    title: str
    message: str
    severity: NotificationSeverity
    source: str
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))
    details: dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "title": self.title,
            "message": self.message,
            "severity": self.severity.value,
            "source": self.source,
            "timestamp": self.timestamp.isoformat(),
            "details": self.details,
        }


@dataclass
class NotificationConfig:
    """Configuration for notification channels."""
    
    # Slack
    slack_webhook_url: str | None = None
    slack_channel: str | None = None  # Override default channel
    
    # Email (SMTP)
    smtp_host: str | None = None
    smtp_port: int = 587
    smtp_use_tls: bool = True
    smtp_user: str | None = None
    smtp_password: str | None = None
    email_from: str | None = None
    email_to: list[str] = field(default_factory=list)
    
    # Filtering
    min_severity: NotificationSeverity = NotificationSeverity.WARNING
    
    @classmethod
    def from_settings(cls, settings) -> "NotificationConfig":
        """Create config from application settings.
        
        Args:
            settings: Application settings object
            
        Returns:
            NotificationConfig instance
        """
        smtp_password = None
        if hasattr(settings, 'smtp_password') and settings.smtp_password:
            smtp_password = settings.smtp_password.get_secret_value() if hasattr(settings.smtp_password, 'get_secret_value') else settings.smtp_password
        
        return cls(
            slack_webhook_url=getattr(settings, 'slack_webhook_url', None),
            slack_channel=getattr(settings, 'slack_channel', None),
            smtp_host=getattr(settings, 'smtp_host', None),
            smtp_port=getattr(settings, 'smtp_port', 587),
            smtp_use_tls=getattr(settings, 'smtp_use_tls', True),
            smtp_user=getattr(settings, 'smtp_user', None),
            smtp_password=smtp_password,
            email_from=getattr(settings, 'notification_email_from', None),
            email_to=getattr(settings, 'notification_email_to', []),
        )


class NotificationService:
    """Service for sending infrastructure notifications.
    
    Supports multiple channels:
    - Slack webhooks
    - Email via SMTP
    """
    
    # Slack color mapping
    SLACK_COLORS = {
        NotificationSeverity.INFO: "#36a64f",      # Green
        NotificationSeverity.WARNING: "#ffcc00",   # Yellow
        NotificationSeverity.ERROR: "#ff6600",     # Orange
        NotificationSeverity.CRITICAL: "#ff0000",  # Red
    }
    
    # Severity ordering for filtering
    SEVERITY_ORDER = {
        NotificationSeverity.INFO: 0,
        NotificationSeverity.WARNING: 1,
        NotificationSeverity.ERROR: 2,
        NotificationSeverity.CRITICAL: 3,
    }
    
    def __init__(self, config: NotificationConfig | None = None):
        """Initialize the notification service.
        
        Args:
            config: Notification configuration
        """
        self.config = config or NotificationConfig()
        self.logger = logger.bind(component="NotificationService")
    
    def _should_send(self, severity: NotificationSeverity) -> bool:
        """Check if notification should be sent based on severity filter.
        
        Args:
            severity: Notification severity
            
        Returns:
            True if notification should be sent
        """
        return self.SEVERITY_ORDER[severity] >= self.SEVERITY_ORDER[self.config.min_severity]
    
    async def send(self, notification: Notification) -> dict[str, bool]:
        """Send notification via all configured channels.
        
        Args:
            notification: Notification to send
            
        Returns:
            Dict of channel -> success status
        """
        if not self._should_send(notification.severity):
            self.logger.debug(
                "Notification filtered by severity",
                severity=notification.severity.value,
                min_severity=self.config.min_severity.value,
            )
            return {}
        
        results: dict[str, bool] = {}
        
        if self.config.slack_webhook_url:
            results["slack"] = await self._send_slack(notification)
        
        if self.config.smtp_host and self.config.email_to:
            results["email"] = await self._send_email(notification)
        
        if not results:
            self.logger.warning(
                "No notification channels configured",
                notification=notification.title,
            )
        
        return results
    
    async def _send_slack(self, notification: Notification) -> bool:
        """Send notification to Slack via webhook.
        
        Args:
            notification: Notification to send
            
        Returns:
            True if successful
        """
        if not self.config.slack_webhook_url:
            return False
        
        payload: dict[str, Any] = {
            "attachments": [
                {
                    "color": self.SLACK_COLORS[notification.severity],
                    "title": f"[{notification.severity.value.upper()}] {notification.title}",
                    "text": notification.message,
                    "footer": f"Source: {notification.source}",
                    "ts": int(notification.timestamp.timestamp()),
                    "fields": [
                        {"title": k, "value": str(v), "short": True}
                        for k, v in notification.details.items()
                    ] if notification.details else [],
                }
            ]
        }
        
        if self.config.slack_channel:
            payload["channel"] = self.config.slack_channel
        
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.post(
                    self.config.slack_webhook_url,
                    json=payload,
                )
                
                if response.status_code == 200:
                    self.logger.info(
                        "Slack notification sent",
                        title=notification.title,
                    )
                    return True
                else:
                    self.logger.error(
                        "Slack notification failed",
                        status_code=response.status_code,
                        response=response.text,
                    )
                    return False
                    
        except Exception as e:
            self.logger.error("Failed to send Slack notification", error=str(e))
            return False
    
    async def _send_email(self, notification: Notification) -> bool:
        """Send notification via email.
        
        Args:
            notification: Notification to send
            
        Returns:
            True if successful
        """
        if not all([self.config.smtp_host, self.config.email_from, self.config.email_to]):
            return False
        
        try:
            msg = MIMEMultipart("alternative")
            msg["Subject"] = f"[{notification.severity.value.upper()}] {notification.title}"
            msg["From"] = self.config.email_from
            msg["To"] = ", ".join(self.config.email_to)
            
            text_body = f"""
{notification.title}
{'=' * len(notification.title)}

{notification.message}

Source: {notification.source}
Time: {notification.timestamp.isoformat()}
Severity: {notification.severity.value.upper()}
"""
            if notification.details:
                text_body += "\nDetails:\n"
                for key, value in notification.details.items():
                    text_body += f"  - {key}: {value}\n"
            
            severity_color = self.SLACK_COLORS[notification.severity]
            details_html = ""
            if notification.details:
                details_html = "<ul>" + "".join(
                    f"<li><strong>{k}:</strong> {v}</li>"
                    for k, v in notification.details.items()
                ) + "</ul>"
            
            html_body = f"""
<!DOCTYPE html>
<html>
<head>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        .header {{ background-color: {severity_color}; color: white; padding: 10px; border-radius: 5px; }}
        .content {{ padding: 15px; border: 1px solid #ddd; border-radius: 5px; margin-top: 10px; }}
        .footer {{ color: #666; font-size: 12px; margin-top: 15px; }}
    </style>
</head>
<body>
    <div class="header">
        <h2>[{notification.severity.value.upper()}] {notification.title}</h2>
    </div>
    <div class="content">
        <p>{notification.message}</p>
        {details_html}
    </div>
    <div class="footer">
        <p>Source: {notification.source}<br>
        Time: {notification.timestamp.isoformat()}</p>
    </div>
</body>
</html>
"""
            
            msg.attach(MIMEText(text_body, "plain"))
            msg.attach(MIMEText(html_body, "html"))
            
            with smtplib.SMTP(self.config.smtp_host, self.config.smtp_port) as server:
                if self.config.smtp_use_tls:
                    server.starttls()
                if self.config.smtp_user and self.config.smtp_password:
                    server.login(self.config.smtp_user, self.config.smtp_password)
                server.send_message(msg)
            
            self.logger.info(
                "Email notification sent",
                title=notification.title,
                recipients=len(self.config.email_to),
            )
            return True
            
        except Exception as e:
            self.logger.error("Failed to send email notification", error=str(e))
            return False
    
    async def notify_collector_failure(
        self,
        collector_name: str,
        error: str,
        consecutive_failures: int = 1,
    ) -> dict[str, bool]:
        """Send notification for collector failure.
        
        Args:
            collector_name: Name of the failed collector
            error: Error message
            consecutive_failures: Number of consecutive failures
            
        Returns:
            Channel results
        """
        severity = NotificationSeverity.CRITICAL if consecutive_failures >= 3 else NotificationSeverity.ERROR
        
        notification = Notification(
            title=f"Collector Failure: {collector_name}",
            message=f"The {collector_name} collector has failed. Error: {error}",
            severity=severity,
            source="CollectorScheduler",
            details={
                "collector": collector_name,
                "consecutive_failures": consecutive_failures,
                "error": error[:200],  # Truncate long errors
            },
        )
        return await self.send(notification)
    
    async def notify_api_key_expiring(
        self,
        key_name: str,
        days_until_expiry: int,
    ) -> dict[str, bool]:
        """Send notification for expiring API key.
        
        Args:
            key_name: Name of the API key
            days_until_expiry: Days until expiration
            
        Returns:
            Channel results
        """
        if days_until_expiry <= 0:
            severity = NotificationSeverity.CRITICAL
            message = f"The API key {key_name} has EXPIRED!"
        elif days_until_expiry <= 7:
            severity = NotificationSeverity.ERROR
            message = f"The API key {key_name} expires in {days_until_expiry} days."
        else:
            severity = NotificationSeverity.WARNING
            message = f"The API key {key_name} expires in {days_until_expiry} days."
        
        notification = Notification(
            title=f"API Key Expiring: {key_name}",
            message=message,
            severity=severity,
            source="APIKeyMonitor",
            details={
                "key_name": key_name,
                "days_until_expiry": days_until_expiry,
            },
        )
        return await self.send(notification)
    
    async def notify_high_error_rate(
        self,
        endpoint: str,
        error_rate: float,
        time_window_minutes: int = 5,
    ) -> dict[str, bool]:
        """Send notification for high error rate.
        
        Args:
            endpoint: API endpoint with high errors
            error_rate: Error rate percentage
            time_window_minutes: Time window for the rate
            
        Returns:
            Channel results
        """
        severity = NotificationSeverity.CRITICAL if error_rate > 50 else NotificationSeverity.ERROR
        
        notification = Notification(
            title=f"High Error Rate: {endpoint}",
            message=f"Endpoint {endpoint} has {error_rate:.1f}% error rate in the last {time_window_minutes} minutes.",
            severity=severity,
            source="APIMonitor",
            details={
                "endpoint": endpoint,
                "error_rate": f"{error_rate:.1f}%",
                "time_window": f"{time_window_minutes}m",
            },
        )
        return await self.send(notification)
    
    async def notify_system_health(
        self,
        component: str,
        status: str,
        details: str,
    ) -> dict[str, bool]:
        """Send notification for system health issue.
        
        Args:
            component: System component (database, redis, etc.)
            status: Current status
            details: Additional details
            
        Returns:
            Channel results
        """
        notification = Notification(
            title=f"System Health: {component}",
            message=f"Component {component} status: {status}. {details}",
            severity=NotificationSeverity.ERROR if status != "healthy" else NotificationSeverity.INFO,
            source="HealthMonitor",
            details={
                "component": component,
                "status": status,
            },
        )
        return await self.send(notification)
