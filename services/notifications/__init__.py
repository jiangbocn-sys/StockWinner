"""
通知服务
事件驱动的消息推送模块
"""

from .service import NotificationService, get_notification_service

__all__ = ["NotificationService", "get_notification_service"]
