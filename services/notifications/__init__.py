"""
通知服务
事件驱动的消息推送模块 — 统一通知管理器
"""

from .manager import NotificationManager, get_notification_manager

__all__ = [
    "NotificationManager", "get_notification_manager",
]
