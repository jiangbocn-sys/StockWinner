"""
通知服务
事件驱动的消息推送模块

接口说明：
- get_notification_manager: 新接口（推荐），统一通知管理器
- get_notification_service: 旧接口（向后兼容），逐步废弃
"""

from .service import NotificationService, get_notification_service  # 旧接口（保留）
from .manager import NotificationManager, get_notification_manager   # 新接口（推荐）

__all__ = [
    "NotificationService", "get_notification_service",  # 向后兼容
    "NotificationManager", "get_notification_manager",  # 新推荐
]
