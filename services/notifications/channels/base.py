"""
通知渠道抽象基类
"""

from abc import ABC, abstractmethod
from typing import Optional


class NotificationChannel(ABC):
    """通知渠道基类"""

    @abstractmethod
    async def send(
        self,
        account_id: str,
        title: str,
        content: str,
        color: str = "blue",
        event_type: Optional[str] = None,
    ) -> dict:
        """
        发送通知

        Args:
            account_id: 账户ID
            title: 通知标题
            content: 通知内容
            color: 颜色主题 (blue/green/red/orange/purple)
            event_type: 事件类型

        Returns:
            发送结果字典
        """
        pass
