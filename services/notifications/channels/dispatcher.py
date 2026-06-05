"""
渠道调度器
统一发送接口
"""

from typing import Dict, Any, Optional
from dataclasses import dataclass

from services.notifications.channels.feishu import FeishuWebhookChannel
from services.notifications.channels.base import NotificationChannel


@dataclass
class ChannelConfigSimple:
    """渠道配置（简化版，用于调度器）"""
    channel_type: str
    webhook_url: str
    extra_config: Dict[str, Any] = None


class ChannelDispatcher:
    """渠道调度器

    管理所有渠道实例，统一发送接口
    """

    def __init__(self):
        self._channels: Dict[str, NotificationChannel] = {}

    def _get_channel(self, config: ChannelConfigSimple) -> Optional[NotificationChannel]:
        """获取或创建渠道实例"""
        cache_key = f"{config.channel_type}:{config.webhook_url}"

        if cache_key not in self._channels:
            if config.channel_type == "feishu":
                self._channels[cache_key] = FeishuWebhookChannel(config.webhook_url)
            # TODO: 添加其他渠道支持
            # elif config.channel_type == "email":
            #     self._channels[cache_key] = EmailChannel(config)

        return self._channels.get(cache_key)

    async def send(
        self,
        channel_config: ChannelConfigSimple,
        account_id: str,
        title: str,
        content: str,
        color: str = "blue",
        event_type: Optional[str] = None,
    ) -> Dict[str, Any]:
        """发送通知

        Args:
            channel_config: 渠道配置
            account_id: 账户ID
            title: 标题
            content: 内容（Markdown 格式）
            color: 颜色
            event_type: 事件类型

        Returns:
            发送结果 {"success": bool, "response": str, "status": str}
        """
        channel = self._get_channel(channel_config)
        if channel is None:
            return {
                "success": False,
                "response": f"不支持渠道类型: {channel_config.channel_type}",
                "status": "unsupported",
            }

        try:
            result = await channel.send(
                account_id=account_id,
                title=title,
                content=content,
                color=color,
                event_type=event_type,
            )
            return result
        except Exception as e:
            return {
                "success": False,
                "response": str(e),
                "status": "error",
            }


# 全局单例
_dispatcher: Optional[ChannelDispatcher] = None


def get_channel_dispatcher() -> ChannelDispatcher:
    """获取渠道调度器单例"""
    global _dispatcher
    if _dispatcher is None:
        _dispatcher = ChannelDispatcher()
    return _dispatcher