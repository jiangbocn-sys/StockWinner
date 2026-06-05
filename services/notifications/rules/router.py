"""
渠道路由
决定发送到哪个渠道
"""

import json
from typing import Dict, Any, Optional, List
from dataclasses import dataclass

from services.common.database import get_db_manager
from services.notifications.events.payload import EventPayload


@dataclass
class ChannelConfig:
    """渠道配置"""
    channel_type: str          # "feishu"
    webhook_url: str           # 渠道地址
    enabled: bool = True
    priority: int = 1          # 渠道优先级

    # 渠道特定配置
    extra_config: Dict[str, Any] = None  # 如邮件的 SMTP 配置


class ChannelRouter:
    """渠道路由

    根据规则和账户配置决定发送渠道
    """

    def __init__(self):
        self._channels_cache: Dict[str, List[ChannelConfig]] = {}  # account_id -> channels
        self._cache_loaded = False

    async def _load_channels(self) -> None:
        """从数据库加载渠道配置"""
        if self._cache_loaded:
            return

        db = get_db_manager()

        # 加载 notification_channels 表
        try:
            rows = await db.fetchall(
                "SELECT * FROM notification_channels WHERE enabled = 1 ORDER BY priority ASC"
            )
            for row in rows:
                channel = ChannelConfig(
                    channel_type=row["channel_type"],
                    webhook_url=row["webhook_url"],
                    enabled=row.get("enabled", 1) == 1,
                    priority=row.get("priority", 1),
                    extra_config=json.loads(row.get("extra_config", "{}")),
                )
                account_id = row["account_id"]
                if account_id not in self._channels_cache:
                    self._channels_cache[account_id] = []
                self._channels_cache[account_id].append(channel)
        except Exception:
            pass  # 表不存在或查询失败

        # 如果 notification_channels 为空，从 notification_config 加载作为 fallback
        if not self._channels_cache:
            try:
                configs = await db.fetchall(
                    "SELECT * FROM notification_config WHERE enabled = 1"
                )
                for config in configs:
                    channel = ChannelConfig(
                        channel_type=config.get("channel", "feishu"),
                        webhook_url=config["webhook_url"],
                        enabled=True,
                        priority=1,
                    )
                    account_id = config["account_id"]
                    if account_id not in self._channels_cache:
                        self._channels_cache[account_id] = []
                    self._channels_cache[account_id].append(channel)
            except Exception:
                pass

        self._cache_loaded = True

    async def get_channels(self, event: EventPayload) -> List[ChannelConfig]:
        """获取适用的渠道列表

        Args:
            event: 事件数据

        Returns:
            渠道配置列表（按优先级排序）
        """
        await self._load_channels()

        return self._channels_cache.get(event.account_id, [])

    def reload_channels(self) -> None:
        """重新加载渠道配置"""
        self._channels_cache = {}
        self._cache_loaded = False


# 全局单例
_router: Optional[ChannelRouter] = None


def get_channel_router() -> ChannelRouter:
    """获取渠道路由单例"""
    global _router
    if _router is None:
        _router = ChannelRouter()
    return _router