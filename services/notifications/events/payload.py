"""
事件数据类
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Any, Optional

from services.notifications.events.types import EventType, EventMeta


@dataclass
class EventPayload:
    """事件数据

    封装事件的所有信息，用于在通知系统内部传递
    """
    event_type: EventType
    account_id: str
    payload: Dict[str, Any]              # 原始数据，不做格式化
    context: Dict[str, Any] = field(default_factory=dict)  # 规则判断用
    meta: EventMeta = None               # 事件元数据
    triggered_at: datetime = None        # 触发时间

    def __post_init__(self):
        """初始化后处理"""
        if self.meta is None:
            from services.notifications.events.types import get_event_meta
            self.meta = get_event_meta(self.event_type)
        if self.triggered_at is None:
            from services.common.timezone import get_china_time
            self.triggered_at = get_china_time()