"""
事件类型定义
"""

from enum import Enum
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field


class EventCategory(Enum):
    """事件分类"""
    TRADE = "trade"           # 交易类（成交、拒绝）
    SIGNAL = "signal"         # 信号类（触发、更新）
    TASK = "task"             # 任务类（完成、失败）
    SYSTEM = "system"         # 系统类（调度异常、监控中断）
    RISK = "risk"             # 风控类（仓位超限、资金不足）


class EventType(Enum):
    """事件类型枚举"""
    # 交易类
    TRADE_EXECUTED = "trade_executed"       # 交易成交
    TRADE_FAILED = "trade_failed"           # 交易失败
    ORDER_REJECTED = "order_rejected"       # 委托被拒

    # 信号类
    SIGNAL_TRIGGERED = "signal_triggered"   # 信号触发
    SIGNAL_UPDATED = "signal_updated"       # 信号更新

    # 任务类
    TASK_COMPLETED = "task_completed"       # 任务完成
    TASK_FAILED = "task_failed"             # 任务失败

    # 系统类
    SCHEDULER_DOWN = "scheduler_down"       # 调度服务异常
    MONITOR_INTERRUPTED = "monitor_interrupted"  # 交易监控中断
    SDK_CONNECTION_ERROR = "sdk_connection_error"  # SDK连接异常
    MONITOR_DATA_STALE = "monitor_data_stale"      # 行情数据过期

    # 风控类
    POSITION_LIMIT_EXCEEDED = "position_limit_exceeded"  # 仓位超限
    CASH_INSUFFICIENT = "cash_insufficient"  # 资金不足


@dataclass
class EventMeta:
    """事件元数据"""
    event_type: EventType
    category: EventCategory
    title_template: str                    # 标题模板
    color: str                             # 默认颜色
    priority: int = 1                      # 优先级 (1-5, 1最高)
    require_confirm: bool = False          # 是否需要确认
    default_channels: List[str] = field(default_factory=lambda: ["feishu"])
    debounce_seconds: int = 0              # 防抖秒数（系统类事件需要）

    # 可配置的开关字段名（映射到 notification_rules 表）
    config_flag_key: Optional[str] = None  # 如 "notify_on_trade"


# 事件元数据注册表
EVENT_META_REGISTRY: Dict[EventType, EventMeta] = {
    # 交易类
    EventType.TRADE_EXECUTED: EventMeta(
        event_type=EventType.TRADE_EXECUTED,
        category=EventCategory.TRADE,
        title_template="成交通知",
        color="green",
        priority=2,
        config_flag_key="notify_on_trade",
    ),
    EventType.TRADE_FAILED: EventMeta(
        event_type=EventType.TRADE_FAILED,
        category=EventCategory.TRADE,
        title_template="交易失败",
        color="red",
        priority=1,
        config_flag_key="notify_on_trade",
    ),
    EventType.ORDER_REJECTED: EventMeta(
        event_type=EventType.ORDER_REJECTED,
        category=EventCategory.TRADE,
        title_template="委托被拒",
        color="red",
        priority=1,
        config_flag_key="notify_on_trade",
    ),

    # 信号类
    EventType.SIGNAL_TRIGGERED: EventMeta(
        event_type=EventType.SIGNAL_TRIGGERED,
        category=EventCategory.SIGNAL,
        title_template="信号触发",
        color="blue",
        priority=3,
        config_flag_key="notify_on_signal",
    ),
    EventType.SIGNAL_UPDATED: EventMeta(
        event_type=EventType.SIGNAL_UPDATED,
        category=EventCategory.SIGNAL,
        title_template="信号更新",
        color="blue",
        priority=3,
        config_flag_key="notify_on_signal",
    ),

    # 任务类
    EventType.TASK_COMPLETED: EventMeta(
        event_type=EventType.TASK_COMPLETED,
        category=EventCategory.TASK,
        title_template="任务完成",
        color="blue",
        priority=4,
        config_flag_key="notify_on_task",
    ),
    EventType.TASK_FAILED: EventMeta(
        event_type=EventType.TASK_FAILED,
        category=EventCategory.TASK,
        title_template="任务失败",
        color="red",
        priority=2,
        config_flag_key="notify_on_task",
    ),

    # 系统类
    EventType.SCHEDULER_DOWN: EventMeta(
        event_type=EventType.SCHEDULER_DOWN,
        category=EventCategory.SYSTEM,
        title_template="调度服务异常",
        color="red",
        priority=1,
        debounce_seconds=300,  # 5分钟防抖
        config_flag_key="notify_on_system",
    ),
    EventType.MONITOR_INTERRUPTED: EventMeta(
        event_type=EventType.MONITOR_INTERRUPTED,
        category=EventCategory.SYSTEM,
        title_template="交易监控中断",
        color="red",
        priority=1,
        debounce_seconds=300,
        config_flag_key="notify_on_system",
    ),
    EventType.SDK_CONNECTION_ERROR: EventMeta(
        event_type=EventType.SDK_CONNECTION_ERROR,
        category=EventCategory.SYSTEM,
        title_template="SDK连接异常",
        color="red",
        priority=1,
        debounce_seconds=300,
        config_flag_key="notify_on_system",
    ),
    EventType.MONITOR_DATA_STALE: EventMeta(
        event_type=EventType.MONITOR_DATA_STALE,
        category=EventCategory.SYSTEM,
        title_template="行情数据过期",
        color="red",
        priority=1,
        debounce_seconds=300,
        config_flag_key="notify_on_system",
    ),

    # 风控类
    EventType.POSITION_LIMIT_EXCEEDED: EventMeta(
        event_type=EventType.POSITION_LIMIT_EXCEEDED,
        category=EventCategory.RISK,
        title_template="仓位超限",
        color="orange",
        priority=1,
        config_flag_key="notify_on_risk",
    ),
    EventType.CASH_INSUFFICIENT: EventMeta(
        event_type=EventType.CASH_INSUFFICIENT,
        category=EventCategory.RISK,
        title_template="资金不足",
        color="orange",
        priority=1,
        config_flag_key="notify_on_risk",
    ),
}


def get_event_meta(event_type: EventType) -> Optional[EventMeta]:
    """获取事件元数据"""
    return EVENT_META_REGISTRY.get(event_type)


def get_event_type_from_string(event_type_str: str) -> Optional[EventType]:
    """从字符串解析事件类型"""
    try:
        return EventType(event_type_str)
    except ValueError:
        return None