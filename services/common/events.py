"""
轻量事件系统 - 模块间解耦通信

底层模块发布事件，上层模块订阅事件，避免逆向依赖。

使用方式：
1. 底层模块: emit_provider_status("amazingdata", True)
2. 上层模块: subscribe(EVENT_PROVIDER_STATUS, handler)
"""

from typing import Callable, Dict, List, Any
from services.common.structured_logger import get_logger

log = get_logger("events")

# 事件处理器注册表
_handlers: Dict[str, List[Callable]] = {}


def subscribe(event_type: str, handler: Callable):
    """订阅事件

    Args:
        event_type: 事件类型
        handler: 处理函数，接收 Dict 参数
    """
    if event_type not in _handlers:
        _handlers[event_type] = []
    _handlers[event_type].append(handler)
    log.debug("event_subscribe", f"{event_type} -> {handler.__name__}")


def unsubscribe(event_type: str, handler: Callable):
    """取消订阅"""
    if event_type in _handlers and handler in _handlers[event_type]:
        _handlers[event_type].remove(handler)


def publish(event_type: str, data: Dict[str, Any]):
    """发布事件（同步调用所有订阅者）

    Args:
        event_type: 事件类型
        data: 事件数据
    """
    handlers = _handlers.get(event_type, [])
    for handler in handlers:
        try:
            handler(data)
        except Exception as e:
            log.warning("event_handler_error", f"{event_type}: {handler.__name__}: {e}")


# ================================================================
# 标准事件类型定义
# ================================================================

EVENT_PROVIDER_STATUS = "provider_status_changed"
EVENT_SDK_HEALTH_CHANGED = "sdk_health_changed"
EVENT_DATA_STALE_CHANGED = "data_stale_changed"


def emit_provider_status(provider_id: str, ok: bool, message: str = ""):
    """发布数据源状态变化事件

    Args:
        provider_id: 数据源 ID（如 "amazingdata"）
        ok: 是否正常
        message: 错误消息（可选）
    """
    publish(EVENT_PROVIDER_STATUS, {
        "provider_id": provider_id,
        "ok": ok,
        "message": message
    })


def emit_sdk_health(ok: bool, message: str = ""):
    """发布 SDK 健康状态变化事件"""
    publish(EVENT_SDK_HEALTH_CHANGED, {
        "ok": ok,
        "message": message
    })


def emit_data_stale(stale: bool, message: str = ""):
    """发布数据过期状态变化事件"""
    publish(EVENT_DATA_STALE_CHANGED, {
        "stale": stale,
        "message": message
    })