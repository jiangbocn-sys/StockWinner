"""
通知 API 接口

提供通知配置管理、通知历史查询、测试通知等功能
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional, List

router = APIRouter()


class NotificationConfig(BaseModel):
    channel: str = "feishu"
    webhook_url: str
    enabled: int = 1
    notify_on_trade: int = 1
    notify_on_signal: int = 1
    notify_on_task: int = 1


class NotificationRule(BaseModel):
    rule_name: str = "默认规则"
    event_types: List[str] = []
    event_categories: List[str] = []
    notify_on_trade: int = 1
    notify_on_signal: int = 1
    notify_on_task: int = 1
    notify_on_system: int = 1
    notify_on_risk: int = 1
    time_range_start: Optional[str] = None
    time_range_end: Optional[str] = None
    trading_hours_only: int = 0
    signal_action_filter: Optional[str] = None
    min_amount_threshold: float = 0
    channels: List[str] = []
    priority: int = 1
    enabled: int = 1


class NotificationChannel(BaseModel):
    channel_type: str = "feishu"
    webhook_url: str
    extra_config: dict = {}
    priority: int = 1
    enabled: int = 1


@router.get("/api/v1/ui/{account_id}/notifications/config")
async def get_notification_config(account_id: str):
    """获取通知配置"""
    from services.notifications import get_notification_manager

    svc = get_notification_manager()
    config = await svc.get_config(account_id)
    return {"success": True, "data": config}


@router.post("/api/v1/ui/{account_id}/notifications/config")
async def save_notification_config(account_id: str, config: NotificationConfig):
    """保存通知配置"""
    from services.notifications import get_notification_manager

    svc = get_notification_manager()
    config_id = await svc.save_config(account_id, config.model_dump())
    return {
        "success": True,
        "message": "通知配置已保存",
        "data": {"id": config_id},
    }


@router.get("/api/v1/ui/{account_id}/notifications/history")
async def get_notification_history(
    account_id: str,
    limit: int = 50,
    offset: int = 0,
    event_type: Optional[str] = None,
):
    """获取通知历史"""
    from services.notifications import get_notification_manager

    svc = get_notification_manager()
    history = await svc.get_history(account_id, limit=limit, offset=offset, event_type=event_type)
    return {"success": True, "data": history, "total": len(history)}


@router.post("/api/v1/ui/{account_id}/notifications/test")
async def send_test_notification(account_id: str):
    """发送测试通知"""
    from services.notifications import get_notification_manager

    svc = get_notification_manager()

    # 获取配置
    config = await svc.get_config(account_id)
    if not config:
        raise HTTPException(status_code=404, detail="未配置通知渠道")

    # 发送测试通知
    result = await svc.trigger_sync(
        event_type="task_completed",
        account_id=account_id,
        payload={
            "task_name": "测试通知",
            "task_type": "test",
            "duration": "0s",
            "output": "这是一条测试通知，如果您收到此消息说明飞书 Webhook 配置正确。",
        },
    )

    return {
        "success": result.get("sent", False),
        "message": "测试通知已发送" if result.get("sent") else f"发送失败: {result.get('error', result.get('skipped_reason', '未知错误'))}",
        "detail": result,
    }


# ==================== 规则配置 API ====================

@router.get("/api/v1/ui/{account_id}/notification-rules")
async def get_notification_rules(account_id: str):
    """获取通知规则列表"""
    from services.common.database import get_db_manager
    import json

    db = get_db_manager()
    rules = await db.fetchall(
        "SELECT * FROM notification_rules WHERE account_id = ? ORDER BY priority ASC",
        (account_id,)
    )

    result = []
    for rule in rules:
        rule_dict = dict(rule)
        rule_dict["event_types"] = json.loads(rule_dict.get("event_types", "[]"))
        rule_dict["event_categories"] = json.loads(rule_dict.get("event_categories", "[]"))
        rule_dict["channels"] = json.loads(rule_dict.get("channels", "[]"))
        result.append(rule_dict)

    return {"success": True, "data": result}


@router.post("/api/v1/ui/{account_id}/notification-rules")
async def save_notification_rule(account_id: str, rule: NotificationRule):
    """创建/更新通知规则"""
    from services.common.database import get_db_manager
    from services.common.timezone import get_china_time
    import json

    db = get_db_manager()
    now = get_china_time().isoformat()

    rule_data = {
        "account_id": account_id,
        "rule_name": rule.rule_name,
        "event_types": json.dumps(rule.event_types),
        "event_categories": json.dumps(rule.event_categories),
        "notify_on_trade": rule.notify_on_trade,
        "notify_on_signal": rule.notify_on_signal,
        "notify_on_task": rule.notify_on_task,
        "notify_on_system": rule.notify_on_system,
        "notify_on_risk": rule.notify_on_risk,
        "time_range_start": rule.time_range_start,
        "time_range_end": rule.time_range_end,
        "trading_hours_only": rule.trading_hours_only,
        "signal_action_filter": rule.signal_action_filter,
        "min_amount_threshold": rule.min_amount_threshold,
        "channels": json.dumps(rule.channels),
        "priority": rule.priority,
        "enabled": rule.enabled,
        "updated_at": now,
    }

    # 检查是否已有默认规则
    existing = await db.fetchone(
        "SELECT id FROM notification_rules WHERE account_id = ? AND rule_name = ?",
        (account_id, rule.rule_name)
    )

    if existing:
        # 更新现有规则
        await db.execute(
            "UPDATE notification_rules SET ... WHERE id = ?",
            (existing["id"],)
        )
        # 简化：逐字段更新
        for key, value in rule_data.items():
            if key != "account_id":
                await db.execute(f"UPDATE notification_rules SET {key} = ? WHERE id = ?", (value, existing["id"]))
        rule_id = existing["id"]
    else:
        # 创建新规则
        rule_data["created_at"] = now
        rule_id = await db.insert("notification_rules", rule_data)

    # 刷新规则引擎缓存
    from services.notifications.rules.engine import get_rule_engine
    get_rule_engine().reload_rules()

    return {"success": True, "message": "规则已保存", "data": {"id": rule_id}}


@router.delete("/api/v1/ui/{account_id}/notification-rules/{rule_id}")
async def delete_notification_rule(account_id: str, rule_id: int):
    """删除通知规则"""
    from services.common.database import get_db_manager

    db = get_db_manager()
    existing = await db.fetchone(
        "SELECT id FROM notification_rules WHERE id = ? AND account_id = ?",
        (rule_id, account_id)
    )

    if not existing:
        raise HTTPException(status_code=404, detail="规则不存在")

    await db.execute("DELETE FROM notification_rules WHERE id = ?", (rule_id,))

    # 刷新规则引擎缓存
    from services.notifications.rules.engine import get_rule_engine
    get_rule_engine().reload_rules()

    return {"success": True, "message": "规则已删除"}


# ==================== 渠道配置 API ====================

@router.get("/api/v1/ui/{account_id}/notification-channels")
async def get_notification_channels(account_id: str):
    """获取通知渠道列表"""
    from services.common.database import get_db_manager
    import json

    db = get_db_manager()
    channels = await db.fetchall(
        "SELECT * FROM notification_channels WHERE account_id = ? ORDER BY priority ASC",
        (account_id,)
    )

    result = []
    for ch in channels:
        ch_dict = dict(ch)
        ch_dict["extra_config"] = json.loads(ch_dict.get("extra_config", "{}"))
        result.append(ch_dict)

    return {"success": True, "data": result}


@router.post("/api/v1/ui/{account_id}/notification-channels")
async def save_notification_channel(account_id: str, channel: NotificationChannel):
    """创建/更新通知渠道"""
    from services.common.database import get_db_manager
    from services.common.timezone import get_china_time
    import json

    db = get_db_manager()
    now = get_china_time().isoformat()

    channel_data = {
        "account_id": account_id,
        "channel_type": channel.channel_type,
        "webhook_url": channel.webhook_url,
        "extra_config": json.dumps(channel.extra_config),
        "priority": channel.priority,
        "enabled": channel.enabled,
        "updated_at": now,
    }

    # 检查是否已有相同类型的渠道
    existing = await db.fetchone(
        "SELECT id FROM notification_channels WHERE account_id = ? AND channel_type = ?",
        (account_id, channel.channel_type)
    )

    if existing:
        # 更新现有渠道
        for key, value in channel_data.items():
            if key != "account_id":
                await db.execute(f"UPDATE notification_channels SET {key} = ? WHERE id = ?", (value, existing["id"]))
        channel_id = existing["id"]
    else:
        # 创建新渠道
        channel_data["created_at"] = now
        channel_id = await db.insert("notification_channels", channel_data)

    # 刷新渠道路由缓存
    from services.notifications.rules.router import get_channel_router
    get_channel_router().reload_channels()

    return {"success": True, "message": "渠道已保存", "data": {"id": channel_id}}
