"""
通知 API 接口

提供通知配置管理、通知历史查询、测试通知等功能
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional

router = APIRouter()


class NotificationConfig(BaseModel):
    channel: str = "feishu"
    webhook_url: str
    enabled: int = 1
    notify_on_trade: int = 1
    notify_on_signal: int = 1
    notify_on_task: int = 1


@router.get("/api/v1/ui/{account_id}/notifications/config")
async def get_notification_config(account_id: str):
    """获取通知配置"""
    from services.notifications import get_notification_service

    svc = get_notification_service()
    config = await svc.get_config(account_id)
    return {"success": True, "data": config}


@router.post("/api/v1/ui/{account_id}/notifications/config")
async def save_notification_config(account_id: str, config: NotificationConfig):
    """保存通知配置"""
    from services.notifications import get_notification_service

    svc = get_notification_service()
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
    from services.notifications import get_notification_service

    svc = get_notification_service()
    history = await svc.get_history(account_id, limit=limit, offset=offset, event_type=event_type)
    return {"success": True, "data": history, "total": len(history)}


@router.post("/api/v1/ui/{account_id}/notifications/test")
async def send_test_notification(account_id: str):
    """发送测试通知"""
    from services.notifications import get_notification_service

    svc = get_notification_service()

    # 获取配置
    config = await svc.get_config(account_id)
    if not config:
        raise HTTPException(status_code=404, detail="未配置通知渠道")

    # 发送测试通知
    await svc.emit(
        event_type="task_completed",
        account_id=account_id,
        payload={
            "task_name": "测试通知",
            "task_type": "test",
            "duration": "0s",
            "output": "这是一条测试通知，如果您收到此消息说明飞书 Webhook 配置正确。",
        },
    )

    return {"success": True, "message": "测试通知已发送"}
