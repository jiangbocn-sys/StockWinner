"""
通知服务 - 事件驱动的消息推送
单例模式，全局共享
"""

import asyncio
import json
from typing import Any, Dict, List, Optional

from services.common.database import get_db_manager
from services.notifications.channels.feishu import FeishuWebhookChannel

# 事件类型到默认颜色的映射
EVENT_COLOR_MAP = {
    "trade_executed": "green",     # 成交成功
    "signal_triggered": "blue",    # 信号触发
    "task_completed": "blue",      # 任务完成
    "task_failed": "red",          # 任务失败
    "trade_failed": "red",         # 交易失败
}

# 事件类型到飞书标题的映射
EVENT_TITLE_MAP = {
    "trade_executed": "成交通知",
    "signal_triggered": "信号触发",
    "task_completed": "任务完成",
    "task_failed": "任务失败",
    "trade_failed": "交易失败",
}


class NotificationService:
    """通知服务单例"""

    def __init__(self):
        self._channels: Dict[str, FeishuWebhookChannel] = {}  # account_id -> channel

    def _get_channel(self, account_id: str, webhook_url: str) -> FeishuWebhookChannel:
        """获取或创建通知渠道（简单缓存）"""
        if account_id not in self._channels or self._channels[account_id].webhook_url != webhook_url:
            self._channels[account_id] = FeishuWebhookChannel(webhook_url)
        return self._channels[account_id]

    async def emit(
        self,
        event_type: str,
        account_id: str,
        payload: Dict[str, Any],
    ):
        """
        发送事件通知

        流程：
        1. 检查账户的通知配置
        2. 检查该事件类型是否开启通知
        3. 构建标题和内容
        4. 异步发送到渠道
        5. 记录到 notification_history

        Args:
            event_type: 事件类型 (trade_executed/signal_triggered/task_completed/task_failed)
            account_id: 账户ID
            payload: 事件数据
        """
        db = get_db_manager()

        # 1. 检查通知配置
        configs = await db.fetchall(
            "SELECT * FROM notification_config WHERE account_id = ? AND enabled = 1",
            (account_id,),
        )

        if not configs:
            return  # 未配置通知

        config = configs[0]  # 取第一个配置

        # 2. 检查事件类型开关
        event_flag_map = {
            "trade_executed": "notify_on_trade",
            "trade_failed": "notify_on_trade",
            "signal_triggered": "notify_on_signal",
            "task_completed": "notify_on_task",
            "task_failed": "notify_on_task",
        }
        flag_key = event_flag_map.get(event_type)
        if flag_key and not config.get(flag_key, 1):
            return  # 该事件类型通知已关闭

        # 3. 构建标题和内容
        title = EVENT_TITLE_MAP.get(event_type, event_type)
        color = EVENT_COLOR_MAP.get(event_type, "blue")
        content = self._build_content(event_type, payload)

        # 4. 发送通知
        try:
            channel = self._get_channel(account_id, config["webhook_url"])
            result = await channel.send(
                account_id=account_id,
                title=title,
                content=content,
                color=color,
                event_type=event_type,
            )
            if not result.get("success"):
                print(f"[通知] 发送失败 ({event_type}): {result.get('response')}")
        except Exception as e:
            print(f"[通知] 发送异常 ({event_type}): {e}")
            result = {"success": False, "response": str(e), "status": "error"}

        # 5. 记录历史
        try:
            from services.common.timezone import get_china_time
            await db.insert("notification_history", {
                "account_id": account_id,
                "channel": config["channel"],
                "event_type": event_type,
                "title": title,
                "content": content,
                "status": result.get("status", "unknown"),
                "response": result.get("response", ""),
                "created_at": get_china_time().isoformat(),
            })
        except Exception as e:
            print(f"通知历史记录写入失败: {e}")

    def _build_content(self, event_type: str, payload: Dict[str, Any]) -> str:
        """构建飞书卡片内容（Markdown 格式）"""

        if event_type == "trade_executed":
            trade_type = payload.get("trade_type", "")
            type_emoji = "买入" if trade_type == "buy" else "卖出"
            lines = [
                f"**操作类型：** {type_emoji}",
                f"**股票代码：** {payload.get('stock_code', '-')}",
                f"**股票名称：** {payload.get('stock_name', '-')}",
                f"**成交价格：** {payload.get('price', '-')}",
                f"**成交数量：** {payload.get('quantity', '-')}",
                f"**成交金额：** {payload.get('amount', '-')}",
                f"**手续费：** {payload.get('fees', '-')}",
                f"**触发来源：** {payload.get('trigger_source', '监控')}",
            ]
            return "\n".join(lines)

        elif event_type == "trade_failed":
            lines = [
                f"**操作类型：** 交易失败",
                f"**股票代码：** {payload.get('stock_code', '-')}",
                f"**失败原因：** {payload.get('reason', '-')}",
            ]
            return "\n".join(lines)

        elif event_type == "signal_triggered":
            lines = [
                f"**策略名称：** {payload.get('strategy_name', '-')}",
                f"**候选组：** {payload.get('group_name', '-')}",
                f"**股票代码：** {payload.get('stock_code', '-')}",
                f"**股票名称：** {payload.get('stock_name', '-')}",
                f"**建议买入价：** {payload.get('buy_price', '-')}",
                f"**止损价：** {payload.get('stop_loss_price', '-')}",
                f"**止盈价：** {payload.get('take_profit_price', '-')}",
                f"**触发原因：** {payload.get('reason', '-')}",
            ]
            return "\n".join(lines)

        elif event_type == "task_completed":
            lines = [
                f"**任务名称：** {payload.get('task_name', '-')}",
                f"**任务类型：** {payload.get('task_type', '-')}",
                f"**执行结果：** 成功",
                f"**耗时：** {payload.get('duration', '-')}",
            ]
            if payload.get("output"):
                lines.append(f"**详情：** {payload['output']}")
            return "\n".join(lines)

        elif event_type == "task_failed":
            lines = [
                f"**任务名称：** {payload.get('task_name', '-')}",
                f"**任务类型：** {payload.get('task_type', '-')}",
                f"**执行结果：** 失败",
                f"**错误信息：** {payload.get('error', '-')}",
            ]
            return "\n".join(lines)

        else:
            # 通用格式
            lines = []
            for key, value in payload.items():
                lines.append(f"**{key}：** {value}")
            return "\n".join(lines)

    async def get_config(self, account_id: str) -> Optional[Dict]:
        """获取通知配置"""
        db = get_db_manager()
        rows = await db.fetchall(
            "SELECT * FROM notification_config WHERE account_id = ?",
            (account_id,),
        )
        return rows[0] if rows else None

    async def save_config(self, account_id: str, config: Dict) -> int:
        """保存通知配置"""
        db = get_db_manager()
        existing = await self.get_config(account_id)

        data = {
            "account_id": account_id,
            "channel": config.get("channel", "feishu"),
            "webhook_url": config["webhook_url"],
            "enabled": config.get("enabled", 1),
            "notify_on_trade": config.get("notify_on_trade", 1),
            "notify_on_signal": config.get("notify_on_signal", 1),
            "notify_on_task": config.get("notify_on_task", 1),
        }

        if existing:
            await db.update("notification_config", data, "id = ?", (existing["id"],))
            return existing["id"]
        else:
            return await db.insert("notification_config", data)

    async def get_history(
        self,
        account_id: str,
        limit: int = 50,
        offset: int = 0,
        event_type: Optional[str] = None,
    ) -> List[Dict]:
        """获取通知历史"""
        db = get_db_manager()
        where = "account_id = ?"
        params: list = [account_id]

        if event_type:
            where += " AND event_type = ?"
            params.append(event_type)

        params.extend([limit, offset])

        return await db.fetchall(
            f"SELECT * FROM notification_history WHERE {where} ORDER BY created_at DESC LIMIT ? OFFSET ?",
            tuple(params),
        )


# 全局单例
_notification_service: Optional[NotificationService] = None


def get_notification_service() -> NotificationService:
    """获取通知服务单例"""
    global _notification_service
    if _notification_service is None:
        _notification_service = NotificationService()
    return _notification_service
