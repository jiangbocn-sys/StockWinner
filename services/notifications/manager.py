"""
统一通知管理器
触发即发送，规则后判断
"""

import asyncio
from typing import Dict, Any, Optional, List
from datetime import datetime

from services.common.database import get_db_manager
from services.common.timezone import get_china_time
from services.common.structured_logger import get_logger
from services.notifications.events.types import EventType, EventCategory, get_event_meta, get_event_type_from_string, EventMeta
from services.notifications.events.payload import EventPayload
from services.notifications.rules.engine import RuleEngine, get_rule_engine
from services.notifications.rules.router import ChannelRouter, ChannelConfig, get_channel_router
from services.notifications.channels.dispatcher import ChannelDispatcher, ChannelConfigSimple, get_channel_dispatcher
from services.notifications.history.recorder import HistoryRecorder, get_history_recorder


class NotificationManager:
    """统一通知管理器

    设计原则：
    1. 调用方只需触发事件，不关心发送逻辑
    2. 规则引擎统一判断是否发送
    3. 渠道路由决定发送到哪里
    4. 异步处理，不阻塞主业务
    """

    def __init__(self):
        self._rule_engine = get_rule_engine()
        self._channel_router = get_channel_router()
        self._dispatcher = get_channel_dispatcher()
        self._recorder = get_history_recorder()
        self._debounce_cache: Dict[str, datetime] = {}  # event_key -> last_sent_time
        self._logger = get_logger("notification")

    async def trigger(
        self,
        event_type: str,
        account_id: str,
        payload: Dict[str, Any],
        context: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """触发通知事件

        Args:
            event_type: 事件类型（字符串，如 "trade_executed"）
            account_id: 账户ID
            payload: 事件数据（原始数据，不做格式化）
            context: 上下文信息（可选，如 signal_action, strategy_id 等）

        Returns:
            是否成功触发（不代表是否发送成功）

        设计要点：
        1. 调用方传入原始数据，不做任何格式化
        2. context 用于规则引擎判断（如 signal_action）
        3. 返回 True 表示事件已触发，False 表示参数错误
        """
        try:
            # 1. 解析事件类型
            evt_type = get_event_type_from_string(event_type)
            if evt_type is None:
                self._logger.warn("notification", f"未知事件类型: {event_type}")
                return False

            meta = get_event_meta(evt_type)
            if meta is None:
                self._logger.warn("notification", f"事件类型无元数据: {event_type}")
                return False

            # 2. 构建 EventPayload
            event_payload = EventPayload(
                event_type=evt_type,
                account_id=account_id,
                payload=payload,
                context=context or {},
                meta=meta,
                triggered_at=get_china_time(),
            )

            # 3. 异步处理（不阻塞主业务）
            asyncio.create_task(self._process_event(event_payload))

            return True

        except Exception as e:
            self._logger.error("notification", f"触发事件异常: {e}")
            return False

    async def trigger_sync(
        self,
        event_type: str,
        account_id: str,
        payload: Dict[str, Any],
        context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """同步触发通知事件（等待发送完成）

        用于需要等待发送结果的场景（如测试通知）

        Returns:
            发送结果 {"triggered": bool, "sent": bool, "channels": [...]}
        """
        try:
            evt_type = get_event_type_from_string(event_type)
            if evt_type is None:
                return {"triggered": False, "sent": False, "error": "未知事件类型"}

            meta = get_event_meta(evt_type)
            if meta is None:
                return {"triggered": False, "sent": False, "error": "事件类型无元数据"}

            event_payload = EventPayload(
                event_type=evt_type,
                account_id=account_id,
                payload=payload,
                context=context or {},
                meta=meta,
                triggered_at=get_china_time(),
            )

            result = await self._process_event_sync(event_payload)
            return result

        except Exception as e:
            return {"triggered": False, "sent": False, "error": str(e)}

    async def _process_event(self, event: EventPayload) -> None:
        """异步处理事件（不等待发送结果）"""
        try:
            await self._process_event_sync(event)
        except Exception as e:
            self._logger.error("notification", f"处理事件异常: {e}")

    async def _process_event_sync(self, event: EventPayload) -> Dict[str, Any]:
        """同步处理事件（等待发送结果）

        流程：
        1. 检查账户是否暂停通知
        2. 规则引擎判断是否发送
        3. 防抖检查（系统类事件）
        4. 渠道路由选择渠道
        5. 构建内容（根据事件类型）
        6. 发送通知
        7. 记录历史
        """
        result = {
            "triggered": True,
            "sent": False,
            "skipped_reason": None,
            "channels": [],
        }

        db = get_db_manager()

        # Step 1: 检查账户是否暂停通知
        try:
            account = await db.fetchone(
                "SELECT notifications_paused FROM accounts WHERE account_id = ?",
                (event.account_id,),
            )
            if account and account.get("notifications_paused"):
                result["skipped_reason"] = "账户暂停通知"
                self._logger.log_event("notification_skipped",
                    f"账户 {event.account_id} 已暂停通知")
                return result
        except Exception:
            pass

        # Step 2: 规则引擎判断是否发送
        should_send = await self._rule_engine.evaluate(event)
        if not should_send:
            result["skipped_reason"] = "规则过滤"
            self._logger.log_event("notification_filtered",
                f"事件 {event.event_type.value} 被规则过滤")
            return result

        # Step 3: 防抖检查（系统类事件）
        if event.meta.debounce_seconds > 0:
            debounce_key = f"{event.event_type.value}:{event.account_id}"
            last_sent = self._debounce_cache.get(debounce_key)
            now = get_china_time()
            if last_sent and (now - last_sent).total_seconds() < event.meta.debounce_seconds:
                result["skipped_reason"] = "防抖跳过"
                self._logger.log_event("notification_debounced",
                    f"事件 {event.event_type.value} 防抖跳过")
                return result
            self._debounce_cache[debounce_key] = now

        # Step 4: 渠道路由
        channels = await self._channel_router.get_channels(event)
        if not channels:
            result["skipped_reason"] = "无可用渠道"
            self._logger.warn("notification",
                f"事件 {event.event_type.value} 无可用渠道")
            return result

        # Step 5: 构建内容
        title, content = self._build_content(event)

        # Step 6: 发送通知
        send_results = []
        for channel_config in channels:
            channel_simple = ChannelConfigSimple(
                channel_type=channel_config.channel_type,
                webhook_url=channel_config.webhook_url,
                extra_config=channel_config.extra_config,
            )
            send_result = await self._dispatcher.send(
                channel_config=channel_simple,
                account_id=event.account_id,
                title=title,
                content=content,
                color=event.meta.color,
                event_type=event.event_type.value,
            )
            send_results.append({
                "channel": channel_config.channel_type,
                "success": send_result.get("success", False),
                "response": send_result.get("response", ""),
            })

        result["channels"] = send_results
        success_count = sum(1 for r in send_results if r.get("success"))
        result["sent"] = success_count > 0

        # Step 7: 记录历史
        await self._recorder.record(
            account_id=event.account_id,
            event_type=event.event_type.value,
            channels=channels,
            title=title,
            content=content,
            results=send_results,
            payload=event.payload,
            context=event.context,
        )

        # 记录成功日志
        self._logger.log_event("notification_sent",
            f"通知发送: {event.event_type.value} → {success_count}/{len(channels)} 渠道",
            raw_event_type=event.event_type.value, success_count=success_count)

        return result

    def _build_content(self, event: EventPayload) -> tuple:
        """构建通知内容

        根据事件类型格式化内容，从 payload 中提取字段
        """
        meta = event.meta
        payload = event.payload

        # 标题：使用模板
        title = meta.title_template

        # 内容：根据事件类型格式化
        content = self._format_content(event.event_type, payload)

        return title, content

    def _format_content(self, event_type: EventType, payload: Dict[str, Any]) -> str:
        """格式化内容（Markdown 格式）"""

        # 交易类事件
        if event_type in (EventType.TRADE_EXECUTED, EventType.TRADE_FAILED):
            trade_type = payload.get("trade_type", "")
            type_label = "买入" if trade_type == "buy" else "卖出"
            lines = [
                f"**操作类型：** {type_label}",
                f"**股票代码：** {payload.get('stock_code', '-')}",
                f"**股票名称：** {payload.get('stock_name', '-')}",
                f"**成交价格：** {self._format_price(payload.get('price'))}",
                f"**成交数量：** {payload.get('quantity', '-')}",
            ]
            if event_type == EventType.TRADE_EXECUTED:
                lines.extend([
                    f"**成交金额：** {self._format_amount(payload.get('amount'))}",
                    f"**手续费：** {self._format_amount(payload.get('fees'))}",
                    f"**触发来源：** {payload.get('trigger_source', '-')}",
                ])
                if payload.get("profit_loss"):
                    lines.append(f"**盈亏：** {payload['profit_loss']}")
            else:
                lines.append(f"**失败原因：** {payload.get('reason', '-')}")
            return "\n".join(lines)

        # 委托被拒
        if event_type == EventType.ORDER_REJECTED:
            trade_type = "买入" if payload.get("trade_type") == "buy" else "卖出"
            lines = [
                f"**操作类型：** {trade_type}委托被拒",
                f"**股票代码：** {payload.get('stock_code', '-')}",
                f"**股票名称：** {payload.get('stock_name', '-')}",
                f"**委托价格：** {self._format_price(payload.get('price'))}",
                f"**委托数量：** {payload.get('quantity', '-')}",
                f"**拒绝原因：** {payload.get('reason', '-')}",
            ]
            return "\n".join(lines)

        # 信号类事件
        if event_type in (EventType.SIGNAL_TRIGGERED, EventType.SIGNAL_UPDATED):
            lines = [
                f"**策略名称：** {payload.get('strategy_name', '-')}",
                f"**候选组：** {payload.get('group_name', '-')}",
                f"**股票代码：** {payload.get('stock_code', '-')}",
                f"**股票名称：** {payload.get('stock_name', '-')}",
                f"**建议买入价：** {self._format_price(payload.get('trigger_price'))}",
                f"**止损价：** {self._format_price(payload.get('stop_loss_price'))}",
                f"**止盈价：** {self._format_price(payload.get('take_profit_price'))}",
                f"**触发原因：** {payload.get('reason', '-')}",
            ]
            return "\n".join(lines)

        # 任务类事件
        if event_type == EventType.TASK_COMPLETED:
            lines = [
                f"**任务名称：** {payload.get('task_name', '-')}",
                f"**任务类型：** {payload.get('task_type', '-')}",
                f"**执行结果：** 成功",
                f"**耗时：** {payload.get('duration', '-')}",
            ]
            if payload.get("output"):
                output_str = str(payload["output"])
                lines.append(f"**详情：** {output_str[:500]}")
            return "\n".join(lines)

        if event_type == EventType.TASK_FAILED:
            lines = [
                f"**任务名称：** {payload.get('task_name', '-')}",
                f"**任务类型：** {payload.get('task_type', '-')}",
                f"**执行结果：** 失败",
                f"**错误信息：** {payload.get('error', '-')}",
            ]
            return "\n".join(lines)

        # 系统类事件
        if event_type == EventType.SCHEDULER_DOWN:
            lines = [
                f"**异常类型：** 调度服务异常",
                f"**检测时间：** {payload.get('detected_at', '-')}",
                f"**异常信息：** {payload.get('detail', '-')}",
                f"**建议操作：** 重启后端服务",
            ]
            return "\n".join(lines)

        if event_type == EventType.MONITOR_INTERRUPTED:
            lines = [
                f"**异常类型：** 交易监控中断",
                f"**检测时间：** {payload.get('detected_at', '-')}",
                f"**账户：** {payload.get('account_id', '-')}",
                f"**持仓股数：** {payload.get('position_count', '-')}",
                f"**建议操作：** 手动启动交易监控或重启后端",
            ]
            return "\n".join(lines)

        if event_type == EventType.SDK_CONNECTION_ERROR:
            lines = [
                f"**异常类型：** SDK连接异常",
                f"**检测时间：** {payload.get('detected_at', '-')}",
                f"**问题描述：** {payload.get('issue', '-')}",
                f"**详情：** {payload.get('detail', '-')}",
            ]
            return "\n".join(lines)

        if event_type == EventType.MONITOR_DATA_STALE:
            lines = [
                f"**异常类型：** 行情数据过期",
                f"**检测时间：** {payload.get('detected_at', '-')}",
                f"**账户：** {payload.get('account_id', '-')}",
                f"**最后数据时间：** {payload.get('last_data_time', '-')}",
                f"**错误信息：** {payload.get('sdk_error_msg', '-')}",
            ]
            return "\n".join(lines)

        # 通用格式
        lines = []
        for key, value in payload.items():
            lines.append(f"**{key}：** {value}")
        return "\n".join(lines)

    def _format_price(self, price: Any) -> str:
        """格式化价格"""
        if price is None:
            return "-"
        if isinstance(price, (int, float)):
            return f"{price:.2f}"
        return str(price)

    def _format_amount(self, amount: Any) -> str:
        """格式化金额"""
        if amount is None:
            return "-"
        if isinstance(amount, (int, float)):
            return f"{amount:.2f}"
        return str(amount)

    def reload_config(self) -> None:
        """重新加载配置（配置变更后调用）"""
        self._rule_engine.reload_rules()
        self._channel_router.reload_channels()
        self._debounce_cache.clear()

    # ── 配置 CRUD ──

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
_manager: Optional[NotificationManager] = None


def get_notification_manager() -> NotificationManager:
    """获取通知管理器单例"""
    global _manager
    if _manager is None:
        _manager = NotificationManager()
    return _manager