"""
规则引擎
统一判断是否发送通知
"""

import json
from typing import Dict, Any, Optional, List
from dataclasses import dataclass

from services.common.database import get_db_manager
from services.notifications.events.payload import EventPayload
from services.notifications.events.types import EventType, EventCategory, get_event_meta


@dataclass
class RuleConfig:
    """规则配置"""
    rule_id: int
    account_id: str
    rule_name: str
    event_types: List[str]             # 适用的事件类型列表（空表示全部）
    event_categories: List[str]        # 适用的事件分类列表

    # 分类开关
    notify_on_trade: bool = True
    notify_on_signal: bool = True
    notify_on_task: bool = True
    notify_on_system: bool = True
    notify_on_risk: bool = True

    # 时间条件
    time_range_start: Optional[str] = None   # "09:00"
    time_range_end: Optional[str] = None     # "15:00"
    trading_hours_only: bool = False          # 仅交易时段发送

    # 业务条件
    signal_action_filter: Optional[str] = None  # "trade" / "watch" / None(全部)
    min_amount_threshold: float = 0            # 最小金额阈值

    # 渠道配置
    channels: List[str] = None                 # ["feishu"]
    priority: int = 1                          # 规则优先级
    enabled: bool = True


class RuleEngine:
    """规则引擎

    判断逻辑：
    1. 检查事件类型是否在规则适用范围
    2. 检查事件分类开关
    3. 检查时间条件
    4. 检查业务条件（如 signal_action）
    """

    def __init__(self):
        self._rules_cache: Dict[str, List[RuleConfig]] = {}  # account_id -> rules
        self._cache_loaded = False
        self._default_configs: Dict[str, Dict] = {}  # account_id -> notification_config

    async def _load_rules(self) -> None:
        """从数据库加载规则"""
        if self._cache_loaded:
            return

        db = get_db_manager()

        # 加载 notification_rules 表
        try:
            rows = await db.fetchall(
                "SELECT * FROM notification_rules WHERE enabled = 1 ORDER BY priority ASC"
            )
            for row in rows:
                rule = RuleConfig(
                    rule_id=row["id"],
                    account_id=row["account_id"],
                    rule_name=row.get("rule_name", "默认规则"),
                    event_types=json.loads(row.get("event_types", "[]")),
                    event_categories=json.loads(row.get("event_categories", "[]")),
                    notify_on_trade=row.get("notify_on_trade", 1) == 1,
                    notify_on_signal=row.get("notify_on_signal", 1) == 1,
                    notify_on_task=row.get("notify_on_task", 1) == 1,
                    notify_on_system=row.get("notify_on_system", 1) == 1,
                    notify_on_risk=row.get("notify_on_risk", 1) == 1,
                    time_range_start=row.get("time_range_start"),
                    time_range_end=row.get("time_range_end"),
                    trading_hours_only=row.get("trading_hours_only", 0) == 1,
                    signal_action_filter=row.get("signal_action_filter"),
                    min_amount_threshold=row.get("min_amount_threshold", 0),
                    channels=json.loads(row.get("channels", "[]")),
                    priority=row.get("priority", 1),
                    enabled=row.get("enabled", 1) == 1,
                )
                if rule.account_id not in self._rules_cache:
                    self._rules_cache[rule.account_id] = []
                self._rules_cache[rule.account_id].append(rule)
        except Exception:
            # 表不存在，使用默认逻辑
            pass

        # 加载 notification_config 表作为默认配置
        try:
            configs = await db.fetchall(
                "SELECT * FROM notification_config WHERE enabled = 1"
            )
            for config in configs:
                self._default_configs[config["account_id"]] = dict(config)
        except Exception:
            pass

        self._cache_loaded = True

    async def evaluate(self, event: EventPayload) -> bool:
        """评估是否发送通知

        Args:
            event: 事件数据

        Returns:
            True 表示应该发送，False 表示不发送
        """
        await self._load_rules()

        # 检查账户是否有自定义规则
        rules = self._rules_cache.get(event.account_id, [])

        if not rules:
            # 无规则时使用默认逻辑（从 notification_config）
            return await self._default_evaluate(event)

        # 按优先级匹配规则（priority 越小优先级越高）
        for rule in sorted(rules, key=lambda r: r.priority):
            if self._match_rule(rule, event):
                return True

        return False

    def _match_rule(self, rule: RuleConfig, event: EventPayload) -> bool:
        """检查规则是否匹配"""
        meta = event.meta
        if meta is None:
            return False

        # 1. 事件类型匹配
        if rule.event_types and event.event_type.value not in rule.event_types:
            return False

        # 2. 事件分类匹配
        if rule.event_categories and meta.category.value not in rule.event_categories:
            return False

        # 3. 分类开关检查
        category_flags = {
            EventCategory.TRADE: rule.notify_on_trade,
            EventCategory.SIGNAL: rule.notify_on_signal,
            EventCategory.TASK: rule.notify_on_task,
            EventCategory.SYSTEM: rule.notify_on_system,
            EventCategory.RISK: rule.notify_on_risk,
        }
        if not category_flags.get(meta.category, True):
            return False

        # 4. 时间条件检查
        if rule.trading_hours_only:
            from services.data.local_data_service import is_trading_hours
            if not is_trading_hours():
                return False

        if rule.time_range_start and rule.time_range_end:
            from services.common.timezone import get_china_time
            now = get_china_time()
            current_time = now.strftime("%H:%M")
            if current_time < rule.time_range_start or current_time > rule.time_range_end:
                return False

        # 5. 业务条件检查：signal_action 过滤
        if rule.signal_action_filter:
            signal_action = event.context.get("signal_action")
            if signal_action and signal_action != rule.signal_action_filter:
                return False

        # 6. 最小金额阈值（交易类事件）
        if rule.min_amount_threshold > 0 and event.event_type in (
            EventType.TRADE_EXECUTED, EventType.ORDER_REJECTED
        ):
            amount = event.payload.get("amount", 0)
            if isinstance(amount, str):
                try:
                    amount = float(amount.replace(",", ""))
                except ValueError:
                    amount = 0
            if amount < rule.min_amount_threshold:
                return False

        return True

    async def _default_evaluate(self, event: EventPayload) -> bool:
        """默认评估逻辑（无规则时，使用 notification_config）"""
        meta = event.meta
        if meta is None:
            return False

        config = self._default_configs.get(event.account_id)
        if not config:
            return False

        # 使用事件元数据中的 config_flag_key
        if meta.config_flag_key:
            if not config.get(meta.config_flag_key, 1):
                return False

        # 检查 signal_action（业务逻辑）
        signal_action = event.context.get("signal_action")
        if signal_action == "watch":
            # watch 类型默认不发送通知（除非规则明确指定）
            return False

        return True

    def reload_rules(self) -> None:
        """重新加载规则（配置变更后调用）"""
        self._rules_cache = {}
        self._default_configs = {}
        self._cache_loaded = False

    def get_rules_for_account(self, account_id: str) -> List[RuleConfig]:
        """获取账户的规则列表"""
        return self._rules_cache.get(account_id, [])


# 全局单例
_engine: Optional[RuleEngine] = None


def get_rule_engine() -> RuleEngine:
    """获取规则引擎单例"""
    global _engine
    if _engine is None:
        _engine = RuleEngine()
    return _engine