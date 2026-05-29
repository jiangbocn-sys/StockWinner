"""
信号分配服务

处理同一策略多个买入信号的资金分配逻辑：
- max_stocks: 单次最多买入股票数
- allocation_mode: 分配模式（equal/weighted/top_n）
- min_amount_per_stock: 单股最小金额
- max_position_pct: 单股最大仓位百分比
"""

import json
from typing import Dict, List, Any, Optional
from services.common.database import get_db_manager
from services.common.structured_logger import get_logger

logger = get_logger("signal_allocator")

# 默认分配配置
DEFAULT_ALLOCATION_CONFIG = {
    "max_stocks": 5,
    "allocation_mode": "equal",  # equal/weighted/top_n
    "min_amount_per_stock": 1000,
    "max_position_pct": 20,
    "score_field": "score",
}


class SignalAllocator:
    """信号分配器 — 多信号资金分配"""

    def __init__(self):
        self._config_cache: Dict[int, Dict] = {}

    async def get_allocation_config(self, strategy_id: int) -> Dict:
        """获取策略的分配配置"""
        if strategy_id in self._config_cache:
            return self._config_cache[strategy_id]

        db = get_db_manager()
        strategy = await db.fetchone(
            "SELECT config, allocated_capital FROM strategies WHERE id = ?",
            (strategy_id,)
        )

        if not strategy:
            return DEFAULT_ALLOCATION_CONFIG.copy()

        # 合并配置
        config = DEFAULT_ALLOCATION_CONFIG.copy()
        if strategy.get("config"):
            try:
                strategy_config = json.loads(strategy["config"]) if isinstance(strategy["config"], str) else strategy["config"]
                if "signal_allocation" in strategy_config:
                    config.update(strategy_config["signal_allocation"])
            except (json.JSONDecodeError, TypeError):
                pass

        # 添加策略可用资金
        config["strategy_capital"] = strategy.get("allocated_capital") or 0

        self._config_cache[strategy_id] = config
        return config

    async def allocate_signals(
        self,
        account_id: str,
        strategy_id: int,
        signals: List[Dict],
        market_data: Dict[str, Any],
    ) -> List[Dict]:
        """
        分配买入信号的资金和数量

        Args:
            account_id: 账户ID
            strategy_id: 策略ID
            signals: 买入信号列表 [{stock_code, stock_name, score, current_price, ...}]
            market_data: 行情数据 {stock_code: MarketData}

        Returns:
            分配后的信号列表 [{stock_code, allocated_amount, quantity, ...}]
        """
        if not signals:
            return []

        config = await self.get_allocation_config(strategy_id)
        max_stocks = config.get("max_stocks", 5)
        allocation_mode = config.get("allocation_mode", "equal")
        min_amount = config.get("min_amount_per_stock", 1000)
        max_position_pct = config.get("max_position_pct", 20)
        score_field = config.get("score_field", "score")
        strategy_capital = config.get("strategy_capital", 0)

        # 获取策略现金（实际可用）
        db = get_db_manager()
        strategy_row = await db.fetchone(
            "SELECT strategy_cash FROM strategies WHERE id = ?",
            (strategy_id,)
        )
        available_capital = strategy_row.get("strategy_cash", 0) if strategy_row else 0

        # 如果策略现金为0，回退到账户可用资金的一定比例
        if available_capital <= 0:
            account = await db.fetchone(
                "SELECT available_cash FROM accounts WHERE account_id = ?",
                (account_id,)
            )
            account_cash = account.get("available_cash", 0) if account else 0
            # 没有分配策略资金时，使用账户资金的 max_position_pct 作为上限
            available_capital = account_cash * (max_position_pct / 100.0) * max_stocks

        if available_capital <= min_amount:
            logger.log_event("allocation_skip", f"策略 {strategy_id} 可用资金不足: {available_capital}",
                            strategy_id=strategy_id, available_capital=available_capital)
            return []

        # 1. 按评分排序（降序）
        sorted_signals = sorted(
            signals,
            key=lambda s: s.get(score_field, 0),
            reverse=True
        )

        # 2. 截断到 max_stocks
        top_signals = sorted_signals[:max_stocks]

        # 3. 分配资金
        allocated = []
        if allocation_mode == "equal":
            # 均分
            per_stock_amount = available_capital / len(top_signals)
            for signal in top_signals:
                allocated.append(self._allocate_single(signal, per_stock_amount, market_data, min_amount))
        elif allocation_mode == "weighted":
            # 按评分加权
            total_score = sum(s.get(score_field, 1) for s in top_signals)
            for signal in top_signals:
                weight = signal.get(score_field, 1) / total_score if total_score > 0 else 1 / len(top_signals)
                amount = available_capital * weight
                allocated.append(self._allocate_single(signal, amount, market_data, min_amount))
        elif allocation_mode == "top_n":
            # 前N只优先，均分
            per_stock_amount = available_capital / len(top_signals)
            for signal in top_signals:
                allocated.append(self._allocate_single(signal, per_stock_amount, market_data, min_amount))
        else:
            # 默认均分
            per_stock_amount = available_capital / len(top_signals)
            for signal in top_signals:
                allocated.append(self._allocate_single(signal, per_stock_amount, market_data, min_amount))

        # 4. 过滤掉金额不足的信号
        valid_allocated = [a for a in allocated if a.get("allocated_amount", 0) >= min_amount]

        logger.log_event("allocation_done",
            f"策略 {strategy_id} 分配完成: {len(valid_allocated)}/{len(signals)} 只股票",
            strategy_id=strategy_id, signals=len(signals), allocated=len(valid_allocated),
            mode=allocation_mode, capital=available_capital)

        return valid_allocated

    def _allocate_single(
        self,
        signal: Dict,
        amount: float,
        market_data: Dict,
        min_amount: float,
    ) -> Dict:
        """分配单只股票的资金和数量"""
        stock_code = signal.get("stock_code")
        price = signal.get("current_price") or market_data.get(stock_code, {}).get("current_price", 0)

        if price <= 0:
            return {**signal, "allocated_amount": 0, "quantity": 0, "reason": "无有效价格"}

        # 计算股数（向下取整到100股）
        quantity = int((amount / price) // 100) * 100
        actual_amount = quantity * price

        if actual_amount < min_amount:
            return {**signal, "allocated_amount": 0, "quantity": 0, "reason": "金额不足"}

        return {
            **signal,
            "allocated_amount": round(actual_amount, 2),
            "quantity": quantity,
        }

    def clear_cache(self):
        """清除配置缓存"""
        self._config_cache.clear()


# 单例
_allocator: Optional[SignalAllocator] = None


def get_signal_allocator() -> SignalAllocator:
    """获取信号分配器单例"""
    global _allocator
    if _allocator is None:
        _allocator = SignalAllocator()
    return _allocator