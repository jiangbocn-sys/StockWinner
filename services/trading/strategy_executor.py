"""
交易策略执行器

从数据库读取 trading_strategy_config，对给定股票评估触发条件，返回执行决策。
"""

import json
from typing import Any, Dict, List, Optional

from services.common.database import get_db_manager
from services.trading.trigger_evaluators import get_evaluator, EVALUATOR_REGISTRY


class StrategyExecutor:
    """交易策略执行器"""

    def __init__(self, account_id: str):
        self.account_id = account_id
        self.db = get_db_manager()

    async def load_strategies(self, enabled_only: bool = True) -> List[Dict]:
        """加载账户的交易策略配置"""
        where = "account_id = ?"
        params: list = [self.account_id]

        if enabled_only:
            where += " AND enabled = 1"

        rows = await self.db.fetchall(
            f"SELECT * FROM trading_strategy_config WHERE {where}",
            tuple(params),
        )
        return rows

    async def evaluate_all(
        self,
        strategies: List[Dict],
        stock_codes: List[str],
    ) -> List[Dict]:
        """
        评估所有策略对指定股票的触发情况

        Args:
            strategies: 策略配置列表
            stock_codes: 股票代码列表

        Returns:
            触发决策列表，每个元素包含：
            - strategy_id, strategy_name, stock_code
            - action (buy/sell)
            - trigger_data (触发时的行情数据)
        """
        decisions = []

        for strategy in strategies:
            # 确定目标股票
            target_stocks = self._parse_target_stocks(strategy.get("target_stocks"))
            stocks_to_check = target_stocks if target_stocks else stock_codes

            # 解析条件
            conditions = self._parse_conditions(strategy.get("conditions"))
            if not conditions:
                continue

            # 获取策略类型
            strategy_type = strategy.get("strategy_type", "")

            # 对每只股票评估
            for stock_code in stocks_to_check:
                # 获取实时行情
                market_data = await self._get_market_data(stock_code)
                if not market_data:
                    continue

                # 获取 K 线数据（用于量比等需要历史的评估器）
                kline_data = await self._get_kline_data(stock_code)

                # 评估条件
                for condition in conditions:
                    cond_type = condition.get("type", "")
                    if cond_type not in EVALUATOR_REGISTRY:
                        continue

                    evaluator = get_evaluator(cond_type)
                    triggered = await evaluator.evaluate(
                        stock_code=stock_code,
                        market_data=market_data,
                        kline_data=kline_data,
                        condition=condition,
                    )

                    if triggered:
                        decisions.append({
                            "strategy_id": strategy["id"],
                            "strategy_name": strategy["name"],
                            "strategy_type": strategy_type,
                            "stock_code": stock_code,
                            "stock_name": market_data.get("stock_name", ""),
                            "action": strategy.get("action", "buy"),
                            "trigger_data": market_data,
                            "condition": condition,
                        })
                        # 一个策略对一只股票只触发一次
                        break

        return decisions

    def _parse_target_stocks(self, target_stocks_str: Optional[str]) -> List[str]:
        """解析目标股票列表"""
        if not target_stocks_str:
            return []
        try:
            data = json.loads(target_stocks_str)
            if isinstance(data, list):
                return data
            elif isinstance(data, str):
                return [data]
        except (json.JSONDecodeError, TypeError):
            pass
        return []

    def _parse_conditions(self, conditions_str: Optional[str]) -> List[Dict]:
        """解析条件列表"""
        if not conditions_str:
            return []
        try:
            data = json.loads(conditions_str)
            if isinstance(data, list):
                return data
            elif isinstance(data, dict):
                return [data]
        except (json.JSONDecodeError, TypeError):
            pass
        return []

    async def _get_market_data(self, stock_code: str) -> Optional[Dict]:
        """获取实时行情数据"""
        try:
            from services.trading.gateway import get_gateway, MarketData
            gateway = await get_gateway()
            market_data: Optional[MarketData] = await gateway.get_market_data(stock_code)
            if market_data:
                return {
                    "stock_code": stock_code,
                    "stock_name": getattr(market_data, 'stock_name', ''),
                    "current_price": market_data.current_price,
                    "change_pct": getattr(market_data, 'change_pct', 0),
                    "volume": getattr(market_data, 'volume', 0),
                    "open": getattr(market_data, 'open', 0),
                    "high": getattr(market_data, 'high', 0),
                    "low": getattr(market_data, 'low', 0),
                    "prev_close": getattr(market_data, 'prev_close', 0),
                }
        except Exception as e:
            print(f"获取行情数据失败 {stock_code}: {e}")
        return None

    async def _get_kline_data(self, stock_code: str) -> Dict[str, Any]:
        """获取 K 线数据（最近 10 天日 K）"""
        try:
            from services.common.timezone import get_china_time
            from services.trading.gateway import get_gateway
            from datetime import datetime, timedelta

            today = get_china_time().strftime("%Y%m%d")
            start = (get_china_time() - timedelta(days=15)).strftime("%Y%m%d")

            gateway = await get_gateway()
            klines = await gateway.get_kline_data(
                stock_code, period="day", start_date=start, end_date=today
            )
            return {"klines": klines or []}
        except Exception as e:
            print(f"获取 K 线数据失败 {stock_code}: {e}")
        return {"klines": []}


def get_strategy_executor(account_id: str) -> StrategyExecutor:
    """获取策略执行器实例"""
    return StrategyExecutor(account_id)
