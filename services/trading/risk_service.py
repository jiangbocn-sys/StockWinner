"""
交易风控服务 (Risk Management)

每笔交易执行前的风控检查：
1. 单笔仓位限制（max_single_position_pct）
2. 总仓位限制（max_total_position_pct）
3. 现金储备要求（cash_reserve_pct）
4. 当日亏损上限（daily_loss_limit，可选）
"""

from typing import Dict, Optional, Tuple
from services.common.database import get_db_manager


class RiskService:
    """交易风控服务"""

    def __init__(self, account_id: str):
        self.account_id = account_id
        self.db = get_db_manager()

    async def check_buy(
        self,
        stock_code: str,
        price: float,
        quantity: int,
    ) -> Tuple[bool, str]:
        """
        买入前风控检查

        Returns:
            (通过, 原因) — 通过返回 (True, "")
        """
        db = self.db

        # 获取账户信息
        account = await db.fetchone(
            "SELECT available_cash, max_total_position_pct, max_single_position_pct, cash_reserve_pct FROM accounts WHERE account_id = ?",
            (self.account_id,),
        )
        if not account:
            return False, "账户不存在"

        available_cash = account.get("available_cash", 0)
        max_total_pct = account.get("max_total_position_pct", 0.80)
        max_single_pct = account.get("max_single_position_pct", 0.15)
        cash_reserve_pct = account.get("cash_reserve_pct", 0.20)

        trade_amount = price * quantity

        # 1. 现金储备检查
        total_assets = available_cash  # 当前可用资金
        # 获取当前持仓市值
        positions = await db.fetchall(
            "SELECT SUM(market_value) as total_mv FROM stock_positions WHERE account_id = ?",
            (self.account_id,),
        )
        current_mv = positions[0]["total_mv"] if positions and positions[0]["total_mv"] else 0
        total_assets = current_mv + available_cash

        # 买入后总持仓
        new_position_value = price * quantity
        new_total_mv = current_mv + new_position_value
        new_cash = available_cash - trade_amount

        # 检查 1：单笔仓位不超过上限
        if total_assets > 0 and new_position_value / total_assets > max_single_pct:
            return False, f"单笔仓位超限（{new_position_value/total_assets*100:.1f}% > {max_single_pct*100:.0f}%）"

        # 检查 2：总仓位不超过上限
        if total_assets > 0 and new_total_mv / total_assets > max_total_pct:
            return False, f"总仓位超限（{new_total_mv/total_assets*100:.1f}% > {max_total_pct*100:.0f}%）"

        # 检查 3：现金储备不低于要求
        if total_assets > 0 and new_cash / total_assets < cash_reserve_pct:
            return False, f"现金储备不足（{new_cash/total_assets*100:.1f}% < {cash_reserve_pct*100:.0f}%）"

        return True, ""

    async def check_sell(
        self,
        stock_code: str,
        quantity: int,
    ) -> Tuple[bool, str]:
        """
        卖出前风控检查（主要检查持仓是否存在且数量足够）
        """
        db = self.db
        position = await db.fetchone(
            "SELECT quantity, available_quantity FROM stock_positions WHERE account_id = ? AND stock_code = ?",
            (self.account_id, stock_code),
        )
        if not position:
            return False, "无此持仓"

        available = position.get("available_quantity", 0)
        if quantity > 0 and quantity > available:
            return False, f"可用持仓不足（可用 {available}，需卖出 {quantity}）"

        return True, ""

    async def check_strategy_position(
        self,
        stock_code: str,
        price: float,
        quantity: int,
        strategy_id: int,
    ) -> Tuple[bool, str]:
        """
        策略级持仓上限检查

        检查该策略下所有持仓股的总市值是否超过策略配置的 max_position_amount。

        Returns:
            (通过, 原因) — 通过返回 (True, "")
        """
        db = self.db

        # 读取策略配置
        strategy = await db.fetchone(
            "SELECT name, config FROM strategies WHERE id = ? AND account_id = ?",
            (strategy_id, self.account_id),
        )
        if not strategy:
            return True, ""  # 策略不存在或不属此账户，不拦截

        import json
        config = strategy.get("config", {})
        if isinstance(config, str):
            try:
                config = json.loads(config)
            except (json.JSONDecodeError, TypeError):
                config = {}

        max_amount = config.get("max_position_amount")
        if not max_amount or max_amount <= 0:
            return True, ""  # 未配置上限，不拦截

        # 查询该策略下当前持仓总市值
        positions = await db.fetchall(
            "SELECT SUM(market_value) as strategy_mv FROM stock_positions WHERE account_id = ? AND strategy_id = ? AND quantity > 0",
            (self.account_id, strategy_id),
        )
        current_strategy_mv = positions[0]["strategy_mv"] if positions and positions[0]["strategy_mv"] else 0

        # 买入后该策略的新持仓市值
        new_strategy_mv = current_strategy_mv + price * quantity

        if new_strategy_mv > max_amount:
            strategy_name = strategy.get("name", str(strategy_id))
            return False, f"策略「{strategy_name}」持仓市值将达 ¥{new_strategy_mv:,.0f}，超过上限 ¥{max_amount:,.0f}"

        return True, ""


# 全局单例
_risk_services: Dict[str, RiskService] = {}


def get_risk_service(account_id: str) -> RiskService:
    """获取风控服务实例（每账户一个）"""
    if account_id not in _risk_services:
        _risk_services[account_id] = RiskService(account_id)
    return _risk_services[account_id]
