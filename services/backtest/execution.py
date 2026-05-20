"""
虚拟撮合引擎

回测用的内存级撮合引擎，模拟 A 股交易规则：
- T+1：当日买入不可卖出
- 涨跌停限制：涨停价不可买入，跌停价不可卖出
- 100股整数倍（A 股最小交易单位）
- 仓位限制：单只最大仓位、总仓位上限
- 手续费计算：佣金 + 印花税（卖出）+ 过户费
"""

import logging
from dataclasses import dataclass, field
from typing import Dict, Optional, List
from datetime import datetime

from services.common.structured_logger import get_logger

logger = get_logger("backtest")


@dataclass
class Position:
    """持仓"""
    stock_code: str
    stock_name: str = ""
    quantity: int = 0
    avg_cost: float = 0.0
    buy_date: str = ""  # 买入日期（用于 T+1 检查）
    highest_price: float = 0.0  # 最高价（用于移动止盈）
    total_cost: float = 0.0  # 总买入成本（含佣金），用于 pnl_pct 计算


@dataclass
class Trade:
    """成交记录"""
    stock_code: str
    stock_name: str = ""
    trade_type: str = ""  # 'buy' | 'sell'
    date: str = ""
    price: float = 0.0
    quantity: int = 0
    commission: float = 0.0
    pnl: float = 0.0
    pnl_pct: float = 0.0
    reason: str = ""
    buy_date: str = ""
    buy_price: float = 0.0
    holding_days: int = 0


@dataclass
class FeeConfig:
    """费率配置"""
    commission_rate: float = 0.0003  # 佣金率
    min_commission: float = 5.0       # 最低佣金
    stamp_tax: float = 0.0005        # 印花税（卖出）
    transfer_fee: float = 0.00002    # 过户费


@dataclass
class PositionLimits:
    """仓位限制"""
    max_total_position_pct: float = 0.80   # 总仓位上限
    max_single_position_pct: float = 0.15  # 单只最大仓位
    cash_reserve_pct: float = 0.10         # 现金保留比例


class BacktestExecutionEngine:
    """虚拟撮合引擎"""

    def __init__(
        self,
        initial_capital: float,
        fee_config: FeeConfig,
        position_limits: PositionLimits,
        slippage_pct: float = 0.0,
    ):
        self.cash = initial_capital
        self.initial_capital = initial_capital
        self.positions: Dict[str, Position] = {}
        self.fee_config = fee_config
        self.position_limits = position_limits
        self.slippage_pct = slippage_pct
        self.trades: List[Trade] = []

    def buy(
        self,
        stock_code: str,
        price: float,
        date: str,
        stock_name: str = "",
        prev_close: float = 0.0,
    ) -> Optional[Trade]:
        """
        模拟买入。

        Returns:
            Trade 记录，或 None（如果无法买入）
        """
        if price <= 0:
            return None

        # 涨跌停检查
        if prev_close > 0:
            limit_up = prev_close * 1.10
            if price >= limit_up:
                return None  # 涨停无法买入

        # 滑点
        fill_price = price * (1 + self.slippage_pct)

        # 计算最大可买数量（考虑仓位限制）
        total_value = self.get_total_value()
        max_single_value = total_value * self.position_limits.max_single_position_pct
        max_qty_by_value = int(max_single_value / fill_price / 100) * 100 if fill_price > 0 else 0

        # 现金限制
        max_qty_by_cash = int(self.cash / fill_price / 100) * 100 if fill_price > 0 else 0

        quantity = min(max_qty_by_value, max_qty_by_cash)
        if quantity < 100:
            return None  # 不足 100 股

        # 计算费用
        cost, commission = self._calc_buy_cost(fill_price, quantity)
        if cost > self.cash:
            # 减少数量直到费用足够
            quantity = (quantity // 100 - 1) * 100
            if quantity < 100:
                return None
            cost, commission = self._calc_buy_cost(fill_price, quantity)
            if cost > self.cash:
                return None

        # 执行买入
        self.cash -= cost

        if stock_code in self.positions:
            pos = self.positions[stock_code]
            total_cost = pos.avg_cost * pos.quantity + fill_price * quantity
            pos.quantity += quantity
            pos.avg_cost = total_cost / pos.quantity if pos.quantity > 0 else fill_price
            pos.total_cost += cost  # 累加总买入成本（含佣金）
        else:
            self.positions[stock_code] = Position(
                stock_code=stock_code,
                stock_name=stock_name,
                quantity=quantity,
                avg_cost=fill_price,
                buy_date=date,
                highest_price=fill_price,
                total_cost=cost,
            )

        trade = Trade(
            stock_code=stock_code,
            stock_name=stock_name,
            trade_type="buy",
            date=date,
            price=fill_price,
            quantity=quantity,
            commission=commission,
        )
        self.trades.append(trade)
        return trade

    def sell(
        self,
        stock_code: str,
        price: float,
        date: str,
        reason: str = "",
        prev_close: float = 0.0,
    ) -> Optional[Trade]:
        """
        模拟卖出。

        Returns:
            Trade 记录，或 None（如果无法卖出）
        """
        if stock_code not in self.positions:
            return None

        pos = self.positions[stock_code]
        if pos.quantity <= 0:
            return None

        # T+1 检查
        if pos.buy_date == date:
            return None  # 今日买入不可卖

        if price <= 0:
            return None

        # 涨跌停检查
        if prev_close > 0:
            limit_down = prev_close * 0.90
            if price <= limit_down:
                return None  # 跌停无法卖出

        # 滑点
        fill_price = price * (1 - self.slippage_pct)

        # 计算费用
        revenue, commission = self._calc_sell_revenue(fill_price, pos.quantity)

        # 执行卖出
        pnl = revenue - pos.total_cost
        buy_cost = pos.total_cost
        pnl_pct = (pnl / buy_cost * 100) if buy_cost > 0 else 0
        holding_days = self._days_between(pos.buy_date, date)

        self.cash += revenue

        trade = Trade(
            stock_code=stock_code,
            stock_name=pos.stock_name,
            trade_type="sell",
            date=date,
            price=fill_price,
            quantity=pos.quantity,
            commission=commission,
            pnl=pnl,
            pnl_pct=pnl_pct,
            reason=reason,
            buy_date=pos.buy_date,
            buy_price=pos.avg_cost,
            holding_days=holding_days,
        )
        self.trades.append(trade)

        # 更新买入记录的卖出信息
        for t in reversed(self.trades):
            if t.stock_code == stock_code and t.trade_type == "buy" and not getattr(t, "_sell_matched", False):
                t.sell_date = date
                t.sell_price = fill_price
                t.sell_commission = commission
                t.pnl = pnl
                t.pnl_pct = pnl_pct
                t.holding_days = holding_days
                t._sell_matched = True
                break

        del self.positions[stock_code]
        return trade

    def update_position_mark(self, stock_code: str, price: float):
        """更新持仓标记价格（用于移动止盈）"""
        if stock_code in self.positions:
            pos = self.positions[stock_code]
            if price > pos.highest_price:
                pos.highest_price = price

    def get_total_value(self) -> float:
        """总资产 = 现金 + 持仓市值"""
        positions_value = sum(
            pos.avg_cost * pos.quantity for pos in self.positions.values()
        )
        return self.cash + positions_value

    def get_positions_value(self, prices: Dict[str, float]) -> float:
        """持仓市值（按当前价格计算）"""
        total = 0
        for code, pos in self.positions.items():
            price = prices.get(code, pos.avg_cost)
            total += price * pos.quantity
        return total

    def get_position_count(self) -> int:
        return len(self.positions)

    def get_cash(self) -> float:
        return self.cash

    def _calc_buy_cost(self, price: float, quantity: int) -> tuple:
        """计算买入总成本（含费用）"""
        amount = price * quantity
        commission = max(amount * self.fee_config.commission_rate, self.fee_config.min_commission)
        transfer = amount * self.fee_config.transfer_fee
        return amount + commission + transfer, commission + transfer

    def _calc_sell_revenue(self, price: float, quantity: int) -> tuple:
        """计算卖出净收入（扣费用）"""
        amount = price * quantity
        commission = max(amount * self.fee_config.commission_rate, self.fee_config.min_commission)
        stamp = amount * self.fee_config.stamp_tax
        transfer = amount * self.fee_config.transfer_fee
        return amount - commission - stamp - transfer, commission + stamp + transfer

    @staticmethod
    def _days_between(date1: str, date2: str) -> int:
        if not date1 or not date2:
            return 0
        try:
            d1 = datetime.strptime(date1, "%Y-%m-%d")
            d2 = datetime.strptime(date2, "%Y-%m-%d")
            return (d2 - d1).days
        except (ValueError, TypeError):
            return 0
