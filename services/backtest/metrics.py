"""
回测绩效统计

从每日 NAV 序列和交易记录计算标准回测指标。
"""

import math
from dataclasses import dataclass
from typing import List, Dict, Optional


@dataclass
class BacktestResult:
    """回测结果汇总"""
    total_return: float = 0.0           # 总收益率
    annualized_return: float = 0.0      # 年化收益率
    max_drawdown: float = 0.0           # 最大回撤
    max_drawdown_start: str = ""        # 最大回撤起始日
    max_drawdown_end: str = ""          # 最大回撤结束日
    sharpe_ratio: float = 0.0           # 夏普比率
    calmar_ratio: float = 0.0           # 卡玛比率
    win_rate: float = 0.0               # 胜率
    profit_factor: float = 0.0          # 盈亏比
    avg_holding_days: float = 0.0       # 平均持仓天数
    total_trades: int = 0               # 总交易次数（完整买卖）
    avg_trade_return: float = 0.0       # 平均每笔交易收益
    best_trade: float = 0.0             # 最佳单笔交易
    worst_trade: float = 0.0            # 最差单笔交易
    total_commission: float = 0.0       # 总手续费
    final_nav: float = 0.0              # 最终净值
    initial_capital: float = 0.0        # 初始资金
    final_value: float = 0.0            # 最终总资产

    def to_dict(self) -> dict:
        return {
            "total_return": round(self.total_return * 100, 2),
            "annualized_return": round(self.annualized_return * 100, 2),
            "max_drawdown": round(self.max_drawdown * 100, 2),
            "max_drawdown_start": self.max_drawdown_start,
            "max_drawdown_end": self.max_drawdown_end,
            "sharpe_ratio": round(self.sharpe_ratio, 2),
            "calmar_ratio": round(self.calmar_ratio, 2),
            "win_rate": round(self.win_rate * 100, 2),
            "profit_factor": round(self.profit_factor, 2),
            "avg_holding_days": round(self.avg_holding_days, 1),
            "total_trades": self.total_trades,
            "avg_trade_return": round(self.avg_trade_return * 100, 2),
            "best_trade": round(self.best_trade * 100, 2),
            "worst_trade": round(self.worst_trade * 100, 2),
            "total_commission": round(self.total_commission, 2),
            "final_nav": round(self.final_nav, 4),
            "initial_capital": round(self.initial_capital, 2),
            "final_value": round(self.final_value, 2),
        }


class PerformanceMetrics:
    """回测绩效计算"""

    RISK_FREE_RATE = 0.02  # 无风险利率 2%

    @classmethod
    def compute(
        cls,
        nav_series: List[Dict],
        trades: List[Dict],
        initial_capital: float,
        start_date: str,
        end_date: str,
    ) -> BacktestResult:
        """
        计算回测指标。

        Args:
            nav_series: [{'trade_date': ..., 'total_value': ..., ...}, ...]
            trades: 完整的买卖交易记录
            initial_capital: 初始资金
            start_date: 回测起始日
            end_date: 回测结束日
        """
        result = BacktestResult()
        result.initial_capital = initial_capital

        # NAV 相关
        if nav_series:
            final_nav = nav_series[-1].get("total_value", initial_capital)
            result.final_value = final_nav
            result.final_nav = final_nav / initial_capital if initial_capital > 0 else 1.0
            result.total_return = (final_nav - initial_capital) / initial_capital if initial_capital > 0 else 0

            # 年化收益率
            days = cls._days_between(start_date, end_date)
            if days > 0:
                result.annualized_return = (
                    (1 + result.total_return) ** (365 / days) - 1
                )

            # 最大回撤
            result.max_drawdown, result.max_drawdown_start, result.max_drawdown_end = cls._calc_max_drawdown(nav_series)

            # 夏普比率
            result.sharpe_ratio = cls._calc_sharpe_ratio(nav_series)

            # 卡玛比率
            if abs(result.max_drawdown) > 0:
                result.calmar_ratio = result.annualized_return / abs(result.max_drawdown)

        # 交易统计
        complete_trades = cls._get_complete_trades(trades)
        result.total_trades = len(complete_trades)

        if complete_trades:
            pnls = [t.get("pnl", 0) for t in complete_trades]
            wins = [p for p in pnls if p > 0]
            losses = [p for p in pnls if p < 0]

            result.win_rate = len(wins) / len(pnls) if pnls else 0
            result.profit_factor = (
                sum(wins) / abs(sum(losses)) if losses and sum(losses) != 0
                else float("inf") if wins else 0
            )

            trade_returns = [t.get("pnl_pct", 0) / 100 for t in complete_trades]
            result.avg_trade_return = sum(trade_returns) / len(trade_returns) if trade_returns else 0
            result.best_trade = max(trade_returns) if trade_returns else 0
            result.worst_trade = min(trade_returns) if trade_returns else 0

            holding_days = [t.get("holding_days", 0) for t in complete_trades if t.get("holding_days", 0) > 0]
            result.avg_holding_days = sum(holding_days) / len(holding_days) if holding_days else 0

        # 总手续费
        result.total_commission = sum(
            t.get("commission", 0) + t.get("buy_commission", 0) for t in trades
        )

        return result

    @staticmethod
    def _calc_max_drawdown(nav_series: List[Dict]) -> tuple:
        """
        计算最大回撤及其起止日期。

        Returns:
            (max_drawdown, peak_date, trough_date)
        """
        if not nav_series:
            return 0, "", ""

        max_dd = 0.0
        peak_date = ""
        trough_date = ""
        peak_value = nav_series[0].get("total_value", 0)
        peak_dt = nav_series[0].get("trade_date", "")

        for item in nav_series:
            value = item.get("total_value", 0)
            date = item.get("trade_date", "")

            if value > peak_value:
                peak_value = value
                peak_dt = date

            dd = (peak_value - value) / peak_value if peak_value > 0 else 0
            if dd > max_dd:
                max_dd = dd
                peak_date = peak_dt
                trough_date = date

        return max_dd, peak_date, trough_date

    @staticmethod
    def _calc_sharpe_ratio(nav_series: List[Dict]) -> float:
        """
        计算夏普比率（年化）。

        Sharpe = (Rp - Rf) / σp
        Rp: 年化收益率, Rf: 无风险利率, σp: 年化波动率
        """
        if len(nav_series) < 2:
            return 0.0

        # 计算日收益率
        returns = []
        for i in range(1, len(nav_series)):
            prev = nav_series[i-1].get("total_value", 0)
            curr = nav_series[i].get("total_value", 0)
            if prev > 0:
                returns.append((curr - prev) / prev)

        if not returns:
            return 0.0

        avg_return = sum(returns) / len(returns)
        variance = sum((r - avg_return) ** 2 for r in returns) / max(len(returns) - 1, 1)
        std_daily = math.sqrt(variance) if variance > 0 else 0

        if std_daily == 0:
            return 0.0

        # 年化
        daily_rf = PerformanceMetrics.RISK_FREE_RATE / 252
        sharpe = (avg_return - daily_rf) / std_daily * math.sqrt(252)
        return sharpe

    @staticmethod
    def _get_complete_trades(trades: List[Dict]) -> List[Dict]:
        """
        配对的完整买卖交易。

        将买入和卖出记录配对，返回有 pnl 数据的完整交易。
        """
        complete = []
        for t in trades:
            if t.get("trade_type") == "sell" and t.get("pnl") is not None:
                complete.append(t)
        return complete

    @staticmethod
    def _days_between(date1: str, date2: str) -> int:
        from datetime import datetime
        try:
            d1 = datetime.strptime(date1, "%Y-%m-%d")
            d2 = datetime.strptime(date2, "%Y-%m-%d")
            return (d2 - d1).days
        except (ValueError, TypeError):
            return 0
