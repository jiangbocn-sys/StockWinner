"""
回测绩效统计

从每日 NAV 序列和交易记录计算标准回测指标。
"""

import math
from dataclasses import dataclass, field
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

    # 基准对比（沪深300）
    benchmark_return: float = 0.0       # 基准收益率
    benchmark_annualized: float = 0.0   # 基准年化收益率
    alpha: float = 0.0                  # 超额收益
    beta: float = 0.0                   # Beta 系数

    # 分年度统计
    yearly_returns: List[Dict] = field(default_factory=list)  # [{'year': 2024, 'return': 0.15, 'max_drawdown': 0.1, ...}]

    def to_dict(self) -> dict:
        def safe_round(v, n=2):
            if v is None or math.isinf(v) or math.isnan(v):
                return None
            return round(v, n)
        d = {
            "total_return": safe_round(self.total_return * 100),
            "annualized_return": safe_round(self.annualized_return * 100),
            "max_drawdown": safe_round(self.max_drawdown * 100),
            "max_drawdown_start": self.max_drawdown_start,
            "max_drawdown_end": self.max_drawdown_end,
            "sharpe_ratio": safe_round(self.sharpe_ratio),
            "calmar_ratio": safe_round(self.calmar_ratio),
            "win_rate": safe_round(self.win_rate * 100),
            "profit_factor": safe_round(self.profit_factor),
            "avg_holding_days": safe_round(self.avg_holding_days, 1),
            "total_trades": self.total_trades,
            "avg_trade_return": safe_round(self.avg_trade_return * 100),
            "best_trade": safe_round(self.best_trade * 100),
            "worst_trade": safe_round(self.worst_trade * 100),
            "total_commission": safe_round(self.total_commission),
            "final_nav": safe_round(self.final_nav, 4),
            "initial_capital": safe_round(self.initial_capital),
            "final_value": safe_round(self.final_value),
        }
        # 基准对比
        if self.benchmark_return is not None:
            d["benchmark_return"] = safe_round(self.benchmark_return * 100)
            d["benchmark_annualized"] = safe_round(self.benchmark_annualized * 100)
            d["alpha"] = safe_round(self.alpha * 100)
            d["beta"] = safe_round(self.beta)
        # 分年度
        if self.yearly_returns:
            d["yearly_returns"] = [
                {**yr, "return": safe_round(yr.get("return", 0) * 100), "max_drawdown": safe_round(yr.get("max_drawdown", 0) * 100)}
                for yr in self.yearly_returns
            ]
        return d


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
        benchmark_series: Optional[List[Dict]] = None,  # [{'trade_date': ..., 'close': ...}]
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

        # 基准对比（沪深300）
        if benchmark_series and len(benchmark_series) > 1:
            cls._calc_benchmark(result, benchmark_series, nav_series)

        # 分年度统计
        if nav_series:
            result.yearly_returns = cls._calc_yearly_returns(nav_series)

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

    @classmethod
    def _calc_benchmark(cls, result: BacktestResult, benchmark_series: List[Dict], nav_series: Optional[List[Dict]] = None):
        """计算基准（沪深300）收益率、年化、alpha、beta"""
        if not benchmark_series or not nav_series:
            return

        first_close = benchmark_series[0].get("close", 0)
        last_close = benchmark_series[-1].get("close", 0)
        if first_close <= 0:
            return

        result.benchmark_return = (last_close - first_close) / first_close

        # 基准年化
        days = cls._days_between(benchmark_series[0].get("trade_date", ""), benchmark_series[-1].get("trade_date", ""))
        if days > 0:
            result.benchmark_annualized = (1 + result.benchmark_return) ** (365 / days) - 1

        # Alpha = 策略年化 - 基准年化
        result.alpha = result.annualized_return - result.benchmark_annualized

        # Beta = Cov(strategy_ret, bench_ret) / Var(bench_ret)
        nav_map = {item["trade_date"]: item["total_value"] for item in nav_series}
        bench_map = {b["trade_date"]: b["close"] for b in benchmark_series}
        common_dates = sorted(set(nav_map.keys()) & set(bench_map.keys()))

        if len(common_dates) > 2:
            strat_rets = []
            bench_rets = []
            for i in range(1, len(common_dates)):
                prev_d, curr_d = common_dates[i-1], common_dates[i]
                sr = (nav_map[curr_d] - nav_map[prev_d]) / nav_map[prev_d]
                br = (bench_map[curr_d] - bench_map[prev_d]) / bench_map[prev_d]
                strat_rets.append(sr)
                bench_rets.append(br)

            if strat_rets and bench_rets:
                mean_s = sum(strat_rets) / len(strat_rets)
                mean_b = sum(bench_rets) / len(bench_rets)
                cov = sum((s - mean_s) * (b - mean_b) for s, b in zip(strat_rets, bench_rets)) / (len(strat_rets) - 1)
                var_b = sum((b - mean_b) ** 2 for b in bench_rets) / (len(bench_rets) - 1)
                result.beta = cov / var_b if var_b > 0 else 1.0

    @staticmethod
    def _calc_yearly_returns(nav_series: List[Dict]) -> List[Dict]:
        """分年度统计"""
        from datetime import datetime
        yearly = {}
        for item in nav_series:
            d = item.get("trade_date", "")
            if not d:
                continue
            try:
                year = int(d[:4])
            except (ValueError, IndexError):
                continue
            value = item.get("total_value", 0)
            drawdown = item.get("drawdown", 0)
            if year not in yearly:
                yearly[year] = {"year": year, "start_value": value, "end_value": value, "max_drawdown": 0, "max_value": value}
            yr = yearly[year]
            yr["end_value"] = value
            yr["max_value"] = max(yr["max_value"], value)
            yr["max_drawdown"] = max(yr["max_drawdown"], drawdown)

        result = []
        for year in sorted(yearly.keys()):
            yr = yearly[year]
            yr_return = (yr["end_value"] - yr["start_value"]) / yr["start_value"] if yr["start_value"] > 0 else 0
            result.append({
                "year": year,
                "return": yr_return,
                "max_drawdown": yr["max_drawdown"],
            })
        return result
