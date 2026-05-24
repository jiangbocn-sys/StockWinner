"""
收益率累积模式

信号快速计算：
1. 预生成所有买卖信号（不逐日撮合）
2. 按信号配对计算每笔交易收益率
3. 累积收益率曲线

适用场景：快速筛选策略，不需要精确的现金/仓位模拟
"""

import logging
from typing import Dict, List, Optional, Any
from datetime import datetime

from services.backtest.metrics import PerformanceMetrics, BacktestResult
from services.screening.condition_parser import get_condition_parser, normalize_conditions
from services.factors.kline_manager import get_kline_manager
from services.common.structured_logger import get_logger

logger = get_logger("backtest")


class ReturnAccumulationEngine:
    """收益率累积引擎"""

    def __init__(
        self,
        strategy_config: Dict,
        initial_capital: float,
        start_date: str,
        end_date: str,
        stock_pool: Optional[List[str]] = None,
        stop_loss_pct: Optional[float] = None,
        take_profit_pct: Optional[float] = None,
        holding_period: int = 5,  # 默认持有5个交易日
    ):
        self.strategy_config = strategy_config
        self.initial_capital = initial_capital
        self.start_date = start_date
        self.end_date = end_date
        self.stock_pool = stock_pool
        self.stop_loss_pct = stop_loss_pct
        self.take_profit_pct = take_profit_pct
        self.holding_period = holding_period

        self.buy_conditions = self._extract_buy_conditions(strategy_config)
        self.sell_conditions = strategy_config.get("sell_conditions", [])

        self.km = get_kline_manager()
        self.parser = get_condition_parser()

        self.signals: List[Dict] = []
        self.nav_series: List[Dict] = []
        self.trades: List[Dict] = []

        # 股票名称映射
        self._stock_name_map: Dict[str, str] = {}
        self._load_stock_names()

    def _load_stock_names(self):
        """从 kline.db 的 stock_base_info 表加载股票名称"""
        from services.common.database import get_sync_connection
        try:
            conn = get_sync_connection("kline")
            cursor = conn.cursor()
            cursor.execute("SELECT stock_code, stock_name FROM stock_base_info")
            self._stock_name_map = {row[0]: row[1].strip() for row in cursor.fetchall()}
        except Exception:
            pass

    def run(self, progress_callback=None) -> Dict:
        """
        执行收益率累积回测。

        Returns:
            {
                "result": BacktestResult.to_dict(),
                "trades": [...],
                "nav_series": [...],
            }
        """
        # 1. 获取交易日序列
        trade_dates = self.km.get_all_trade_dates(self.start_date, self.end_date)
        if not trade_dates:
            return {"error": f"时间范围内无交易日: {self.start_date} ~ {self.end_date}"}

        # 2. 确定股票池
        if not self.stock_pool:
            self.stock_pool = self.km.get_all_stocks()

        # 3. 生成买卖信号
        self._generate_signals(trade_dates)

        # 4. 信号配对，计算每笔收益
        self._pair_signals()

        # 5. 构建 NAV 序列
        self._build_nav_series(trade_dates)

        # 5.5 回测结束，清仓所有未卖出的持仓（按最后一天收盘价）
        self._liquidate_remaining(trade_dates)

        # 6. 计算绩效指标
        result = PerformanceMetrics.compute(
            nav_series=self.nav_series,
            trades=self.trades,
            initial_capital=self.initial_capital,
            start_date=self.start_date,
            end_date=self.end_date,
        )

        return {
            "result": result.to_dict(),
            "trades": self.trades,
            "nav_series": self.nav_series,
            "daily_positions": [],
        }

    def _generate_signals(self, trade_dates: List[str]):
        """预生成所有买卖信号"""
        pool_set = set(self.stock_pool)

        for i, trade_date in enumerate(trade_dates):
            # 获取当日收盘价
            prices = self._get_daily_prices(trade_date)
            for code, price in prices.items():
                if code not in pool_set:
                    continue

                indicators = self._get_stock_indicators(code, trade_date, price)
                if not indicators:
                    continue

                # 买入信号
                if self.buy_conditions:
                    normalized_buy_cond = normalize_conditions(self.buy_conditions)
                    if self.parser.evaluate(normalized_buy_cond, indicators):
                        self.signals.append({
                            "stock_code": code,
                            "date": trade_date,
                            "signal_type": "buy",
                            "price": price,
                            "stock_name": indicators.get("stock_name", code),
                        })

                # 卖出信号
                if self.sell_conditions:
                    normalized_sell_cond = normalize_conditions(self.sell_conditions)
                    if self.parser.evaluate(normalized_sell_cond, indicators):
                        self.signals.append({
                            "stock_code": code,
                            "date": trade_date,
                            "signal_type": "sell",
                            "price": price,
                        })

    def _pair_signals(self):
        """配对买卖信号，计算收益率"""
        # 按股票分组
        from collections import defaultdict
        by_stock = defaultdict(list)
        for s in self.signals:
            by_stock[s["stock_code"]].append(s)

        for code, stock_signals in by_stock.items():
            stock_signals.sort(key=lambda x: x["date"])

            pending_buy = None
            for sig in stock_signals:
                if sig["signal_type"] == "buy" and pending_buy is None:
                    pending_buy = sig
                elif sig["signal_type"] == "sell" and pending_buy is not None:
                    # 配对成功
                    buy_price = pending_buy["price"]
                    sell_price = sig["price"]
                    pnl = sell_price - buy_price
                    pnl_pct = (pnl / buy_price * 100) if buy_price > 0 else 0

                    holding_days = self._days_between(pending_buy["date"], sig["date"])

                    trade = {
                        "stock_code": code,
                        "stock_name": pending_buy.get("stock_name", code),
                        "trade_type": "sell",
                        "date": sig["date"],
                        "price": sell_price,
                        "quantity": 100,  # 标准化数量
                        "commission": 0,
                        "pnl": pnl,
                        "pnl_pct": pnl_pct,
                        "reason": "策略卖出",
                        "buy_date": pending_buy["date"],
                        "buy_price": buy_price,
                        "holding_days": holding_days,
                    }
                    self.trades.append(trade)
                    pending_buy = None

                # 固定止盈止损检查
                elif pending_buy is not None:
                    buy_price = pending_buy["price"]
                    current_price = sig["price"]

                    if self.stop_loss_pct and current_price <= buy_price * (1 - self.stop_loss_pct):
                        # 止损
                        pnl = current_price - buy_price
                        pnl_pct = (pnl / buy_price * 100) if buy_price > 0 else 0
                        holding_days = self._days_between(pending_buy["date"], sig["date"])
                        self.trades.append({
                            "stock_code": code,
                            "stock_name": pending_buy.get("stock_name", code),
                            "trade_type": "sell",
                            "date": sig["date"],
                            "price": current_price,
                            "quantity": 100,
                            "commission": 0,
                            "pnl": pnl,
                            "pnl_pct": pnl_pct,
                            "reason": "止损",
                            "buy_date": pending_buy["date"],
                            "buy_price": buy_price,
                            "holding_days": holding_days,
                        })
                        pending_buy = None

                    elif self.take_profit_pct and current_price >= buy_price * (1 + self.take_profit_pct):
                        # 止盈
                        pnl = current_price - buy_price
                        pnl_pct = (pnl / buy_price * 100) if buy_price > 0 else 0
                        holding_days = self._days_between(pending_buy["date"], sig["date"])
                        self.trades.append({
                            "stock_code": code,
                            "stock_name": pending_buy.get("stock_name", code),
                            "trade_type": "sell",
                            "date": sig["date"],
                            "price": current_price,
                            "quantity": 100,
                            "commission": 0,
                            "pnl": pnl,
                            "pnl_pct": pnl_pct,
                            "reason": "止盈",
                            "buy_date": pending_buy["date"],
                            "buy_price": buy_price,
                            "holding_days": holding_days,
                        })
                        pending_buy = None

    def _liquidate_remaining(self, trade_dates: List[str]):
        """回测结束，清仓所有未配对买入信号（按最后一天收盘价）"""
        if not trade_dates:
            return

        last_date = trade_dates[-1]
        prices = self._get_daily_prices(last_date)

        # 找出所有未配对的买入信号（在 _pair_signals 中 pending_buy 被重置为 None 的已配对）
        # 重新扫描信号找出未配对的 buy
        from collections import defaultdict
        by_stock = defaultdict(list)
        for s in self.signals:
            by_stock[s["stock_code"]].append(s)

        for code, stock_signals in by_stock.items():
            stock_signals.sort(key=lambda x: x["date"])
            pending_buy = None
            for sig in stock_signals:
                if sig["signal_type"] == "buy" and pending_buy is None:
                    pending_buy = sig
                elif sig["signal_type"] == "sell" and pending_buy is not None:
                    pending_buy = None  # 配对成功，清空
                # 止盈止损也会清空 pending_buy

            # 如果还有未配对的买入信号，按最后一天收盘价清仓
            if pending_buy is not None:
                sell_price = prices.get(code, pending_buy["price"])
                buy_price = pending_buy["price"]
                pnl = sell_price - buy_price
                pnl_pct = (pnl / buy_price * 100) if buy_price > 0 else 0
                holding_days = self._days_between(pending_buy["date"], last_date)
                self.trades.append({
                    "stock_code": code,
                    "stock_name": pending_buy.get("stock_name", code),
                    "trade_type": "sell",
                    "date": last_date,
                    "price": sell_price,
                    "quantity": 100,
                    "commission": 0,
                    "pnl": pnl,
                    "pnl_pct": pnl_pct,
                    "reason": "回测清仓",
                    "buy_date": pending_buy["date"],
                    "buy_price": buy_price,
                    "holding_days": holding_days,
                })

    def _build_nav_series(self, trade_dates: List[str]):
        """构建 NAV 序列"""
        cumulative_return = 0.0
        peak_nav = 1.0

        # 按日期聚合收益
        daily_pnl = {}
        for t in self.trades:
            date = t["date"]
            daily_pnl.setdefault(date, 0)
            daily_pnl[date] += t.get("pnl_pct", 0) / 100  # 百分比转小数

        max_dd = 0.0
        for trade_date in trade_dates:
            if trade_date in daily_pnl:
                cumulative_return += daily_pnl[trade_date]

            nav = 1 + cumulative_return
            if nav > peak_nav:
                peak_nav = nav
            dd = (peak_nav - nav) / peak_nav if peak_nav > 0 else 0
            if dd > max_dd:
                max_dd = dd

            total_value = self.initial_capital * nav
            self.nav_series.append({
                "trade_date": trade_date,
                "nav": nav,
                "total_value": total_value,
                "cash": total_value,
                "positions_value": 0,
                "drawdown": dd,
                "max_drawdown": max_dd,
                "daily_return": daily_pnl.get(trade_date, 0),
            })

    def _get_daily_prices(self, trade_date: str) -> Dict[str, float]:
        """获取指定交易日所有股票的收盘价"""
        conn = self.km._conn()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT stock_code, close FROM kline_data WHERE trade_date = ?",
            (trade_date,)
        )
        return {row[0]: float(row[1]) for row in cursor.fetchall() if row[1]}

    def _get_stock_indicators(self, stock_code, trade_date, current_price):
        """获取单只股票在指定日期的技术指标"""
        from datetime import datetime, timedelta
        try:
            dt = datetime.strptime(trade_date, "%Y-%m-%d")
            start_dt = dt - timedelta(days=120)
            start_str = start_dt.strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            start_str = None

        kline = self.km.get_kline_data(stock_code, start_date=start_str, end_date=trade_date)
        if len(kline) > 60:
            kline = kline.iloc[-60:].reset_index(drop=True)
        if kline is None or len(kline) < 26:
            return {}

        closes = kline["close"].tolist() if hasattr(kline, "__getitem__") else [r["close"] for r in kline] if isinstance(kline, list) else []
        if len(closes) < 26:
            return {}

        highs = kline["high"].tolist() if hasattr(kline, "__getitem__") else [r.get("high", r["close"]) for r in kline] if isinstance(kline, list) else []
        lows = kline["low"].tolist() if hasattr(kline, "__getitem__") else [r.get("low", r["close"]) for r in kline] if isinstance(kline, list) else []
        volumes = kline["volume"].tolist() if hasattr(kline, "__getitem__") else [r.get("volume", 0) for r in kline] if isinstance(kline, list) else []

        from services.common.technical_indicators import calculate_indicators_for_screening

        indicators = calculate_indicators_for_screening(closes, highs, lows, volumes)
        indicators["price"] = current_price
        indicators["PRICE"] = current_price

        # 获取股票名称（从 stock_base_info）
        indicators["stock_name"] = self._stock_name_map.get(stock_code, stock_code)

        return indicators

    def _extract_buy_conditions(self, config):
        buy_conditions = config.get("buy_conditions", [])
        if not buy_conditions:
            buy_conditions = config.get("buy", [])
        if not buy_conditions:
            conditions = config.get("conditions", {})
            buy_conditions = conditions.get("buy", [])
        return buy_conditions

    @staticmethod
    def _days_between(date1: str, date2: str) -> int:
        try:
            d1 = datetime.strptime(date1, "%Y-%m-%d")
            d2 = datetime.strptime(date2, "%Y-%m-%d")
            return (d2 - d1).days
        except (ValueError, TypeError):
            return 0
