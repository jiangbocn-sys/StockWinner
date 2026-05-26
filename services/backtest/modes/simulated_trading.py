"""
撮合模拟盘模式

逐日推进回测：
1. 获取 [start_date, end_date] 交易日序列
2. 对每个交易日：加载当日收盘价 → 检查持仓卖出信号 → 执行选股买入信号 → 记录净值
3. 支持固定止盈止损 / 移动止盈 / 策略代码型卖出信号
"""

import logging
from typing import Dict, List, Optional, Any, Callable
from datetime import datetime

from services.backtest.execution import (
    BacktestExecutionEngine, Trade, Position, FeeConfig, PositionLimits
)
from services.backtest.metrics import PerformanceMetrics, BacktestResult
from services.screening.condition_parser import get_condition_parser, normalize_conditions
from services.factors.kline_manager import get_kline_manager
from services.common.technical_indicators import calculate_indicators_for_screening
from services.common.structured_logger import get_logger

logger = get_logger("backtest")


class SimulatedTradingEngine:
    """撮合模拟盘引擎"""

    def __init__(
        self,
        strategy_config: Dict,
        initial_capital: float,
        start_date: str,
        end_date: str,
        stock_pool: Optional[List[str]] = None,
        fee_config: Optional[FeeConfig] = None,
        position_limits: Optional[PositionLimits] = None,
        slippage_pct: float = 0.0,
        stop_loss_pct: Optional[float] = None,
        take_profit_pct: Optional[float] = None,
        trailing_stop_pct: Optional[float] = None,
        stop_execution_price: str = "close",
        initial_positions: Optional[List[Dict]] = None,
        initial_cash: Optional[float] = None,
        liquidate_at_end: bool = True,
    ):
        self.strategy_config = strategy_config
        self.initial_capital = initial_capital
        self.start_date = start_date
        self.end_date = end_date
        self.stock_pool = stock_pool
        self.stop_loss_pct = stop_loss_pct
        self.take_profit_pct = take_profit_pct
        self.trailing_stop_pct = trailing_stop_pct
        self.stop_execution_price = stop_execution_price  # "close" | "trigger"
        self.initial_positions = initial_positions  # 分段回测用：上一段继承的持仓
        self.initial_cash = initial_cash  # 分段回测用：上一段继承的现金
        self.liquidate_at_end = liquidate_at_end  # 是否在段末清仓
        self._pending_signals: List[Dict] = []  # 当日策略信号，次日执行

        # 策略类型检测
        self.is_code_strategy = strategy_config.get("strategy_type") == "python"

        if self.is_code_strategy:
            from services.strategy.engine import get_strategy_engine
            self.strategy_engine = get_strategy_engine()
        else:
            self.parser = get_condition_parser()

        # 买入条件（配置型策略用）
        self.buy_conditions = self._extract_buy_conditions(strategy_config) if not self.is_code_strategy else None

        # 撮合引擎
        self.execution = BacktestExecutionEngine(
            initial_capital=initial_capital,
            fee_config=fee_config or FeeConfig(),
            position_limits=position_limits or PositionLimits(),
            slippage_pct=slippage_pct,
        )

        # 结果
        self.nav_series: List[Dict] = []
        self.daily_positions: List[Dict] = []
        self.trades: List[Dict] = []
        self._synced_trade_count = 0  # 已同步的执行引擎交易数

        # K线管理器
        self.km = get_kline_manager()

        # 因子缓存：{trade_date: {stock_code: {factor: value}}}
        self._factor_cache: Dict[str, Dict[str, Dict]] = {}

        # 股票名称映射（从 stock_base_info 加载）
        self._stock_name_map: Dict[str, str] = {}
        self._load_stock_names()

        # 回测用 K 线缓存（按股票代码缓存，减少重复查询）
        self._kline_cache: Dict[str, Any] = {}

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

    def _inject_initial_positions(self, positions: List[Dict]):
        """注入初始持仓（分段回测用：上一段继承的持仓）

        关键点：
        - buy_date 设为 start_date 之前的一个虚拟日期，使 T+1 检查在第一天就能通过
        - highest_price 重置为 avg_cost，新段从头追踪最高点
        - total_cost 设为 quantity * avg_cost（假设上一段已含手续费）
        """
        from datetime import datetime, timedelta
        # 虚拟买入日期：段开始日期的前一天
        try:
            start_dt = datetime.strptime(self.start_date, "%Y-%m-%d")
            virtual_date = (start_dt - timedelta(days=1)).strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            virtual_date = "2000-01-01"

        for pos_data in positions:
            code = pos_data.get("stock_code", "")
            if not code:
                continue
            qty = pos_data.get("quantity", 0)
            avg_cost = pos_data.get("avg_cost", 0)
            if qty <= 0 or avg_cost <= 0:
                continue

            self.execution.positions[code] = Position(
                stock_code=code,
                stock_name=pos_data.get("stock_name", ""),
                quantity=qty,
                avg_cost=avg_cost,
                buy_date=pos_data.get("buy_date", virtual_date),
                highest_price=avg_cost,  # 新段从头追踪
                total_cost=pos_data.get("total_cost", qty * avg_cost),
            )

            logger.warn("backtest", f"注入初始持仓: {code} × {qty} @ {avg_cost:.2f}")

        # 如果指定了 initial_cash，覆盖默认现金
        if hasattr(self, 'initial_cash') and self.initial_cash is not None:
            self.execution.cash = self.initial_cash

    def run(self, progress_callback=None) -> Dict:
        """
        执行逐日撮合回测。

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

        # 2.5. 注入初始持仓（分段回测用）
        if self.initial_positions:
            self._inject_initial_positions(self.initial_positions)

        # 3. 预加载所有交易日的因子数据（加速）
        self._prefetch_factors(trade_dates)

        total_days = len(trade_dates)
        for i, trade_date in enumerate(trade_dates):
            try:
                self._step(trade_date)
            except Exception as e:
                logger.warn("backtest", f"回测日 {trade_date} 处理失败: {e}")

            # 进度回调（传入当前交易日）
            if progress_callback:
                progress_callback(i + 1, total_days, trade_date)

        # 4. 回测结束，清仓所有持仓（仅最后一段需要清仓）
        if self.liquidate_at_end:
            last_date = trade_dates[-1]
            ohlc_last = self._get_daily_ohlc(last_date)
            for code, pos in list(self.execution.positions.items()):
                close_price = ohlc_last.get(code, {}).get("close", pos.avg_cost)
                prev_close = self._get_prev_close(code, last_date)
                self.execution.sell(code, close_price, last_date, reason="回测清仓", prev_close=prev_close)
            # 同步清仓交易记录
            self._sync_trades()

        # 5. 计算绩效指标
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
            "daily_positions": self.daily_positions,
        }

    def _step(self, trade_date: str):
        """单个交易日的处理流程

        T日盘后: 策略选股 → 生成买入信号 → 存为pending
        T+1日盘中: 执行pending信号（检查触发价/收盘价）
        """
        # 1. 获取当日 OHLC 数据
        ohlc_data = self._get_daily_ohlc(trade_date)
        if not ohlc_data:
            self._record_nav(trade_date)
            return

        prices = {code: d["close"] for code, d in ohlc_data.items()}

        # 2. 执行前一日pending买入信号（T日执行T-1日生成的信号）
        self._execute_pending_buys(trade_date, prices, ohlc_data)

        # 3. 检查持仓卖出信号
        self._check_sell_signals(trade_date, prices, ohlc_data)

        # 4. 更新持仓标记价格（移动止盈用）
        for code, price in prices.items():
            self.execution.update_position_mark(code, price)

        # 5. 盘后选股：仓位满则跳过（卖出后仓位不满则正常扫描）
        if not self._is_position_full():
            self._check_buy_signals(trade_date, prices)

        # 6. 记录当日净值
        self._record_nav(trade_date, prices)

    def _get_daily_ohlc(self, trade_date: str) -> Dict[str, Dict[str, float]]:
        """获取指定交易日所有股票的 OHLC 数据"""
        km = self.km
        conn = km._conn()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT stock_code, open, high, low, close FROM kline_data WHERE trade_date = ?",
            (trade_date,)
        )
        result = {}
        for row in cursor.fetchall():
            if row[4]:  # close
                result[row[0]] = {
                    "open": float(row[1]) if row[1] else float(row[4]),
                    "high": float(row[2]) if row[2] else float(row[4]),
                    "low": float(row[3]) if row[3] else float(row[4]),
                    "close": float(row[4]),
                }
        return result

    def _check_sell_signals(self, trade_date: str, prices: Dict[str, float], ohlc_data: Dict[str, Dict[str, float]]):
        """检查持仓的卖出信号"""
        codes_to_check = list(self.execution.positions.keys())

        for code in codes_to_check:
            if code not in self.execution.positions:
                continue

            pos = self.execution.positions[code]
            price = prices.get(code, pos.avg_cost)
            ohlc = ohlc_data.get(code, {})
            prev_close = self._get_prev_close(code, trade_date)

            # Priority 1: 固定止盈止损
            if self._check_fixed_stop(code, price, pos, trade_date, prev_close, ohlc):
                continue

            # Priority 2: 移动止盈
            if self._check_trailing_stop(code, price, pos, trade_date, prev_close, ohlc):
                continue

            # Priority 3: 策略代码型卖出信号
            if self._check_strategy_sell(code, price, pos, trade_date, prev_close):
                continue

    def _check_fixed_stop(
        self, code: str, price: float, pos: Position,
        date: str, prev_close: float, ohlc: Optional[Dict] = None
    ) -> bool:
        """固定止盈止损检查"""
        if self.stop_loss_pct is not None:
            stop_price = pos.avg_cost * (1 - self.stop_loss_pct)
            if ohlc and self.stop_execution_price == "trigger":
                low = ohlc.get("low", price)
                if low <= stop_price:
                    # trigger 模式：用止损价格成交
                    self.execution.sell(code, stop_price, date, reason="止损", prev_close=prev_close)
                    return True
            elif price <= stop_price:
                self.execution.sell(code, price, date, reason="止损", prev_close=prev_close)
                return True

        if self.take_profit_pct is not None:
            take_price = pos.avg_cost * (1 + self.take_profit_pct)
            if ohlc and self.stop_execution_price == "trigger":
                high = ohlc.get("high", price)
                if high >= take_price:
                    # trigger 模式：用止盈价格成交
                    self.execution.sell(code, take_price, date, reason="止盈", prev_close=prev_close)
                    return True
            elif price >= take_price:
                self.execution.sell(code, price, date, reason="止盈", prev_close=prev_close)
                return True

        return False

    def _check_trailing_stop(
        self, code: str, price: float, pos: Position,
        date: str, prev_close: float, ohlc: Optional[Dict] = None
    ) -> bool:
        """移动止盈：最高价须先超过成本价 × (1 + 阈值) 才生效"""
        if self.trailing_stop_pct is None:
            return False

        # 更新当日最高价（用 OHLC 的 high 如果可用）
        if ohlc:
            day_high = ohlc.get("high", price)
            if day_high > pos.highest_price:
                pos.highest_price = day_high
        elif price > pos.highest_price:
            pos.highest_price = price

        # 最高价须先超过成本价一定比例（有足够浮盈）才启用移动止盈
        trigger_price = pos.avg_cost * (1 + self.trailing_stop_pct)
        if pos.highest_price < trigger_price:
            return False

        # 从最高点回撤超过阈值则卖出
        trigger_price = pos.highest_price * (1 - self.trailing_stop_pct)
        if ohlc:
            low = ohlc.get("low", price)
            if low <= trigger_price:
                self.execution.sell(code, low, date, reason="移动止盈", prev_close=prev_close)
                return True
        elif (pos.highest_price - price) / pos.highest_price >= self.trailing_stop_pct:
            self.execution.sell(code, price, date, reason="移动止盈", prev_close=prev_close)
            return True

        return False

    def _check_strategy_sell(
        self, code: str, price: float, pos: Position,
        date: str, prev_close: float
    ) -> bool:
        """策略代码型卖出信号检查"""
        if self.is_code_strategy:
            return self._check_strategy_sell_code(code, price, pos, date, prev_close)

        # 配置型策略
        sell_conditions = self.strategy_config.get("sell_conditions", [])
        if not sell_conditions:
            return False

        indicators = self._get_stock_indicators(code, date, price)
        if not indicators:
            return False

        normalized = normalize_conditions(sell_conditions)
        is_triggered = self.parser.evaluate(normalized, indicators)
        if is_triggered:
            self.execution.sell(code, price, date, reason="策略卖出", prev_close=prev_close)
            return True

        return False

    def _check_strategy_sell_code(
        self, code: str, price: float, pos: Position,
        date: str, prev_close: float
    ) -> bool:
        """代码型策略：通过 StrategyEngine 检查卖出信号"""
        strategy = {
            "code": self.strategy_config.get("code", ""),
            "function_name": self.strategy_config.get("function_name", "run"),
            "name": self.strategy_config.get("name", ""),
            "config": self.strategy_config.get("config", {}),
        }
        if not strategy["code"]:
            return False

        # 只传入当前持仓股票的价格
        prices = {code: price}
        context = self._build_backtest_context(date, prices, "sell")

        try:
            signals = self.strategy_engine.execute_strategy(strategy, context)
        except Exception as e:
            logger.warn("backtest", f"策略执行失败 ({date}, {code}): {e}")
            return False

        for signal in signals:
            if signal.get("action") == "sell" and signal.get("stock_code") == code:
                self.execution.sell(
                    code, price, date,
                    reason=signal.get("reason", "策略卖出"),
                    prev_close=prev_close,
                )
                return True

        return False

    def _check_buy_signals(self, trade_date: str, prices: Dict[str, float]):
        """盘后选股：生成买入信号存入pending，次日执行"""
        if not self._has_buy_capacity():
            return

        if not self.is_code_strategy and not self.buy_conditions:
            return

        candidates = self._screen_candidates(trade_date, prices)

        # 清空旧pending，存入新信号
        self._pending_signals = []
        for candidate in candidates:
            code = candidate["stock_code"]
            if code in self.execution.positions:
                continue
            self._pending_signals.append({
                "stock_code": code,
                "stock_name": candidate.get("stock_name", code),
                "signal_price": candidate.get("price", 0),        # 信号日收盘价
                "trigger_price": candidate.get("trigger_price", 0),  # 策略建议买入价
                "stop_loss_pct": candidate.get("stop_loss_pct", 0.05),
                "take_profit_pct": candidate.get("take_profit_pct", 0.15),
            })

    def _execute_pending_buys(self, trade_date: str, prices: Dict[str, float],
                               ohlc_data: Dict[str, Dict[str, float]]):
        """T日执行T-1日生成的pending买入信号

        根据stop_execution_price模式决定成交价：
        - "close": 用当日收盘价（简化模式）
        - "trigger": 当日最低价≤触发价≤当日最高价时，用触发价成交
        """
        if not self._pending_signals:
            return

        executed = []
        for sig in self._pending_signals:
            code = sig["stock_code"]
            if code in self.execution.positions:
                continue
            if not self._has_buy_capacity():
                break

            ohlc = ohlc_data.get(code)
            if not ohlc:
                continue

            trigger = sig.get("trigger_price", 0)
            signal_price = sig.get("signal_price", 0)

            if self.stop_execution_price == "trigger" and trigger > 0:
                low = ohlc.get("low", trigger)
                high = ohlc.get("high", trigger)
                # 触发价在当日波动区间内 → 用触发价成交
                if low <= trigger <= high:
                    buy_price = trigger
                else:
                    continue  # 当日未触及触发价，信号失效
            else:
                # close模式：用当日收盘价
                buy_price = ohlc.get("close", 0)
                if buy_price <= 0:
                    continue

            prev_close = self._get_prev_close(code, trade_date)
            self.execution.buy(
                stock_code=code,
                price=buy_price,
                date=trade_date,
                stock_name=sig.get("stock_name", code),
                prev_close=prev_close,
            )
            executed.append(code)

        # 已执行的信号从pending中移除
        self._pending_signals = [s for s in self._pending_signals if s["stock_code"] not in executed]

    def _is_position_full(self) -> bool:
        """仓位已满（资金不足或持仓数达上限）"""
        return not self._has_buy_capacity()

    def _has_buy_capacity(self) -> bool:
        """判断是否有买入空间（资金 + 仓位数量）"""
        cash = self.execution.get_cash()
        limits = self.execution.position_limits
        total_value = self.execution.get_total_value()

        # 资金检查：至少能买 100 股（按最低 1 元估算）
        if cash < 100 * 1.01:  # 100 股 + 少量手续费
            return False

        # 总仓位检查
        positions_value = total_value - cash
        if limits.max_total_position_pct > 0:
            if positions_value >= total_value * limits.max_total_position_pct:
                return False

        # 持仓数量检查（估算：如果单只最小市值已满，无法再开新仓）
        max_positions = int(1.0 / limits.max_single_position_pct) if limits.max_single_position_pct > 0 else 999
        if self.execution.get_position_count() >= max_positions:
            return False

        return True

    def _screen_candidates(self, trade_date: str, prices: Dict[str, float]) -> List[Dict]:
        """从股票池中筛选满足买入条件的候选股票"""
        if self.is_code_strategy:
            return self._screen_candidates_code(trade_date, prices)

        # 配置型策略：使用 ConditionParser
        candidates = []
        pool_set = set(self.stock_pool) if self.stock_pool else None
        for code, price in prices.items():
            if pool_set and code not in pool_set:
                continue

            indicators = self._get_stock_indicators(code, trade_date, price)
            if not indicators:
                continue

            normalized = normalize_conditions(self.buy_conditions)
            is_matched = self.parser.evaluate(normalized, indicators)
            if is_matched:
                candidates.append({
                    "stock_code": code,
                    "price": price,
                    "stock_name": indicators.get("stock_name", code),
                })

        return candidates

    def _screen_candidates_code(self, trade_date: str, prices: Dict[str, float]) -> List[Dict]:
        """代码型策略：通过 StrategyEngine 获取买入信号"""
        strategy = {
            "code": self.strategy_config.get("code", ""),
            "function_name": self.strategy_config.get("function_name", "run"),
            "name": self.strategy_config.get("name", ""),
            "config": self.strategy_config.get("config", {}),
        }
        if not strategy["code"]:
            return []

        context = self._build_backtest_context(trade_date, prices, "buy")
        try:
            signals = self.strategy_engine.execute_strategy(strategy, context)
        except Exception as e:
            logger.warn("backtest", f"策略执行失败 ({trade_date}): {e}")
            return []

        # 将信号转换为候选列表
        candidates = []
        for signal in signals:
            if signal.get("action") != "buy":
                continue
            code = signal["stock_code"]
            price = prices.get(code, signal.get("buy_price", 0))
            if price <= 0:
                continue
            candidates.append({
                "stock_code": code,
                "price": price,
                "trigger_price": signal.get("trigger_price", price),
                "stock_name": signal.get("stock_name", code),
                "stop_loss_pct": signal.get("stop_loss_pct", 0.05),
                "take_profit_pct": signal.get("take_profit_pct", 0.15),
            })

        return candidates

    def _get_stock_indicators(
        self, stock_code: str, trade_date: str, current_price: float
    ) -> Dict[str, Any]:
        """获取单只股票在指定日期的技术指标"""
        # 先查缓存
        cached = self._factor_cache.get(trade_date, {}).get(stock_code)
        if cached:
            cached["price"] = current_price
            cached["PRICE"] = current_price
            return cached

        # 从数据库获取K线数据（截至当前交易日的前60条）
        # 注意：get_kline_data 用 ASC 排序 + limit 返回最早 N 条，
        # 所以需要用 start_date 来限制范围，再取最后 60 条
        from datetime import datetime, timedelta
        try:
            dt = datetime.strptime(trade_date, "%Y-%m-%d")
            start_dt = dt - timedelta(days=120)  # 约120个日历日覆盖~60个交易日
            start_str = start_dt.strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            start_str = None

        kline = self.km.get_kline_data(stock_code, start_date=start_str, end_date=trade_date)
        # 取最后60条（最近的）
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

        # 获取股票名称（优先从 stock_base_info，其次从 kline）
        stock_name = self._stock_name_map.get(stock_code, stock_code)
        indicators["stock_name"] = stock_name

        return indicators

    def _prefetch_factors(self, trade_dates: List[str]):
        """预加载因子数据到缓存"""
        if not trade_dates:
            return

        try:
            conn = self.km._conn()
            cursor = conn.cursor()

            placeholders = ",".join(["?" for _ in trade_dates])
            cursor.execute(
                f"SELECT stock_code, trade_date, ma5, ma10, ma20, ma60, dif, dea, rsi_14, volume_ratio "
                f"FROM stock_daily_factors WHERE trade_date IN ({placeholders})",
                trade_dates
            )

            for row in cursor.fetchall():
                date = row[1]
                code = row[0]
                self._factor_cache.setdefault(date, {})[code] = {
                    "MA5": row[2],
                    "MA10": row[3],
                    "MA20": row[4],
                    "MA60": row[5],
                    "DIF": row[6],
                    "DEA": row[7],
                    "RSI_14": row[8],
                    "VOLUME_RATIO": row[9],
                    "ma5": row[2],
                    "ma10": row[3],
                    "ma20": row[4],
                    "ma60": row[5],
                    "dif": row[6],
                    "dea": row[7],
                    "rsi_14": row[8],
                    "volume_ratio": row[9],
                }
        except Exception as e:
            logger.warn("backtest", f"因子预加载失败: {e}")

    def _get_prev_close(self, stock_code: str, trade_date: str) -> float:
        """获取前一日收盘价"""
        km = self.km
        conn = km._conn()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT close FROM kline_data WHERE stock_code = ? AND trade_date < ? ORDER BY trade_date DESC LIMIT 1",
            (stock_code, trade_date)
        )
        row = cursor.fetchone()
        return float(row[0]) if row and row[0] else 0.0

    def _record_nav(self, trade_date: str, prices: Optional[Dict] = None):
        """记录当日净值"""
        cash = self.execution.get_cash()

        if prices:
            positions_value = sum(
                prices.get(code, pos.avg_cost) * pos.quantity
                for code, pos in self.execution.positions.items()
            )
        else:
            positions_value = sum(
                pos.avg_cost * pos.quantity
                for pos in self.execution.positions.values()
            )

        total_value = cash + positions_value
        nav = total_value / self.initial_capital if self.initial_capital > 0 else 1.0

        # 计算回撤
        peak_nav = max((item["nav"] for item in self.nav_series), default=nav)
        drawdown = (peak_nav - nav) / peak_nav if peak_nav > 0 else 0
        max_drawdown = max(drawdown, max((item.get("max_drawdown", 0) for item in self.nav_series), default=0))

        # 计算日收益率
        daily_return = 0.0
        if len(self.nav_series) > 0:
            prev_nav = self.nav_series[-1]["nav"]
            daily_return = (nav - prev_nav) / prev_nav if prev_nav > 0 else 0

        nav_record = {
            "trade_date": trade_date,
            "nav": nav,
            "total_value": total_value,
            "cash": cash,
            "positions_value": positions_value,
            "position_count": self.execution.get_position_count(),
            "drawdown": drawdown,
            "max_drawdown": max_drawdown,
            "daily_return": daily_return,
        }
        self.nav_series.append(nav_record)

        # 记录持仓快照
        for code, pos in self.execution.positions.items():
            close_price = prices.get(code, pos.avg_cost) if prices else pos.avg_cost
            self.daily_positions.append({
                "trade_date": trade_date,
                "stock_code": code,
                "stock_name": pos.stock_name,
                "quantity": pos.quantity,
                "avg_cost": pos.avg_cost,
                "close_price": close_price,
                "market_value": close_price * pos.quantity,
                "unrealized_pnl": (close_price - pos.avg_cost) * pos.quantity,
            })

        # 同步交易记录
        self._sync_trades()

    def _sync_trades(self):
        """将撮合引擎的新增 Trade 记录同步为字典格式"""
        start_idx = self._synced_trade_count
        for trade in self.execution.trades[start_idx:]:
            trade_dict = {
                "stock_code": trade.stock_code,
                "stock_name": self._stock_name_map.get(trade.stock_code, trade.stock_name or trade.stock_code),
                "trade_type": trade.trade_type,
                "date": trade.date,
                "price": trade.price,
                "quantity": trade.quantity,
                "commission": trade.commission,
                "pnl": trade.pnl,
                "pnl_pct": trade.pnl_pct,
                "reason": trade.reason,
                "buy_date": getattr(trade, "buy_date", ""),
                "buy_price": getattr(trade, "buy_price", 0),
                "sell_date": getattr(trade, "sell_date", None),
                "sell_price": getattr(trade, "sell_price", None),
                "sell_commission": getattr(trade, "sell_commission", 0),
                "holding_days": trade.holding_days,
            }
            self.trades.append(trade_dict)

        self._synced_trade_count = len(self.execution.trades)

    def _extract_buy_conditions(self, config: Dict) -> Any:
        """从策略配置中提取买入条件"""
        buy_conditions = config.get("buy_conditions", [])
        if not buy_conditions:
            buy_conditions = config.get("buy", [])
        if not buy_conditions:
            conditions = config.get("conditions", {})
            buy_conditions = conditions.get("buy", [])
        return buy_conditions

    def _build_backtest_context(
        self, trade_date: str, prices: Dict[str, float], mode: str = "buy"
    ) -> Dict:
        """
        构建回测用执行上下文，注入 StrategyEngine 所需的函数。

        所有数据源指向 KlineManager（本地 kline.db），截断到 trade_date。

        Args:
            trade_date: 当前回测日期
            prices: 当日股票收盘价字典
            mode: "buy" 或 "sell"，决定传入的股票列表
        """
        from services.common.technical_indicators import (
            calculate_ma, calculate_rsi, calculate_macd, calculate_kdj,
            calculate_bollinger_bands, calculate_adx, calculate_atr, calculate_ema,
        )
        from services.common.stock_code import normalize_stock_code
        from services.common.kronos_service import get_kronos_service

        # 确定需要处理的股票列表
        if mode == "buy":
            stocks = list(self.stock_pool) if self.stock_pool else list(prices.keys())
        else:
            stocks = list(prices.keys())

        # K 线数据获取函数（回测用）
        def _get_kline(sc: str, limit: int = 60, start_date: Optional[str] = None):
            """获取 K 线数据，截断到 trade_date，返回 List[Dict] 格式"""
            cache_key = f"{sc}_{trade_date}_{limit}"
            if cache_key in self._kline_cache:
                return self._kline_cache[cache_key]
            end_dt = trade_date
            df = self.km.get_kline_data(
                normalize_stock_code(sc),
                start_date=start_date,
                end_date=end_dt,
                limit=limit,
            )
            # 转换为 List[Dict] 格式，与实盘 SDK 返回格式一致
            if df is not None and hasattr(df, 'to_dict'):
                result = df.to_dict('records')
            elif isinstance(df, list):
                result = df
            else:
                result = []
            self._kline_cache[cache_key] = result
            return result

        def _get_batch_kline(codes, limit: int = 60, start_date: Optional[str] = None):
            """批量获取 K 线数据"""
            results = {}
            for sc in codes:
                results[sc] = _get_kline(sc, limit, start_date)
            return results

        def _get_factors(sc: str, date: Optional[str] = None):
            """获取因子数据"""
            d = date or trade_date
            return self._factor_cache.get(d, {}).get(sc, {})

        def _get_factors_batch(codes, date: Optional[str] = None):
            """批量获取因子数据"""
            d = date or trade_date
            results = {}
            for sc in codes:
                results[sc] = self._factor_cache.get(d, {}).get(sc, {})
            return results

        def _get_market_data(sc: str = None):
            """获取当日行情（回测中返回收盘价）"""
            if sc:
                return prices.get(sc, {})
            return prices

        def _get_realtime_quote(sc: str):
            """实时行情（回测中不可用，返回 None）"""
            return None

        def _get_kline_smart(sc, limit: int = 60, lookback: int = 0, start_date: Optional[str] = None):
            """智能 K 线获取（回测中直接返回本地数据）— 支持单只/批量"""
            if isinstance(sc, list):
                # 批量模式：返回 {code: kline_df}
                result = {}
                for code in sc:
                    result[code] = _get_kline(code, limit if lookback == 0 else lookback, start_date)
                return result
            else:
                return _get_kline(sc, limit if lookback == 0 else lookback, start_date)

        def _get_kline_spliced(sc, limit: int = 60, **kwargs):
            """拼接 K 线（回测中返回本地数据）"""
            return _get_kline(sc, limit)

        # Kronos 预测：回测中同样可用，只要传入的历史 K 线截断到 trade_date 之前
        kronos_service = get_kronos_service()
        def _kronos_predict(df_hist, pred_len=5, future_dates=None, **kwargs):
            return kronos_service.predict(df_hist, pred_len=pred_len, future_dates=future_dates, **kwargs)

        context = {
            "stocks": [
                {"stock_code": sc, "stock_name": self._stock_name_map.get(sc, sc)}
                for sc in stocks
            ],
            "account_id": "backtest",
            "today": trade_date,
            "prices": prices,
            "get_kline": _get_kline,
            "get_batch_kline": _get_batch_kline,
            "get_factors": _get_factors,
            "get_factors_batch": _get_factors_batch,
            "get_kline_smart": _get_kline_smart,
            "get_kline_spliced": _get_kline_spliced,
            "get_market_data": _get_market_data,
            "get_realtime_quote": _get_realtime_quote,
            "indicators": {
                "calculate_ma": calculate_ma,
                "calculate_rsi": calculate_rsi,
                "calculate_macd": calculate_macd,
                "calculate_kdj": calculate_kdj,
                "calculate_bollinger_bands": calculate_bollinger_bands,
                "calculate_adx": calculate_adx,
                "calculate_atr": calculate_atr,
                "calculate_ema": calculate_ema,
            },
            "kronos_predict": _kronos_predict,
            "kronos_available": kronos_service.is_available,
        }

        # 同步数据库查询（只读，用于策略中特殊查询需求）
        def _query_db(sql: str, params: tuple = None):
            from services.common.database import get_sync_connection
            conn = get_sync_connection()
            cursor = conn.cursor()
            try:
                if params:
                    cursor.execute(sql, params)
                else:
                    cursor.execute(sql)
                if sql.strip().upper().startswith("SELECT"):
                    rows = [dict(r) for r in cursor.fetchall()]
                    return rows
                else:
                    conn.commit()
                    return cursor.rowcount
            except Exception:
                conn.rollback()
                raise

        context["query_db"] = _query_db

        return context
