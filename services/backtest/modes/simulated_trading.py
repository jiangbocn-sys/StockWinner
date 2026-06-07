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
from services.common.price_adjuster import adjust_klines
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
        trading_strategy_ids: Optional[List[int]] = None,
        trading_strategies: Optional[List[Dict]] = None,  # 预加载的策略代码
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
        self._trading_strategies: List[Dict] = []  # 交易策略列表（卖出信号）

        # 策略类型检测
        self.is_code_strategy = strategy_config.get("strategy_type") == "python"

        if self.is_code_strategy:
            from services.strategy.engine import get_strategy_engine
            self.strategy_engine = get_strategy_engine()
        else:
            self.parser = get_condition_parser()

        # 买入条件（配置型策略用）
        self.buy_conditions = self._extract_buy_conditions(strategy_config) if not self.is_code_strategy else None

        # 加载交易策略（卖出信号策略）
        # 如果传入预加载的策略代码，直接使用（避免子进程访问数据库）
        if trading_strategies:
            self._trading_strategies = trading_strategies
        elif trading_strategy_ids:
            # 主进程模式下从数据库加载
            self._load_trading_strategies(trading_strategy_ids)

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
                import traceback
                tb = traceback.format_exc()
                logger.warn("backtest", f"回测日 {trade_date} 处理失败: {e}\n{tb}")

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
        position_count_before = self.execution.get_position_count()
        logger.log_event("backtest_step", f"date={trade_date}, positions_before_sell={position_count_before}, trading_strategies={len(self._trading_strategies)}")
        self._check_sell_signals(trade_date, prices, ohlc_data)
        position_count_after = self.execution.get_position_count()
        logger.log_event("backtest_sell_done", f"date={trade_date}, positions_after_sell={position_count_after}, sold={position_count_before - position_count_after}")

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
        print(f"[CHECK_SELL_SIGNALS] date={trade_date}, positions={len(codes_to_check)}, trading_strategies={len(self._trading_strategies)}")

        for code in codes_to_check:
            if code not in self.execution.positions:
                continue

            pos = self.execution.positions[code]
            price = prices.get(code, pos.avg_cost)
            ohlc = ohlc_data.get(code, {})
            prev_close = self._get_prev_close(code, trade_date)

            # Priority 1: 交易策略卖出（用户策略优先）
            if self._check_trading_strategy_sell(code, price, pos, trade_date, prev_close, ohlc):
                continue

            # Priority 2: 选股策略卖出信号
            if self._check_strategy_sell(code, price, pos, trade_date, prev_close):
                continue

            # Priority 3: 固定止盈止损（风控兜底）
            if self._check_fixed_stop(code, price, pos, trade_date, prev_close, ohlc):
                continue

            # Priority 4: 移动止盈（风控兜底）
            if self._check_trailing_stop(code, price, pos, trade_date, prev_close, ohlc):
                continue

    @staticmethod
    def _adjust_klines_wrapper(klines, stock_code):
        """策略沙箱中的K线复权包装函数"""
        return adjust_klines(klines, stock_code)

    @staticmethod
    def _auto_adjust_query_result(rows, sql_lower, params):
        """自动对K线查询结果进行复权。

        检测条件：SQL 中包含 kline_data 或 weekly_kline_data，
        且 params[0] 是单个股票代码（非 IN 批量查询）。
        """
        if not rows or not params:
            return rows
        if "kline_data" not in sql_lower and "weekly_kline_data" not in sql_lower:
            return rows
        # 批量查询（IN 子句）跳过自动复权
        if " in " in sql_lower and "(" in sql_lower:
            return rows
        stock_code = params[0]
        if not isinstance(stock_code, str) or "." not in stock_code:
            return rows
        date_field = "week_start_date" if "weekly_kline_data" in sql_lower else "trade_date"
        return adjust_klines(rows, stock_code, date_field=date_field)

    def _check_fixed_stop(
        self, code: str, price: float, pos: Position,
        date: str, prev_close: float, ohlc: Optional[Dict] = None
    ) -> bool:
        """固定止盈止损检查"""
        # 检查策略是否标记为"继续持有"（跳过固定止损和固定止盈）
        if getattr(pos, '_strategy_hold', False):
            debug_file = "/tmp/backtest_debug.log"
            with open(debug_file, "a") as f:
                f.write(f"[FIXED_STOP_SKIPPED] code={code}, reason=策略标记继续持有\n")
            # 清除标记（仅对当日有效）
            delattr(pos, '_strategy_hold')
            return False

        # 只有参数 > 0 时才启用止损止盈
        if self.stop_loss_pct is not None and self.stop_loss_pct > 0:
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

        if self.take_profit_pct is not None and self.take_profit_pct > 0:
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
        # 只有参数 > 0 时才启用移动止盈
        if self.trailing_stop_pct is None or self.trailing_stop_pct <= 0:
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

        # 构建带持仓信息的 stocks 列表（与实盘 SignalEvaluator 保持一致）
        prices_with_pos = {code: price}
        context = self._build_backtest_context(date, prices_with_pos, "sell")
        # 用持仓数据覆盖 stocks[0]，注入 buy_price/buy_date/quantity/buy_pattern/eval_price
        context["stocks"] = [{
            "stock_code": code,
            "stock_name": self._stock_name_map.get(code, code),
            "buy_price": pos.avg_cost,
            "buy_date": pos.buy_date,
            "quantity": pos.quantity,
            "score": getattr(pos, 'score', 60),
            "reduced_pct": getattr(pos, 'reduced_pct', 0) or 0,
            "buy_pattern": getattr(pos, 'buy_pattern', None) or getattr(pos, 'details', None),
            "eval_price": price,
        }]

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
                "buy_pattern": candidate.get("details"),  # 选股策略关键点位（直接传递details）
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

            trigger = sig.get("trigger_price") or 0
            signal_price = sig.get("signal_price") or 0

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
                buy_pattern=sig.get("buy_pattern"),  # 传递选股策略关键点位
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
                "details": signal.get("details"),  # 选股策略输出的特征数据
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

        # 回测用数据库查询函数（截断到 trade_date）
        def _query_kline_db(sql: str, params: tuple = None):
            """回测用K线数据库查询，自动截断到 trade_date

            对于包含 trade_date/week_start_date 的查询，自动添加日期截断条件。
            正确处理 LIMIT 子句：在 SQL 中添加日期过滤后执行，而非后置过滤。
            """
            import re
            from services.common.database import get_sync_connection
            conn = get_sync_connection("kline")
            cursor = conn.cursor()

            sql_upper = sql.strip().upper()

            if not sql_upper.startswith("SELECT"):
                # 非 SELECT 查询
                if params:
                    cursor.execute(sql, params)
                else:
                    cursor.execute(sql)
                return cursor.rowcount

            # 检查是否有日期字段
            sql_lower = sql.lower()
            has_trade_date = "trade_date" in sql_lower
            has_week_start_date = "week_start_date" in sql_lower

            if not (has_trade_date or has_week_start_date):
                # 无日期字段的查询，直接执行
                if params:
                    cursor.execute(sql, params)
                else:
                    cursor.execute(sql)
                return [dict(r) for r in cursor.fetchall()]

            # 有日期字段，需要修改 SQL 添加截断条件
            # 检测日期字段名称
            date_field = "week_start_date" if has_week_start_date else "trade_date"

            # 提取 LIMIT 子句
            limit_match = re.search(r'\bLIMIT\s+(\d+)', sql_upper)
            limit_count = int(limit_match.group(1)) if limit_match else None

            # 构建新的 SQL：添加日期截断 WHERE 条件
            # 检查是否已有 WHERE 子句
            where_match = re.search(r'\bWHERE\b', sql_upper)
            order_match = re.search(r'\bORDER\s+BY\b', sql_upper)
            limit_match_pos = re.search(r'\bLIMIT\b', sql_upper)

            if where_match:
                # 已有 WHERE，添加 AND 条件
                # 找到 WHERE 后面第一个关键字（ORDER BY 或 LIMIT 或结尾）
                where_end = where_match.end()
                next_clause_pos = len(sql)
                for m in [order_match, limit_match_pos]:
                    if m and m.start() > where_end:
                        next_clause_pos = min(next_clause_pos, m.start())

                # 在 WHERE 条件后插入 AND
                before_where = sql[:where_end]
                after_where_clause = sql[where_end:next_clause_pos]
                rest = sql[next_clause_pos:]

                # 确保格式正确：添加 AND 和日期条件
                insert_pos = where_end + len(after_where_clause.rstrip())
                new_sql = sql[:insert_pos] + f" AND {date_field} <= '{trade_date}'" + sql[insert_pos:]
            else:
                # 没有 WHERE，需要添加 WHERE
                # 找到 ORDER BY 或 LIMIT 或结尾的位置
                insert_pos = len(sql)
                for m in [order_match, limit_match_pos]:
                    if m:
                        insert_pos = min(insert_pos, m.start())

                new_sql = sql[:insert_pos] + f" WHERE {date_field} <= '{trade_date}'" + sql[insert_pos:]

            # 执行修改后的 SQL
            if params:
                cursor.execute(new_sql, params)
            else:
                cursor.execute(new_sql)

            rows = [dict(r) for r in cursor.fetchall()]

            # 如果原 SQL 有 LIMIT 但我们没有保留它，需要截取
            if limit_count and len(rows) > limit_count:
                rows = rows[:limit_count]

            # 自动复权：K线查询自动应用后复权
            rows = SimulatedTradingEngine._auto_adjust_query_result(rows, sql_lower, params)

            return rows

        def _query_db(sql: str, params: tuple = None):
            """回测用stockwinner数据库查询"""
            from services.common.database import get_sync_connection
            conn = get_sync_connection("stockwinner")
            cursor = conn.cursor()
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
            if sql.strip().upper().startswith("SELECT"):
                return [dict(r) for r in cursor.fetchall()]
            return cursor.rowcount

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
            "query_kline_db": _query_kline_db,
            "query_db": _query_db,
        }

        return context

    def _load_trading_strategies(self, trading_strategy_ids: List[int]) -> None:
        """从数据库加载交易策略代码"""
        # 写入调试文件
        debug_file = "/tmp/backtest_debug.log"
        with open(debug_file, "a") as f:
            f.write(f"[LOAD_TRADING_STRATEGIES] 开始加载交易策略, ids={trading_strategy_ids}\n")

        if not trading_strategy_ids:
            self._trading_strategies = []
            with open(debug_file, "a") as f:
                f.write(f"[LOAD_TRADING_STRATEGIES] 无交易策略ID，跳过\n")
            return

        from services.common.database import get_sync_connection
        conn = get_sync_connection("stockwinner")
        cursor = conn.cursor()

        placeholders = ",".join(["?"] * len(trading_strategy_ids))
        cursor.execute(
            f"SELECT id, name, code, function_name FROM strategies WHERE id IN ({placeholders}) AND code_scope = 'trading'",
            trading_strategy_ids
        )

        self._trading_strategies = []
        for row in cursor.fetchall():
            strategy = {
                "id": row[0],
                "name": row[1],
                "code": row[2],
                "function_name": row[3] or "run",
            }
            self._trading_strategies.append(strategy)
            with open(debug_file, "a") as f:
                code_preview = row[2][:100] if row[2] else "EMPTY"
                f.write(f"[LOAD_TRADING_STRATEGIES] 加载策略: id={row[0]}, name={row[1]}, code_len={len(row[2]) if row[2] else 0}, preview={code_preview}\n")
        conn.close()
        with open(debug_file, "a") as f:
            f.write(f"[LOAD_TRADING_STRATEGIES] 已加载 {len(self._trading_strategies)} 个交易策略\n")
        logger.log_event("trading_strategies_loaded", f"已加载 {len(self._trading_strategies)} 个交易策略")

    def _check_trading_strategy_sell(
        self, code: str, price: float, pos: Position,
        date: str, prev_close: float, ohlc: Optional[Dict] = None
    ) -> bool:
        """调用交易策略检查卖出信号"""
        debug_file = "/tmp/backtest_debug.log"
        with open(debug_file, "a") as f:
            f.write(f"[CHECK_TRADING_STRATEGY] code={code}, date={date}, strategies={len(self._trading_strategies)}\n")
            f.write(f"[BUY_PATTERN_CHECK] code={code}, pos.buy_pattern={pos.buy_pattern}\n")

        if not self._trading_strategies:
            with open(debug_file, "a") as f:
                f.write(f"[CHECK_TRADING_STRATEGY] 无交易策略，跳过\n")
            return False

        logger.log_event("trading_strategy_check", f"code={code}, date={date}, strategies={len(self._trading_strategies)}")

        # 计算触发止损的有效价格（考虑trigger模式用最低价）
        stop_price = pos.avg_cost * (1 - (self.stop_loss_pct or 0.05))
        if ohlc and self.stop_execution_price == "trigger":
            low = ohlc.get("low", price)
            eval_price = low if low <= stop_price else price
        else:
            eval_price = price

        # 构造 context
        stocks = [{
            "stock_code": pos.stock_code,
            "stock_name": pos.stock_name,
            "buy_date": pos.buy_date,
            "buy_price": pos.avg_cost,
            "quantity": pos.quantity,
            "score": 60,  # 默认值
            "reduced_pct": pos.reduced_pct,  # 累计减仓比例
            "buy_pattern": pos.buy_pattern,  # 选股策略关键点位（用于形态验证）
            "eval_price": eval_price,  # 止损评估用价格（trigger模式=最低价，否则=收盘价）
            "ohlc_low": ohlc.get("low", price) if ohlc else price,  # 当日最低价
            "ohlc_high": ohlc.get("high", price) if ohlc else price,  # 当日最高价
        }]

        # 创建截断到回测日期的 query_kline_db 函数
        def _query_kline_db_truncated(sql: str, params: tuple = None):
            """回测用K线数据库查询，自动截断到回测日期"""
            import re
            from services.common.database import get_sync_connection
            conn = get_sync_connection("kline")
            cursor = conn.cursor()

            sql_upper = sql.strip().upper()

            if not sql_upper.startswith("SELECT"):
                if params:
                    cursor.execute(sql, params)
                else:
                    cursor.execute(sql)
                return cursor.rowcount

            sql_lower = sql.lower()
            has_trade_date = "trade_date" in sql_lower
            has_week_start_date = "week_start_date" in sql_lower

            if not (has_trade_date or has_week_start_date):
                if params:
                    cursor.execute(sql, params)
                else:
                    cursor.execute(sql)
                rows = [dict(r) for r in cursor.fetchall()]
                return rows

            date_field = "week_start_date" if has_week_start_date else "trade_date"

            # 检测并添加日期截断条件
            where_match = re.search(r'\bWHERE\b', sql_upper)
            order_match = re.search(r'\bORDER\s+BY\b', sql_upper)
            limit_match_pos = re.search(r'\bLIMIT\b', sql_upper)

            if where_match:
                where_end = where_match.end()
                next_clause_pos = len(sql)
                for m in [order_match, limit_match_pos]:
                    if m and m.start() > where_end:
                        next_clause_pos = min(next_clause_pos, m.start())
                insert_pos = where_end
                after_where = sql[where_end:next_clause_pos].rstrip()
                if after_where:
                    insert_pos = where_end + len(after_where)
                new_sql = sql[:insert_pos] + f" AND {date_field} <= '{date}'" + sql[insert_pos:]
            else:
                insert_pos = len(sql)
                for m in [order_match, limit_match_pos]:
                    if m:
                        insert_pos = min(insert_pos, m.start())
                new_sql = sql[:insert_pos] + f" WHERE {date_field} <= '{date}'" + sql[insert_pos:]

            if params:
                cursor.execute(new_sql, params)
            else:
                cursor.execute(new_sql)

            rows = [dict(r) for r in cursor.fetchall()]

            # 自动复权
            sql_lower2 = sql.lower()
            rows = SimulatedTradingEngine._auto_adjust_query_result(rows, sql_lower2, params)

            # 写入调试文件
            debug_file = "/tmp/backtest_debug.log"
            with open(debug_file, "a") as f:
                f.write(f"[QUERY_KLINE] sql={sql[:100]}..., params={params}, rows_count={len(rows)}\n")

            return rows

        context = {
            "stocks": stocks,
            "query_kline_db": _query_kline_db_truncated,  # 使用截断版本
            "current_date": date,
            "adjust_klines": self._adjust_klines_wrapper,  # K线复权处理
        }

        # 执行所有交易策略
        for strategy in self._trading_strategies:
            try:
                signals = self._execute_trading_strategy(strategy, context)
                debug_file = "/tmp/backtest_debug.log"
                with open(debug_file, "a") as f:
                    stop_price = pos.avg_cost * (1 - (self.stop_loss_pct or 0.05))
                    engine_pnl = (price - pos.avg_cost) / pos.avg_cost * 100
                    trigger = price <= stop_price
                    f.write(f"[TRADING_STRATEGY_RESULT] 策略={strategy['name']}, code={code}, signals={len(signals)}\n")
                    f.write(f"[PNL_DIAG] code={code}, engine_pnl={engine_pnl:.2f}%, engine_price={price:.4f}, cost={pos.avg_cost:.4f}, stop_price={stop_price:.4f}, trigger={trigger}, signals={len(signals)}\n")
                    for s in signals:
                        f.write(f"[TRADING_STRATEGY_SIGNAL] {s}\n")
                logger.log_event("trading_strategy_signals", f"策略={strategy['name']}, code={code}, signals={len(signals)}, actions={[s.get('action') for s in signals]}")
                for signal in signals:
                    # 匹配股票：信号有 stock_code 则匹配，无则默认当前股票
                    signal_code = signal.get("stock_code")
                    if signal_code and signal_code != code and signal_code != pos.stock_code:
                        continue  # 不匹配当前股票，跳过

                    action = signal.get("action", "")
                    if action == "hold":
                        # hold 信号：阻止后续的固定止损，但允许其他策略继续检查
                        # 返回 True 会跳过后续检查，返回 False 会继续执行固定止损
                        # 这里记录日志但不返回 True，让其他策略（如移动止盈）有机会检查
                        debug_file = "/tmp/backtest_debug.log"
                        with open(debug_file, "a") as f:
                            f.write(f"[TRADING_STRATEGY_HOLD] code={code}, reason={signal.get('reason', 'hold')}\n")
                        logger.log_event("trading_strategy_hold", f"code={code}, reason={signal.get('reason', 'hold')}")
                        # 设置标记，让固定止损跳过此股票
                        setattr(pos, '_strategy_hold', True)
                        return False  # 继续检查其他策略，但固定止损会检查 _strategy_hold 标记
                    elif action in ("sell", "reduce_half", "reduce_30"):
                        # 执行卖出或减仓
                        reason = f"交易策略({strategy['name']}): {signal.get('reason', action)}"
                        with open(debug_file, "a") as f:
                            f.write(f"[TRADING_STRATEGY_SELL_TRIGGERED] code={code}, action={action}, reason={reason}\n")
                        logger.log_event("trading_strategy_sell_triggered", f"code={code}, action={action}, reason={reason}")
                        if action == "sell":
                            pos_before = code in self.execution.positions
                            pos_count_before = len(self.execution.positions)
                            result = self.execution.sell(code, price, date, reason=reason, prev_close=prev_close)
                            pos_after = code in self.execution.positions
                            debug_file = "/tmp/backtest_debug.log"
                            with open(debug_file, "a") as f:
                                f.write(f"[SELL_EXEC] code={code}, date={date}, price={price:.2f}, result={'OK' if result else 'FAIL'}, pos_before={pos_before}, pos_after={pos_after}, count={pos_count_before}->{len(self.execution.positions)}\n")
                            if result is None:
                                f2 = open(debug_file, "a")
                                f2.write(f"[SELL_FAILED] code={code}, date={date}, price={price}, reason=execution.sell returned None\n")
                                f2.close()
                                return False
                            return True
                        elif action in ("reduce_half", "reduce_30"):
                            # 减仓处理
                            reduce_pct = 0.5 if action == "reduce_half" else 0.3
                            new_qty = int(pos.quantity * (1 - reduce_pct))
                            if new_qty > 0:
                                # 使用 sell 方法部分卖出
                                sell_qty = pos.quantity - new_qty
                                self.execution.sell_partial(code, price, sell_qty, date, reason=reason, prev_close=prev_close)
                                return True
            except Exception as e:
                import traceback
                debug_file = "/tmp/backtest_debug.log"
                with open(debug_file, "a") as f:
                    f.write(f"[TRADING_STRATEGY_ERROR] 策略 {strategy['name']} 执行失败: {e}\n{traceback.format_exc()}\n")
                logger.warn("backtest", f"交易策略执行失败 ({strategy['name']}, {code}): {e}")

        return False

    def _execute_trading_strategy(self, strategy: Dict, context: Dict) -> List[Dict]:
        """在沙箱中执行交易策略代码"""
        code = strategy.get("code", "")
        function_name = strategy.get("function_name", "run")

        if not code:
            logger.log_event("trading_strategy_no_code", f"策略 {strategy.get('name')} 没有代码")
            return []

        # 写入调试文件 - 检查 context 内容
        debug_file = "/tmp/backtest_debug.log"
        with open(debug_file, "a") as f:
            f.write(f"[EXECUTE_STRATEGY] name={strategy['name']}, context_keys={list(context.keys())}\n")
            f.write(f"[EXECUTE_STRATEGY] stocks={len(context.get('stocks', []))}, query_kline_db={context.get('query_kline_db') is not None}\n")
            # 打印 stocks 中的 buy_pattern 信息
            for s in context.get("stocks", []):
                f.write(f"[CONTEXT_STOCK] code={s.get('stock_code')}, buy_price={s.get('buy_price')}, buy_pattern={s.get('buy_pattern')}\n")

        # 沙箱执行（与选股策略类似）
        # 创建受限的 __import__，只允许导入安全模块
        ALLOWED_MODULES = {"typing", "datetime", "collections", "math", "re", "time", "_strptime"}
        def safe_import(name, *args, **kwargs):
            if name in ALLOWED_MODULES:
                return __import__(name, *args, **kwargs)
            raise ImportError(f"模块 '{name}' 不允许在策略沙箱中导入")

        safe_globals = {
            "__builtins__": {
                "abs": abs, "min": min, "max": max, "sum": sum, "len": len,
                "round": round, "int": int, "float": float, "str": str,
                "list": list, "dict": dict, "True": True, "False": False,
                "None": None, "sorted": sorted, "enumerate": enumerate,
                "range": range, "zip": zip, "map": map, "filter": filter,
                "any": any, "all": all, "isinstance": isinstance,
                "datetime": datetime,
                "reversed": reversed,  # 添加 reversed 函数
                "__import__": safe_import,  # 受限的导入函数
            },
            "datetime": datetime,
            "List": List,
            "Dict": Dict,
        }

        try:
            debug_file2 = "/tmp/backtest_debug.log"
            with open(debug_file2, "a") as f:
                f.write(f"[EXEC_CODE] len={len(code)}, first200={code[:200]}\n")
            exec(code, safe_globals)
            func = safe_globals.get(function_name)
            if func and callable(func):
                result = func(context) or []
                with open(debug_file, "a") as f:
                    f.write(f"[EXECUTE_STRATEGY_RESULT] signals={len(result)}\n")
                logger.log_event("trading_strategy_result", f"策略={strategy['name']}, stocks={len(context.get('stocks', []))}, signals={len(result)}")
                return result
            else:
                logger.log_event("trading_strategy_no_func", f"策略 {strategy['name']} 没有找到函数 {function_name}")
        except Exception as e:
            import traceback
            with open(debug_file, "a") as f:
                f.write(f"[EXECUTE_STRATEGY_ERROR] {e}\n{traceback.format_exc()}\n")
            logger.log_event("trading_strategy_error", f"策略 {strategy['name']} 执行失败: {e}\\n{traceback.format_exc()}")

        return []
