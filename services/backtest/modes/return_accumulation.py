"""
收益率累积模式

逐日推进计算：
1. 每日根据买入条件选股 → 选中即买入（每只净值=1）
2. 每日追踪每只持仓股票的净值变化（当前价/买入价）
3. 根据卖出条件检查持仓 → 触发卖出时计算整体收益率
4. 当日策略净值 = 所有持仓净值的平均值（无持仓时=1）

假设：完美执行、无滑点、无手续费、按收盘价成交
适用场景：快速筛选策略，不需要精确的撮合模拟
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
    """收益率累积引擎 - 逐日选股追踪净值"""

    def __init__(
        self,
        strategy_config: Dict,
        initial_capital: float,
        start_date: str,
        end_date: str,
        stock_pool: Optional[List[str]] = None,
        stop_loss_pct: Optional[float] = None,
        take_profit_pct: Optional[float] = None,
        holding_period: Optional[int] = None,  # 固定持有天数（可选）
        trading_strategies: Optional[List[Dict]] = None,  # 预加载的交易策略代码
    ):
        self.strategy_config = strategy_config
        self.initial_capital = initial_capital
        self.start_date = start_date
        self.end_date = end_date
        self.stock_pool = stock_pool
        self.stop_loss_pct = stop_loss_pct
        self.take_profit_pct = take_profit_pct
        self.holding_period = holding_period
        self.trading_strategies = trading_strategies or []  # 交易策略列表

        self.buy_conditions = self._extract_buy_conditions(strategy_config)
        self.sell_conditions = strategy_config.get("sell_conditions", [])

        # 策略类型检测
        self.is_code_strategy = strategy_config.get("strategy_type") == "python"

        if self.is_code_strategy:
            self.strategy_code = strategy_config.get("code", "")
            self.strategy_function_name = strategy_config.get("function_name", "run")
            self.parser = None  # 代码型策略不需要 condition_parser
        else:
            self.parser = get_condition_parser()
            self.strategy_code = None

        self.km = get_kline_manager()

        # 结果数据
        self.nav_series: List[Dict] = []
        self.trades: List[Dict] = []
        self.daily_positions: List[Dict] = []

        # 股票名称映射
        self._stock_name_map: Dict[str, str] = {}
        self._load_stock_names()

        # 缓存
        self._price_cache: Dict[str, Dict[str, float]] = {}  # {trade_date: {stock_code: close}}
        self._factor_cache: Dict[str, Dict[str, Dict]] = {}  # {trade_date: {stock_code: {factor: value}}}

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

        逻辑：
        1. 每日选股 → 选中即买入（净值=1）
        2. 每日追踪每只持仓净值变化
        3. 卖出条件触发 → 计算整体收益率
        4. 当日策略净值 = 持仓净值平均值

        Returns:
            {
                "result": BacktestResult.to_dict(),
                "trades": [...],
                "nav_series": [...],
                "daily_positions": [...],
            }
        """
        # 1. 获取交易日序列
        trade_dates = self.km.get_all_trade_dates(self.start_date, self.end_date)
        if not trade_dates:
            return {"error": f"时间范围内无交易日: {self.start_date} ~ {self.end_date}"}

        # 2. 确定股票池
        if not self.stock_pool:
            self.stock_pool = self.km.get_all_stocks()
        pool_set = set(self.stock_pool)

        total_days = len(trade_dates)

        # 3. 预加载因子数据（加速）
        self._prefetch_factors(trade_dates)

        # 4. 初始化持仓
        # holdings: {stock_code: {buy_date, buy_price, stock_name, highest_price}}
        holdings: Dict[str, Dict] = {}

        peak_nav = 1.0
        max_dd = 0.0

        # 5. 逐日推进
        for i, trade_date in enumerate(trade_dates):
            # 进度回调
            if progress_callback and total_days > 0:
                progress_callback(i + 1, total_days, trade_date)

            # 获取当日收盘价
            prices = self._get_cached_prices(trade_date)

            # 5.1 检查持仓卖出信号
            sold_codes = []
            for code, pos in list(holdings.items()):
                price = prices.get(code)
                if price is None:
                    continue

                sell_reason = self._check_sell_signal(code, trade_date, price, pos)
                if sell_reason:
                    # 卖出：记录交易
                    holding_days = self._days_between(pos["buy_date"], trade_date)
                    stock_nav = price / pos["buy_price"] if pos["buy_price"] > 0 else 1.0
                    pnl_pct = (stock_nav - 1) * 100  # 收益率 = (净值-1) × 100

                    self.trades.append({
                        "stock_code": code,
                        "stock_name": pos.get("stock_name", code),
                        "trade_type": "sell",
                        "date": trade_date,
                        "price": price,
                        "quantity": 100,
                        "commission": 0,
                        "pnl": (price - pos["buy_price"]) * 100,
                        "pnl_pct": pnl_pct,
                        "reason": sell_reason,
                        "buy_date": pos["buy_date"],
                        "buy_price": pos["buy_price"],
                        "holding_days": holding_days,
                        "final_nav": stock_nav,
                    })
                    sold_codes.append(code)

            # 清除已卖出持仓
            for code in sold_codes:
                holdings.pop(code, None)

            # 5.2 更新持仓最高价（用于移动止盈）
            for code, pos in holdings.items():
                price = prices.get(code)
                if price and price > pos.get("highest_price", pos["buy_price"]):
                    pos["highest_price"] = price

            # 5.3 每日选股买入
            if self.is_code_strategy or self.buy_conditions:
                candidates = self._screen_candidates(trade_date, prices, pool_set, holdings)

                # 选中的股票全部买入（净值=1）
                for candidate in candidates:
                    code = candidate["stock_code"]
                    price = candidate["price"]

                    holdings[code] = {
                        "buy_date": trade_date,
                        "buy_price": price,
                        "stock_name": candidate.get("stock_name", code),
                        "highest_price": price,
                        "score": candidate.get("score", 0),  # 保存评分信息
                    }

                    self.trades.append({
                        "stock_code": code,
                        "stock_name": candidate.get("stock_name", code),
                        "trade_type": "buy",
                        "date": trade_date,
                        "price": price,
                        "quantity": 100,
                        "commission": 0,
                        "reason": candidate.get("reason", "策略买入"),
                        "initial_nav": 1.0,
                        "score": candidate.get("score", 0),
                    })

            # 5.4 计算当日策略净值（按买入成本加权）
            # 策略净值 = 当前总市值 / 买入总成本
            if holdings:
                total_current_value = 0  # 当前总市值
                total_buy_cost = 0       # 买入总成本
                for code, pos in holdings.items():
                    price = prices.get(code, pos["buy_price"])
                    quantity = pos.get("quantity", 100)
                    total_current_value += price * quantity
                    total_buy_cost += pos["buy_price"] * quantity

                strategy_nav = total_current_value / total_buy_cost if total_buy_cost > 0 else 1.0
            else:
                strategy_nav = 1.0

            # 计算回撤
            if strategy_nav > peak_nav:
                peak_nav = strategy_nav
            drawdown = (peak_nav - strategy_nav) / peak_nav if peak_nav > 0 else 0
            if drawdown > max_dd:
                max_dd = drawdown

            # 计算日收益率
            daily_return = 0.0
            if len(self.nav_series) > 0:
                prev_nav = self.nav_series[-1]["nav"]
                daily_return = (strategy_nav - prev_nav) / prev_nav if prev_nav > 0 else 0

            # 记录净值
            self.nav_series.append({
                "trade_date": trade_date,
                "nav": strategy_nav,
                "total_value": self.initial_capital * strategy_nav,
                "cash": self.initial_capital,
                "positions_value": 0,
                "position_count": len(holdings),
                "drawdown": drawdown,
                "max_drawdown": max_dd,
                "daily_return": daily_return,
            })

            # 记录持仓快照
            for code, pos in holdings.items():
                price = prices.get(code, pos["buy_price"])
                stock_nav = price / pos["buy_price"] if pos["buy_price"] > 0 else 1.0
                self.daily_positions.append({
                    "trade_date": trade_date,
                    "stock_code": code,
                    "stock_name": pos.get("stock_name", code),
                    "quantity": 100,
                    "avg_cost": pos["buy_price"],
                    "close_price": price,
                    "market_value": price * 100,
                    "unrealized_pnl": (price - pos["buy_price"]) * 100,
                    "stock_nav": stock_nav,
                })

        # 6. 回测结束，清仓所有持仓
        last_date = trade_dates[-1]
        last_prices = self._get_cached_prices(last_date)

        for code, pos in holdings.items():
            price = last_prices.get(code, pos["buy_price"])
            holding_days = self._days_between(pos["buy_date"], last_date)
            stock_nav = price / pos["buy_price"] if pos["buy_price"] > 0 else 1.0
            pnl_pct = (stock_nav - 1) * 100

            self.trades.append({
                "stock_code": code,
                "stock_name": pos.get("stock_name", code),
                "trade_type": "sell",
                "date": last_date,
                "price": price,
                "quantity": 100,
                "commission": 0,
                "pnl": (price - pos["buy_price"]) * 100,
                "pnl_pct": pnl_pct,
                "reason": "回测清仓",
                "buy_date": pos["buy_date"],
                "buy_price": pos["buy_price"],
                "holding_days": holding_days,
                "final_nav": stock_nav,
            })

        # 7. 计算绩效指标
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
                    "MA5": row[2], "MA10": row[3], "MA20": row[4], "MA60": row[5],
                    "DIF": row[6], "DEA": row[7], "RSI_14": row[8], "VOLUME_RATIO": row[9],
                    "ma5": row[2], "ma10": row[3], "ma20": row[4], "ma60": row[5],
                    "dif": row[6], "dea": row[7], "rsi_14": row[8], "volume_ratio": row[9],
                }
        except Exception as e:
            logger.warn("backtest", f"因子预加载失败: {e}")

    def _get_cached_prices(self, trade_date: str) -> Dict[str, float]:
        """获取当日收盘价（带缓存）"""
        if trade_date in self._price_cache:
            return self._price_cache[trade_date]

        conn = self.km._conn()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT stock_code, close FROM kline_data WHERE trade_date = ?",
            (trade_date,)
        )
        prices = {row[0]: float(row[1]) for row in cursor.fetchall() if row[1]}
        self._price_cache[trade_date] = prices
        return prices

    def _check_sell_signal(self, code: str, trade_date: str, price: float, pos: Dict) -> Optional[str]:
        """检查持仓是否触发卖出信号"""
        buy_price = pos["buy_price"]
        buy_date = pos["buy_date"]
        highest_price = pos.get("highest_price", buy_price)
        holding_days = self._days_between(buy_date, trade_date)

        # 1. 交易策略卖出检查（最高优先级）
        if self.trading_strategies:
            sell_reason = self._check_trading_strategy_sell(code, trade_date, price, pos)
            if sell_reason:
                return sell_reason

        # 2. 固定持有天数检查
        if self.holding_period and self.holding_period > 0 and holding_days >= self.holding_period:
            return "持有期满"

        # 3. 固定止损检查
        if self.stop_loss_pct and self.stop_loss_pct > 0:
            if price <= buy_price * (1 - self.stop_loss_pct):
                return "止损"

        # 4. 固定止盈检查
        if self.take_profit_pct and self.take_profit_pct > 0:
            if price >= buy_price * (1 + self.take_profit_pct):
                return "止盈"

        # 5. 代码型选股策略卖出检查
        if self.is_code_strategy:
            sell_reason = self._check_sell_signal_code(code, trade_date, price, pos)
            if sell_reason:
                return sell_reason

        # 6. 配置型策略卖出条件检查
        if self.sell_conditions and self.parser:
            indicators = self._get_cached_indicators(code, trade_date, price)
            if indicators:
                normalized = normalize_conditions(self.sell_conditions)
                if self.parser.evaluate(normalized, indicators):
                    return "策略卖出"

        return None

    def _check_trading_strategy_sell(self, code: str, trade_date: str, price: float, pos: Dict) -> Optional[str]:
        """检查交易策略卖出信号"""
        if not self.trading_strategies:
            return None

        # 构建持仓股票信息
        stocks = [{
            "stock_code": code,
            "stock_name": pos.get("stock_name", code),
            "buy_date": pos["buy_date"],
            "buy_price": pos["buy_price"],
            "quantity": pos.get("quantity", 100),
            "score": pos.get("score", 0),
        }]

        # 构建查询函数
        from services.common.database import get_sync_connection
        import re

        def query_kline_db(sql: str, params: tuple = None):
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
                rows = cursor.fetchall()
                conn.close()
                return [dict(r) for r in rows] if rows else []

            date_field = "week_start_date" if has_week_start_date else "trade_date"
            where_match = re.search(r'\bWHERE\b', sql_upper)
            order_match = re.search(r'\bORDER\s+BY\b', sql_upper)
            limit_match_pos = re.search(r'\bLIMIT\b', sql_upper)

            if where_match:
                where_end = where_match.end()
                next_clause_pos = len(sql)
                for m in [order_match, limit_match_pos]:
                    if m and m.start() > where_end:
                        next_clause_pos = min(next_clause_pos, m.start())

                after_where_clause = sql[where_end:next_clause_pos]
                insert_pos = where_end + len(after_where_clause.rstrip())
                new_sql = sql[:insert_pos] + f" AND {date_field} <= '{trade_date}'" + sql[insert_pos:]
            else:
                insert_pos = len(sql)
                for m in [order_match, limit_match_pos]:
                    if m:
                        insert_pos = min(insert_pos, m.start())
                new_sql = sql[:insert_pos] + f" WHERE {date_field} <= '{trade_date}'" + sql[insert_pos:]

            if params:
                cursor.execute(new_sql, params)
            else:
                cursor.execute(new_sql)

            rows = cursor.fetchall()
            conn.close()
            return [dict(r) for r in rows] if rows else []

        context = {
            "stocks": stocks,
            "query_kline_db": query_kline_db,
            "current_date": trade_date,
        }

        # 执行所有交易策略
        for strategy in self.trading_strategies:
            strategy_code = strategy.get("code", "")
            function_name = strategy.get("function_name", "run")
            strategy_name = strategy.get("name", "")

            if not strategy_code:
                continue

            try:
                signals = self._execute_trading_strategy(strategy_code, function_name, context)
                for signal in signals:
                    signal_code = signal.get("stock_code")
                    if signal_code and signal_code != code:
                        continue

                    action = signal.get("action", "")
                    if action in ("sell", "reduce_half", "reduce_30"):
                        return f"交易策略({strategy_name}): {signal.get('reason', action)}"
            except Exception as e:
                logger.warn("backtest", f"交易策略 {strategy_name} 执行失败: {e}")

        return None

    def _execute_trading_strategy(self, code: str, function_name: str, context: Dict) -> List[Dict]:
        """执行交易策略代码"""
        ALLOWED_MODULES = {"typing", "datetime", "collections", "math", "re", "time"}
        def safe_import(name, *args, **kwargs):
            if name in ALLOWED_MODULES:
                return __import__(name, *args, **kwargs)
            raise ImportError(f"模块 '{name}' 不允许在策略沙箱中导入")

        safe_globals = {
            "__builtins__": {
                "abs": abs, "min": min, "max": max, "sum": sum, "len": len,
                "round": round, "int": int, "float": float, "str": str, "bool": bool,
                "list": list, "dict": dict, "tuple": tuple, "set": set,
                "True": True, "False": False, "None": None,
                "sorted": sorted, "enumerate": enumerate,
                "range": range, "zip": zip, "map": map, "filter": filter,
                "any": any, "all": all, "isinstance": isinstance, "type": type,
                "reversed": reversed, "print": print,
                "__import__": safe_import,
            },
            "datetime": datetime,
            "List": List,
            "Dict": Dict,
            "Optional": Optional,
            "Any": Any,
        }

        try:
            exec(code, safe_globals)
            func = safe_globals.get(function_name)
            if func and callable(func):
                return func(context) or []
        except Exception as e:
            logger.warn("backtest", f"交易策略代码执行失败: {e}")

        return []

    def _check_sell_signal_code(self, code: str, trade_date: str, price: float, pos: Dict) -> Optional[str]:
        """代码型策略：检查持仓卖出信号"""
        if not self.strategy_code:
            return None

        # 构建卖出检查上下文（只传入当前持仓股票）
        from services.common.database import get_sync_connection

        def query_kline_db(sql: str, params: tuple = None):
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
                rows = cursor.fetchall()
                conn.close()
                return [dict(r) for r in rows] if rows else []

            date_field = "week_start_date" if has_week_start_date else "trade_date"
            import re
            where_match = re.search(r'\bWHERE\b', sql_upper)
            order_match = re.search(r'\bORDER\s+BY\b', sql_upper)
            limit_match_pos = re.search(r'\bLIMIT\b', sql_upper)

            if where_match:
                where_end = where_match.end()
                next_clause_pos = len(sql)
                for m in [order_match, limit_match_pos]:
                    if m and m.start() > where_end:
                        next_clause_pos = min(next_clause_pos, m.start())

                after_where_clause = sql[where_end:next_clause_pos]
                insert_pos = where_end + len(after_where_clause.rstrip())
                new_sql = sql[:insert_pos] + f" AND {date_field} <= '{trade_date}'" + sql[insert_pos:]
            else:
                insert_pos = len(sql)
                for m in [order_match, limit_match_pos]:
                    if m:
                        insert_pos = min(insert_pos, m.start())
                new_sql = sql[:insert_pos] + f" WHERE {date_field} <= '{trade_date}'" + sql[insert_pos:]

            if params:
                cursor.execute(new_sql, params)
            else:
                cursor.execute(new_sql)

            rows = cursor.fetchall()
            conn.close()
            return [dict(r) for r in rows] if rows else []

        # 构建持仓股票信息
        stocks = [{
            "stock_code": code,
            "stock_name": pos.get("stock_name", code),
            "buy_date": pos["buy_date"],
            "buy_price": pos["buy_price"],
            "quantity": pos.get("quantity", 100),
            "score": pos.get("score", 0),
        }]

        context = {
            "stocks": stocks,
            "today": trade_date,
            "prices": {code: price},
            "query_kline_db": query_kline_db,
            "mode": "sell",  # 标记为卖出检查模式
        }

        # 执行策略代码
        try:
            signals = self._execute_strategy_code(context)
            for signal in signals:
                if signal.get("stock_code") == code:
                    action = signal.get("action", "")
                    if action in ("sell", "reduce_half", "reduce_30"):
                        return signal.get("reason", "策略卖出")
        except Exception as e:
            logger.warn("backtest", f"策略卖出检查失败 ({code}): {e}")

        return None

    def _screen_candidates(self, trade_date: str, prices: Dict[str, float], pool_set: set, holdings: Dict) -> List[Dict]:
        """每日选股：筛选满足买入条件的股票"""
        if self.is_code_strategy:
            return self._screen_candidates_code(trade_date, prices, pool_set, holdings)

        # 配置型策略：使用 ConditionParser
        candidates = []

        for code, price in prices.items():
            if code not in pool_set:
                continue
            if code in holdings:  # 已持有则跳过
                continue

            indicators = self._get_cached_indicators(code, trade_date, price)
            if not indicators:
                continue

            normalized = normalize_conditions(self.buy_conditions)
            if self.parser.evaluate(normalized, indicators):
                candidates.append({
                    "stock_code": code,
                    "price": price,
                    "stock_name": indicators.get("stock_name", code),
                })

        return candidates

    def _screen_candidates_code(self, trade_date: str, prices: Dict[str, float], pool_set: set, holdings: Dict) -> List[Dict]:
        """代码型策略：在沙箱中执行策略获取买入信号"""
        if not self.strategy_code:
            return []

        # 构建执行上下文
        context = self._build_backtest_context(trade_date, prices, pool_set)

        # 沙箱执行策略
        try:
            signals = self._execute_strategy_code(context)
        except Exception as e:
            logger.warn("backtest", f"策略执行失败 ({trade_date}): {e}")
            return []

        # 转换信号为候选列表
        candidates = []
        for signal in signals:
            action = signal.get("action", "")
            if action not in ("buy", "watch"):  # watch 也是候选，但标记为观察
                continue

            code = signal.get("stock_code", "")
            if code in holdings:  # 已持有则跳过
                continue

            price = prices.get(code, signal.get("buy_price", 0))
            if price <= 0:
                continue

            candidates.append({
                "stock_code": code,
                "price": price,
                "stock_name": signal.get("stock_name", code),
                "score": signal.get("score", 0),
                "reason": signal.get("reason", ""),
                "action": action,  # buy 或 watch
            })

        return candidates

    def _build_backtest_context(self, trade_date: str, prices: Dict[str, float], pool_set: set) -> Dict:
        """构建回测用执行上下文"""
        from services.common.database import get_sync_connection

        # 股票列表
        stocks = [
            {"stock_code": code, "stock_name": self._stock_name_map.get(code, code)}
            for code in pool_set
        ]

        # 数据库查询函数（截断到当前回测日期）
        def query_kline_db(sql: str, params: tuple = None):
            conn = get_sync_connection("kline")
            cursor = conn.cursor()

            sql_upper = sql.strip().upper()
            if not sql_upper.startswith("SELECT"):
                if params:
                    cursor.execute(sql, params)
                else:
                    cursor.execute(sql)
                return cursor.rowcount

            # 检查日期字段并截断
            sql_lower = sql.lower()
            has_trade_date = "trade_date" in sql_lower
            has_week_start_date = "week_start_date" in sql_lower

            if not (has_trade_date or has_week_start_date):
                if params:
                    cursor.execute(sql, params)
                else:
                    cursor.execute(sql)
                rows = cursor.fetchall()
                conn.close()
                return [dict(r) for r in rows] if rows else []

            date_field = "week_start_date" if has_week_start_date else "trade_date"

            import re
            where_match = re.search(r'\bWHERE\b', sql_upper)
            order_match = re.search(r'\bORDER\s+BY\b', sql_upper)
            limit_match_pos = re.search(r'\bLIMIT\b', sql_upper)

            if where_match:
                # 已有 WHERE，在现有条件末尾添加 AND
                where_end = where_match.end()
                next_clause_pos = len(sql)
                for m in [order_match, limit_match_pos]:
                    if m and m.start() > where_end:
                        next_clause_pos = min(next_clause_pos, m.start())

                # 找到 WHERE 子句条件的末尾位置
                after_where_clause = sql[where_end:next_clause_pos]
                insert_pos = where_end + len(after_where_clause.rstrip())
                new_sql = sql[:insert_pos] + f" AND {date_field} <= '{trade_date}'" + sql[insert_pos:]
            else:
                # 没有 WHERE，添加 WHERE
                insert_pos = len(sql)
                for m in [order_match, limit_match_pos]:
                    if m:
                        insert_pos = min(insert_pos, m.start())
                new_sql = sql[:insert_pos] + f" WHERE {date_field} <= '{trade_date}'" + sql[insert_pos:]

            if params:
                cursor.execute(new_sql, params)
            else:
                cursor.execute(new_sql)

            rows = cursor.fetchall()
            conn.close()
            return [dict(r) for r in rows] if rows else []

        context = {
            "stocks": stocks,
            "today": trade_date,
            "prices": prices,
            "query_kline_db": query_kline_db,
        }

        return context

    def _execute_strategy_code(self, context: Dict) -> List[Dict]:
        """在沙箱中执行策略代码"""
        if not self.strategy_code:
            return []

        # 安全导入限制
        ALLOWED_MODULES = {"typing", "datetime", "collections", "math", "re", "time"}
        def safe_import(name, *args, **kwargs):
            if name in ALLOWED_MODULES:
                return __import__(name, *args, **kwargs)
            raise ImportError(f"模块 '{name}' 不允许在策略沙箱中导入")

        safe_globals = {
            "__builtins__": {
                "abs": abs, "min": min, "max": max, "sum": sum, "len": len,
                "round": round, "int": int, "float": float, "str": str, "bool": bool,
                "list": list, "dict": dict, "tuple": tuple, "set": set,
                "True": True, "False": False, "None": None,
                "sorted": sorted, "enumerate": enumerate,
                "range": range, "zip": zip, "map": map, "filter": filter,
                "any": any, "all": all, "isinstance": isinstance, "type": type,
                "reversed": reversed, "print": print,
                "__import__": safe_import,
            },
            "datetime": datetime,
            "List": List,
            "Dict": Dict,
            "Optional": Optional,
            "Any": Any,
        }

        try:
            exec(self.strategy_code, safe_globals)
            func = safe_globals.get(self.strategy_function_name)
            if func and callable(func):
                return func(context) or []
        except Exception as e:
            logger.warn("backtest", f"策略代码执行失败: {e}")

        return []

    def _get_cached_indicators(self, stock_code: str, trade_date: str, current_price: float) -> Dict:
        """获取股票指标（优先用缓存）"""
        # 先查因子缓存
        cached = self._factor_cache.get(trade_date, {}).get(stock_code)
        if cached:
            cached["price"] = current_price
            cached["PRICE"] = current_price
            cached["stock_name"] = self._stock_name_map.get(stock_code, stock_code)
            return cached

        # 缓存未命中，从K线计算（较慢）
        return self._get_stock_indicators(stock_code, trade_date, current_price)

    def _get_stock_indicators(self, stock_code: str, trade_date: str, current_price: float) -> Dict:
        """获取单只股票在指定日期的技术指标"""
        from datetime import timedelta
        try:
            dt = datetime.strptime(trade_date, "%Y-%m-%d")
            start_dt = dt - timedelta(days=120)
            start_str = start_dt.strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            start_str = None

        kline = self.km.get_kline_data(stock_code, start_date=start_str, end_date=trade_date)
        if kline is None or len(kline) < 26:
            return {}

        if len(kline) > 60:
            kline = kline.iloc[-60:].reset_index(drop=True)

        closes = kline["close"].tolist() if hasattr(kline, "__getitem__") else []
        highs = kline["high"].tolist() if hasattr(kline, "__getitem__") else []
        lows = kline["low"].tolist() if hasattr(kline, "__getitem__") else []
        volumes = kline["volume"].tolist() if hasattr(kline, "__getitem__") else []

        if len(closes) < 26:
            return {}

        from services.common.technical_indicators import calculate_indicators_for_screening

        indicators = calculate_indicators_for_screening(closes, highs, lows, volumes)
        indicators["price"] = current_price
        indicators["PRICE"] = current_price
        indicators["stock_name"] = self._stock_name_map.get(stock_code, stock_code)

        return indicators

    def _extract_buy_conditions(self, config: Dict) -> List:
        """从策略配置中提取买入条件"""
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