"""
信号评估器 — 策略评估、卖出决策、watchlist 监控。
"""
import time
from typing import Dict, List, Optional, Any

from services.common.database import get_db_manager
from services.common.timezone import get_china_time
from services.common.structured_logger import get_logger


class SignalEvaluator:
    """信号评估：策略触发 + 卖出决策 + watchlist 止损止盈"""

    def __init__(self, executor):
        self._executor = executor
        self._cooldown: Dict[tuple, float] = {}
        self._strategy_cache: Dict[int, Any] = {}

    async def evaluate_trading_strategies(self, account_id: str):
        """评估交易策略配置中的触发条件"""
        from services.trading.strategy_executor import get_strategy_executor

        executor = get_strategy_executor(account_id)
        strategies = await executor.load_strategies(enabled_only=True)
        if not strategies:
            return

        db = get_db_manager()
        watchlist = await db.fetchall(
            "SELECT stock_code FROM watchlist WHERE account_id = ? AND status IN ('pending', 'watching', 'bought')",
            (account_id,),
        )
        stock_codes = [w["stock_code"] for w in watchlist]
        if not stock_codes:
            return

        decisions = await executor.evaluate_all(strategies, stock_codes)
        for decision in decisions:
            strategy_id = decision["strategy_id"]
            cooldown_seconds = await self._get_cooldown_for_strategy(strategy_id)
            cooldown_key = (account_id, strategy_id)
            last_trigger = self._cooldown.get(cooldown_key, 0)
            if time.time() - last_trigger < cooldown_seconds:
                continue

            self._cooldown[cooldown_key] = time.time()

            if decision["action"] == "buy":
                await self._executor.execute_buy_signal(
                    account_id,
                    {"stock_code": decision["stock_code"], "stock_name": decision["stock_name"]},
                    decision["trigger_data"]["current_price"],
                    100,
                    trigger_source=decision["strategy_name"],
                )
            elif decision["action"] == "sell":
                await self._executor.execute_sell_signal(
                    account_id,
                    {"stock_code": decision["stock_code"], "stock_name": decision["stock_name"]},
                    decision["trigger_data"]["current_price"],
                    f"strategy_{decision['strategy_type']}",
                    0,
                    trigger_source=decision["strategy_name"],
                )

    async def _get_cooldown_for_strategy(self, strategy_id: int) -> int:
        try:
            db = get_db_manager()
            row = await db.fetchone(
                "SELECT cooldown_seconds FROM trading_strategy_config WHERE id = ?",
                (strategy_id,)
            )
            if row and row.get("cooldown_seconds"):
                return row["cooldown_seconds"]
        except Exception:
            pass
        return 300

    async def monitor_watchlist(self, account_id: str, market_data_cache: Optional[Dict[str, Any]] = None):
        """传统的 watchlist 止损止盈监控（批量行情 + 策略预加载）"""
        db = get_db_manager()
        watchlist = await db.fetchall(
            "SELECT * FROM watchlist WHERE account_id = ? AND status IN ('pending', 'watching', 'bought')",
            (account_id,),
        )
        if not watchlist:
            return

        stock_codes = [w["stock_code"] for w in watchlist]

        # 批量获取行情
        if market_data_cache:
            batch_data = market_data_cache
        else:
            from services.common.price_cache import get_price_cache
            from services.trading.gateway import MarketData
            cache = get_price_cache()
            batch_ohlcv = cache.get_all_for_codes(set(stock_codes))
            if batch_ohlcv:
                batch_data = {}
                for code, ohlcv in batch_ohlcv.items():
                    batch_data[code] = MarketData(
                        stock_code=code, stock_name="",
                        current_price=ohlcv.get('close', 0),
                        change_percent=ohlcv.get('change_pct', 0),
                        high=ohlcv.get('high', 0), low=ohlcv.get('low', 0),
                        open_price=ohlcv.get('open', 0), prev_close=ohlcv.get('close', 0),
                        volume=int(ohlcv.get('volume', 0)), amount=ohlcv.get('amount', 0),
                        source=ohlcv.get('source', ''),
                    )
            else:
                get_logger("monitor").warn("monitor", f"_monitor_watchlist 无缓存数据，跳过账户 {account_id}")
                return

        # 预加载交易策略配置
        ts_rows = await db.fetchall("SELECT * FROM trading_strategies WHERE account_id = ?", (account_id,))
        ts_map: Dict[str, Dict] = {}
        for ts in ts_rows:
            code = ts.get("stock_code")
            if code:
                ts_map[code] = ts

        # 预加载卖出策略 ID
        sell_strategy_ids = set()
        strategy_id_map: Dict[str, Optional[int]] = {}
        for stock in watchlist:
            sid = stock.get("strategy_id")
            sell_sid = None
            if sid:
                row = await db.fetchone(
                    "SELECT sell_strategy_id FROM strategies WHERE id = ? AND account_id = ? AND strategy_type = 'python'",
                    (sid, account_id),
                )
                if row and row.get("sell_strategy_id"):
                    sell_sid = row["sell_strategy_id"]
                    sell_strategy_ids.add(sell_sid)
            strategy_id_map[stock["stock_code"]] = sell_sid

        # 预编译卖出策略代码
        if sell_strategy_ids:
            placeholders = ",".join("?" for _ in sell_strategy_ids)
            strategies = await db.fetchall(
                f"SELECT id, code FROM strategies WHERE id IN ({placeholders}) AND strategy_type = 'python'",
                list(sell_strategy_ids),
            )
            for s in strategies:
                sid = s["id"]
                code_text = s.get("code", "")
                if code_text and sid not in self._strategy_cache:
                    try:
                        compiled = compile(code_text, f"<strategy_{sid}>", "exec")
                        self._strategy_cache[sid] = compiled
                        get_logger("monitor").log_event("strategy_compiled", f"策略 #{sid} 已编译缓存", strategy_id=sid)
                    except Exception as e:
                        get_logger("monitor").error("monitor", f"策略 #{sid} 编译失败: {e}")
                        self._strategy_cache[sid] = None

        # 批量预加载 K 线缓存
        kline_cache: Dict[str, Any] = {}
        try:
            from services.data.local_data_service import get_local_data_service
            lds = get_local_data_service()
            kline_cache = lds.get_batch_kline(stock_codes, limit=60)
        except Exception:
            pass

        # 逐个检查
        for stock in watchlist:
            stock_code = stock.get("stock_code")
            market_data = batch_data.get(stock_code)
            if not market_data or market_data.current_price <= 0:
                continue
            stock_with_price = {**stock, "current_price": market_data.current_price}
            await self._check_stock_signals_with_price(
                account_id, stock_with_price,
                ts_map=ts_map,
                sell_strategy_map=strategy_id_map,
                kline_cache=kline_cache,
            )

    async def evaluate_positions_stop_loss(self, account_id: str, market_data_cache: Optional[Dict[str, Any]] = None):
        """评估不在 watchlist 中的持仓的止损止盈

        持仓股票可能不在 watchlist 中（status='unknown'），但仍需要检查 trading_strategies 配置的止损。
        此方法作为 monitor_watchlist 的补充，确保所有持仓都被评估。
        """
        db = get_db_manager()

        # 获取所有持仓
        positions = await db.fetchall(
            "SELECT stock_code, stock_name, quantity, avg_cost, highest_price FROM stock_positions WHERE account_id = ? AND quantity > 0",
            (account_id,),
        )
        if not positions:
            return

        # 获取已在 watchlist 中被监控的股票（避免重复）
        watchlist_codes = await db.fetchall(
            "SELECT DISTINCT stock_code FROM watchlist WHERE account_id = ? AND status IN ('pending', 'watching', 'bought')",
            (account_id,),
        )
        monitored_codes = {r["stock_code"] for r in watchlist_codes}

        # 过滤出不在 watchlist 的持仓
        unmonitored_positions = [p for p in positions if p["stock_code"] not in monitored_codes]
        if not unmonitored_positions:
            return

        get_logger("monitor").log_event("position_sl_check",
            f"检查 {len(unmonitored_positions)} 只不在 watchlist 的持仓止损",
            account_id=account_id, count=len(unmonitored_positions))

        # 获取 trading_strategies 配置
        ts_rows = await db.fetchall(
            "SELECT stock_code, stop_loss_pct, take_profit_pct, stop_loss_price, take_profit_price, strategy_type FROM trading_strategies WHERE account_id = ?",
            (account_id,),
        )
        ts_map = {r["stock_code"]: r for r in ts_rows}

        # 批量获取行情
        if market_data_cache:
            batch_data = market_data_cache
        else:
            from services.common.price_cache import get_price_cache
            from services.trading.gateway import MarketData
            cache = get_price_cache()
            codes = [p["stock_code"] for p in unmonitored_positions]
            batch_ohlcv = cache.get_all_for_codes(set(codes))
            if batch_ohlcv:
                batch_data = {}
                for code, ohlcv in batch_ohlcv.items():
                    batch_data[code] = MarketData(
                        stock_code=code, stock_name="",
                        current_price=ohlcv.get('close', 0),
                        change_percent=ohlcv.get('change_pct', 0),
                        high=ohlcv.get('high', 0), low=ohlcv.get('low', 0),
                        open_price=ohlcv.get('open', 0), prev_close=ohlcv.get('close', 0),
                        volume=int(ohlcv.get('volume', 0)), amount=ohlcv.get('amount', 0),
                        source=ohlcv.get('source', ''),
                    )
            else:
                return

        # 逐个检查止损
        for pos in unmonitored_positions:
            stock_code = pos["stock_code"]
            stock_name = pos.get("stock_name", stock_code)
            avg_cost = pos.get("avg_cost", 0) or 0
            highest_price = pos.get("highest_price", 0) or 0

            md = batch_data.get(stock_code)
            if not md or md.current_price <= 0:
                continue

            current_price = md.current_price

            # 从 trading_strategies 获取止损配置
            ts = ts_map.get(stock_code)
            if not ts:
                continue  # 没有止损配置，跳过

            ts_sl_price = ts.get("stop_loss_price", 0) or 0
            ts_sl_pct = ts.get("stop_loss_pct", 0) or 0
            ts_tp_price = ts.get("take_profit_price", 0) or 0
            ts_tp_pct = ts.get("take_profit_pct", 0) or 0
            ts_stype = ts.get("strategy_type", "fixed") or "fixed"

            # 计算止损价
            sl = 0
            if ts_sl_price > 0:
                sl = ts_sl_price
            elif ts_stype == "trailing_stop" and highest_price > 0 and ts_tp_pct > 0:
                sl = highest_price * (1 - ts_tp_pct)
            elif ts_sl_pct > 0 and avg_cost > 0:
                sl = avg_cost * (1 - ts_sl_pct)

            # 计算止盈价
            tp = ts_tp_price if ts_tp_price > 0 else (avg_cost * (1 + ts_tp_pct) if ts_tp_pct > 0 and avg_cost > 0 else 0)

            # 检查是否触发
            should_sell = False
            reason = ""
            if sl > 0 and current_price <= sl:
                should_sell = True
                reason = "stop_loss"
            elif tp > 0 and current_price >= tp:
                should_sell = True
                reason = "take_profit"

            if should_sell:
                signal_type = "sell_stop_loss" if reason == "stop_loss" else "sell_take_profit"
                get_logger("monitor").log_event("position_sell_signal",
                    f"持仓止损触发：{stock_code} {stock_name}, 原因: {reason}",
                    stock_code=stock_code, reason=reason,
                    stop_loss_price=sl, take_profit_price=tp, current_price=current_price)

                stock_info = {
                    "stock_code": stock_code,
                    "stock_name": stock_name,
                    "avg_cost": avg_cost,
                    "highest_price": highest_price,
                }
                await self._executor.execute_sell_signal(
                    account_id, stock_info, current_price, signal_type, 0, trigger_source="position_stop_loss"
                )

    async def _check_stock_signals_with_price(
        self, account_id: str, stock: Dict,
        ts_map: Optional[Dict[str, Dict]] = None,
        sell_strategy_map: Optional[Dict[str, Optional[int]]] = None,
        kline_cache: Optional[Dict[str, Any]] = None,
    ):
        """检查单只股票的交易信号（行情已预取版本）"""
        stock_code = stock.get('stock_code')
        trigger_price = stock.get('trigger_price', 0)
        target_quantity = stock.get('target_quantity') or 0
        status = stock.get('status')
        current_price = stock.get('current_price')
        source_type = stock.get('source_type', 'screening')
        signal_type = stock.get('signal_type', 'buy')

        db = get_db_manager()

        if current_price is None or current_price <= 0:
            return

        if status == 'pending':
            if source_type == 'manual':
                if signal_type == 'sell':
                    if trigger_price > 0 and current_price >= trigger_price:
                        get_logger("monitor").log_event("manual_sell_signal",
                            f"执行手动卖出信号：{stock_code}, 触发价：{trigger_price:.2f}, 现价：{current_price:.2f}, 数量：{target_quantity}",
                            stock_code=stock_code, trigger_price=trigger_price, current_price=current_price, quantity=target_quantity)
                        await self._executor.execute_sell_signal(
                            account_id,
                            {"stock_code": stock_code, "stock_name": stock.get("stock_name", stock_code)},
                            trigger_price, "sell", target_quantity, trigger_source="manual",
                        )
                        # 使用 executor 方法，会自动检查剩余持仓
                        await self._executor._update_watchlist_status(account_id, stock_code, 'sold')
                    elif trigger_price <= 0:
                        await self._executor.execute_sell_signal(
                            account_id,
                            {"stock_code": stock_code, "stock_name": stock.get("stock_name", stock_code)},
                            current_price, "sell", target_quantity, trigger_source="manual",
                        )
                        # 使用 executor 方法，会自动检查剩余持仓
                        await self._executor._update_watchlist_status(account_id, stock_code, 'sold')
                else:
                    if trigger_price > 0 and current_price <= trigger_price:
                        get_logger("monitor").log_event("manual_buy_signal",
                            f"执行手动买入信号：{stock_code}, 触发价：{trigger_price:.2f}, 现价：{current_price:.2f}",
                            stock_code=stock_code, trigger_price=trigger_price, current_price=current_price)
                        await self._executor.execute_buy_signal(account_id, stock, trigger_price, target_quantity)
            else:
                if trigger_price > 0 and current_price <= trigger_price:
                    get_logger("monitor").log_event("buy_signal",
                        f"触发买入信号：{stock_code}, 触发价：{trigger_price:.2f}, 当前价：{current_price:.2f}",
                        stock_code=stock_code, trigger_price=trigger_price, current_price=current_price)
                    await self._executor.create_pending_signal(account_id, stock, 'buy', current_price, target_quantity)
                    await self._executor.execute_buy_signal(account_id, stock, trigger_price, target_quantity)

        elif status in ('watching', 'bought'):
            position = await db.fetchone(
                "SELECT quantity, avg_cost, highest_price FROM stock_positions WHERE account_id = ? AND stock_code = ?",
                (account_id, stock_code),
            )
            if not position or position.get("quantity", 0) == 0:
                return

            # 将持仓信息合并到 stock 对象（用于止损止盈计算）
            stock = {**stock, "avg_cost": position.get("avg_cost"), "highest_price": position.get("highest_price")}

            ts_config = ts_map.get(stock_code) if ts_map else None
            sell_sid = sell_strategy_map.get(stock_code) if sell_strategy_map else None

            decision = await self._evaluate_sell_decision(
                account_id, stock_code, stock, current_price,
                screening_strategy_id=stock.get("strategy_id"),
                ts_config=ts_config,
                sell_strategy_id=sell_sid,
                kline_cache=kline_cache,
            )
            if decision["should_sell"]:
                reason = decision["reason"]
                signal_type = "sell_stop_loss" if reason == "stop_loss" else (
                    "sell_take_profit" if reason == "take_profit" else f"sell_{reason}"
                )
                get_logger("monitor").log_event("sell_signal",
                    f"触发卖出：{stock_code}, 原因: {reason}",
                    stock_code=stock_code, reason=reason,
                    stop_loss_price=decision.get('stop_loss_price'),
                    take_profit_price=decision.get('take_profit_price'),
                    current_price=current_price)
                await self._executor.create_pending_signal(account_id, stock, signal_type, current_price, 0)
                await self._executor.execute_sell_signal(account_id, stock, current_price, signal_type, 0)

    async def _evaluate_sell_decision(
        self, account_id: str, stock_code: str, stock: Dict, current_price: float,
        screening_strategy_id: Optional[int] = None,
        ts_config: Optional[Dict] = None,
        sell_strategy_id: Optional[int] = None,
        kline_cache: Optional[Dict[str, Any]] = None,
    ) -> Dict:
        """评估是否应该卖出 — 2 优先级：卖出代码策略 → watchlist 止盈止损"""
        db = get_db_manager()
        result = {"should_sell": False, "reason": "", "stop_loss_price": 0, "take_profit_price": 0}

        # 优先级 1：关联的卖出代码策略
        if screening_strategy_id:
            if sell_strategy_id is None:
                sell_strategy_id = await db.fetchval(
                    "SELECT sell_strategy_id FROM strategies WHERE id = ? AND account_id = ?",
                    (screening_strategy_id, account_id),
                )
            if sell_strategy_id:
                code_result = await self._evaluate_sell_strategy_code(
                    account_id, stock_code, sell_strategy_id, current_price, stock,
                    kline_cache=kline_cache,
                )
                if code_result["should_sell"]:
                    return code_result

        # 优先级 2：止损止盈判断
        # 先看 watchlist 的固定价格，如果没有则用 trading_strategies 配置计算
        sl = stock.get("stop_loss_price", 0) or 0
        tp = stock.get("take_profit_price", 0) or 0

        # 如果 watchlist 没有止损价，使用 ts_config 计算
        if sl == 0 and ts_config:
            avg_cost = stock.get("avg_cost", 0) or 0
            highest_price = stock.get("highest_price", 0) or 0
            strategy_type = ts_config.get("strategy_type", "fixed")
            stop_loss_pct = ts_config.get("stop_loss_pct", 0) or 0
            stop_loss_price_ts = ts_config.get("stop_loss_price", 0) or 0
            take_profit_pct = ts_config.get("take_profit_pct", 0) or 0
            take_profit_price_ts = ts_config.get("take_profit_price", 0) or 0

            # 计算止损价
            if stop_loss_price_ts > 0:
                sl = stop_loss_price_ts
            elif strategy_type == "trailing_stop" and highest_price > 0 and take_profit_pct > 0:
                # 移动止损：最高价 × (1 - take_profit_pct)
                sl = highest_price * (1 - take_profit_pct)
            elif stop_loss_pct > 0 and avg_cost > 0:
                # 固定百分比止损：成本价 × (1 - stop_loss_pct)
                sl = avg_cost * (1 - stop_loss_pct)

            # 计算止盈价
            if take_profit_price_ts > 0:
                tp = take_profit_price_ts
            elif take_profit_pct > 0 and avg_cost > 0:
                tp = avg_cost * (1 + take_profit_pct)

        result["stop_loss_price"] = float(sl)
        result["take_profit_price"] = float(tp)

        if current_price <= 0:
            result["should_sell"] = False
            result["reason"] = "invalid_price"
        elif sl > 0 and current_price <= sl:
            result["should_sell"] = True
            result["reason"] = "stop_loss"
        elif tp > 0 and current_price >= tp:
            result["should_sell"] = True
            result["reason"] = "take_profit"

        return result

    async def _evaluate_sell_strategy_code(
        self, account_id: str, stock_code: str, sell_strategy_id: int,
        current_price: float, stock: Dict, kline_cache: Optional[Dict[str, Any]] = None,
    ) -> Dict:
        """执行关联的卖出代码策略，返回卖出决策结果"""
        db = get_db_manager()

        strategy = await db.fetchone(
            "SELECT id, name, code FROM strategies WHERE id = ? AND account_id = ? AND strategy_type = 'python'",
            (sell_strategy_id, account_id),
        )
        if not strategy:
            get_logger("monitor").error("monitor", f"卖出策略 #{sell_strategy_id} 不存在",
                                         strategy_id=sell_strategy_id)
            return {"should_sell": False, "reason": "sell_strategy_not_found"}

        from services.common import technical_indicators

        kline_df = None
        if kline_cache and stock_code in kline_cache:
            kline_df = kline_cache[stock_code]
        else:
            from services.data.local_data_service import get_local_data_service
            lds = get_local_data_service()
            kline_df = lds.get_kline_data(stock_code, limit=60)
            if kline_df is not None and kline_cache is not None:
                kline_cache[stock_code] = kline_df

        if kline_df is not None and hasattr(kline_df, 'to_dict'):
            kline_data = kline_df.to_dict('records')
        else:
            kline_data = kline_df if kline_df else []

        def _get_kline_local(sc, limit=60, start_date=None):
            if kline_cache and sc in kline_cache:
                return kline_cache[sc]
            from services.data.local_data_service import get_local_data_service
            lds = get_local_data_service()
            return lds.get_kline_data(sc, start_date=start_date, limit=limit)

        def _get_kline_local_single(sc, limit=60):
            result = _get_kline_local(sc, limit=limit)
            if hasattr(result, 'to_dict'):
                return result.to_dict('records')
            return result if result else []

        def _get_realtime_quote_sync(sc):
            return stock

        context = {
            "account_id": account_id, "stock_code": stock_code,
            "stock_name": stock.get("stock_name", stock_code),
            "current_price": current_price,
            "trigger_price": stock.get("trigger_price", 0),
            "stop_loss_price": stock.get("stop_loss_price", 0),
            "take_profit_price": stock.get("take_profit_price", 0),
            "kline_data": kline_data,
            "today": get_china_time().strftime("%Y-%m-%d"),
            "get_kline_local": _get_kline_local,
            "get_realtime_quote": _get_realtime_quote_sync,
            "indicators": {
                "calculate_ma": technical_indicators.calculate_ma,
                "calculate_rsi": technical_indicators.calculate_rsi,
                "calculate_macd": technical_indicators.calculate_macd,
                "calculate_kdj": technical_indicators.calculate_kdj,
                "calculate_bollinger_bands": technical_indicators.calculate_bollinger_bands,
                "calculate_adx": technical_indicators.calculate_adx,
                "calculate_atr": technical_indicators.calculate_atr,
                "calculate_ema": technical_indicators.calculate_ema,
            },
        }

        from services.strategy.engine import get_strategy_engine
        engine = get_strategy_engine()
        try:
            signals = engine.execute_strategy(strategy, context)
        except Exception as e:
            get_logger("monitor").error("monitor", f"卖出策略 {sell_strategy_id} 执行失败 {stock_code}: {e}",
                                         strategy_id=sell_strategy_id, stock_code=stock_code)
            return {"should_sell": False, "reason": f"strategy_execution_error: {e}"}

        for signal in signals:
            if signal.get("action") == "sell":
                return {
                    "should_sell": True,
                    "reason": signal.get("reason", "sell_strategy_code"),
                    "stop_loss_price": signal.get("stop_loss_price", 0),
                    "take_profit_price": signal.get("take_profit_price", 0),
                }

        return {"should_sell": False, "reason": "no_sell_signal"}
