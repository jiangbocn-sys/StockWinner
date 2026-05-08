"""
交易监控模块 (Trading Monitor)
监控 watchlist 中的股票，根据策略条件执行交易

功能：
1. 按 watchlist 监控候选股票行情，到达预设买卖价位进行交易
2. 读取交易策略配置（trading_strategy_config），评估触发条件
3. 读取持仓策略，确定买入份额
4. 交易前读取账户可用资金，确定可买数量
5. 交易后更新可用资金 + 发送通知
6. 计算并记录交易手续费
"""

import asyncio
from typing import Dict, List, Optional, Any
from services.common.database import get_db_manager
from services.common.timezone import get_china_time
from services.trading.gateway import get_gateway, MarketData
from services.trading.execution_service import get_trade_execution_service
from services.trading.strategy_executor import get_strategy_executor
from services.notifications import get_notification_service


class TradingMonitor:
    """交易监控服务"""

    def __init__(self):
        self._running = False
        self._task = None
        self._account_id = None
        # 冷却期记录：{strategy_id: last_trigger_time}
        self._cooldown: Dict[int, float] = {}

    async def start_monitoring(
        self,
        account_id: str,
        interval: int = 30
    ):
        """
        启动交易监控服务

        Args:
            account_id: 账户 ID
            interval: 监控间隔（秒）
        """
        if self._running:
            return {"success": False, "message": "交易监控服务已在运行"}

        self._running = True
        self._account_id = account_id
        self._task = asyncio.create_task(
            self._run_monitoring_loop(account_id, interval)
        )
        return {"success": True, "message": "交易监控服务已启动"}

    async def stop_monitoring(self):
        """停止交易监控服务"""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        return {"success": True, "message": "交易监控服务已停止"}

    async def _run_monitoring_loop(
        self,
        account_id: str,
        interval: int
    ):
        """交易监控循环"""
        print(f"[Monitor] 启动交易监控服务 - 账户：{account_id}, 间隔：{interval}s")

        while self._running:
            try:
                await self._run_monitoring(account_id)
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"[Monitor] 错误：{e}")
                await asyncio.sleep(interval)

        print(f"[Monitor] 交易监控服务已停止")

    async def _run_monitoring(self, account_id: str):
        """执行一次交易监控"""
        db = get_db_manager()

        # === 第一部分：基于交易策略的触发评估 ===
        await self._evaluate_trading_strategies(account_id)

        # === 第二部分：基于 watchlist 的传统监控（止损止盈）===
        await self._monitor_watchlist(account_id)

        # === 第三部分：刷新持仓盈亏 ===
        await self._refresh_positions_pnl(account_id)

        # === 第四部分：更新 watchlist 现价 ===
        await self._update_watchlist_current_price(account_id)

    async def _evaluate_trading_strategies(self, account_id: str):
        """评估交易策略配置中的触发条件"""
        executor = get_strategy_executor(account_id)
        strategies = await executor.load_strategies(enabled_only=True)

        if not strategies:
            return

        # 获取 watchlist 中的股票代码
        db = get_db_manager()
        watchlist = await db.fetchall(
            "SELECT stock_code FROM watchlist WHERE account_id = ? AND status IN ('pending', 'watching', 'bought')",
            (account_id,),
        )
        stock_codes = [w["stock_code"] for w in watchlist]

        if not stock_codes:
            return

        # 评估所有策略
        decisions = await executor.evaluate_all(strategies, stock_codes)

        for decision in decisions:
            strategy_id = decision["strategy_id"]
            cooldown_seconds = self._get_cooldown_for_strategy(strategy_id)

            # 检查冷却期
            import time
            last_trigger = self._cooldown.get(strategy_id, 0)
            if time.time() - last_trigger < cooldown_seconds:
                continue  # 冷却期内，跳过

            # 记录触发时间
            self._cooldown[strategy_id] = time.time()

            # 执行交易
            if decision["action"] == "buy":
                await self._execute_buy_signal(
                    account_id,
                    {"stock_code": decision["stock_code"], "stock_name": decision["stock_name"]},
                    decision["trigger_data"]["current_price"],
                    100,  # 默认 100 股，后续可从策略配置读取
                    trigger_source=decision["strategy_name"],
                )
            elif decision["action"] == "sell":
                await self._execute_sell_signal(
                    account_id,
                    {"stock_code": decision["stock_code"], "stock_name": decision["stock_name"]},
                    decision["trigger_data"]["current_price"],
                    0,  # 全部卖出
                    signal_type=f"strategy_{decision['strategy_type']}",
                    trigger_source=decision["strategy_name"],
                )

    def _get_cooldown_for_strategy(self, strategy_id: int) -> int:
        """获取策略的冷却时间"""
        import sqlite3
        from pathlib import Path
        db_path = Path(__file__).parent.parent.parent / "data" / "stockwinner.db"
        try:
            conn = sqlite3.connect(str(db_path))
            cursor = conn.cursor()
            cursor.execute("SELECT cooldown_seconds FROM trading_strategy_config WHERE id = ?", (strategy_id,))
            row = cursor.fetchone()
            conn.close()
            if row:
                return row[0]
        except Exception:
            pass
        return 300  # 默认 5 分钟

    async def _monitor_watchlist(self, account_id: str):
        """传统的 watchlist 止损止盈监控（批量行情）"""
        db = get_db_manager()

        # 获取 watchlist 中的股票
        watchlist = await db.fetchall("""
            SELECT * FROM watchlist
            WHERE account_id = ? AND status IN ('pending', 'watching', 'bought')
        """, (account_id,))

        if not watchlist:
            return

        stock_codes = [w["stock_code"] for w in watchlist]

        # 批量获取行情数据（一次 SDK 调用）
        try:
            from services.trading.gateway import get_gateway
            gateway = await get_gateway()
            batch_data = await gateway.get_batch_market_data(stock_codes)
        except Exception as e:
            print(f"[Monitor] 批量获取行情失败: {e}")
            # 降级为逐个获取
            for stock in watchlist:
                await self._check_stock_signals(account_id, stock)
            return

        # 逐个检查止损止盈
        for stock in watchlist:
            stock_code = stock.get("stock_code")
            market_data = batch_data.get(stock_code)
            if not market_data:
                continue

            # 注入行情数据到 stock 对象
            stock_with_price = {**stock, "current_price": market_data.current_price}
            await self._check_stock_signals_with_price(account_id, stock_with_price)

    async def _refresh_positions_pnl(self, account_id: str):
        """刷新持仓盈亏（从行情数据更新 current_price）"""
        from services.trading.gateway import get_gateway
        from services.trading.position_manager import get_position_manager

        db = get_db_manager()
        positions = await db.fetchall(
            "SELECT stock_code FROM stock_positions WHERE account_id = ?",
            (account_id,),
        )
        if not positions:
            return

        stock_codes = [p["stock_code"] for p in positions]
        try:
            gateway = await get_gateway()
            batch_data = await gateway.get_batch_market_data(stock_codes)
        except Exception:
            return

        prices = {}
        for code in stock_codes:
            md = batch_data.get(code)
            if md:
                prices[code] = md.current_price

        if prices:
            pm = get_position_manager(account_id)
            count = await pm.refresh_pnl(prices)
            if count > 0:
                print(f"[Monitor] 已刷新 {count} 只持仓盈亏")

    async def _update_watchlist_current_price(self, account_id: str):
        """更新 watchlist 中所有股票的 current_price 字段

        批量获取所有监控中股票的实时行情，写入数据库。
        候选股票清单和持仓分析页面都依赖此字段显示实时价格。
        """
        db = get_db_manager()

        # 获取所有需要更新现价的 watchlist 记录
        watchlist = await db.fetchall(
            "SELECT stock_code FROM watchlist WHERE account_id = ? AND status IN ('pending', 'watching', 'bought')",
            (account_id,),
        )
        if not watchlist:
            return

        stock_codes = [w["stock_code"] for w in watchlist]

        # 批量获取行情
        try:
            from services.trading.gateway import get_gateway
            gateway = await get_gateway()
            batch_data = await gateway.get_batch_market_data(stock_codes)
        except Exception as e:
            print(f"[Monitor] 批量获取行情失败，跳过现价更新: {e}")
            return

        # 批量更新
        update_list = []
        for code in stock_codes:
            md = batch_data.get(code)
            if md and md.current_price and md.current_price > 0:
                update_list.append((md.current_price, account_id, code))

        if update_list:
            await db.executemany(
                "UPDATE watchlist SET current_price = ?, updated_at = ? WHERE account_id = ? AND stock_code = ?",
                [(price, get_china_time(), aid, code) for price, aid, code in update_list],
            )
            print(f"[Monitor] 已更新 {len(update_list)} 只 watchlist 现价")

    async def _evaluate_sell_strategy_code(
        self,
        account_id: str,
        stock_code: str,
        sell_strategy_id: int,
        current_price: float,
        stock: Dict,
    ) -> Dict:
        """执行关联的卖出代码策略，返回卖出决策结果

        Returns:
            {"should_sell": bool, "reason": str, "stop_loss_price": float, "take_profit_price": float}
        """
        db = get_db_manager()

        # 获取策略代码
        strategy = await db.fetchone(
            "SELECT * FROM strategies WHERE id = ? AND account_id = ? AND strategy_type = 'python'",
            (sell_strategy_id, account_id),
        )
        if not strategy:
            print(f"[Monitor] 卖出策略 #{sell_strategy_id} 不存在")
            return {"should_sell": False, "reason": "sell_strategy_not_found"}

        # 构建执行上下文
        from services.data.local_data_service import get_local_data_service
        from services.common import technical_indicators

        lds = get_local_data_service()

        def _get_kline_local(sc, limit=60, start_date=None):
            return lds.get_kline_data(sc, start_date=start_date, limit=limit)

        def _get_kline_local_single(sc, limit=60):
            result = _get_kline_local(sc, limit=limit)
            if hasattr(result, 'to_dict'):
                return result.to_dict('records')
            return result if result else []

        def _get_realtime_quote_sync(sc):
            return stock

        kline_data = _get_kline_local_single(stock_code, limit=60)

        context = {
            "account_id": account_id,
            "stock_code": stock_code,
            "stock_name": stock.get("stock_name", stock_code),
            "current_price": current_price,
            "buy_price": stock.get("buy_price", 0),
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

        # 执行策略
        from services.strategy.engine import get_strategy_engine
        engine = get_strategy_engine()
        try:
            signals = engine.execute_strategy(strategy, context)
        except Exception as e:
            print(f"[Monitor] 卖出策略 {sell_strategy_id} 执行失败 {stock_code}: {e}")
            return {"should_sell": False, "reason": f"strategy_execution_error: {e}"}

        # 解析信号：查找卖出信号
        for signal in signals:
            if signal.get("action") == "sell":
                return {
                    "should_sell": True,
                    "reason": signal.get("reason", "sell_strategy_code"),
                    "stop_loss_price": signal.get("stop_loss_price", 0),
                    "take_profit_price": signal.get("take_profit_price", 0),
                }

        return {"should_sell": False, "reason": "no_sell_signal"}

    async def _evaluate_sell_decision(
        self,
        account_id: str,
        stock_code: str,
        stock: Dict,
        current_price: float,
        screening_strategy_id: Optional[int] = None,
    ) -> Dict:
        """评估是否应该卖出

        决策流程：
        1. 如果有个股策略(trading_strategies) → 执行策略逻辑（动态止盈止损）
        2. 如果选股策略关联了卖出代码策略 → 执行代码策略
        3. 无上述配置 → 直接使用 watchlist 中的止盈止损价格
           - 这些价格在选股完成/买入执行时已填入

        Returns:
            {
                "should_sell": bool,
                "reason": str,       # 'stop_loss' / 'take_profit' / 'trailing_stop' / ''
                "stop_loss_price": float,
                "take_profit_price": float,
            }
        """
        db = get_db_manager()
        result = {"should_sell": False, "reason": "", "stop_loss_price": 0, "take_profit_price": 0}

        # === 优先级 1：检查个股动态策略 ===
        ts = await db.fetchone(
            "SELECT * FROM trading_strategies WHERE account_id = ? AND stock_code = ?",
            (account_id, stock_code),
        )

        if ts:
            strategy_type = ts.get("strategy_type", "fixed")
            avg_cost = stock.get("buy_price", 0) or stock.get("current_price", 0)

            # --- 动态策略：回撤止盈（trailing_stop）---
            if strategy_type == "trailing_stop":
                # 获取持仓最高价
                position = await db.fetchone(
                    "SELECT highest_price FROM stock_positions WHERE account_id = ? AND stock_code = ?",
                    (account_id, stock_code),
                )
                highest = position.get("highest_price", 0) if position else 0

                # 更新最高价
                if current_price > highest:
                    highest = current_price
                    await db.execute(
                        "UPDATE stock_positions SET highest_price = ? WHERE account_id = ? AND stock_code = ?",
                        (highest, account_id, stock_code),
                    )

                take_profit_pct = ts.get("take_profit_pct", 0.05)  # 默认回落 5%
                stop_loss_pct = ts.get("stop_loss_pct", 0.05)  # 硬止损

                if highest > 0 and take_profit_pct > 0:
                    trail_stop = round(highest * (1 - take_profit_pct), 2)
                    result["take_profit_price"] = trail_stop
                    if current_price <= trail_stop:
                        result["should_sell"] = True
                        result["reason"] = "trailing_stop"
                        return result

                # 硬止损：基于成本价
                if avg_cost > 0 and stop_loss_pct > 0:
                    hard_stop = round(avg_cost * (1 - stop_loss_pct), 2)
                    result["stop_loss_price"] = hard_stop
                    if current_price <= hard_stop:
                        result["should_sell"] = True
                        result["reason"] = "stop_loss"
                        return result

                return result

            # --- 固定策略（fixed）：使用绝对价格或百分比 ---
            if strategy_type == "fixed":
                # 绝对价格优先
                sl = ts.get("stop_loss_price", 0) or 0
                tp = ts.get("take_profit_price", 0) or 0
                if sl > 0:
                    result["stop_loss_price"] = float(sl)
                if tp > 0:
                    result["take_profit_price"] = float(tp)

                # 百分比次之
                if sl <= 0 and ts.get("stop_loss_pct", 0) > 0 and avg_cost > 0:
                    result["stop_loss_price"] = round(avg_cost * (1 - ts["stop_loss_pct"]), 2)
                if tp <= 0 and ts.get("take_profit_pct", 0) > 0 and avg_cost > 0:
                    result["take_profit_price"] = round(avg_cost * (1 + ts["take_profit_pct"]), 2)

                if result["stop_loss_price"] > 0 and current_price <= result["stop_loss_price"]:
                    result["should_sell"] = True
                    result["reason"] = "stop_loss"
                elif result["take_profit_price"] > 0 and current_price >= result["take_profit_price"]:
                    result["should_sell"] = True
                    result["reason"] = "take_profit"

                return result

            # 未知策略类型，回退到 watchlist
            print(f"[Monitor] {stock_code} 未知策略类型: {strategy_type}，回退到 watchlist")

        # === 优先级 2：关联的卖出代码策略 ===
        if screening_strategy_id:
            sell_strategy_id = await db.fetchval(
                "SELECT sell_strategy_id FROM strategies WHERE id = ? AND account_id = ?",
                (screening_strategy_id, account_id),
            )
            if sell_strategy_id:
                code_result = await self._evaluate_sell_strategy_code(
                    account_id, stock_code, sell_strategy_id, current_price, stock,
                )
                if code_result["should_sell"]:
                    return code_result
                # 代码策略执行但未触发卖出，继续到优先级 3

        # === 优先级 3：watchlist 止盈止损值（选股完成/买入时已填入）===
        sl = stock.get("stop_loss_price", 0) or 0
        tp = stock.get("take_profit_price", 0) or 0
        result["stop_loss_price"] = float(sl)
        result["take_profit_price"] = float(tp)

        if sl > 0 and current_price <= sl:
            result["should_sell"] = True
            result["reason"] = "stop_loss"
        elif tp > 0 and current_price >= tp:
            result["should_sell"] = True
            result["reason"] = "take_profit"

        return result

    async def _check_stock_signals(self, account_id: str, stock: Dict):
        """
        检查单只股票的交易信号
        使用真实行情数据
        """
        db = get_db_manager()
        stock_code = stock.get('stock_code')
        buy_price = stock.get('buy_price', 0)
        target_quantity = stock.get('target_quantity', 100)
        status = stock.get('status')

        # 获取交易网关
        gateway = None
        current_price = None

        try:
            gateway = await get_gateway()
            market_data: Optional[MarketData] = await gateway.get_market_data(stock_code)

            if market_data:
                current_price = market_data.current_price
                print(f"[Monitor] {stock_code} 实时价格：{current_price:.2f}")
            else:
                print(f"[Monitor] {stock_code} 无法获取行情数据")
                return
        except Exception as e:
            print(f"[Monitor] 获取 {stock_code} 行情数据失败：{e}")
            return

        if current_price is None:
            return

        # 检查买入条件
        if status == 'pending':
            if buy_price > 0 and abs(current_price - buy_price) / buy_price <= 0.02:
                print(f"[Monitor] 触发买入信号：{stock_code}, 目标价：{buy_price:.2f}, 当前价：{current_price:.2f}")
                await self._create_pending_signal(account_id, stock, 'buy', current_price, target_quantity)
                await self._execute_buy_signal(account_id, stock, current_price, target_quantity)

        # 检查止损/止盈条件（动态策略引擎）
        elif status == 'watching':
            # 先检查持仓，无持仓直接跳过
            position = await db.fetchone(
                "SELECT 1 FROM stock_positions WHERE account_id = ? AND stock_code = ?",
                (account_id, stock_code),
            )
            if not position:
                return
            decision = await self._evaluate_sell_decision(
                account_id, stock_code, stock, current_price,
                screening_strategy_id=stock.get("strategy_id"),
            )
            if decision["should_sell"]:
                reason = decision["reason"]
                signal_type = "sell_stop_loss" if reason == "stop_loss" else (
                    "sell_take_profit" if reason == "take_profit" else f"sell_{reason}"
                )
                print(f"[Monitor] 触发卖出：{stock_code}, 原因: {reason}, "
                      f"止损价={decision['stop_loss_price']:.2f}, "
                      f"止盈价={decision['take_profit_price']:.2f}, "
                      f"当前价={current_price:.2f}")
                await self._create_pending_signal(account_id, stock, signal_type, current_price, 0)
                await self._execute_sell_signal(
                    account_id, stock, current_price,
                    signal_type, target_quantity
                )

    async def _check_stock_signals_with_price(self, account_id: str, stock: Dict):
        """
        检查单只股票的交易信号（行情已预取版本）
        stock 对象中需包含 current_price 字段
        """
        stock_code = stock.get('stock_code')
        buy_price = stock.get('buy_price', 0)
        target_quantity = stock.get('target_quantity', 100)
        status = stock.get('status')
        current_price = stock.get('current_price')

        db = get_db_manager()

        if current_price is None:
            return

        # 检查买入条件
        if status == 'pending':
            if buy_price > 0 and abs(current_price - buy_price) / buy_price <= 0.02:
                print(f"[Monitor] 触发买入信号：{stock_code}, 目标价：{buy_price:.2f}, 当前价：{current_price:.2f}")
                await self._create_pending_signal(account_id, stock, 'buy', current_price, target_quantity)
                await self._execute_buy_signal(account_id, stock, current_price, target_quantity)

        # 检查止损/止盈条件（动态策略引擎）
        elif status in ('watching', 'bought'):
            # 无持仓直接跳过，不检查卖出条件
            position = await db.fetchone(
                "SELECT quantity FROM stock_positions WHERE account_id = ? AND stock_code = ?",
                (account_id, stock_code),
            )
            if not position or position.get("quantity", 0) == 0:
                return
            decision = await self._evaluate_sell_decision(
                account_id, stock_code, stock, current_price,
                screening_strategy_id=stock.get("strategy_id"),
            )
            if decision["should_sell"]:
                reason = decision["reason"]
                signal_type = "sell_stop_loss" if reason == "stop_loss" else (
                    "sell_take_profit" if reason == "take_profit" else f"sell_{reason}"
                )
                print(f"[Monitor] 触发卖出：{stock_code}, 原因: {reason}, "
                      f"止损价={decision['stop_loss_price']:.2f}, "
                      f"止盈价={decision['take_profit_price']:.2f}, "
                      f"当前价={current_price:.2f}")
                await self._create_pending_signal(account_id, stock, signal_type, current_price, 0)
                await self._execute_sell_signal(
                    account_id, stock, current_price,
                    signal_type, target_quantity
                )

    async def _execute_buy_signal(
        self,
        account_id: str,
        stock: Dict,
        current_price: float,
        target_quantity: int,
        trigger_source: Optional[str] = None,
    ):
        """执行买入交易

        买入数量解析：
        1. 个股策略 max_trade_quantity > 0 → 按该数量
        2. 否则使用 watchlist target_quantity
        实际买入由 execution_service 根据账户风控参数计算上限
        """
        stock_code = stock.get('stock_code')
        stock_name = stock.get('stock_name', '')

        # 检查个股策略是否有最大买入数量限制
        db = get_db_manager()
        ts = await db.fetchone(
            "SELECT max_trade_quantity FROM trading_strategies WHERE account_id = ? AND stock_code = ?",
            (account_id, stock_code),
        )
        if ts and ts.get("max_trade_quantity", 0) > 0:
            target_quantity = ts["max_trade_quantity"]

        # 获取交易执行服务
        execution = get_trade_execution_service(account_id)

        # 执行买入（自动计算可用资金和手续费）
        result = await execution.execute_buy(
            stock_code=stock_code,
            stock_name=stock_name,
            price=current_price,
            target_quantity=target_quantity,
            trigger_source=trigger_source,
        )

        if result["success"]:
            print(f"[Monitor] 买入成功：{stock_code}")
            print(f"  数量：{result['quantity']} 股")
            print(f"  价格：{result['price']:.2f} 元")
            print(f"  总金额：{result['total_amount']:.2f} 元")
            print(f"  手续费：{result['fees']['total_fee']:.2f} 元")

            # 更新 watchlist 状态为已买入
            await self._update_watchlist_status(
                account_id, stock_code, 'bought'
            )

            # 信号 pending → executed
            await self._update_signal_status(
                account_id, stock_code, 'executed', result['quantity']
            )

            # 发送通知
            notification = get_notification_service()
            await notification.emit(
                event_type="trade_executed",
                account_id=account_id,
                payload={
                    "trade_type": "buy",
                    "stock_code": stock_code,
                    "stock_name": stock_name,
                    "price": f"{current_price:.2f}",
                    "quantity": result['quantity'],
                    "amount": f"{result['total_amount']:.2f}",
                    "fees": f"{result['fees']['total_fee']:.2f}",
                    "trigger_source": trigger_source or "监控",
                },
            )
        else:
            print(f"[Monitor] 买入失败：{result['message']}")
            # 信号 pending → cancelled
            await self._update_signal_status(
                account_id, stock_code, 'cancelled', target_quantity
            )

            # 发送失败通知
            notification = get_notification_service()
            await notification.emit(
                event_type="trade_failed",
                account_id=account_id,
                payload={
                    "stock_code": stock_code,
                    "stock_name": stock_name,
                    "reason": result['message'],
                },
            )

    async def _execute_sell_signal(
        self,
        account_id: str,
        stock: Dict,
        current_price: float,
        signal_type: str,
        target_quantity: int,
        trigger_source: Optional[str] = None,
    ):
        """执行卖出交易"""
        stock_code = stock.get('stock_code')
        stock_name = stock.get('stock_name', '')

        # 获取交易执行服务
        execution = get_trade_execution_service(account_id)

        # 执行卖出（自动计算持仓数量和手续费）
        result = await execution.execute_sell(
            stock_code=stock_code,
            stock_name=stock_name,
            price=current_price,
            target_quantity=target_quantity,
            trigger_source=trigger_source,
        )

        if result["success"]:
            print(f"[Monitor] 卖出成功：{stock_code}")
            print(f"  数量：{result['quantity']} 股")
            print(f"  价格：{result['price']:.2f} 元")
            print(f"  净得：{result['net_amount']:.2f} 元")
            print(f"  手续费：{result['fees']['total_fee']:.2f} 元")
            print(f"  盈亏：{result['profit_loss']:.2f} 元")

            # 如果清空持仓，更新 watchlist 状态
            await self._update_watchlist_status(
                account_id, stock_code, 'sold'
            )

            # 信号 pending → executed
            await self._update_signal_status(
                account_id, stock_code, 'executed', result['quantity']
            )

            # 发送通知
            notification = get_notification_service()
            await notification.emit(
                event_type="trade_executed",
                account_id=account_id,
                payload={
                    "trade_type": "sell",
                    "stock_code": stock_code,
                    "stock_name": stock_name,
                    "price": f"{current_price:.2f}",
                    "quantity": result['quantity'],
                    "amount": f"{result['net_amount']:.2f}",
                    "fees": f"{result['fees']['total_fee']:.2f}",
                    "profit_loss": f"{result['profit_loss']:.2f}",
                    "trigger_source": trigger_source or signal_type,
                },
            )
        else:
            print(f"[Monitor] 卖出失败：{result['message']}")
            # 信号 pending → cancelled
            await self._update_signal_status(
                account_id, stock_code, 'cancelled', 0
            )

    async def _create_pending_signal(
        self,
        account_id: str,
        stock: Dict,
        signal_type: str,
        price: float,
        target_quantity: int,
    ):
        """创建 pending 状态的交易信号"""
        db = get_db_manager()
        now = get_china_time()

        signal_data = {
            "account_id": account_id,
            "strategy_id": stock.get('strategy_id'),
            "stock_code": stock.get('stock_code'),
            "stock_name": stock.get('stock_name'),
            "signal_type": signal_type,
            "price": price,
            "target_quantity": target_quantity,
            "status": "pending",
            "created_at": now.isoformat(),
            "executed_at": None,
        }

        signal_id = await db.insert("trading_signals", signal_data)
        print(f"[Monitor] 创建交易信号(pending)：{stock.get('stock_code')} - {signal_type}")
        return signal_id

    async def _update_signal_status(
        self,
        account_id: str,
        stock_code: str,
        status: str,
        quantity: int,
    ):
        """更新信号状态：pending → executed / cancelled"""
        db = get_db_manager()
        now = get_china_time().isoformat()

        if status == "executed":
            await db.execute(
                "UPDATE trading_signals SET status = ?, target_quantity = ?, executed_at = ? "
                "WHERE account_id = ? AND stock_code = ? AND status = 'pending' "
                "ORDER BY created_at DESC LIMIT 1",
                (status, quantity, now, account_id, stock_code),
            )
        else:
            await db.execute(
                "UPDATE trading_signals SET status = ?, executed_at = ? "
                "WHERE account_id = ? AND stock_code = ? AND status = 'pending' "
                "ORDER BY created_at DESC LIMIT 1",
                (status, now, account_id, stock_code),
            )

    async def _update_watchlist_status(
        self,
        account_id: str,
        stock_code: str,
        status: str
    ):
        """更新 watchlist 状态"""
        db = get_db_manager()
        await db.update(
            "watchlist",
            {"status": status, "updated_at": get_china_time()},
            "account_id = ? AND stock_code = ?",
            (account_id, stock_code)
        )

    def get_status(self) -> Dict:
        """获取服务状态"""
        return {
            "running": self._running,
            "account_id": self._account_id,
            "task": "active" if self._task else None
        }


# 全局单例
_trading_monitor: Optional[TradingMonitor] = None


def get_trading_monitor() -> TradingMonitor:
    """获取交易监控服务单例"""
    global _trading_monitor
    if _trading_monitor is None:
        _trading_monitor = TradingMonitor()
    return _trading_monitor


def reset_trading_monitor():
    """重置交易监控服务（用于测试）"""
    global _trading_monitor
    _trading_monitor = None
