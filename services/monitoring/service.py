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
import time
import threading
from typing import Dict, List, Optional, Any
from services.common.database import get_db_manager
from services.common.timezone import get_china_time
from services.trading.gateway import get_gateway, MarketData
from services.trading.execution_service import get_trade_execution_service
from services.trading.strategy_executor import get_strategy_executor
from services.notifications import get_notification_service
from services.common.structured_logger import get_logger


class TradingMonitor:
    """交易监控服务"""

    def __init__(self):
        self._running = False
        self._task = None
        self._account_ids: list[str] = []
        self._state_lock = threading.Lock()  # 保护 _running 和 _account_ids 的并发访问
        # 冷却期记录：{(account_id, strategy_id): last_trigger_time}
        self._cooldown: Dict[tuple, float] = {}
        # 策略代码编译缓存：{strategy_id: compiled_code}
        self._strategy_cache: Dict[int, Any] = {}
        # SDK/TGW 连接健康状态
        self._sdk_healthy = True  # SDK 连接是否正常
        self._sdk_error_time: Optional[str] = None  # 最近一次 SDK 错误时间
        self._sdk_error_msg: str = ""  # 最近一次 SDK 错误信息
        self._consecutive_errors = 0  # 连续错误计数

    async def start_monitoring(
        self,
        account_ids: Optional[list[str]] = None,
        interval: int = 30
    ):
        """
        启动交易监控服务

        Args:
            account_ids: 账户 ID 列表，不传则自动查询所有活跃账户
            interval: 监控间隔（秒）
        """
        if self._running:
            # 僵尸状态检测：_running=True 但 task 已死
            if self._task and self._task.done():
                log = get_logger("monitor")
                log.log_event("monitor_zombie_detect", f"检测到监控僵尸状态（task 已死），自动清理")
                self._running = False
                self._task = None
            else:
                return {"success": False, "message": "交易监控服务已在运行"}

        if not account_ids:
            from services.common.database import get_db_manager
            db = get_db_manager()
            accounts = await db.fetchall(
                "SELECT account_id FROM accounts WHERE is_active = 1"
            )
            account_ids = [a["account_id"] for a in accounts]

        if not account_ids:
            return {"success": False, "message": "没有活跃账户可监控"}

        self._running = True
        self._account_ids = account_ids
        self._task = asyncio.create_task(
            self._run_monitoring_loop(account_ids, interval)
        )
        log = get_logger("monitor")
        log.log_event("monitor_start", f"交易监控服务已启动，账户：{', '.join(account_ids)}",
                      account_ids=account_ids, interval=interval)
        return {"success": True, "message": f"交易监控服务已启动，账户：{', '.join(account_ids)}"}

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
        account_ids: list[str],
        interval: int
    ):
        """交易监控循环 — 遍历所有账户"""
        from services.trading.trading_hours import can_trade, get_next_trading_window

        log = get_logger("monitor")
        log.log_event("monitor_loop_start", "启动交易监控服务",
                      account_ids=account_ids, interval=interval)

        while self._running:
            try:
                if can_trade():
                    for acct_id in account_ids:
                        if not self._running:
                            break
                        await self._run_monitoring(acct_id)
                    # 交易中按 interval 轮询
                    await asyncio.sleep(interval)
                else:
                    # 非交易时段：计算下一个交易窗口并休眠
                    next_time, reason = get_next_trading_window()
                    if next_time is None:
                        log.log_event("monitor_no_trading", reason)
                        await asyncio.sleep(60)  # 无交易日，每小时检查一次
                    else:
                        wait_seconds = (next_time - get_china_time()).total_seconds()
                        wait_seconds = max(wait_seconds, 0)
                        log.log_event("monitor_sleep_until", reason,
                                      next_time=next_time.isoformat(),
                                      wait_seconds=round(wait_seconds))
                        # 最多等 5 分钟后检查一次是否被手动停止
                        max_wait = min(wait_seconds, 300)
                        await asyncio.sleep(max_wait)
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error("monitor_loop", f"监控循环错误: {e}")
                await asyncio.sleep(interval)

        self._running = False  # 循环退出后重置状态，防止僵尸状态
        log.log_event("monitor_loop_stop", "交易监控服务已停止")

    async def _run_monitoring(self, account_id: str):
        """执行一次交易监控"""
        db = get_db_manager()

        # === 预取：收集所有需要监控的股票代码，一次性批量获取行情 ===
        all_stock_codes = set()
        try:
            wl = await db.fetchall(
                "SELECT stock_code FROM watchlist WHERE account_id = ? AND status IN ('pending', 'watching', 'bought')",
                (account_id,),
            )
            all_stock_codes.update(w["stock_code"] for w in wl)
        except Exception:
            pass
        try:
            pos = await db.fetchall(
                "SELECT stock_code FROM stock_positions WHERE account_id = ?",
                (account_id,),
            )
            all_stock_codes.update(p["stock_code"] for p in pos)
        except Exception:
            pass

        market_data_cache: Dict[str, Any] = {}
        if all_stock_codes:
            try:
                from services.trading.gateway import get_gateway
                gateway = await get_gateway()
                market_data_cache = await gateway.get_batch_market_data(list(all_stock_codes))
                if not self._sdk_healthy:
                    get_logger("monitor").log_event("sdk_recovered", "SDK/TGW 连接已恢复")
                    self._sdk_healthy = True
                    self._sdk_error_time = None
                    self._sdk_error_msg = ""
                    self._consecutive_errors = 0
            except Exception as e:
                self._sdk_healthy = False
                self._sdk_error_time = get_china_time().strftime("%Y-%m-%d %H:%M:%S")
                self._sdk_error_msg = str(e)
                self._consecutive_errors += 1

        # === 第一部分：基于交易策略的触发评估 ===
        await self._evaluate_trading_strategies(account_id)

        # === 第二部分：基于 watchlist 的传统监控（止损止盈）===
        await self._monitor_watchlist(account_id, market_data_cache=market_data_cache)

        # === 第三部分：扫描手动 pending 信号并执行 ===
        await self._scan_pending_signals(account_id, market_data_cache=market_data_cache)

        # === 第四部分：刷新持仓盈亏 ===
        await self._refresh_positions_pnl(account_id, market_data_cache=market_data_cache)

        # === 第五部分：更新实时价格到内存缓存（不再写 DB）===
        self._update_price_cache(account_id, market_data_cache=market_data_cache)

        # === 第六部分：每 15 分钟兜底刷盘 ===
        if self._cache.should_flush():
            await self._flush_price_cache_to_db(account_id)

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
            cooldown_seconds = await self._get_cooldown_for_strategy(strategy_id)

            # 检查冷却期（按账户+策略组合区分）
            cooldown_key = (account_id, strategy_id)
            last_trigger = self._cooldown.get(cooldown_key, 0)
            if time.time() - last_trigger < cooldown_seconds:
                continue  # 冷却期内，跳过

            # 记录触发时间
            self._cooldown[cooldown_key] = time.time()

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

    async def _get_cooldown_for_strategy(self, strategy_id: int) -> int:
        """获取策略的冷却时间"""
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
        return 300  # 默认 5 分钟

    async def _monitor_watchlist(self, account_id: str, market_data_cache: Optional[Dict[str, Any]] = None):
        """传统的 watchlist 止损止盈监控（批量行情 + 策略预加载）"""
        db = get_db_manager()

        # 获取 watchlist 中的股票
        watchlist = await db.fetchall("""
            SELECT * FROM watchlist
            WHERE account_id = ? AND status IN ('pending', 'watching', 'bought')
        """, (account_id,))

        if not watchlist:
            return

        stock_codes = [w["stock_code"] for w in watchlist]

        # 批量获取行情数据（优先用预取缓存，否则自己调用）
        if market_data_cache:
            batch_data = market_data_cache
        else:
            try:
                from services.trading.gateway import get_gateway
                gateway = await get_gateway()
                batch_data = await gateway.get_batch_market_data(stock_codes)
            except Exception as e:
                log = get_logger("monitor")
                log.error("monitor", f"批量获取行情失败: {e}")
                for stock in watchlist:
                    await self._check_stock_signals(account_id, stock)
                return

        # === 优化 1：一次 SQL JOIN 预加载所有股票的策略配置 ===
        # P1: trading_strategies（个股策略）
        ts_rows = await db.fetchall(
            "SELECT * FROM trading_strategies WHERE account_id = ?",
            (account_id,),
        )
        ts_map: Dict[str, Dict] = {}
        for ts in ts_rows:
            code = ts.get("stock_code")
            if code:
                ts_map[code] = ts

        # P2: 选股策略关联的卖出代码策略
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

        # 预加载 P2 策略代码到编译缓存
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

        # === 优化 2：批量预加载 K 线缓存 ===
        kline_cache: Dict[str, Any] = {}
        try:
            from services.data.local_data_service import get_local_data_service
            lds = get_local_data_service()
            kline_cache = lds.get_batch_kline(stock_codes, limit=60)
        except Exception:
            pass

        # 逐个检查止损止盈
        for stock in watchlist:
            stock_code = stock.get("stock_code")
            market_data = batch_data.get(stock_code)
            if not market_data:
                continue

            # 注入行情数据到 stock 对象
            if market_data.current_price <= 0:
                # 价格无效（竞价阶段/数据缺失），跳过不检查
                continue
            stock_with_price = {**stock, "current_price": market_data.current_price}
            await self._check_stock_signals_with_price(
                account_id, stock_with_price,
                ts_map=ts_map,
                sell_strategy_map=strategy_id_map,
                kline_cache=kline_cache,
            )

    async def _refresh_positions_pnl(self, account_id: str, market_data_cache: Optional[Dict[str, Any]] = None):
        """刷新持仓盈亏（只更新内存缓存，不写 DB）"""
        from services.common.price_cache import get_price_cache
        self._cache = get_price_cache()

        db = get_db_manager()
        positions = await db.fetchall(
            "SELECT stock_code FROM stock_positions WHERE account_id = ?",
            (account_id,),
        )
        if not positions:
            return

        stock_codes = [p["stock_code"] for p in positions]

        # 使用预取的行情数据
        if not market_data_cache:
            try:
                gateway = await get_gateway()
                market_data_cache = await gateway.get_batch_market_data(stock_codes)
                if not self._sdk_healthy:
                    get_logger("monitor").log_event("sdk_recovered", "SDK/TGW 连接已恢复")
                    self._sdk_healthy = True
                    self._sdk_error_time = None
                    self._sdk_error_msg = ""
                    self._consecutive_errors = 0
            except Exception as e:
                self._sdk_healthy = False
                self._sdk_error_time = get_china_time().strftime("%Y-%m-%d %H:%M:%S")
                self._sdk_error_msg = str(e)
                self._consecutive_errors += 1
                return

        # 更新内存价格缓存
        data = {}
        for code in stock_codes:
            md = market_data_cache.get(code)
            if md and md.current_price and md.current_price > 0:
                data[code] = (md.current_price, md.change_percent if hasattr(md, 'change_percent') and md.change_percent else 0.0)

        if data:
            self._cache.update_batch(account_id, data)

    def _update_price_cache(self, account_id: str, market_data_cache: Optional[Dict[str, Any]] = None):
        """将实时行情写入内存价格缓存（不写 DB）"""
        from services.common.price_cache import get_price_cache
        self._cache = get_price_cache()

        if not market_data_cache:
            return

        # 从预取行情中提取价格（只缓存有效价格）
        data = {}
        for code, md in market_data_cache.items():
            if md and md.current_price and md.current_price > 0:
                data[code] = (md.current_price, md.change_percent if hasattr(md, 'change_percent') and md.change_percent else 0.0)

        if data:
            self._cache.update_batch(account_id, data)

    async def _flush_price_cache_to_db(self, account_id: str):
        """每 15 分钟将内存缓存的价格兜底写入数据库"""
        from services.common.price_cache import get_price_cache
        self._cache = get_price_cache()
        prices = self._cache.get_all_for_account(account_id)
        if not prices:
            return

        db = get_db_manager()

        # 刷写 watchlist current_price
        wl_updates = []
        pos_updates = []
        for code, price in prices.items():
            wl_updates.append((price, get_china_time(), account_id, code))
            pos_updates.append((price, price, price, get_china_time(), account_id, code))

        if wl_updates:
            try:
                await db.executemany(
                    "UPDATE watchlist SET current_price = ?, updated_at = ? WHERE account_id = ? AND stock_code = ?",
                    wl_updates,
                )
                get_logger("monitor").log_event("price_flush", f"已刷盘 {len(wl_updates)} 条 watchlist 现价", count=len(wl_updates))
            except Exception as e:
                get_logger("monitor").error("monitor", f"刷盘 watchlist 现价失败: {e}")

        if pos_updates:
            try:
                await db.executemany(
                    """UPDATE stock_positions
                       SET current_price = ?,
                           market_value = ? * quantity,
                           profit_loss = (? - avg_cost) * quantity,
                           updated_at = ?
                       WHERE account_id = ? AND stock_code = ?""",
                    pos_updates,
                )
                get_logger("monitor").log_event("price_flush", f"已刷盘 {len(pos_updates)} 条 position 盈亏", count=len(pos_updates))
            except Exception as e:
                get_logger("monitor").error("monitor", f"刷盘 position 盈亏失败: {e}")

        self._cache.mark_flushed()

    async def _evaluate_sell_strategy_code(
        self,
        account_id: str,
        stock_code: str,
        sell_strategy_id: int,
        current_price: float,
        stock: Dict,
        kline_cache: Optional[Dict[str, Any]] = None,
    ) -> Dict:
        """执行关联的卖出代码策略，返回卖出决策结果

        Args:
            kline_cache: K 线缓存字典，{stock_code: DataFrame}

        Returns:
            {"should_sell": bool, "reason": str, "stop_loss_price": float, "take_profit_price": float}
        """
        db = get_db_manager()

        # 获取策略配置（优先用编译缓存，其次 DB 查询）
        strategy_code = None
        if sell_strategy_id in self._strategy_cache:
            # 已缓存，但仍需从 DB 获取代码和元数据
            strategy = await db.fetchone(
                "SELECT id, name, code FROM strategies WHERE id = ? AND account_id = ? AND strategy_type = 'python'",
                (sell_strategy_id, account_id),
            )
        else:
            # 未缓存，完整查询
            strategy = await db.fetchone(
                "SELECT * FROM strategies WHERE id = ? AND account_id = ? AND strategy_type = 'python'",
                (sell_strategy_id, account_id),
            )
            if strategy:
                strategy_code = strategy.get("code", "")

        if not strategy:
            get_logger("monitor").error("monitor", f"卖出策略 #{sell_strategy_id} 不存在",
                                         strategy_id=sell_strategy_id)
            return {"should_sell": False, "reason": "sell_strategy_not_found"}

        from services.common import technical_indicators

        # === 优化 2：使用 K 线缓存 ===
        kline_df = None
        if kline_cache and stock_code in kline_cache:
            kline_df = kline_cache[stock_code]
        else:
            # 缓存未命中，实时查询
            from services.data.local_data_service import get_local_data_service
            lds = get_local_data_service()
            kline_df = lds.get_kline_data(stock_code, limit=60)
            if kline_df is not None:
                if kline_cache is not None:
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
            get_logger("monitor").error("monitor", f"卖出策略 {sell_strategy_id} 执行失败 {stock_code}: {e}",
                                         strategy_id=sell_strategy_id, stock_code=stock_code)
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
        ts_config: Optional[Dict] = None,
        sell_strategy_id: Optional[int] = None,
        kline_cache: Optional[Dict[str, Any]] = None,
    ) -> Dict:
        """评估是否应该卖出

        决策流程：
        1. 如果有个股策略(trading_strategies) → 执行策略逻辑（动态止盈止损）
        2. 如果选股策略关联了卖出代码策略 → 执行代码策略
        3. 无上述配置 → 直接使用 watchlist 中的止盈止损价格
           - 这些价格在选股完成/买入执行时已填入

        Args:
            ts_config: 预加载的 trading_strategies 配置（避免循环内查询）
            sell_strategy_id: 预加载的卖出策略 ID（避免循环内查询）
            kline_cache: K 线缓存（避免重复查询）

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
        # 如果传入 ts_config 则使用，否则回退到 DB 查询
        ts = ts_config
        if ts is None:
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
            get_logger("monitor").error("monitor", f"{stock_code} 未知策略类型: {strategy_type}，回退到 watchlist",
                                         stock_code=stock_code, strategy_type=strategy_type)

        # === 优先级 2：关联的卖出代码策略 ===
        if screening_strategy_id:
            # 如果传入 sell_strategy_id 则使用，否则回退到 DB 查询
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
                # 代码策略执行但未触发卖出，继续到优先级 3

        # === 优先级 3：watchlist 止盈止损值（选股完成/买入时已填入）===
        sl = stock.get("stop_loss_price", 0) or 0
        tp = stock.get("take_profit_price", 0) or 0
        result["stop_loss_price"] = float(sl)
        result["take_profit_price"] = float(tp)

        # 价格无效时不触发止盈止损（防止竞价阶段 price=0 误触发）
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

    async def _check_stock_signals(self, account_id: str, stock: Dict):
        """
        检查单只股票的交易信号
        使用真实行情数据
        """
        db = get_db_manager()
        stock_code = stock.get('stock_code')
        buy_price = stock.get('buy_price', 0)
        target_quantity = stock.get('target_quantity') or 0
        status = stock.get('status')

        # 获取交易网关
        gateway = None
        current_price = None

        try:
            gateway = await get_gateway()
            market_data: Optional[MarketData] = await gateway.get_market_data(stock_code)

            if market_data:
                current_price = market_data.current_price
            else:
                get_logger("monitor").warn("monitor", f"{stock_code} 无法获取行情数据", stock_code=stock_code)
                return
        except Exception as e:
            get_logger("monitor").error("monitor", f"获取 {stock_code} 行情数据失败: {e}", stock_code=stock_code)
            return

        if current_price is None:
            return

        # 检查买入条件
        if status == 'pending':
            if buy_price > 0 and current_price <= buy_price:
                get_logger("monitor").log_event("buy_signal",
                    f"触发买入信号：{stock_code}, 目标价：{buy_price:.2f}, 当前价：{current_price:.2f}",
                    stock_code=stock_code, buy_price=buy_price, current_price=current_price)
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
                get_logger("monitor").log_event("sell_signal",
                    f"触发卖出：{stock_code}, 原因: {reason}",
                    stock_code=stock_code, reason=reason,
                    stop_loss_price=decision.get('stop_loss_price'),
                    take_profit_price=decision.get('take_profit_price'),
                    current_price=current_price)
                await self._create_pending_signal(account_id, stock, signal_type, current_price, 0)
                await self._execute_sell_signal(
                    account_id, stock, current_price,
                    signal_type, 0  # 0 = 全部卖出
                )

    async def _check_stock_signals_with_price(
        self, account_id: str, stock: Dict,
        ts_map: Optional[Dict[str, Dict]] = None,
        sell_strategy_map: Optional[Dict[str, Optional[int]]] = None,
        kline_cache: Optional[Dict[str, Any]] = None,
    ):
        """
        检查单只股票的交易信号（行情已预取版本）
        stock 对象中需包含 current_price 字段

        Args:
            ts_map: 预加载的 trading_strategies 配置
            sell_strategy_map: 预加载的卖出策略 ID 映射
            kline_cache: K 线缓存
        """
        stock_code = stock.get('stock_code')
        buy_price = stock.get('buy_price', 0)
        target_quantity = stock.get('target_quantity') or 0
        status = stock.get('status')
        current_price = stock.get('current_price')

        db = get_db_manager()

        if current_price is None:
            return

        # 价格无效时跳过（防止竞价阶段 price=0 触发止损/止盈）
        if current_price <= 0:
            get_logger("monitor").warn("monitor", f"跳过价格为0的股票 {stock_code}，不触发止盈止损",
                                        stock_code=stock_code, current_price=current_price)
            return

        # 检查买入条件
        if status == 'pending':
            if buy_price > 0 and current_price <= buy_price:
                get_logger("monitor").log_event("buy_signal",
                    f"触发买入信号：{stock_code}, 目标价：{buy_price:.2f}, 当前价：{current_price:.2f}",
                    stock_code=stock_code, buy_price=buy_price, current_price=current_price)
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

            # 使用预加载的策略配置（如果传入）
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
                await self._create_pending_signal(account_id, stock, signal_type, current_price, 0)
                await self._execute_sell_signal(
                    account_id, stock, current_price,
                    signal_type, 0  # 0 = 全部卖出
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

        买入数量解析（按优先级）：
        1. 个股策略 max_trade_quantity > 0 → 按该数量
        2. 选股策略 config.quantity → 按固定数量
        3. 选股策略 config.position_pct → 按可用资金比例计算
        4. 账户 max_single_position_pct → 按单股最大仓位计算
        5. 回退到 watchlist target_quantity
        """
        stock_code = stock.get('stock_code')
        stock_name = stock.get('stock_name', '')

        db = get_db_manager()

        # 优先级 1：个股策略 max_trade_quantity
        ts = await db.fetchone(
            "SELECT max_trade_quantity FROM trading_strategies WHERE account_id = ? AND stock_code = ?",
            (account_id, stock_code),
        )
        if ts and ts.get("max_trade_quantity", 0) > 0:
            target_quantity = ts["max_trade_quantity"]
        else:
            # 优先级 2-3：从选股策略配置读取 quantity 或 position_pct
            strategy_id = stock.get('strategy_id')
            if strategy_id:
                strategy = await db.fetchone(
                    "SELECT config FROM strategies WHERE id = ? AND account_id = ?",
                    (strategy_id, account_id),
                )
                if strategy and strategy.get("config"):
                    try:
                        import json
                        config = json.loads(strategy["config"]) if isinstance(strategy["config"], str) else strategy["config"]
                        if config.get("quantity"):
                            target_quantity = int(config["quantity"])
                        elif config.get("position_pct"):
                            # 按可用资金比例计算
                            position_pct = float(config["position_pct"]) / 100.0
                            account = await db.fetchone(
                                "SELECT available_cash FROM accounts WHERE account_id = ?",
                                (account_id,)
                            )
                            if account:
                                available_cash = account.get('available_cash', 0)
                                buy_amount = available_cash * position_pct
                                if current_price > 0:
                                    qty = int((buy_amount / current_price) // 100) * 100
                                    target_quantity = max(qty, 0)
                    except (json.JSONDecodeError, ValueError, TypeError):
                        pass

            # 优先级 4：如果 target_quantity 未设置，按账户单股最大仓位计算
            if not target_quantity:
                account = await db.fetchone(
                    "SELECT available_cash, max_single_position_pct FROM accounts WHERE account_id = ?",
                    (account_id,)
                )
                if account and current_price > 0:
                    available_cash = account.get('available_cash', 0)
                    max_single_pct = account.get('max_single_position_pct', 0.15)
                    # 总资产 = 持仓市值 + 可用资金
                    positions = await db.fetchall(
                        "SELECT SUM(market_value) as total_mv FROM stock_positions WHERE account_id = ?",
                        (account_id,)
                    )
                    current_mv = positions[0]["total_mv"] if positions and positions[0]["total_mv"] else 0
                    total_assets = current_mv + available_cash
                    risk_limit = int(total_assets * max_single_pct / current_price)
                    if risk_limit >= 100:
                        target_quantity = (risk_limit // 100) * 100

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
            log = get_logger("monitor")
            log.log_event("buy_success", f"买入成功：{stock_code}",
                          stock_code=stock_code, quantity=result['quantity'],
                          price=result['price'], total_amount=result['total_amount'],
                          fees=result['fees']['total_fee'])

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
            get_logger("monitor").warn("monitor", f"买入失败: {result['message']}",
                                        stock_code=stock_code, message=result['message'])
            # 信号 pending → cancelled
            await self._update_signal_status(
                account_id, stock_code, 'cancelled', target_quantity
            )

            # 发送券商拒绝通知
            notification = get_notification_service()
            await notification.emit(
                event_type="order_rejected",
                account_id=account_id,
                payload={
                    "trade_type": "buy",
                    "stock_code": stock_code,
                    "stock_name": stock_name,
                    "price": f"{current_price:.2f}",
                    "quantity": target_quantity,
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
            log = get_logger("monitor")
            log.log_event("sell_success", f"卖出成功：{stock_code}",
                          stock_code=stock_code, quantity=result['quantity'],
                          price=result['price'], net_amount=result['net_amount'],
                          fees=result['fees']['total_fee'], profit_loss=result['profit_loss'])

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
            get_logger("monitor").warn("monitor", f"卖出失败: {result['message']}",
                                        stock_code=stock_code, message=result['message'])
            # 信号 pending → cancelled
            await self._update_signal_status(
                account_id, stock_code, 'cancelled', 0
            )

            # 发送券商拒绝通知
            notification = get_notification_service()
            await notification.emit(
                event_type="order_rejected",
                account_id=account_id,
                payload={
                    "trade_type": "sell",
                    "stock_code": stock_code,
                    "stock_name": stock_name,
                    "price": f"{current_price:.2f}",
                    "quantity": target_quantity,
                    "reason": result['message'],
                },
            )

    async def _scan_pending_signals(self, account_id: str, market_data_cache: Optional[Dict[str, Any]] = None):
        """扫描手动创建的 pending 信号并执行

        流程：
        1. 查询 trading_signals 中 status=pending 的记录
        2. 获取实时行情
        3. 执行交易（买入/卖出）
        4. 更新信号状态

        注意：此方法只处理无 strategy_id 的手动信号，
        有 strategy_id 的信号由 _execute_buy_signal / _execute_sell_signal 即时执行。
        """
        db = get_db_manager()
        signals = await db.fetchall(
            "SELECT * FROM trading_signals WHERE account_id = ? AND status = 'pending' AND strategy_id IS NULL ORDER BY created_at ASC",
            (account_id,)
        )
        if not signals:
            return

        stock_codes = [s['stock_code'] for s in signals]

        # 使用预取的行情数据，或自行获取
        if market_data_cache:
            market_data = market_data_cache
        else:
            try:
                from services.trading.gateway import get_gateway
                gateway = await get_gateway()
                market_data = await gateway.get_batch_market_data(stock_codes)
            except Exception as e:
                get_logger("monitor").error("monitor", f"批量获取行情失败: {e}")
                return

        for signal in signals:
            stock_code = signal['stock_code']
            signal_type = signal['signal_type']
            target_price = signal.get('price', 0)
            quantity = signal.get('target_quantity', 0)

            md = market_data.get(stock_code)
            if not md or not md.current_price:
                get_logger("monitor").warn("monitor", f"无法获取 {stock_code} 行情，跳过信号 {signal['id']}",
                                            stock_code=stock_code, signal_id=signal['id'])
                continue

            current_price = md.current_price

            if signal_type == 'buy':
                # 买入信号：现价必须 <= 目标价
                if target_price > 0 and current_price > target_price:
                    get_logger("monitor").log_event("pending_signal_skip",
                        f"跳过手动 pending 信号: {stock_code} 买入，现价 {current_price:.2f} 高于目标价 {target_price:.2f}",
                        stock_code=stock_code, signal_type=signal_type,
                        target_price=target_price, current_price=current_price, quantity=quantity)
                    continue

                get_logger("monitor").log_event("pending_signal_execute",
                    f"执行手动 pending 信号: {stock_code} {signal_type}",
                    stock_code=stock_code, signal_type=signal_type,
                    target_price=target_price, current_price=current_price, quantity=quantity)

                await self._execute_buy_signal(
                    account_id,
                    {"stock_code": stock_code, "stock_name": signal.get('stock_name', stock_code)},
                    current_price,
                    quantity,
                    trigger_source="manual",
                )
                # 无论成功或失败，标记信号为 executed/cancelled，防止重复执行
                if signal_type == 'buy':
                    # 买入信号：执行后标记为 executed（即使被风控拒绝，也不再重复尝试）
                    await db.update(
                        "trading_signals",
                        {"status": "cancelled", "executed_at": format_china_time(),
                         "result": '{"message": "执行失败，见订单记录"}'},
                        "id = ?",
                        (signal['id'],)
                    )
            elif signal_type in ('sell_stop_loss', 'sell_take_profit', 'sell'):
                await self._execute_sell_signal(
                    account_id,
                    {"stock_code": stock_code, "stock_name": signal.get('stock_name', stock_code)},
                    current_price,
                    signal_type,
                    quantity,
                    trigger_source="manual",
                )
            else:
                # 未知信号类型，标记为 cancelled
                await db.update(
                    "trading_signals",
                    {"status": "cancelled", "executed_at": format_china_time()},
                    "id = ?",
                    (signal['id'],)
                )
                get_logger("monitor").warn("monitor", f"未知信号类型 {signal_type}，取消信号",
                                            signal_type=signal_type, signal_id=signal['id'])

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
        get_logger("monitor").log_event("signal_created",
            f"创建交易信号(pending)：{stock.get('stock_code')} - {signal_type}",
            stock_code=stock.get('stock_code'), signal_type=signal_type, signal_id=signal_id)
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
                "WHERE rowid = (SELECT rowid FROM trading_signals "
                "WHERE account_id = ? AND stock_code = ? AND status = 'pending' "
                "ORDER BY created_at DESC LIMIT 1)",
                (status, quantity, now, account_id, stock_code),
            )
        else:
            await db.execute(
                "UPDATE trading_signals SET status = ?, executed_at = ? "
                "WHERE rowid = (SELECT rowid FROM trading_signals "
                "WHERE account_id = ? AND stock_code = ? AND status = 'pending' "
                "ORDER BY created_at DESC LIMIT 1)",
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
        old = await db.fetchone(
            "SELECT status FROM watchlist WHERE account_id = ? AND stock_code = ?",
            (account_id, stock_code)
        )
        old_status = old["status"] if old else "unknown"
        await db.update(
            "watchlist",
            {"status": status, "updated_at": get_china_time()},
            "account_id = ? AND stock_code = ?",
            (account_id, stock_code)
        )
        get_logger("monitor").log_event("watchlist_status_change",
            f"watchlist 状态变更: {stock_code} {old_status} → {status}",
            account_id=account_id, stock_code=stock_code,
            old_status=old_status, new_status=status)

    def get_status(self) -> Dict:
        """获取服务状态"""
        return {
            "running": self._running,
            "account_ids": self._account_ids,
            "task": "active" if self._task else None,
            "sdk_healthy": self._sdk_healthy,
            "sdk_error_time": self._sdk_error_time,
            "sdk_error_msg": self._sdk_error_msg,
            "consecutive_errors": self._consecutive_errors,
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
