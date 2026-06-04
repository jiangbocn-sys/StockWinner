"""
交易监控模块 (Trading Monitor) — 协调器
监控 watchlist 中的股票，根据策略条件执行交易

功能：
1. 按 watchlist 监控候选股票行情，到达预设买卖价位进行交易
2. 读取交易策略配置，评估触发条件
3. 读取持仓策略，确定买入份额
4. 交易前读取账户可用资金，确定可买数量
5. 交易后更新可用资金 + 发送通知
6. 计算并记录交易手续费
"""

import asyncio
import time
import threading
from typing import Dict, Optional, Any

from services.common.database import get_db_manager
from services.common.timezone import get_china_time
from services.common.structured_logger import get_logger
from services.trading.gateway import get_gateway

from services.monitoring.signal_evaluator import SignalEvaluator
from services.monitoring.signal_executor import SignalExecutor
from services.monitoring.price_cache_manager import PriceCacheManager
from services.monitoring.health_tracker import HealthTracker


class TradingMonitor:
    """交易监控服务 — 协调器，委托给子模块执行"""

    def __init__(self):
        self._running = False
        self._task = None
        self._account_ids: list[str] = []
        self._state_lock = threading.Lock()
        self._last_heartbeat: float = 0.0  # 上次心跳时间戳

        # 子模块
        self._executor = SignalExecutor()
        self._evaluator = SignalEvaluator(self._executor)
        self._price_mgr = PriceCacheManager()
        self._health = HealthTracker()

    # 健康状态属性委托（供 scheduler heartbeat 检查）
    @property
    def _data_stale(self) -> bool:
        return self._health._data_stale

    @property
    def _last_data_time(self) -> Optional[str]:
        return self._health._last_data_time

    @property
    def _sdk_error_msg(self) -> str:
        return self._health._sdk_error_msg

    async def start_monitoring(self, account_ids: Optional[list[str]] = None, interval: int = 60):
        """启动交易监控服务"""
        if self._running:
            if self._task and self._task.done():
                log = get_logger("monitor")
                log.log_event("monitor_zombie_detect", "检测到监控僵尸状态（task 已死），自动清理")
                self._running = False
                self._task = None
            else:
                return {"success": False, "message": "交易监控服务已在运行"}

        if not account_ids:
            db = get_db_manager()
            accounts = await db.fetchall("SELECT account_id FROM accounts WHERE is_active = 1")
            account_ids = [a["account_id"] for a in accounts]

        if not account_ids:
            return {"success": False, "message": "没有活跃账户可监控"}

        self._running = True
        self._account_ids = account_ids
        self._health.set_account_ids(account_ids)
        self._task = asyncio.create_task(self._run_monitoring_loop(account_ids, interval))
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
        # 释放 dispatcher 订阅
        try:
            from services.trading.gateway_dispatcher import get_gateway_dispatcher
            dispatcher = get_gateway_dispatcher()
            dispatcher.unsubscribe("monitor")
        except Exception:
            pass
        return {"success": True, "message": "交易监控服务已停止"}

    async def _run_monitoring_loop(self, account_ids: list[str], interval: int):
        """交易监控循环"""
        from services.trading.trading_hours import can_trade, get_next_trading_window

        log = get_logger("monitor")
        log.log_event("monitor_loop_start", "启动交易监控服务",
                      account_ids=account_ids, interval=interval)

        while self._running:
            try:
                # 更新心跳（每次循环开始）
                self._last_heartbeat = time.time()

                if can_trade():
                    await self._run_monitoring_global(account_ids)
                    # 成功完成一轮，再次更新心跳
                    self._last_heartbeat = time.time()
                    await asyncio.sleep(interval)
                else:
                    next_time, reason = get_next_trading_window()
                    if next_time is None:
                        log.log_event("monitor_no_trading", reason)
                        await asyncio.sleep(60)
                    else:
                        wait_seconds = (next_time - get_china_time()).total_seconds()
                        wait_seconds = max(wait_seconds, 0)
                        log.log_event("monitor_sleep_until", reason,
                                      next_time=next_time.isoformat(),
                                      wait_seconds=round(wait_seconds))
                        # 非交易时段：尝试恢复 SDK 健康状态
                        if not self._health.is_healthy():
                            self._health.reset_if_sdk_connected()
                        max_wait = min(wait_seconds, 300)
                        await asyncio.sleep(max_wait)
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error("monitor_loop", f"监控循环错误: {e}")
                await asyncio.sleep(interval)

        self._running = False
        # 释放 dispatcher 订阅
        try:
            from services.trading.gateway_dispatcher import get_gateway_dispatcher
            dispatcher = get_gateway_dispatcher()
            dispatcher.unsubscribe("monitor")
        except Exception:
            pass
        log.log_event("monitor_loop_stop", "交易监控服务已停止")

    async def _run_monitoring_global(self, account_ids: list[str]):
        """全局监控：按交易需求分优先级刷新股票

        优先级：
        - highest (0): pending 信号股票（需立即判断是否执行交易）
        - high (1): 持仓止损止盈股票
        - medium (2): 策略任务股票池
        - low (3): Watchlist 纯观察
        """
        db = get_db_manager()
        log = get_logger("monitor")

        # 按交易需求分类股票
        stocks_by_priority = await self._classify_stocks_by_priority(account_ids, db)

        # 收集所有需要刷新的股票（用于 dispatcher 订阅）
        all_codes: set = set()
        for codes in stocks_by_priority.values():
            all_codes.update(codes)

        if not all_codes:
            return

        # 记录分类结果
        log.info("monitor",
            f"股票分类: pending={len(stocks_by_priority.get('pending', []))}, "
            f"positions_stop={len(stocks_by_priority.get('positions_stop', []))}, "
            f"strategy_pool={len(stocks_by_priority.get('strategy_pool', []))}, "
            f"watch_only={len(stocks_by_priority.get('watch_only', []))}")

        market_data_cache: Dict[str, Any] = {}

        try:
            gw = await get_gateway()

            # ① highest: pending 信号股票（立即判断）
            pending_codes = stocks_by_priority.get('pending', [])
            if pending_codes:
                gw.subscribe("monitor_pending", set(pending_codes), refresh_interval=60, priority=0)
                pending_data = await asyncio.wait_for(gw.refresh_now("monitor_pending"), timeout=30.0)
                market_data_cache.update(pending_data)
                # 立即评估 pending 信号
                for acct_id in account_ids:
                    if self._running:
                        await self._executor.scan_pending_signals(acct_id, market_data_cache=pending_data)
                gw.unsubscribe("monitor_pending")

            # ② high: 持仓止损止盈
            positions_stop_codes = stocks_by_priority.get('positions_stop', [])
            if positions_stop_codes:
                gw.subscribe("monitor_positions", set(positions_stop_codes), refresh_interval=60, priority=1)
                positions_data = await asyncio.wait_for(gw.refresh_now("monitor_positions"), timeout=30.0)
                market_data_cache.update(positions_data)
                # 立即评估止损止盈
                for acct_id in account_ids:
                    if self._running:
                        await self._evaluator.evaluate_positions_stop_loss(acct_id, market_data_cache=positions_data)
                gw.unsubscribe("monitor_positions")

            # ③ medium: 策略任务股票池（分批刷新）
            strategy_codes = stocks_by_priority.get('strategy_pool', [])
            if strategy_codes:
                # 分批刷新，每批 100 只，批次间让用户请求插队
                batch_size = 100
                for i in range(0, len(strategy_codes), batch_size):
                    if not self._running:
                        return
                    batch = strategy_codes[i:i + batch_size]
                    gw.subscribe("monitor_strategy", set(batch), refresh_interval=60, priority=2)
                    batch_data = await asyncio.wait_for(gw.refresh_now("monitor_strategy"), timeout=20.0)
                    market_data_cache.update(batch_data)
                    gw.unsubscribe("monitor_strategy")
                    # 策略条件评估（每批后执行）
                    for acct_id in account_ids:
                        if self._running:
                            await self._evaluator.evaluate_trading_strategies(acct_id)
                    await asyncio.sleep(0.05)  # 让用户请求插队

            # ④ low: Watchlist 纯观察（分批刷新）
            watch_only_codes = stocks_by_priority.get('watch_only', [])
            if watch_only_codes:
                batch_size = 100
                for i in range(0, len(watch_only_codes), batch_size):
                    if not self._running:
                        return
                    batch = watch_only_codes[i:i + batch_size]
                    gw.subscribe("monitor_watch", set(batch), refresh_interval=120, priority=3)
                    batch_data = await asyncio.wait_for(gw.refresh_now("monitor_watch"), timeout=20.0)
                    market_data_cache.update(batch_data)
                    gw.unsubscribe("monitor_watch")
                    # watchlist 止损止盈评估
                    for acct_id in account_ids:
                        if self._running:
                            acct_codes = set(batch)  # 简化：每批全局评估
                            acct_market = {code: batch_data.get(code) for code in acct_codes if code in batch_data}
                            await self._evaluator.monitor_watchlist(acct_id, market_data_cache=acct_market)
                    await asyncio.sleep(0.1)  # 让用户请求插队

            self._health.record_sdk_success()
            valid_count = sum(1 for md in market_data_cache.values() if md and getattr(md, 'current_price', 0) > 0)
            self._health.record_data_valid(valid_count, len(market_data_cache))

            # 全局更新 PriceCache
            self._price_mgr.update_price_cache(market_data_cache)

        except asyncio.TimeoutError:
            log.log_event("monitor_sdk_timeout", "SDK 刷新超时，跳过本轮监控")
            self._health.record_sdk_error(Exception("SDK refresh timeout"))
            return
        except Exception as e:
            self._health.record_sdk_error(e)
            return

        # 价格刷盘
        if self._price_mgr.should_flush():
            for acct_id in account_ids:
                await self._price_mgr.flush_to_db(acct_id)

    async def _classify_stocks_by_priority(self, account_ids: list[str], db) -> Dict[str, list]:
        """按交易需求分类股票

        Returns:
            {
                'pending': [],       # highest - pending 信号股票
                'positions_stop': [], # high - 持仓且有止损止盈设置
                'strategy_pool': [],  # medium - 策略任务股票池
                'watch_only': [],     # low - Watchlist 无交易计划
            }
        """
        result = {
            'pending': [],
            'positions_stop': [],
            'strategy_pool': [],
            'watch_only': [],
        }
        all_codes: set = set()

        try:
            # pending 信号股票
            pending = await db.fetchall(
                "SELECT DISTINCT stock_code FROM trading_signals WHERE status = 'pending'"
            )
            result['pending'] = [r['stock_code'] for r in pending]
            all_codes.update(result['pending'])
        except Exception:
            pass

        try:
            # 持仓且有止损止盈设置（排除已在 watchlist 中的，由 monitor_watchlist 处理）
            positions_stop = await db.fetchall(
                "SELECT DISTINCT sp.stock_code FROM stock_positions sp "
                "JOIN trading_strategies ts ON sp.stock_code = ts.stock_code "
                "WHERE sp.quantity > 0 AND (ts.stop_loss_pct > 0 OR ts.take_profit_pct > 0 OR ts.stop_loss_price > 0 OR ts.take_profit_price > 0) "
                "AND sp.stock_code NOT IN (SELECT stock_code FROM watchlist WHERE status IN ('pending', 'watching', 'bought'))"
            )
            result['positions_stop'] = [r['stock_code'] for r in positions_stop]
            all_codes.update(result['positions_stop'])
        except Exception:
            pass

        try:
            # 策略任务股票池
            strategy_tasks = await db.fetchall(
                "SELECT stock_pool FROM strategy_tasks WHERE enabled = 1"
            )
            import json
            for task in strategy_tasks:
                pool = task.get('stock_pool') or []
                if isinstance(pool, str):
                    pool = json.loads(pool)
                result['strategy_pool'].extend(pool)
            result['strategy_pool'] = list(set(result['strategy_pool']))  # 去重
            all_codes.update(result['strategy_pool'])
        except Exception:
            pass

        try:
            # Watchlist 无交易计划
            watchlist = await db.fetchall(
                "SELECT stock_code FROM watchlist WHERE status IN ('pending', 'watching', 'bought')"
            )
            for r in watchlist:
                code = r['stock_code']
                if code not in all_codes:
                    result['watch_only'].append(code)
            result['watch_only'] = list(set(result['watch_only']))  # 去重
        except Exception:
            pass

        return result

    async def _run_per_account(self, account_id: str, market_data_cache: Dict[str, Any]):
        """单个账户的监控逻辑"""
        from services.trading.trading_hours import can_trade

        # PriceCache 已在 _run_monitoring_global 中统一更新，此处不再重复

        # 交易时段：过滤掉 kline_db 兜底数据，防止策略用陈旧价格误判
        if can_trade():
            tradable_data = {code: md for code, md in market_data_cache.items()
                           if md and getattr(md, 'source', '') != 'kline_db'}
            if len(tradable_data) < len(market_data_cache):
                skipped = len(market_data_cache) - len(tradable_data)
                get_logger("monitor").log_event("monitor_skip_stale",
                    f"交易时段跳过 {skipped} 只 kline_db 兜底数据，仅用实时行情")
        else:
            tradable_data = market_data_cache

        # 策略触发评估（需要实时行情）
        await self._evaluator.evaluate_trading_strategies(account_id)

        # watchlist 止损止盈（需要实时行情）
        await self._evaluator.monitor_watchlist(account_id, market_data_cache=tradable_data)

        # 持仓止损止盈（不在 watchlist 的持仓）
        await self._evaluator.evaluate_positions_stop_loss(account_id, market_data_cache=tradable_data)

        # 扫描手动 pending 信号（需要实时行情）
        await self._executor.scan_pending_signals(account_id, market_data_cache=tradable_data)

    def get_status(self) -> Dict:
        """获取服务状态"""
        # 僵尸检测：如果心跳超过 5 分钟无更新，标记为僵尸
        heartbeat_age = time.time() - self._last_heartbeat if self._last_heartbeat > 0 else 0
        is_zombie = heartbeat_age > 300 and self._running  # 5分钟无心跳且声称运行

        return {
            "running": self._running,
            "account_ids": self._account_ids,
            "task": "active" if self._task else None,
            "heartbeat_age": round(heartbeat_age, 1),
            "is_zombie": is_zombie,
            **self._health.get_status(),
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
