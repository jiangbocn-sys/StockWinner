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
        """全局监控：刷新所有活跃 watchlist + 持仓股行情，统一通过 dispatcher 批量获取"""
        db = get_db_manager()
        log = get_logger("monitor")

        # 收集所有需要刷新的股票：全账户去重，不按用户区分
        monitoring_codes: set = set()
        account_stocks: Dict[str, set] = {}
        try:
            # 全部活跃 watchlist 股票（跨账户去重）
            wl = await db.fetchall("SELECT DISTINCT stock_code FROM watchlist WHERE is_active = 1")
            all_wl_codes = {r["stock_code"] for r in wl}
            # 全部持仓股
            pos = await db.fetchall("SELECT DISTINCT stock_code FROM stock_positions WHERE quantity > 0")
            all_pos_codes = {r["stock_code"] for r in pos}
            monitoring_codes = all_wl_codes | all_pos_codes
        except Exception:
            pass

        # 每个账户分发时只传该账户实际有持仓/watchlist的股票
        for acct_id in account_ids:
            if not self._running:
                return
            codes = set()
            try:
                wl = await db.fetchall(
                    "SELECT DISTINCT stock_code FROM watchlist WHERE account_id = ? AND is_active = 1",
                    (acct_id,),
                )
                codes.update(r["stock_code"] for r in wl)
                pos = await db.fetchall(
                    "SELECT stock_code FROM stock_positions WHERE account_id = ? AND quantity > 0",
                    (acct_id,),
                )
                codes.update(r["stock_code"] for r in pos)
            except Exception:
                pass
            account_stocks[acct_id] = codes

        if not monitoring_codes:
            return

        # 通过 dispatcher 批量获取行情（一次订阅，批量 kline 查询）
        market_data_cache: Dict[str, Any] = {}
        try:
            gw = await get_gateway()
            gw.subscribe("monitor", monitoring_codes, refresh_interval=120, priority=1)
            # 添加超时保护：SDK 调用最多等待 90 秒（考虑 984 只股票分批查询）
            try:
                market_data_cache = await asyncio.wait_for(gw.refresh_now("monitor"), timeout=90.0)
            except asyncio.TimeoutError:
                log.log_event("monitor_sdk_timeout", "SDK 刷新超时（90秒），跳过本轮监控")
                self._health.record_sdk_error(Exception("SDK refresh timeout"))
                return
            self._health.record_sdk_success()

            valid_count = sum(1 for md in market_data_cache.values() if md and getattr(md, 'current_price', 0) > 0)
            self._health.record_data_valid(valid_count, len(market_data_cache))

            # 全局更新 PriceCache（一次写入，所有用户共享）
            self._price_mgr.update_price_cache(market_data_cache)
        except Exception as e:
            self._health.record_sdk_error(e)
            return

        # 分发到各账户
        for acct_id in account_ids:
            if not self._running:
                return
            acct_codes = account_stocks.get(acct_id, set())
            if not acct_codes:
                continue
            acct_market = {code: market_data_cache.get(code) for code in acct_codes if code in market_data_cache}
            if not acct_market:
                continue
            await self._run_per_account(acct_id, acct_market)

        # 价格刷盘
        if self._price_mgr.should_flush():
            for acct_id in account_ids:
                await self._price_mgr.flush_to_db(acct_id)

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

        # 扫描手动 pending 信号（需要实时行情）
        await self._executor.scan_pending_signals(account_id, market_data_cache=tradable_data)

        # 刷新持仓盈亏（可接受任何数据源）
        await self._price_mgr.refresh_positions_pnl(account_id, market_data_cache=market_data_cache)

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
