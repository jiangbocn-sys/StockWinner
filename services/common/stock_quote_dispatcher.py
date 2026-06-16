"""
股票行情调度器 — StockQuoteDispatcher

统一管理所有 SDK 行情查询，支持按需订阅、去重、优先级调度。

替代方案：各模块直接调用 gateway.get_batch_market_data → 所有股票一次性查询。
新方案：各模块订阅关心的股票集合，dispatcher 合并去重后分批查询 SDK snapshot。

调用链：subscriber → dispatcher → sdk_mgr.query_snapshot (直接调用，不经过 gateway)
"""

import asyncio
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Any

from services.common.structured_logger import get_logger
from services.common.timezone import get_china_time


@dataclass
class Subscription:
    """行情订阅：某个组件关心的股票集合"""
    subscriber_id: str              # "monitor", "wl:acct:group1", "pos:acct"
    stock_codes: Set[str] = field(default_factory=set)
    last_refresh_ts: float = 0.0    # 上次成功刷新的时间戳
    refresh_interval: int = 30      # 自动刷新间隔（秒），0 = 不自动刷新
    priority: int = 2               # 1=高(监控), 2=中(UI), 3=低(后台)


class StockQuoteDispatcher:
    """行情刷新调度器

    - 管理多个订阅者的股票集合
    - 合并去重后分批调 SDK snapshot
    - 结果写入 PriceCache
    - 后台循环自动刷新活跃的订阅
    """

    def __init__(self):
        self._lock: Optional[asyncio.Lock] = None
        self._subscriptions: Dict[str, Subscription] = {}
        self._running = False
        self._bg_task: Optional[asyncio.Task] = None
        self._stock_last_refresh: Dict[str, float] = {}  # stock_code → last refresh time
        self._min_refresh_gap: float = 25.0  # 同一股票最小刷新间隔
        self._tick_interval: float = 10.0    # 后台循环 tick 间隔
        self._sdk_batch_size: int = 100      # 每批 SDK 查询股票数（增大批量减少调用次数）
        self._loop_id: Optional[int] = None  # 记录当前事件循环 ID

    def _get_lock(self) -> asyncio.Lock:
        """延迟初始化 Lock，支持事件循环切换"""
        current_loop_id = id(asyncio.get_event_loop())
        if self._lock is None or self._loop_id != current_loop_id:
            self._lock = asyncio.Lock()
            self._loop_id = current_loop_id
        return self._lock

    # ── 订阅管理 ──

    def subscribe(self, subscriber_id: str, stock_codes: Set[str],
                  refresh_interval: int = 30, priority: int = 2):
        """注册或更新订阅"""
        if subscriber_id in self._subscriptions:
            sub = self._subscriptions[subscriber_id]
            sub.stock_codes = stock_codes
            sub.refresh_interval = refresh_interval
            sub.priority = priority
        else:
            self._subscriptions[subscriber_id] = Subscription(
                subscriber_id=subscriber_id,
                stock_codes=stock_codes,
                refresh_interval=refresh_interval,
                priority=priority,
            )

    def unsubscribe(self, subscriber_id: str):
        """移除订阅"""
        self._subscriptions.pop(subscriber_id, None)

    def update_subscription(self, subscriber_id: str, stock_codes: Set[str],
                             interval: int = 30, priority: int = 2):
        """更新或注册订阅（兼容别名）"""
        self.subscribe(subscriber_id, stock_codes, interval, priority)

    def update_codes(self, subscriber_id: str, stock_codes: Set[str]):
        """更新某订阅的股票列表"""
        if subscriber_id in self._subscriptions:
            self._subscriptions[subscriber_id].stock_codes = stock_codes

    # ── 即时刷新 ──

    async def refresh_now(self, subscriber_id: str) -> Dict[str, Any]:
        """立即刷新指定订阅的股票，返回 {stock_code: MarketData}，并写入 PriceCache"""
        sub = self._subscriptions.get(subscriber_id)
        if not sub or not sub.stock_codes:
            return {}

        codes = list(sub.stock_codes)
        result = await self._query_sdk(codes)

        # 写入 PriceCache
        if result:
            self._write_to_price_cache(result)

        # 更新订阅的时间戳
        sub.last_refresh_ts = time.time()
        return result

    # ── 后台调度循环 ──

    async def start(self):
        """启动后台调度循环"""
        if self._running:
            return
        self._running = True
        self._bg_task = asyncio.create_task(self._dispatch_loop())
        get_logger("dispatcher").info("dispatcher", "StockQuoteDispatcher 已启动")

    async def stop(self):
        """停止后台调度循环"""
        self._running = False
        if self._bg_task:
            self._bg_task.cancel()
            try:
                await self._bg_task
            except asyncio.CancelledError:
                pass
            self._bg_task = None
        get_logger("dispatcher").info("dispatcher", "StockQuoteDispatcher 已停止")

    async def _dispatch_loop(self):
        """后台调度循环，每 tick_interval 秒执行一次"""
        while self._running:
            try:
                await self._dispatch_tick()
            except asyncio.CancelledError:
                break
            except Exception as e:
                get_logger("dispatcher").error("dispatcher", f"调度循环异常: {e}")
            await asyncio.sleep(self._tick_interval)

    async def _dispatch_tick(self):
        """单次调度 tick：收集需要刷新的订阅，合并去重后刷新"""
        from services.trading.trading_hours import can_trade

        if not can_trade():
            return

        now = time.time()

        # 收集需要刷新的订阅（间隔到期且未在冷却中）
        due_subs = []
        for sub in self._subscriptions.values():
            if sub.refresh_interval <= 0:
                continue  # 不自动刷新
            elapsed = now - sub.last_refresh_ts
            if elapsed >= sub.refresh_interval - 5:  # 提前 5 秒触发
                due_subs.append(sub)

        if not due_subs:
            return

        # 按优先级排序
        due_subs.sort(key=lambda s: s.priority)

        # 合并所有需要刷新的股票，去重
        all_codes: Set[str] = set()
        for sub in due_subs:
            for code in sub.stock_codes:
                last = self._stock_last_refresh.get(code, 0)
                if now - last >= self._min_refresh_gap:
                    all_codes.add(code)

        if not all_codes:
            return

        # 执行 SDK 查询
        async with self._get_lock():
            result = await self._query_sdk(list(all_codes))

        if result:
            self._write_to_price_cache(result)
            for sub in due_subs:
                sub.last_refresh_ts = time.time()

    # ── SDK 查询 ──

    async def _query_sdk(self, codes: List[str]) -> Dict[str, Any]:
        """分批调 SDK snapshot，失败后回退 K 线数据，返回 {stock_code: MarketData}"""
        from services.common.sdk_manager import get_sdk_manager
        from services.common.timezone import get_china_time
        from services.trading.gateway import MarketData

        if not codes:
            return {}

        sdk_mgr = get_sdk_manager()
        today_int = int(get_china_time().strftime('%Y%m%d'))
        all_snapshots: Dict[str, Any] = {}

        # ① 优先尝试 snapshot
        for i in range(0, len(codes), self._sdk_batch_size):
            batch = codes[i:i + self._sdk_batch_size]
            if not sdk_mgr.connect():
                break
            result = sdk_mgr.query_snapshot(code_list=batch, begin_date=today_int, end_date=today_int)

            if result and isinstance(result, dict):
                for date_key in result:
                    inner = result[date_key]
                    if isinstance(inner, dict):
                        for code, df in inner.items():
                            if df is not None and hasattr(df, 'empty') and not df.empty:
                                all_snapshots[code] = df

        # 更新股票最后刷新时间
        now = time.time()
        for code in codes:
            self._stock_last_refresh[code] = now

        # 构造 MarketData 并收集 snapshot 失败的代码
        results = {}
        snapshot_failed = []
        for code in codes:
            snap = all_snapshots.get(code)
            if snap is None or (hasattr(snap, "empty") and snap.empty) or len(snap) == 0:
                snapshot_failed.append(code)
                results[code] = None
            else:
                row = snap.iloc[0] if hasattr(snap, "iloc") else snap
                try:
                    results[code] = self._build_market_data(row, code)
                except Exception as e:
                    get_logger("dispatcher").log_event("parse_error", f"快照数据解析失败 {code}: {e}")
                    results[code] = None

        # ② snapshot 失败的代码，回退到 K 线数据
        if snapshot_failed:
            kline_results = await self._fallback_kline(snapshot_failed)
            for code in snapshot_failed:
                if code in kline_results:
                    results[code] = kline_results[code]

        valid_count = sum(1 for v in results.values() if v is not None)
        get_logger("dispatcher").info("dispatcher",
            f"SDK 行情: requested={len(codes)}, snapshot_valid={len(codes)-len(snapshot_failed)}, "
            f"kline_fallback={sum(1 for c in snapshot_failed if c in results and results[c] is not None)}, "
            f"total_valid={valid_count}")
        return results

    async def _fallback_kline(self, codes: List[str]) -> Dict[str, Any]:
        """snapshot 失败时回退到 K 线数据获取价格

        注意：snapshot 超时后 SDK 连接可能处于不稳定状态，K 线查询也会很慢。
        策略：只尝试前 10 只股票，每批 5 只快速查询，避免进一步阻塞。
        """
        import datetime
        from datetime import timedelta
        from services.common.sdk_manager import get_sdk_manager
        from services.trading.gateway import MarketData

        if not codes:
            return {}

        # 正确的日K线周期值
        try:
            from AmazingData import constant
            DAY_PERIOD = constant.Period.day.value
        except Exception:
            DAY_PERIOD = 10008

        # snapshot 超时后连接可能已坏，只尝试少量股票
        max_fallback = 10
        codes_to_try = codes[:max_fallback]

        sdk_mgr = get_sdk_manager()
        end_dt = get_china_time()
        begin_dt = end_dt - timedelta(days=2)
        end_date_int = int((end_dt + timedelta(days=1)).strftime('%Y%m%d'))
        begin_date_int = int(begin_dt.strftime('%Y%m%d'))

        results: Dict[str, Any] = {}

        # 分批查询 K 线，每批 5 只（snapshot 超时后 K 线也会很慢）
        batch_size = 5
        for i in range(0, len(codes_to_try), batch_size):
            batch = codes_to_try[i:i + batch_size]
            if not sdk_mgr.is_connected():
                break
            result = sdk_mgr.query_kline(
                code_list=batch,
                begin_date=begin_date_int,
                end_date=end_date_int,
                period=DAY_PERIOD,  # day
            )

            if result and isinstance(result, dict):
                for code in batch:
                    df = result.get(code)
                    if df is None or not hasattr(df, 'empty') or df.empty:
                        continue
                    last_row = df.iloc[-1]
                    results[code] = self._build_market_data_from_kline(last_row, code)

        return results

    @staticmethod
    def _build_market_data_from_kline(row, stock_code: str) -> Any:
        """从 K 线数据构造 MarketData（无真实五档，用现价填充）"""
        from services.trading.gateway import MarketData

        def get_float(key, default=0.0):
            v = row.get(key, default) if hasattr(row, 'get') else getattr(row, key, default)
            try:
                return float(v) if v is not None else default
            except (ValueError, TypeError):
                return default

        current_price = get_float('close', 0)
        prev_close = get_float('pre_close', 0)
        if prev_close == 0:
            # 尝试从上一行获取 prev_close，或用当前 close
            prev_close = current_price
        change_percent = ((current_price - prev_close) / prev_close * 100) if prev_close != 0 else 0

        stock_name = stock_code
        if hasattr(row, 'get') and row.get('stock_name'):
            stock_name = str(row.get('stock_name'))
        elif hasattr(row, 'get') and row.get('name'):
            stock_name = str(row.get('name'))

        return MarketData(
            stock_code=stock_code,
            stock_name=str(stock_name) if stock_name != current_price else stock_code,
            current_price=current_price,
            change_percent=round(change_percent, 2),
            high=get_float('high', current_price),
            low=get_float('low', current_price),
            open_price=get_float('open', current_price),
            prev_close=prev_close,
            volume=int(get_float('volume', 0)),
            amount=get_float('amount', 0),
            bid=[current_price] * 5,      # 五档不可用，用现价填充
            ask=[current_price] * 5,
            bid_volume=[0] * 5,
            ask_volume=[0] * 5,
            trade_date=str(row.get('trade_date', row.get('kline_time', ''))),
            source="kline",
        )

    # ── 工具方法 ──

    @staticmethod
    def _build_market_data(row, stock_code: str) -> Any:
        """从 SDK snapshot DataFrame row 构造 MarketData"""
        from services.trading.gateway import MarketData

        def get_float(key, default=0.0):
            v = row.get(key, default) if hasattr(row, 'get') else getattr(row, key, default)
            try:
                return float(v) if v is not None else default
            except (ValueError, TypeError):
                return default

        def get_int(key, default=0):
            return int(get_float(key, default))

        def get_str(key, default=''):
            v = row.get(key, default) if hasattr(row, 'get') else getattr(row, key, default)
            return str(v) if v is not None else default

        # 股票名称
        stock_name = get_str('stock_name') or get_str('name') or stock_code

        # 五档价格
        bid = [get_float(f'bid_price{i}', 0) for i in range(1, 6)]
        ask = [get_float(f'ask_price{i}', 0) for i in range(1, 6)]
        bid_volume = [get_int(f'bid_volume{i}', 0) for i in range(1, 6)]
        ask_volume = [get_int(f'ask_volume{i}', 0) for i in range(1, 6)]

        # current_price：与 gateway 一致
        current_price = get_float('current_price', get_float('price', 0))
        prev_close = get_float('prev_close', get_float('preclose', 0))
        if prev_close == 0:
            prev_close = current_price
        change_percent = ((current_price - prev_close) / prev_close * 100) if prev_close != 0 else 0

        return MarketData(
            stock_code=stock_code,
            stock_name=stock_name,
            current_price=current_price,
            change_percent=round(change_percent, 2),
            high=get_float('high', current_price),
            low=get_float('low', current_price),
            open_price=get_float('open', current_price),
            prev_close=prev_close,
            volume=get_int('volume'),
            amount=get_float('amount'),
            bid=bid,
            ask=ask,
            bid_volume=bid_volume,
            ask_volume=ask_volume,
            trade_date=get_str('trade_date', ''),
            source="snapshot",
        )

    @staticmethod
    def _write_to_price_cache(results: Dict[str, Any]):
        """将 MarketData 结果写入 PriceCache"""
        from services.common.price_cache import get_price_cache

        cache = get_price_cache()
        for code, md in results.items():
            if md and md.current_price > 0:
                cache.update_ohlcv(
                    code,
                    open=md.open_price or md.current_price,
                    high=md.high or md.current_price,
                    low=md.low or md.current_price,
                    close=md.current_price,
                    volume=md.volume or 0,
                    amount=md.amount or 0,
                    change_pct=md.change_percent or 0.0,
                )

    def get_status(self) -> Dict[str, Any]:
        """返回调度器状态"""
        now = time.time()
        sub_info = {}
        for sub_id, sub in self._subscriptions.items():
            sub_info[sub_id] = {
                "codes": len(sub.stock_codes),
                "last_refresh_age": round(now - sub.last_refresh_ts, 1) if sub.last_refresh_ts > 0 else None,
                "interval": sub.refresh_interval,
                "priority": sub.priority,
            }

        monitor_age = None
        monitor_sub = self._subscriptions.get("monitor")
        if monitor_sub and monitor_sub.last_refresh_ts > 0:
            monitor_age = round(now - monitor_sub.last_refresh_ts, 1)

        return {
            "running": self._running,
            "subscription_count": len(self._subscriptions),
            "subscriptions": sub_info,
            "monitor_age_seconds": monitor_age,
        }


# ── 全局单例 ──

_dispatcher: Optional[StockQuoteDispatcher] = None
_dispatcher_lock = __import__('threading').Lock()


def get_stock_quote_dispatcher() -> StockQuoteDispatcher:
    """获取全局调度器单例"""
    global _dispatcher
    if _dispatcher is None:
        with _dispatcher_lock:
            if _dispatcher is None:
                _dispatcher = StockQuoteDispatcher()
    return _dispatcher


def reset_stock_quote_dispatcher():
    """重置调度器（用于测试）"""
    global _dispatcher
    _dispatcher = None
