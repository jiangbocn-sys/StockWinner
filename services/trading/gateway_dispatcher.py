"""
GatewayDispatcher — 整合到 gateway 模块的行情调度器

职责：
1. 管理订阅（subscriber_id → stock_codes + priority + interval）
2. 后台定时刷新（合并去重后分批调 SDK snapshot）
3. 即时刷新（refresh_now）
4. 结果写入 PriceCache
5. 通过 gateway 的 _serial_lock 排队，消除多头竞争

调用方通过 gateway 访问：
    gateway.subscribe(sub_id, codes, interval, priority)
    await gateway.refresh_now(sub_id)
    gateway.unsubscribe(sub_id)
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


class GatewayDispatcher:
    """行情刷新调度器 — 作为 gateway 的内部组件

    - 管理多个订阅者的股票集合
    - 合并去重后分批调 SDK snapshot（通过 gateway 的 _serial_lock 排队）
    - 结果写入 PriceCache
    - 后台循环自动刷新活跃的订阅
    """

    _snapshot_disabled = True   # pandas 2.x 不兼容 snapshot，默认跳过

    def __init__(self):
        self._lock: Optional[asyncio.Lock] = None
        self._subscriptions: Dict[str, Subscription] = {}
        self._running = False
        self._bg_task: Optional[asyncio.Task] = None
        self._stock_last_refresh: Dict[str, float] = {}  # stock_code → last refresh time
        self._min_refresh_gap: float = 25.0  # 同一股票最小刷新间隔
        self._tick_interval: float = 10.0    # 后台循环 tick 间隔
        self._sdk_batch_size: int = 200       # SDK kline 单次批量上限约 200-300 只
        self._loop_id: Optional[int] = None  # 记录当前事件循环 ID
        self._sdk_healthy = True
        self._sdk_error_time: Optional[str] = None
        self._sdk_error_msg: str = ""
        self._consecutive_errors = 0
        self._last_data_time: Optional[str] = None
        self._data_stale = False

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

        # 写入 PriceCache（source 由 MarketData.source 自动决定）
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
        get_logger("dispatcher").info("dispatcher", "GatewayDispatcher 已启动")

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
        get_logger("dispatcher").info("dispatcher", "GatewayDispatcher 已停止")

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

        # 执行 SDK 查询（通过 gateway 的 _serial_lock 排队）
        async with self._get_lock():
            result = await self._query_sdk(list(all_codes))

        if result:
            self._write_to_price_cache(result)
            for sub in due_subs:
                sub.last_refresh_ts = time.time()

    # ── SDK 查询 ──

    async def _query_sdk(self, codes: List[str]) -> Dict[str, Any]:
        """分批调 SDK snapshot，失败后回退 K 线数据，返回 {stock_code: MarketData}

        所有 SDK 调用通过 asyncio.to_thread 运行在线程池中，不阻塞事件循环。
        """
        import asyncio
        from services.common.sdk_manager import get_sdk_manager
        from services.common.timezone import get_china_time
        from services.trading.gateway import MarketData

        if not codes:
            return {}

        sdk_mgr = get_sdk_manager()
        today_int = int(get_china_time().strftime('%Y%m%d'))
        all_snapshots: Dict[str, Any] = {}

        # ① snapshot 默认跳过（pandas 2.x 不兼容），直接走 kline fallback
        if not GatewayDispatcher._snapshot_disabled and await asyncio.to_thread(sdk_mgr.connect):
            for i in range(0, len(codes), self._sdk_batch_size):
                batch = codes[i:i + self._sdk_batch_size]
                result = await asyncio.to_thread(
                    sdk_mgr.query_snapshot,
                    code_list=batch, begin_date=today_int, end_date=today_int
                )

                batch_valid = 0
                if result and isinstance(result, dict):
                    for date_key in result:
                        inner = result[date_key]
                        if isinstance(inner, dict):
                            for code, df in inner.items():
                                if df is not None and hasattr(df, 'empty') and not df.empty:
                                    all_snapshots[code] = df
                                    batch_valid += 1
                # 首批返回空 → snapshot 不可用，永久禁用
                if i == 0 and batch_valid == 0:
                    GatewayDispatcher._snapshot_disabled = True
                    get_logger("dispatcher").log_event("snapshot_disabled",
                        "snapshot 不可用（pandas 2.x 不兼容），已永久禁用，后续直接走 kline fallback")
                    break
        # snapshot 跳过/失败时 all_snapshots 为空 → 所有 code 走 kline fallback

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
                    md = self._build_market_data(row, code)
                    # 非交易时段 snapshot 可能返回有行但价格全为 0 的数据，当作失败处理
                    if md and md.current_price > 0:
                        results[code] = md
                    else:
                        snapshot_failed.append(code)
                        results[code] = None
                except Exception as e:
                    get_logger("dispatcher").log_event("parse_error", f"快照数据解析失败 {code}: {e}")
                    results[code] = None

        # ② snapshot 失败的代码，回退到 K 线数据
        if snapshot_failed:
            kline_results = await self._fallback_kline(snapshot_failed)
            for code in snapshot_failed:
                if code in kline_results:
                    results[code] = kline_results[code]

        # ③ 仍有 None 的代码 → 从本地 kline.db 兜底（仅非交易时段）
        # 交易时段不使用 kline.db 兜底，避免用昨收价冒充实时价误导策略
        still_none = [c for c in codes if results.get(c) is None]
        if still_none:
            from services.trading.trading_hours import can_trade
            if not can_trade():
                kline_db_results = self._fill_from_kline_db_local(set(still_none))
                for code, md in kline_db_results.items():
                    if md and md.current_price > 0:
                        results[code] = md

        valid_count = sum(1 for v in results.values() if v is not None)

        # 更新健康状态
        if valid_count > 0:
            self._last_data_time = get_china_time().strftime("%Y-%m-%d %H:%M:%S")
            if self._data_stale:
                get_logger("dispatcher").log_event("data_recovered", f"行情数据已恢复，有效股票数={valid_count}")
                self._data_stale = False
            if not self._sdk_healthy:
                self._sdk_healthy = True
                self._sdk_error_time = None
                self._sdk_error_msg = ""
                self._consecutive_errors = 0
        elif len(codes) > 0:
            self._data_stale = True
            self._sdk_healthy = False
            self._sdk_error_msg = f"SDK 返回 {len(codes)} 只股票但全部现价无效"
            self._consecutive_errors += 1

        get_logger("dispatcher").info("dispatcher",
            f"SDK 行情: requested={len(codes)}, snapshot_valid={len(codes)-len(snapshot_failed)}, "
            f"kline_fallback={sum(1 for c in snapshot_failed if c in results and results[c] is not None)}, "
            f"total_valid={valid_count}")
        return results

    async def _fallback_kline(self, codes: List[str]) -> Dict[str, Any]:
        """snapshot 失败时回退到 K 线数据获取价格（不限股票数，分批 15 只）

        所有 SDK 调用通过 asyncio.to_thread 运行在线程池中，不阻塞事件循环。
        """
        import asyncio
        import datetime
        from datetime import timedelta
        from services.common.sdk_manager import get_sdk_manager
        from services.trading.gateway import MarketData

        if not codes:
            return {}

        sdk_mgr = get_sdk_manager()

        # 获取正确的日K线周期值
        try:
            from AmazingData import constant
            DAY_PERIOD = constant.Period.day.value
        except Exception:
            DAY_PERIOD = 10008  # fallback

        end_dt = get_china_time()
        begin_dt = end_dt - timedelta(days=3)
        end_date_int = int((end_dt + timedelta(days=1)).strftime('%Y%m%d'))
        begin_date_int = int(begin_dt.strftime('%Y%m%d'))

        results: Dict[str, Any] = {}

        # 分批查询 K 线，SDK 单次上限约 200 只
        batch_size = 200
        for i in range(0, len(codes), batch_size):
            batch = codes[i:i + batch_size]
            result = await asyncio.to_thread(
                sdk_mgr.query_kline,
                code_list=batch,
                begin_date=begin_date_int,
                end_date=end_date_int,
                period=DAY_PERIOD,
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
            bid=[current_price] * 5,
            ask=[current_price] * 5,
            bid_volume=[0] * 5,
            ask_volume=[0] * 5,
            trade_date=str(row.get('trade_date', row.get('kline_time', ''))),
            source="kline",
        )

    @staticmethod
    def _fill_from_kline_db_local(codes: Set[str]) -> Dict[str, Any]:
        """从本地 kline.db 读取最新收盘价兜底（不依赖 SDK）"""
        from services.common.database import get_sync_connection
        from services.trading.gateway import MarketData

        results: Dict[str, Any] = {}
        try:
            conn = get_sync_connection("kline")
            cursor = conn.cursor()
            placeholders = ','.join(['?'] * len(codes))
            cursor.execute(f"""
                SELECT k.stock_code, k.close, k.open, k.high, k.low, k.volume, k.amount, k.trade_date
                FROM kline_data k
                INNER JOIN (
                    SELECT stock_code, MAX(trade_date) as max_date
                    FROM kline_data
                    WHERE stock_code IN ({placeholders})
                    GROUP BY stock_code
                ) latest ON k.stock_code = latest.stock_code AND k.trade_date = latest.max_date
            """, list(codes))
            for row in cursor.fetchall():
                code = row['stock_code']
                close = float(row['close'] or 0)
                if close > 0:
                    open_p = float(row['open'] or close)
                    high = float(row['high'] or close)
                    low = float(row['low'] or close)
                    vol = float(row['volume'] or 0)
                    amt = float(row['amount'] or 0)
                    results[code] = MarketData(
                        stock_code=code,
                        stock_name=code,
                        current_price=close,
                        change_percent=0,
                        high=high,
                        low=low,
                        open_price=open_p,
                        prev_close=close,
                        volume=int(vol),
                        amount=amt,
                        bid=[close] * 5,
                        ask=[close] * 5,
                        bid_volume=[0] * 5,
                        ask_volume=[0] * 5,
                        trade_date=str(row['trade_date'] or '').replace('-', ''),
                        source="kline_db",
                    )
        except Exception:
            pass
        return results

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

        def get_first_float(candidates, default=0.0):
            """依次尝试多个列名，返回第一个非零且非 NaN 的值"""
            import math
            for key in candidates:
                try:
                    v = row.get(key) if hasattr(row, 'get') else getattr(row, key, None)
                    fval = float(v)
                    if fval == 0 or math.isnan(fval) or fval is None:
                        continue
                    return fval
                except (ValueError, TypeError, AttributeError):
                    continue
            return default

        stock_name = get_str('stock_name') or get_str('name') or stock_code
        bid = [get_float(f'bid_price{i}', 0) for i in range(1, 6)]
        ask = [get_float(f'ask_price{i}', 0) for i in range(1, 6)]
        bid_volume = [get_int(f'bid_volume{i}', 0) for i in range(1, 6)]
        ask_volume = [get_int(f'ask_volume{i}', 0) for i in range(1, 6)]

        current_price = get_first_float(['last', 'current_price', 'price', 'close'], 0)
        prev_close = get_first_float(['pre_close', 'prev_close', 'preclose'], 0)
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
        """将 MarketData 结果写入 PriceCache（source 由 MarketData.source 决定）"""
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
                    source=md.source if hasattr(md, 'source') else "",
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
            "sdk_healthy": self._sdk_healthy,
            "sdk_error_time": self._sdk_error_time,
            "sdk_error_msg": self._sdk_error_msg,
            "data_stale": self._data_stale,
            "last_data_time": self._last_data_time,
        }


# ── 全局单例 ──

_dispatcher: Optional[GatewayDispatcher] = None
_dispatcher_lock = __import__('threading').Lock()


def get_gateway_dispatcher() -> GatewayDispatcher:
    """获取全局调度器单例"""
    global _dispatcher
    if _dispatcher is None:
        with _dispatcher_lock:
            if _dispatcher is None:
                _dispatcher = GatewayDispatcher()
    return _dispatcher


def reset_gateway_dispatcher():
    """重置调度器（用于测试）"""
    global _dispatcher
    _dispatcher = None
