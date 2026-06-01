"""
实时行情缓存服务

将 SDK 实时缓存在内存中，避免策略任务重复占用 TGW 连接。
- 监控循环/API 调用更新完整 OHLCV 行情
- 策略任务直接从缓存取当日数据，无需再调 SDK snapshot
- 每 15 分钟刷盘兜底
- 容量上限：最多 10000 条股票记录，超出时清理最旧条目

注意：行情是市场数据，与账户无关。缓存按 stock_code 索引，不按 account 隔离。

资源管理文档：
- MAX_SIZE = 10000 条股票（约 A 股全市场 5000+ 只的两倍）
- 超过上限时，按 timestamp 清理最旧的条目
- TTL 过期条目不自动清理，但在读取时跳过
"""

import time
import threading
from typing import Dict, Optional, Any, Set
from services.common.structured_logger import get_logger

# 容量上限配置（记录到系统文档）
PRICE_CACHE_MAX_SIZE = 10000  # 最多缓存 10000 只股票


# 数据格式 — 存储完整 OHLCV
class PriceEntry:
    __slots__ = ('price', 'open', 'high', 'low', 'close', 'volume', 'amount',
                 'change_pct', 'prev_close', 'timestamp', 'source')

    def __init__(self, price: float, open: float = 0, high: float = 0, low: float = 0,
                 close: float = 0, volume: float = 0, amount: float = 0, change_pct: float = 0.0,
                 prev_close: float = 0.0, source: str = ""):
        self.price = price
        self.open = open or price
        self.high = high or price
        self.low = low or price
        self.close = close or price
        self.volume = volume or 0
        self.amount = amount or 0
        self.change_pct = change_pct
        self.prev_close = prev_close
        self.timestamp = time.time()
        self.source = source  # "snapshot" / "kline" / "kline_db" / ""(unknown)


class PriceCache:
    """线程安全的内存行情缓存（全局共享，不按账户隔离）

    prev_close 管理：
    - 前一天收盘价是固定历史数据，启动时从 kline.db 预加载
    - 新增股票时自动从 kline.db 补充 prev_close
    - 实时行情刷新不覆盖 prev_close（保留预热/补充的正确值）
    """

    def __init__(self):
        self._lock = threading.Lock()
        # {stock_code: PriceEntry} — 行情与账户无关，全局一份
        self._prices: Dict[str, PriceEntry] = {}
        self._flush_interval = 900  # 15 分钟
        self._last_flush = time.time()
        self._ttl = 600  # 缓存过期时间（秒），默认 10 分钟
        self._max_size = PRICE_CACHE_MAX_SIZE

    def _load_prev_close_from_db(self, stock_code: str) -> float:
        """从 kline.db 加载上一个交易日收盘价（考虑除权调整）

        查询逻辑：使用上一个交易日日期查询 close，然后检查今天是否有除权。
        若今天有除权（复权因子变化），需调整 prev_close 为除权基准价。
        """
        try:
            from services.common.database import get_sync_connection
            from services.trading.trading_hours import get_previous_trading_day, get_china_time
            from services.data.adj_factor_service import get_adj_factor_for_stock

            prev_date = get_previous_trading_day()
            today = get_china_time().strftime('%Y-%m-%d')

            conn = get_sync_connection("kline")
            cursor = conn.cursor()
            cursor.execute("""
                SELECT close FROM kline_data
                WHERE stock_code = ? AND trade_date = ?
            """, (stock_code, prev_date))
            row = cursor.fetchone()
            if not row or not row['close']:
                return 0.0

            prev_close = float(row['close'])

            # 从数据库检查今天是否有除权
            try:
                factors = get_adj_factor_for_stock(stock_code, start_date=prev_date, end_date=today)
                if factors:
                    # 找到 prev_date 和 today 的累计因子
                    prev_cum = None
                    today_cum = None
                    for f in factors:
                        if f['trade_date'] == prev_date:
                            prev_cum = f.get('cumulative_factor', 1.0)
                        elif f['trade_date'] == today:
                            today_cum = f.get('cumulative_factor', 1.0)

                    # 若今天有除权（累计因子变化），调整 prev_close
                    if prev_cum and today_cum and abs(today_cum - prev_cum) > 0.001:
                        adj_ratio = today_cum / prev_cum
                        prev_close = prev_close / adj_ratio
            except Exception:
                pass  # 无复权因子数据时跳过调整

            return prev_close
        except Exception:
            pass
        return 0.0

    def _prune_if_exceeded(self):
        """清理超限条目：按 timestamp 清理最旧的 100 条"""
        if len(self._prices) <= self._max_size:
            return 0

        # 按时间戳排序，清理最旧的
        entries = [(code, entry.timestamp) for code, entry in self._prices.items()]
        entries.sort(key=lambda x: x[1])  # 升序

        excess = len(self._prices) - self._max_size + 50  # 多清理 50 条，留缓冲空间
        removed = 0
        for i in range(min(excess, len(entries))):
            code = entries[i][0]
            del self._prices[code]
            removed += 1

        get_logger("price_cache").log_event(
            "cache_prune",
            f"PriceCache 容量超限，清理 {removed} 条最旧条目",
            before=len(self._prices) + removed, after=len(self._prices), max_size=self._max_size
        )
        return removed

    def update(self, stock_code: str, price: float,
               open: float = 0, high: float = 0, low: float = 0,
               close: float = 0, volume: float = 0, amount: float = 0,
               change_pct: float = 0.0):
        """更新单只股票行情（线程安全）

        保护机制：当新数据某字段为 0 但缓存中已有有效值时，保留旧值。
        容量管理：新条目添加后检查是否超限，超出则清理最旧条目。
        """
        with self._lock:
            existing = self._prices.get(stock_code)
            if existing:
                open = open if open > 0 else existing.open
                high = high if high > 0 else existing.high
                low = low if low > 0 else existing.low
                close = close if close > 0 else existing.close
                volume = volume if volume > 0 else existing.volume
                amount = amount if amount > 0 else existing.amount
            self._prices[stock_code] = PriceEntry(
                price=close, open=open, high=high, low=low,
                close=close, volume=volume, amount=amount, change_pct=change_pct
            )
            # 新条目添加后检查容量
            if not existing:
                self._prune_if_exceeded()

    def update_ohlcv(self, stock_code: str,
                     open: float, high: float, low: float, close: float,
                     volume: float, amount: float, change_pct: float = 0.0,
                     prev_close: float = 0.0, source: str = ""):
        """更新完整 OHLCV 行情（线程安全）

        保护机制：当新数据某字段为 0 但缓存中已有有效值时，保留旧值，避免用 0 覆盖。
        source 优先级：snapshot > kline > kline_db。新数据 source 优先级低于已有数据时保留旧 source。
        容量管理：新条目添加后检查是否超限。
        自动计算涨跌幅：如果 change_pct=0 但有 prev_close，自动计算。
        新增股票时自动从 kline.db 补充 prev_close。
        """
        _source_rank = {"snapshot": 3, "kline": 2, "kline_db": 1}
        with self._lock:
            existing = self._prices.get(stock_code)
            if existing:
                # 用新值覆盖，但 0 值回退到旧值
                open = open if open > 0 else existing.open
                high = high if high > 0 else existing.high
                low = low if low > 0 else existing.low
                close = close if close > 0 else existing.close
                volume = volume if volume > 0 else existing.volume
                amount = amount if amount > 0 else existing.amount
                # prev_close: 新值 > 0 用新值，否则用旧值或从数据库补充
                if prev_close == 0:
                    prev_close = existing.prev_close
                    if prev_close == 0:
                        # 缓存中也没有，从数据库补充
                        prev_close = self._load_prev_close_from_db(stock_code)
                # source 优先级保护：低优先级不能覆盖高优先级
                if _source_rank.get(source, 0) < _source_rank.get(existing.source, 0):
                    source = existing.source
            else:
                # 新增股票：自动从 kline.db 补充 prev_close
                if prev_close == 0:
                    prev_close = self._load_prev_close_from_db(stock_code)

            # 自动计算涨跌幅
            if change_pct == 0 and prev_close > 0 and close > 0:
                change_pct = (close - prev_close) / prev_close * 100

            self._prices[stock_code] = PriceEntry(
                price=close, open=open, high=high, low=low,
                close=close, volume=volume, amount=amount, change_pct=change_pct,
                prev_close=prev_close, source=source,
            )
            # 新条目添加后检查容量
            if not existing:
                self._prune_if_exceeded()

    def update_batch(self, data: Dict[str, dict]):
        """批量更新行情
        data: {stock_code: OHLCV_dict}
        OHLCV_dict 需包含: close(或 price), open, high, low, volume, amount, change_pct

        保护机制：当新数据某字段为 0 但缓存中已有有效值时，保留旧值。
        容量管理：批量添加后检查是否超限。
        """
        with self._lock:
            new_count = 0
            for code, item in data.items():
                existing = self._prices.get(code)
                if isinstance(item, dict):
                    close_val = item.get('close', item.get('price', 0))
                    open_val = item.get('open', 0)
                    high_val = item.get('high', 0)
                    low_val = item.get('low', 0)
                    vol_val = item.get('volume', 0)
                    amt_val = item.get('amount', 0)
                    if existing:
                        open_val = open_val if open_val > 0 else existing.open
                        high_val = high_val if high_val > 0 else existing.high
                        low_val = low_val if low_val > 0 else existing.low
                        close_val = close_val if close_val > 0 else existing.close
                        vol_val = vol_val if vol_val > 0 else existing.volume
                        amt_val = amt_val if amt_val > 0 else existing.amount
                    self._prices[code] = PriceEntry(
                        price=close_val,
                        open=open_val, high=high_val, low=low_val,
                        close=close_val, volume=vol_val, amount=amt_val,
                        change_pct=item.get('change_pct', 0)
                    )
                    if not existing:
                        new_count += 1
                elif isinstance(item, tuple) and len(item) >= 2:
                    price, change_pct = item[0], item[1]
                    if existing and existing.close > 0:
                        # 仅更新 price/change_pct，保留已有完整 OHLCV
                        existing.price = price
                        existing.change_pct = change_pct
                        existing.timestamp = time.time()
                    else:
                        self._prices[code] = PriceEntry(price, change_pct=change_pct)
                        new_count += 1
            # 批量添加后检查容量
            if new_count > 0:
                self._prune_if_exceeded()

    def get(self, stock_code: str) -> Optional[float]:
        """获取最新价，未找到或过期返回 None"""
        with self._lock:
            entry = self._prices.get(stock_code)
            if not entry:
                return None
            if time.time() - entry.timestamp > self._ttl:
                return None
            return entry.price

    def is_expired(self, stock_code: str, max_age: int = None) -> bool:
        """判断某股票缓存是否过期"""
        with self._lock:
            entry = self._prices.get(stock_code)
            if not entry:
                return True
            age = time.time() - entry.timestamp
            return age > (max_age or self._ttl)

    def get_batch_freshness(self, codes: Set[str], max_age: int = None) -> Dict[str, bool]:
        """返回 {stock_code: is_fresh} 映射"""
        with self._lock:
            now = time.time()
            limit = max_age or self._ttl
            result = {}
            for code in codes:
                entry = self._prices.get(code)
                result[code] = entry is not None and (now - entry.timestamp <= limit)
            return result

    def get_ohlcv(self, stock_code: str, max_age: int = None) -> Optional[Dict[str, float]]:
        """获取完整 OHLCV 行情，未找到或过期返回 None
        Returns: {open, high, low, close, volume, amount, change_pct, prev_close}
        """
        with self._lock:
            entry = self._prices.get(stock_code)
            if not entry:
                return None
            if time.time() - entry.timestamp > (max_age or self._ttl):
                return None
            return {
                'open': entry.open, 'high': entry.high, 'low': entry.low,
                'close': entry.close, 'volume': entry.volume, 'amount': entry.amount,
                'change_pct': entry.change_pct, 'prev_close': entry.prev_close,
            }

    def get_ohlcv_with_ttl(self, stock_code: str, max_age: int = None) -> Optional[Dict[str, Any]]:
        """获取 OHLCV + 新鲜度标记
        Returns: {'data': {...}, 'is_fresh': bool, 'source': str} 或 None（缓存不存在）
        """
        with self._lock:
            entry = self._prices.get(stock_code)
            if not entry:
                return None
            now = time.time()
            limit = max_age or self._ttl
            is_fresh = (now - entry.timestamp) <= limit
            return {
                'data': {
                    'open': entry.open, 'high': entry.high, 'low': entry.low,
                    'close': entry.close, 'volume': entry.volume, 'amount': entry.amount,
                    'change_pct': entry.change_pct, 'prev_close': entry.prev_close,
                },
                'is_fresh': is_fresh,
                'source': entry.source or "",
            }

    def get_all(self, max_age: int = None) -> Dict[str, Dict[str, float]]:
        """获取所有股票的完整 OHLCV 行情
        Returns: {stock_code: {open, high, low, close, volume, amount, change_pct, prev_close}}
        """
        with self._lock:
            result = {}
            now = time.time()
            limit = max_age or self._ttl
            for code, entry in self._prices.items():
                if now - entry.timestamp <= limit:
                    result[code] = {
                        'open': entry.open, 'high': entry.high, 'low': entry.low,
                        'close': entry.close, 'volume': entry.volume, 'amount': entry.amount,
                        'change_pct': entry.change_pct, 'prev_close': entry.prev_close,
                    }
            return result

    def get_all_for_codes(self, codes: Set[str], max_age: int = None) -> Dict[str, Dict[str, float]]:
        """获取指定股票列表的 OHLCV 行情（过滤过期条目）
        Returns: {stock_code: {open, high, low, close, volume, amount, change_pct, prev_close, source}}
        """
        with self._lock:
            result = {}
            now = time.time()
            limit = max_age or self._ttl
            for code in codes:
                entry = self._prices.get(code)
                if entry and now - entry.timestamp <= limit:
                    result[code] = {
                        'open': entry.open, 'high': entry.high, 'low': entry.low,
                        'close': entry.close, 'volume': entry.volume, 'amount': entry.amount,
                        'change_pct': entry.change_pct,
                        'source': entry.source or "",
                    }
            return result

    def get_all_prices(self, max_age: int = None) -> Dict[str, float]:
        """获取所有股票最新价 {stock_code: price}（兼容旧接口）"""
        with self._lock:
            result = {}
            now = time.time()
            limit = max_age or self._ttl
            for code, entry in self._prices.items():
                if now - entry.timestamp <= limit:
                    result[code] = entry.price
            return result

    def get_fresh_count(self, codes: Set[str], max_age: int = 60) -> int:
        """统计指定股票中缓存新鲜的数量（max_age 秒内）"""
        with self._lock:
            now = time.time()
            count = 0
            for code in codes:
                entry = self._prices.get(code)
                if entry and now - entry.timestamp <= max_age:
                    count += 1
            return count

    def should_flush(self) -> bool:
        """是否需要刷盘"""
        return time.time() - self._last_flush >= self._flush_interval

    def mark_flushed(self):
        """标记已刷盘"""
        self._last_flush = time.time()

    def set_ttl(self, seconds: int):
        """动态调整缓存过期时间（秒）"""
        self._ttl = max(seconds, 60)  # 最低 60 秒

    def get_stats(self) -> Dict[str, Any]:
        """获取缓存统计（含容量上限信息）"""
        with self._lock:
            now = time.time()
            entries = len(self._prices)
            # 统计新鲜条目（在 TTL 内）
            valid_count = sum(1 for e in self._prices.values() if now - e.timestamp <= self._ttl)
            return {
                "cache_total": entries,  # 总条目数
                "cache_valid": valid_count,  # 新鲜条目数（TTL内）
                "max_size": self._max_size,
                "usage_pct": round(entries / self._max_size * 100, 1) if self._max_size > 0 else 0,
                "last_flush_age_seconds": round(time.time() - self._last_flush, 0),
                "ttl_seconds": self._ttl,
            }

    def is_tradable(self, stock_code: str, max_age: int = None) -> bool:
        """判断缓存数据是否可用于交易决策

        规则：source 必须为 'snapshot' 或 'kline'（SDK 实时源），且未过期。
        kline_db 兜底数据不可用于交易决策。
        """
        with self._lock:
            entry = self._prices.get(stock_code)
            if not entry:
                return False
            if entry.source not in ("snapshot", "kline"):
                return False
            now = time.time()
            return (now - entry.timestamp) <= (max_age or self._ttl)


# 全局单例
_price_cache: Optional[PriceCache] = None
_cache_lock = threading.Lock()


def get_price_cache() -> PriceCache:
    """获取价格缓存单例"""
    global _price_cache
    if _price_cache is None:
        with _cache_lock:
            if _price_cache is None:
                _price_cache = PriceCache()
    return _price_cache


def reset_price_cache():
    """重置（用于测试）"""
    global _price_cache
    _price_cache = None
