"""
实时价格缓存服务

将 SDK 实时行情缓存在内存中，避免频繁写入数据库。
- 监控循环只更新内存缓存，不写 DB
- API 返回时从缓存注入 current_price
- 每 15 分钟刷盘兜底
"""

import time
import threading
from typing import Dict, Optional, Any
from services.common.timezone import get_china_time
from services.common.structured_logger import get_logger

# 数据格式
class PriceEntry:
    __slots__ = ('price', 'timestamp', 'change_pct')

    def __init__(self, price: float, change_pct: float = 0.0):
        self.price = price
        self.timestamp = time.time()
        self.change_pct = change_pct


class PriceCache:
    """线程安全的内存价格缓存"""

    def __init__(self):
        self._lock = threading.Lock()
        # {account_id: {stock_code: PriceEntry}}
        self._prices: Dict[str, Dict[str, PriceEntry]] = {}
        self._flush_interval = 900  # 15 分钟
        self._last_flush = time.time()

    def update(self, account_id: str, stock_code: str, price: float, change_pct: float = 0.0):
        """更新单只股票价格（线程安全）"""
        with self._lock:
            if account_id not in self._prices:
                self._prices[account_id] = {}
            self._prices[account_id][stock_code] = PriceEntry(price, change_pct)

    def update_batch(self, account_id: str, data: Dict[str, tuple]):
        """批量更新价格
        data: {stock_code: (price, change_pct)}
        """
        with self._lock:
            if account_id not in self._prices:
                self._prices[account_id] = {}
            for code, (price, change_pct) in data.items():
                self._prices[account_id][code] = PriceEntry(price, change_pct)

    def get(self, account_id: str, stock_code: str) -> Optional[float]:
        """获取价格，未找到或过期返回 None"""
        with self._lock:
            acct = self._prices.get(account_id)
            if not acct:
                return None
            entry = acct.get(stock_code)
            if not entry:
                return None
            # 超过 10 分钟认为数据过期
            if time.time() - entry.timestamp > 600:
                return None
            return entry.price

    def get_all_for_account(self, account_id: str) -> Dict[str, float]:
        """获取某账户所有股票的实时价格
        Returns: {stock_code: price}
        """
        with self._lock:
            acct = self._prices.get(account_id, {})
            result = {}
            now = time.time()
            for code, entry in acct.items():
                if now - entry.timestamp <= 600:  # 10 分钟内有效
                    result[code] = entry.price
            return result

    def should_flush(self) -> bool:
        """是否需要刷盘"""
        return time.time() - self._last_flush >= self._flush_interval

    def mark_flushed(self):
        """标记已刷盘"""
        self._last_flush = time.time()

    def get_stats(self) -> Dict[str, Any]:
        """获取缓存统计"""
        with self._lock:
            total = sum(len(v) for v in self._prices.values())
            return {
                "accounts": len(self._prices),
                "total_entries": total,
                "last_flush_age_seconds": round(time.time() - self._last_flush, 0),
            }


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
