"""
热点股票池 — 动态扩展 monitor 刷新覆盖范围

场景：
- 用户临时查看某股票（可能未加入 watchlist）
- 用户刚加入 watchlist 但 monitor 下次刷新还没覆盖

机制：
- 用户请求股票时，缓存过期则加入热点池
- monitor 刷新时，热点池股票加入 watch_only 分类
- TTL 过期自动移除（5分钟无人请求）
- 最大容量 200 只（临时查看场景足够）

效果：
- 减少用户对同一股票的重复 SDK 调用
- 热点股票自动进入 PriceCache
- 不影响 monitor 正常优先级逻辑
"""

import time
import threading
from typing import Dict, List, Optional, Set

from services.common.structured_logger import get_logger


class HotStockPool:
    """热点股票池：用户近期请求的股票"""

    _instance: Optional['HotStockPool'] = None
    _lock = threading.Lock()

    # 配置
    DEFAULT_TTL = 300  # 5分钟无人请求则移除
    DEFAULT_MAX_SIZE = 200  # 最大容量（临时查看场景足够）

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return
        self._initialized = True

        self._stocks: Dict[str, float] = {}  # code → last_request_time
        self._data_lock = threading.Lock()
        self._ttl = self.DEFAULT_TTL
        self._max_size = self.DEFAULT_MAX_SIZE
        self._logger = get_logger("hot_pool")

        # 统计
        self._total_added = 0
        self._total_expired = 0
        self._total_pruned = 0

    def add(self, stock_code: str) -> bool:
        """用户请求股票时加入热点池

        Returns:
            True: 新加入
            False: 已存在（仅更新时间）
        """
        # 规范化股票代码
        from services.common.stock_code import normalize_stock_code
        code = normalize_stock_code(stock_code)

        with self._data_lock:
            now = time.time()
            is_new = code not in self._stocks
            self._stocks[code] = now

            if is_new:
                self._total_added += 1
                # 超限时移除最旧的
                if len(self._stocks) > self._max_size:
                    self._prune_oldest()

            self._logger.debug(f"热点池: {code} {'新加入' if is_new else '更新时间'}, 当前 {len(self._stocks)} 只")
            return is_new

    def get_active_stocks(self) -> List[str]:
        """获取活跃股票列表（TTL 内有请求）

        同时清理过期股票。
        """
        with self._data_lock:
            now = time.time()
            active = []
            expired = []

            for code, ts in self._stocks.items():
                if now - ts < self._ttl:
                    active.append(code)
                else:
                    expired.append(code)

            # 清理过期
            for code in expired:
                del self._stocks[code]
                self._total_expired += 1

            if expired:
                self._logger.log_event("hot_pool_expired",
                    f"清理 {len(expired)} 只过期股票，剩余 {len(self._stocks)} 只")

            return active

    def get_stats(self) -> Dict:
        """获取统计信息"""
        with self._data_lock:
            now = time.time()
            active_count = sum(1 for ts in self._stocks.values()
                              if now - ts < self._ttl)
            return {
                "total_stocks": len(self._stocks),
                "active_stocks": active_count,
                "max_size": self._max_size,
                "ttl_seconds": self._ttl,
                "total_added": self._total_added,
                "total_expired": self._total_expired,
                "total_pruned": self._total_pruned,
            }

    def _prune_oldest(self):
        """移除最旧的股票（超限时）"""
        if not self._stocks:
            return

        # 找最旧的
        oldest_code = min(self._stocks.keys(),
                         key=lambda c: self._stocks[c])
        del self._stocks[oldest_code]
        self._total_pruned += 1

        self._logger.log_event("hot_pool_pruned",
            f"超限移除最旧股票 {oldest_code}，剩余 {len(self._stocks)} 只")

    def clear(self):
        """清空热点池（测试用）"""
        with self._data_lock:
            self._stocks.clear()

    def set_ttl(self, seconds: int):
        """设置 TTL（配置调整）"""
        self._ttl = max(seconds, 60)

    def set_max_size(self, size: int):
        """设置最大容量"""
        self._max_size = max(size, 50)


def get_hot_stock_pool() -> HotStockPool:
    """获取热点股票池单例"""
    return HotStockPool()