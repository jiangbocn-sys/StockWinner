"""
价格缓存管理器 — 价格缓存更新、DB 刷盘、持仓 PNL 刷新。

改造：使用数据库写入队列异步批量写入，避免锁竞争。
"""
from typing import Dict, Optional, Any
import asyncio

from services.common.database import get_db_manager
from services.common.price_cache import get_price_cache
from services.common.structured_logger import get_logger
from services.common.timezone import get_china_time


class PriceCacheManager:
    """价格缓存管理：内存价格更新 + 定时刷盘 + 持仓盈亏刷新"""

    def __init__(self):
        self._cache = get_price_cache()
        self._flush_lock = asyncio.Lock()  # 防止并发刷盘

    def update_price_cache(self, market_data_cache: Optional[Dict[str, Any]] = None):
        """将实时行情写入内存价格缓存（完整 OHLCV，不写 DB）"""
        if not market_data_cache:
            return

        count = 0
        none_count = 0
        for code, md in market_data_cache.items():
            if md is None:
                none_count += 1
                continue
            if md.current_price and md.current_price > 0:
                self._cache.update_ohlcv(
                    code,
                    open=getattr(md, 'open_price', 0) or getattr(md, 'open', 0) or md.current_price,
                    high=getattr(md, 'high', 0) or md.current_price,
                    low=getattr(md, 'low', 0) or md.current_price,
                    close=md.current_price,
                    volume=getattr(md, 'volume', 0) or 0,
                    amount=getattr(md, 'amount', 0) or 0,
                    change_pct=getattr(md, 'change_percent', 0) or 0.0,
                )
                count += 1
        if count > 0:
            get_logger("monitor").log_event("price_cache_update", f"price_cache 更新: stocks={count}, none={none_count}")
        elif none_count > 0:
            get_logger("monitor").log_event("price_cache_empty", f"price_cache 无有效数据: total={len(market_data_cache)}, none={none_count}")

    async def flush_to_db(self, account_id: str):
        """每 15 分钟将内存缓存的价格兜底写入数据库

        使用写入队列异步批量写入，避免锁竞争。
        """
        # 防止并发刷盘
        if self._flush_lock.locked():
            get_logger("monitor").log_event("price_flush_skip", "刷盘正在进行，跳过")
            return

        async with self._flush_lock:
            prices = self._cache.get_all_prices()
            if not prices:
                return

            from services.common.db_write_queue import get_db_write_queue
            write_queue = get_db_write_queue()

            wl_updates = []
            pos_updates = []
            for code, price in prices.items():
                wl_updates.append((price, get_china_time(), account_id, code))
                pos_updates.append((price, price, price, get_china_time(), account_id, code))

            if wl_updates:
                write_queue.execute_many_async(
                    "UPDATE watchlist SET current_price = ?, updated_at = ? WHERE account_id = ? AND stock_code = ?",
                    wl_updates,
                    callback=lambda count, err: get_logger("monitor").log_event(
                        "price_flush_callback",
                        f"watchlist 刷盘完成: {count} 条" if not err else f"watchlist 刷盘失败: {err}"
                    )
                )
                get_logger("monitor").log_event("price_flush_queued", f"已提交 {len(wl_updates)} 条 watchlist 现价写入队列")

            if pos_updates:
                write_queue.execute_many_async(
                    """UPDATE stock_positions
                       SET current_price = ?,
                           market_value = ? * quantity,
                           profit_loss = (? - avg_cost) * quantity,
                           updated_at = ?
                       WHERE account_id = ? AND stock_code = ?""",
                    pos_updates,
                    callback=lambda count, err: get_logger("monitor").log_event(
                        "position_flush_callback",
                        f"position 刷盘完成: {count} 条" if not err else f"position 刷盘失败: {err}"
                    )
                )
                get_logger("monitor").log_event("price_flush_queued", f"已提交 {len(pos_updates)} 条 position 盈亏写入队列")

            self._cache.mark_flushed()

    def should_flush(self) -> bool:
        """检查是否需要刷盘"""
        return self._cache.should_flush()

    async def refresh_positions_pnl(self, account_id: str, market_data_cache: Optional[Dict[str, Any]] = None):
        """刷新持仓盈亏（只更新内存缓存，不写 DB）"""
        db = get_db_manager()
        positions = await db.fetchall(
            "SELECT stock_code FROM stock_positions WHERE account_id = ?",
            (account_id,),
        )
        if not positions:
            return

        stock_codes = [p["stock_code"] for p in positions]
        if not market_data_cache:
            return

        # 更新内存价格缓存
        data = {}
        for code in stock_codes:
            md = market_data_cache.get(code)
            if md and md.current_price and md.current_price > 0:
                data[code] = (md.current_price, md.change_percent if hasattr(md, 'change_percent') and md.change_percent else 0.0)

        if data:
            self._cache.update_batch(data)
