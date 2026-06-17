"""
行情数据服务 — 单只/批量行情查询、缓存、多通道并发、kline.db 兜底。
"""
import asyncio
import logging
from typing import Optional, Dict, Any, List, Set

from services.trading.models import MarketData

logger = logging.getLogger(__name__)


class MarketDataService:
    """行情数据服务：优先级 PriceCache → 并发(SDK + 备用) → kline.db 兜底"""

    def __init__(self):
        pass

    @staticmethod
    async def _fill_stale_from_kline_db(codes: Set[str]) -> Dict[str, MarketData]:
        """从 kline.db 读取最新收盘价兜底（仅非交易时段），计算涨跌幅"""
        from services.trading.trading_hours import can_trade
        if can_trade():
            return {}
        from services.common.database import get_sync_connection

        results: Dict[str, MarketData] = {}
        try:
            conn = get_sync_connection("kline")
            cursor = conn.cursor()
            placeholders = ','.join(['?'] * len(codes))
            # 获取最新两天数据以计算涨跌幅
            cursor.execute(f"""
                SELECT k.stock_code, k.close, k.open, k.high, k.low, k.volume, k.amount, k.trade_date,
                       k2.close as prev_close
                FROM kline_data k
                INNER JOIN (
                    SELECT stock_code, MAX(trade_date) as max_date
                    FROM kline_data WHERE stock_code IN ({placeholders})
                    GROUP BY stock_code
                ) latest ON k.stock_code = latest.stock_code AND k.trade_date = latest.max_date
                LEFT JOIN kline_data k2 ON k.stock_code = k2.stock_code
                    AND k2.trade_date = (
                        SELECT MAX(trade_date) FROM kline_data WHERE stock_code = k.stock_code AND trade_date < k.trade_date
                    )
            """, list(codes))
            for row in cursor.fetchall():
                code = row['stock_code']
                close = float(row['close'] or 0)
                prev_close = float(row['prev_close'] or close)
                # 计算涨跌幅
                change_pct = 0.0
                if prev_close > 0 and close > 0:
                    change_pct = (close - prev_close) / prev_close * 100
                if close > 0:
                    results[code] = MarketData(
                        stock_code=code, stock_name=code, current_price=close,
                        change_percent=change_pct, high=float(row['high'] or close),
                        low=float(row['low'] or close), open_price=float(row['open'] or close),
                        prev_close=prev_close, volume=int(row['volume'] or 0),
                        amount=float(row['amount'] or 0),
                        bid=[close] * 5, ask=[close] * 5,
                        bid_volume=[0] * 5, ask_volume=[0] * 5,
                        trade_date=str(row['trade_date'] or '').replace('-', ''),
                        source="kline_db",
                    )
        except Exception as e:
            logger.debug(f"kline.db 兜底查询失败: {e}")
        return results

    async def get_market_data(self, stock_code: str, connected: bool) -> Optional[MarketData]:
        """获取真实行情数据 — 优先读 PriceCache，未命中则并发走 SDK + 备用通道"""
        if not connected:
            raise Exception("网关未连接")

        from services.common.stock_code import normalize_stock_code
        norm_code = normalize_stock_code(stock_code)

        # ① 优先从 PriceCache 读取（带 TTL 检查）
        try:
            from services.common.price_cache import get_price_cache
            cache = get_price_cache()
            entry = cache.get_ohlcv_with_ttl(norm_code)
            if entry and entry.get('is_fresh') and entry.get('data', {}).get('close', 0) > 0:
                return MarketData.from_ohlcv(entry['data'], stock_code)
        except Exception:
            pass

        # 【优化】缓存过期时加入热点池，让 monitor 下次刷新覆盖
        try:
            from services.common.hot_stock_pool import get_hot_stock_pool
            hot_pool = get_hot_stock_pool()
            hot_pool.add(norm_code)
        except Exception:
            pass

        # ② 缓存未命中 → 并发：SDK + 备用通道，先赢者胜
        md = await self._race_market_data(norm_code)
        if md and md.current_price > 0:
            return md

        # ③ kline.db 最终兜底（仅非交易时段）
        try:
            kline_results = await self._fill_stale_from_kline_db({stock_code})
            if stock_code in kline_results:
                return kline_results[stock_code]
        except Exception:
            pass

        logger.error(f"获取行情失败：{stock_code}")
        raise Exception(f"获取行情失败：所有数据源均不可用")

    async def _race_market_data(self, stock_code: str) -> Optional[MarketData]:
        """并发获取行情：SDK 与备用 Provider 同时启动，先赢者胜"""
        sdk_result: Optional[MarketData] = None
        fallback_result: Optional[Dict[str, Any]] = None

        async def try_sdk():
            nonlocal sdk_result
            try:
                from services.trading.gateway_dispatcher import get_gateway_dispatcher
                dispatcher = get_gateway_dispatcher()
                dispatcher.subscribe("_single_race", {stock_code}, interval=0, priority=1)
                results = await dispatcher.refresh_now("_single_race")
                dispatcher.unsubscribe("_single_race")
                md = results.get(stock_code)
                if md and md.current_price > 0:
                    sdk_result = md
            except Exception as e:
                logger.debug(f"SDK 行情查询失败: {e}")

        async def try_fallback():
            nonlocal fallback_result
            try:
                from services.data.channel import get_channel_router, ChannelType
                router = get_channel_router()
                raw_data = await router.execute(
                    ChannelType.TRADING, "get_market_data", stock_code=stock_code,
                )
                if raw_data and raw_data.get("current_price", 0) > 0:
                    fallback_result = raw_data
            except Exception:
                pass

        await asyncio.gather(try_sdk(), try_fallback(), return_exceptions=True)

        if sdk_result:
            return sdk_result
        if fallback_result:
            return MarketData.from_fallback(fallback_result, stock_code)
        return None

    async def get_batch_market_data(self, stock_codes: List[str], connected: bool) -> Dict[str, Optional[MarketData]]:
        """批量获取行情数据 — 优先读 PriceCache，过期/缺失的并发走 SDK + 备用通道"""
        if not stock_codes:
            return {}

        from services.common.price_cache import get_price_cache
        from services.common.stock_code import normalize_stock_code

        cache = get_price_cache()
        results: Dict[str, Optional[MarketData]] = {}
        stale_codes: Set[str] = set()

        # ① 检查 freshness
        for code in stock_codes:
            norm = normalize_stock_code(code)
            entry = cache.get_ohlcv_with_ttl(norm)
            if entry and entry.get('is_fresh') and entry.get('data', {}).get('close', 0) > 0:
                results[code] = MarketData.from_ohlcv(entry['data'], code)
            else:
                stale_codes.add(norm)

        # 【优化】缓存过期的股票加入热点池，让 monitor 下次刷新覆盖
        if stale_codes:
            try:
                from services.common.hot_stock_pool import get_hot_stock_pool
                hot_pool = get_hot_stock_pool()
                for code in stale_codes:
                    hot_pool.add(code)
            except Exception:
                pass

        # ② 并发 SDK + 备用通道
        if stale_codes:
            sdk_results: Dict[str, Optional[MarketData]] = {}
            fallback_results: Dict[str, Optional[Dict[str, Any]]] = {}

            async def try_sdk_batch():
                nonlocal sdk_results
                try:
                    from services.trading.gateway_dispatcher import get_gateway_dispatcher
                    dispatcher = get_gateway_dispatcher()
                    sub_id = "_batch_race"
                    dispatcher.subscribe(sub_id, stale_codes, interval=0, priority=1)
                    sdk_results = await dispatcher.refresh_now(sub_id)
                    dispatcher.unsubscribe(sub_id)
                except Exception as e:
                    logger.debug(f"SDK 批量行情查询失败: {e}")

            async def try_fallback_batch():
                nonlocal fallback_results
                try:
                    from services.data.channel import get_channel_router, ChannelType
                    router = get_channel_router()
                    fallback_results = await router.execute(
                        ChannelType.TRADING, "get_batch_market_data",
                        stock_codes=list(stale_codes),
                    )
                except Exception as e:
                    logger.debug(f"备用通道批量行情查询失败: {e}")

            await asyncio.gather(try_sdk_batch(), try_fallback_batch(), return_exceptions=True)

            for code in stock_codes:
                norm = normalize_stock_code(code)
                if code in results:
                    continue
                md = sdk_results.get(norm) or sdk_results.get(code)
                if md and md.current_price > 0:
                    results[code] = md
                else:
                    fb = fallback_results.get(norm) or fallback_results.get(code)
                    if fb and fb.get("current_price", 0) > 0:
                        results[code] = MarketData.from_fallback(fb, code)
                    else:
                        results[code] = None

        # ③ kline.db 兜底
        still_missing = {
            normalize_stock_code(code)
            for code in stock_codes if code not in results or results[code] is None
        }
        if still_missing:
            kline_results = await self._fill_stale_from_kline_db(still_missing)
            for code in stock_codes:
                norm = normalize_stock_code(code)
                if (code not in results or results[code] is None) and norm in kline_results:
                    results[code] = kline_results[norm]

        return results
