"""
K 线数据服务 — 单只/批量 K 线查询、SDK 查询封装、日期计算、周期映射。
"""
import asyncio
import logging
from datetime import timedelta
from typing import Optional, Dict, Any, List

from services.common.timezone import get_china_time

logger = logging.getLogger(__name__)


class KlineDataService:
    """K 线数据服务：通过 SDKManager 查询 + ChannelRouter 回退"""

    def __init__(self, sdk_available: bool, constant=None):
        self.sdk_available = sdk_available
        self.constant = constant

    def _query_kline_via_sdk(self, code_list: list, begin_date: int, end_date: int, period: int, task_type: str = "query") -> dict:
        """通过 SDKManager 查询K线数据（自动排队）"""
        from services.common.sdk_manager import get_sdk_manager
        sdk_mgr = get_sdk_manager()
        return sdk_mgr.query_kline(code_list=code_list, begin_date=begin_date,
                                   end_date=end_date, period=period, task_type=task_type)

    async def get_kline_data(
        self, stock_code: str, period: str = "day",
        start_date: Optional[str] = None, end_date: Optional[str] = None,
        limit: int = 100, task_type: str = "query", connected: bool = True
    ) -> List[Dict[str, Any]]:
        """获取 K 线历史数据 — SDK 优先，失败回退 ChannelRouter"""
        if not connected:
            raise Exception("网关未连接")

        # 第一优先：SDK 路径
        try:
            result = await asyncio.wait_for(
                asyncio.to_thread(
                    self._query_kline_data_sync,
                    stock_code, period, start_date, end_date, limit, task_type
                ),
                timeout=10.0
            )
            if result:
                return result
        except asyncio.TimeoutError:
            logger.warning(f"查询 {stock_code} K线超时（>10s）")
        except Exception as e:
            logger.warning(f"AmazingData K线查询失败，尝试备用通道: {e}")

        # 回退：ChannelRouter
        try:
            from services.data.channel import get_channel_router, ChannelType
            router = get_channel_router()
            return await router.execute(
                ChannelType.MARKET_DATA, "get_kline_data",
                stock_code=stock_code, period=period,
                start_date=start_date or "", end_date=end_date or "", limit=limit,
            )
        except Exception as e:
            logger.error(f"所有数据源 K 线查询失败: {e}")
            raise Exception("获取 K 线数据失败：所有数据源均不可用")

    async def get_batch_kline_data(
        self, stock_codes: List[str], period: str = "day",
        start_date: Optional[str] = None, end_date: Optional[str] = None,
        limit: int = 100, task_type: str = "query", connected: bool = True
    ) -> Dict[str, Any]:
        """批量获取 K 线历史数据 — 通过 SDKManager（自带排队 + 超时）"""
        if not connected:
            raise Exception("网关未连接")

        count = len(stock_codes) if stock_codes else 1
        if task_type == "download":
            batch_timeout = min(count * 0.36, 180.0)
            batch_timeout = max(batch_timeout, 30.0)
        elif count <= 5:
            batch_timeout = 10.0
        elif count <= 20:
            batch_timeout = 20.0
        elif count <= 100:
            batch_timeout = 60.0
        else:
            batch_timeout = 120.0

        try:
            thread_call = asyncio.to_thread(
                self._query_batch_kline_data_sync,
                stock_codes, period, start_date, end_date, limit, task_type
            )
            return await asyncio.wait_for(thread_call, timeout=batch_timeout)
        except asyncio.TimeoutError:
            logger.warning(f"批量查询 K 线超时（{count} 只股票，>{batch_timeout:.0f}s）")
            raise Exception(f"获取 K 线数据超时（{count} 只股票，超时 {batch_timeout:.0f}s）")

    def _query_kline_data_sync(
        self, stock_code: str, period: str = "day",
        start_date: Optional[str] = None, end_date: Optional[str] = None,
        limit: int = 100, task_type: str = "query"
    ) -> List[Dict[str, Any]]:
        """同步查询 K 线数据（通过 SDKManager）"""
        import datetime as dt
        import pandas as pd

        if end_date:
            end_dt = dt.datetime.strptime(end_date, "%Y%m%d")
        else:
            end_dt = get_china_time()

        if start_date:
            start_dt = dt.datetime.strptime(start_date, "%Y%m%d")
        else:
            if period in ["1m", "3m", "5m", "10m", "15m", "30m", "60m", "120m"]:
                start_dt = end_dt - dt.timedelta(days=limit // 240)
            elif period == "day":
                start_dt = end_dt - dt.timedelta(days=limit)
            elif period == "week":
                start_dt = end_dt - dt.timedelta(weeks=limit)
            elif period == "month":
                start_dt = end_dt - dt.timedelta(days=limit * 30)
            else:
                start_dt = end_dt - dt.timedelta(days=limit)

        if '.' not in stock_code:
            stock_code = f"{stock_code}.{'SH' if stock_code.startswith('6') else 'SZ'}"

        period_map = {
            "1m": self.constant.Period.min1.value, "3m": self.constant.Period.min3.value,
            "5m": self.constant.Period.min5.value, "10m": self.constant.Period.min10.value,
            "15m": self.constant.Period.min15.value, "30m": self.constant.Period.min30.value,
            "60m": self.constant.Period.min60.value, "120m": self.constant.Period.min120.value,
            "day": self.constant.Period.day.value, "week": self.constant.Period.week.value,
            "month": self.constant.Period.month.value
        }
        actual_period = period_map.get(period, self.constant.Period.day.value)

        kline_data = self._query_kline_via_sdk(
            code_list=[stock_code],
            begin_date=int(start_dt.strftime("%Y%m%d")),
            end_date=int((end_dt + timedelta(days=1)).strftime("%Y%m%d")),
            period=actual_period, task_type="query"
        )

        if kline_data and stock_code in kline_data:
            df = kline_data[stock_code]
            if df is not None and len(df) > 0:
                if len(df) > limit:
                    df = df.tail(limit)
                if 'kline_time' in df.columns:
                    df = df.rename(columns={'kline_time': 'trade_date'})
                if 'trade_date' in df.columns:
                    df['trade_date'] = df['trade_date'].apply(
                        lambda x: x.strftime('%Y-%m-%d') if pd.notnull(x) else ''
                    )
                return [{
                    "stock_code": stock_code,
                    "trade_date": str(row.get("trade_date", "")),
                    "open": float(row.get("open", 0)),
                    "high": float(row.get("high", 0)),
                    "low": float(row.get("low", 0)),
                    "close": float(row.get("close", 0)),
                    "volume": int(row.get("volume", 0)),
                    "amount": float(row.get("amount", 0))
                } for _, row in df.iterrows()]

        return []

    def _query_batch_kline_data_sync(
        self, stock_codes: List[str], period: str = "day",
        start_date: Optional[str] = None, end_date: Optional[str] = None,
        limit: int = 100, task_type: str = "query"
    ) -> Dict[str, Any]:
        """同步批量查询 K 线数据（通过 SDKManager）"""
        import datetime as dt
        import pandas as pd

        if end_date:
            end_date_str = str(end_date) if not isinstance(end_date, str) else end_date
            end_dt = dt.datetime.strptime(end_date_str, "%Y%m%d")
        else:
            end_dt = get_china_time()

        if start_date:
            start_date_str = str(start_date) if not isinstance(start_date, str) else start_date
            start_dt = dt.datetime.strptime(start_date_str, "%Y%m%d")
        else:
            start_dt = end_dt - dt.timedelta(days=limit) if period == "day" else end_dt - dt.timedelta(days=limit * 30)

        period_map = {
            "1m": self.constant.Period.min1.value, "3m": self.constant.Period.min3.value,
            "5m": self.constant.Period.min5.value, "10m": self.constant.Period.min10.value,
            "15m": self.constant.Period.min15.value, "30m": self.constant.Period.min30.value,
            "60m": self.constant.Period.min60.value, "120m": self.constant.Period.min120.value,
            "day": self.constant.Period.day.value, "week": self.constant.Period.week.value,
            "month": self.constant.Period.month.value,
        }
        period_value = period_map.get(period, self.constant.Period.day.value)

        begin_date_int = int(start_dt.strftime('%Y%m%d'))
        end_date_int = int((end_dt + timedelta(days=1)).strftime('%Y%m%d'))

        kline_data = self._query_kline_via_sdk(
            code_list=stock_codes, begin_date=begin_date_int,
            end_date=end_date_int, period=period_value, task_type=task_type
        )

        if kline_data:
            for code, df in kline_data.items():
                if df is not None and isinstance(df, pd.DataFrame) and 'kline_time' in df.columns:
                    df = df.rename(columns={'kline_time': 'trade_date'})
                    if 'trade_date' in df.columns:
                        df['trade_date'] = df['trade_date'].apply(
                            lambda x: x.strftime('%Y-%m-%d') if pd.notnull(x) else ''
                        )
                    kline_data[code] = df

        return kline_data or {}
