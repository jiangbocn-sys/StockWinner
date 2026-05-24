"""
外部数据服务 — 行业/指数/财报/龙虎榜/两融/大宗/国债收益率。
"""
import asyncio
import math
import logging
from typing import Optional, Dict, Any, List

import numpy as np

logger = logging.getLogger(__name__)


class ExternalDataService:
    """外部数据查询服务：行业、指数、财务、特殊行情数据"""

    def __init__(self, sdk_available: bool, constant=None):
        self.sdk_available = sdk_available
        self.constant = constant

    @staticmethod
    def _sanitize_nan(obj):
        """将 NaN/Inf 转换为 None，避免 JSON 序列化失败"""
        if obj is None:
            return None
        if isinstance(obj, (np.floating, np.integer)):
            if np.isnan(obj) or np.isinf(obj):
                return None
            return obj.item()
        if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
            return None
        try:
            import pandas as pd
            if obj is pd.NA or obj is pd.NaT:
                return None
            if isinstance(obj, pd.Timestamp):
                return obj.strftime("%Y-%m-%d")
        except Exception:
            pass
        return obj

    def _records_from_df(self, df, stock_code: str) -> List[Dict[str, Any]]:
        """将 DataFrame 按 stock_code 过滤并转为 JSON records，清理 NaN"""
        if df.empty:
            return []
        df.columns = df.columns.str.lower()
        records = df[df["market_code"] == stock_code].to_dict(orient="records")
        for record in records:
            for key, value in record.items():
                record[key] = self._sanitize_nan(value)
        return records

    def _records_from_raw_df(self, df) -> List[Dict[str, Any]]:
        """将 DataFrame 转为 JSON records，清理 NaN"""
        if df.empty:
            return []
        df.columns = df.columns.str.lower()
        records = df.to_dict(orient="records")
        for record in records:
            for key, value in record.items():
                record[key] = self._sanitize_nan(value)
        return records

    async def get_code_info(self, security_type: str = 'EXTRA_STOCK_A', connected: bool = True) -> List[Dict[str, Any]]:
        if not connected:
            raise Exception("网关未连接")
        try:
            return await asyncio.to_thread(self._get_code_info_sync, security_type)
        except Exception as e:
            raise Exception(f"获取代码信息失败：{str(e)}")

    def _get_code_info_sync(self, security_type: str = 'EXTRA_STOCK_A') -> List[Dict[str, Any]]:
        from services.common.sdk_manager import get_sdk_manager
        sdk_mgr = get_sdk_manager()
        df = sdk_mgr.get_code_info(security_type=security_type)
        if df.empty:
            return []
        return [{
            "code": str(idx) if idx else '',
            "name": row.get('symbol', '') or str(idx) if idx else '',
            "market": str(idx).split('.')[-1] if '.' in str(idx) else ('SH' if str(idx).startswith('6') else 'SZ'),
        } for idx, row in df.iterrows()]

    async def get_industry_list(self, level: int = 1, connected: bool = True) -> List[Dict[str, Any]]:
        if not connected:
            raise Exception("网关未连接")
        try:
            return await asyncio.to_thread(self._get_industry_list_sync, level)
        except Exception as e:
            raise Exception(f"获取行业列表失败：{str(e)}")

    def _get_industry_list_sync(self, level: int = 1) -> List[Dict[str, Any]]:
        from services.common.sdk_manager import get_sdk_manager
        sdk_mgr = get_sdk_manager()
        df = sdk_mgr.get_industry_base_info()
        if df.empty:
            return []
        filtered = df[df["LEVEL_TYPE"] == level]
        records = filtered.to_dict(orient="records")
        for record in records:
            for key, value in record.items():
                record[key] = self._sanitize_nan(value)
        return records

    async def get_industry_kline(self, index_code: str, connected: bool = True) -> List[Dict[str, Any]]:
        if not connected:
            raise Exception("网关未连接")
        try:
            return await asyncio.to_thread(self._get_industry_kline_sync, index_code)
        except Exception as e:
            raise Exception(f"获取行业行情失败：{str(e)}")

    def _get_industry_kline_sync(self, index_code: str) -> List[Dict[str, Any]]:
        from services.common.sdk_manager import get_sdk_manager
        sdk_mgr = get_sdk_manager()
        result = sdk_mgr.get_industry_daily(code_list=[index_code])
        if not result or index_code not in result:
            return []
        df = result[index_code].reset_index()
        df.columns = df.columns.str.lower()
        if "trade_date" in df.columns:
            df["trade_date"] = df["trade_date"].apply(
                lambda x: x.strftime("%Y-%m-%d") if hasattr(x, "strftime") else str(x)[:10]
            )
        records = df.to_dict(orient="records")
        for record in records:
            for key, value in record.items():
                record[key] = self._sanitize_nan(value)
        return records

    async def get_industry_constituent(self, index_code: str, connected: bool = True) -> List[Dict[str, Any]]:
        if not connected:
            raise Exception("网关未连接")
        try:
            return await asyncio.to_thread(self._get_industry_constituent_sync, index_code)
        except Exception as e:
            raise Exception(f"获取行业成分股失败：{str(e)}")

    def _get_industry_constituent_sync(self, index_code: str) -> List[Dict[str, Any]]:
        from services.common.sdk_manager import get_sdk_manager
        sdk_mgr = get_sdk_manager()
        df = sdk_mgr.get_industry_constituent(index_codes=[index_code])
        return self._records_from_raw_df(df)

    async def get_index_constituent(self, index_code: str, connected: bool = True) -> List[Dict[str, Any]]:
        if not connected:
            raise Exception("网关未连接")
        try:
            return await asyncio.to_thread(self._get_index_constituent_sync, index_code)
        except Exception as e:
            raise Exception(f"获取指数成分股失败：{str(e)}")

    def _get_index_constituent_sync(self, index_code: str) -> List[Dict[str, Any]]:
        from services.common.sdk_manager import get_sdk_manager
        sdk_mgr = get_sdk_manager()
        df = sdk_mgr.get_index_constituent(index_codes=[index_code])
        return self._records_from_raw_df(df)

    async def get_income_statement(self, stock_code: str, connected: bool = True) -> List[Dict[str, Any]]:
        if not connected:
            raise Exception("网关未连接")
        try:
            return await asyncio.to_thread(self._get_income_statement_sync, stock_code)
        except Exception as e:
            raise Exception(f"获取利润表失败：{str(e)}")

    def _get_income_statement_sync(self, stock_code: str) -> List[Dict[str, Any]]:
        from services.common.sdk_manager import get_sdk_manager
        sdk_mgr = get_sdk_manager()
        df = sdk_mgr.get_income_statement(stock_codes=[stock_code])
        return self._records_from_df(df, stock_code)

    async def get_balance_sheet(self, stock_code: str, connected: bool = True) -> List[Dict[str, Any]]:
        if not connected:
            raise Exception("网关未连接")
        try:
            return await asyncio.to_thread(self._get_balance_sheet_sync, stock_code)
        except Exception as e:
            raise Exception(f"获取资产负债表失败：{str(e)}")

    def _get_balance_sheet_sync(self, stock_code: str) -> List[Dict[str, Any]]:
        from services.common.sdk_manager import get_sdk_manager
        sdk_mgr = get_sdk_manager()
        df = sdk_mgr.get_balance_sheet(stock_codes=[stock_code])
        return self._records_from_df(df, stock_code)

    async def get_cash_flow_statement(self, stock_code: str, connected: bool = True) -> List[Dict[str, Any]]:
        if not connected:
            raise Exception("网关未连接")
        try:
            return await asyncio.to_thread(self._get_cash_flow_statement_sync, stock_code)
        except Exception as e:
            raise Exception(f"获取现金流量表失败：{str(e)}")

    def _get_cash_flow_statement_sync(self, stock_code: str) -> List[Dict[str, Any]]:
        from services.common.sdk_manager import get_sdk_manager
        sdk_mgr = get_sdk_manager()
        df = sdk_mgr.get_cash_flow_statement(stock_codes=[stock_code])
        return self._records_from_df(df, stock_code)

    async def get_profit_notice(self, stock_code: str, connected: bool = True) -> List[Dict[str, Any]]:
        if not connected:
            raise Exception("网关未连接")
        try:
            return await asyncio.to_thread(self._get_profit_notice_sync, stock_code)
        except Exception as e:
            raise Exception(f"获取业绩预告失败：{str(e)}")

    def _get_profit_notice_sync(self, stock_code: str) -> List[Dict[str, Any]]:
        from services.common.sdk_manager import get_sdk_manager
        sdk_mgr = get_sdk_manager()
        df = sdk_mgr.get_profit_notice(stock_codes=[stock_code])
        return self._records_from_df(df, stock_code)

    async def get_profit_express(self, stock_code: str, connected: bool = True) -> List[Dict[str, Any]]:
        if not connected:
            raise Exception("网关未连接")
        try:
            return await asyncio.to_thread(self._get_profit_express_sync, stock_code)
        except Exception as e:
            raise Exception(f"获取业绩快报失败：{str(e)}")

    def _get_profit_express_sync(self, stock_code: str) -> List[Dict[str, Any]]:
        from services.common.sdk_manager import get_sdk_manager
        sdk_mgr = get_sdk_manager()
        df = sdk_mgr.get_profit_express(stock_codes=[stock_code])
        return self._records_from_df(df, stock_code)

    async def get_long_hu_bang(self, stock_code: str, begin_date: int, end_date: int, connected: bool = True) -> List[Dict[str, Any]]:
        if not connected:
            raise Exception("网关未连接")
        try:
            return await asyncio.to_thread(self._get_long_hu_bang_sync, stock_code, begin_date, end_date)
        except Exception as e:
            raise Exception(f"获取龙虎榜失败：{str(e)}")

    def _get_long_hu_bang_sync(self, stock_code: str, begin_date: int, end_date: int) -> List[Dict[str, Any]]:
        from services.common.sdk_manager import get_sdk_manager
        sdk_mgr = get_sdk_manager()
        df = sdk_mgr.get_long_hu_bang(stock_codes=[stock_code], begin_date=begin_date, end_date=end_date)
        return self._records_from_raw_df(df)

    async def get_margin_summary(self, begin_date: int, end_date: int, connected: bool = True) -> List[Dict[str, Any]]:
        if not connected:
            raise Exception("网关未连接")
        try:
            return await asyncio.to_thread(self._get_margin_summary_sync, begin_date, end_date)
        except Exception as e:
            raise Exception(f"获取两融汇总失败：{str(e)}")

    def _get_margin_summary_sync(self, begin_date: int, end_date: int) -> List[Dict[str, Any]]:
        from services.common.sdk_manager import get_sdk_manager
        sdk_mgr = get_sdk_manager()
        df = sdk_mgr.get_margin_summary(begin_date=begin_date, end_date=end_date)
        return self._records_from_raw_df(df)

    async def get_margin_detail(self, stock_code: str, begin_date: int, end_date: int, connected: bool = True) -> List[Dict[str, Any]]:
        if not connected:
            raise Exception("网关未连接")
        try:
            return await asyncio.to_thread(self._get_margin_detail_sync, stock_code, begin_date, end_date)
        except Exception as e:
            raise Exception(f"获取两融明细失败：{str(e)}")

    def _get_margin_detail_sync(self, stock_code: str, begin_date: int, end_date: int) -> List[Dict[str, Any]]:
        from services.common.sdk_manager import get_sdk_manager
        sdk_mgr = get_sdk_manager()
        df = sdk_mgr.get_margin_detail(stock_codes=[stock_code], begin_date=begin_date, end_date=end_date)
        return self._records_from_raw_df(df)

    async def get_block_trading(self, stock_code: str, begin_date: int, end_date: int, connected: bool = True) -> List[Dict[str, Any]]:
        if not connected:
            raise Exception("网关未连接")
        try:
            return await asyncio.to_thread(self._get_block_trading_sync, stock_code, begin_date, end_date)
        except Exception as e:
            raise Exception(f"获取大宗交易失败：{str(e)}")

    def _get_block_trading_sync(self, stock_code: str, begin_date: int, end_date: int) -> List[Dict[str, Any]]:
        from services.common.sdk_manager import get_sdk_manager
        sdk_mgr = get_sdk_manager()
        df = sdk_mgr.get_block_trading(stock_codes=[stock_code], begin_date=begin_date, end_date=end_date)
        return self._records_from_raw_df(df)

    async def get_treasury_yield(self, connected: bool = True) -> List[Dict[str, Any]]:
        if not connected:
            raise Exception("网关未连接")
        try:
            return await asyncio.to_thread(self._get_treasury_yield_sync)
        except Exception as e:
            raise Exception(f"获取国债收益率失败：{str(e)}")

    def _get_treasury_yield_sync(self) -> List[Dict[str, Any]]:
        from services.common.sdk_manager import get_sdk_manager
        sdk_mgr = get_sdk_manager()
        df = sdk_mgr.get_treasury_yield()
        return self._records_from_raw_df(df)
