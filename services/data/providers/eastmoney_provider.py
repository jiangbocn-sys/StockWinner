"""
东方财富数据源适配器

通过东方财富公开 HTTP API 获取行情数据，无需登录/Token。
使用 httpx 进行异步 HTTP 调用。
"""

import time
import httpx
from typing import Optional, Dict, Any, List

from services.data.providers.base import (
    DataProvider,
    ProviderInfo,
    ProviderCapabilities,
    DataProviderError,
)
from services.common.structured_logger import get_logger

logger = get_logger("data_provider")


class EastmoneyDataProvider(DataProvider):
    """东方财富 HTTP API 适配器"""

    # 东方财富 API 基础 URL
    KLINE_BASE_URL = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
    QUOTE_BASE_URL = "https://push2.eastmoney.com/api/qt/stock/get"
    STOCK_LIST_BASE_URL = "https://push2.eastmoney.com/api/qt/clist/get"

    def __init__(self):
        self._config: Dict[str, Any] = {}
        self._ready = False

    @property
    def provider_id(self) -> str:
        return "eastmoney"

    @property
    def info(self) -> ProviderInfo:
        return ProviderInfo(
            provider_id="eastmoney",
            display_name="东方财富",
            description="东方财富免费行情接口",
            capabilities=ProviderCapabilities(
                supports_kline=True,
                supports_realtime=True,
                supports_fundamentals=False,
                supports_stock_list=True,
                supports_industry=False,
                supports_trading=False,
                supports_realtime_snapshot=False,
            ),
            requires_config=False,
            is_built_in=True,
        )

    async def initialize(self, config: Dict[str, Any]) -> bool:
        self._config = config
        self._ready = True
        return self._ready

    async def health_check(self) -> Dict[str, Any]:
        start = time.monotonic()
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Referer": "https://quote.eastmoney.com/",
            }
            async with httpx.AsyncClient(timeout=10.0, headers=headers) as client:
                resp = await client.get(
                    self.QUOTE_BASE_URL,
                    params={"secid": "1.600000", "fields": "f43,f44,f45,f46,f47,f48,f57,f58,f60"},
                )
                resp.raise_for_status()
                data = resp.json()
                ok = data.get("data") is not None and data["data"].get("f43") is not None
                latency_ms = (time.monotonic() - start) * 1000
                return {
                    "ok": ok,
                    "message": "已连接" if ok else "接口返回空数据",
                    "latency_ms": round(latency_ms, 1),
                }
        except Exception as e:
            latency_ms = (time.monotonic() - start) * 1000
            return {
                "ok": False,
                "message": str(e),
                "latency_ms": round(latency_ms, 1),
            }

    # ============================================================
    # 内部辅助方法
    # ============================================================

    @staticmethod
    def _to_secid(stock_code: str) -> str:
        """将 600000.SH 格式转换为东方财富 secid 格式"""
        code = stock_code.upper()
        if code.endswith(".SH"):
            return f"1.{code[:-3]}"
        elif code.endswith(".SZ"):
            return f"0.{code[:-3]}"
        # 尝试根据代码前缀判断市场
        code_num = code.replace(".", "")
        if code_num.startswith("6"):
            return f"1.{code_num}"
        return f"0.{code_num}"

    @staticmethod
    def _to_kline_period(period: str) -> int:
        period_map = {
            "day": 101, "week": 102, "month": 103,
            "1m": 1, "3m": 2, "5m": 3, "10m": 4,
            "15m": 5, "30m": 6, "60m": 7, "120m": 8,
        }
        return period_map.get(period, 101)

    @staticmethod
    def _normalize_kline_records(klines: List[str], stock_code: str) -> List[Dict[str, Any]]:
        """解析 K 线字符串列表
        格式: "日期,开盘,最高,最低,收盘,涨跌幅,涨跌额,成交量,成交额,振幅,换手率"
        """
        output = []
        for line in klines:
            parts = line.split(",")
            if len(parts) < 9:
                continue
            try:
                rec = {
                    "stock_code": stock_code,
                    "trade_date": parts[0],
                    "open": float(parts[1]) if parts[1] != "-" else 0,
                    "high": float(parts[2]) if parts[2] != "-" else 0,
                    "low": float(parts[3]) if parts[3] != "-" else 0,
                    "close": float(parts[4]) if parts[4] != "-" else 0,
                    "volume": float(parts[7]) if parts[7] != "-" else 0,
                    "amount": float(parts[8]) if parts[8] != "-" else 0,
                }
                output.append(rec)
            except (ValueError, IndexError):
                continue
        return output

    @staticmethod
    def _normalize_market_data(data: Dict, stock_code: str) -> Optional[Dict[str, Any]]:
        """将东方财富行情数据转换为系统标准格式"""
        if not data:
            return None

        def fval(key, default=0.0):
            v = data.get(key)
            if v is None or v == "-":
                return default
            try:
                return float(v)
            except (ValueError, TypeError):
                return default

        price = fval("f43", 0)
        # 东方财富 f43 单位为分，需除以 100
        if price > 0:
            price = price / 100

        return {
            "stock_code": stock_code,
            "stock_name": data.get("f58", ""),
            "current_price": price,
            "change_percent": fval("f169", 0),  # 涨跌幅%
            "high": fval("f44", 0) / 100 if fval("f44", 0) > 0 else 0,
            "low": fval("f45", 0) / 100 if fval("f45", 0) > 0 else 0,
            "open_price": fval("f46", 0) / 100 if fval("f46", 0) > 0 else 0,
            "prev_close": fval("f60", 0) / 100 if fval("f60", 0) > 0 else 0,
            "volume": fval("f47", 0),  # 成交量（手）
            "amount": fval("f48", 0),  # 成交额（元）
            "bid": [0.0] * 5,
            "ask": [0.0] * 5,
            "bid_volume": [0.0] * 5,
            "ask_volume": [0.0] * 5,
            "trade_date": data.get("trade_date", ""),
        }

    # ============================================================
    # K 线数据
    # ============================================================

    async def get_kline_data(
        self, stock_code: str, period: str = "day",
        start_date: str = "", end_date: str = "", limit: int = 0
    ) -> List[Dict[str, Any]]:
        secid = self._to_secid(stock_code)
        klt = self._to_kline_period(period)

        # 东方财富 API 日期格式: YYYYMMDD
        beg = start_date.replace("-", "") if start_date else "0"
        end = end_date.replace("-", "") if end_date else "20500101"

        params = {
            "secid": secid,
            "fields1": "f1,f2,f3,f4,f5,f6",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
            "klt": klt,
            "fqt": 1,  # 前复权
            "beg": beg,
            "end": end,
            "lmt": limit if limit > 0 else 0,
        }

        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(self.KLINE_BASE_URL, params=params)
            result = resp.json()

        data = result.get("data")
        if not data:
            return []

        klines = data.get("klines", [])
        records = self._normalize_kline_records(klines, stock_code)

        if limit > 0:
            records = records[-limit:]

        return records

    async def get_batch_kline_data(
        self, stock_codes: List[str], period: str = "day",
        start_date: str = "", end_date: str = ""
    ) -> Dict[str, List[Dict[str, Any]]]:
        output = {}
        for code in stock_codes:
            try:
                records = await self.get_kline_data(
                    code, period, start_date, end_date
                )
                output[code] = records
            except Exception:
                output[code] = []
        return output

    # ============================================================
    # 实时行情
    # ============================================================

    async def get_market_data(self, stock_code: str) -> Optional[Dict[str, Any]]:
        secid = self._to_secid(stock_code)
        fields = "f43,f44,f45,f46,f47,f48,f50,f57,f58,f60,f116,f117,f162,f168,f169,f170"

        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(
                self.QUOTE_BASE_URL,
                params={"secid": secid, "fields": fields},
            )
            result = resp.json()

        data = result.get("data")
        if not data:
            return None

        return self._normalize_market_data(data, stock_code)

    async def get_batch_market_data(
        self, stock_codes: List[str]
    ) -> Dict[str, Optional[Dict[str, Any]]]:
        output = {}
        for code in stock_codes:
            try:
                output[code] = await self.get_market_data(code)
            except Exception:
                output[code] = None
        return output

    # ============================================================
    # 参考数据
    # ============================================================

    async def get_stock_list(self) -> List[Dict[str, Any]]:
        """获取沪深 A 股列表"""
        result = []
        # 深市 + 沪市
        for market_fs in ["m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23,m:0+t:81+s:2048"]:
            params = {
                "pn": 1,
                "pz": 5000,
                "fs": market_fs,
                "fields": "f12,f14",
            }
            async with httpx.AsyncClient(timeout=15.0) as client:
                resp = await client.get(self.STOCK_LIST_BASE_URL, params=params)
                data = resp.json()

            diff = data.get("data", {}).get("diff", [])
            for item in diff:
                code = item.get("f12", "")
                name = item.get("f14", "")
                if code:
                    market = "SH" if code.startswith("6") else "SZ"
                    result.append({
                        "stock_code": f"{code}.{market}",
                        "stock_name": name,
                        "market": market,
                    })
        return result

    # ============================================================
    # 基本面数据（东方财富不支持，返回空）
    # ============================================================

    async def get_income_statement(self, stock_code: str) -> List[Dict[str, Any]]:
        return []

    async def get_balance_sheet(self, stock_code: str) -> List[Dict[str, Any]]:
        return []

    async def get_cash_flow_statement(self, stock_code: str) -> List[Dict[str, Any]]:
        return []

    # ============================================================
    # 行业/指数数据（东方财富不支持，返回空）
    # ============================================================

    async def get_industry_list(self, level: int = 1) -> List[Dict[str, Any]]:
        return []

    async def get_industry_base_info(self) -> List[Dict[str, Any]]:
        return []

    async def get_industry_daily(self, index_code: str) -> List[Dict[str, Any]]:
        return []
