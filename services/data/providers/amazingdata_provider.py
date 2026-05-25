"""
AmazingData (银河证券) 数据源适配器

将现有 SDKManager 封装为 DataProvider 接口。
由于 SDKManager 返回的格式已经是系统标准格式，此适配器基本是透传。
"""

import time
import asyncio
from typing import Optional, Dict, Any, List

from services.data.providers.base import (
    DataProvider,
    ProviderInfo,
    ProviderCapabilities,
    DataProviderError,
)
from services.common.structured_logger import get_logger

logger = get_logger("data_provider")


class AmazingDataProvider(DataProvider):
    """AmazingData SDK 适配器 — 透传现有 SDKManager"""

    def __init__(self):
        self._config: Dict[str, Any] = {}
        self._ready = False

    @property
    def provider_id(self) -> str:
        return "amazingdata"

    @property
    def info(self) -> ProviderInfo:
        return ProviderInfo(
            provider_id="amazingdata",
            display_name="银河证券",
            description="银河证券 AmazingData SDK",
            capabilities=ProviderCapabilities(
                supports_kline=True,
                supports_realtime=True,
                supports_fundamentals=True,
                supports_stock_list=True,
                supports_industry=True,
                supports_trading=True,
                supports_realtime_snapshot=True,
            ),
            requires_config=True,
            is_built_in=True,
        )

    async def initialize(self, config: Dict[str, Any]) -> bool:
        """AmazingData 不需要额外初始化，SDKManager 使用环境变量"""
        self._config = config
        from services.common.sdk_manager import get_sdk_manager
        sdk_mgr = get_sdk_manager()
        self._ready = sdk_mgr.is_connected()
        return self._ready

    async def health_check(self) -> Dict[str, Any]:
        import os
        start = time.monotonic()
        try:
            from services.common.sdk_manager import get_sdk_manager
            from services.common.sdk_proxy_client import get_subprocess_manager
            from services.common.sdk_ipc import SOCKET_PATH
            sdk_mgr = get_sdk_manager()

            # 如果未连接，先尝试重连
            if not sdk_mgr.is_connected():
                try:
                    sdk_mgr.connect()
                except Exception:
                    pass

            # 连接判断：IPC flag 或者 socket 存在+子进程存活
            connected = sdk_mgr.is_connected()
            if not connected:
                sub_mgr = get_subprocess_manager()
                if sub_mgr.is_subprocess_alive() and os.path.exists(SOCKET_PATH):
                    connected = True  # 实际可用，下次 IPC 自动重连

            if not connected:
                latency_ms = (time.monotonic() - start) * 1000
                return {
                    "ok": False,
                    "message": "SDK 未连接",
                    "latency_ms": round(latency_ms, 1),
                }

            # 实际调用 SDK 验证连接有效性
            # 先用 get_code_list() 测试，它返回纯 Python list，不涉
            try:
                codes = sdk_mgr.get_code_list()
                # 能拿到股票列表（哪怕是空列表）说明 IPC + SDK 都正常
                connected = isinstance(codes, list)
            except Exception:
                connected = False

            latency_ms = (time.monotonic() - start) * 1000
            return {
                "ok": connected,
                "message": "已连接" if connected else "SDK 调用失败",
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
    # K 线数据
    # SDKManager.query_kline 返回: {stock_code: DataFrame}
    # 系统标准格式: [{stock_code, trade_date, open, high, low, close, volume, amount}]
    # ============================================================

    async def get_kline_data(
        self, stock_code: str, period: str = "day",
        start_date: str = "", end_date: str = "", limit: int = 0
    ) -> List[Dict[str, Any]]:
        from services.common.sdk_manager import get_sdk_manager
        from services.common.stock_code import normalize_stock_code

        sdk_mgr = get_sdk_manager()
        period_map = {
            "day": 0, "week": 5, "month": 6,
            "1m": 1, "3m": 2, "5m": 3, "10m": 8,
            "15m": 4, "30m": 9, "60m": 5, "120m": 10,
        }
        sdk_period = period_map.get(period, 0)

        # AmazingData SDK 使用 YYYYMMDD 整数格式
        begin_date = int(start_date.replace("-", "")) if start_date else 0
        end_date = int(end_date.replace("-", "")) if end_date else 0

        # SDK 使用半开区间，end_date 需 +1 天
        if end_date > 0:
            end_date += 1

        result = sdk_mgr.query_kline(
            code_list=[normalize_stock_code(stock_code)],
            begin_date=begin_date,
            end_date=end_date,
            period=sdk_period,
            task_type="query",
        )

        df = result.get(stock_code)
        if df is None or len(df) == 0:
            return []

        if limit > 0:
            df = df.tail(limit)

        records = df.to_dict("records")
        # 标准化字段名
        return self._normalize_kline_records(records, stock_code)

    async def get_batch_kline_data(
        self, stock_codes: List[str], period: str = "day",
        start_date: str = "", end_date: str = ""
    ) -> Dict[str, List[Dict[str, Any]]]:
        from services.common.sdk_manager import get_sdk_manager
        from services.common.stock_code import normalize_stock_code

        sdk_mgr = get_sdk_manager()
        period_map = {"day": 0, "week": 5, "month": 6}
        sdk_period = period_map.get(period, 0)

        begin_date = int(start_date.replace("-", "")) if start_date else 0
        end_date = int(end_date.replace("-", "")) if end_date else 0
        if end_date > 0:
            end_date += 1

        normalized_codes = [normalize_stock_code(c) for c in stock_codes]
        result = sdk_mgr.query_kline(
            code_list=normalized_codes,
            begin_date=begin_date,
            end_date=end_date,
            period=sdk_period,
            task_type="query",
        )

        output = {}
        for code, df in result.items():
            if df is not None and len(df) > 0:
                records = df.to_dict("records")
                output[code] = self._normalize_kline_records(records, code)
            else:
                output[code] = []
        return output

    # ============================================================
    # 实时行情
    # ============================================================

    async def get_market_data(self, stock_code: str) -> Optional[Dict[str, Any]]:
        from services.common.sdk_manager import get_sdk_manager
        from services.common.stock_code import normalize_stock_code

        sdk_mgr = get_sdk_manager()
        md = sdk_mgr.get_market_data()

        # 获取快照
        result = sdk_mgr.query_snapshot(
            code_list=[normalize_stock_code(stock_code)],
            begin_date=0,
            end_date=0,
        )

        # SDK 返回结构: {date: {code: DataFrame}}
        snap = None
        if result and isinstance(result, dict):
            for date_key in result:
                inner = result[date_key]
                if isinstance(inner, dict) and stock_code in inner:
                    snap = inner[stock_code]
                    break

        if snap is None or (hasattr(snap, "empty") and snap.empty) or len(snap) == 0:
            return None

        row = snap.iloc[0] if hasattr(snap, "iloc") else snap[0] if isinstance(snap, list) else snap
        return self._normalize_market_data(row, stock_code)

    async def get_batch_market_data(
        self, stock_codes: List[str]
    ) -> Dict[str, Optional[Dict[str, Any]]]:
        from services.common.sdk_manager import get_sdk_manager
        from services.common.stock_code import normalize_stock_code

        sdk_mgr = get_sdk_manager()
        normalized = [normalize_stock_code(c) for c in stock_codes]

        result = sdk_mgr.query_snapshot(
            code_list=normalized,
            begin_date=0,
            end_date=0,
        )

        # SDK 返回结构: {date: {code: DataFrame}} — 展平为单层
        flat = {}
        if result and isinstance(result, dict):
            for date_key in result:
                inner = result[date_key]
                if isinstance(inner, dict):
                    for code, df in inner.items():
                        flat[code] = df

        output = {}
        for code in stock_codes:
            snap = flat.get(code)
            if snap is None or (hasattr(snap, "empty") and snap.empty) or len(snap) == 0:
                output[code] = None
            else:
                row = snap.iloc[0] if hasattr(snap, "iloc") else snap[0] if isinstance(snap, list) else snap
                output[code] = self._normalize_market_data(row, code)
        return output

    # ============================================================
    # 参考数据
    # ============================================================

    async def get_stock_list(self) -> List[Dict[str, Any]]:
        from services.common.sdk_manager import get_sdk_manager
        sdk_mgr = get_sdk_manager()
        codes = sdk_mgr.get_code_list(security_type="EXTRA_STOCK_A")
        result = []
        for item in codes:
            if isinstance(item, dict):
                code = item.get("stock_code") or item.get("code", "")
                name = item.get("stock_name") or item.get("name", "")
                market = item.get("market", "")
                if code:
                    result.append({"stock_code": code, "stock_name": name, "market": market})
        return result

    # ============================================================
    # 基本面数据
    # ============================================================

    async def get_income_statement(self, stock_code: str) -> List[Dict[str, Any]]:
        from services.common.sdk_manager import get_sdk_manager
        sdk_mgr = get_sdk_manager()
        df = sdk_mgr.get_income_statement([stock_code])
        if df is None or df.empty:
            return []
        return df.to_dict("records")

    async def get_balance_sheet(self, stock_code: str) -> List[Dict[str, Any]]:
        from services.common.sdk_manager import get_sdk_manager
        sdk_mgr = get_sdk_manager()
        df = sdk_mgr.get_balance_sheet([stock_code])
        if df is None or df.empty:
            return []
        return df.to_dict("records")

    async def get_cash_flow_statement(self, stock_code: str) -> List[Dict[str, Any]]:
        from services.common.sdk_manager import get_sdk_manager
        sdk_mgr = get_sdk_manager()
        df = sdk_mgr.get_cash_flow_statement([stock_code])
        if df is None or df.empty:
            return []
        return df.to_dict("records")

    # ============================================================
    # 行业/指数数据
    # ============================================================

    async def get_industry_list(self, level: int = 1) -> List[Dict[str, Any]]:
        from services.common.sdk_manager import get_sdk_manager
        sdk_mgr = get_sdk_manager()
        df = sdk_mgr.get_industry_base_info()
        if df is None or df.empty:
            return []
        return df.to_dict("records")

    async def get_industry_base_info(self) -> List[Dict[str, Any]]:
        from services.common.sdk_manager import get_sdk_manager
        sdk_mgr = get_sdk_manager()
        df = sdk_mgr.get_industry_base_info()
        if df is None or df.empty:
            return []
        return df.to_dict("records")

    async def get_industry_daily(self, index_code: str) -> List[Dict[str, Any]]:
        from services.common.sdk_manager import get_sdk_manager
        sdk_mgr = get_sdk_manager()
        result = sdk_mgr.get_industry_daily(code_list=[index_code])
        df = result.get(index_code)
        if df is None or df.empty:
            return []
        return df.to_dict("records")

    # ============================================================
    # 格式转换辅助方法
    # ============================================================

    @staticmethod
    def _normalize_kline_records(records: List[Dict], stock_code: str) -> List[Dict[str, Any]]:
        """将 SDK K 线 DataFrame records 转换为系统标准格式"""
        output = []
        for row in records:
            # AmazingData 返回字段名通常已是小写: trade_date, open, high, low, close, volume, amount
            # 但需要确保 stock_code 存在
            rec = {
                "stock_code": stock_code,
                "trade_date": str(row.get("trade_date", "")),
                "open": float(row.get("open", 0) or 0),
                "high": float(row.get("high", 0) or 0),
                "low": float(row.get("low", 0) or 0),
                "close": float(row.get("close", 0) or 0),
                "volume": float(row.get("volume", 0) or 0),
                "amount": float(row.get("amount", 0) or 0),
            }
            # trade_date 可能是 int (YYYYMMDD)，转换为 YYYY-MM-DD
            td = rec["trade_date"]
            if td.isdigit() and len(td) == 8:
                rec["trade_date"] = f"{td[:4]}-{td[4:6]}-{td[6:8]}"
            output.append(rec)
        return output

    @staticmethod
    def _normalize_market_data(row, stock_code: str) -> Dict[str, Any]:
        """将 SDK 快照行转换为系统标准 MarketData 格式"""
        if hasattr(row, "to_dict"):
            d = row.to_dict()
        elif isinstance(row, dict):
            d = row
        else:
            d = {}

        # AmazingData 快照字段名映射
        def get_float(key, default=0.0):
            v = d.get(key, default)
            try:
                return float(v) if v is not None else default
            except (ValueError, TypeError):
                return default

        # 买1-5 / 卖1-5（SDK 实际字段名）
        bid = []
        ask = []
        bid_volume = []
        ask_volume = []
        for i in range(1, 6):
            bid.append(get_float(f"bid_price{i}", 0))
            ask.append(get_float(f"ask_price{i}", 0))
            bid_volume.append(get_float(f"bid_volume{i}", 0))
            ask_volume.append(get_float(f"ask_volume{i}", 0))

        # 如果五档价格全为 0，用现价填充
        cp = get_float("current_price", get_float("price", 0))
        if all(b == 0 for b in bid) and all(a == 0 for a in ask):
            bid = [cp] * 5
            ask = [cp] * 5

        return {
            "stock_code": stock_code,
            "stock_name": d.get("stock_name", d.get("name", "")),
            "current_price": get_float("current_price", get_float("price", 0)),
            "change_percent": get_float("change_percent", get_float("pct_chg", 0)),
            "high": get_float("high", 0),
            "low": get_float("low", 0),
            "open_price": get_float("open", get_float("open_price", 0)),
            "prev_close": get_float("prev_close", get_float("preclose", 0)),
            "volume": get_float("volume", 0),
            "amount": get_float("amount", 0),
            "bid": bid,
            "ask": ask,
            "bid_volume": bid_volume,
            "ask_volume": ask_volume,
            "trade_date": str(d.get("trade_date", "")),
        }
