"""
Akshare 数据源适配器

基于 akshare 库封装的多数据源聚合 Provider。
内部使用新浪/腾讯等免费数据源，无需 Token。
- 实时行情: stock_zh_a_spot (新浪)
- 日K线: stock_zh_a_daily (新浪) / stock_zh_a_hist_tx (腾讯)
- 分钟K线: 暂不支持（新浪分钟接口有 bug）
- 股票列表: stock_info_a_code_name
- 财务报表: stock_profit_sheet_by_report_em 等
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


class AkshareDataProvider(DataProvider):
    """Akshare 多数据源聚合适配器"""

    def __init__(self):
        self._config: Dict[str, Any] = {}
        self._ready = False

    @property
    def provider_id(self) -> str:
        return "akshare"

    @property
    def info(self) -> ProviderInfo:
        return ProviderInfo(
            provider_id="akshare",
            display_name="Akshare",
            description="Akshare 聚合数据源（新浪/腾讯）",
            capabilities=ProviderCapabilities(
                supports_kline=True,
                supports_realtime=False,
                supports_fundamentals=True,
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
        try:
            import akshare
            self._ready = True
        except ImportError:
            self._ready = False
        return self._ready

    async def health_check(self) -> Dict[str, Any]:
        start = time.monotonic()
        try:
            import akshare as ak

            # 快速测试：获取一只股票的基本信息
            df = ak.stock_info_a_code_name()
            ok = df is not None and hasattr(df, "empty") and not df.empty
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

    @staticmethod
    def _to_akshare_code(stock_code: str) -> str:
        """将 600000.SH 格式转换为 akshare 格式 sh600000"""
        code = stock_code.upper()
        if code.endswith(".SH"):
            return f"sh{code[:-3]}"
        elif code.endswith(".SZ"):
            return f"sz{code[:-3]}"
        return code

    @staticmethod
    def _normalize_kline_from_sina(df, stock_code: str) -> List[Dict[str, Any]]:
        """新浪日K线数据标准化

        列: date, open, high, low, close, volume, amount, outstanding_share, turnover
        """
        if df is None or df.empty:
            return []
        records = []
        for _, row in df.iterrows():
            try:
                records.append({
                    "stock_code": stock_code,
                    "trade_date": str(row.get("date", "")),
                    "open": float(row.get("open", 0) or 0),
                    "high": float(row.get("high", 0) or 0),
                    "low": float(row.get("low", 0) or 0),
                    "close": float(row.get("close", 0) or 0),
                    "volume": float(row.get("volume", 0) or 0),
                    "amount": float(row.get("amount", 0) or 0),
                })
            except (ValueError, TypeError):
                continue
        return records

    @staticmethod
    def _normalize_kline_from_tencent(df, stock_code: str) -> List[Dict[str, Any]]:
        """腾讯K线数据标准化

        列: date, open, close, high, low, amount
        腾讯 amount 单位可能为万元，需确认
        """
        if df is None or df.empty:
            return []
        records = []
        for _, row in df.iterrows():
            try:
                records.append({
                    "stock_code": stock_code,
                    "trade_date": str(row.get("date", "")),
                    "open": float(row.get("open", 0) or 0),
                    "high": float(row.get("high", 0) or 0),
                    "low": float(row.get("low", 0) or 0),
                    "close": float(row.get("close", 0) or 0),
                    "volume": float(row.get("volume", 0) or 0),
                    "amount": float(row.get("amount", 0) or 0),
                })
            except (ValueError, TypeError):
                continue
        return records

    # ============================================================
    # K 线数据
    # ============================================================

    async def get_kline_data(
        self, stock_code: str, period: str = "day",
        start_date: str = "", end_date: str = "", limit: int = 0
    ) -> List[Dict[str, Any]]:
        import akshare as ak

        ak_code = self._to_akshare_code(stock_code)

        if period == "day":
            # 日K线使用新浪数据源
            try:
                df = ak.stock_zh_a_daily(
                    symbol=ak_code,
                    start_date=start_date.replace("-", "") if start_date else "",
                    end_date=end_date.replace("-", "") if end_date else "",
                )
                records = self._normalize_kline_from_sina(df, stock_code)
                if limit > 0:
                    records = records[-limit:]
                return records
            except Exception:
                # 新浪失败，降级到腾讯
                try:
                    df = ak.stock_zh_a_hist_tx(
                        symbol=ak_code,
                        start_date=start_date.replace("-", "") if start_date else "",
                        end_date=end_date.replace("-", "") if end_date else "",
                    )
                    records = self._normalize_kline_from_tencent(df, stock_code)
                    if limit > 0:
                        records = records[-limit:]
                    return records
                except Exception:
                    return []
        else:
            # 分钟/周/月K线暂不支持
            return []

    async def get_batch_kline_data(
        self, stock_codes: List[str], period: str = "day",
        start_date: str = "", end_date: str = ""
    ) -> Dict[str, List[Dict[str, Any]]]:
        # akshare 无批量K线接口，逐个获取
        output = {}
        for code in stock_codes:
            try:
                output[code] = await self.get_kline_data(
                    code, period, start_date, end_date
                )
            except Exception:
                output[code] = []
        return output

    # ============================================================
    # 实时行情（akshare 新浪实时接口已不可用，返回 None）
    # ============================================================

    async def get_market_data(self, stock_code: str) -> Optional[Dict[str, Any]]:
        return None

    async def get_batch_market_data(
        self, stock_codes: List[str]
    ) -> Dict[str, Optional[Dict[str, Any]]]:
        return {code: None for code in stock_codes}

    # ============================================================
    # 参考数据
    # ============================================================

    async def get_stock_list(self) -> List[Dict[str, Any]]:
        import akshare as ak

        df = ak.stock_info_a_code_name()
        if df is None or df.empty:
            return []

        result = []
        for _, row in df.iterrows():
            code = str(row.get("code", ""))
            name = str(row.get("name", ""))
            if code:
                # 根据代码前缀判断市场
                if code.startswith("6"):
                    market = "SH"
                elif code.startswith("0") or code.startswith("3"):
                    market = "SZ"
                else:
                    market = ""
                result.append({
                    "stock_code": f"{code}.{market}",
                    "stock_name": name,
                    "market": market,
                })
        return result

    # ============================================================
    # 基本面数据（akshare 财报接口返回全市场数据，需过滤）
    # ============================================================

    async def get_income_statement(self, stock_code: str) -> List[Dict[str, Any]]:
        import akshare as ak
        try:
            # 无参数获取全市场数据
            df = await asyncio.to_thread(ak.stock_profit_sheet_by_report_em)
            if df is None or df.empty:
                return []
            # 过滤目标股票
            code_num = stock_code.split(".")[0]
            mask = df["SECURITY_CODE"].astype(str) == code_num
            filtered = df[mask]
            if filtered.empty:
                return []
            return filtered.to_dict("records")
        except Exception:
            return []

    async def get_balance_sheet(self, stock_code: str) -> List[Dict[str, Any]]:
        import akshare as ak
        try:
            df = await asyncio.to_thread(ak.stock_balance_sheet_by_report_em)
            if df is None or df.empty:
                return []
            code_num = stock_code.split(".")[0]
            mask = df["SECURITY_CODE"].astype(str) == code_num
            filtered = df[mask]
            if filtered.empty:
                return []
            return filtered.to_dict("records")
        except Exception:
            return []

    async def get_cash_flow_statement(self, stock_code: str) -> List[Dict[str, Any]]:
        import akshare as ak
        try:
            df = await asyncio.to_thread(ak.stock_cash_flow_sheet_by_report_em)
            if df is None or df.empty:
                return []
            code_num = stock_code.split(".")[0]
            mask = df["SECURITY_CODE"].astype(str) == code_num
            filtered = df[mask]
            if filtered.empty:
                return []
            return filtered.to_dict("records")
        except Exception:
            return []

    # ============================================================
    # 行业/指数数据（akshare 不支持）
    # ============================================================

    async def get_industry_list(self, level: int = 1) -> List[Dict[str, Any]]:
        return []

    async def get_industry_base_info(self) -> List[Dict[str, Any]]:
        return []

    async def get_industry_daily(self, index_code: str) -> List[Dict[str, Any]]:
        return []
