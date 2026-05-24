"""
Tushare Pro 数据源适配器

通过 Tushare Pro Python SDK 获取行情数据。
需要用户自行申请 API Token（https://tushare.pro）
"""

import time
from typing import Optional, Dict, Any, List

from services.data.providers.base import (
    DataProvider,
    ProviderInfo,
    ProviderCapabilities,
    DataProviderError,
)
from services.common.structured_logger import get_logger

logger = get_logger("data_provider")


class TushareDataProvider(DataProvider):
    """Tushare Pro API 适配器"""

    def __init__(self):
        self._config: Dict[str, Any] = {}
        self._ready = False
        self._pro = None

    @property
    def provider_id(self) -> str:
        return "tushare"

    @property
    def info(self) -> ProviderInfo:
        return ProviderInfo(
            provider_id="tushare",
            display_name="Tushare Pro",
            description="Tushare Pro 金融数据接口（需要 API Token）",
            capabilities=ProviderCapabilities(
                supports_kline=True,
                supports_realtime=False,
                supports_fundamentals=True,
                supports_stock_list=True,
                supports_industry=False,
                supports_trading=False,
                supports_realtime_snapshot=False,
            ),
            requires_config=True,
            is_built_in=True,
        )

    async def initialize(self, config: Dict[str, Any]) -> bool:
        self._config = config
        token = config.get("api_token", "")
        if not token:
            self._ready = False
            self._pro = None
            return False

        try:
            import tushare as ts
            ts.set_token(token)
            self._pro = ts.pro_api()
            self._ready = True
            return True
        except ImportError:
            logger.warning("tushare_init", "tushare 包未安装，pip install tushare")
            self._ready = False
            return False
        except Exception as e:
            logger.warning("tushare_init", f"Tushare 初始化失败: {e}")
            self._ready = False
            return False

    async def health_check(self) -> Dict[str, Any]:
        start = time.monotonic()
        if not self._ready or not self._pro:
            return {
                "ok": False,
                "message": "未配置 API Token" if not self._config.get("api_token") else "初始化失败",
                "latency_ms": 0,
            }
        try:
            # 使用 daily 接口验证连接（trade_cal 频率限制太严格，1次/小时）
            df = self._pro.daily(ts_code="600000.SH", start_date="20260101", end_date="20260105")
            latency_ms = (time.monotonic() - start) * 1000
            if df is not None and len(df) > 0:
                return {
                    "ok": True,
                    "message": "已连接",
                    "latency_ms": round(latency_ms, 1),
                }
            return {
                "ok": False,
                "message": "接口返回空数据",
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
    def _to_ts_code(stock_code: str) -> str:
        """将 600000.SH 格式转换为 Tushare ts_code 格式 (600000.SH)"""
        code = stock_code.upper().replace("SH", "").replace("SZ", "")
        if stock_code.upper().endswith("SH"):
            return f"{code}.SH"
        elif stock_code.upper().endswith("SZ"):
            return f"{code}.SZ"
        # 根据前缀判断
        if code.startswith(("6", "9")):
            return f"{code}.SH"
        return f"{code}.SZ"

    @staticmethod
    def _from_ts_code(ts_code: str) -> str:
        """将 Tushare ts_code 转换为系统 stock_code 格式"""
        return ts_code.replace(".", ".")

    @staticmethod
    def _normalize_kline_records(df, stock_code: str) -> List[Dict[str, Any]]:
        """将 Tushare DataFrame 转换为系统标准 K 线格式"""
        if df is None or (hasattr(df, "empty") and df.empty):
            return []
        output = []
        for _, row in df.iterrows():
            rec = {
                "stock_code": stock_code,
                "trade_date": str(row.get("trade_date", "")),
                "open": float(row.get("open", 0) or 0),
                "high": float(row.get("high", 0) or 0),
                "low": float(row.get("low", 0) or 0),
                "close": float(row.get("close", 0) or 0),
                "volume": float(row.get("vol", 0) or 0),  # Tushare 用 vol
                "amount": float(row.get("amount", 0) or 0),
            }
            # trade_date 格式: YYYYMMDD -> YYYY-MM-DD
            td = rec["trade_date"]
            if len(td) == 8 and td.isdigit():
                rec["trade_date"] = f"{td[:4]}-{td[4:6]}-{td[6:8]}"
            output.append(rec)
        return output

    # ============================================================
    # K 线数据
    # ============================================================

    async def get_kline_data(
        self, stock_code: str, period: str = "day",
        start_date: str = "", end_date: str = "", limit: int = 0
    ) -> List[Dict[str, Any]]:
        if not self._pro:
            return []

        ts_code = self._to_ts_code(stock_code)
        start = start_date.replace("-", "") if start_date else ""
        end = end_date.replace("-", "") if end_date else ""

        if period == "day":
            df = self._pro.daily(ts_code=ts_code, start_date=start or "20200101", end_date=end)
        elif period == "week":
            df = self._pro.weekly(ts_code=ts_code, start_date=start or "20200101", end_date=end)
        elif period == "month":
            df = self._pro.monthly(ts_code=ts_code, start_date=start or "20200101", end_date=end)
        else:
            # Tushare 不支持分钟线免费接口，返回空
            return []

        if df is None or (hasattr(df, "empty") and df.empty):
            return []

        records = self._normalize_kline_records(df, stock_code)

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
    # 实时行情（Tushare 不支持免费实时行情）
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
        if not self._pro:
            return []

        df = self._pro.stock_basic(exchange="", list_status="L",
                                    fields="ts_code,symbol,name,market,list_date")
        if df is None or (hasattr(df, "empty") and df.empty):
            return []

        result = []
        for _, row in df.iterrows():
            ts_code = row.get("ts_code", "")
            name = row.get("name", "")
            symbol = row.get("symbol", "")
            market = row.get("market", "")

            # ts_code 格式: 600000.SH
            code_parts = ts_code.split(".")
            if len(code_parts) == 2:
                code, exchange = code_parts
                stock_code = f"{code}.{exchange}"
            else:
                stock_code = ts_code

            result.append({
                "stock_code": stock_code,
                "stock_name": name,
                "market": exchange if len(code_parts) == 2 else "",
            })
        return result

    # ============================================================
    # 基本面数据
    # ============================================================

    async def get_income_statement(self, stock_code: str) -> List[Dict[str, Any]]:
        if not self._pro:
            return []
        ts_code = self._to_ts_code(stock_code)
        try:
            df = self._pro.income(ts_code=ts_code, period="20250331")
            if df is None or (hasattr(df, "empty") and df.empty):
                return []
            return df.to_dict("records")
        except Exception:
            return []

    async def get_balance_sheet(self, stock_code: str) -> List[Dict[str, Any]]:
        if not self._pro:
            return []
        ts_code = self._to_ts_code(stock_code)
        try:
            df = self._pro.balancesheet(ts_code=ts_code, period="20250331")
            if df is None or (hasattr(df, "empty") and df.empty):
                return []
            return df.to_dict("records")
        except Exception:
            return []

    async def get_cash_flow_statement(self, stock_code: str) -> List[Dict[str, Any]]:
        if not self._pro:
            return []
        ts_code = self._to_ts_code(stock_code)
        try:
            df = self._pro.cashflow(ts_code=ts_code, period="20250331")
            if df is None or (hasattr(df, "empty") and df.empty):
                return []
            return df.to_dict("records")
        except Exception:
            return []

    # ============================================================
    # 行业/指数数据（Tushare 需要积分，免费版不支持）
    # ============================================================

    async def get_industry_list(self, level: int = 1) -> List[Dict[str, Any]]:
        return []

    async def get_industry_base_info(self) -> List[Dict[str, Any]]:
        return []

    async def get_industry_daily(self, index_code: str) -> List[Dict[str, Any]]:
        return []
