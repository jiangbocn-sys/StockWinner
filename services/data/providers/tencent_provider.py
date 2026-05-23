"""
腾讯行情数据源适配器

通过腾讯公开 HTTP API 获取行情数据，无需登录/Token。
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


class TencentDataProvider(DataProvider):
    """腾讯行情 HTTP API 适配器"""

    QUOTE_BASE_URL = "https://qt.gtimg.cn"

    def __init__(self):
        self._config: Dict[str, Any] = {}
        self._ready = False

    @property
    def provider_id(self) -> str:
        return "tencent"

    @property
    def info(self) -> ProviderInfo:
        return ProviderInfo(
            provider_id="tencent",
            display_name="腾讯行情",
            description="腾讯免费行情接口",
            capabilities=ProviderCapabilities(
                supports_kline=False,
                supports_realtime=True,
                supports_fundamentals=False,
                supports_stock_list=False,
                supports_industry=False,
                supports_trading=False,
                supports_realtime_snapshot=True,
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
            async with httpx.AsyncClient(timeout=10.0, follow_redirects=True) as client:
                resp = await client.get(
                    self.QUOTE_BASE_URL + "/q=sh600000",
                )
                resp.raise_for_status()
                text = resp.text
                # 腾讯返回格式: v_sh600000="市场~股票名称~代码~现价~昨收~今开~..."
                ok = "v_sh600000" in text and "~" in text
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
    def _to_tencent_code(stock_code: str) -> str:
        """将 600000.SH 格式转换为腾讯格式 sh600000"""
        code = stock_code.upper()
        if code.endswith(".SH"):
            return f"sh{code[:-3]}"
        elif code.endswith(".SZ"):
            return f"sz{code[:-3]}"
        return code

    @staticmethod
    def _parse_tencent_line(line: str, stock_code: str) -> Optional[Dict[str, Any]]:
        """解析腾讯行情行

        实际字段索引(基于 qt.gtimg.cn 真实返回):
          0: 市场, 1: 名称, 2: 代码, 3: 现价, 4: 昨收, 5: 今开,
          6: 外盘, 7: 内盘, 8: 成交量(手)(旧版,可能为空),
          9: 买1价, 10: 买1量, 11: 买2价, 12: 买2量, ..., 17: 买5价, 18: 买5量
          19: 卖1价, 20: 卖1量, 21: 卖2价, 22: 卖2量, ..., 27: 卖5价, 28: 卖5量
          30: 日期时间戳, 31: 涨跌额, 32: 涨跌幅%,
          33: 最高, 34: 最低,
          36: 成交量(手), 37: 成交额(万元)
        """
        if "~" not in line or "=" not in line:
            return None
        content = line.split('="', 1)[1].rstrip('";\n')
        if not content:
            return None

        fields = content.split("~")
        if len(fields) < 35:
            return None

        def fval(idx: int, default: float = 0.0) -> float:
            try:
                return float(fields[idx]) if fields[idx] else default
            except (ValueError, IndexError):
                return default

        current_price = fval(3, 0)
        prev_close = fval(4, 0)
        open_price = fval(5, 0)
        high = fval(33, fval(6, 0))
        low = fval(34, fval(7, 0))
        volume = fval(36, 0) * 100  # 成交量单位是手，转为股数
        amount = fval(37, 0) * 10000  # 成交额单位是万元，转为元

        # 买盘: 9=买1价, 10=买1量, ..., 17=买5价, 18=买5量
        bid = [fval(9 + i * 2) for i in range(5)]
        bid_volume = [int(fval(10 + i * 2) * 100) for i in range(5)]  # 手→股
        # 卖盘: 19=卖1价, 20=卖1量, ..., 27=卖5价, 28=卖5量
        ask = [fval(19 + i * 2) for i in range(5)]
        ask_volume = [int(fval(20 + i * 2) * 100) for i in range(5)]  # 手→股

        # 如果五档全为 0，用现价填充
        if all(b == 0 for b in bid) and all(a == 0 for a in ask):
            bid = [current_price] * 5
            ask = [current_price] * 5

        change_pct = fval(32, 0)

        return {
            "stock_code": stock_code,
            "stock_name": fields[1],
            "current_price": current_price,
            "change_percent": change_pct,
            "high": high,
            "low": low,
            "open_price": open_price,
            "prev_close": prev_close,
            "volume": volume,
            "amount": amount,
            "bid": bid,
            "ask": ask,
            "bid_volume": bid_volume,
            "ask_volume": ask_volume,
            "trade_date": "",
        }

    # ============================================================
    # 实时行情
    # ============================================================

    async def get_market_data(self, stock_code: str) -> Optional[Dict[str, Any]]:
        tencent_code = self._to_tencent_code(stock_code)
        async with httpx.AsyncClient(timeout=10.0, follow_redirects=True) as client:
            resp = await client.get(self.QUOTE_BASE_URL + f"/q={tencent_code}")
            resp.raise_for_status()
            text = resp.text

        for line in text.strip().split("\n"):
            result = self._parse_tencent_line(line, stock_code)
            if result:
                return result
        return None

    async def get_batch_market_data(
        self, stock_codes: List[str]
    ) -> Dict[str, Optional[Dict[str, Any]]]:
        tencent_codes = ",".join(self._to_tencent_code(c) for c in stock_codes)
        async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
            resp = await client.get(self.QUOTE_BASE_URL + f"/q={tencent_codes}")
            resp.raise_for_status()
            text = resp.text

        results = {code: None for code in stock_codes}
        for line in text.strip().split("\n"):
            if "=" not in line:
                continue
            var_name = line.split("=", 1)[0].replace("v_", "")
            normalized = var_name[2:].upper()
            if var_name.startswith("sh"):
                normalized = f"{normalized}.SH"
            elif var_name.startswith("sz"):
                normalized = f"{normalized}.SZ"
            for code in stock_codes:
                if code.upper() == normalized.upper():
                    result = self._parse_tencent_line(line, code)
                    if result:
                        results[code] = result
                    break
        return results

    # ============================================================
    # K 线数据（腾讯不支持）
    # ============================================================

    async def get_kline_data(
        self, stock_code: str, period: str = "day",
        start_date: str = "", end_date: str = "", limit: int = 0
    ) -> List[Dict[str, Any]]:
        return []

    async def get_batch_kline_data(
        self, stock_codes: List[str], period: str = "day",
        start_date: str = "", end_date: str = ""
    ) -> Dict[str, List[Dict[str, Any]]]:
        return {code: [] for code in stock_codes}

    # ============================================================
    # 参考数据（腾讯不支持）
    # ============================================================

    async def get_stock_list(self) -> List[Dict[str, Any]]:
        return []

    # ============================================================
    # 基本面数据（不支持）
    # ============================================================

    async def get_income_statement(self, stock_code: str) -> List[Dict[str, Any]]:
        return []

    async def get_balance_sheet(self, stock_code: str) -> List[Dict[str, Any]]:
        return []

    async def get_cash_flow_statement(self, stock_code: str) -> List[Dict[str, Any]]:
        return []

    # ============================================================
    # 行业/指数数据（不支持）
    # ============================================================

    async def get_industry_list(self, level: int = 1) -> List[Dict[str, Any]]:
        return []

    async def get_industry_base_info(self) -> List[Dict[str, Any]]:
        return []

    async def get_industry_daily(self, index_code: str) -> List[Dict[str, Any]]:
        return []
