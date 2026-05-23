"""
新浪财经数据源适配器

通过新浪公开 HTTP API 获取行情数据，无需登录/Token。
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


class SinaDataProvider(DataProvider):
    """新浪财经 HTTP API 适配器"""

    QUOTE_BASE_URL = "https://hq.sinajs.cn"

    def __init__(self):
        self._config: Dict[str, Any] = {}
        self._ready = False

    @property
    def provider_id(self) -> str:
        return "sina"

    @property
    def info(self) -> ProviderInfo:
        return ProviderInfo(
            provider_id="sina",
            display_name="新浪财经",
            description="新浪财经免费行情接口",
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
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Referer": "https://finance.sina.com.cn/",
            }
            async with httpx.AsyncClient(timeout=10.0, headers=headers, follow_redirects=True) as client:
                resp = await client.get(
                    self.QUOTE_BASE_URL + "/list=sh600000",
                )
                resp.raise_for_status()
                text = resp.text
                # 新浪返回格式: var hq_str_sh600000="..."
                ok = "hq_str_sh600000" in text and len(text) > 50
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
    def _to_sina_code(stock_code: str) -> str:
        """将 600000.SH 格式转换为新浪格式 sh600000"""
        code = stock_code.upper()
        if code.endswith(".SH"):
            return f"sh{code[:-3]}"
        elif code.endswith(".SZ"):
            return f"sz{code[:-3]}"
        return code

    @staticmethod
    def _parse_sina_line(line: str, stock_code: str) -> Optional[Dict[str, Any]]:
        """解析新浪行情行

        格式: var hq_str_sh600000="股票名称,今开,昨收,现价,最高,最低,买入价,卖出价,成交量,成交额,
              买1量,买1价,...,买5价,买5量,卖1价,卖1量,...,卖5价,卖5量,日期,时间,...";
        """
        if "=" not in line:
            return None
        content = line.split('="', 1)[1].rstrip('";')
        if not content:
            return None

        fields = content.split(",")
        if len(fields) < 32:
            return None

        def fval(idx: int, default: float = 0.0) -> float:
            try:
                return float(fields[idx]) if fields[idx] else default
            except (ValueError, IndexError):
                return default

        current_price = fval(3, 0)
        prev_close = fval(2, 0)
        open_price = fval(1, 0)
        high = fval(4, 0)
        low = fval(5, 0)
        volume = fval(8, 0)
        amount = fval(9, 0)

        # 五档盘口: [11]=买1价,[10]=买1量,[13]=买2价,[12]=买2量,...
        # [21]=卖1价,[20]=卖1量,[23]=卖2价,[22]=卖2量,...
        bid = [fval(11 + i * 2) for i in range(5)]
        ask = [fval(21 + i * 2) for i in range(5)]
        bid_volume = [int(fval(10 + i * 2)) for i in range(5)]
        ask_volume = [int(fval(20 + i * 2)) for i in range(5)]

        # 如果五档全为 0，用现价填充
        if all(b == 0 for b in bid) and all(a == 0 for a in ask):
            bid = [current_price] * 5
            ask = [current_price] * 5

        change_pct = 0.0
        if prev_close > 0 and current_price > 0:
            change_pct = round((current_price - prev_close) / prev_close * 100, 2)

        return {
            "stock_code": stock_code,
            "stock_name": fields[0],
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
            "trade_date": fields[30] if len(fields) > 30 else "",
        }

    # ============================================================
    # 实时行情
    # ============================================================

    async def get_market_data(self, stock_code: str) -> Optional[Dict[str, Any]]:
        sina_code = self._to_sina_code(stock_code)
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer": "https://finance.sina.com.cn/",
        }
        async with httpx.AsyncClient(timeout=10.0, headers=headers, follow_redirects=True) as client:
            resp = await client.get(self.QUOTE_BASE_URL + f"/list={sina_code}")
            resp.raise_for_status()
            text = resp.text

        for line in text.strip().split("\n"):
            result = self._parse_sina_line(line, stock_code)
            if result:
                return result
        return None

    async def get_batch_market_data(
        self, stock_codes: List[str]
    ) -> Dict[str, Optional[Dict[str, Any]]]:
        sina_codes = ",".join(self._to_sina_code(c) for c in stock_codes)
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer": "https://finance.sina.com.cn/",
        }
        async with httpx.AsyncClient(timeout=15.0, headers=headers, follow_redirects=True) as client:
            resp = await client.get(self.QUOTE_BASE_URL + f"/list={sina_codes}")
            resp.raise_for_status()
            text = resp.text

        results = {code: None for code in stock_codes}
        for line in text.strip().split("\n"):
            # 从行中提取股票代码
            if "=" not in line:
                continue
            var_name = line.split("=", 1)[0].replace("var hq_str_", "")
            # var_name 格式: sh600000 或 sz000001
            normalized = var_name[2:].upper()
            if var_name.startswith("sh"):
                normalized = f"{normalized}.SH"
            elif var_name.startswith("sz"):
                normalized = f"{normalized}.SZ"
            for code in stock_codes:
                if code.upper() == normalized.upper():
                    result = self._parse_sina_line(line, code)
                    if result:
                        results[code] = result
                    break
        return results

    # ============================================================
    # K 线数据（新浪不支持）
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
    # 参考数据（新浪不支持）
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
