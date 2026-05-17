"""
数据源提供者抽象层

定义所有数据源（AmazingData、Tushare、东方财富等）必须实现的接口。
每个 Provider 负责将自己的数据格式转换为系统标准格式。
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List


# ============================================================
# 系统标准数据格式（以 AmazingData 返回格式为基准）
# ============================================================

@dataclass
class ProviderCapabilities:
    """数据源能力标识"""
    supports_kline: bool = True
    supports_realtime: bool = True
    supports_fundamentals: bool = True
    supports_stock_list: bool = True
    supports_industry: bool = True
    supports_trading: bool = False   # 仅 AmazingData 支持交易
    supports_realtime_snapshot: bool = False  # 实时快照（五档盘口）


@dataclass
class ProviderInfo:
    """数据源元信息"""
    provider_id: str              # "amazingdata" | "tushare" | "eastmoney"
    display_name: str             # "银河证券" | "Tushare Pro" | "东方财富"
    description: str
    capabilities: ProviderCapabilities = field(default_factory=ProviderCapabilities)
    requires_config: bool = False  # 是否需要配置项（如 API Token）
    is_built_in: bool = True       # 内置或动态注册


class DataProviderError(Exception):
    """数据源错误"""
    def __init__(self, provider_id: str, message: str, original_error: Exception = None):
        self.provider_id = provider_id
        self.original_error = original_error
        super().__init__(f"[{provider_id}] {message}")


class DataProvider(ABC):
    """数据源提供者抽象基类

    所有方法必须返回系统标准格式的数据。各 Provider 内部负责格式转换。
    """

    @property
    @abstractmethod
    def provider_id(self) -> str:
        """唯一标识，如 'amazingdata'"""

    @property
    @abstractmethod
    def info(self) -> ProviderInfo:
        """数据源元信息"""

    @abstractmethod
    async def initialize(self, config: Dict[str, Any]) -> bool:
        """初始化数据源（加载配置、建立连接等）。返回是否就绪。"""

    @abstractmethod
    async def health_check(self) -> Dict[str, Any]:
        """健康检查。返回 {"ok": bool, "message": str, "latency_ms": float}"""

    # ============================================================
    # K 线数据
    # 标准返回格式: [{"stock_code", "trade_date", "open", "high", "low", "close", "volume", "amount"}, ...]
    # ============================================================

    @abstractmethod
    async def get_kline_data(
        self, stock_code: str, period: str = "day",
        start_date: str = "", end_date: str = "", limit: int = 0
    ) -> List[Dict[str, Any]]:
        """获取单只股票 K 线数据"""

    @abstractmethod
    async def get_batch_kline_data(
        self, stock_codes: List[str], period: str = "day",
        start_date: str = "", end_date: str = ""
    ) -> Dict[str, List[Dict[str, Any]]]:
        """批量获取 K 线数据。返回 {stock_code: [records]}"""

    # ============================================================
    # 实时行情
    # 标准返回格式: {"stock_code", "stock_name", "current_price", "change_percent",
    #                "high", "low", "open_price", "prev_close", "volume", "amount",
    #                "bid": [买1..买5], "ask": [卖1..卖5], "trade_date"}
    # ============================================================

    @abstractmethod
    async def get_market_data(self, stock_code: str) -> Optional[Dict[str, Any]]:
        """获取单只股票实时行情"""

    @abstractmethod
    async def get_batch_market_data(
        self, stock_codes: List[str]
    ) -> Dict[str, Optional[Dict[str, Any]]]:
        """批量获取实时行情。返回 {stock_code: data_or_None}"""

    # ============================================================
    # 参考数据
    # ============================================================

    @abstractmethod
    async def get_stock_list(self) -> List[Dict[str, Any]]:
        """获取股票列表。标准格式: [{"stock_code", "stock_name", "market"}, ...]"""

    # ============================================================
    # 基本面数据
    # ============================================================

    @abstractmethod
    async def get_income_statement(self, stock_code: str) -> List[Dict[str, Any]]:
        """利润表"""

    @abstractmethod
    async def get_balance_sheet(self, stock_code: str) -> List[Dict[str, Any]]:
        """资产负债表"""

    @abstractmethod
    async def get_cash_flow_statement(self, stock_code: str) -> List[Dict[str, Any]]:
        """现金流量表"""

    # ============================================================
    # 行业/指数数据
    # ============================================================

    @abstractmethod
    async def get_industry_list(self, level: int = 1) -> List[Dict[str, Any]]:
        """行业列表"""

    @abstractmethod
    async def get_industry_base_info(self) -> List[Dict[str, Any]]:
        """行业基本信息"""

    @abstractmethod
    async def get_industry_daily(self, index_code: str) -> List[Dict[str, Any]]:
        """行业指数日K线"""

    # ============================================================
    # 可选方法（Provider 不支持时返回空列表）
    # ============================================================

    async def get_index_list(self) -> List[Dict[str, str]]:
        return []

    async def get_index_constituent(self, index_code: str) -> List[Dict[str, Any]]:
        return []

    async def get_industry_constituent(self, index_code: str) -> List[Dict[str, Any]]:
        return []

    async def get_profit_notice(self, stock_code: str) -> List[Dict[str, Any]]:
        return []

    async def get_profit_express(self, stock_code: str) -> List[Dict[str, Any]]:
        return []

    async def get_code_info(self, security_type: str) -> List[Dict[str, Any]]:
        return []

    async def get_long_hu_bang(
        self, stock_code: str, begin_date: int, end_date: int
    ) -> List[Dict[str, Any]]:
        return []

    async def get_margin_summary(
        self, begin_date: int, end_date: int
    ) -> List[Dict[str, Any]]:
        return []

    async def get_margin_detail(
        self, stock_code: str, begin_date: int, end_date: int
    ) -> List[Dict[str, Any]]:
        return []

    async def get_block_trading(
        self, stock_code: str, begin_date: int, end_date: int
    ) -> List[Dict[str, Any]]:
        return []

    async def get_treasury_yield(self) -> List[Dict[str, Any]]:
        return []

    async def get_calendar(self) -> List[str]:
        """交易日历。返回交易日期列表。"""
        return []
