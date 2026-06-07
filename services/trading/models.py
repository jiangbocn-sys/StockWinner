"""
交易数据模型与接口定义
"""
from abc import ABC, abstractmethod
from typing import Optional, Dict, Any, List


class OrderResult:
    """订单结果"""
    def __init__(self, success: bool, order_id: Optional[str] = None,
                 message: str = "", error_code: int = 0):
        self.success = success
        self.order_id = order_id
        self.message = message
        self.error_code = error_code


class MarketData:
    """行情数据"""
    def __init__(self, stock_code: str, stock_name: str,
                 current_price: float, change_percent: float,
                 high: float, low: float, open_price: float,
                 prev_close: float, volume: int, amount: float,
                 bid: List[float] = None, ask: List[float] = None,
                 bid_volume: List[float] = None, ask_volume: List[float] = None,
                 trade_date: str = "", source: str = ""):
        self.stock_code = stock_code
        self.stock_name = stock_name
        self.current_price = current_price
        self.change_percent = change_percent
        self.high = high
        self.low = low
        self.open_price = open_price
        self.prev_close = prev_close
        self.volume = volume
        self.amount = amount
        self.bid = bid or []
        self.ask = ask or []
        self.bid_volume = bid_volume or []
        self.ask_volume = ask_volume or []
        self.trade_date = trade_date
        self.source = source  # "snapshot" / "kline" / "kline_db" / "" (unknown)

    @classmethod
    def from_ohlcv(cls, ohlcv: dict, stock_code: str, stock_name: str = "") -> "MarketData":
        """从 OHLCV 字典（PriceCache 格式）构造 MarketData"""
        close = ohlcv.get("close", 0)
        return cls(
            stock_code=stock_code, stock_name=stock_name or stock_code,
            current_price=close, change_percent=ohlcv.get("change_pct", 0),
            high=ohlcv.get("high", close), low=ohlcv.get("low", close),
            open_price=ohlcv.get("open", close), prev_close=close,
            volume=int(ohlcv.get("volume", 0)), amount=ohlcv.get("amount", 0),
            bid=[close] * 5, ask=[close] * 5,
            bid_volume=[0] * 5, ask_volume=[0] * 5,
            trade_date="",
            source=ohlcv.get("source", ""),
        )

    @classmethod
    def from_fallback(cls, data: dict, stock_code: str, source: str = "channel") -> "MarketData":
        """从通道回退/外部数据源 dict 构造 MarketData"""
        return cls(
            stock_code=data.get("stock_code", stock_code),
            stock_name=data.get("stock_name", ""),
            current_price=float(data.get("current_price", 0)),
            change_percent=float(data.get("change_percent", 0)),
            high=float(data.get("high", 0)),
            low=float(data.get("low", 0)),
            open_price=float(data.get("open_price", 0)),
            prev_close=float(data.get("prev_close", 0)),
            volume=int(data.get("volume", 0)),
            amount=float(data.get("amount", 0)),
            bid=data.get("bid", []),
            ask=data.get("ask", []),
            bid_volume=data.get("bid_volume", []),
            ask_volume=data.get("ask_volume", []),
            trade_date=data.get("trade_date", ""),
            source=source,
        )


class TradingGatewayInterface(ABC):
    """交易网关接口"""

    @abstractmethod
    async def connect(self) -> bool:
        pass

    @abstractmethod
    async def disconnect(self):
        pass

    @abstractmethod
    async def get_market_data(self, stock_code: str) -> Optional[MarketData]:
        pass

    @abstractmethod
    async def get_stock_list(self) -> List[Dict[str, str]]:
        pass

    @abstractmethod
    async def get_index_list(self) -> List[Dict[str, str]]:
        pass

    @abstractmethod
    async def get_industry_index_list(self) -> List[Dict[str, str]]:
        pass

    @abstractmethod
    async def buy(self, stock_code: str, price: float, quantity: int) -> OrderResult:
        pass

    @abstractmethod
    async def sell(self, stock_code: str, price: float, quantity: int) -> OrderResult:
        pass

    @abstractmethod
    async def get_positions(self) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    async def get_orders(self) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    async def get_batch_market_data(self, stock_codes: List[str]) -> Dict[str, Optional[MarketData]]:
        pass

    @abstractmethod
    async def get_kline_data(
        self, stock_code: str, period: str = "day",
        start_date: Optional[str] = None, end_date: Optional[str] = None, limit: int = 100
    ) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    async def get_code_info(self, security_type: str = 'EXTRA_STOCK_A') -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    async def get_industry_list(self, level: int = 1) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    async def get_industry_kline(self, index_code: str) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    async def get_industry_constituent(self, index_code: str) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    async def get_index_constituent(self, index_code: str) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    async def get_income_statement(self, stock_code: str) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    async def get_balance_sheet(self, stock_code: str) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    async def get_cash_flow_statement(self, stock_code: str) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    async def get_profit_notice(self, stock_code: str) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    async def get_profit_express(self, stock_code: str) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    async def get_long_hu_bang(self, stock_code: str, begin_date: int, end_date: int) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    async def get_margin_summary(self, begin_date: int, end_date: int) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    async def get_margin_detail(self, stock_code: str, begin_date: int, end_date: int) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    async def get_block_trading(self, stock_code: str, begin_date: int, end_date: int) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    async def get_treasury_yield(self) -> List[Dict[str, Any]]:
        pass
