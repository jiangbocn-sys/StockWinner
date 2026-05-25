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
