"""
交易网关 — 薄门面，委托给子服务模块。

子模块：
- models.py: OrderResult, MarketData, TradingGatewayInterface
- market_data_service.py: 行情数据（单只/批量/缓存/并发）
- kline_service.py: K 线数据（单只/批量）
- external_data_service.py: 行业/指数/财报/龙虎榜/两融/大宗/国债
- trading_executor.py: 交易执行（买入/卖出/撤单）
"""
import logging
from typing import Optional, Dict, Any, List, Set

from services.trading.models import OrderResult, MarketData, TradingGatewayInterface
from services.trading.market_data_service import MarketDataService
from services.trading.kline_service import KlineDataService
from services.trading.external_data_service import ExternalDataService
from services.trading.trading_executor import TradingExecutorService

logger = logging.getLogger(__name__)


class TradingGateway(TradingGatewayInterface):
    """交易网关（使用 AmazingData SDK） — 薄门面，委托给子服务"""

    def __init__(self, app_id: str = "", password: str = ""):
        self.app_id = app_id
        self.password = password
        self.server_ip = "140.206.44.234"
        self.server_port = 8600

        try:
            from AmazingData import constant
            self.constant = constant
            self.sdk_available = True
            logger.info("AmazingData SDK 加载成功（通过 SDKManager 管理）")
        except ImportError as e:
            logger.warning(f"AmazingData SDK 不可用：{e}")
            self.sdk_available = False

        # 子服务
        self._market_data = MarketDataService()
        self._kline_data = KlineDataService(self.sdk_available, getattr(self, 'constant', None))
        self._external_data = ExternalDataService(self.sdk_available, getattr(self, 'constant', None))
        self._trading = TradingExecutorService()

    @property
    def connected(self) -> bool:
        """从 SDKManager 读取连接状态"""
        if not self.sdk_available:
            return False
        from services.common.sdk_manager import get_sdk_manager
        return get_sdk_manager().is_connected()

    async def connect(self) -> bool:
        """连接交易服务器 — 委托给 SDKManager"""
        if not self.sdk_available:
            logger.error("AmazingData SDK 未安装，无法连接")
            return False
        try:
            from services.common.sdk_manager import get_sdk_manager
            return get_sdk_manager().connect()
        except Exception as e:
            logger.error(f"交易网关连接失败：{e}")
            return False

    async def disconnect(self):
        """断开连接 — 委托给 SDKManager"""
        from services.common.sdk_manager import get_sdk_manager
        get_sdk_manager().disconnect()

    # ── 行情数据 ──

    async def get_market_data(self, stock_code: str) -> Optional[MarketData]:
        return await self._market_data.get_market_data(stock_code, self.connected)

    async def get_batch_market_data(self, stock_codes: List[str]) -> Dict[str, Optional[MarketData]]:
        return await self._market_data.get_batch_market_data(stock_codes, self.connected)

    # ── K 线数据 ──

    async def get_kline_data(
        self, stock_code: str, period: str = "day",
        start_date: Optional[str] = None, end_date: Optional[str] = None,
        limit: int = 100, priority: int = 1
    ) -> List[Dict[str, Any]]:
        """获取 K 线数据（用户查询，默认 high priority）"""
        return await self._kline_data.get_kline_data(
            stock_code, period, start_date, end_date, limit,
            task_type="query", priority=priority, connected=self.connected
        )

    async def get_batch_kline_data(
        self, stock_codes: List[str], period: str = "day",
        start_date: Optional[str] = None, end_date: Optional[str] = None,
        limit: int = 100, task_type: str = "query", priority: int = 1
    ) -> Dict[str, Any]:
        """批量获取 K 线数据

        Args:
            priority: 默认 high (1)，后台下载用 low (3)
        """
        return await self._kline_data.get_batch_kline_data(
            stock_codes, period, start_date, end_date, limit,
            task_type=task_type, priority=priority, connected=self.connected
        )

    # ── 外部数据 ──

    async def get_code_info(self, security_type: str = 'EXTRA_STOCK_A', priority: int = 2) -> List[Dict[str, Any]]:
        """获取代码信息（用户查询，默认 medium priority）"""
        return await self._external_data.get_code_info(security_type, priority=priority, connected=self.connected)

    async def get_index_list(self, priority: int = 2) -> List[Dict[str, Any]]:
        """获取指数列表（用户查询，默认 medium priority）"""
        if not self.connected:
            raise Exception("网关未连接")
        try:
            import asyncio
            from services.common.sdk_manager import get_sdk_manager
            return await asyncio.to_thread(lambda: get_sdk_manager().get_index_list(priority=priority))
        except Exception as e:
            logger.error(f"获取指数列表失败: {e}")
            return []

    async def get_industry_list(self, level: int = 1, priority: int = 2) -> List[Dict[str, Any]]:
        """获取行业列表（用户查询，默认 medium priority）"""
        return await self._external_data.get_industry_list(level, priority=priority, connected=self.connected)

    async def get_industry_kline(self, index_code: str, priority: int = 2) -> List[Dict[str, Any]]:
        """获取行业行情（用户查询，默认 medium priority）"""
        return await self._external_data.get_industry_kline(index_code, priority=priority, connected=self.connected)

    async def get_industry_constituent(self, index_code: str, priority: int = 2) -> List[Dict[str, Any]]:
        """获取行业成分股（用户查询，默认 medium priority）"""
        return await self._external_data.get_industry_constituent(index_code, priority=priority, connected=self.connected)

    async def get_index_constituent(self, index_code: str, priority: int = 2) -> List[Dict[str, Any]]:
        """获取指数成分股（用户查询，默认 medium priority）"""
        return await self._external_data.get_index_constituent(index_code, priority=priority, connected=self.connected)

    async def get_income_statement(self, stock_code: str, priority: int = 2) -> List[Dict[str, Any]]:
        """获取利润表（用户查询，默认 medium priority）"""
        return await self._external_data.get_income_statement(stock_code, priority=priority, connected=self.connected)

    async def get_balance_sheet(self, stock_code: str, priority: int = 2) -> List[Dict[str, Any]]:
        """获取资产负债表（用户查询，默认 medium priority）"""
        return await self._external_data.get_balance_sheet(stock_code, priority=priority, connected=self.connected)

    async def get_cash_flow_statement(self, stock_code: str, priority: int = 2) -> List[Dict[str, Any]]:
        """获取现金流量表（用户查询，默认 medium priority）"""
        return await self._external_data.get_cash_flow_statement(stock_code, priority=priority, connected=self.connected)

    async def get_profit_notice(self, stock_code: str, priority: int = 2) -> List[Dict[str, Any]]:
        """获取业绩预告（用户查询，默认 medium priority）"""
        return await self._external_data.get_profit_notice(stock_code, priority=priority, connected=self.connected)

    async def get_profit_express(self, stock_code: str, priority: int = 2) -> List[Dict[str, Any]]:
        """获取业绩快报（用户查询，默认 medium priority）"""
        return await self._external_data.get_profit_express(stock_code, priority=priority, connected=self.connected)

    async def get_long_hu_bang(self, stock_code: str, begin_date: int, end_date: int, priority: int = 2) -> List[Dict[str, Any]]:
        """获取龙虎榜（用户查询，默认 medium priority）"""
        return await self._external_data.get_long_hu_bang(stock_code, begin_date, end_date, priority=priority, connected=self.connected)

    async def get_margin_summary(self, begin_date: int, end_date: int, priority: int = 2) -> List[Dict[str, Any]]:
        """获取融资融券汇总（用户查询，默认 medium priority）"""
        return await self._external_data.get_margin_summary(begin_date, end_date, priority=priority, connected=self.connected)

    async def get_margin_detail(self, stock_code: str, begin_date: int, end_date: int, priority: int = 2) -> List[Dict[str, Any]]:
        """获取融资融券明细（用户查询，默认 medium priority）"""
        return await self._external_data.get_margin_detail(stock_code, begin_date, end_date, priority=priority, connected=self.connected)

    async def get_block_trading(self, stock_code: str, begin_date: int, end_date: int, priority: int = 2) -> List[Dict[str, Any]]:
        """获取大宗交易（用户查询，默认 medium priority）"""
        return await self._external_data.get_block_trading(stock_code, begin_date, end_date, priority=priority, connected=self.connected)

    async def get_treasury_yield(self, priority: int = 2) -> List[Dict[str, Any]]:
        """获取国债收益率（用户查询，默认 medium priority）"""
        return await self._external_data.get_treasury_yield(priority=priority, connected=self.connected)

    # ── 代码列表/日历/快照 ──

    async def get_code_list(self, security_type: str = 'EXTRA_STOCK_A', priority: int = 2) -> List[str]:
        """获取证券代码列表"""
        from services.common.sdk_manager import get_sdk_manager
        sdk_mgr = get_sdk_manager()
        import asyncio
        code_list = await asyncio.to_thread(sdk_mgr.get_code_list, security_type, priority)
        return code_list

    async def get_calendar(self, market: str = 'SH', priority: int = 2) -> List[int]:
        """获取交易日历"""
        from services.common.sdk_manager import get_sdk_manager
        sdk_mgr = get_sdk_manager()
        import asyncio
        calendar = await asyncio.to_thread(sdk_mgr.get_calendar, priority)
        return calendar

    async def query_snapshot(self, code_list: List[str], begin_date: int, end_date: int, priority: int = 2) -> Dict:
        """查询历史快照数据"""
        from services.common.sdk_manager import get_sdk_manager
        sdk_mgr = get_sdk_manager()
        import asyncio
        snapshot = await asyncio.to_thread(sdk_mgr.query_snapshot, code_list, begin_date, end_date, priority)
        return snapshot

    # ── 交易执行 ──

    async def buy(self, stock_code: str, price: float, quantity: int, account_id: str = None) -> OrderResult:
        return await self._trading.buy(stock_code, price, quantity, account_id, connected=self.connected)

    async def sell(self, stock_code: str, price: float, quantity: int, account_id: str = None) -> OrderResult:
        return await self._trading.sell(stock_code, price, quantity, account_id, connected=self.connected)

    async def cancel_order(self, order_no: str, account_id: str = None) -> dict:
        return await self._trading.cancel_order(order_no, account_id)

    async def query_order_status(self, order_no: str, account_id: str = None) -> dict:
        return await self._trading.query_order_status(order_no, account_id)

    async def cancel_all_pending_orders(self, account_id: str = None) -> dict:
        return await self._trading.cancel_all_pending_orders(account_id)

    # ── 订阅/调度器委托 ──

    def subscribe(self, subscriber_id: str, stock_codes: Set[str],
                  refresh_interval: int = 30, priority: int = 2):
        from services.trading.gateway_dispatcher import get_gateway_dispatcher
        get_gateway_dispatcher().subscribe(subscriber_id, stock_codes, refresh_interval, priority)

    def unsubscribe(self, subscriber_id: str):
        from services.trading.gateway_dispatcher import get_gateway_dispatcher
        get_gateway_dispatcher().unsubscribe(subscriber_id)

    async def refresh_now(self, subscriber_id: str) -> Dict[str, Any]:
        from services.trading.gateway_dispatcher import get_gateway_dispatcher
        return await get_gateway_dispatcher().refresh_now(subscriber_id)

    def get_dispatch_status(self) -> Dict[str, Any]:
        from services.trading.gateway_dispatcher import get_gateway_dispatcher
        return get_gateway_dispatcher().get_status()

    # ── 列表查询 ──

    async def get_stock_list(self) -> List[Dict[str, str]]:
        if not self.connected:
            raise Exception("网关未连接")
        try:
            import asyncio
            stock_list = await asyncio.to_thread(self._query_stock_list_sync)
            return stock_list
        except Exception as e:
            logger.error(f"获取股票列表失败：{e}")
            raise Exception(f"获取股票列表失败 - {str(e)}")

    def _query_stock_list_sync(self) -> List[Dict[str, str]]:
        from services.common.sdk_manager import get_sdk_manager
        from services.common.database import get_sync_connection

        stock_list = []
        try:
            conn = get_sync_connection("kline")
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='kline_data'")
            if cursor.fetchone():
                cursor.execute("SELECT DISTINCT stock_code FROM kline_data")
                stock_codes = [row[0] for row in cursor.fetchall()]
                if stock_codes:
                    for code in stock_codes:
                        parts = code.split('.')
                        if len(parts) == 2:
                            stock_list.append({"code": parts[0], "name": "", "market": parts[1]})
                    logger.info(f"从本地数据库获取到 {len(stock_list)} 只股票")
                    return stock_list
        except Exception as e:
            logger.warning(f"从本地数据库获取股票列表失败：{e}")

        try:
            sdk_mgr = get_sdk_manager()
            code_info = sdk_mgr.get_code_info(security_type='EXTRA_STOCK_A')
            if code_info is not None and len(code_info) > 0:
                for idx, row in code_info.iterrows():
                    code = str(idx)
                    symbol = row.get('symbol', '')
                    security_status = row.get('security_status', '')
                    code_without_suffix = code.split('.')[0] if '.' in code else code
                    market = code.split('.')[1] if '.' in code else ("SH" if code.startswith('6') else "SZ")
                    stock_list.append({
                        "code": code_without_suffix, "name": symbol or code_without_suffix,
                        "market": market, "status": security_status
                    })
                return stock_list
        except Exception as e:
            logger.warning(f"SDK获取股票列表失败：{e}")

        raise Exception("无法获取股票列表，请先下载基础数据或检查SDK连接")

    async def get_index_list(self) -> List[Dict[str, str]]:
        if not self.connected:
            raise Exception("网关未连接")
        try:
            import asyncio
            return await asyncio.to_thread(self._query_index_list_sync)
        except Exception as e:
            logger.error(f"获取指数列表失败：{e}")
            raise Exception(f"获取指数列表失败 - {str(e)}")

    def _query_index_list_sync(self) -> List[Dict[str, str]]:
        from services.common.sdk_manager import get_sdk_manager
        major_indices = [
            {'code': '000001', 'name': '上证指数', 'market': 'SH'},
            {'code': '000016', 'name': '上证50', 'market': 'SH'},
            {'code': '000300', 'name': '沪深300', 'market': 'SH'},
            {'code': '000905', 'name': '中证500', 'market': 'SH'},
            {'code': '000852', 'name': '中证1000', 'market': 'SH'},
            {'code': '399001', 'name': '深证成指', 'market': 'SZ'},
            {'code': '399006', 'name': '创业板指', 'market': 'SZ'},
            {'code': '399005', 'name': '中小板指', 'market': 'SZ'},
        ]
        try:
            sdk_mgr = get_sdk_manager()
            code_list = sdk_mgr.get_code_list(security_type='EXTRA_INDEX_A_SH_SZ')
            if code_list:
                code_info = None
                try:
                    code_info = sdk_mgr.get_code_info(security_type='EXTRA_INDEX_A_SH_SZ')
                except Exception as e:
                    logger.warning(f"获取指数名称信息失败：{e}")
                index_list = []
                for code in code_list:
                    code_str = str(code)
                    code_without_suffix = code_str.split('.')[0] if '.' in code_str else code_str
                    market = code_str.split('.')[1] if '.' in code_str else ("SH" if code_str.startswith('0') else "SZ")
                    name = code_without_suffix
                    if code_info is not None:
                        for idx, row in code_info.iterrows():
                            if str(idx) == code_str:
                                name = row.get('symbol', code_without_suffix)
                                break
                    index_list.append({"code": code_without_suffix, "name": name, "market": market})
                return index_list
        except Exception as e:
            logger.warning(f"SDK获取指数列表失败：{e}, 使用预设指数列表")
        return major_indices

    async def get_industry_index_list(self) -> List[Dict[str, str]]:
        if not self.connected:
            raise Exception("网关未连接")
        try:
            import asyncio
            return await asyncio.to_thread(self._query_industry_index_list_sync)
        except Exception as e:
            logger.error(f"获取行业指数列表失败：{e}")
            return []

    def _query_industry_index_list_sync(self) -> List[Dict[str, str]]:
        from services.common.sdk_manager import get_sdk_manager
        try:
            sdk_mgr = get_sdk_manager()
            industry_info = sdk_mgr.get_industry_base_info()
            if industry_info is not None and len(industry_info) > 0:
                level1 = industry_info[industry_info['LEVEL_TYPE'] == 1]
                index_list = []
                for idx, row in level1.iterrows():
                    index_code = row.get('INDEX_CODE', '')
                    level1_name = row.get('LEVEL1_NAME', '')
                    code_without_suffix = str(index_code).split('.')[0] if '.' in str(index_code) else str(index_code)
                    market = str(index_code).split('.')[1] if '.' in str(index_code) else 'SI'
                    index_list.append({
                        "code": code_without_suffix, "name": level1_name,
                        "market": market, "level": 1, "type": "industry"
                    })
                return index_list
        except Exception as e:
            logger.warning(f"SDK获取行业指数列表失败：{e}")
        return []

    # ── 持仓/委托（SDK 暂不支持） ──

    async def get_positions(self) -> List[Dict[str, Any]]:
        if not self.connected:
            return []
        return []

    async def get_orders(self) -> List[Dict[str, Any]]:
        if not self.connected:
            return []
        return []


class ErrorReportingGateway(TradingGatewayInterface):
    """错误报告网关 - 当真实数据不可用时返回具体错误原因"""

    def __init__(self, error_message: str):
        self.error_message = error_message
        self.connected = True

    async def connect(self) -> bool:
        self.connected = True
        return True

    async def disconnect(self):
        self.connected = False

    async def get_market_data(self, stock_code: str) -> Optional[MarketData]:
        if not self.connected:
            return None
        raise Exception(self.error_message)

    async def get_stock_list(self) -> List[Dict[str, str]]:
        if not self.connected:
            return []
        raise Exception(self.error_message)

    async def buy(self, stock_code: str, price: float, quantity: int) -> OrderResult:
        return OrderResult(False, message=self.error_message)

    async def sell(self, stock_code: str, price: float, quantity: int) -> OrderResult:
        return OrderResult(False, message=self.error_message)

    async def get_positions(self) -> List[Dict[str, Any]]:
        return []

    async def get_orders(self) -> List[Dict[str, Any]]:
        return []

    async def get_index_list(self) -> List[Dict[str, str]]:
        raise Exception(self.error_message)

    async def get_industry_index_list(self) -> List[Dict[str, str]]:
        return []

    async def get_batch_market_data(self, stock_codes: List[str]) -> Dict[str, Optional[MarketData]]:
        return {}

    async def get_kline_data(self, stock_code: str, period: str = "day", start_date: str = None, end_date: str = None, limit: int = 100) -> List[Dict[str, Any]]:
        raise Exception(self.error_message)

    async def get_code_info(self, security_type: str = 'EXTRA_STOCK_A') -> List[Dict[str, Any]]:
        return []

    async def get_industry_list(self, level: int = 1) -> List[Dict[str, Any]]:
        return []

    async def get_industry_kline(self, index_code: str) -> List[Dict[str, Any]]:
        return []

    async def get_industry_constituent(self, index_code: str) -> List[Dict[str, Any]]:
        return []

    async def get_index_constituent(self, index_code: str) -> List[Dict[str, Any]]:
        return []

    async def get_income_statement(self, stock_code: str) -> List[Dict[str, Any]]:
        return []

    async def get_balance_sheet(self, stock_code: str) -> List[Dict[str, Any]]:
        return []

    async def get_cash_flow_statement(self, stock_code: str) -> List[Dict[str, Any]]:
        return []

    async def get_profit_notice(self, stock_code: str) -> List[Dict[str, Any]]:
        return []

    async def get_profit_express(self, stock_code: str) -> List[Dict[str, Any]]:
        return []

    async def get_long_hu_bang(self, stock_code: str, begin_date: int, end_date: int) -> List[Dict[str, Any]]:
        return []

    async def get_margin_summary(self, begin_date: int, end_date: int) -> List[Dict[str, Any]]:
        return []

    async def get_margin_detail(self, stock_code: str, begin_date: int, end_date: int) -> List[Dict[str, Any]]:
        return []

    async def get_block_trading(self, stock_code: str, begin_date: int, end_date: int) -> List[Dict[str, Any]]:
        return []

    async def get_treasury_yield(self) -> List[Dict[str, Any]]:
        return []


# ── 工厂函数 ──

def create_gateway(galaxy_app_id: str = "", galaxy_password: str = "") -> TradingGatewayInterface:
    gateway = TradingGateway(app_id=galaxy_app_id, password=galaxy_password)
    if not gateway.sdk_available:
        raise Exception("AmazingData SDK 不可用，无法创建交易网关")
    return gateway


_gateway: Optional[TradingGatewayInterface] = None
_gateway_cache: Dict[str, TradingGatewayInterface] = {}


def clear_gateway_cache():
    global _gateway
    _gateway_cache.clear()
    _gateway = None


async def get_gateway_for_account(broker_credentials: Optional[Dict[str, str]] = None) -> TradingGatewayInterface:
    if not broker_credentials or not broker_credentials.get("broker_account"):
        return await get_gateway()

    account_id = broker_credentials.get("broker_account")
    if account_id in _gateway_cache:
        return _gateway_cache[account_id]

    gateway = create_gateway(
        galaxy_app_id=broker_credentials.get("broker_account", ""),
        galaxy_password=broker_credentials.get("broker_password", "")
    )
    await gateway.connect()
    _gateway_cache[account_id] = gateway
    logger.info(f"为账户 {account_id} 创建了独立的交易网关")
    return gateway


async def get_gateway() -> TradingGatewayInterface:
    global _gateway
    if _gateway is None:
        _gateway = create_gateway()
        await _gateway.connect()
    return _gateway


async def get_gateway_with_credentials(auth_token: Optional[str] = None) -> TradingGatewayInterface:
    if not auth_token:
        return await get_gateway()
    try:
        from services.auth.service import get_auth_service
        auth_service = get_auth_service()
        account = auth_service.validate_token(auth_token)
        if not account:
            logger.warning("无效的认证 token，使用默认网关")
            return await get_gateway()

        broker_creds = {
            "broker_account": account.get("broker_account", ""),
            "broker_password": account.get("broker_password", ""),
        }
        if not broker_creds["broker_account"]:
            logger.warning(f"用户 {account.get('username')} 未配置券商账号，使用默认网关")
            return await get_gateway()

        return await get_gateway_for_account(broker_creds)
    except Exception as e:
        logger.error(f"获取带 credentials 的网关失败：{e}")
        return await get_gateway()


async def init_gateway(galaxy_app_id: str = "", galaxy_password: str = ""):
    global _gateway
    if _gateway:
        await _gateway.disconnect()
    _gateway = create_gateway(galaxy_app_id=galaxy_app_id, galaxy_password=galaxy_password)
    await _gateway.connect()
    return _gateway
