"""
交易网关
- 当 AmazingData SDK 可用时，使用真实交易接口
- 当 SDK 不可用时，抛出异常
"""
import asyncio
import logging
import math
import datetime
from datetime import timedelta
from typing import Optional, Dict, Any, List, Set
from abc import ABC, abstractmethod

import numpy as np

from services.common.timezone import get_china_time

logger = logging.getLogger(__name__)

# 为了解决pandas版本兼容性问题，临时替换有问题的pandas功能
def patch_pandas_frequency_issue():
    """修补pandas频率字符串兼容性问题"""
    try:
        import pandas as pd
        import pandas._libs.tslibs.offsets as offsets

        # 修补 pandas 频率字符串映射：将 "S"(秒) 映射为 "s"
        # SDK 内部使用大写 "S"，但 pandas 3.x 只认小写 "s"
        orig_to_offset = getattr(offsets, 'to_offset', None)
        if orig_to_offset:
            def _patched_to_offset(freqstr, *args, **kwargs):
                if isinstance(freqstr, str):
                    freqstr = freqstr.replace('S', 's')
                return orig_to_offset(freqstr, *args, **kwargs)
            offsets.to_offset = _patched_to_offset

    except Exception as e:
        logger.warning(f"pandas兼容性修补失败: {e}")


# 在导入时尝试修补
patch_pandas_frequency_issue()


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
                 trade_date: str = ""):
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
        self.bid = bid or []  # 买一至买五价格
        self.ask = ask or []  # 卖一至卖五价格
        self.bid_volume = bid_volume or []  # 买一至买五委托量（股）
        self.ask_volume = ask_volume or []  # 卖一至卖五委托量（股）
        self.trade_date = trade_date


class TradingGatewayInterface(ABC):
    """交易网关接口"""

    @abstractmethod
    async def connect(self) -> bool:
        """连接交易服务器"""
        pass

    @abstractmethod
    async def disconnect(self):
        """断开连接"""
        pass

    @abstractmethod
    async def get_market_data(self, stock_code: str) -> Optional[MarketData]:
        """获取行情数据"""
        pass

    @abstractmethod
    async def get_stock_list(self) -> List[Dict[str, str]]:
        """获取股票列表"""
        pass

    @abstractmethod
    async def get_index_list(self) -> List[Dict[str, str]]:
        """获取交易所指数列表"""
        pass

    @abstractmethod
    async def get_industry_index_list(self) -> List[Dict[str, str]]:
        """获取行业指数列表（申万行业分类）"""
        pass

    @abstractmethod
    async def buy(self, stock_code: str, price: float, quantity: int) -> OrderResult:
        """买入"""
        pass

    @abstractmethod
    async def sell(self, stock_code: str, price: float, quantity: int) -> OrderResult:
        """卖出"""
        pass

    @abstractmethod
    async def get_positions(self) -> List[Dict[str, Any]]:
        """获取持仓"""
        pass

    @abstractmethod
    async def get_orders(self) -> List[Dict[str, Any]]:
        """获取委托单"""
        pass

    @abstractmethod
    async def get_batch_market_data(self, stock_codes: List[str]) -> Dict[str, Optional[MarketData]]:
        """批量获取行情数据"""
        pass

    @abstractmethod
    async def get_kline_data(
        self,
        stock_code: str,
        period: str = "day",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """获取 K 线历史数据"""
        pass

    @abstractmethod
    async def get_code_info(self, security_type: str = 'EXTRA_STOCK_A') -> List[Dict[str, Any]]:
        """获取股票代码信息"""
        pass

    @abstractmethod
    async def get_industry_list(self, level: int = 1) -> List[Dict[str, Any]]:
        """申万行业分类列表"""
        pass

    @abstractmethod
    async def get_industry_kline(self, index_code: str) -> List[Dict[str, Any]]:
        """行业指数日行情"""
        pass

    @abstractmethod
    async def get_industry_constituent(self, index_code: str) -> List[Dict[str, Any]]:
        """行业成分股列表"""
        pass

    @abstractmethod
    async def get_index_constituent(self, index_code: str) -> List[Dict[str, Any]]:
        """指数成分股列表"""
        pass

    @abstractmethod
    async def get_income_statement(self, stock_code: str) -> List[Dict[str, Any]]:
        """利润表"""
        pass

    @abstractmethod
    async def get_balance_sheet(self, stock_code: str) -> List[Dict[str, Any]]:
        """资产负债表"""
        pass

    @abstractmethod
    async def get_cash_flow_statement(self, stock_code: str) -> List[Dict[str, Any]]:
        """现金流量表"""
        pass

    @abstractmethod
    async def get_profit_notice(self, stock_code: str) -> List[Dict[str, Any]]:
        """业绩预告"""
        pass

    @abstractmethod
    async def get_profit_express(self, stock_code: str) -> List[Dict[str, Any]]:
        """业绩快报"""
        pass

    @abstractmethod
    async def get_long_hu_bang(self, stock_code: str, begin_date: int, end_date: int) -> List[Dict[str, Any]]:
        """龙虎榜数据"""
        pass

    @abstractmethod
    async def get_margin_summary(self, begin_date: int, end_date: int) -> List[Dict[str, Any]]:
        """融资融券汇总"""
        pass

    @abstractmethod
    async def get_margin_detail(self, stock_code: str, begin_date: int, end_date: int) -> List[Dict[str, Any]]:
        """融资融券明细"""
        pass

    @abstractmethod
    async def get_block_trading(self, stock_code: str, begin_date: int, end_date: int) -> List[Dict[str, Any]]:
        """大宗交易数据"""
        pass

    @abstractmethod
    async def get_treasury_yield(self) -> List[Dict[str, Any]]:
        """国债收益率曲线"""
        pass




class TradingGateway(TradingGatewayInterface):
    """交易网关（使用 AmazingData SDK）"""

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

    @property
    def connected(self) -> bool:
        """从 SDKManager 读取连接状态"""
        if not self.sdk_available:
            return False
        from services.common.sdk_manager import get_sdk_manager
        return get_sdk_manager().is_connected()

    def _query_kline_via_sdk(self, code_list: list, begin_date: int, end_date: int, period: int, task_type: str = "query") -> dict:
        """通过 SDKManager 查询K线数据（自动排队）"""
        from services.common.sdk_manager import get_sdk_manager
        sdk_mgr = get_sdk_manager()
        return sdk_mgr.query_kline(code_list=code_list, begin_date=begin_date,
                                   end_date=end_date, period=period, task_type=task_type)

    async def connect(self) -> bool:
        """连接交易服务器 — 委托给 SDKManager"""
        if not self.sdk_available:
            logger.error("AmazingData SDK 未安装，无法连接")
            return False
        try:
            from services.common.sdk_manager import get_sdk_manager
            sdk_mgr = get_sdk_manager()
            return sdk_mgr.connect()
        except Exception as e:
            logger.error(f"交易网关连接失败：{e}")
            return False

    async def disconnect(self):
        """断开连接 — 委托给 SDKManager"""
        from services.common.sdk_manager import get_sdk_manager
        get_sdk_manager().disconnect()

    async def get_market_data(self, stock_code: str) -> Optional[MarketData]:
        """获取真实行情数据 — 优先读 PriceCache（TTL 检查），未命中则并发走 SDK + 备用通道，先赢者胜"""
        if not self.connected:
            raise Exception("网关未连接")

        # ① 优先从 PriceCache 读取（带 TTL 检查）
        try:
            from services.common.price_cache import get_price_cache
            cache = get_price_cache()
            entry = cache.get_ohlcv_with_ttl(stock_code)
            if entry and entry.get('is_fresh') and entry.get('data', {}).get('close', 0) > 0:
                return self._market_data_from_ohlcv(entry['data'], stock_code)
        except Exception:
            pass

        # ② 缓存未命中/过期 → 并发：SDK 路径 + 备用 Provider 路径，先赢者胜
        md = await self._race_market_data(stock_code)
        if md and md.current_price > 0:
            return md

        # ③ kline.db 最终兜底（仅非交易时段）
        try:
            kline_results = await self._fill_stale_from_kline_db({stock_code})
            if stock_code in kline_results:
                return kline_results[stock_code]
        except Exception:
            pass

        logger.error(f"获取行情失败：{stock_code}")
        raise Exception(f"获取行情失败：所有数据源均不可用")

    async def _race_market_data(self, stock_code: str) -> Optional[MarketData]:
        """并发获取行情数据：SDK 与备用 Provider 同时启动，先赢者胜

        当 SDK 被占用时，备用通道（sina/tencent 等 HTTP 接口）通常 100ms 内返回，
        无需等待 SDK 15s 超时。SDK 空闲时两者几乎同时完成，SDK 通常更快。
        """
        sdk_result: Optional[MarketData] = None
        fallback_result: Optional[Dict[str, Any]] = None
        sdk_done = False
        fallback_done = False

        async def try_sdk():
            nonlocal sdk_result, sdk_done
            try:
                from services.trading.gateway_dispatcher import get_gateway_dispatcher
                dispatcher = get_gateway_dispatcher()
                dispatcher.subscribe("_single_race", {stock_code}, interval=0, priority=1)
                results = await dispatcher.refresh_now("_single_race")
                dispatcher.unsubscribe("_single_race")
                md = results.get(stock_code)
                if md and md.current_price > 0:
                    sdk_result = md
            except Exception as e:
                logger.debug(f"SDK 行情查询失败: {e}")
            finally:
                sdk_done = True

        async def try_fallback():
            nonlocal fallback_result, fallback_done
            try:
                from services.data.channel import get_channel_router, ChannelType
                router = get_channel_router()
                raw_data = await router.execute(
                    ChannelType.TRADING,
                    "get_market_data",
                    stock_code=stock_code,
                )
                if raw_data and raw_data.get("current_price", 0) > 0:
                    fallback_result = raw_data
            except Exception:
                pass
            finally:
                fallback_done = True

        # 并发执行两条路径
        await asyncio.gather(try_sdk(), try_fallback(), return_exceptions=True)

        # 优先用 SDK 结果（数据质量更高，含五档），备用结果作为降级
        if sdk_result:
            return sdk_result
        if fallback_result:
            return MarketData(
                stock_code=fallback_result.get("stock_code", stock_code),
                stock_name=fallback_result.get("stock_name", ""),
                current_price=float(fallback_result.get("current_price", 0)),
                change_percent=float(fallback_result.get("change_percent", 0)),
                high=float(fallback_result.get("high", 0)),
                low=float(fallback_result.get("low", 0)),
                open_price=float(fallback_result.get("open_price", 0)),
                prev_close=float(fallback_result.get("prev_close", 0)),
                volume=int(fallback_result.get("volume", 0)),
                amount=float(fallback_result.get("amount", 0)),
                bid=fallback_result.get("bid", []),
                ask=fallback_result.get("ask", []),
                bid_volume=fallback_result.get("bid_volume", []),
                ask_volume=fallback_result.get("ask_volume", []),
                trade_date=fallback_result.get("trade_date", ""),
            )
        return None

    def _market_data_from_ohlcv(self, ohlcv: Dict[str, float], stock_code: str) -> MarketData:
        """从 OHLCV 字典构造 MarketData（缓存数据，五档用现价填充）"""
        close = ohlcv.get('close', 0)
        return MarketData(
            stock_code=stock_code,
            stock_name=stock_code,
            current_price=close,
            change_percent=ohlcv.get('change_pct', 0),
            high=ohlcv.get('high', close),
            low=ohlcv.get('low', close),
            open_price=ohlcv.get('open', close),
            prev_close=close,
            volume=int(ohlcv.get('volume', 0)),
            amount=ohlcv.get('amount', 0),
            bid=[close] * 5,
            ask=[close] * 5,
            bid_volume=[0] * 5,
            ask_volume=[0] * 5,
            trade_date='',
        )

    async def _fill_stale_from_kline_db(self, codes: Set[str]) -> Dict[str, MarketData]:
        """从 kline.db 读取最新收盘价兜底（仅非交易时段，避免用昨收价误导策略）"""
        from services.trading.trading_hours import can_trade
        if can_trade():
            return {}  # 交易时段不使用 kline.db 兜底
        import sqlite3
        from pathlib import Path
        from services.common.database import configure_kline_connection

        kline_db = Path(__file__).parent.parent.parent / "data" / "kline.db"
        if not kline_db.exists():
            return {}

        results: Dict[str, MarketData] = {}
        try:
            conn = sqlite3.connect(str(kline_db), timeout=10)
            configure_kline_connection(conn)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            placeholders = ','.join(['?'] * len(codes))
            cursor.execute(f"""
                SELECT k.stock_code, k.close, k.open, k.high, k.low, k.volume, k.amount, k.trade_date
                FROM kline_data k
                INNER JOIN (
                    SELECT stock_code, MAX(trade_date) as max_date
                    FROM kline_data
                    WHERE stock_code IN ({placeholders})
                    GROUP BY stock_code
                ) latest ON k.stock_code = latest.stock_code AND k.trade_date = latest.max_date
            """, list(codes))
            for row in cursor.fetchall():
                code = row['stock_code']
                close = float(row['close'] or 0)
                if close > 0:
                    open_p = float(row['open'] or close)
                    high = float(row['high'] or close)
                    low = float(row['low'] or close)
                    vol = float(row['volume'] or 0)
                    amt = float(row['amount'] or 0)
                    results[code] = MarketData(
                        stock_code=code,
                        stock_name=code,
                        current_price=close,
                        change_percent=0,
                        high=high,
                        low=low,
                        open_price=open_p,
                        prev_close=close,
                        volume=int(vol),
                        amount=amt,
                        bid=[close] * 5,
                        ask=[close] * 5,
                        bid_volume=[0] * 5,
                        ask_volume=[0] * 5,
                        trade_date=str(row['trade_date'] or '').replace('-', ''),
                    )
            conn.close()
        except Exception as e:
            logger.debug(f"kline.db 兜底查询失败: {e}")

        return results

    # ── Dispatcher 委托方法 ──

    def subscribe(self, subscriber_id: str, stock_codes: Set[str],
                  refresh_interval: int = 30, priority: int = 2):
        """注册或更新订阅（委托给 gateway dispatcher）"""
        from services.trading.gateway_dispatcher import get_gateway_dispatcher
        get_gateway_dispatcher().subscribe(subscriber_id, stock_codes, refresh_interval, priority)

    def unsubscribe(self, subscriber_id: str):
        """移除订阅"""
        from services.trading.gateway_dispatcher import get_gateway_dispatcher
        get_gateway_dispatcher().unsubscribe(subscriber_id)

    async def refresh_now(self, subscriber_id: str) -> Dict[str, Any]:
        """立即刷新指定订阅的股票"""
        from services.trading.gateway_dispatcher import get_gateway_dispatcher
        return await get_gateway_dispatcher().refresh_now(subscriber_id)

    def get_dispatch_status(self) -> Dict[str, Any]:
        """获取调度器状态（含健康度、数据新鲜度）"""
        from services.trading.gateway_dispatcher import get_gateway_dispatcher
        return get_gateway_dispatcher().get_status()

    async def get_stock_list(self) -> List[Dict[str, str]]:
        """获取股票列表"""
        if not self.connected:
            raise Exception("网关未连接")

        try:
            stock_list = await asyncio.to_thread(self._query_stock_list_sync)
            return stock_list
        except Exception as e:
            logger.error(f"获取股票列表失败：{e}")
            raise Exception(f"获取股票列表失败 - {str(e)}")

    def _query_stock_list_sync(self) -> List[Dict[str, str]]:
        """同步查询股票列表（在线程池中执行）"""
        import sqlite3
        from pathlib import Path
        from services.common.sdk_manager import get_sdk_manager

        stock_list = []

        # 首先尝试从本地数据库获取股票列表
        db_paths = [
            Path('/home/bobo/StockWinner/data/kline.db'),
            Path('/home/bobo/StockWinner/stockwinner.db'),
        ]

        for db_path in db_paths:
            if db_path.exists():
                try:
                    from services.common.database import configure_kline_connection
                    conn = sqlite3.connect(str(db_path))
                    configure_kline_connection(conn)
                    cursor = conn.cursor()
                    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='kline_data'")
                    if cursor.fetchone():
                        cursor.execute("SELECT DISTINCT stock_code FROM kline_data")
                        stock_codes = [row[0] for row in cursor.fetchall()]
                        conn.close()

                        if stock_codes:
                            for code in stock_codes:
                                parts = code.split('.')
                                if len(parts) == 2:
                                    stock_list.append({
                                        "code": parts[0],
                                        "name": "",
                                        "market": parts[1]
                                    })
                            logger.info(f"从本地数据库获取到 {len(stock_list)} 只股票")
                            return stock_list
                    conn.close()
                except Exception as e:
                    logger.warning(f"从本地数据库获取股票列表失败：{e}")

        # 使用 AmazingData SDK 的 get_code_info 获取股票信息
        try:
            sdk_mgr = get_sdk_manager()
            code_info = sdk_mgr.get_code_info(security_type='EXTRA_STOCK_A')

            if code_info is not None and len(code_info) > 0:
                logger.info(f"SDK获取到股票信息：{len(code_info)} 条")

                for idx, row in code_info.iterrows():
                    code = str(idx)
                    symbol = row.get('symbol', '')
                    security_status = row.get('security_status', '')

                    if '.' in code:
                        code_without_suffix = code.split('.')[0]
                        market = code.split('.')[1]
                    else:
                        code_without_suffix = code
                        market = "SH" if code.startswith('6') else "SZ"

                    stock_list.append({
                        "code": code_without_suffix,
                        "name": symbol if symbol else code_without_suffix,
                        "market": market,
                        "status": security_status
                    })

                logger.info(f"SDK股票列表处理完成：{len(stock_list)} 只")
                return stock_list

        except Exception as e:
            logger.warning(f"SDK获取股票列表失败：{e}")

        raise Exception("无法获取股票列表，请先下载基础数据或检查SDK连接")

    async def get_index_list(self) -> List[Dict[str, str]]:
        """获取指数列表"""
        if not self.connected:
            raise Exception("网关未连接")

        try:
            index_list = await asyncio.to_thread(self._query_index_list_sync)
            return index_list
        except Exception as e:
            logger.error(f"获取指数列表失败：{e}")
            raise Exception(f"获取指数列表失败 - {str(e)}")

    def _query_index_list_sync(self) -> List[Dict[str, str]]:
        """同步查询指数列表（在线程池中执行）- 使用 SDK"""
        from services.common.sdk_manager import get_sdk_manager

        # 主要指数代码列表（上交所和深交所）
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

        # 尝试从SDK获取完整指数列表
        try:
            sdk_mgr = get_sdk_manager()
            code_list = sdk_mgr.get_code_list(security_type='EXTRA_INDEX_A_SH_SZ')

            if code_list:
                logger.info(f"SDK获取到指数列表：{len(code_list)} 个")

                # 一次性获取所有指数信息（避免在循环内多次调用 SDK）
                code_info = None
                try:
                    code_info = sdk_mgr.get_code_info(security_type='EXTRA_INDEX_A_SH_SZ')
                except Exception as e:
                    logger.warning(f"获取指数名称信息失败：{e}")

                index_list = []
                for code in code_list:
                    code_str = str(code)
                    if '.' in code_str:
                        code_without_suffix = code_str.split('.')[0]
                        market = code_str.split('.')[1]
                    else:
                        code_without_suffix = code_str
                        market = "SH" if code_str.startswith('0') else "SZ"

                    # 从预先获取的 code_info 中查找名称
                    name = code_without_suffix
                    if code_info is not None:
                        for idx, row in code_info.iterrows():
                            if str(idx) == code_str:
                                name = row.get('symbol', code_without_suffix)
                                break

                    index_list.append({
                        "code": code_without_suffix,
                        "name": name,
                        "market": market
                    })
                return index_list
        except Exception as e:
            logger.warning(f"SDK获取指数列表失败：{e}, 使用预设指数列表")

        # 备用方案：预设的主要指数列表
        return major_indices

    async def get_industry_index_list(self) -> List[Dict[str, str]]:
        """获取行业指数列表（申万行业分类）"""
        if not self.connected:
            raise Exception("网关未连接")

        try:
            index_list = await asyncio.to_thread(self._query_industry_index_list_sync)
            return index_list
        except Exception as e:
            logger.error(f"获取行业指数列表失败：{e}")
            return []

    def _query_industry_index_list_sync(self) -> List[Dict[str, str]]:
        """同步查询行业指数列表 - 使用SDK"""
        from services.common.sdk_manager import get_sdk_manager

        try:
            sdk_mgr = get_sdk_manager()
            industry_info = sdk_mgr.get_industry_base_info()

            if industry_info is not None and len(industry_info) > 0:
                # 只获取一级分类（主要行业）
                level1 = industry_info[industry_info['LEVEL_TYPE'] == 1]
                logger.info(f"SDK获取到行业指数：{len(level1)} 个一级分类")

                index_list = []
                for idx, row in level1.iterrows():
                    index_code = row.get('INDEX_CODE', '')
                    level1_name = row.get('LEVEL1_NAME', '')
                    # 申万行业指数代码格式：801xxx.SI
                    if '.' in str(index_code):
                        code_without_suffix = str(index_code).split('.')[0]
                        market = str(index_code).split('.')[1]
                    else:
                        code_without_suffix = str(index_code)
                        market = 'SI'

                    index_list.append({
                        "code": code_without_suffix,
                        "name": level1_name,
                        "market": market,
                        "level": 1,
                        "type": "industry"
                    })
                return index_list
        except Exception as e:
            logger.warning(f"SDK获取行业指数列表失败：{e}")

        return []

    async def buy(self, stock_code: str, price: float, quantity: int, account_id: str = None) -> OrderResult:
        """买入（支持 mock/实盘切换）

        实盘交易流程（A 股）：
        1. 交易时间检查（trading_hours.py）
        2. 挂单时段判断（集合竞价 / 连续竞价）
        3. 向券商 API 发出买入委托
        4. 轮询查询委托成交情况
        5. 全部成交/部分成交/撤单
        """
        if not self.connected:
            return OrderResult(False, message="网关未连接")

        # 判断交易模式
        is_mock = True
        if account_id:
            try:
                from services.common.database import get_db_manager
                db = get_db_manager()
                acct = await db.fetchone(
                    "SELECT trade_mode FROM accounts WHERE account_id = ?",
                    (account_id,)
                )
                if acct:
                    is_mock = acct.get("trade_mode", "mock") == "mock"
            except Exception as e:
                logger.warning(f"获取账户交易模式失败，默认 mock: {e}")

        if is_mock:
            logger.info(f"[Mock] 买入 {stock_code} {quantity}股 @ {price:.2f}")
            return OrderResult(True, message=f"[Mock] 买入成功 {stock_code} {quantity}股 @ {price:.2f}")
        else:
            # ================================================================
            # 实盘买入 - 调用券商交易 API
            # ================================================================
            # from services.trading.trading_hours import can_place_order, get_trading_phase
            #
            # # 1. 交易时间检查
            # if not can_place_order():
            #     phase = get_trading_phase()
            #     return OrderResult(False, message=f"非挂单时段，当前阶段: {phase.value}")
            #
            # # 2. 获取券商交易连接
            # broker = await self._get_broker_connection(account_id)
            # if not broker:
            #     return OrderResult(False, message="券商交易连接不可用")
            #
            # # 3. 发出买入委托（限价单）
            # # A 股买入规则：
            # # - 价格可以是限价或市价
            # # - 数量必须是 100 的整数倍（1 手 = 100 股）
            # # - 涨停价 = 前收盘价 × 1.1（主板），创业板/科创板 × 1.2
            # try:
            #     order_no = await broker.place_buy_order(
            #         stock_code=stock_code,
            #         price=price,
            #         quantity=quantity,
            #         order_type="limit",  # limit=限价, market=市价
            #     )
            #     logger.info(f"[实盘] 买入委托已提交: {stock_code} {quantity}股 @ {price:.2f}, 委托号={order_no}")
            #
            #     # 4. 轮询查询成交情况
            #     # 实盘委托是异步的，需要轮询确认成交
            #     max_retries = 60  # 最多轮询 60 次
            #     poll_interval = 2  # 每 2 秒查询一次
            #     import asyncio
            #     for i in range(max_retries):
            #         await asyncio.sleep(poll_interval)
            #         order_status = await broker.query_order(order_no)
            #
            #         if order_status["status"] == "filled":
            #             # 全部成交
            #             filled_price = order_status.get("avg_price", price)
            #             filled_qty = order_status.get("filled_qty", quantity)
            #             logger.info(f"[实盘] 买入全部成交: {stock_code} {filled_qty}股 @ {filled_price:.2f}")
            #             return OrderResult(
            #                 True,
            #                 order_id=order_no,
            #                 message=f"实盘买入 {stock_code} {filled_qty}股 @ {filled_price:.2f}",
            #             )
            #         elif order_status["status"] == "partial":
            #             # 部分成交，继续等待
            #             filled_qty = order_status.get("filled_qty", 0)
            #             logger.info(f"[实盘] 买入部分成交: {stock_code} {filled_qty}/{quantity}股")
            #         elif order_status["status"] in ("cancelled", "rejected"):
            #             # 被拒绝或已撤销
            #             reason = order_status.get("reason", "未知原因")
            #             logger.warning(f"[实盘] 买入委托失败: {stock_code}, 原因: {reason}")
            #             return OrderResult(False, message=f"实盘买入失败: {reason}")
            #         # else: pending, 继续等待
            #
            #     # 5. 超时未全部成交，发出撤单指令
            #     logger.warning(f"[实盘] 买入委托超时，尝试撤单: {stock_code} {order_no}")
            #     cancel_result = await broker.cancel_order(order_no)
            #     if cancel_result:
            #         logger.info(f"[实盘] 撤单成功: {order_no}")
            #     return OrderResult(
            #         False,
            #         order_id=order_no,
            #         message=f"买入委托超时未成交，已撤单: {stock_code}",
            #     )
            #
            # except Exception as e:
            #     logger.error(f"[实盘] 买入异常: {stock_code}: {e}")
            #     return OrderResult(False, message=f"实盘买入异常: {e}")
            # ================================================================
            logger.warning(f"实盘交易接口尚未接入，当前为 mock 模式")
            return OrderResult(False, message="SDK 实盘交易接口待实现")

    async def sell(self, stock_code: str, price: float, quantity: int, account_id: str = None) -> OrderResult:
        """卖出（支持 mock/实盘切换）

        实盘交易流程（A 股）：
        1. 交易时间检查
        2. 挂单（限价/市价）
        3. 轮询成交
        4. 收盘前撤销全部未成交委托
        """
        if not self.connected:
            return OrderResult(False, message="网关未连接")

        # 判断交易模式
        is_mock = True
        if account_id:
            try:
                from services.common.database import get_db_manager
                db = get_db_manager()
                acct = await db.fetchone(
                    "SELECT trade_mode FROM accounts WHERE account_id = ?",
                    (account_id,)
                )
                if acct:
                    is_mock = acct.get("trade_mode", "mock") == "mock"
            except Exception as e:
                logger.warning(f"获取账户交易模式失败，默认 mock: {e}")

        if is_mock:
            logger.info(f"[Mock] 卖出 {stock_code} {quantity}股 @ {price:.2f}")
            return OrderResult(True, message=f"[Mock] 卖出成功 {stock_code} {quantity}股 @ {price:.2f}")
        else:
            # ================================================================
            # 实盘卖出 - 调用券商交易 API
            # ================================================================
            # from services.trading.trading_hours import (
            #     can_place_order, can_cancel_order, get_trading_phase,
            #     should_cancel_all_orders
            # )
            #
            # # 1. 交易时间检查
            # if not can_place_order():
            #     phase = get_trading_phase()
            #     return OrderResult(False, message=f"非挂单时段，当前阶段: {phase.value}")
            #
            # # 2. 获取券商交易连接
            # broker = await self._get_broker_connection(account_id)
            # if not broker:
            #     return OrderResult(False, message="券商交易连接不可用")
            #
            # # 3. 发出卖出委托（限价单）
            # # A 股卖出规则：
            # # - 卖出数量必须是 100 的整数倍（零股一次性卖出除外）
            # # - T+1 规则：当日买入的股票次日才能卖出
            # # - 跌停价 = 前收盘价 × 0.9（主板），创业板/科创板 × 0.8
            # try:
            #     order_no = await broker.place_sell_order(
            #         stock_code=stock_code,
            #         price=price,
            #         quantity=quantity,
            #         order_type="limit",  # limit=限价, market=市价
            #     )
            #     logger.info(f"[实盘] 卖出委托已提交: {stock_code} {quantity}股 @ {price:.2f}, 委托号={order_no}")
            #
            #     # 4. 轮询查询成交情况
            #     max_retries = 60
            #     poll_interval = 2
            #     import asyncio
            #     for i in range(max_retries):
            #         await asyncio.sleep(poll_interval)
            #         order_status = await broker.query_order(order_no)
            #
            #         if order_status["status"] == "filled":
            #             filled_price = order_status.get("avg_price", price)
            #             filled_qty = order_status.get("filled_qty", quantity)
            #             logger.info(f"[实盘] 卖出全部成交: {stock_code} {filled_qty}股 @ {filled_price:.2f}")
            #             return OrderResult(
            #                 True,
            #                 order_id=order_no,
            #                 message=f"实盘卖出 {stock_code} {filled_qty}股 @ {filled_price:.2f}",
            #             )
            #         elif order_status["status"] == "partial":
            #             filled_qty = order_status.get("filled_qty", 0)
            #             logger.info(f"[实盘] 卖出部分成交: {stock_code} {filled_qty}/{quantity}股")
            #         elif order_status["status"] in ("cancelled", "rejected"):
            #             reason = order_status.get("reason", "未知原因")
            #             logger.warning(f"[实盘] 卖出委托失败: {stock_code}, 原因: {reason}")
            #             return OrderResult(False, message=f"实盘卖出失败: {reason}")
            #
            #     # 5. 超时未成交，发出撤单指令
            #     logger.warning(f"[实盘] 卖出委托超时，尝试撤单: {stock_code} {order_no}")
            #     cancel_result = await broker.cancel_order(order_no)
            #     if cancel_result:
            #         logger.info(f"[实盘] 撤单成功: {order_no}")
            #     return OrderResult(
            #         False,
            #         order_id=order_no,
            #         message=f"卖出委托超时未成交，已撤单: {stock_code}",
            #     )
            #
            # except Exception as e:
            #     logger.error(f"[实盘] 卖出异常: {stock_code}: {e}")
            #     return OrderResult(False, message=f"实盘卖出异常: {e}")
            # ================================================================
            logger.warning(f"实盘交易接口尚未接入，当前为 mock 模式")
            return OrderResult(False, message="SDK 实盘交易接口待实现")

    async def cancel_order(self, order_no: str, account_id: str = None) -> dict:
        """撤销委托单（实盘）

        Args:
            order_no: 券商委托编号
            account_id: 账户 ID

        Returns:
            {"success": bool, "message": str}

        撤单规则：
        - 09:15-09:20 可撤单
        - 09:20-09:25 不可撤单（集合竞价锁定）
        - 09:25-09:30 不可撤单（撮合等待）
        - 09:30-11:30 可撤单（连续竞价上午）
        - 11:30-13:00 不可撤单（午间休市）
        - 13:00-15:00 可撤单（连续竞价下午）
        """
        # ================================================================
        # 实盘撤单 - 调用券商交易 API
        # ================================================================
        # from services.trading.trading_hours import can_cancel_order
        #
        # if not can_cancel_order():
        #     return {"success": False, "message": "当前时段不可撤单"}
        #
        # broker = await self._get_broker_connection(account_id)
        # if not broker:
        #     return {"success": False, "message": "券商交易连接不可用"}
        #
        # try:
        #     result = await broker.cancel_order(order_no)
        #     return {"success": result, "message": "撤单成功" if result else "撤单失败"}
        # except Exception as e:
        #     return {"success": False, "message": f"撤单异常: {e}"}
        # ================================================================
        return {"success": False, "message": "实盘交易接口待实现"}

    async def query_order_status(self, order_no: str, account_id: str = None) -> dict:
        """查询委托成交情况（实盘）

        Args:
            order_no: 券商委托编号
            account_id: 账户 ID

        Returns:
            {
                "order_no": str,
                "status": "pending" | "partial" | "filled" | "cancelled" | "rejected",
                "filled_qty": int,
                "avg_price": float,
                "message": str,
            }
        """
        # ================================================================
        # 实盘查询 - 调用券商交易 API
        # ================================================================
        # broker = await self._get_broker_connection(account_id)
        # if not broker:
        #     return {"status": "unknown", "message": "券商交易连接不可用"}
        #
        # try:
        #     status = await broker.query_order(order_no)
        #     return {
        #         "order_no": order_no,
        #         "status": status.get("status", "unknown"),
        #         "filled_qty": status.get("filled_qty", 0),
        #         "avg_price": status.get("avg_price", 0),
        #         "message": status.get("message", ""),
        #     }
        # except Exception as e:
        #     return {"status": "error", "message": f"查询异常: {e}"}
        # ================================================================
        return {"status": "unknown", "message": "实盘交易接口待实现"}

    async def cancel_all_pending_orders(self, account_id: str = None) -> dict:
        """收盘后撤销全部未成交委托（实盘）

        每个交易日 15:00 收盘后自动调用，
        撤销当日所有未成交（pending/submitted/partial）的委托。

        Returns:
            {
                "total": int,
                "cancelled": int,
                "failed": int,
                "messages": List[str],
            }
        """
        # ================================================================
        # 实盘全撤 - 调用券商交易 API
        # ================================================================
        # from services.trading.trading_hours import should_cancel_all_orders
        #
        # broker = await self._get_broker_connection(account_id)
        # if not broker:
        #     return {"total": 0, "cancelled": 0, "failed": 0, "messages": ["券商连接不可用"]}
        #
        # try:
        #     # 获取当日全部未成交委托
        #     pending_orders = await broker.get_pending_orders()
        #     total = len(pending_orders)
        #     cancelled = 0
        #     failed = 0
        #     messages = []
        #
        #     for order in pending_orders:
        #         order_no = order.get("order_no")
        #         try:
        #             result = await broker.cancel_order(order_no)
        #             if result:
        #                 cancelled += 1
        #                 messages.append(f"已撤销 {order_no}: {order.get('stock_code')}")
        #             else:
        #                 failed += 1
        #                 messages.append(f"撤单失败 {order_no}")
        #         except Exception as e:
        #             failed += 1
        #             messages.append(f"撤单异常 {order_no}: {e}")
        #
        #     logger.info(f"[实盘] 收盘全撤: 总委托 {total}, 成功 {cancelled}, 失败 {failed}")
        #     return {"total": total, "cancelled": cancelled, "failed": failed, "messages": messages}
        #
        # except Exception as e:
        #     logger.error(f"[实盘] 收盘全撤异常: {e}")
        #     return {"total": 0, "cancelled": 0, "failed": 0, "messages": [f"异常: {e}"]}
        # ================================================================
        return {"total": 0, "cancelled": 0, "failed": 0, "messages": ["实盘交易接口待实现"]}

    async def get_positions(self) -> List[Dict[str, Any]]:
        """获取持仓"""
        if not self.connected:
            return []
        return []

    async def get_orders(self) -> List[Dict[str, Any]]:
        """获取委托单"""
        if not self.connected:
            return []
        return []

    async def get_batch_market_data(self, stock_codes: List[str]) -> Dict[str, Optional[MarketData]]:
        """批量获取行情数据 — 优先读 PriceCache（TTL 检查），过期/缺失的并发走 SDK + 备用通道，先赢者胜"""
        if not stock_codes:
            return {}

        from services.common.price_cache import get_price_cache
        from services.common.stock_code import normalize_stock_code

        cache = get_price_cache()
        results: Dict[str, Optional[MarketData]] = {}
        stale_codes: Set[str] = set()

        # ① 遍历每只股票，检查 freshness
        for code in stock_codes:
            norm = normalize_stock_code(code)
            entry = cache.get_ohlcv_with_ttl(norm)
            if entry and entry.get('is_fresh') and entry.get('data', {}).get('close', 0) > 0:
                results[code] = self._market_data_from_ohlcv(entry['data'], code)
            else:
                stale_codes.add(norm)

        # ② 对过期/缺失的股票，并发走 SDK + 备用 Provider
        if stale_codes:
            sdk_results: Dict[str, Optional[MarketData]] = {}
            fallback_results: Dict[str, Optional[Dict[str, Any]]] = {}

            async def try_sdk_batch():
                nonlocal sdk_results
                try:
                    from services.trading.gateway_dispatcher import get_gateway_dispatcher
                    dispatcher = get_gateway_dispatcher()
                    sub_id = "_batch_race"
                    dispatcher.subscribe(sub_id, stale_codes, interval=0, priority=1)
                    sdk_results = await dispatcher.refresh_now(sub_id)
                    dispatcher.unsubscribe(sub_id)
                except Exception as e:
                    logger.debug(f"SDK 批量行情查询失败: {e}")

            async def try_fallback_batch():
                nonlocal fallback_results
                try:
                    from services.data.channel import get_channel_router, ChannelType
                    router = get_channel_router()
                    fallback_results = await router.execute(
                        ChannelType.TRADING,
                        "get_batch_market_data",
                        stock_codes=list(stale_codes),
                    )
                except Exception as e:
                    logger.debug(f"备用通道批量行情查询失败: {e}")

            # 并发执行
            await asyncio.gather(try_sdk_batch(), try_fallback_batch(), return_exceptions=True)

            # 合并结果：优先 SDK，缺失用备用
            for code in stock_codes:
                norm = normalize_stock_code(code)
                if code in results:
                    continue
                md = sdk_results.get(norm) or sdk_results.get(code)
                if md and md.current_price > 0:
                    results[code] = md
                else:
                    fb = fallback_results.get(norm) or fallback_results.get(code)
                    if fb and fb.get("current_price", 0) > 0:
                        results[code] = MarketData(
                            stock_code=fb.get("stock_code", code),
                            stock_name=fb.get("stock_name", ""),
                            current_price=float(fb.get("current_price", 0)),
                            change_percent=float(fb.get("change_percent", 0)),
                            high=float(fb.get("high", 0)),
                            low=float(fb.get("low", 0)),
                            open_price=float(fb.get("open_price", 0)),
                            prev_close=float(fb.get("prev_close", 0)),
                            volume=int(fb.get("volume", 0)),
                            amount=float(fb.get("amount", 0)),
                            bid=fb.get("bid", []),
                            ask=fb.get("ask", []),
                            bid_volume=fb.get("bid_volume", []),
                            ask_volume=fb.get("ask_volume", []),
                            trade_date=fb.get("trade_date", ""),
                        )
                    else:
                        results[code] = None

        # ③ kline.db 兜底（仅非交易时段）
        still_missing = {
            normalize_stock_code(code)
            for code in stock_codes
            if code not in results or results[code] is None
        }
        if still_missing:
            kline_results = await self._fill_stale_from_kline_db(still_missing)
            for code in stock_codes:
                norm = normalize_stock_code(code)
                if (code not in results or results[code] is None) and norm in kline_results:
                    results[code] = kline_results[norm]

        return results

    async def get_kline_data(
        self,
        stock_code: str,
        period: str = "day",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: int = 100,
        task_type: str = "query"
    ) -> List[Dict[str, Any]]:
        """获取 K 线历史数据 — 通过 SDKManager 查询（自带排队 + 超时），失败回退 ChannelRouter"""
        if not self.connected:
            raise Exception("网关未连接")

        # 第一优先：现有 AmazingData 路径
        try:
            result = await asyncio.wait_for(
                asyncio.to_thread(
                    self._query_kline_data_sync,
                    stock_code, period, start_date, end_date, limit, task_type
                ),
                timeout=10.0
            )
            if result:
                return result
        except asyncio.TimeoutError:
            logger.warning(f"查询 {stock_code} K线超时（>10s）")
        except Exception as e:
            logger.warning(f"AmazingData K线查询失败，尝试备用通道: {e}")

        # 回退：ChannelRouter
        try:
            from services.data.channel import get_channel_router, ChannelType

            router = get_channel_router()
            return await router.execute(
                ChannelType.MARKET_DATA,
                "get_kline_data",
                stock_code=stock_code,
                period=period,
                start_date=start_date or "",
                end_date=end_date or "",
                limit=limit,
            )
        except Exception as e:
            logger.error(f"所有数据源 K 线查询失败: {e}")
            raise Exception("获取 K 线数据失败：所有数据源均不可用")

    async def get_batch_kline_data(
        self,
        stock_codes: List[str],
        period: str = "day",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: int = 100,
        task_type: str = "query"
    ) -> Dict[str, Any]:
        """批量获取 K 线历史数据 — 通过 SDKManager 查询（自带排队 + 超时）"""
        if not self.connected:
            raise Exception("网关未连接")

        # 根据股票数量和任务类型动态计算超时
        count = len(stock_codes) if stock_codes else 1
        if task_type == "download":
            # 500只约需30秒，上限180秒
            batch_timeout = min(count * 0.36, 180.0)
            batch_timeout = max(batch_timeout, 30.0)
        elif count <= 5:
            batch_timeout = 10.0
        elif count <= 20:
            batch_timeout = 20.0
        elif count <= 100:
            batch_timeout = 60.0
        else:
            batch_timeout = 120.0

        try:
            thread_call = asyncio.to_thread(
                self._query_batch_kline_data_sync,
                stock_codes, period, start_date, end_date, limit, task_type
            )
            result = await asyncio.wait_for(thread_call, timeout=batch_timeout)
            return result
        except asyncio.TimeoutError:
            logger.warning(f"批量查询 K 线超时（{count} 只股票，>{batch_timeout:.0f}s）")
            raise Exception(f"获取 K 线数据超时（{count} 只股票，超时 {batch_timeout:.0f}s）")

    def _query_batch_kline_data_sync(
        self,
        stock_codes: List[str],
        period: str = "day",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: int = 100,
        task_type: str = "query"
    ) -> Dict[str, Any]:
        """同步批量查询 K 线数据（通过 SDKManager，自带排队 + 超时）"""
        import datetime as dt
        import pandas as pd

        if end_date:
            end_date_str = str(end_date) if not isinstance(end_date, str) else end_date
            end_dt = dt.datetime.strptime(end_date_str, "%Y%m%d")
        else:
            end_dt = get_china_time()

        if start_date:
            start_date_str = str(start_date) if not isinstance(start_date, str) else start_date
            start_dt = dt.datetime.strptime(start_date_str, "%Y%m%d")
        else:
            if period == "day":
                start_dt = end_dt - dt.timedelta(days=limit)
            else:
                start_dt = end_dt - dt.timedelta(days=limit * 30)

        period_map = {
            "1m": self.constant.Period.min1.value,
            "3m": self.constant.Period.min3.value,
            "5m": self.constant.Period.min5.value,
            "10m": self.constant.Period.min10.value,
            "15m": self.constant.Period.min15.value,
            "30m": self.constant.Period.min30.value,
            "60m": self.constant.Period.min60.value,
            "120m": self.constant.Period.min120.value,
            "day": self.constant.Period.day.value,
            "week": self.constant.Period.week.value,
            "month": self.constant.Period.month.value,
        }
        period_value = period_map.get(period, self.constant.Period.day.value)

        begin_date_int = int(start_dt.strftime('%Y%m%d'))
        end_date_int = int((end_dt + timedelta(days=1)).strftime('%Y%m%d'))

        kline_data = self._query_kline_via_sdk(
            code_list=stock_codes,
            begin_date=begin_date_int,
            end_date=end_date_int,
            period=period_value,
            task_type=task_type
        )

        # 字段重命名：将 SDK 的 kline_time 重命名为 trade_date
        if kline_data:
            for code, df in kline_data.items():
                if df is not None and isinstance(df, pd.DataFrame) and 'kline_time' in df.columns:
                    df = df.rename(columns={'kline_time': 'trade_date'})
                    if 'trade_date' in df.columns:
                        df['trade_date'] = df['trade_date'].apply(
                            lambda x: x.strftime('%Y-%m-%d') if pd.notnull(x) else ''
                        )
                    kline_data[code] = df

        return kline_data or {}

    def _query_kline_data_sync(
        self,
        stock_code: str,
        period: str = "day",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: int = 100,
        task_type: str = "query"
    ) -> List[Dict[str, Any]]:
        """同步查询 K 线数据（通过 SDKManager，自带排队 + 超时）"""
        import datetime as dt

        if end_date:
            end_dt = dt.datetime.strptime(end_date, "%Y%m%d")
        else:
            end_dt = get_china_time()

        if start_date:
            start_dt = dt.datetime.strptime(start_date, "%Y%m%d")
        else:
            if period in ["1m", "3m", "5m", "10m", "15m", "30m", "60m", "120m"]:
                start_dt = end_dt - dt.timedelta(days=limit // 240)
            elif period == "day":
                start_dt = end_dt - dt.timedelta(days=limit)
            elif period == "week":
                start_dt = end_dt - dt.timedelta(weeks=limit)
            elif period == "month":
                start_dt = end_dt - dt.timedelta(days=limit * 30)
            else:
                start_dt = end_dt - dt.timedelta(days=limit)

        if '.' not in stock_code:
            if stock_code.startswith('6'):
                stock_code = f"{stock_code}.SH"
            else:
                stock_code = f"{stock_code}.SZ"

        period_map = {
            "1m": self.constant.Period.min1.value,
            "3m": self.constant.Period.min3.value,
            "5m": self.constant.Period.min5.value,
            "10m": self.constant.Period.min10.value,
            "15m": self.constant.Period.min15.value,
            "30m": self.constant.Period.min30.value,
            "60m": self.constant.Period.min60.value,
            "120m": self.constant.Period.min120.value,
            "day": self.constant.Period.day.value,
            "week": self.constant.Period.week.value,
            "month": self.constant.Period.month.value
        }
        actual_period = period_map.get(period, self.constant.Period.day.value)

        kline_data = self._query_kline_via_sdk(
            code_list=[stock_code],
            begin_date=int(start_dt.strftime("%Y%m%d")),
            end_date=int((end_dt + timedelta(days=1)).strftime("%Y%m%d")),
            period=actual_period,
            task_type="query"
        )

        if kline_data and stock_code in kline_data:
            df = kline_data[stock_code]
            if df is not None and len(df) > 0:
                if len(df) > limit:
                    df = df.tail(limit)

                # 字段重命名：将 SDK 的 kline_time 重命名为 trade_date（与批量方法保持一致）
                if 'kline_time' in df.columns:
                    df = df.rename(columns={'kline_time': 'trade_date'})
                if 'trade_date' in df.columns:
                    import pandas as pd
                    df['trade_date'] = df['trade_date'].apply(
                        lambda x: x.strftime('%Y-%m-%d') if pd.notnull(x) else ''
                    )

                result = []
                for _, row in df.iterrows():
                    time_val = row.get("trade_date", "")

                    result.append({
                        "stock_code": stock_code,
                        "trade_date": str(time_val) if time_val else "",
                        "open": float(row.get("open", 0)),
                        "high": float(row.get("high", 0)),
                        "low": float(row.get("low", 0)),
                        "close": float(row.get("close", 0)),
                        "volume": int(row.get("volume", 0)),
                        "amount": float(row.get("amount", 0))
                    })
                return result

        return []

    # ================================================================
    # 辅助方法
    # ================================================================

    @staticmethod
    def _sanitize_nan(obj):
        """将 NaN/Inf 转换为 None，避免 JSON 序列化失败"""
        if obj is None:
            return None
        if isinstance(obj, (np.floating, np.integer)):
            if np.isnan(obj) or np.isinf(obj):
                return None
            return obj.item()
        if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
            return None
        try:
            import pandas as pd
            if obj is pd.NA or obj is pd.NaT:
                return None
            if isinstance(obj, pd.Timestamp):
                return obj.strftime("%Y-%m-%d")
        except Exception:
            pass
        return obj

    def _records_from_df(self, df, stock_code: str) -> List[Dict[str, Any]]:
        """将 DataFrame 按 stock_code 过滤并转为 JSON records，清理 NaN"""
        if df.empty:
            return []
        df.columns = df.columns.str.lower()
        records = df[df["market_code"] == stock_code].to_dict(orient="records")
        for record in records:
            for key, value in record.items():
                record[key] = self._sanitize_nan(value)
        return records

    def _records_from_raw_df(self, df) -> List[Dict[str, Any]]:
        """将 DataFrame 转为 JSON records，清理 NaN"""
        if df.empty:
            return []
        df.columns = df.columns.str.lower()
        records = df.to_dict(orient="records")
        for record in records:
            for key, value in record.items():
                record[key] = self._sanitize_nan(value)
        return records

    # ================================================================
    # 外部数据查询（行业/指数/财报/行情扩展）
    # ================================================================

    async def get_code_info(self, security_type: str = 'EXTRA_STOCK_A') -> List[Dict[str, Any]]:
        """获取股票代码信息"""
        if not self.connected:
            raise Exception("网关未连接")
        try:
            return await asyncio.to_thread(self._get_code_info_sync, security_type)
        except Exception as e:
            raise Exception(f"获取代码信息失败：{str(e)}")

    def _get_code_info_sync(self, security_type: str = 'EXTRA_STOCK_A') -> List[Dict[str, Any]]:
        from services.common.sdk_manager import get_sdk_manager
        sdk_mgr = get_sdk_manager()
        df = sdk_mgr.get_code_info(security_type=security_type)
        if df.empty:
            return []
        results = []
        for idx, row in df.iterrows():
            code = str(idx) if idx else ''
            results.append({
                "code": code,
                "name": row.get('symbol', '') or code,
                "market": code.split('.')[-1] if '.' in code else ('SH' if code.startswith('6') else 'SZ'),
            })
        return results

    async def get_industry_list(self, level: int = 1) -> List[Dict[str, Any]]:
        """申万行业分类列表"""
        if not self.connected:
            raise Exception("网关未连接")
        try:
            return await asyncio.to_thread(self._get_industry_list_sync, level)
        except Exception as e:
            raise Exception(f"获取行业列表失败：{str(e)}")

    def _get_industry_list_sync(self, level: int = 1) -> List[Dict[str, Any]]:
        from services.common.sdk_manager import get_sdk_manager
        sdk_mgr = get_sdk_manager()
        df = sdk_mgr.get_industry_base_info()
        if df.empty:
            return []
        filtered = df[df["LEVEL_TYPE"] == level]
        records = filtered.to_dict(orient="records")
        for record in records:
            for key, value in record.items():
                record[key] = self._sanitize_nan(value)
        return records

    async def get_industry_kline(self, index_code: str) -> List[Dict[str, Any]]:
        """行业指数日行情"""
        if not self.connected:
            raise Exception("网关未连接")
        try:
            return await asyncio.to_thread(self._get_industry_kline_sync, index_code)
        except Exception as e:
            raise Exception(f"获取行业行情失败：{str(e)}")

    def _get_industry_kline_sync(self, index_code: str) -> List[Dict[str, Any]]:
        from services.common.sdk_manager import get_sdk_manager
        sdk_mgr = get_sdk_manager()
        result = sdk_mgr.get_industry_daily(code_list=[index_code])
        if not result or index_code not in result:
            return []
        df = result[index_code]
        df = df.reset_index()
        df.columns = df.columns.str.lower()
        if "trade_date" in df.columns:
            df["trade_date"] = df["trade_date"].apply(
                lambda x: x.strftime("%Y-%m-%d") if hasattr(x, "strftime") else str(x)[:10]
            )
        records = df.to_dict(orient="records")
        for record in records:
            for key, value in record.items():
                record[key] = self._sanitize_nan(value)
        return records

    async def get_industry_constituent(self, index_code: str) -> List[Dict[str, Any]]:
        """行业成分股列表"""
        if not self.connected:
            raise Exception("网关未连接")
        try:
            return await asyncio.to_thread(self._get_industry_constituent_sync, index_code)
        except Exception as e:
            raise Exception(f"获取行业成分股失败：{str(e)}")

    def _get_industry_constituent_sync(self, index_code: str) -> List[Dict[str, Any]]:
        from services.common.sdk_manager import get_sdk_manager
        sdk_mgr = get_sdk_manager()
        df = sdk_mgr.get_industry_constituent(index_codes=[index_code])
        return self._records_from_raw_df(df)

    async def get_index_constituent(self, index_code: str) -> List[Dict[str, Any]]:
        """指数成分股列表"""
        if not self.connected:
            raise Exception("网关未连接")
        try:
            return await asyncio.to_thread(self._get_index_constituent_sync, index_code)
        except Exception as e:
            raise Exception(f"获取指数成分股失败：{str(e)}")

    def _get_index_constituent_sync(self, index_code: str) -> List[Dict[str, Any]]:
        from services.common.sdk_manager import get_sdk_manager
        sdk_mgr = get_sdk_manager()
        df = sdk_mgr.get_index_constituent(index_codes=[index_code])
        return self._records_from_raw_df(df)

    async def get_income_statement(self, stock_code: str) -> List[Dict[str, Any]]:
        """利润表"""
        if not self.connected:
            raise Exception("网关未连接")
        try:
            return await asyncio.to_thread(self._get_income_statement_sync, stock_code)
        except Exception as e:
            raise Exception(f"获取利润表失败：{str(e)}")

    def _get_income_statement_sync(self, stock_code: str) -> List[Dict[str, Any]]:
        from services.common.sdk_manager import get_sdk_manager
        sdk_mgr = get_sdk_manager()
        df = sdk_mgr.get_income_statement(stock_codes=[stock_code])
        return self._records_from_df(df, stock_code)

    async def get_balance_sheet(self, stock_code: str) -> List[Dict[str, Any]]:
        """资产负债表"""
        if not self.connected:
            raise Exception("网关未连接")
        try:
            return await asyncio.to_thread(self._get_balance_sheet_sync, stock_code)
        except Exception as e:
            raise Exception(f"获取资产负债表失败：{str(e)}")

    def _get_balance_sheet_sync(self, stock_code: str) -> List[Dict[str, Any]]:
        from services.common.sdk_manager import get_sdk_manager
        sdk_mgr = get_sdk_manager()
        df = sdk_mgr.get_balance_sheet(stock_codes=[stock_code])
        return self._records_from_df(df, stock_code)

    async def get_cash_flow_statement(self, stock_code: str) -> List[Dict[str, Any]]:
        """现金流量表"""
        if not self.connected:
            raise Exception("网关未连接")
        try:
            return await asyncio.to_thread(self._get_cash_flow_statement_sync, stock_code)
        except Exception as e:
            raise Exception(f"获取现金流量表失败：{str(e)}")

    def _get_cash_flow_statement_sync(self, stock_code: str) -> List[Dict[str, Any]]:
        from services.common.sdk_manager import get_sdk_manager
        sdk_mgr = get_sdk_manager()
        df = sdk_mgr.get_cash_flow_statement(stock_codes=[stock_code])
        return self._records_from_df(df, stock_code)

    async def get_profit_notice(self, stock_code: str) -> List[Dict[str, Any]]:
        """业绩预告"""
        if not self.connected:
            raise Exception("网关未连接")
        try:
            return await asyncio.to_thread(self._get_profit_notice_sync, stock_code)
        except Exception as e:
            raise Exception(f"获取业绩预告失败：{str(e)}")

    def _get_profit_notice_sync(self, stock_code: str) -> List[Dict[str, Any]]:
        from services.common.sdk_manager import get_sdk_manager
        sdk_mgr = get_sdk_manager()
        df = sdk_mgr.get_profit_notice(stock_codes=[stock_code])
        return self._records_from_df(df, stock_code)

    async def get_profit_express(self, stock_code: str) -> List[Dict[str, Any]]:
        """业绩快报"""
        if not self.connected:
            raise Exception("网关未连接")
        try:
            return await asyncio.to_thread(self._get_profit_express_sync, stock_code)
        except Exception as e:
            raise Exception(f"获取业绩快报失败：{str(e)}")

    def _get_profit_express_sync(self, stock_code: str) -> List[Dict[str, Any]]:
        from services.common.sdk_manager import get_sdk_manager
        sdk_mgr = get_sdk_manager()
        df = sdk_mgr.get_profit_express(stock_codes=[stock_code])
        return self._records_from_df(df, stock_code)

    async def get_long_hu_bang(self, stock_code: str, begin_date: int, end_date: int) -> List[Dict[str, Any]]:
        """龙虎榜数据"""
        if not self.connected:
            raise Exception("网关未连接")
        try:
            return await asyncio.to_thread(self._get_long_hu_bang_sync, stock_code, begin_date, end_date)
        except Exception as e:
            raise Exception(f"获取龙虎榜失败：{str(e)}")

    def _get_long_hu_bang_sync(self, stock_code: str, begin_date: int, end_date: int) -> List[Dict[str, Any]]:
        from services.common.sdk_manager import get_sdk_manager
        sdk_mgr = get_sdk_manager()
        df = sdk_mgr.get_long_hu_bang(stock_codes=[stock_code], begin_date=begin_date, end_date=end_date)
        return self._records_from_raw_df(df)

    async def get_margin_summary(self, begin_date: int, end_date: int) -> List[Dict[str, Any]]:
        """融资融券汇总"""
        if not self.connected:
            raise Exception("网关未连接")
        try:
            return await asyncio.to_thread(self._get_margin_summary_sync, begin_date, end_date)
        except Exception as e:
            raise Exception(f"获取两融汇总失败：{str(e)}")

    def _get_margin_summary_sync(self, begin_date: int, end_date: int) -> List[Dict[str, Any]]:
        from services.common.sdk_manager import get_sdk_manager
        sdk_mgr = get_sdk_manager()
        df = sdk_mgr.get_margin_summary(begin_date=begin_date, end_date=end_date)
        return self._records_from_raw_df(df)

    async def get_margin_detail(self, stock_code: str, begin_date: int, end_date: int) -> List[Dict[str, Any]]:
        """融资融券明细"""
        if not self.connected:
            raise Exception("网关未连接")
        try:
            return await asyncio.to_thread(self._get_margin_detail_sync, stock_code, begin_date, end_date)
        except Exception as e:
            raise Exception(f"获取两融明细失败：{str(e)}")

    def _get_margin_detail_sync(self, stock_code: str, begin_date: int, end_date: int) -> List[Dict[str, Any]]:
        from services.common.sdk_manager import get_sdk_manager
        sdk_mgr = get_sdk_manager()
        df = sdk_mgr.get_margin_detail(stock_codes=[stock_code], begin_date=begin_date, end_date=end_date)
        return self._records_from_raw_df(df)

    async def get_block_trading(self, stock_code: str, begin_date: int, end_date: int) -> List[Dict[str, Any]]:
        """大宗交易数据"""
        if not self.connected:
            raise Exception("网关未连接")
        try:
            return await asyncio.to_thread(self._get_block_trading_sync, stock_code, begin_date, end_date)
        except Exception as e:
            raise Exception(f"获取大宗交易失败：{str(e)}")

    def _get_block_trading_sync(self, stock_code: str, begin_date: int, end_date: int) -> List[Dict[str, Any]]:
        from services.common.sdk_manager import get_sdk_manager
        sdk_mgr = get_sdk_manager()
        df = sdk_mgr.get_block_trading(stock_codes=[stock_code], begin_date=begin_date, end_date=end_date)
        return self._records_from_raw_df(df)

    async def get_treasury_yield(self) -> List[Dict[str, Any]]:
        """国债收益率曲线"""
        if not self.connected:
            raise Exception("网关未连接")
        try:
            return await asyncio.to_thread(self._get_treasury_yield_sync)
        except Exception as e:
            raise Exception(f"获取国债收益率失败：{str(e)}")

    def _get_treasury_yield_sync(self) -> List[Dict[str, Any]]:
        from services.common.sdk_manager import get_sdk_manager
        sdk_mgr = get_sdk_manager()
        df = sdk_mgr.get_treasury_yield()
        return self._records_from_raw_df(df)


# 工厂函数：创建交易网关
def create_gateway(galaxy_app_id: str = "", galaxy_password: str = "") -> TradingGatewayInterface:
    """创建交易网关实例"""
    gateway = TradingGateway(app_id=galaxy_app_id, password=galaxy_password)
    if not gateway.sdk_available:
        raise Exception("AmazingData SDK 不可用，无法创建交易网关")
    return gateway


# 全局网关实例
_gateway: Optional[TradingGatewayInterface] = None
# 全局网关 credentials 缓存：account_id -> gateway instance
_gateway_cache: Dict[str, TradingGatewayInterface] = {}


def clear_gateway_cache():
    """断开并清理所有网关缓存（shutdown 时调用）"""
    global _gateway
    # 所有 gateway 共享同一个 SDKManager 连接，不需逐个断开
    # 由 SDKManager.disconnect() 统一处理
    _gateway_cache.clear()
    _gateway = None


async def get_gateway_for_account(broker_credentials: Optional[Dict[str, str]] = None) -> TradingGatewayInterface:
    """
    获取指定账户 credentials 的交易网关实例

    Args:
        broker_credentials: 券商 credentials，包含 broker_account, broker_password 等

    Returns:
        TradingGatewayInterface 实例
    """
    if not broker_credentials or not broker_credentials.get("broker_account"):
        # 没有 credentials，返回默认网关
        return await get_gateway()

    account_id = broker_credentials.get("broker_account")

    # 检查缓存
    if account_id in _gateway_cache:
        return _gateway_cache[account_id]

    # 创建新的网关实例
    gateway = create_gateway(
        galaxy_app_id=broker_credentials.get("broker_account", ""),
        galaxy_password=broker_credentials.get("broker_password", "")
    )

    # 连接网关
    await gateway.connect()

    # 缓存网关实例
    _gateway_cache[account_id] = gateway

    logger.info(f"为账户 {account_id} 创建了独立的交易网关")
    return gateway


class ErrorReportingGateway(TradingGatewayInterface):
    """错误报告网关 - 当真实数据不可用时返回具体错误原因"""

    def __init__(self, error_message: str):
        self.error_message = error_message
        self.connected = True  # 为了避免连接检查失败

    async def connect(self) -> bool:
        self.connected = True
        return True

    async def disconnect(self):
        self.connected = False

    async def get_market_data(self, stock_code: str) -> Optional[MarketData]:
        if not self.connected:
            return None
        # 抛出具体的错误信息而不是返回模拟数据
        raise Exception(self.error_message)

    async def get_stock_list(self) -> List[Dict[str, str]]:
        if not self.connected:
            return []
        # 抛出具体的错误信息而不是返回模拟数据
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


async def get_gateway() -> TradingGatewayInterface:
    """获取全局交易网关实例，优先使用真实数据"""
    global _gateway
    if _gateway is None:
        # 默认使用真实数据网关（AmazingData或银河，按优先级），但如果都不行则使用模拟网关
        _gateway = create_gateway()
        await _gateway.connect()
    return _gateway


async def get_gateway_with_credentials(auth_token: Optional[str] = None) -> TradingGatewayInterface:
    """
    根据认证 token 获取交易网关实例

    Args:
        auth_token: 用户认证 token

    Returns:
        使用对应用户券商 credentials 的网关实例
    """
    if not auth_token:
        return await get_gateway()

    # 从认证服务获取券商 credentials
    try:
        from services.auth.service import get_auth_service
        auth_service = get_auth_service()
        account = auth_service.validate_token(auth_token)

        if not account:
            logger.warning("无效的认证 token，使用默认网关")
            return await get_gateway()

        # 提取券商 credentials
        broker_creds = {
            "broker_account": account.get("broker_account", ""),
            "broker_password": account.get("broker_password", ""),
        }

        # 如果没有配置券商账号，使用默认网关
        if not broker_creds["broker_account"]:
            logger.warning(f"用户 {account.get('username')} 未配置券商账号，使用默认网关")
            return await get_gateway()

        # 使用用户的券商 credentials 创建网关
        return await get_gateway_for_account(broker_creds)

    except Exception as e:
        logger.error(f"获取带 credentials 的网关失败：{e}")
        return await get_gateway()


async def init_gateway(galaxy_app_id: str = "", galaxy_password: str = ""):
    """初始化交易网关"""
    global _gateway
    if _gateway:
        await _gateway.disconnect()
    _gateway = create_gateway(galaxy_app_id=galaxy_app_id, galaxy_password=galaxy_password)
    await _gateway.connect()
    return _gateway
