"""
交易网关抽象层
- 当银河 SDK 或 AmazingData SDK 可用时，使用真实交易接口
- 当所有 SDK 不可用时，抛出异常
"""
import asyncio
import logging
from datetime import datetime, timedelta, timezone

# 中国时区
CHINA_TZ = timezone(timedelta(hours=8))

def get_china_time():
    """获取中国时区时间"""
    return datetime.now(CHINA_TZ).replace(tzinfo=None)
from typing import Optional, Dict, Any, List
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)

# 为了解决pandas版本兼容性问题，临时替换有问题的pandas功能
def patch_pandas_frequency_issue():
    """修补pandas频率字符串兼容性问题"""
    try:
        import pandas as pd

        # 检查pandas版本，如果版本较高则可能存在频率解析问题
        version_tuple = tuple(map(int, pd.__version__.split('.')[:2]))

        if version_tuple >= (2, 0):  # pandas 2.0+ 及 3.x 版本
            # 尝试动态修补pandas的时间频率映射
            import pandas._libs.tslibs.offsets as offsets

            # 备份原始函数
            orig_get_offset = getattr(offsets, '_get_offset', None)

            if orig_get_offset:
                def patched_get_offset(freqstr, tick_class=None):
                    # 将大写S转换为小写s
                    freqstr = freqstr.upper().replace('S', 's')
                    return orig_get_offset(freqstr, tick_class)

                # 注意：这个修补可能无法完全解决所有问题
                # 因为SDK内部可能以其他方式引用pandas
                pass  # 暂时跳过，因为这种修补可能有风险

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
                 bid: List[float] = None, ask: List[float] = None):
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




class GalaxyTradingGateway(TradingGatewayInterface):
    """银河交易网关（使用 AmazingData SDK 作为底层实现）"""

    def __init__(self, app_id: str = "", password: str = ""):
        self.app_id = app_id or "REDACTED_SDK_USERNAME"
        self.password = password or "REDACTED_SDK_PASSWORD"
        self.connected = False
        self._token = None
        self.server_ip = "140.206.44.234"
        self.server_port = 8600

        # 银河 SDK 参数（保留用于兼容）
        try:
            from tgw import (
                IGMDApi, IGMDSpi, IGMDSnapshotSpi, Cfg, ApiMode,
                MarketType, SubscribeDataType, SubscribeItem,
                SetLogSpi, ILogSpi, LogLevel, GetVersion,
                Tools_CreateSubscribeItem
            )
            self.IGMDApi = IGMDApi
            self.IGMDSpi = IGMDSpi
            self.IGMDSnapshotSpi = IGMDSnapshotSpi
            self.Cfg = Cfg
            self.ApiMode = ApiMode
            self.MarketType = MarketType
            self.SubscribeDataType = SubscribeDataType
            self.SubscribeItem = SubscribeItem
            self.SetLogSpi = SetLogSpi
            self.ILogSpi = ILogSpi
            self.LogLevel = LogLevel
            self.GetVersion = GetVersion
            self.Tools_CreateSubscribeItem = Tools_CreateSubscribeItem
            logger.info(f"银河 SDK 框架加载成功，版本：{GetVersion()}")
        except ImportError as e:
            logger.warning(f"银河 SDK 不可用：{e}")
            self.IGMDApi = None
            self.IGMDSpi = None

        # 使用 SDKManager 管理 SDK 实例，避免重复创建导致 TGW 连接数超限
        # 不再在 Gateway 内部创建 BaseData/MarketData 实例
        try:
            from AmazingData import constant
            self.constant = constant
            self.sdk_available = True
            logger.info("AmazingData SDK 加载成功（通过 SDKManager 管理）")
        except ImportError as e:
            logger.warning(f"AmazingData SDK 不可用：{e}")
            self.sdk_available = False

    def _get_base_data(self):
        """获取 BaseData 实例（使用 SDKManager 缓存，避免重复连接）"""
        from services.common.sdk_manager import get_sdk_manager
        sdk_mgr = get_sdk_manager()
        return sdk_mgr.get_base_data()

    def _get_market_data(self):
        """获取 MarketData 实例（使用 SDKManager 缓存，避免重复连接）"""
        from services.common.sdk_manager import get_sdk_manager
        sdk_mgr = get_sdk_manager()
        return sdk_mgr.get_market_data()

    async def connect(self) -> bool:
        """连接交易服务器 - 使用 SDKManager 登录（避免重复连接）"""
        if not self.sdk_available:
            logger.error("AmazingData SDK 未安装，无法连接")
            return False

        try:
            # 使用 SDKManager 登录，避免重复创建连接
            from services.common.sdk_manager import get_sdk_manager
            sdk_mgr = get_sdk_manager()
            sdk_mgr._ensure_login()
            self.connected = True
            logger.info("银河交易网关连接成功（通过 SDKManager）")
            return True
        except Exception as e:
            logger.error(f"银河网关连接失败：{e}")
            return False

    async def disconnect(self):
        """断开连接 - SDK 实例由 SDKManager 管理，此处只更新状态"""
        self.connected = False

    async def get_market_data(self, stock_code: str) -> Optional[MarketData]:
        """获取真实行情数据 - 使用 AmazingData SDK（银河网关统一接口）"""
        if not self.connected:
            raise Exception("银河 SDK: 网关未连接")

        try:
            # 使用 asyncio.to_thread 将同步 SDK 调用移到线程池执行，避免阻塞事件循环
            result = await asyncio.to_thread(self._query_market_data_sync, stock_code)
            return result

        except Exception as e:
            logger.error(f"获取行情失败：{e}")
            raise Exception(f"银河 SDK 获取行情失败：{str(e)}")

    def _query_market_data_sync(self, stock_code: str) -> Optional[MarketData]:
        """同步查询行情数据（在线程池中执行）- 使用 AmazingData SDK"""
        import datetime
        import sqlite3
        from pathlib import Path

        # 使用缓存的 MarketData 实例，避免重复创建导致连接数超限
        md = self._get_market_data()

        # 规范化股票代码格式 - 确保有 .SH/.SZ 后缀
        original_code = stock_code
        if '.' not in stock_code:
            if stock_code.startswith('6'):
                stock_code = f"{stock_code}.SH"
            else:
                stock_code = f"{stock_code}.SZ"

        # 从本地数据库获取股票名称（SDK 不返回股票名称）
        # 优先从 stock_monthly_factors 获取，因为 kline_data 可能只有股票代码
        stock_name = original_code  # 默认使用股票代码
        try:
            kline_db_path = Path(__file__).parent.parent.parent / "data" / "kline.db"
            if kline_db_path.exists():
                # 使用同步 sqlite3 查询（在线程池中执行）
                conn = sqlite3.connect(str(kline_db_path))
                cursor = conn.cursor()
                # 优先从 stock_monthly_factors 获取股票名称
                cursor.execute(
                    "SELECT stock_name FROM stock_monthly_factors WHERE stock_code = ? LIMIT 1",
                    (stock_code,)
                )
                row = cursor.fetchone()
                logger.warning(f"DB query for {stock_code}: row={row}")
                if row and row[0]:
                    stock_name = row[0]
                    logger.warning(f"Found stock_name from stock_monthly_factors: {stock_name}")
                else:
                    # 退而求其次，从 kline_data 获取
                    cursor.execute(
                        "SELECT DISTINCT stock_name FROM kline_data WHERE stock_code = ? LIMIT 1",
                        (stock_code,)
                    )
                    row = cursor.fetchone()
                    if row and row[0]:
                        stock_name = row[0]
                        logger.warning(f"Found stock_name from kline_data: {stock_name}")
                cursor.close()
                conn.close()
        except Exception as e:
            logger.error(f"从本地数据库获取股票名称失败：{e}")

        # 使用 query_kline 方法获取当日数据 - SDK 使用半开区间 [begin_date, end_date)，需要将 end_date 加 1 天
        end_dt = get_china_time()
        begin_dt = end_dt - datetime.timedelta(days=1)
        end_date = int((end_dt + datetime.timedelta(days=1)).strftime('%Y%m%d'))
        begin_date = int(begin_dt.strftime('%Y%m%d'))

        kline_data = md.query_kline(
            code_list=[stock_code],
            begin_date=begin_date,
            end_date=end_date,
            period=self.constant.Period.day.value
        )

        if kline_data and stock_code in kline_data:
            df = kline_data[stock_code]
            if len(df) > 0:
                # 获取最后一行数据（最新的 K 线数据）
                last_row = df.iloc[-1]

                # 提取关键字段
                current_price = float(last_row.get('close', 0)) if 'close' in last_row else 0
                high = float(last_row.get('high', current_price)) if 'high' in last_row else current_price
                low = float(last_row.get('low', current_price)) if 'low' in last_row else current_price
                open_price = float(last_row.get('open', current_price)) if 'open' in last_row else current_price
                prev_close = float(last_row.get('pre_close', current_price)) if 'pre_close' in last_row else current_price
                volume = int(last_row.get('volume', 0)) if 'volume' in last_row else 0
                amount = float(last_row.get('amount', 0)) if 'amount' in last_row else 0

                # 计算涨跌幅
                change_percent = ((current_price - prev_close) / prev_close * 100) if prev_close != 0 else 0

                return MarketData(
                    stock_code=stock_code,
                    stock_name=stock_name,
                    current_price=current_price,
                    change_percent=change_percent,
                    high=high,
                    low=low,
                    open_price=open_price,
                    prev_close=prev_close,
                    volume=volume,
                    amount=amount,
                    bid=[current_price * 0.999] * 5,
                    ask=[current_price * 1.001] * 5
                )

        logger.warning(f"未获取到 {stock_code} 的 K 线数据")
        raise Exception(f"银河 SDK: 未获取到 {stock_code} 的行情数据")


    async def get_stock_list(self) -> List[Dict[str, str]]:
        """获取股票列表 - 使用 AmazingData SDK"""
        if not self.connected:
            raise Exception("银河 SDK: 网关未连接")

        try:
            # 使用 asyncio.to_thread 将同步 SDK 调用移到线程池执行
            stock_list = await asyncio.to_thread(self._query_stock_list_sync)
            return stock_list

        except Exception as e:
            logger.error(f"获取股票列表失败：{e}")
            raise Exception(f"银河 SDK: 获取股票列表失败 - {str(e)}")

    def _query_stock_list_sync(self) -> List[Dict[str, str]]:
        """同步查询股票列表（在线程池中执行）- 使用本地数据库或 AmazingData SDK"""
        import sqlite3
        from pathlib import Path

        # 首先尝试从本地数据库获取股票列表
        db_paths = [
            Path('/home/bobo/StockWinner/data/kline.db'),
            Path('/home/bobo/StockWinner/stockwinner.db'),
        ]

        for db_path in db_paths:
            if db_path.exists():
                try:
                    conn = sqlite3.connect(str(db_path))
                    cursor = conn.cursor()
                    # 检查是否有 kline_data 表
                    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='kline_data'")
                    if cursor.fetchone():
                        # 获取所有股票代码
                        cursor.execute("SELECT DISTINCT stock_code FROM kline_data")
                        stock_codes = [row[0] for row in cursor.fetchall()]
                        conn.close()

                        if stock_codes:
                            # 解析股票代码格式 (CODE.MARKET)
                            stock_list = []
                            for code in stock_codes:
                                parts = code.split('.')
                                if len(parts) == 2:
                                    stock_list.append({
                                        "code": parts[0],
                                        "name": "",  # 名称从其他地方获取
                                        "market": parts[1]
                                    })
                            logger.info(f"从本地数据库获取到 {len(stock_list)} 只股票")
                            return stock_list
                    conn.close()
                except Exception as e:
                    logger.warning(f"从本地数据库获取股票列表失败：{e}")

        # 无法获取股票列表，返回错误
        raise Exception("银河 SDK: 无法获取股票列表，请先下载基础数据或检查数据库连接")

    async def get_index_list(self) -> List[Dict[str, str]]:
        """获取指数列表 - 使用 AmazingData SDK"""
        if not self.connected:
            raise Exception("银河 SDK: 网关未连接")

        try:
            index_list = await asyncio.to_thread(self._query_index_list_sync)
            return index_list
        except Exception as e:
            logger.error(f"获取指数列表失败：{e}")
            raise Exception(f"银河 SDK: 获取指数列表失败 - {str(e)}")

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
            raise Exception("银河 SDK: 网关未连接")

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

    async def buy(self, stock_code: str, price: float, quantity: int) -> OrderResult:
        """真实买入"""
        if not self.connected or not self.session:
            return OrderResult(False, message="网关未连接")

        try:
            # 根据实际 SDK API 调整
            # order_id = self.session.buy(stock_code, price, quantity)
            # return OrderResult(success=True, order_id=order_id)
            logger.warning(f"buy 待实现 - 参数：{stock_code}, {price}, {quantity}")
            return OrderResult(False, message="SDK API 待实现")
        except Exception as e:
            logger.error(f"买入失败：{e}")
            return OrderResult(False, message=str(e))

    async def sell(self, stock_code: str, price: float, quantity: int) -> OrderResult:
        """真实卖出"""
        if not self.connected or not self.session:
            return OrderResult(False, message="网关未连接")

        try:
            # order_id = self.session.sell(stock_code, price, quantity)
            # return OrderResult(success=True, order_id=order_id)
            logger.warning(f"sell 待实现 - 参数：{stock_code}, {price}, {quantity}")
            return OrderResult(False, message="SDK API 待实现")
        except Exception as e:
            logger.error(f"卖出失败：{e}")
            return OrderResult(False, message=str(e))

    async def get_positions(self) -> List[Dict[str, Any]]:
        """获取持仓"""
        if not self.connected or not self.session:
            return []

        try:
            # positions = self.session.get_positions()
            # return [...]
            return []
        except Exception as e:
            logger.error(f"获取持仓失败：{e}")
            return []

    async def get_orders(self) -> List[Dict[str, Any]]:
        """获取委托单"""
        if not self.connected or not self.session:
            return []

        try:
            # orders = self.session.get_orders()
            # return [...]
            return []
        except Exception as e:
            logger.error(f"获取委托单失败：{e}")
            return []

    async def get_batch_market_data(self, stock_codes: List[str]) -> Dict[str, Optional[MarketData]]:
        """批量获取行情数据 - 银河 SDK"""
        results = {}
        for code in stock_codes:
            try:
                results[code] = await self.get_market_data(code)
            except Exception as e:
                logger.warning(f"获取 {code} 行情失败：{e}")
                results[code] = None
        return results

    async def get_kline_data(
        self,
        stock_code: str,
        period: str = "day",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: int = 100,
        task_type: str = "query"  # query, download, screening
    ) -> List[Dict[str, Any]]:
        """获取 K 线历史数据 - 使用 AmazingData SDK（带连接排队）"""
        if not self.connected:
            raise Exception("银河 SDK: 网关未连接")

        # 使用连接管理器获取令牌
        from services.common.sdk_connection_manager import get_connection_manager, TaskType

        conn_mgr = get_connection_manager()
        task_type_enum = TaskType.QUERY if task_type == "query" else (
            TaskType.DOWNLOAD if task_type == "download" else TaskType.SCREENING
        )

        # 获取连接令牌（会自动排队等待）
        token = await conn_mgr.acquire(task_type=task_type_enum)

        try:
            # 使用 asyncio.to_thread 将同步 SDK 调用移到线程池执行
            result = await asyncio.to_thread(
                self._query_kline_data_sync,
                stock_code, period, start_date, end_date, limit
            )
            return result
        finally:
            # 释放连接令牌
            token.release()

    async def get_batch_kline_data(
        self,
        stock_codes: List[str],
        period: str = "day",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: int = 100,
        task_type: str = "query"  # query, download, screening
    ) -> Dict[str, Any]:
        """批量获取 K 线历史数据 - 使用 AmazingData SDK（带连接排队）"""
        if not self.connected:
            raise Exception("银河 SDK: 网关未连接")

        # 使用连接管理器获取令牌
        from services.common.sdk_connection_manager import get_connection_manager, TaskType

        conn_mgr = get_connection_manager()
        task_type_enum = TaskType.QUERY if task_type == "query" else (
            TaskType.DOWNLOAD if task_type == "download" else TaskType.SCREENING
        )

        # 获取连接令牌（会自动排队等待）
        token = await conn_mgr.acquire(task_type=task_type_enum)

        try:
            # 使用 asyncio.to_thread 将同步 SDK 调用移到线程池执行
            result = await asyncio.to_thread(
                self._query_batch_kline_data_sync,
                stock_codes, period, start_date, end_date, limit
            )
            return result
        finally:
            # 释放连接令牌
            token.release()

    def _query_batch_kline_data_sync(
        self,
        stock_codes: List[str],
        period: str = "day",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: int = 100
    ) -> Dict[str, Any]:
        """同步批量查询 K 线数据（在线程池中执行）- 使用 AmazingData SDK"""
        import datetime as dt
        import pandas as pd

        # 解析日期
        if end_date:
            end_dt = dt.datetime.strptime(end_date, "%Y%m%d")
        else:
            end_dt = get_china_time()

        if start_date:
            start_dt = dt.datetime.strptime(start_date, "%Y%m%d")
        else:
            if period == "day":
                start_dt = end_dt - dt.timedelta(days=limit)
            else:
                start_dt = end_dt - dt.timedelta(days=limit * 30)

        # 使用缓存的 MarketData 实例，避免重复创建导致连接数超限
        md = self._get_market_data()

        # 映射 period 参数 - 支持所有可用周期
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

        # 格式化日期 - SDK 使用半开区间 [begin_date, end_date)，需要将 end_date 加 1 天
        begin_date = int(start_dt.strftime('%Y%m%d'))
        end_date_int = int((end_dt + timedelta(days=1)).strftime('%Y%m%d'))

        # 批量查询
        try:
            kline_data = md.query_kline(
                code_list=stock_codes,
                begin_date=begin_date,
                end_date=end_date_int,
                period=period_value
            )
        except Exception as e:
            logger.error(f"SDK query_kline 调用失败：{e}")
            raise Exception(f"SDK 查询 K 线失败：{str(e)}")

        # 检查 SDK 返回结果 - 如果返回 None 或空字典，说明 SDK 调用失败
        if not kline_data:
            logger.warning(f"SDK 返回空结果，股票数：{len(stock_codes)}")
            # 抛出异常让调用方知道
            raise Exception(f"SDK 返回空结果（可能连接数超限或 SDK 未连接），请求股票数：{len(stock_codes)}")

        # 统计有效数据
        valid_count = 0
        for code, df in kline_data.items():
            if df is not None and len(df) > 0:
                valid_count += 1

        # 如果所有股票都无数据，可能是 SDK 连接问题
        if valid_count == 0 and len(stock_codes) > 0:
            logger.error(f"SDK 返回数据但所有 {len(stock_codes)} 只股票都为空，可能 SDK 连接异常")
            raise Exception(f"SDK 返回数据但所有 {len(stock_codes)} 只股票都为空（可能连接异常）")

        # 转换结果格式
        result = {}
        for code, df in kline_data.items():
            if df is not None and len(df) > 0:
                result[code] = df

        return result

    def _query_kline_data_sync(
        self,
        stock_code: str,
        period: str = "day",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """同步查询 K 线数据（在线程池中执行）- 使用 AmazingData SDK"""
        import datetime as dt

        # 解析日期
        if end_date:
            end_dt = dt.datetime.strptime(end_date, "%Y%m%d")
        else:
            end_dt = get_china_time()

        if start_date:
            start_dt = dt.datetime.strptime(start_date, "%Y%m%d")
        else:
            # 根据 limit 和 period 推算开始日期
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

        # 使用缓存的 MarketData 实例，避免重复创建导致连接数超限
        md = self._get_market_data()

        # 规范化股票代码格式
        if '.' not in stock_code:
            if stock_code.startswith('6'):
                stock_code = f"{stock_code}.SH"
            else:
                stock_code = f"{stock_code}.SZ"

        # 映射 period 参数
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

        # 查询 K 线数据
        kline_data = md.query_kline(
            code_list=[stock_code],
            begin_date=int(start_dt.strftime("%Y%m%d")),
            end_date=int(end_dt.strftime("%Y%m%d")),
            period=actual_period
        )

        if kline_data and stock_code in kline_data:
            df = kline_data[stock_code]
            if len(df) > 0:
                # 限制返回数量
                if len(df) > limit:
                    df = df.tail(limit)

                # 转换为字典列表
                result = []
                for idx, row in df.iterrows():
                    # 处理时间字段 - SDK 返回的是 trade_date 列
                    time_val = row.get("trade_date", "")
                    if not time_val:
                        time_val = row.get("time", "")

                    result.append({
                        "stock_code": stock_code,
                        "time": str(time_val) if time_val else "",
                        "open": float(row.get("open", 0)),
                        "high": float(row.get("high", 0)),
                        "low": float(row.get("low", 0)),
                        "close": float(row.get("close", 0)),
                        "volume": int(row.get("volume", 0)),
                        "amount": float(row.get("amount", 0))
                    })
                return result

        return []
        


class AmazingDataTradingGateway(TradingGatewayInterface):
    """AmazingData 交易网关"""

    def __init__(self, app_id: str = "", password: str = ""):
        self.app_id = app_id or "REDACTED_SDK_USERNAME"
        self.password = password or "REDACTED_SDK_PASSWORD"
        self.connected = False
        self._token = None
        self.server_ip = "140.206.44.234"
        self.server_port = 8600

        # 使用 SDKManager 管理 SDK 实例，避免重复创建导致 TGW 连接数超限
        # 不再在 Gateway 内部创建 BaseData/MarketData 实例
        try:
            from AmazingData import constant
            self.constant = constant
            self.sdk_available = True
            logger.info("AmazingData SDK 加载成功（通过 SDKManager 管理）")
        except ImportError as e:
            logger.warning(f"AmazingData SDK 不可用：{e}")
            self.sdk_available = False

    def _get_base_data(self):
        """获取 BaseData 实例（使用 SDKManager 缓存，避免重复连接）"""
        from services.common.sdk_manager import get_sdk_manager
        sdk_mgr = get_sdk_manager()
        return sdk_mgr.get_base_data()

    def _get_market_data(self):
        """获取 MarketData 实例（使用 SDKManager 缓存，避免重复连接）"""
        from services.common.sdk_manager import get_sdk_manager
        sdk_mgr = get_sdk_manager()
        return sdk_mgr.get_market_data()

    async def connect(self) -> bool:
        """连接交易服务器 - 使用 SDKManager 登录（避免重复连接）"""
        if not self.sdk_available:
            logger.error("AmazingData SDK 未安装，无法连接")
            return False

        try:
            # 使用 SDKManager 登录，避免重复创建连接
            from services.common.sdk_manager import get_sdk_manager
            sdk_mgr = get_sdk_manager()
            sdk_mgr._ensure_login()
            self.connected = True
            logger.info("AmazingData 交易网关连接成功（通过 SDKManager）")
            return True
        except Exception as e:
            logger.error(f"AmazingData 网关连接失败：{e}")
            return False

    async def disconnect(self):
        """断开连接 - SDK 实例由 SDKManager 管理，此处只更新状态"""
        self.connected = False

    async def get_market_data(self, stock_code: str) -> Optional[MarketData]:
        """获取真实行情数据 - 使用 AmazingData SDK"""
        if not self.connected:
            raise Exception("AmazingData SDK: 网关未连接")

        try:
            # 使用 asyncio.to_thread 将同步 SDK 调用移到线程池执行，避免阻塞事件循环
            result = await asyncio.to_thread(self._query_market_data_sync, stock_code)
            return result

        except Exception as e:
            logger.error(f"获取 AmazingData 行情失败：{e}")
            raise Exception(f"AmazingData SDK 获取行情失败：{str(e)}")

    def _query_market_data_sync(self, stock_code: str) -> Optional[MarketData]:
        """同步查询行情数据（在线程池中执行）"""
        import datetime
        import sqlite3
        from pathlib import Path

        # 使用缓存的 MarketData 实例，避免重复创建导致连接数超限
        md = self._get_market_data()

        # 规范化股票代码格式 - 确保有 .SH/.SZ 后缀
        original_code = stock_code
        if '.' not in stock_code:
            if stock_code.startswith('6'):
                stock_code = f"{stock_code}.SH"
            else:
                stock_code = f"{stock_code}.SZ"

        # 从本地数据库获取股票名称（SDK 不返回股票名称）
        # 优先从 stock_monthly_factors 获取，因为 kline_data 可能只有股票代码
        stock_name = original_code  # 默认使用股票代码
        try:
            kline_db_path = Path(__file__).parent.parent.parent / "data" / "kline.db"
            if kline_db_path.exists():
                # 使用同步 sqlite3 查询（在线程池中执行）
                conn = sqlite3.connect(str(kline_db_path))
                cursor = conn.cursor()
                # 优先从 stock_monthly_factors 获取股票名称
                cursor.execute(
                    "SELECT stock_name FROM stock_monthly_factors WHERE stock_code = ? LIMIT 1",
                    (stock_code,)
                )
                row = cursor.fetchone()
                if row and row[0]:
                    stock_name = row[0]
                else:
                    # 退而求其次，从 kline_data 获取
                    cursor.execute(
                        "SELECT DISTINCT stock_name FROM kline_data WHERE stock_code = ? LIMIT 1",
                        (stock_code,)
                    )
                    row = cursor.fetchone()
                    if row and row[0]:
                        stock_name = row[0]
                cursor.close()
                conn.close()
        except Exception as e:
            logger.debug(f"从本地数据库获取股票名称失败：{e}")

        # 使用 query_kline 方法获取当日数据（获取 2 天用于计算 prev_close）- SDK 使用半开区间，需要将 end_date 加 1 天
        end_dt = get_china_time()
        begin_dt = end_dt - datetime.timedelta(days=2)  # 获取 2 天数据，用于计算 prev_close
        end_date = int((end_dt + datetime.timedelta(days=1)).strftime('%Y%m%d'))
        begin_date = int(begin_dt.strftime('%Y%m%d'))

        kline_data = md.query_kline(
            code_list=[stock_code],
            begin_date=begin_date,
            end_date=end_date,
            period=self.constant.Period.day.value
        )

        if kline_data and stock_code in kline_data:
            df = kline_data[stock_code]
            if len(df) > 0:
                # 获取最后一行数据（最新的 K 线数据）
                last_row = df.iloc[-1]

                # 提取关键字段
                current_price = float(last_row.get('close', 0)) if 'close' in last_row else 0
                high = float(last_row.get('high', current_price)) if 'high' in last_row else current_price
                low = float(last_row.get('low', current_price)) if 'low' in last_row else current_price
                open_price = float(last_row.get('open', current_price)) if 'open' in last_row else current_price

                # 计算 prev_close：优先使用 SDK 返回的 pre_close，否则使用前一日收盘价
                prev_close = None
                if 'pre_close' in last_row:
                    prev_close = float(last_row.get('pre_close', 0))
                elif len(df) >= 2:
                    # 使用前一日收盘价作为 prev_close
                    prev_row = df.iloc[-2]
                    prev_close = float(prev_row.get('close', current_price))

                # 如果仍然没有 prev_close，使用当前价格（fallback）
                if prev_close is None or prev_close == 0:
                    prev_close = current_price

                volume = int(last_row.get('volume', 0)) if 'volume' in last_row else 0
                amount = float(last_row.get('amount', 0)) if 'amount' in last_row else 0

                # 计算涨跌幅
                change_percent = ((current_price - prev_close) / prev_close * 100) if prev_close != 0 else 0

                return MarketData(
                    stock_code=stock_code,
                    stock_name=stock_name,
                    current_price=current_price,
                    change_percent=change_percent,
                    high=high,
                    low=low,
                    open_price=open_price,
                    prev_close=prev_close,
                    volume=volume,
                    amount=amount,
                    bid=[current_price * 0.999] * 5,
                    ask=[current_price * 1.001] * 5
                )

        logger.warning(f"未获取到 {stock_code} 的 K 线数据")
        raise Exception(f"AmazingData SDK: 未获取到 {stock_code} 的行情数据")

    async def get_stock_list(self) -> List[Dict[str, str]]:
        """获取股票列表 - 使用 AmazingData SDK 查询主要指数成分股"""
        if not self.connected:
            raise Exception("AmazingData SDK: 网关未连接")

        try:
            # 使用 asyncio.to_thread 将同步 SDK 调用移到线程池执行
            stock_list = await asyncio.to_thread(self._query_stock_list_sync)
            return stock_list

        except Exception as e:
            logger.error(f"获取股票列表失败：{e}")
            raise Exception(f"AmazingData SDK: 获取股票列表失败 - {str(e)}")

    def _query_stock_list_sync(self) -> List[Dict[str, str]]:
        """同步查询股票列表（在线程池中执行）"""
        import sqlite3
        from pathlib import Path
        from services.common.sdk_manager import get_sdk_manager

        stock_list = []

        # 首先尝试从本地数据库获取股票列表（最可靠）
        db_paths = [
            Path('/home/bobo/StockWinner/data/kline.db'),
            Path('/home/bobo/StockWinner/stockwinner.db'),
        ]

        for db_path in db_paths:
            if db_path.exists():
                try:
                    conn = sqlite3.connect(str(db_path))
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

        # 无法获取股票列表，返回错误
        raise Exception("AmazingData SDK: 无法获取股票列表，请先下载基础数据或检查SDK连接")

    async def get_index_list(self) -> List[Dict[str, str]]:
        """获取指数列表 - 使用 AmazingData SDK"""
        if not self.connected:
            raise Exception("AmazingData SDK: 网关未连接")

        try:
            index_list = await asyncio.to_thread(self._query_index_list_sync)
            return index_list
        except Exception as e:
            logger.error(f"获取指数列表失败：{e}")
            raise Exception(f"AmazingData SDK: 获取指数列表失败 - {str(e)}")

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
            raise Exception("AmazingData SDK: 网关未连接")

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

    async def buy(self, stock_code: str, price: float, quantity: int) -> OrderResult:
        """真实买入"""
        if not self.connected:
            return OrderResult(False, message="网关未连接")

        try:
            logger.warning(f"buy 待实现 - 参数：{stock_code}, {price}, {quantity}")
            return OrderResult(False, message="AmazingData SDK 交易接口待实现")
        except Exception as e:
            logger.error(f"买入失败：{e}")
            return OrderResult(False, message=str(e))

    async def sell(self, stock_code: str, price: float, quantity: int) -> OrderResult:
        """真实卖出"""
        if not self.connected:
            return OrderResult(False, message="网关未连接")

        try:
            logger.warning(f"sell 待实现 - 参数：{stock_code}, {price}, {quantity}")
            return OrderResult(False, message="AmazingData SDK 交易接口待实现")
        except Exception as e:
            logger.error(f"卖出失败：{e}")
            return OrderResult(False, message=str(e))

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
        """批量获取行情数据 - AmazingData"""
        results = {}
        for code in stock_codes:
            try:
                results[code] = await self.get_market_data(code)
            except Exception as e:
                logger.warning(f"获取 {code} 行情失败：{e}")
                results[code] = None
        return results

    async def get_kline_data(
        self,
        stock_code: str,
        period: str = "day",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: int = 100,
        task_type: str = "query"  # query, download, screening
    ) -> List[Dict[str, Any]]:
        """获取 K 线历史数据 - AmazingData（带连接排队）"""
        if not self.connected:
            raise Exception("AmazingData SDK: 网关未连接")

        # 使用连接管理器获取令牌
        from services.common.sdk_connection_manager import get_connection_manager, TaskType

        conn_mgr = get_connection_manager()
        task_type_enum = TaskType.QUERY if task_type == "query" else (
            TaskType.DOWNLOAD if task_type == "download" else TaskType.SCREENING
        )

        # 获取连接令牌（会自动排队等待）
        token = await conn_mgr.acquire(task_type=task_type_enum)

        try:
            # 使用 asyncio.to_thread 将同步 SDK 调用移到线程池执行
            result = await asyncio.to_thread(
                self._query_kline_data_sync,
                stock_code, period, start_date, end_date, limit
            )
            return result
        finally:
            # 释放连接令牌
            token.release()

    async def get_batch_kline_data(
        self,
        stock_codes: List[str],
        period: str = "day",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: int = 100,
        task_type: str = "query"  # query, download, screening
    ) -> Dict[str, Any]:
        """批量获取 K 线历史数据 - AmazingData SDK（带连接排队）"""
        if not self.connected:
            raise Exception("AmazingData SDK: 网关未连接")

        # 使用连接管理器获取令牌
        from services.common.sdk_connection_manager import get_connection_manager, TaskType

        conn_mgr = get_connection_manager()
        task_type_enum = TaskType.QUERY if task_type == "query" else (
            TaskType.DOWNLOAD if task_type == "download" else TaskType.SCREENING
        )

        # 获取连接令牌（会自动排队等待）
        token = await conn_mgr.acquire(task_type=task_type_enum)

        try:
            result = await asyncio.to_thread(
                self._query_batch_kline_data_sync,
                stock_codes, period, start_date, end_date, limit
            )
            return result
        finally:
            # 释放连接令牌
            token.release()

    def _query_batch_kline_data_sync(
        self,
        stock_codes: List[str],
        period: str = "day",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: int = 100
    ) -> Dict[str, Any]:
        """同步批量查询 K 线数据（在线程池中执行）- AmazingData SDK"""
        import datetime as dt
        import pandas as pd

        # 解析日期
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

        # 使用缓存的 MarketData 实例，避免重复创建导致连接数超限
        md = self._get_market_data()

        # 映射 period 参数
        period_map = {
            "1m": self.constant.Period.min1.value,
            "5m": self.constant.Period.min5.value,
            "15m": self.constant.Period.min15.value,
            "30m": self.constant.Period.min30.value,
            "60m": self.constant.Period.min60.value,
            "day": self.constant.Period.day.value,
            "week": self.constant.Period.week.value,
            "month": self.constant.Period.month.value,
        }
        period_value = period_map.get(period, self.constant.Period.day.value)

        # 格式化日期 - SDK 使用半开区间 [begin_date, end_date)，需要将 end_date 加 1 天
        begin_date_int = int(start_dt.strftime('%Y%m%d'))
        end_date_int = int((end_dt + timedelta(days=1)).strftime('%Y%m%d'))

        # 批量查询
        kline_data = md.query_kline(
            code_list=stock_codes,
            begin_date=begin_date_int,
            end_date=end_date_int,
            period=period_value
        )

        # 字段重命名：将 SDK 的 kline_time 重命名为 trade_date，与数据库保持一致
        if kline_data:
            for code, df in kline_data.items():
                if df is not None and isinstance(df, pd.DataFrame) and 'kline_time' in df.columns:
                    # 将 kline_time 重命名为 trade_date
                    df = df.rename(columns={'kline_time': 'trade_date'})
                    # 将日期转换为字符串格式 YYYY-MM-DD
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
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """同步查询 K 线数据（在线程池中执行）"""
        import datetime as dt

        # 解析日期
        if end_date:
            end_dt = dt.datetime.strptime(end_date, "%Y%m%d")
        else:
            end_dt = get_china_time()

        if start_date:
            start_dt = dt.datetime.strptime(start_date, "%Y%m%d")
        else:
            # 根据 limit 和 period 推算开始日期
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

        # 使用缓存的 MarketData 实例，避免重复创建导致连接数超限
        md = self._get_market_data()

        # 规范化股票代码格式
        if '.' not in stock_code:
            if stock_code.startswith('6'):
                stock_code = f"{stock_code}.SH"
            else:
                stock_code = f"{stock_code}.SZ"

        # 映射 period 参数
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

        # 查询 K 线数据
        kline_data = md.query_kline(
            code_list=[stock_code],
            begin_date=int(start_dt.strftime("%Y%m%d")),
            end_date=int(end_dt.strftime("%Y%m%d")),
            period=actual_period
        )

        if kline_data and stock_code in kline_data:
            df = kline_data[stock_code]
            if len(df) > 0:
                # 限制返回数量
                if len(df) > limit:
                    df = df.tail(limit)

                # 转换为字典列表（统一使用 trade_date 字段名，与 SDK 和数据库保持一致）
                result = []
                for idx, row in df.iterrows():
                    time_val = row.get("trade_date", "")
                    if not time_val:
                        time_val = row.get("time", "")

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

# 工厂函数：根据环境返回合适的网关
def create_gateway(galaxy_app_id: str = "", galaxy_password: str = "") -> TradingGatewayInterface:
    """
    创建交易网关实例

    Args:
        galaxy_app_id: 银河 SDK 应用 ID (broker_account)
        galaxy_password: 银河 SDK 密码 (broker_password)

    Returns:
        TradingGatewayInterface 实例，如果真实数据源不可用则抛出异常
    """
    # 优先尝试使用 AmazingData 网关
    amazing_data_gateway = AmazingDataTradingGateway(app_id=galaxy_app_id, password=galaxy_password)
    if amazing_data_gateway.sdk_available:
        logger.info("使用 AmazingData 交易网关")
        return amazing_data_gateway
    else:
        logger.warning("AmazingData SDK 不可用")

    # 如果 AmazingData 不可用，尝试使用银河网关
    galaxy_gateway = GalaxyTradingGateway(app_id=galaxy_app_id, password=galaxy_password)
    if galaxy_gateway.sdk_available:
        logger.info("使用银河交易网关")
        return galaxy_gateway
    else:
        raise Exception("银河 SDK 和 AmazingData SDK 均不可用，无法创建交易网关")


# 全局网关实例
_gateway: Optional[TradingGatewayInterface] = None
# 全局网关 credentials 缓存：account_id -> gateway instance
_gateway_cache: Dict[str, TradingGatewayInterface] = {}


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
